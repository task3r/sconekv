package pt.ulisboa.tecnico.sconekv.server.db;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.exceptions.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class Store {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Map<String, Value> values;
    private final Map<TransactionID, Transaction> transactions;
    private final Queue<Pair<TransactionID, LocalDateTime>> completedTransactions;

    public Store() {
        this.values = new ConcurrentHashMap<>();
        this.transactions = new ConcurrentHashMap<>();
        this.completedTransactions = new ConcurrentLinkedQueue<>();

        // Garbage Collection
        Timer timer = new Timer("GarbageCollectionTimer", true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                LocalDateTime gcTime = LocalDateTime.now().minusSeconds(SconeConstants.TX_TTL);
                boolean runGC = true;
                while (runGC) {
                    Pair<TransactionID, LocalDateTime> head = completedTransactions.peek();
                    if (head != null && head.getValue1().isBefore(gcTime)) {
                        transactions.remove(head.getValue0());
                        completedTransactions.remove();
                    } else {
                        runGC = false;
                    }
                }
            }
        }, 0, TimeUnit.SECONDS.toMillis(SconeConstants.GC_PERIOD));
    }

    public synchronized Value get(String key) {
        if (!values.containsKey(key))
            values.put(key, new Value(key));
        return values.get(key);
    }

    private synchronized Set<TransactionID> put(String key, byte[] value, short version) {
        if (!values.containsKey(key))
            values.put(key, new Value(key));

        return values.get(key).update(value, version);
    }

    public boolean addTransaction(Transaction tx) {
        if (transactions.containsKey(tx.getId())) {
            return false;
        } else {
            this.transactions.put(tx.getId(), tx);
            return true;
        }
    }

    public Transaction getTransaction(TransactionID txID) {
        return transactions.get(txID);
    }

    public synchronized void validate(TransactionID txID) throws ValidTransactionNotLockableException, AlreadyProcessedTransaction {
        Transaction tx = transactions.get(txID);
        try {
            if (tx.isDecided()) {
                logger.debug("Already decided transaction {}, ignoring", txID);
                throw new AlreadyProcessedTransaction();
            } else {
                Set<TransactionID> currentOwners = validateAndLock(tx);

                if (currentOwners.isEmpty()) { // acquired locks for all keys
                    logger.debug("Locally accepted {}", txID);
                    tx.setState(TransactionState.PREPARED);
                } else {
                    simplyReleaseLocks(tx);
                    queueLocks(tx);

                    if (logger.isDebugEnabled()) {
                        StringBuilder s = new StringBuilder();
                        for (TransactionID id : currentOwners)
                            s.append(id).append(",");
                        logger.debug("Locally accepted {} but it's not lockable {}", txID, s);
                    }

                    Optional<TransactionID> min = currentOwners.stream().min(TransactionID::compareTo);
                    if (min.isPresent() && min.get().isGreater(txID)) // if txID is lower than all currentOwners, then they should rollback
                        throw new ValidTransactionNotLockableException(currentOwners);
                    else
                        throw new ValidTransactionNotLockableException();
                }
            }
        } catch (InvalidVersionException e) {
            localReject(tx);
        }
    }

    private void simplyReleaseLocks(Transaction tx) {
        for (Operation op : tx.getRwSet()) // release any locks it acquired but do not queue next (as it is synchronized, if it acquired any locks the txIDs in the queue were there already)
            this.get(op.getKey()).releaseLock(tx.getId());
    }

    public void localReject(Transaction tx) {
        simplyReleaseLocks(tx);
        unqueueTransaction(tx);
        tx.setState(TransactionState.ABORTED);
        logger.debug("Locally rejected {}", tx.getId());
    }

    private void unqueueTransaction(Transaction tx) {
        for (Operation op : tx.getRwSet())
            this.get(op.getKey()).removeFromQueue(tx.getId());
    }

    private Set<TransactionID> validateAndLock(Transaction tx) throws InvalidVersionException {
        Set<TransactionID> owners = new HashSet<>();
        Set<TransactionID> currentOwnersForKey;
        for (Operation op : tx.getRwSet()) {
            if (owners.isEmpty()) { // transaction is still lockable
                currentOwnersForKey = this.get(op.getKey()).validateAndLock(tx.getId(), op);
                if (!currentOwnersForKey.contains(tx.getId()))
                    owners.addAll(currentOwnersForKey);
            } else {
                currentOwnersForKey = this.get(op.getKey()).validate(op);
                currentOwnersForKey.remove(tx.getId()); // rollback could have locked key with this txID
                owners.addAll(currentOwnersForKey);
            }
        }
        return owners;
    }

    private synchronized void queueLocks(Transaction tx) {
        for (Operation op : tx.getRwSet()) {
            this.get(op.getKey()).queueLock(tx.getId(), op.getType());
        }
    }

    public synchronized Set<TransactionID> releaseLocks(TransactionID txID, boolean butQueue) {
        Transaction tx = transactions.get(txID);
        Set<TransactionID> restartTxs = new HashSet<>();
        for (Operation op : tx.getRwSet()) {
            if (butQueue) { // in case of a rollback of txID
                restartTxs.addAll(this.get(op.getKey()).releaseLockButQueue(tx.getId(), op.getType()));
            } else { // the normal case, txID is decided or reset
                restartTxs.addAll(this.get(op.getKey()).releaseLockAndChangeToNext(tx.getId()));
            }
        }
        if (logger.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            for (TransactionID id : restartTxs)
                s.append(id).append(",");
            logger.debug("Released locks for {}, restarting {}", txID, s);
        }
        return restartTxs;
    }

    public synchronized Set<TransactionID> commit(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        HashSet<TransactionID> txsToAbort = new HashSet<>();
        if (tx.getState() != TransactionState.COMMITTED) {
            for (Operation op : tx.getRwSet()) {
                if (op instanceof WriteOperation) {
                    txsToAbort.addAll(this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1)));
                } else if (op instanceof DeleteOperation) {
                    txsToAbort.addAll(values.get(op.getKey()).getLockQueue());
                    values.remove(op.getKey()); // maybe could simply turn it invisible in the future
                }
            }
            tx.setState(TransactionState.COMMITTED);
        }
        tx.setDecided();
        completedTransactions.add(new Pair<>(tx.getId(), LocalDateTime.now()));
        return txsToAbort;
    }

    public synchronized void abort(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx.getState() != TransactionState.ABORTED) {
            tx.setState(TransactionState.ABORTED);
        }
        tx.setDecided();
        completedTransactions.add(new Pair<>(tx.getId(), LocalDateTime.now()));
    }

    public synchronized Set<TransactionID> resetTx(TransactionID txID) {
        logger.debug("Reset {}", txID);
        transactions.get(txID).setState(TransactionState.RECEIVED);
        return releaseLocks(txID ,false);
    }

    public synchronized List<Transaction> getPendingTransactions() {
        List<Transaction> pendingTransactions = new ArrayList<>();
        for (Transaction tx : transactions.values()) {
            if (!tx.isDecided())
                pendingTransactions.add(tx);
        }
        return  pendingTransactions;
    }

    public synchronized boolean rollback(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx != null && !tx.isDecided()) {
            tx.setState(TransactionState.RECEIVED);
            return true;
        } else {
            return false;
        }
    }
}
