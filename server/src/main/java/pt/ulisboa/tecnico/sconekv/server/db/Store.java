package pt.ulisboa.tecnico.sconekv.server.db;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.exceptions.AlreadyProcessedTransaction;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidVersionException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;

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

    private synchronized void put(String key, byte[] value, short version) {
        if (values.containsKey(key))
            values.get(key).update(value, version);
        else
            values.put(key, new Value(key, value, version));
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
            simplyReleaseLocks(tx);
            tx.setState(TransactionState.ABORTED);
            logger.debug("Locally rejected {}", txID);
        }
    }

    private void simplyReleaseLocks(Transaction tx) {
        for (Operation op : tx.getRwSet()) // release any locks it acquired but do not queue next (as it is synchronized, if it acquired any locks the txIDs in the queue were there already)
            this.get(op.getKey()).releaseLock(tx.getId());
    }

    private Set<TransactionID> validateAndLock(Transaction tx) throws InvalidVersionException {
        Set<TransactionID> owners = new HashSet<>();
        TransactionID currentOwner;
        for (Operation op : tx.getRwSet()) {
            if (owners.isEmpty()) { // transaction is still lockable
                currentOwner = this.get(op.getKey()).validateAndLock(tx.getId(), op);
                if (!tx.getId().equals(currentOwner))
                    owners.add(currentOwner);
            } else {
                currentOwner = this.get(op.getKey()).validate(op);
                if (currentOwner != null)
                    owners.add(currentOwner);
            }
        }
        return owners;
    }

    public synchronized void queueLocks(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        for (Operation op : tx.getRwSet()) {
            this.get(op.getKey()).queueLock(tx.getId());
        }
    }

    public synchronized Set<TransactionID> releaseLocks(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        Set<TransactionID> restartTxs = new HashSet<>();
        for (Operation op : tx.getRwSet()) {
            restartTxs.add(this.get(op.getKey()).releaseLockAndQueueNext(tx.getId()));
        }
        restartTxs.remove(null); // releaseLock might return null
        if (logger.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            for (TransactionID id : restartTxs)
                s.append(id).append(",");
            logger.debug("Released locks for {}, restarting {}", txID, s);
        }
        return restartTxs;
    }

    public synchronized void commit(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx.getState() != TransactionState.COMMITTED) {
            for (Operation op : tx.getRwSet()) {
                if (op instanceof WriteOperation) {
                    this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1));
                } else if (op instanceof DeleteOperation) {
                    values.remove(op.getKey()); // maybe could simply turn it invisible in the future
                }
            }
            tx.setState(TransactionState.COMMITTED);
        }
        tx.setDecided();
        completedTransactions.add(new Pair<>(tx.getId(), LocalDateTime.now()));
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
        transactions.get(txID).setState(TransactionState.RECEIVED);
        return releaseLocks(txID);
    }

    public synchronized List<Transaction> getPendingTransactions() {
        List<Transaction> pendingTransactions = new ArrayList<>();
        for (Transaction tx : transactions.values()) {
            if (!tx.isDecided())
                pendingTransactions.add(tx);
        }
        return  pendingTransactions;
    }
}
