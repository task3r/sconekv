package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidVersionException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Store {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Map<String, Value> values;
    private final Map<TransactionID, Transaction> transactions;

    public Store() {
        this.values = new ConcurrentHashMap<>();
        this.transactions = new ConcurrentHashMap<>();
    }

    public synchronized Value get(String key) {
        if (!values.containsKey(key))
            values.put(key, new Value());
        return values.get(key);
    }

    private synchronized void put(String key, byte[] value, short version) {
        if (values.containsKey(key))
            values.get(key).update(value, version);
        else
            values.put(key, new Value(value, version));
    }

    public void addTransaction(Transaction tx) {
        if (transactions.containsKey(tx.getId()))
            logger.error("TxID {} was already added to the store", tx.getId());
        else
            this.transactions.put(tx.getId(), tx);
    }

    public Transaction getTransaction(TransactionID txID) {
        return transactions.get(txID);
    }

    public synchronized void validate(TransactionID txID) throws ValidTransactionNotLockableException {
        Transaction tx = transactions.get(txID);
        try {
            boolean lockable = true;
            for (Operation op : tx.getRwSet()) {
                if (lockable)
                    lockable = this.get(op.getKey()).validateAndLock(tx.getId(), op);
                else
                    this.get(op.getKey()).validate(op);
            }

            if (lockable) {
                tx.setState(TransactionState.PREPARED);
            } else {
                throw new ValidTransactionNotLockableException();
            }
        } catch (InvalidVersionException e) {
            tx.setState(TransactionState.ABORTED);
        }
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
            restartTxs.add(this.get(op.getKey()).releaseLock(tx.getId()));
        }
        restartTxs.remove(null); // releaseLock might return null
        return restartTxs;
    }

    public synchronized void commit(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx.getState() != TransactionState.COMMITTED) {
            for (Operation op : tx.getRwSet()) {
                if (op instanceof WriteOperation) {
                    this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1));
                }
            }
            tx.setState(TransactionState.COMMITTED);
        }
    }

    public synchronized void abort(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx.getState() != TransactionState.ABORTED) {
            tx.setState(TransactionState.ABORTED);
        }
    }

    public synchronized Set<TransactionID> resetTx(TransactionID txID) {
        transactions.get(txID).setState(TransactionState.NONE);
        return releaseLocks(txID);
    }
}
