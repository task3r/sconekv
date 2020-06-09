package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.OutdatedVersionException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;

import java.util.Map;
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

            // here maybe queue the locks?

            if (lockable)
                tx.setState(TransactionState.PREPARED);
            else
                throw new ValidTransactionNotLockableException();

        } catch (OutdatedVersionException e) {
            releaseLocks(txID);
            tx.setState(TransactionState.ABORTED);
        }
    }

    public synchronized void releaseLocks(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        for (Operation op : tx.getRwSet())
            this.get(op.getKey()).releaseLock(tx.getId());
    }

    public synchronized void commit(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        for (Operation op : tx.getRwSet()) {
            if (op instanceof WriteOperation) {
                commit((WriteOperation) op);
            } else if (op instanceof ReadOperation) {
                commit((ReadOperation) op);
            } else {
                logger.error("Received invalid operation, ignoring...");
            }
        }
        tx.setState(TransactionState.COMMITTED);
    }

    private void commit(WriteOperation op) {
        this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1));
    }

    private void commit(ReadOperation op) {
        // do nothing?
    }

    public synchronized void abort(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        tx.setState(TransactionState.ABORTED);
    }

    public synchronized void resetTx(TransactionID txID) {
        releaseLocks(txID);
        transactions.get(txID).setState(TransactionState.NONE);
    }
}
