package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.OutdatedVersionException;

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

    private synchronized void put(String key, byte[] value, short version) throws OutdatedVersionException {
        if (values.containsKey(key))
            values.get(key).update(value, version);
        else
            values.put(key, new Value(value, version));
    }

    public void addTransaction(Transaction tx) {
        this.transactions.put(tx.getId(), tx);
    }

    public Transaction getTransaction(TransactionID txID) {
        return transactions.get(txID);
    }

    public synchronized boolean validate(TransactionID txID) throws OutdatedVersionException {
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

            return lockable;

        } catch (OutdatedVersionException e) {
            releaseLocks(txID);
            throw e;
        }
    }

    public synchronized void releaseLocks(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        for (Operation op : tx.getRwSet())
            this.get(op.getKey()).releaseLock(tx.getId());
    }

    public synchronized void perform(TransactionID txID) throws InvalidOperationException, InvalidTransactionStateChangeException {
        Transaction tx = transactions.get(txID);
        for (Operation op : tx.getRwSet()) {
            if (op instanceof WriteOperation) {
                perform((WriteOperation) op);
            } else if (op instanceof ReadOperation) {
                perform((ReadOperation) op);
            } else {
                logger.error("Received invalid operation, aborting...");
                throw new InvalidOperationException();
            }
        }
        tx.setState(TransactionState.COMMITTED);
    }

    private void perform(WriteOperation op) throws OutdatedVersionException {
        this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1));
    }

    private void perform(ReadOperation op) {
        // do nothing?
    }

    public synchronized void abort(TransactionID txID) {
        Transaction tx = transactions.get(txID);
        if (tx.getState() != TransactionState.ABORTED) {
            try {
                tx.setState(TransactionState.ABORTED);
            } catch (InvalidTransactionStateChangeException e) {
                logger.error("Error aborting transaction {}, state is {}", txID, tx.getState());
            }
        }
    }
}
