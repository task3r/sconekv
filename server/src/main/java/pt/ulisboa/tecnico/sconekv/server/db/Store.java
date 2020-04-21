package pt.ulisboa.tecnico.sconekv.server.db;

import kotlin.reflect.jvm.internal.ReflectProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.ReadOperation;
import pt.ulisboa.tecnico.sconekv.common.db.WriteOperation;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.OutdatedVersionException;

import java.util.HashMap;
import java.util.Map;

public class Store {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private final Map<String, Value> values;

    public Store() {
        this.values = new HashMap<>();
    }

    public Value get(String key) {
        synchronized (values) {
            if (!values.containsKey(key))
                values.put(key, new Value());
            return values.get(key);
        }
    }

    private void put(String key, byte[] value, short version) throws OutdatedVersionException {
        synchronized (values) {
            if (values.containsKey(key))
                values.get(key).update(value, version);
            else
                values.put(key, new Value(value, version));
        }
    }

    public boolean validate(Transaction tx) throws OutdatedVersionException {
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
            releaseLocks(tx);
            throw e;
        }
    }

    public void releaseLocks(Transaction tx) {
        for (Operation op : tx.getRwSet())
            this.get(op.getKey()).releaseLock(tx.getId());
    }

    public void perform(Transaction tx) throws InvalidOperationException {
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
    }

    private void perform(WriteOperation op) throws OutdatedVersionException {
        this.put(op.getKey(), op.getValue(), (short) (op.getVersion() + 1));
    }

    private void perform(ReadOperation op) {
        // do nothing?
    }
}
