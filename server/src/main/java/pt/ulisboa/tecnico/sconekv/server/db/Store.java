package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.ReadOperation;
import pt.ulisboa.tecnico.sconekv.common.db.WriteOperation;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.WriteOutdatedVersionException;

import java.util.HashMap;
import java.util.Map;

public class Store {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private Map<String, Value> values;

    public Store() {
        this.values = new HashMap<>();
    }

    public Value get(String key) {
        return values.getOrDefault(key, new Value());
    }

    private void put(String key, byte[] value, short version) throws WriteOutdatedVersionException {
        if (values.containsKey(key))
            values.get(key).update(value, version);
        else
            values.put(key, new Value(value, version));
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

    private void perform(WriteOperation op) throws WriteOutdatedVersionException {
        this.put(op.getKey(), op.getValue(), (short) (op.getVersion()+1));
    }

    private void perform(ReadOperation op) {
        // do nothing?
    }
}