package pt.ulisboa.tecnico.sconekv.client.db;

import kotlin.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private SconeClient client;
    private Map<String, Operation> rwSet;
    // map bucket:ops ?

    protected Transaction(SconeClient client,TransactionID id) {
        super(id);
        this.client = client;
        this.rwSet = new HashMap<>();
    }

    public byte[] read(String key) throws IOException {
        if (rwSet.containsKey(key)) // repeatable reads and read-my-writes
            return rwSet.get(key).getValue();
        Pair<byte[], Short> response = client.performRead(getId(), key);
        addOperation(new ReadOperation(key, response.getSecond(), response.getFirst()));
        return response.getFirst();
    }

    public void write(String key, byte[] value) throws IOException {
        if (rwSet.containsKey(key)) {
            rwSet.replace(key, new WriteOperation(key, rwSet.get(key).getVersion(), value));
        } else {
            short version = client.performWrite(getId(), key);
            addOperation(new WriteOperation(key, version, value));
        }
    }

    public void commit() throws InvalidTransactionStateChangeException, IOException, CommitFailedException {
        if (getState() != State.NONE)
            throw new InvalidTransactionStateChangeException();
        if (!client.performCommit(getId(), getRwSet()))
            throw new CommitFailedException();
        setState(State.COMMITTED);
    }

    public void abort() throws InvalidTransactionStateChangeException { //Specific client side exceptions?
        setState(State.ABORTED);
    }

    @Override
    protected void addOperation(Operation op) {
        rwSet.put(op.getKey(), op);
    }

    @Override
    public List<Operation> getRwSet() {
        return new ArrayList<>(rwSet.values());
    }
}
