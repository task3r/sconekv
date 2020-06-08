package pt.ulisboa.tecnico.sconekv.client.db;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.SconeClient;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.RequestFailedException;
import pt.ulisboa.tecnico.sconekv.common.db.*;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private SconeClient client;
    private Map<String, Operation> rwSet;
    // map bucket:ops ?

    public Transaction(SconeClient client,TransactionID id) {
        super(id);
        this.client = client;
        this.rwSet = new HashMap<>();
    }

    public byte[] read(String key) throws InvalidTransactionStateChangeException, RequestFailedException {
        validate();
        if (rwSet.containsKey(key)) // repeatable reads and read-my-writes
            return rwSet.get(key).getValue();
        Pair<byte[], Short> response = client.performRead(getId(), key);
        addOperation(new ReadOperation(key, response.getValue1(), response.getValue0()));
        return response.getValue0();
    }

    public void write(String key, byte[] value) throws InvalidTransactionStateChangeException, RequestFailedException {
        validate();
        if (rwSet.containsKey(key)) {
            rwSet.replace(key, new WriteOperation(key, rwSet.get(key).getVersion(), value));
        } else {
            short version = client.performWrite(getId(), key);
            addOperation(new WriteOperation(key, version, value));
        }
    }

    public void commit() throws InvalidTransactionStateChangeException, RequestFailedException {
        validate();
        if (!client.performCommit(getId(), getRwSet()))
            throw new CommitFailedException();
        setState(TransactionState.COMMITTED);
    }

    public void abort() throws InvalidTransactionStateChangeException { //Specific client side exceptions?
        setState(TransactionState.ABORTED);
    }

    private void validate() throws InvalidTransactionStateChangeException {
        if (getState() != TransactionState.NONE)
            throw new InvalidTransactionStateChangeException();
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
