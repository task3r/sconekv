package pt.ulisboa.tecnico.sconekv.client.db;

import kotlin.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.common.db.WriteOperation;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import pt.ulisboa.tecnico.sconekv.common.db.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.db.ReadOperation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

import java.io.IOException;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private SconeClient client;
    // map bucket:ops ?

    protected Transaction(SconeClient client,TransactionID id) {
        super(id);
        this.client = client;
    }

    public byte[] read(String key) throws IOException {
        Pair<byte[], ReadOperation> response = client.performRead(getId(), key);
        addOperation(response.getSecond());
        return response.getFirst();
    }

    public void write(String key, byte[] value) throws IOException {
        WriteOperation op = client.performWrite(getId(), key, value);
        addOperation(op);
    }

    public void commit() throws InvalidTransactionStateChangeException, IOException, CommitFailedException {
        if (getState() != State.NONE)
            throw new InvalidTransactionStateChangeException();
        if (!client.performCommit(getId(), getRwSet()))
            throw new CommitFailedException();
        setState(State.COMMITED);
    }

    public void abort() throws InvalidTransactionStateChangeException { //Specific client side exceptions?
        setState(State.ABORTED);
    }
}
