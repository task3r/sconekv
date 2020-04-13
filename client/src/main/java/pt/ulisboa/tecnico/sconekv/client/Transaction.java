package pt.ulisboa.tecnico.sconekv.client;

import kotlin.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import pt.ulisboa.tecnico.sconekv.common.transaction.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.transaction.ReadOperation;
import pt.ulisboa.tecnico.sconekv.common.transaction.TransactionID;

import java.io.IOException;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private SconeClient client;

    protected Transaction(SconeClient client,TransactionID id) {
        super(id);
        this.client = client;
    }


    public byte[] read(String key) throws IOException {
        Pair<byte[], Short> response = client.performRead(getId(), key);
        addOperation(new ReadOperation(key, response.getSecond()));
        logger.info("Read {}: {}, version: {}", key, new String(response.getFirst()), response.getSecond());
        return response.getFirst();
    }

    public void write(String key, byte[] value) {

    }

    public void commit() throws InvalidTransactionStateChangeException {
        if (getState() != State.NONE)
            throw new InvalidTransactionStateChangeException();

    }

    public void abort() throws InvalidTransactionStateChangeException { //Specific client side exceptions?
        setState(State.ABORTED);
    }
}
