package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.util.List;

public abstract class AbstractTransaction {



    private TransactionID id;
    private TransactionState state;

    public AbstractTransaction(TransactionID id) {
        this.id = id;
        this.state = TransactionState.NONE;
    }

    protected abstract void addOperation(Operation op);

    public TransactionID getId() {
        return id;
    }

    public abstract List<Operation> getRwSet();

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) throws InvalidTransactionStateChangeException {
        if (this.state == TransactionState.COMMITTED || this.state == TransactionState.ABORTED)
            throw new InvalidTransactionStateChangeException();
        this.state = state;
    }
}
