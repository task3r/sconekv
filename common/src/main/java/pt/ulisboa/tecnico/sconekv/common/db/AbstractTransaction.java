package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.util.List;

public abstract class AbstractTransaction {

    protected enum State {
        COMMITTED,
        ABORTED,
        NONE
    }

    private TransactionID id;
    private State state;

    protected AbstractTransaction(TransactionID id) {
        this.id = id;
        this.state = State.NONE;
    }

    protected abstract void addOperation(Operation op);

    public TransactionID getId() {
        return id;
    }

    public abstract List<Operation> getRwSet();

    public State getState() {
        return state;
    }

    protected void setState(State state) throws InvalidTransactionStateChangeException {
        if (this.state != State.NONE)
            throw new InvalidTransactionStateChangeException();
        this.state = state;
    }
}
