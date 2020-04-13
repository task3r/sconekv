package pt.ulisboa.tecnico.sconekv.common.transaction;

import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTransaction {

    protected enum State {
        COMMITED,
        ABORTED,
        NONE
    }

    private TransactionID id;
    private List<Operation> rwSet;
    private State state;

    protected AbstractTransaction(TransactionID id) {
        this.id = id;
        this.state = State.NONE;
        this.rwSet = new ArrayList<>();
    }

    protected void addOperation(Operation op) {
        rwSet.add(op);
    }

    public TransactionID getId() {
        return id;
    }

    public List<Operation> getRwSet() {
        return rwSet;
    }

    public State getState() {
        return state;
    }

    protected void setState(State state) throws InvalidTransactionStateChangeException {
        if (this.state != State.NONE)
            throw new InvalidTransactionStateChangeException();
        this.state = state;
    }

}
