package pt.ulisboa.tecnico.sconekv.common.db;

import java.util.List;

public abstract class AbstractTransaction {

    private TransactionID id;
    private TransactionState state;

    public AbstractTransaction(TransactionID id) {
        this.id = id;
        this.state = TransactionState.RECEIVED;
    }

    protected abstract void addOperation(Operation op);

    public TransactionID getId() {
        return id;
    }

    public abstract List<Operation> getRwSet();

    public TransactionState getState() {
        return state;
    }

    public void applyState(TransactionState state) {
        this.state = state;
    }
}
