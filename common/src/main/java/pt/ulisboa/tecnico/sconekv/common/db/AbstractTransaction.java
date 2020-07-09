package pt.ulisboa.tecnico.sconekv.common.db;

import java.util.List;

/**
 * SconeKV abstraction of transaction
 *     - has implementations for both client and server side
 *     - has an ID, state and should implement a collection of Operations
 */
public abstract class AbstractTransaction {

    private TransactionID id;
    private TransactionState state;

    public AbstractTransaction(TransactionID id, TransactionState state) {
        this.id = id;
        this.state = state;
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
