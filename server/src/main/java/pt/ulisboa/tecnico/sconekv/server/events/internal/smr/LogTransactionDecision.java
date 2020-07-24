package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LogTransactionDecision extends LogEvent {

    private TransactionID txID;
    private TransactionState decision;

    public LogTransactionDecision(TransactionID txID, boolean toCommit, Internal.LogEvent.Reader reader) {
        this(txID, toCommit? TransactionState.COMMITTED : TransactionState.ABORTED, reader);
    }

    public LogTransactionDecision(TransactionID txID, TransactionState decision, Internal.LogEvent.Reader reader) {
        super(reader);
        this.txID = txID;
        this.decision = decision;
    }

    public TransactionState getDecision() {
        return decision;
    }

    @Override
    public TransactionID getTxID() {
        return txID;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
