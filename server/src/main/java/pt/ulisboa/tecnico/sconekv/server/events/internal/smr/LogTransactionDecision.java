package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.server.db.CommitDecision;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LogTransactionDecision extends LogEvent {

    private TransactionID txID;
    private CommitDecision decision;

    public LogTransactionDecision(Pair<Short, Integer> id, TransactionID txID, boolean toCommit, Internal.LogEvent.Reader reader) {
        this(id, txID, toCommit? CommitDecision.COMMIT : CommitDecision.ABORT, reader);
    }

    public LogTransactionDecision(Pair<Short, Integer> id, TransactionID txID, CommitDecision decision, Internal.LogEvent.Reader reader) {
        super(id, reader);
        this.txID = txID;
        this.decision = decision;
    }

    public CommitDecision getDecision() {
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
