package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LogRollback implements LogEvent {

    private TransactionID txID;

    public LogRollback(TransactionID txID) {
        this.txID = txID;
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
