package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class RequestMissingGlobalDecision implements SconeEvent {

    private TransactionID txID;

    public RequestMissingGlobalDecision(TransactionID txID) {
        this.txID = txID;
    }

    public TransactionID getTxID() {
        return txID;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
