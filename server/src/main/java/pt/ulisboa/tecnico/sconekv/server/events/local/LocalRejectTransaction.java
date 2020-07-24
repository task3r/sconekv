package pt.ulisboa.tecnico.sconekv.server.events.local;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LocalRejectTransaction implements SconeEvent {

    private TransactionID txID;

    public LocalRejectTransaction(TransactionID txID) {
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
