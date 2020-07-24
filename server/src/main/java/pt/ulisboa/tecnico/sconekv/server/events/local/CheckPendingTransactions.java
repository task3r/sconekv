package pt.ulisboa.tecnico.sconekv.server.events.local;

import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class CheckPendingTransactions implements SconeEvent {

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
