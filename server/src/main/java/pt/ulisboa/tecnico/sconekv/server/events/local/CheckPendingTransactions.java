package pt.ulisboa.tecnico.sconekv.server.events.local;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class CheckPendingTransactions extends SconeEvent {
    public CheckPendingTransactions(Pair<Short, Integer> id) {
        super(id);
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
