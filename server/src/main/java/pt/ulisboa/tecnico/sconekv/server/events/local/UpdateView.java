package pt.ulisboa.tecnico.sconekv.server.events.local;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class UpdateView extends SconeEvent {

    private Ring ring;

    public UpdateView(Pair<Short, Integer> id, Ring ring) {
        super(id);
        this.ring = ring;
    }

    public Ring getRing() {
        return ring;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
