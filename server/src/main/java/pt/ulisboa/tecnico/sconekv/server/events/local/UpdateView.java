package pt.ulisboa.tecnico.sconekv.server.events.local;

import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class UpdateView implements SconeEvent {

    private Ring ring;

    public UpdateView(Ring ring) {
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
