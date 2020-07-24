package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public class StartViewChange extends InternalEvent {

    public StartViewChange(Node node, Version newVersion) {
        super(node, newVersion);
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
