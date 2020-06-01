package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class StartViewChange extends InternalMessage {

    public StartViewChange(Pair<Short, Integer> id, Node node, Version newVersion) {
        super(id, node, newVersion);
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
