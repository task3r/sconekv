package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class InternalMessage extends SconeEvent {

    private String node;
    private Version viewVersion;

    public InternalMessage(Pair<Short, Integer> id, String node, Version viewVersion) {
        super(id);
        this.node = node;
        this.viewVersion = viewVersion;
    }

    public String getNode() {
        return node;
    }

    public Version getViewVersion() {
        return viewVersion;
    }
}
