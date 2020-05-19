package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class InternalMessage extends SconeEvent {

    private Node node;
    private Version viewVersion;

    public InternalMessage(Pair<Short, Integer> id, Node node, Version viewVersion) {
        super(id);
        this.node = node;
        this.viewVersion = viewVersion;
    }

    public Node getNode() {
        return node;
    }

    public Version getViewVersion() {
        return viewVersion;
    }
}
