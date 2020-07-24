package pt.ulisboa.tecnico.sconekv.server.events.internal;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class InternalEvent implements SconeEvent {

    private Node node;
    private Version viewVersion;

    public InternalEvent(Node node, Version viewVersion) {
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
