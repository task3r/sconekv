package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public class GetState extends InternalEvent {

    private int opNumber;

    public GetState(Pair<Short, Integer> id, Node node, Version viewVersion, int opNumber) {
        super(id, node, viewVersion);
        this.opNumber = opNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
