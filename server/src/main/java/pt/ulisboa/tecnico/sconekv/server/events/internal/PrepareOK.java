package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class PrepareOK extends InternalMessage {

    private long opNumber;

    public PrepareOK(Pair<Short, Integer> id, String node, Version viewNumber, long opNumber) {
        super(id, node, viewNumber);
        this.opNumber = opNumber;
    }

    public long getOpNumber() {
        return opNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
