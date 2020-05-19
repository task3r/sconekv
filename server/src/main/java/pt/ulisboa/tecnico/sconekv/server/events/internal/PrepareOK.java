package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class PrepareOK extends InternalMessage {

    private long opNumber;
    private short bucket;

    public PrepareOK(Pair<Short, Integer> id, String node, Version viewNumber, long opNumber, short bucket) {
        super(id, node, viewNumber);
        this.opNumber = opNumber;
        this.bucket = bucket;
    }

    public long getOpNumber() {
        return opNumber;
    }

    public short getBucket() {
        return bucket;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
