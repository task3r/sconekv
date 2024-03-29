package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public class PrepareOK extends InternalEvent {

    private long opNumber;
    private short bucket;

    public PrepareOK(Node node, Version viewNumber, long opNumber, short bucket) {
        super(node, viewNumber);
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
