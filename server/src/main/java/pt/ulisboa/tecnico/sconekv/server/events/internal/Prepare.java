package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class Prepare extends InternalMessage {

    private long opNumber;
    private long commitNumber;

    public Prepare(Pair<Short, Integer> id, String node, Version viewNumber, long opNumber, long commitNumber) {
        super(id, node, viewNumber);
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }

    public long getOpNumber() {
        return opNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
