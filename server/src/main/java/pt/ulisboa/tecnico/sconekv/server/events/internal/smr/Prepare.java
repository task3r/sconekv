package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public class Prepare extends InternalEvent {

    private int opNumber;
    private int commitNumber;
    private short bucket;
    private LogEvent event;

    public Prepare(Node node, Version viewNumber, int opNumber, int commitNumber, short bucket, LogEvent event) {
        super(node, viewNumber);
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.bucket = bucket;
        this.event = event;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public short getBucket() {
        return bucket;
    }

    public LogEvent getEvent() {
        return event;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
