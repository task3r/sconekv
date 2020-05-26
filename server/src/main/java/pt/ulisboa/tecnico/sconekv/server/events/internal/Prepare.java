package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;

public class Prepare extends InternalMessage {

    private long opNumber;
    private long commitNumber;
    private short bucket;
    // at this moment only commit requests are replicated
    private CommitRequest clientRequest;

    public Prepare(Pair<Short, Integer> id, Node node, Version viewNumber, long opNumber, long commitNumber, short bucket, CommitRequest clientRequest) {
        super(id, node, viewNumber);
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.bucket = bucket;
        this.clientRequest = clientRequest;
    }

    public long getOpNumber() {
        return opNumber;
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    public short getBucket() {
        return bucket;
    }

    public CommitRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
