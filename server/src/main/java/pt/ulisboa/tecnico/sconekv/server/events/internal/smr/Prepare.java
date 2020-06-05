package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public class Prepare extends InternalEvent {

    private int opNumber;
    private int commitNumber;
    private short bucket;
    // at this moment only commit requests are replicated
    private CommitRequest clientRequest;

    public Prepare(Pair<Short, Integer> id, Node node, Version viewNumber, int opNumber, int commitNumber, short bucket, CommitRequest clientRequest) {
        super(id, node, viewNumber);
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
        this.bucket = bucket;
        this.clientRequest = clientRequest;
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

    public CommitRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
