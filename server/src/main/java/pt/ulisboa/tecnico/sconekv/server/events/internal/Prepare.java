package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.external.ClientRequest;

public class Prepare extends InternalMessage {

    private long opNumber;
    private long commitNumber;
    private short bucket;
    private ClientRequest clientRequest;

    public Prepare(Pair<Short, Integer> id, String node, Version viewNumber, long opNumber, long commitNumber, short bucket, ClientRequest clientRequest) {
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

    public ClientRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
