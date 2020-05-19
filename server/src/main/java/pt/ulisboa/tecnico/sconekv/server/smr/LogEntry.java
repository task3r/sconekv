package pt.ulisboa.tecnico.sconekv.server.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;

import java.util.HashSet;
import java.util.Set;

public class LogEntry {
    private CommitRequest request;
    private long opNumber;
    private Set<Node> oksReceived;

    public LogEntry(CommitRequest request, long opNumber) {
        this.request = request;
        this.opNumber = opNumber;
        this.oksReceived = new HashSet<>();
    }

    public CommitRequest getRequest() {
        return request;
    }

    public long getOpNumber() {
        return opNumber;
    }

    public void addOk(Node node) {
        oksReceived.add(node);
    }

    public boolean isReady() {
        return oksReceived.size() >= SconeConstants.REPLICATION;
    }


}
