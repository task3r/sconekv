package pt.ulisboa.tecnico.sconekv.server.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;

import java.util.HashSet;
import java.util.Set;

public class LogEntry {
    private CommitRequest request;
    private Set<Node> oksReceived;

    public LogEntry(CommitRequest request) {
        this.request = request;
        this.oksReceived = new HashSet<>();
    }

    public CommitRequest getRequest() {
        return request;
    }

    public void addOk(Node node) {
        oksReceived.add(node);
    }

    public boolean isReady() {
        return oksReceived.size() >= SconeConstants.FAILURES_PER_BUCKET;
    }
}
