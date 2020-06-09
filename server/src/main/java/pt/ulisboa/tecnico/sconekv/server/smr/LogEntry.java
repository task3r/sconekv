package pt.ulisboa.tecnico.sconekv.server.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.LogEvent;

import java.util.HashSet;
import java.util.Set;

public class LogEntry {
    private LogEvent event;
    private Set<Node> oksReceived;

    public LogEntry(LogEvent event) {
        this.event = event;
        this.oksReceived = new HashSet<>();
    }

    public LogEvent getEvent() {
        return event;
    }

    public void addOk(Node node) {
        oksReceived.add(node);
    }

    public boolean isReady() {
        return oksReceived.size() >= SconeConstants.FAILURES_PER_BUCKET;
    }
}
