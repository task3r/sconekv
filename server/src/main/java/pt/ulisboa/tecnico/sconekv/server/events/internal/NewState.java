package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.util.List;

public class NewState extends InternalMessage {

    private List<LogEntry> logSegment;
    private int commitNumber;
    private int opNumber;

    public NewState(Pair<Short, Integer> id, Node node, Version viewVersion, List<LogEntry> logSegment, int opNumber, int commitNumber) {
        super(id, node, viewVersion);
        this.logSegment = logSegment;
        this.commitNumber = commitNumber;
        this.opNumber = opNumber;
    }

    public List<LogEntry> getLogSegment() {
        return logSegment;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
