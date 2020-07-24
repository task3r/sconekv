package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.util.List;


public class StartView extends InternalEvent {

    private List<LogEntry> log;
    private int commitNumber;

    public StartView(Node node, Version viewVersion, List<LogEntry> log, int commitNumber) {
        super(node, viewVersion);
        this.log = log;
        this.commitNumber = commitNumber;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
