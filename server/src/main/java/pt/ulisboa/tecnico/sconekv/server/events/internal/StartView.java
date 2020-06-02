package pt.ulisboa.tecnico.sconekv.server.events.internal;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.util.List;


public class StartView extends InternalMessage {

    private List<LogEntry> log;
    private int commitNumber;

    public StartView(Pair<Short, Integer> id, Node node, Version viewVersion, List<LogEntry> log, int commitNumber) {
        super(id, node, viewVersion);
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
