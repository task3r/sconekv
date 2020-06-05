package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.util.List;

public class DoViewChange extends InternalEvent {

    private List<LogEntry> log;
    private Version term;
    private int commitNumber;

    public DoViewChange(Pair<Short, Integer> id, Node node, Version newVersion, List<LogEntry> log, Version term,
                        int commitNumber) {
        super(id, node, newVersion);
        this.log = log;
        this.term = term;
        this.commitNumber = commitNumber;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public Version getTerm() {
        return term;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
