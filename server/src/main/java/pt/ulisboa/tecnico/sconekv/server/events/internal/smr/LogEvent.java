package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class LogEvent extends SconeEvent {

    private Internal.LogEvent.Reader reader;

    public LogEvent(Pair<Short, Integer> id, Internal.LogEvent.Reader reader) {
        super(id);
        this.reader = reader;
    }

    public abstract TransactionID getTxID();

    public Internal.LogEvent.Reader getReader() {
        return reader;
    }

    public void setReader(Internal.LogEvent.Reader reader) {
        this.reader = reader;
    }
}
