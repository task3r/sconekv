package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LogRollback extends LogEvent {

    private TransactionID txID;

    public LogRollback(Pair<Short, Integer> id, TransactionID txID, Internal.LogEvent.Reader reader) {
        super(id, reader);
        this.txID = txID;
    }

    @Override
    public TransactionID getTxID() {
        return txID;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
