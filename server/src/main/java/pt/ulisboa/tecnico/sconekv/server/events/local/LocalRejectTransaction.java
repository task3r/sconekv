package pt.ulisboa.tecnico.sconekv.server.events.local;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LocalRejectTransaction extends SconeEvent {

    private TransactionID txID;

    public LocalRejectTransaction(Pair<Short, Integer> id, TransactionID txID) {
        super(id);
        this.txID = txID;
    }

    public TransactionID getTxID() {
        return txID;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
