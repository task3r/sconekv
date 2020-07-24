package pt.ulisboa.tecnico.sconekv.server.events.internal.smr;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LogTransaction extends LogEvent {

    private Transaction tx;

    public LogTransaction(Transaction tx, Internal.LogEvent.Reader reader) {
        super(reader);
        this.tx = tx;
    }

    public Transaction getTx() {
        return tx;
    }

    @Override
    public TransactionID getTxID() {
        return tx.getId();
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
