package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class CommitRequest extends ClientRequest {

    private Transaction tx;

    public CommitRequest(Pair<Short, Integer> id, String client, Transaction tx) {
        super(id, client, tx.getId());
        this.tx = tx;
    }

    public Transaction getTx() {
        return tx;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
