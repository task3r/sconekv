package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class CommitRequest extends ClientRequest {

    private Transaction tx;
    private External.Request.Reader request;

    public CommitRequest(Pair<Short, Integer> id, String client, Transaction tx, External.Request.Reader request) {
        super(id, client, tx.getId());
        this.tx = tx;
        this.request = request;
    }

    public Transaction getTx() {
        return tx;
    }

    public External.Request.Reader getRequest() {
        return request;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
