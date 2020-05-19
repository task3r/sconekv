package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class WriteRequest extends ClientRequest {

    private String key;

    public WriteRequest(Pair<Short, Integer> id, String client, TransactionID txID, String key, External.Request.Reader request) {
        super(id, client, txID, request);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
