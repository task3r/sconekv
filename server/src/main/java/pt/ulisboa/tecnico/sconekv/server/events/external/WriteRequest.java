package pt.ulisboa.tecnico.sconekv.server.events;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

public class WriteRequest extends ClientRequest {

    private String key;

    public WriteRequest(Pair<Short, Integer> id, String client, TransactionID txID, String key) {
        super(id, client, txID);
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
