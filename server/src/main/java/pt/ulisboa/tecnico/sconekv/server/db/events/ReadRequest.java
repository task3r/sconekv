package pt.ulisboa.tecnico.sconekv.server.db.events;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

public class ReadRequest extends ClientRequest {

    private String key;

    public ReadRequest(Pair<Short, Integer> id, String client, TransactionID txID, String key) {
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
