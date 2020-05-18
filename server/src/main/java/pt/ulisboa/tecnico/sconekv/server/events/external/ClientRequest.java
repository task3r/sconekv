package pt.ulisboa.tecnico.sconekv.server.events;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

public abstract class ClientRequest extends SconeEvent {

    private String client;
    private TransactionID txID;

    public ClientRequest(Pair<Short, Integer> id, String client, TransactionID txID) {
        super(id);
        this.client = client;
        this.txID = txID;
    }

    public String getClient() {
        return client;
    }

    public TransactionID getTxID() {
        return txID;
    }
}
