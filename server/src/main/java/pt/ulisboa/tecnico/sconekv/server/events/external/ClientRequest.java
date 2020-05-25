package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class ClientRequest extends SconeEvent {

    private String client;
    private TransactionID txID;
    private boolean prepared;

    public ClientRequest(Pair<Short, Integer> id, String client, TransactionID txID) {
        super(id);
        this.client = client;
        this.txID = txID;
        this.prepared = false;
    }

    public String getClient() {
        return client;
    }

    public TransactionID getTxID() {
        return txID;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared() {
        this.prepared = true;
    }
}
