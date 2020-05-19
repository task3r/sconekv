package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class ClientRequest extends SconeEvent {

    private String client;
    private TransactionID txID;
    private External.Request.Reader request;
    private boolean prepared;

    public ClientRequest(Pair<Short, Integer> id, String client, TransactionID txID, External.Request.Reader request) {
        super(id);
        this.client = client;
        this.txID = txID;
        this.request = request;
        this.prepared = false;
    }

    public String getClient() {
        return client;
    }

    public TransactionID getTxID() {
        return txID;
    }

    public External.Request.Reader getRequest() {
        return request;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared() {
        this.prepared = true;
    }
}
