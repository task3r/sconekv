package pt.ulisboa.tecnico.sconekv.server.events.external;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

public abstract class ClientRequest implements SconeEvent {

    private String client;
    private TransactionID txID;

    public ClientRequest(String client, TransactionID txID) {
        this.client = client;
        this.txID = txID;
    }

    public String getClient() {
        return client;
    }

    public TransactionID getTxID() {
        return txID;
    }

    public abstract boolean checkBucket(DHT dht, Node self);
}
