package pt.ulisboa.tecnico.sconekv.server.events.external;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class DeleteRequest extends ClientRequest {

    private String key;

    public DeleteRequest(String client, TransactionID txID, String key) {
        super(client, txID);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }

    @Override
    public boolean checkBucket(DHT dht, Node self) {
        return dht.getBucketForKey(key.getBytes()) == dht.getBucketOfNode(self).getId();
    }
}
