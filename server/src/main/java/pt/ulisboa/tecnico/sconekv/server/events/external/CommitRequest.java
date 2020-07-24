package pt.ulisboa.tecnico.sconekv.server.events.external;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class CommitRequest extends ClientRequest {

    private Transaction tx;
    private External.Request.Reader request;

    public CommitRequest(String client, Transaction tx, External.Request.Reader request) {
        super(client, tx.getId());
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

    @Override
    public boolean checkBucket(DHT dht, Node self) {
        for (Operation op : tx.getRwSet()) {
            if (!dht.getMasterForKey(op.getKey().getBytes()).equals(self)) {
                return false;
            }
        }
        return true;
    }

}
