package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class GetDHTRequest extends ClientRequest {

    public GetDHTRequest(String client) {
        super(client, null); // getView does not have txID but it is easier if it is still a ClientRequest
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }

    @Override
    public boolean checkBucket(DHT dht, Node self) {
        return true;
    }
}
