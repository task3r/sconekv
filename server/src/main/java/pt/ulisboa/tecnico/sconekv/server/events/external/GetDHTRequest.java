package pt.ulisboa.tecnico.sconekv.server.events.external;

import org.javatuples.Pair;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class GetDHTRequest extends ClientRequest {

    public GetDHTRequest(Pair<Short, Integer> id, String client) {
        super(id, client, null); // getView does not have txID but it is easier if it is still a ClientRequest
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
