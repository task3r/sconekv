package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class AbortTransaction extends DistributedTransactionEvent {

    public AbortTransaction(Pair<Short, Integer> id, Node node, Version viewVersion, TransactionID txID) {
        super(id, node, viewVersion, txID);
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
