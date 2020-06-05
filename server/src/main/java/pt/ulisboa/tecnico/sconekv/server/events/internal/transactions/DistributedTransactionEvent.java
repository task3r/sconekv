package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public abstract class DistributedTransactionEvent extends InternalEvent {
    private TransactionID txID;

    public DistributedTransactionEvent(Pair<Short, Integer> id, Node node, Version viewVersion, TransactionID txID) {
        super(id, node, viewVersion);
        this.txID = txID;
    }

    public TransactionID getTxID() {
        return txID;
    }
}
