package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.internal.InternalEvent;

public abstract class DistributedTransactionEvent extends InternalEvent {
    private TransactionID txID;

    public DistributedTransactionEvent(Node node, Version viewVersion, TransactionID txID) {
        super(node, viewVersion);
        this.txID = txID;
    }

    public TransactionID getTxID() {
        return txID;
    }
}
