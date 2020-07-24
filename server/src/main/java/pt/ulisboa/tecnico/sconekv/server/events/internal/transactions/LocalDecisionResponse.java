package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LocalDecisionResponse extends DistributedTransactionEvent {
    private TransactionState localDecision;

    public LocalDecisionResponse(Node node, Version viewVersion, TransactionID txID, boolean toCommit) {
        super(node, viewVersion, txID);
        this.localDecision = toCommit? TransactionState.COMMITTED : TransactionState.ABORTED;
    }

    public TransactionState getLocalDecision() {
        return localDecision;
    }

    public boolean shouldAbort() {
        return localDecision == TransactionState.ABORTED;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
