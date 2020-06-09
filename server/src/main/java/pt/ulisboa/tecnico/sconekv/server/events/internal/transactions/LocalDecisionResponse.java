package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.db.CommitDecision;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LocalDecisionResponse extends DistributedTransactionEvent {
    private CommitDecision localDecision;

    public LocalDecisionResponse(Pair<Short, Integer> id, Node node, Version viewVersion, TransactionID txID, boolean toCommit) {
        super(id, node, viewVersion, txID);
        this.localDecision = toCommit? CommitDecision.COMMIT : CommitDecision.ABORT;
    }

    public CommitDecision getLocalDecision() {
        return localDecision;
    }

    public boolean shouldAbort() {
        return localDecision == CommitDecision.ABORT;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
