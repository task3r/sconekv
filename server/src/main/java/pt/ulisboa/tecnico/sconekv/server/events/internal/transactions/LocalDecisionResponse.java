package pt.ulisboa.tecnico.sconekv.server.events.internal.transactions;

import org.javatuples.Pair;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEventHandler;

public class LocalDecisionResponse extends DistributedTransactionEvent {
    enum Decision {
        COMMIT,
        ABORT
    }

    private Decision localDecision;

    public LocalDecisionResponse(Pair<Short, Integer> id, Node node, Version viewVersion, TransactionID txID, boolean toCommit) {
        super(id, node, viewVersion, txID);
        this.localDecision = toCommit? Decision.COMMIT : Decision.ABORT;
    }

    public Decision getLocalDecision() {
        return localDecision;
    }

    public boolean shouldAbort() {
        return localDecision == Decision.ABORT;
    }

    @Override
    public void handledBy(SconeEventHandler handler) {
        handler.handle(this);
    }
}
