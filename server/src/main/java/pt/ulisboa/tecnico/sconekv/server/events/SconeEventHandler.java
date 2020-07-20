package pt.ulisboa.tecnico.sconekv.server.events;

import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.local.CheckPendingTransactions;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.events.local.LocalRejectTransaction;

public interface SconeEventHandler {

    // External Events
    void handle(ReadRequest readRequest);
    void handle(WriteRequest writeRequest);
    void handle(DeleteRequest deleteRequest);
    void handle(CommitRequest commitRequest);
    void handle(GetDHTRequest getViewRequest);

    // Internal Events
    // State Machine Replication
    void handle(LogTransaction logTransaction);
    void handle(LogTransactionDecision logTransactionDecision);
    void handle(LogRollback logRollback);
    void handle(Prepare prepare);
    void handle(PrepareOK prepareOK);
    void handle(StartViewChange startViewChange);
    void handle(DoViewChange doViewChange);
    void handle(StartView startView);
    void handle(GetState getState);
    void handle(NewState newState);

    // Distributed Transactions
    void handle(LocalDecisionResponse localDecisionResponse);
    void handle(RequestRollbackLocalDecision requestRollbackLocalDecision);
    void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse);
    void handle(CommitTransaction commitTransaction);
    void handle(AbortTransaction abortTransaction);
    void handle(MakeLocalDecision makeLocalDecision);
    void handle(RequestGlobalDecision requestGlobalDecision);
    void handle(RequestLocalDecision requestLocalDecision);

    // Local Events
    void handle(CheckPendingTransactions checkPendingTransactions);
    void handle(LocalRejectTransaction localRejectTransaction);
}
