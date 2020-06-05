package pt.ulisboa.tecnico.sconekv.server.events;

import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.GetDHTRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.WriteRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;

public interface SconeEventHandler {

    // External Events
    void handle(ReadRequest readRequest);
    void handle(WriteRequest writeRequest);
    void handle(CommitRequest commitRequest);
    void handle(GetDHTRequest getViewRequest);

    // Internal Events
    // State Machine Replication
    void handle(Prepare prepare);
    void handle(PrepareOK prepareOK);
    void handle(StartViewChange startViewChange);
    void handle(DoViewChange doViewChange);
    void handle(StartView startView);
    void handle(GetState getState);
    void handle(NewState newState);

    // Distributed Transactions
    void handle(CommitLocalDecision commitLocalDecision);
    void handle(RequestRollbackLocalDecision requestRollbackLocalDecision);
    void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse);
    void handle(CommitTransaction commitTransaction);
    void handle(AbortTransaction abortTransaction);
}
