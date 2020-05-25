package pt.ulisboa.tecnico.sconekv.server.events;

import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.GetDHTRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.WriteRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;

public interface SconeEventHandler {

    // External Events
    void handle(ReadRequest readRequest);
    void handle(WriteRequest writeRequest);
    void handle(CommitRequest commitRequest);
    void handle(GetDHTRequest getViewRequest);

    // Internal Events
    void handle(Prepare prepare);
    void handle(PrepareOK prepareOK);
}
