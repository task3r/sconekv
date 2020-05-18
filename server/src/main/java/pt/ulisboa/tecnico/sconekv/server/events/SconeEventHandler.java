package pt.ulisboa.tecnico.sconekv.server.events;

public interface SconeEventHandler {
    void handle(ReadRequest readRequest);
    void handle(WriteRequest writeRequest);
    void handle(CommitRequest commitRequest);
}
