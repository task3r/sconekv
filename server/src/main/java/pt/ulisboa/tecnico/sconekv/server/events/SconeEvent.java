package pt.ulisboa.tecnico.sconekv.server.events;

public interface SconeEvent {

    void handledBy(SconeEventHandler handler);

}
