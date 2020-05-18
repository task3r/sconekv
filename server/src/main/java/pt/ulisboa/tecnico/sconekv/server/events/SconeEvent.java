package pt.ulisboa.tecnico.sconekv.server.events;

import org.javatuples.Pair;

public abstract class SconeEvent {

    private Pair<Short, Integer> id;

    public SconeEvent(Pair<Short,Integer> id) {
        this.id = id;
    }

    public abstract void handledBy(SconeEventHandler handler);

    public Pair<Short, Integer> getId() {
        return id;
    }
}
