package pt.ulisboa.tecnico.sconekv.server.events;

import org.javatuples.Pair;

public abstract class SconeEvent {

    private Pair<Short, Integer> id; // ids are used for debugging, could possibly be removed in the future

    public SconeEvent(Pair<Short,Integer> id) {
        this.id = id;
    }

    public abstract void handledBy(SconeEventHandler handler);

    public Pair<Short, Integer> getId() {
        return id;
    }

    public void setId(Pair<Short, Integer> id) {
        if (this.id == null) {
            this.id = id;
        }
    }
}
