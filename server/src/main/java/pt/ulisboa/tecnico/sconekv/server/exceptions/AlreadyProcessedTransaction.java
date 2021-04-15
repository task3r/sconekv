package pt.ulisboa.tecnico.sconekv.server.exceptions;

public class AlreadyProcessedTransaction extends Exception {

    private final boolean decided;

    public AlreadyProcessedTransaction(boolean decided) {
        this.decided = decided;
    }

    public boolean isDecided() {
        return decided;
    }
}
