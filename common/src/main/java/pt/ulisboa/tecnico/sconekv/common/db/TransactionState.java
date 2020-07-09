package pt.ulisboa.tecnico.sconekv.common.db;

public enum TransactionState {
        COMMITTED,
        ABORTED,
        PREPARED, // server-side, meaning it was accepted locally
        RECEIVED, // server-side, meaning it was received but no local decision was made
        NONE // client-side, meaning it is yet to be committed or aborted
}
