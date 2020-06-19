package pt.ulisboa.tecnico.sconekv.server.exceptions;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

import java.util.Set;

public class ValidTransactionNotLockableException extends Exception {
    private Set<TransactionID> currentOwners;
    public ValidTransactionNotLockableException(Set<TransactionID> currentOwners) {
        this.currentOwners = currentOwners;
    }

    public ValidTransactionNotLockableException() {
    }

    public boolean possibleRollback() {
        return currentOwners != null;
    }

    public Set<TransactionID> getCurrentOwners() {
        return currentOwners;
    }
}
