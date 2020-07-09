package pt.ulisboa.tecnico.sconekv.common.exceptions;

public class InvalidTransactionStateChangeException extends Exception {
    public InvalidTransactionStateChangeException(String message) {
        super(message);
    }

    public InvalidTransactionStateChangeException(Throwable cause) {
        super(cause);
    }
}
