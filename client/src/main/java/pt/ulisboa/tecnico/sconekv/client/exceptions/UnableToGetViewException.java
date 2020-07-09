package pt.ulisboa.tecnico.sconekv.client.exceptions;

public class UnableToGetViewException extends Exception {
    public UnableToGetViewException(Throwable cause) {
        super(cause);
    }

    public UnableToGetViewException(String message) {
        super(message);
    }
}
