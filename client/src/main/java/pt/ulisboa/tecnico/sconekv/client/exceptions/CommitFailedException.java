package pt.ulisboa.tecnico.sconekv.client.exceptions;

public class CommitFailedException extends Exception {
    public CommitFailedException(String message) {
        super(message);
    }

    public CommitFailedException(Throwable cause) {
        super(cause);
    }
}
