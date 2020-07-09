package pt.ulisboa.tecnico.sconekv.client.exceptions;

public class RequestFailedException extends Exception {
    public RequestFailedException(String message) {
        super(message);
    }

    public RequestFailedException(Throwable cause) {
        super(cause);
    }
}
