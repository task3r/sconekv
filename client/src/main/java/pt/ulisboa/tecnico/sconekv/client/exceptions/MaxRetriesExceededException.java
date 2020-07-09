package pt.ulisboa.tecnico.sconekv.client.exceptions;

public class MaxRetriesExceededException extends RequestFailedException {
    public MaxRetriesExceededException(String message) {
        super(message);
    }
}
