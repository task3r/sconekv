package pt.ulisboa.tecnico.sconekv.common.exceptions;

public class InvalidBucketException extends Exception {
    public InvalidBucketException(String message) {
        super(message);
    }

    public InvalidBucketException(Throwable cause) {
        super(cause);
    }
}
