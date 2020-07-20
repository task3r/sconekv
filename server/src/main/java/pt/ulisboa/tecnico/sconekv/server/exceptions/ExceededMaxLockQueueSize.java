package pt.ulisboa.tecnico.sconekv.server.exceptions;

import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;

public class ExceededMaxLockQueueSize extends Exception {

    private TransactionID txID;
    private String key;
    private int size;

    public ExceededMaxLockQueueSize(TransactionID txID, String key, int size) {
        super(String.format("Tx %s aborted as lock queue for key %s is %d (above %d)", txID, key, size, SconeConstants.MAX_TX_LOCK_QUEUE_SIZE));
        this.txID = txID;
        this.key = key;
        this.size = size;
    }

    public TransactionID getTxID() {
        return txID;
    }

    public String getKey() {
        return key;
    }

    public int getSize() {
        return size;
    }
}
