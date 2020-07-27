package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;

import java.util.*;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private short[] buckets;
    private List<Operation> rwSet;
    // which buckets responded
    private Set<Short> responses;
    private String client;
    private Common.Transaction.Reader reader;
    private boolean decided;
    private boolean rollbackInProgress;

    public Transaction(TransactionID txID, String client, Common.Transaction.Reader transaction) {
        super(txID, TransactionState.RECEIVED);

        this.client = client;
        this.rwSet = new ArrayList<>();
        this.responses = new HashSet<>();
        this.reader = transaction;

        for (int i = 0; i < transaction.getOps().size(); i++) {
            addOperation(Operation.unserialize(transaction.getOps().get(i)));
        }

        this.buckets = new short[transaction.getBuckets().size()];
        for (int i = 0; i < buckets.length; i++) {
            this.buckets[i] = transaction.getBuckets().get(i);
        }
    }

    public String getClient() {
        return client;
    }

    public short[] getBuckets() {
        return buckets;
    }

    public short getCoordinatorBucket() {
        return this.buckets[0];
    }

    public Common.Transaction.Reader getReader() {
        return reader;
    }

    public boolean isReady() {
        return responses.size() == buckets.length;
    }

    public boolean addResponse(short bucket) {
        synchronized (this) {
            if (!this.responses.add(bucket))
                logger.error("Received a second response from bucket {} for transaction {}", bucket, getId());
            return isReady();
        }
    }

    public boolean isDecided() {
        return decided;
    }

    public void setDecided() {
        this.decided = true;
    }

    @Override
    protected void addOperation(Operation op) {
        this.rwSet.add(op);
    }

    @Override
    public List<Operation> getRwSet() {
        return Collections.unmodifiableList(this.rwSet);
    }

    public void setState(TransactionState state) {
        applyState(state);
    }

    public void removeResponse(short bucket) {
        responses.remove(bucket);
    }

    public boolean rollback() {
        synchronized (getId()) {
            if (this.rollbackInProgress) {
                return false;
            } else {
                this.rollbackInProgress = true;
                return true;
            }

        }
    }

    public void completedRollback() {
        synchronized (getId()) {
            this.rollbackInProgress = false;
        }
    }
}
