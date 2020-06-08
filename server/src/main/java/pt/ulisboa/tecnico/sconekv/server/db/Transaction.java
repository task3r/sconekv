package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;

import java.util.*;

public class Transaction extends AbstractTransaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private short[] buckets;
    private List<Operation> rwSet;
    // which buckets responded
    private Set<Short> responses;
    private String client;

    public Transaction(TransactionID txID, String client, External.Commit.Reader commit) {
        super(txID);

        this.client = client;
        this.rwSet = new ArrayList<>();
        this.responses = new HashSet<>();

        for (int i = 0; i < commit.getOps().size(); i++) {
            addOperation(Operation.unserialize(commit.getOps().get(i)));
        }

        this.buckets = new short[commit.getBuckets().size()];
        for (int i = 0; i < buckets.length; i++) {
            this.buckets[i] = commit.getBuckets().get(i);
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

    public boolean isReady() {
        return responses.size() == buckets.length;
    }

    public void addResponse(short bucket) {
        if (!this.responses.add(bucket))
            logger.error("Received a second response from bucket {} for transaction {}", bucket, getId());
    }

    @Override
    protected void addOperation(Operation op) {
        this.rwSet.add(op);
    }

    @Override
    public List<Operation> getRwSet() {
        return Collections.unmodifiableList(this.rwSet);
    }
}
