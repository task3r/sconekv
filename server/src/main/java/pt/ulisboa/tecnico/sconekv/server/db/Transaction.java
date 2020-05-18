package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.common.db.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Transaction extends AbstractTransaction {

    private short[] buckets;
    private List<Operation> rwSet;

    public Transaction(TransactionID txID, External.Commit.Reader commit) {
        super(txID);

        rwSet = new ArrayList<>();

        for (int i = 0; i < commit.getOps().size(); i++) {
            addOperation(Operation.unserialize(commit.getOps().get(i)));
        }

        buckets = new short[commit.getBuckets().size()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = commit.getBuckets().get(i);
        }
    }

    public short[] getBuckets() {
        return buckets;
    }

    @Override
    protected void addOperation(Operation op) {
        rwSet.add(op);
    }

    @Override
    public List<Operation> getRwSet() {
        return Collections.unmodifiableList(rwSet);
    }
}
