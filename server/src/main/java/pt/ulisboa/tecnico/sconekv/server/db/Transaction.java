package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.common.db.AbstractTransaction;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class Transaction extends AbstractTransaction {

    private short[] buckets;

    protected Transaction(TransactionID txID, Message.Commit.Reader commit) {
        super(txID);

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
}
