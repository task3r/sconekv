package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ReadWriteLock implements Lock {
    private TreeSet<TransactionID> lockOwners = new TreeSet<>();
    private Operation.Type currenMode;
    private TreeSet<TransactionID> lockQueue = new TreeSet<>();
    private Set<TransactionID> readQueue = new HashSet<>();

    @Override
    public Set<TransactionID> lock(TransactionID txID, Operation.Type type) {
        if (this.lockOwners.isEmpty()) {
            this.lockOwners.add(txID);
            this.currenMode = type;
        } else if (type == Operation.Type.READ && this.currenMode == Operation.Type.READ) {
            this.lockOwners.add(txID);
        }
        if (this.lockOwners.contains(txID)) {
            removeFromQueue(txID);
        }
        return lockOwners;
    }

    @Override
    public void unlock(TransactionID txID) {
        if (lockOwners.contains(txID)) {
            lockOwners.remove(txID);
            if (lockOwners.isEmpty())
                this.currenMode = null;
        }
    }

    @Override
    public void queue(TransactionID txID, Operation.Type type) {
        lockQueue.add(txID);
        if (type == Operation.Type.READ)
            readQueue.add(txID);
    }

    @Override
    public void removeFromQueue(TransactionID txID) {
        lockQueue.remove(txID);
        readQueue.remove(txID);
    }

    @Override
    public Set<TransactionID> unlockAndLockNext(TransactionID txID) {
        if (lockOwners.contains(txID) || lockOwners.isEmpty()) {
            lockOwners.remove(txID);
            TransactionID next = lockQueue.pollFirst();
            if (next != null) {
                if (lockOwners.isEmpty()) {
                    if (readQueue.contains(next)) {
                        lockOwners.addAll(readQueue);
                    } else {
                        lockOwners.add(next);
                    }
                    return lockOwners;
                } else if (lockOwners.lower(next) == null) {
                    // if next is lower than any tx currently owning the lock
                    // we trigger MakeLocalDecision on next to rollback current owners
                    HashSet<TransactionID> nextHolder = new HashSet<>();
                    nextHolder.add(next);
                    return nextHolder;
                } else {
                    lockQueue.add(next); // next in the queue is not ready to be processed, goes back in the queue
                }
            }
        }
        return new HashSet<>();
    }

    @Override
    public Set<TransactionID> unlockButQueue(TransactionID txID, Operation.Type type) {
        if (lockOwners.contains(txID) || lockOwners.isEmpty()) {
            Set<TransactionID> txs = unlockAndLockNext(txID);
            queue(txID, type);
            return txs;
        }
        return new HashSet<>();
    }

    @Override
    public Set<TransactionID> getLockQueue() {
        return lockQueue;
    }

    @Override
    public Set<TransactionID> getLockOwners() {
        return lockOwners;
    }

    @Override
    public Set<TransactionID> clearLockQueue() {
        Set<TransactionID> oldQueue = lockQueue;
        lockQueue = new TreeSet<>();
        readQueue = new HashSet<>();
        return oldQueue;
    }
}
