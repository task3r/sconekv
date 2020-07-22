package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class SingleLock implements Lock {
    private TransactionID lockOwner;
    private TreeSet<TransactionID> lockQueue = new TreeSet<>();

    @Override
    public Set<TransactionID> lock(TransactionID txID, Operation.Type type) {
        if (this.lockOwner == null) {
            this.lockOwner = txID;
        }
        if (this.lockOwner.equals(txID)) {
            removeFromQueue(txID);
        }
        return getLockOwners();
    }

    @Override
    public void unlock(TransactionID txID) {
        if (txID.equals(lockOwner)) {
            lockOwner = null;
        }
    }

    @Override
    public void queue(TransactionID txID, Operation.Type type) {
        lockQueue.add(txID);
    }

    @Override
    public void removeFromQueue(TransactionID txID) {
        lockQueue.remove(txID);
    }

    @Override
    public Set<TransactionID> unlockAndLockNext(TransactionID txID) {
        if (txID.equals(lockOwner) || lockOwner == null) {
            lockOwner = lockQueue.pollFirst();
            return getLockOwners();
        }
        return new HashSet<>();
    }

    @Override
    public Set<TransactionID> unlockButQueue(TransactionID txID, Operation.Type type) {
        if (txID.equals(lockOwner) || lockOwner == null) {
            lockOwner = lockQueue.pollFirst();
            queue(txID, type);
            return getLockOwners();
        }
        return new HashSet<>();
    }

    @Override
    public Set<TransactionID> getLockQueue() {
        return lockQueue;
    }

    @Override
    public Set<TransactionID> getLockOwners() {
        HashSet<TransactionID> owners = new HashSet<>();
        if (lockOwner != null)
            owners.add(lockOwner);
        return owners;
    }

    @Override
    public Set<TransactionID> clearLockQueue() {
        Set<TransactionID> oldQueue = lockQueue;
        lockQueue = new TreeSet<>();
        return oldQueue;
    }
}
