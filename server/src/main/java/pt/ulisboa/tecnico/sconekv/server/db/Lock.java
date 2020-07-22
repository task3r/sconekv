package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;

import java.util.Set;

public interface Lock {

    Set<TransactionID> lock(TransactionID txID, Operation.Type type);
    void unlock(TransactionID txID);
    void queue(TransactionID txID, Operation.Type type);
    void removeFromQueue(TransactionID txID);
    Set<TransactionID> unlockAndLockNext(TransactionID txID);
    Set<TransactionID> unlockButQueue(TransactionID txID, Operation.Type type);
    Set<TransactionID> getLockQueue();
    Set<TransactionID> getLockOwners();
    Set<TransactionID> clearLockQueue();


}
