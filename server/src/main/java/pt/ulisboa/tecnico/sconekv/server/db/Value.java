package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ExceededMaxLockQueueSize;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidVersionException;

import java.util.*;

public class Value {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private String key;
    private byte[] content;
    private short version;
    private TransactionID lockOwner;
    private TreeSet<TransactionID> lockQueue = new TreeSet<>();

    public Value(String key) {
        this.key = key;
        this.content = new byte[0];
        this.version = 0;
    }
    public Value(String key, byte[] content, short version) {
        this.key = key;
        this.content = content;
        this.version = version;
    }

    public byte[] getContent() {
        return content;
    }

    public short getVersion() {
        return version;
    }

    public synchronized TransactionID getLockOwner() {
        return lockOwner;
    }

    public SortedSet<TransactionID> getLockQueue() {
        return lockQueue;
    }

    public synchronized Set<TransactionID> update(byte[] content, short version) {
        if (version > this.version) {
            this.content = content;
            Set<TransactionID> outdatedTxs = lockQueue;
            lockQueue = new TreeSet<>();
            this.version = version;
            return outdatedTxs;
        } else {
            logger.error("Tried applying previous version {} with content {} for object with version {}", version, content, this.version);
            return new HashSet<>();
        }
    }

    public synchronized TransactionID validateAndLock(TransactionID txID, Operation op) throws InvalidVersionException {
        logger.debug("Validate&Lock {} v{} == v{}", op.getKey(), version, op.getVersion());
        if (version == op.getVersion()) {
            if (this.lockOwner == null) {
                logger.debug("Locked {} for {}", key, txID);
                this.lockOwner = txID;
            }
            if (this.lockOwner.equals(txID)) {
                removeFromQueue(txID);
            }
            return this.lockOwner;
        } else {
            throw new InvalidVersionException();
        }
    }

    public synchronized TransactionID validate(Operation op) throws InvalidVersionException {
        logger.debug("Validate {} v{} == v{}", op.getKey(), version, op.getVersion());
        if (version != op.getVersion()) {
            throw new InvalidVersionException();
        }
        return this.lockOwner;
    }

    public synchronized TransactionID releaseLockAndChangeToNext(TransactionID txID) {
        if (txID.equals(lockOwner) || lockOwner == null) {
            lockOwner = lockQueue.pollFirst();
            if (logger.isDebugEnabled()) {
                StringBuilder s = new StringBuilder();
                for (TransactionID id : lockQueue)
                    s.append(id).append(",");
                logger.debug("Released lock {} of tx {}, selected {} and left {} in the queue", key, txID, lockOwner, s);
            }
            return lockOwner;
        }
        return null;
    }

    public synchronized TransactionID releaseLockButQueue(TransactionID txID) {
        if (txID.equals(lockOwner) || lockOwner == null) {
            lockOwner = lockQueue.pollFirst();
            queueLock(txID);
            if (logger.isDebugEnabled()) {
                StringBuilder s = new StringBuilder();
                for (TransactionID id : lockQueue)
                    s.append(id).append(",");
                logger.debug("Released but queued lock {} of tx {}, selected {} and left {} in the queue", key, txID, lockOwner, s);
            }
            return lockOwner;
        }
        return null;
    }

    public synchronized void releaseLock(TransactionID txID) {
        if (txID.equals(lockOwner)) {
            lockOwner = null;
        }
    }

    public synchronized void removeFromQueue(TransactionID txID) {
        lockQueue.remove(txID);
    }

    public synchronized void queueLock(TransactionID txID) {
        lockQueue.add(txID);
        logger.debug("Added {} to {}'s queue", txID, key);
    }
}
