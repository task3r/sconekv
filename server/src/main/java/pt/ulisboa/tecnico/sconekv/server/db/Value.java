package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidVersionException;

import java.nio.ByteBuffer;
import java.util.*;

public class Value {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private String key;
    private byte[] content;
    private short version;
    private Lock lock;

    public Value(String key) {
        this(key, new byte[0], (short) 0);
    }

    public Value(String key, byte[] versionAndContent) {
        this.key = key;
        ByteBuffer buffer = ByteBuffer.wrap(versionAndContent);
        this.version = buffer.getShort();
        this.content = new byte[buffer.remaining()];
        buffer.get(this.content);
        if (SconeConstants.LOCK_TYPE == SconeConstants.LockType.SINGLE) {
            this.lock = new SingleLock();
        } else if (SconeConstants.LOCK_TYPE == SconeConstants.LockType.READ_WRITE) {
            this.lock = new ReadWriteLock();
        }
    }

    public Value(String key, byte[] content, short version) {
        this.key = key;
        this.content = content;
        this.version = version;
        if (SconeConstants.LOCK_TYPE == SconeConstants.LockType.SINGLE) {
            this.lock = new SingleLock();
        } else if (SconeConstants.LOCK_TYPE == SconeConstants.LockType.READ_WRITE) {
            this.lock = new ReadWriteLock();
        }
    }

    public byte[] getContent() {
        return content;
    }

    public short getVersion() {
        return version;
    }

    public Set<TransactionID> getLockQueue() {
        return lock.getLockQueue();
    }

    public Set<TransactionID> update(byte[] content, short version) {
        synchronized (this) {
            if (version > this.version) {
                this.content = content;
                this.version = version;
                return lock.clearLockQueue();
            } else {
                logger.error("Tried applying previous version {} with content {} for object with version {}", version, content, this.version);
                return new HashSet<>();
            }
        }
    }

    public Set<TransactionID> validateAndLock(TransactionID txID, Operation op) throws InvalidVersionException {
        synchronized (this) {
            if (version == op.getVersion()) {
                return lock.lock(txID, op.getType());
            } else {
                throw new InvalidVersionException();
            }
        }
    }

    public Set<TransactionID> validate(Operation op) throws InvalidVersionException {
        synchronized (this) {
            if (version != op.getVersion()) {
                throw new InvalidVersionException();
            }
            return lock.getLockOwners();
        }
    }

    public Set<TransactionID> releaseLockAndChangeToNext(TransactionID txID) {
        synchronized (this) {
            Set<TransactionID> owners = lock.unlockAndLockNext(txID);

            if (logger.isDebugEnabled()) {
                if (owners.contains(txID)) {
                    StringBuilder ownersSB = new StringBuilder();
                    for (TransactionID id : owners)
                        ownersSB.append(id).append(",");
                    StringBuilder queueSB = new StringBuilder();
                    for (TransactionID id : lock.getLockQueue())
                        queueSB.append(id).append(",");
                    logger.debug("Released lock {} of tx {}, selected {} and left {} in the queue", key, txID, ownersSB, queueSB);
                }
            }

            return owners;
        }
    }

    public Set<TransactionID> releaseLockButQueue(TransactionID txID, Operation.Type type) {
        synchronized (this) {
            Set<TransactionID> owners = lock.unlockButQueue(txID, type);

            if (logger.isDebugEnabled()) {
                StringBuilder ownersSB = new StringBuilder();
                for (TransactionID id : owners)
                    ownersSB.append(id).append(",");
                StringBuilder queueSB = new StringBuilder();
                for (TransactionID id : lock.getLockQueue())
                    queueSB.append(id).append(",");
                logger.debug("Released but queued lock {} of tx {}, selected {} and left {} in the queue", key, txID, ownersSB, queueSB);
            }
            return owners;
        }

    }

    public void releaseLock(TransactionID txID) {
        synchronized (this) {
            lock.unlock(txID);
        }
    }

    public void removeFromQueue(TransactionID txID) {
        synchronized (this) {
            lock.removeFromQueue(txID);
        }
    }

    public void queueLock(TransactionID txID, Operation.Type type) {
        synchronized (this) {
            lock.queue(txID, type);
        }
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(content.length + 2);
        buffer.putShort(version);
        buffer.put(content);
        return buffer.array();
    }
}
