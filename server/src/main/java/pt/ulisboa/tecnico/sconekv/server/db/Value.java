package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidVersionException;

import java.util.TreeSet;

public class Value {
    private static final Logger logger = LoggerFactory.getLogger(Store.class);

    private byte[] content;
    private short version;
    private TransactionID lockOwner;
    private TreeSet<TransactionID> lockQueue = new TreeSet<>();

    public Value() {
        this.content = new byte[0];
        this.version = 0;
    }
    public Value(byte[] content, short version) {
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

    public synchronized void update(byte[] content, short version) {
        if (version > this.version) {
            this.content = content;
            this.version = version;
        } else
            logger.error("Tried applying previous version {} with content {} for object with version {}", version, content, this.version);
    }

    public synchronized TransactionID validateAndLock(TransactionID txID, Operation op) throws InvalidVersionException {
        if (version == op.getVersion()) {
            if (this.lockOwner == null) {
                this.lockOwner = txID;
            }
            return this.lockOwner;
        } else {
            throw new InvalidVersionException();
        }
    }

    public synchronized TransactionID validate(Operation op) throws InvalidVersionException {
        if (version != op.getVersion()) {
            throw new InvalidVersionException();
        }
        return this.lockOwner;
    }

    public synchronized TransactionID releaseLockAndQueueNext(TransactionID txID) {
        if (txID.equals(lockOwner)) {
            lockOwner = lockQueue.pollFirst();
            return lockOwner;
        }
        return null;
    }

    public synchronized  boolean releaseLock(TransactionID txID) {
        if (txID.equals(lockOwner)) {
            lockOwner = null;
            return true;
        }
        return false;
    }

    public synchronized void queueLock(TransactionID txID) {
        lockQueue.add(txID);
    }
}
