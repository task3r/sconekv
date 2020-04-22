package pt.ulisboa.tecnico.sconekv.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.server.exceptions.OutdatedVersionException;

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

    public synchronized void update(byte[] content, short version) throws OutdatedVersionException {
        if (version > this.version) {
            this.content = content;
            this.version = version;
        } else
            throw new OutdatedVersionException();
    }

    public synchronized boolean validateAndLock(TransactionID txID, Operation op) throws OutdatedVersionException {
        logger.debug("validate {} : {} <- {}", op.getKey(), version, op.getVersion());
        if (version == op.getVersion()) {
            if (this.lockOwner == null) {
                this.lockOwner = txID;
                return true;
            } else {
                return false;
            }
        } else {
            throw new OutdatedVersionException();
        }
    }

    public synchronized void validate(Operation op) throws OutdatedVersionException {
        if (version != op.getVersion()) {
            throw new OutdatedVersionException();
        }
    }

    public synchronized TransactionID releaseLock(TransactionID txID) {
        if (txID.equals(lockOwner)) {
            lockOwner = lockQueue.pollFirst();
            return lockOwner;
        }
        return null;
    }

    public synchronized void queueLock(TransactionID txID) {
        lockQueue.add(txID);
    }
}
