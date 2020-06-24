package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.SMRStatusException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachine;

import java.util.Set;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    private short id;
    private Store store;
    private CommunicationManager cm;
    private StateMachine sm;
    private DHT dht;
    private Node self;
    private int eventCounter;

    public SconeWorker(short id, CommunicationManager cm, StateMachine sm, Store store, DHT dht, Node self) {
        this.id = id;
        this.cm = cm;
        this.store = store;
        this.dht = dht;
        this.self = self;
        this.sm = sm;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                SconeEvent event = cm.takeEvent();
                if (event.getId() == null)
                    event.setId(generateId());
                if (event instanceof ClientRequest) {
                    ClientRequest request = (ClientRequest) event;
                    if (!request.checkBucket(this.dht, self)) {
                        cm.queueEvent(new GetDHTRequest(request.getId(), request.getClient())); // maybe just process it here?
                        continue; // do not process this event as it is not in the correct bucket
                    }
                }
                event.handledBy(this);
            }
        } catch (InterruptedException e) {
            logger.info("Worker {} interrupted.", id);
            Thread.currentThread().interrupt();
        }
    }

    private Pair<Short, Integer> generateId() {
        return new Pair<>(id, eventCounter++);
    }

    // External Events

    @Override
    public void handle(ReadRequest readRequest) {
        logger.info("Read {} : {}", readRequest.getKey(), readRequest.getTxID());
        Value value = store.get(readRequest.getKey());
        MessageBuilder response = CommunicationUtils.generateReadResponse(readRequest.getTxID(), readRequest.getKey().getBytes(), value);
        cm.replyToClient(readRequest.getClient(), response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        logger.info("Write {} : {}", writeRequest.getKey(), writeRequest.getTxID());
        Value value = store.get(writeRequest.getKey());
        MessageBuilder response = CommunicationUtils.generateWriteResponse(writeRequest.getTxID(), writeRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(writeRequest.getClient(), response);
    }

    @Override
    public void handle(DeleteRequest deleteRequest) {
        logger.info("Delete {} : {}", deleteRequest.getKey(), deleteRequest.getTxID());
        Value value = store.get(deleteRequest.getKey());
        MessageBuilder response = CommunicationUtils.generateDeleteResponse(deleteRequest.getTxID(), deleteRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(deleteRequest.getClient(), response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        if (sm.isMaster()) {
            if (store.addTransaction(commitRequest.getTx())) {
                cm.queueEvent(new MakeLocalDecision(generateId(), commitRequest.getTxID()));
            } else {
                logger.error("Received duplicated commitRequest {}, client must be patient", commitRequest.getTxID());
            }
        } else {
            logger.error("Received commit request {} but I am not the master of bucket {}", commitRequest.getId(), sm.getCurrentBucketId());
        }
    }

    @Override
    public void handle(GetDHTRequest getViewRequest) {
        logger.info("GetView : {}", getViewRequest.getClient());
        MessageBuilder response = CommunicationUtils.generateGetDHTResponse(this.dht);
        cm.replyToClient(getViewRequest.getClient(), response);
    }

    // Internal Events
    // State Machine Replication

    @Override
    public void handle(LogTransaction logTransaction) {
        if (sm.isMaster()) {
            try {
                Node coordinator = dht.getMasterOfBucket(store.getTransaction(logTransaction.getTxID()).getCoordinatorBucket());
                if (coordinator.equals(self)) {
                    cm.queueEvent(new LocalDecisionResponse(generateId(), self, sm.getCurrentVersion(), logTransaction.getTxID(),
                            store.getTransaction(logTransaction.getTxID()).getState() == TransactionState.PREPARED));
                } else {
                    cm.send(CommunicationUtils.generateLocalDecisionResponse(self, sm.getCurrentVersion(), logTransaction.getTxID(),
                            store.getTransaction(logTransaction.getTxID()).getState()), coordinator);
                }
            } catch (InvalidBucketException e) {
                logger.error("Invalid buckets in transaction {}, ignoring", logTransaction.getTxID());
            }
        } else {
            store.addTransaction(logTransaction.getTx());
        }
    }

    @Override
    public void handle(LogTransactionDecision logTransactionDecision) {
        if (logTransactionDecision.getDecision() == TransactionState.COMMITTED) {
            logger.info("Commit : {}", logTransactionDecision.getTxID());
            store.commit(logTransactionDecision.getTxID());
        } else {
            logger.info("Abort : {}", logTransactionDecision.getTxID());
            store.abort(logTransactionDecision.getTxID());
        }
        if (sm.isMaster()) {
            store.releaseLocks(logTransactionDecision.getTxID());
            if (store.getTransaction(logTransactionDecision.getTxID()).getClient() != null) { // might be from the old master, in that case this node will not be able to reply
                MessageBuilder response = CommunicationUtils.generateCommitResponse(logTransactionDecision.getTxID(),
                        logTransactionDecision.getDecision() == TransactionState.COMMITTED);
                cm.replyToClient(store.getTransaction(logTransactionDecision.getTxID()).getClient(), response);
            }
        }
    }

    @Override
    public void handle(LogRollback logRollback) {
        logger.info("Rollback : {}", logRollback.getTxID());
        store.getTransaction(logRollback.getTxID()).setState(TransactionState.NONE);
        if (sm.isMaster()) {
            queueMakeLocalDecisions(store.releaseLocks(logRollback.getTxID()));
            store.queueLocks(logRollback.getTxID());
        }
    }

    @Override
    public void handle(Prepare prepare) {
        sm.prepareLogReplica(prepare);
    }

    @Override
    public void handle(PrepareOK prepareOK) {
        sm.prepareOK(prepareOK);
    }

    @Override
    public void handle(StartViewChange startViewChange) {
        sm.startViewChange(startViewChange);
    }

    @Override
    public void handle(DoViewChange doViewChange) {
        sm.doViewChange(doViewChange);
    }

    @Override
    public void handle(StartView startView) {
        sm.startView(startView);
    }

    @Override
    public void handle(GetState getState) {
        sm.getState(getState);
    }

    @Override
    public void handle(NewState newState) {
        sm.newState(newState);
    }

    // Distributed Transactions

    @Override
    public void handle(LocalDecisionResponse localDecisionResponse) {
        Transaction tx = store.getTransaction(localDecisionResponse.getTxID());
        if (tx == null) {
            logger.debug("Received a LocalDecisionResponse for a transaction I've yet to receive, delaying");
            cm.queueEvent(localDecisionResponse); // maybe scheduled task, so there is really a delay
        } else {
            try {
                if (!dht.getMasterOfBucket(tx.getCoordinatorBucket()).equals(self)) {
                    logger.error("Received incorrect LocalDecisionResponse for tx {}, i am not the coordinator", tx.getId());
                } else {
                    if (!tx.isDecided()) { // not yet committed nor aborted
                        if (localDecisionResponse.shouldAbort()) {
                            tx.setDecided();
                            tx.setState(TransactionState.ABORTED);
                            Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                            masters.remove(self);
                            cm.broadcast(CommunicationUtils.generateAbortTransaction(self, sm.getCurrentVersion(), tx.getId()), masters);
                            cm.queueEvent(new AbortTransaction(generateId(), self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
                        } else {
                            tx.addResponse(dht.getBucketOfNode(localDecisionResponse.getNode()).getId());
                            if (tx.isReady()) {
                                tx.setDecided();
                                Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                                masters.remove(self);
                                cm.broadcast(CommunicationUtils.generateCommitTransaction(self, sm.getCurrentVersion(), tx.getId()), masters);
                                cm.queueEvent(new CommitTransaction(generateId(), self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
                            }
                        }
                    } // else maybe inform once more the sender about the global decision
                }
            } catch (InvalidBucketException e) {
                logger.error("Invalid buckets in transaction {}, ignoring", tx.getId());
            }
        }
    }

    @Override
    public void handle(RequestRollbackLocalDecision requestRollbackLocalDecision) {
        Transaction tx = store.getTransaction(requestRollbackLocalDecision.getTxID());
        try {
            if (!dht.getMasterOfBucket(tx.getCoordinatorBucket()).equals(self)) {
                logger.error("Received incorrect RollbackLocalDecision for tx {}, i am not the coordinator", tx.getId());
            } else if (!tx.isDecided()) { // not yet committed nor aborted
                tx.removeResponse(requestRollbackLocalDecision.getNode());
                if (self.equals(requestRollbackLocalDecision.getNode()))
                    cm.queueEvent(new RollbackLocalDecisionResponse(generateId(), self, sm.getCurrentVersion(), tx.getId()));
                else
                    cm.send(CommunicationUtils.generateRollbackLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId()), requestRollbackLocalDecision.getNode());
            }
        } catch (InvalidBucketException e) {
            logger.error("Invalid buckets in transaction {}, ignoring", tx.getId());
        }
    }

    @Override
    public void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse) {
        try {
            sm.prepareLogMaster(new LogRollback(generateId(), rollbackLocalDecisionResponse.getTxID(), null), rollbackLocalDecisionResponse);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(CommitTransaction commitTransaction) {
        try {
            sm.prepareLogMaster(new LogTransactionDecision(generateId(), commitTransaction.getTxID(), true, null), commitTransaction);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(AbortTransaction abortTransaction) {
        try {
            sm.prepareLogMaster(new LogTransactionDecision(generateId(), abortTransaction.getTxID(), false, null), abortTransaction);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(MakeLocalDecision makeLocalDecision) {
        logger.info("MakeLocalDecision : {}", makeLocalDecision.getTxID());
        LogTransaction logTransaction = new LogTransaction(generateId(), store.getTransaction(makeLocalDecision.getTxID()), null);
        try {
            store.validate(makeLocalDecision.getTxID());
            sm.prepareLogMaster(logTransaction, makeLocalDecision);
        } catch (SMRStatusException e) {
            queueMakeLocalDecisions(store.resetTx(makeLocalDecision.getTxID()));
        } catch (ValidTransactionNotLockableException e) {
            store.queueLocks(makeLocalDecision.getTxID());
            if (e.possibleRollback()) {
                for (TransactionID txID : e.getCurrentOwners()) {
                    try {
                        Node coordinator = dht.getMasterOfBucket(store.getTransaction(txID).getCoordinatorBucket());
                        if (coordinator.equals(self)) {
                            cm.queueEvent(new RequestRollbackLocalDecision(generateId(), self, sm.getCurrentVersion(), txID));
                        } else {
                            cm.send(CommunicationUtils.generateRequestRollbackLocalDecision(self, sm.getCurrentVersion(), txID), coordinator);
                        }
                    } catch (InvalidBucketException ignored) {
                        logger.error("Invalid buckets in transaction {}, ignoring", txID); // should not happen
                    }
                }
            }
        }
    }

    @Override
    public void handle(RequestGlobalDecision requestGlobalDecision) {
        Transaction tx = store.getTransaction(requestGlobalDecision.getTxID());
        if (tx != null && tx.isDecided()) {
            if (tx.getState() == TransactionState.ABORTED)
                cm.send(CommunicationUtils.generateAbortTransaction(self, sm.getCurrentVersion(), tx.getId()),
                        requestGlobalDecision.getNode());
            else
                cm.send(CommunicationUtils.generateCommitTransaction(self, sm.getCurrentVersion(), tx.getId()),
                        requestGlobalDecision.getNode());
        }
    }

    @Override
    public void handle(RequestLocalDecision requestLocalDecision) {
        Transaction tx = store.getTransaction(requestLocalDecision.getTxID());
        if (tx != null) {
            cm.send(CommunicationUtils.generateLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId(),
                    tx.getState()), requestLocalDecision.getNode());
        }
    }

    private void queueMakeLocalDecisions(Set<TransactionID> transactions) {
        for (TransactionID txID : transactions) {
            cm.queueEvent(new MakeLocalDecision(generateId(), txID));
        }
    }
}
