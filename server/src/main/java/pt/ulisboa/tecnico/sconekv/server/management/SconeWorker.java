package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.local.CheckPendingTransactions;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.AlreadyProcessedTransaction;
import pt.ulisboa.tecnico.sconekv.server.exceptions.SMRStatusException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachine;

import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    private short id;
    private Store store;
    private CommunicationManager cm;
    private StateMachine sm;
    private DHT dht;
    private Node self;
    private int eventCounter;
    private Timer timer;

    public SconeWorker(short id, CommunicationManager cm, StateMachine sm, Store store, DHT dht, Node self) {
        this.id = id;
        this.cm = cm;
        this.store = store;
        this.dht = dht;
        this.self = self;
        this.sm = sm;
        this.timer = new Timer("DelayEventTimer", true);
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

    private void delayEvent(SconeEvent event) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                cm.queueEvent(event);
            }
        }, 500);
    }

    // External Events

    @Override
    public void handle(ReadRequest readRequest) {
        Value value = store.get(readRequest.getKey());
        logger.info("Read {} v{} : {}", readRequest.getKey(), value.getVersion(), readRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateReadResponse(readRequest.getTxID(), readRequest.getKey().getBytes(), value);
        cm.replyToClient(readRequest.getClient(), response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        Value value = store.get(writeRequest.getKey());
        logger.info("Write {} v{} : {}", writeRequest.getKey(), value.getVersion(), writeRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateWriteResponse(writeRequest.getTxID(), writeRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(writeRequest.getClient(), response);
    }

    @Override
    public void handle(DeleteRequest deleteRequest) {
        Value value = store.get(deleteRequest.getKey());
        logger.info("Delete {} v{} : {}", deleteRequest.getKey(), value.getVersion(), deleteRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateDeleteResponse(deleteRequest.getTxID(), deleteRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(deleteRequest.getClient(), response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        logger.info("CommitRequest {}", commitRequest.getTxID());
        if (sm.isMaster()) {
            if (store.addTransaction(commitRequest.getTx())) {
                handle(new MakeLocalDecision(generateId(), commitRequest.getTxID()));
            } else if (store.getTransaction(commitRequest.getTxID()).isDecided()) {
                replyIfAmCoordinator(commitRequest.getTxID());
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
                logger.info("Sending Local Decision for {}", logTransaction.getTxID());
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
        if (store.getTransaction(logTransactionDecision.getTxID()) != null) {
            if (logTransactionDecision.getDecision() == TransactionState.COMMITTED) {
                logger.info("Commit : {}", logTransactionDecision.getTxID());
                store.commit(logTransactionDecision.getTxID());
            } else {
                logger.info("Abort : {}", logTransactionDecision.getTxID());
                store.abort(logTransactionDecision.getTxID());
            }
            if (sm.isMaster()) {
                queueMakeLocalDecisions(store.releaseLocks(logTransactionDecision.getTxID(), false));
                replyIfAmCoordinator(logTransactionDecision.getTxID());
            }
        } else {
            logger.debug("Received log decision before logging transaction, added to end of queue");
            cm.queueEvent(logTransactionDecision);
        }
    }

    private void replyIfAmCoordinator(TransactionID txID) {
        Transaction tx = store.getTransaction(txID);
        try {
            Node coordinator = dht.getMasterOfBucket(tx.getCoordinatorBucket());
            // even if I am the coordinator, the request might be from the old master, in that case this node will not be able to reply
            if (self.equals(coordinator) && tx.getClient() != null) {
                MessageBuilder response = CommunicationUtils.generateCommitResponse(txID,
                        tx.getState() == TransactionState.COMMITTED);
                cm.replyToClient(tx.getClient(), response);
                logger.debug("Responded to client {}", txID);
            }
        } catch (InvalidBucketException ignored) {
            // this does not happen if the number of buckets e static
            // right now if it were to happen, it would've happened earlier
        }
    }

    @Override
    public void handle(LogRollback logRollback) {
        logger.info("Rollback : {}", logRollback.getTxID());
        store.getTransaction(logRollback.getTxID()).setState(TransactionState.RECEIVED);

        if (store.getTransaction(logRollback.getTxID()) != null) {
            if (sm.isMaster()) {
                queueMakeLocalDecisions(store.releaseLocks(logRollback.getTxID(), true));
            }
        } else {
            logger.debug("Received log rollback before logging transaction, added to end of queue");
            cm.queueEvent(logRollback);
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
            logger.debug("Received a LocalDecisionResponse for a transaction {} I've yet to receive, delaying", localDecisionResponse.getTxID());
            delayEvent(localDecisionResponse);
        } else {
            logger.debug("Received a LocalDecisionResponse from {} for transaction {}", localDecisionResponse.getNode(), localDecisionResponse.getTxID());
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
                            Bucket bucket = dht.getBucketOfNode(localDecisionResponse.getNode());
                            if (bucket != null) {
                                tx.addResponse(bucket.getId());
                                if (tx.isReady()) {
                                    tx.setDecided();
                                    Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                                    masters.remove(self);
                                    cm.broadcast(CommunicationUtils.generateCommitTransaction(self, sm.getCurrentVersion(), tx.getId()), masters);
                                    cm.queueEvent(new CommitTransaction(generateId(), self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
                                }
                            } else {
                                logger.error("Received LocalDecision from node that does not belong to the system, ignoring");
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
        if (tx == null) {
            logger.debug("Received a RequestRollbackLocalDecision for a transaction {} I've yet to receive, delaying", requestRollbackLocalDecision.getTxID());
            delayEvent(requestRollbackLocalDecision);
        } else {
            try {
                if (!dht.getMasterOfBucket(tx.getCoordinatorBucket()).equals(self)) {
                    logger.error("Received incorrect RollbackLocalDecision for tx {}, i am not the coordinator", tx.getId());
                } else if (!tx.isDecided()) { // not yet committed nor aborted
                    tx.removeResponse(dht.getBucketOfNode(requestRollbackLocalDecision.getNode()).getId());
                    if (self.equals(requestRollbackLocalDecision.getNode()))
                        cm.queueEvent(new RollbackLocalDecisionResponse(generateId(), self, sm.getCurrentVersion(), tx.getId()));
                    else
                        cm.send(CommunicationUtils.generateRollbackLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId()), requestRollbackLocalDecision.getNode());
                }
            } catch (InvalidBucketException e) {
                logger.error("Invalid buckets in transaction {}, ignoring", tx.getId());
            }
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
            if (e.possibleRollback()) {
                logger.debug("Will ask to rollback because of {}", makeLocalDecision.getTxID());
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
        } catch (AlreadyProcessedTransaction e) {
            // this occurs if the makeDecision was already in the queue as the tx was aborted,
            // although it didn't acquire locks, it could be ahead of others in the queue
            // so we need to queue other txs that could be waiting for this one
            queueMakeLocalDecisions(store.releaseLocks(makeLocalDecision.getTxID(), false));
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

    // Local Events

    @Override
    public void handle(CheckPendingTransactions checkPendingTransactions) {
        if (sm.isMaster()) {
            List<Transaction> pendingTransactions = store.getPendingTransactions();
            for (Transaction tx : pendingTransactions) {
                try {
                    if (tx.getCoordinatorBucket() == sm.getCurrentBucketId()) {
                        Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                        masters.remove(self);
                        cm.broadcast(CommunicationUtils.generateRequestLocalDecision(self, sm.getCurrentVersion(), tx.getId()), masters);
                    } else {
                        Node coordinator = dht.getMasterOfBucket(tx.getCoordinatorBucket());
                        cm.send(CommunicationUtils.generateRequestGlobalDecision(self, sm.getCurrentVersion(), tx.getId()), coordinator);
                    }
                } catch (InvalidBucketException e) {
                    logger.error("Invalid buckets in transaction {}, ignoring", tx.getId()); // should not happen
                }
            }
        }
    }

    // Aux methods

    private void queueMakeLocalDecisions(Set<TransactionID> transactions) {
        for (TransactionID txID : transactions) {
            cm.queueEvent(new MakeLocalDecision(generateId(), txID));
        }
    }
}
