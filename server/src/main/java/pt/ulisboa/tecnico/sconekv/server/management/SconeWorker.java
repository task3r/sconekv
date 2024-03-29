package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
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
import pt.ulisboa.tecnico.sconekv.server.events.local.LocalRejectTransaction;
import pt.ulisboa.tecnico.sconekv.server.events.local.UpdateView;
import pt.ulisboa.tecnico.sconekv.server.exceptions.AlreadyProcessedTransaction;
import pt.ulisboa.tecnico.sconekv.server.exceptions.SMRStatusException;
import pt.ulisboa.tecnico.sconekv.server.exceptions.ValidTransactionNotLockableException;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachine;

import java.util.*;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    private final short id;
    private final Store store;
    private final CommunicationManager cm;
    private final StateMachine sm;
    private final DHT dht;
    private final Node self;
    private final Timer timer;

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
                if (event instanceof ClientRequest) {
                    ClientRequest request = (ClientRequest) event;
                    if (!request.checkBucket(this.dht, self)) {
                        logger.warn("Received request to incorrect bucket from {}, responding with updated DHT", request.getTxID());
                        handle(new GetDHTRequest(request.getClient()));
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

    private void delayEvent(SconeEvent event, int delay) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                cm.queueEvent(event);
            }
        }, delay);
    }

    // External Events

    @Override
    public void handle(ReadRequest readRequest) {
        Value value = store.get(readRequest.getKey());
        logger.info("Read {} v{} : {}", readRequest.getKey(), value.getVersion(), readRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateReadResponse(readRequest.getTxID(), readRequest.getKey().getBytes(), value);
        cm.replyToClient(readRequest.getClient(), response, this.id);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        Value value = store.get(writeRequest.getKey());
        logger.info("Write {} v{} : {}", writeRequest.getKey(), value.getVersion(), writeRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateWriteResponse(writeRequest.getTxID(), writeRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(writeRequest.getClient(), response, this.id);
    }

    @Override
    public void handle(DeleteRequest deleteRequest) {
        Value value = store.get(deleteRequest.getKey());
        logger.info("Delete {} v{} : {}", deleteRequest.getKey(), value.getVersion(), deleteRequest.getTxID());
        MessageBuilder response = CommunicationUtils.generateDeleteResponse(deleteRequest.getTxID(), deleteRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(deleteRequest.getClient(), response, this.id);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        logger.info("CommitRequest {}", commitRequest.getTxID());
        if (sm.isMaster()) {
            if (store.addTransaction(commitRequest.getTx())) {
                handle(new MakeLocalDecision(commitRequest.getTxID()));
            } else if (store.getTransaction(commitRequest.getTxID()).isDecided()) {
                replyIfAmCoordinator(commitRequest.getTxID());
            } else {
                logger.warn("Received duplicated commitRequest {}, client must be patient", commitRequest.getTxID());
            }
        } else {
            logger.error("Received commit request {} but I am not the master of bucket {}", commitRequest.getTxID(), sm.getCurrentBucketId());
        }
    }

    @Override
    public void handle(GetDHTRequest getViewRequest) {
        logger.info("GetView : {}", getViewRequest.getClient());
        MessageBuilder response = CommunicationUtils.generateGetDHTResponse(this.dht);
        cm.replyToClient(getViewRequest.getClient(), response, this.id);
    }

    // Internal Events
    // State Machine Replication

    @Override
    public void handle(LogTransaction logTransaction) {
        if (sm.isMaster()) {
            try {
                Transaction tx = store.getTransaction(logTransaction.getTxID());
                if (tx.getBuckets() == null)
                    tx.setTransactionReader(logTransaction.getTx().getReader());
                Node coordinator = dht.getMasterOfBucket(tx.getCoordinatorBucket());
                logger.info("Sending Local Decision for {} to {}", logTransaction.getTxID(), coordinator);
                if (coordinator.equals(self)) {
                    cm.queueEvent(new LocalDecisionResponse(self, sm.getCurrentVersion(), logTransaction.getTxID(),
                            store.getTransaction(logTransaction.getTxID()).getState() == TransactionState.PREPARED));
                    delayEvent(new RequestMissingLocalDecisions(logTransaction.getTxID()), 5000);
                } else {
                    cm.send(CommunicationUtils.generateLocalDecisionResponse(self, sm.getCurrentVersion(), logTransaction.getTxID(),
                            store.getTransaction(logTransaction.getTxID()).getState()), coordinator, id);
                    delayEvent(new RequestMissingGlobalDecision(logTransaction.getTxID()), 5000);
                }
            } catch (InvalidBucketException e) {
                logger.error("Invalid buckets in transaction {}, ignoring", logTransaction.getTxID());
            }
        } else {
            logger.debug("Logged transaction {}", logTransaction.getTxID());
            if (!store.addTransaction(logTransaction.getTx()))
                logger.debug("Transaction {} was already in the system", logTransaction.getTxID());
        }
        store.getTransaction(logTransaction.getTxID()).setReplicated();
    }

    @Override
    public void handle(LogTransactionDecision logTransactionDecision) {
        if (store.getTransaction(logTransactionDecision.getTxID()) == null && !sm.isMaster() && logTransactionDecision.getDecision() == TransactionState.ABORTED) {
            Transaction tx = new Transaction(logTransactionDecision.getTxID(), null, null);
            store.addTransaction(tx);
        }
        if (store.getTransaction(logTransactionDecision.getTxID()) != null) {
            if (logTransactionDecision.getDecision() == TransactionState.COMMITTED) {
                logger.info("Commit : {}", logTransactionDecision.getTxID());
                Set<TransactionID> txsToAbort = store.commit(logTransactionDecision.getTxID());
                for (TransactionID otherTxID : txsToAbort) {
                    cm.queueEvent(new LocalRejectTransaction(otherTxID));
                }
            } else {
                logger.info("Abort : {}", logTransactionDecision.getTxID());
                store.abort(logTransactionDecision.getTxID());
            }
            if (sm.isMaster()) {
                queueMakeLocalDecisions(store.releaseLocks(logTransactionDecision.getTxID(), false));
                replyIfAmCoordinator(logTransactionDecision.getTxID());
            }
        } else {
            logger.debug("Received decision before logging transaction {}", logTransactionDecision.getTxID());
            delayEvent(logTransactionDecision, 500);
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
                cm.replyToClient(tx.getClient(), response, this.id);
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

        if (store.getTransaction(logRollback.getTxID()) != null) {
            if (store.rollback(logRollback.getTxID()) && sm.isMaster()) {
                queueMakeLocalDecisions(store.releaseLocks(logRollback.getTxID(), true));
            }
        } else {
            logger.debug("Received log rollback before logging transaction, added to end of queue");
            delayEvent(logRollback, 500);
        }
    }

    @Override
    public void handle(Prepare prepare) {
        sm.prepareLogReplica(prepare, id);
    }

    @Override
    public void handle(PrepareOK prepareOK) {
        sm.prepareOK(prepareOK);
    }

    @Override
    public void handle(StartViewChange startViewChange) {
        sm.startViewChange(startViewChange, id);
    }

    @Override
    public void handle(DoViewChange doViewChange) {
        sm.doViewChange(doViewChange, id);
    }

    @Override
    public void handle(StartView startView) {
        sm.startView(startView, id);
    }

    @Override
    public void handle(GetState getState) {
        sm.getState(getState, id);
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
            logger.debug("Received a LocalDecisionResponse for a transaction {} from {} . I've yet to receive the commit request, delaying", localDecisionResponse.getTxID(), localDecisionResponse.getNode());
            delayEvent(localDecisionResponse, 500);
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
                            cm.broadcast(CommunicationUtils.generateAbortTransaction(self, sm.getCurrentVersion(), tx.getId()), masters, id);
                            cm.queueEvent(new AbortTransaction(self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
                        } else {
                            Bucket bucket = dht.getBucketOfNode(localDecisionResponse.getNode());
                            if (bucket != null) {
                                if (tx.addResponse(bucket.getId())) {
                                    tx.setDecided();
                                    Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                                    masters.remove(self);
                                    cm.broadcast(CommunicationUtils.generateCommitTransaction(self, sm.getCurrentVersion(), tx.getId()), masters, id);
                                    cm.queueEvent(new CommitTransaction(self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
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
        logger.debug("RequestRollback : {} {}", requestRollbackLocalDecision.getTxID(), requestRollbackLocalDecision.getNode());
        Transaction tx = store.getTransaction(requestRollbackLocalDecision.getTxID());
        if (tx == null) {
            logger.debug("Received a RequestRollbackLocalDecision for a transaction {} I've yet to receive, delaying", requestRollbackLocalDecision.getTxID());
            delayEvent(requestRollbackLocalDecision, 500);
        } else {
            try {
                if (!dht.getMasterOfBucket(tx.getCoordinatorBucket()).equals(self)) {
                    logger.error("Received incorrect RollbackLocalDecision for tx {}, i am not the coordinator", tx.getId());
                } else if (!tx.isDecided()) { // not yet committed nor aborted
                    tx.removeResponse(dht.getBucketOfNode(requestRollbackLocalDecision.getNode()).getId()); //orderrrr
                    if (self.equals(requestRollbackLocalDecision.getNode()))
                        cm.queueEvent(new RollbackLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId()));
                    else
                        cm.send(CommunicationUtils.generateRollbackLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId()), requestRollbackLocalDecision.getNode(), id);
                }
            } catch (InvalidBucketException e) {
                logger.error("Invalid buckets in transaction {}, ignoring", tx.getId());
            }
        }
    }

    @Override
    public void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse) {
        try {
            sm.prepareLogMaster(new LogRollback(rollbackLocalDecisionResponse.getTxID()), rollbackLocalDecisionResponse, id);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(CommitTransaction commitTransaction) {
        try {
            sm.prepareLogMaster(new LogTransactionDecision(commitTransaction.getTxID(), true), commitTransaction, id);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(AbortTransaction abortTransaction) {
        try {
            sm.prepareLogMaster(new LogTransactionDecision(abortTransaction.getTxID(), false), abortTransaction, id);
        } catch (SMRStatusException ignored) {
        }
    }

    @Override
    public void handle(MakeLocalDecision makeLocalDecision) {
        logger.info("MakeLocalDecision : {}", makeLocalDecision.getTxID());
        LogTransaction logTransaction = new LogTransaction(store.getTransaction(makeLocalDecision.getTxID()));
        try {
            store.validate(makeLocalDecision.getTxID());
            sm.prepareLogMaster(logTransaction, makeLocalDecision, id);
        } catch (SMRStatusException e) {
            queueMakeLocalDecisions(store.resetTx(makeLocalDecision.getTxID()));
        } catch (ValidTransactionNotLockableException e) {
            if (e.possibleRollback()) {
                rollbackTransactions(makeLocalDecision.getTxID(), e.getCurrentOwners());
            }
        } catch (AlreadyProcessedTransaction e) {
            if (e.isDecided()) {
                // this occurs if the makeDecision was already in the queue as the tx was aborted,
                // although it didn't acquire locks, it could be ahead of others in the queue
                // so we need to queue other txs that could be waiting for this one
                queueMakeLocalDecisions(store.releaseLocks(makeLocalDecision.getTxID(), false));
            }
        }
    }

    private void rollbackTransactions(TransactionID txID, Set<TransactionID> txsToRollback) {
        logger.debug("Will ask to rollback because of {}", txID);
        for (TransactionID otherTxID : txsToRollback) {
            try {
                Transaction otherTx = store.getTransaction(otherTxID);
                if (otherTx != null && otherTx.rollback()) {
                    Node coordinator = dht.getMasterOfBucket(otherTx.getCoordinatorBucket());
                    if (coordinator.equals(self)) {
                        cm.queueEvent(new RequestRollbackLocalDecision(self, sm.getCurrentVersion(), otherTxID));
                    } else {
                        cm.send(CommunicationUtils.generateRequestRollbackLocalDecision(self, sm.getCurrentVersion(), otherTxID), coordinator, id);
                    }
                }
            } catch (InvalidBucketException ignored) {
                logger.error("Invalid buckets in transaction {}, ignoring", otherTxID); // should not happen
            }
        }
    }

    @Override
    public void handle(RequestGlobalDecision requestGlobalDecision) {
        Transaction tx = store.getTransaction(requestGlobalDecision.getTxID());
        if (tx != null && tx.isDecided()) {
            if (tx.getState() == TransactionState.ABORTED)
                cm.send(CommunicationUtils.generateAbortTransaction(self, sm.getCurrentVersion(), tx.getId()),
                        requestGlobalDecision.getNode(), id);
            else
                cm.send(CommunicationUtils.generateCommitTransaction(self, sm.getCurrentVersion(), tx.getId()),
                        requestGlobalDecision.getNode(), id);
        }
    }

    @Override
    public void handle(RequestLocalDecision requestLocalDecision) {
        Transaction tx = store.getTransaction(requestLocalDecision.getTxID());
        if (tx == null) {
            tx = new Transaction(requestLocalDecision.getTxID(), null, null);
            tx.setState(TransactionState.ABORTED);
            store.addTransaction(tx);
        }
        cm.send(CommunicationUtils.generateLocalDecisionResponse(self, sm.getCurrentVersion(), tx.getId(),
                tx.getState()), requestLocalDecision.getNode(), id);
    }

    // Local Events

    @Override
    public void handle(CheckPendingTransactions checkPendingTransactions) {
        if (sm.isMaster()) {
            logger.info("CheckPendingTransactions and I'm the master");
            List<Transaction> pendingTransactions = store.getPendingTransactions();
            for (Transaction tx : pendingTransactions) {
                store.acquireLocks(tx);
                try {
                    if (tx.getBuckets() == null) { //TODO hotfix but should discover source of bug
                        logger.error("Tx {} has no buckets", tx.getId());
                        continue;
                    }
                    if (tx.getCoordinatorBucket() == sm.getCurrentBucketId()) {
                        Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                        masters.remove(self);
                        cm.broadcast(CommunicationUtils.generateRequestLocalDecision(self, sm.getCurrentVersion(), tx.getId()), masters, id);
                    } else {
                        Node coordinator = dht.getMasterOfBucket(tx.getCoordinatorBucket());
                        cm.send(CommunicationUtils.generateRequestGlobalDecision(self, sm.getCurrentVersion(), tx.getId()), coordinator, id);
                    }
                } catch (InvalidBucketException e) {
                    logger.error("Invalid buckets in transaction {}, ignoring", tx.getId()); // should not happen
                }
            }
            sm.setStatusNormal(); // accept new transaction commits
        } else {
            logger.error("CheckPendingTransactions but I'm not the master!!"); // shouldn't happen
        }
    }

    @Override
    public void handle(LocalRejectTransaction localRejectTransaction) {
        logger.info("LocalRejectTransaction : {}", localRejectTransaction.getTxID());
        Transaction tx = store.getTransaction(localRejectTransaction.getTxID());
        if (tx != null && tx.getState() != TransactionState.ABORTED) {
            LogTransaction logTransaction = new LogTransaction(tx);
            try {
                store.localReject(tx);
                sm.prepareLogMaster(logTransaction, localRejectTransaction, id);
            } catch (SMRStatusException ignored) {}
        }
    }

    @Override
    public void handle(UpdateView updateView) {
        Ring ring = updateView.getRing();
        logger.debug("Applying new view...");
        dht.applyView(ring);
        Bucket currentBucket = dht.getBucketOfNode(self);
        if (currentBucket == null) {
            logger.error("Was I removed from the membership? I do not belong to the new view");
            logger.debug("Ring contains myself: {}", ring.contains(self));
            System.exit(-1);
        }
        logger.info("Belong to bucket {}, master: {}", currentBucket.getId(),
                self.equals(currentBucket.getMaster()) ? "myself" : currentBucket.getMaster());
        cm.updateBucket(currentBucket);
        sm.updateBucket(currentBucket, ring.getVersion(), id);
    }

    @Override
    public void handle(RequestMissingLocalDecisions requestLocalDecision) {
        Transaction tx = store.getTransaction(requestLocalDecision.getTxID());
        try {
            if (!tx.isDecided()) {
                logger.info("RequestMissingLocalDecisions : {}", requestLocalDecision.getTxID());
                Set<Node> masters = dht.getMastersOfBuckets(tx.getBuckets());
                masters.remove(self);
                cm.broadcast(CommunicationUtils.generateRequestLocalDecision(self, sm.getCurrentVersion(), tx.getId()), masters, id);
                delayEvent(new RequestMissingLocalDecisions(tx.getId()), 5000);
            }
        } catch (InvalidBucketException e) {
            logger.error("Invalid buckets in transaction {}, ignoring", tx.getId()); // should not happen
        }
    }

    @Override
    public void handle(RequestMissingGlobalDecision requestLocalDecision) {
        Transaction tx = store.getTransaction(requestLocalDecision.getTxID());
        try {
            if (!tx.isDecided()) {
                logger.info("RequestMissingGlobalDecision : {}", requestLocalDecision.getTxID());
                Node coordinator = dht.getMasterOfBucket(tx.getCoordinatorBucket());
                cm.send(CommunicationUtils.generateRequestGlobalDecision(self, sm.getCurrentVersion(), tx.getId()), coordinator, id);
                delayEvent(new RequestMissingGlobalDecision(tx.getId()), 5000);
            }
        } catch (InvalidBucketException e) {
            logger.error("Invalid buckets in transaction {}, ignoring", tx.getId()); // should not happen
        }
    }

    // Aux methods

    private void queueMakeLocalDecisions(Set<TransactionID> transactions) {
        for (TransactionID txID : transactions) {
            cm.queueEvent(new MakeLocalDecision(txID));
        }
    }
}
