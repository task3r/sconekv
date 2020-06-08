package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachine;

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
    public void handle(CommitRequest commitRequest) {
        // if I am the master and this request was not replicated
        if (sm.isMaster() && !commitRequest.isPrepared()) {
            sm.prepareLogMaster(commitRequest);
        } else {
            store.addTransaction(commitRequest.getTx());
            cm.queueEvent(new MakeLocalDecision(generateId(), commitRequest.getTxID()));
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
        // if I'm not the coordinator -> error
        if (localDecisionResponse.shouldAbort()) {
            cm.queueEvent(new AbortTransaction(generateId(), self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
        } else {
            Transaction tx = store.getTransaction(localDecisionResponse.getTxID());
            tx.addResponse(dht.getBucketOfNode(localDecisionResponse.getNode()).getId());
            if (tx.isReady()) {
                cm.queueEvent(new CommitTransaction(generateId(), self, sm.getCurrentVersion(), localDecisionResponse.getTxID()));
            }
        }
    }

    @Override
    public void handle(RequestRollbackLocalDecision requestRollbackLocalDecision) {

    }

    @Override
    public void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse) {

    }

    @Override
    public void handle(CommitTransaction commitTransaction) {
        try {
            store.perform(commitTransaction.getTxID());
            store.releaseLocks(commitTransaction.getTxID());
        } catch (InvalidOperationException | InvalidTransactionStateChangeException e) {
            logger.error("Transaction failed after being validated {}", commitTransaction.getTxID());
        }
        if (sm.isMaster()) {
            MessageBuilder response = CommunicationUtils.generateCommitResponse(commitTransaction.getTxID(), true);
            cm.replyToClient(store.getTransaction(commitTransaction.getTxID()).getClient(), response);
        }
    }

    @Override
    public void handle(AbortTransaction abortTransaction) {
        store.abort(abortTransaction.getTxID());
        store.releaseLocks(abortTransaction.getTxID());
        if (sm.isMaster()) {
            MessageBuilder response = CommunicationUtils.generateCommitResponse(abortTransaction.getTxID(), false);
            cm.replyToClient(store.getTransaction(abortTransaction.getTxID()).getClient(), response);
        }
    }

    @Override
    public void handle(MakeLocalDecision makeLocalDecision) {
        logger.info("Commit : {}", makeLocalDecision.getTxID());
        boolean valid;
        try {
            store.validate(makeLocalDecision.getTxID());
            valid = true;
        } catch (InvalidOperationException e) {
            valid = false;
        }
        cm.queueEvent(new LocalDecisionResponse(generateId(), self, sm.getCurrentVersion(), makeLocalDecision.getTxID(), valid));
    }
}
