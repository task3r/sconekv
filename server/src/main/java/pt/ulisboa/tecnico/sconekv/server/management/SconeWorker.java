package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachineManager;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    private short id;
    private Store store;
    private CommunicationManager cm;
    private StateMachineManager smm;
    private DHT dht;
    private Node self;
    private int eventCounter;

    public SconeWorker(short id, CommunicationManager cm, StateMachineManager smm, Store store, DHT dht, Node self) {
        this.id = id;
        this.cm = cm;
        this.store = store;
        this.dht = dht;
        this.self = self;
        this.smm = smm;
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
        cm.replyToClient(readRequest, response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        logger.info("Write {} : {}", writeRequest.getKey(), writeRequest.getTxID());
        Value value = store.get(writeRequest.getKey());
        MessageBuilder response = CommunicationUtils.generateWriteResponse(writeRequest.getTxID(), writeRequest.getKey().getBytes(), value.getVersion());
        cm.replyToClient(writeRequest, response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        // if I am the master and this request was not replicated
        if (commitRequest.getClient() != null && !commitRequest.isPrepared()) {
            smm.prepareLogMaster(commitRequest);
        } else {
            logger.info("Commit : {}", commitRequest.getTxID());
            boolean wasSuccessful;
            try {
                store.validate(commitRequest.getTx());
                store.perform(commitRequest.getTx());
                store.releaseLocks(commitRequest.getTx());
                wasSuccessful = true;
            } catch (InvalidOperationException e) {
                wasSuccessful = false;
            }
            // if I am the master
            if (commitRequest.getClient() != null) {
                MessageBuilder response = CommunicationUtils.generateCommitResponse(commitRequest.getTxID(), wasSuccessful);
                cm.replyToClient(commitRequest, response);
            }
        }
    }

    @Override
    public void handle(GetDHTRequest getViewRequest) {
        logger.info("GetView : {}", getViewRequest.getClient());
        MessageBuilder response = CommunicationUtils.generateGetDHTResponse(this.dht);
        cm.replyToClient(getViewRequest, response);
    }

    // Internal Events
    // State Machine Replication

    @Override
    public void handle(Prepare prepare) {
        smm.prepareLogReplica(prepare);
    }

    @Override
    public void handle(PrepareOK prepareOK) {
        smm.prepareOK(prepareOK);
    }

    @Override
    public void handle(StartViewChange startViewChange) {
        smm.startViewChange(startViewChange);
    }

    @Override
    public void handle(DoViewChange doViewChange) {
        smm.doViewChange(doViewChange);
    }

    @Override
    public void handle(StartView startView) {
        smm.startView(startView);
    }

    @Override
    public void handle(GetState getState) {
        smm.getState(getState);
    }

    @Override
    public void handle(NewState newState) {
        smm.newState(newState);
    }

    // Distributed Transactions

    @Override
    public void handle(CommitLocalDecision commitLocalDecision) {

    }

    @Override
    public void handle(RequestRollbackLocalDecision requestRollbackLocalDecision) {

    }

    @Override
    public void handle(RollbackLocalDecisionResponse rollbackLocalDecisionResponse) {

    }

    @Override
    public void handle(CommitTransaction commitTransaction) {

    }

    @Override
    public void handle(AbortTransaction abortTransaction) {

    }
}
