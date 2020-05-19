package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.ClientRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.WriteRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    private short id;
    private Store store;
    private CommunicationManager cm;
    private StateMachineManager smm;
    private DHT dht;
    private Node self;

    public SconeWorker(short id, CommunicationManager cm, Store store, DHT dht, Node self) {
        this.id = id;
        this.cm = cm;
        this.store = store;
        this.dht = dht;
        this.self = self;
        this.smm = StateMachineManager.getInstance();
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                SconeEvent event = cm.takeEvent();
                event.handledBy(this);
            }
        } catch (InterruptedException e) {
            logger.info("Worker {} interrupted.", id);
            Thread.currentThread().interrupt();
        }
    }

    // External Events

    private boolean notReady(ClientRequest clientRequest) {
        // testar bucket & enviar nova view caso desatualizada
        if (clientRequest.isPrepared()) {
            return false;
        } else {
            smm.prepareLogMaster(clientRequest);
            return true;
        }
    }

    @Override
    public void handle(ReadRequest readRequest) {
        if (readRequest.getClient() != null && notReady(readRequest)) { // client != detects master for now
            return;
        }
        logger.info("Read {} : {}", readRequest.getKey(), readRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        readRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(readRequest.getKey());

        External.ReadResponse.Builder builder = rBuilder.initRead();
        builder.setKey(readRequest.getKey().getBytes());
        builder.setValue(value.getContent());
        builder.setVersion(value.getVersion());

        // if I am the master
        if (readRequest.getClient() != null)
            cm.replyToClient(readRequest, response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        if (writeRequest.getClient() != null && notReady(writeRequest)) {
            return;
        }
        logger.info("Write {} : {}", writeRequest.getKey(), writeRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        writeRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(writeRequest.getKey());

        External.WriteResponse.Builder builder = rBuilder.initWrite();
        builder.setKey(writeRequest.getKey().getBytes());
        builder.setVersion(value.getVersion());

        // if I am the master
        if (writeRequest.getClient() != null)
            cm.replyToClient(writeRequest, response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        if (commitRequest.getClient() != null && notReady(commitRequest)) {
            return;
        }
        logger.info("Commit : {}", commitRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        commitRequest.getTxID().serialize(rBuilder.getTxID());
        External.CommitResponse.Builder builder = rBuilder.initCommit();

        try {
            store.validate(commitRequest.getTx());
            store.perform(commitRequest.getTx());
            store.releaseLocks(commitRequest.getTx());
            builder.setResult(External.CommitResponse.Result.OK);
        } catch (InvalidOperationException e) {
            builder.setResult(External.CommitResponse.Result.NOK);
        }

        // if I am the master
        if (commitRequest.getClient() != null)
            cm.replyToClient(commitRequest, response);
    }

    // Internal Events

    @Override
    public void handle(Prepare prepare) {
        logger.info("Received prepare message");
        smm.prepareLogReplica(prepare);
    }

    @Override
    public void handle(PrepareOK prepareOK) {
        logger.info("Received prepareOK message");
        smm.prepareOK(prepareOK);
    }
}
