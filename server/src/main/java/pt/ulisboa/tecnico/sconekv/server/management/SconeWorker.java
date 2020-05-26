package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;
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
                if (event instanceof ClientRequest) {
                    ClientRequest request = (ClientRequest) event;
                    if (!request.checkBucket(this.dht, self))
                        cm.queueEvent(new GetDHTRequest(request.getId(), request.getClient())); // maybe just process it here?
                }

                event.handledBy(this);
            }
        } catch (InterruptedException e) {
            logger.info("Worker {} interrupted.", id);
            Thread.currentThread().interrupt();
        }
    }

    // External Events

    @Override
    public void handle(ReadRequest readRequest) {
        logger.info("Read {} : {}", readRequest.getKey(), readRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        readRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(readRequest.getKey());

        External.ReadResponse.Builder builder = rBuilder.initRead();
        builder.setKey(readRequest.getKey().getBytes());
        builder.setValue(value.getContent());
        builder.setVersion(value.getVersion());

        cm.replyToClient(readRequest, response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        logger.info("Write {} : {}", writeRequest.getKey(), writeRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        writeRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(writeRequest.getKey());

        External.WriteResponse.Builder builder = rBuilder.initWrite();
        builder.setKey(writeRequest.getKey().getBytes());
        builder.setVersion(value.getVersion());

        cm.replyToClient(writeRequest, response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        // if I am the master and this request was not replicated
        if (commitRequest.getClient() != null && !commitRequest.isPrepared()) {
            smm.prepareLogMaster(commitRequest);
        } else {
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
    }

    @Override
    public void handle(GetDHTRequest getViewRequest) {
        logger.info("GetView : {}", getViewRequest.getClient());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        dht.serialize(rBuilder.initDht());
        cm.replyToClient(getViewRequest, response);
    }

    // Internal Events

    @Override
    public void handle(Prepare prepare) {
        smm.prepareLogReplica(prepare);
    }

    @Override
    public void handle(PrepareOK prepareOK) {
        smm.prepareOK(prepareOK);
    }
}
