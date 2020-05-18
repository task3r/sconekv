package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    short id;
    Store store;
    ZMQ.Socket clientRequestSocket;
    BlockingQueue<SconeEvent> eventQueue;

    public SconeWorker(short id, ZMQ.Socket clientRequestSocket, Store store, BlockingQueue<SconeEvent> eventQueue) {
        this.id = id;
        this.clientRequestSocket = clientRequestSocket;
        this.store = store;
        this.eventQueue = eventQueue;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                SconeEvent event = eventQueue.take();
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

        replyToClient(readRequest, response);
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

        replyToClient(writeRequest, response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
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

        replyToClient(commitRequest, response);
    }

    private void replyToClient(ClientRequest request, MessageBuilder response) {
        try {
            clientRequestSocket.sendMore(request.getClient());
            clientRequestSocket.sendMore("");
            clientRequestSocket.send(SerializationUtils.getBytesFromMessage(response), 0);
        } catch (IOException e) {
            logger.error("IOException serializing response to {}", request);
        }
    }

    // Internal Events

    @Override
    public void handle(Prepare prepare) {

    }

    @Override
    public void handle(PrepareOK prepareOK) {

    }
}