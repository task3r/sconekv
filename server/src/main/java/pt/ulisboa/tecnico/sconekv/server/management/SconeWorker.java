package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.*;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class SconeWorker implements Runnable, SconeEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(SconeWorker.class);

    short id;
    Store store;
    ZMQ.Socket socket;
    BlockingQueue<SconeEvent> eventQueue;

    public SconeWorker(short id, ZMQ.Socket socket, Store store, BlockingQueue<SconeEvent> eventQueue) {
        this.id = id;
        this.socket = socket;
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

    @Override
    public void handle(ReadRequest readRequest) {
        logger.info("Read {} : {}", readRequest.getKey(), readRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        Message.Response.Builder rBuilder = response.initRoot(Message.Response.factory);
        readRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(readRequest.getKey());

        Message.ReadResponse.Builder builder = rBuilder.initRead();
        builder.setKey(readRequest.getKey().getBytes());
        builder.setValue(value.getContent());
        builder.setVersion(value.getVersion());

        reply(readRequest, response);
    }

    @Override
    public void handle(WriteRequest writeRequest) {
        logger.info("Write {} : {}", writeRequest.getKey(), writeRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        Message.Response.Builder rBuilder = response.initRoot(Message.Response.factory);
        writeRequest.getTxID().serialize(rBuilder.getTxID());

        Value value = store.get(writeRequest.getKey());

        Message.WriteResponse.Builder builder = rBuilder.initWrite();
        builder.setKey(writeRequest.getKey().getBytes());
        builder.setVersion(value.getVersion());

        reply(writeRequest, response);
    }

    @Override
    public void handle(CommitRequest commitRequest) {
        logger.info("Commit : {}", commitRequest.getTxID());
        MessageBuilder response = new org.capnproto.MessageBuilder();
        Message.Response.Builder rBuilder = response.initRoot(Message.Response.factory);
        commitRequest.getTxID().serialize(rBuilder.getTxID());
        Message.CommitResponse.Builder builder = rBuilder.initCommit();

        try {
            store.validate(commitRequest.getTx());
            store.perform(commitRequest.getTx());
            store.releaseLocks(commitRequest.getTx());
            builder.setResult(Message.CommitResponse.Result.OK);
        } catch (InvalidOperationException e) {
            builder.setResult(Message.CommitResponse.Result.NOK);
        }

        reply(commitRequest, response);
    }

    private void reply(ClientRequest request, MessageBuilder response) {
        try {
            socket.sendMore(request.getClient());
            socket.sendMore("");
            socket.send(SerializationUtils.getBytesFromMessage(response), 0);
        } catch (IOException e) {
            logger.error("IOException serializing response to {}", request);
        }
    }
}
