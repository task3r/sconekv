package pt.ulisboa.tecnico.sconekv.server.management;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.external.WriteRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class SconeServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    short id;
    int eventCounter;
    ZMQ.Socket clientRequestSocket;
    ZMQ.Socket internalCommSocket;
    ZMQ.Poller poller;
    BlockingQueue<SconeEvent> eventQueue;

    public SconeServer(short id, ZMQ.Socket clientRequestSocket, ZMQ.Socket internalCommSocket, ZMQ.Poller poller, BlockingQueue<SconeEvent> eventQueue) {
        this.id = id;
        this.clientRequestSocket = clientRequestSocket;
        this.internalCommSocket = internalCommSocket;
        this.poller = poller;
        this.eventQueue = eventQueue;
    }

    @Override
    public void run() {
        logger.info("Listening for requests...");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                poller.poll();
                if (poller.pollin(0))
                    recvClientRequest();
                if (poller.pollin(1))
                    recvInternalComm();
            } catch (ZMQException e) {
                logger.error(e.toString());
            }
        }
    }

    private Pair<Short, Integer> generateId() {
        return new Pair<>(id, eventCounter++);
    }

    private Pair<String, byte[]> recvMessage(ZMQ.Socket socket) {
        String client = clientRequestSocket.recvStr();
        clientRequestSocket.recv(0); // delimiter
        byte[] requestBytes = clientRequestSocket.recv(0);
        return new Pair<>(client, requestBytes);
    }

    private void recvClientRequest() {

        Pair<String, byte[]> receivedMessage = recvMessage(clientRequestSocket);

        External.Request.Reader request;
        try {
            request = SerializationUtils.getMessageFromBytes(receivedMessage.getValue1()).getRoot(External.Request.factory);
        } catch (IOException e) {
            logger.error("IOException deserializing client request. Continuing...");
            return;
        }

        if (request == null) {
            logger.error("Request deserialized to null. Ignoring...");
            return;
        }

        TransactionID txID = new TransactionID(request.getTxID());

        switch (request.which()) {
            case WRITE:
                eventQueue.add(new WriteRequest(generateId(), receivedMessage.getValue0(), txID, new String(request.getRead().toArray())));
                break;

            case READ:
                eventQueue.add(new ReadRequest(generateId(), receivedMessage.getValue0(), txID, new String(request.getRead().toArray())));
                break;

            case COMMIT:
                Transaction tx = new Transaction(txID, request.getCommit());
                eventQueue.add(new CommitRequest(generateId(), receivedMessage.getValue0(), tx));
                break;

            case _NOT_IN_SCHEMA:
                logger.error("Received an incorrect request, ignoring...");
                break;
        }
    }

    private void recvInternalComm() {
        Pair<String, byte[]> receivedMessage = recvMessage(internalCommSocket);

        Internal.InternalMessage.Reader message;
        try {
            message = SerializationUtils.getMessageFromBytes(receivedMessage.getValue1()).getRoot(Internal.InternalMessage.factory);
        } catch (IOException e) {
            logger.error("IOException deserializing internal message. Continuing...");
            return;
        }

        if (message == null) {
            logger.error("Internal message deserialized to null. Ignoring...");
            return;
        }

        Version viewVersion = new Version(message.getViewVersion().getTimestamp(),
                                          new UUID(message.getViewVersion().getMessageId().getMostSignificant(),
                                                   message.getViewVersion().getMessageId().getLeastSignificant()));

        switch (message.which()) {
            case PREPARE:
                eventQueue.add(new Prepare(generateId(), receivedMessage.getValue0(), viewVersion, message.getPrepare().getOpNumber(), message.getPrepare().getCommitNumber()));
                break;
            case PREPARE_OK:
                eventQueue.add(new PrepareOK(generateId(), receivedMessage.getValue0(), viewVersion, message.getPrepareOk().getOpNumber()));
                break;
            case _NOT_IN_SCHEMA:
                logger.error("Received an incorrect internal message, ignoring...");
                break;
        }
    }
}
