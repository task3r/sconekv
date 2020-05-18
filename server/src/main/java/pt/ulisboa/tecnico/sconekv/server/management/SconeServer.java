package pt.ulisboa.tecnico.sconekv.server.management;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.MessageType;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.events.external.WriteRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;

import java.io.IOException;
import java.util.UUID;

public class SconeServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    short id;
    int eventCounter;
    CommunicationManager cm;

    public SconeServer(short id, CommunicationManager cm) {
        this.id = id;
        this.cm = cm;
    }

    @Override
    public void run() {
        logger.info("Listening for requests...");

        while (!Thread.currentThread().isInterrupted()) {
            Triplet<MessageType, String, byte[]> message = cm.recvMessage();
            if (message != null) {
                if (message.getValue0() == MessageType.INTERNAL) {
                    recvInternalComm(message.getValue1(), message.getValue2());
                } else if (message.getValue0() == MessageType.EXTERNAL) {
                    recvClientRequest(message.getValue1(), message.getValue2());
                }
            }
        }
    }

    private Pair<Short, Integer> generateId() {
        return new Pair<>(id, eventCounter++);
    }

    private void recvClientRequest(String client, byte[] messageBytes) {

        External.Request.Reader request;
        try {
            request = SerializationUtils.getMessageFromBytes(messageBytes).getRoot(External.Request.factory);
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
                cm.queueEvent(new WriteRequest(generateId(), client, txID, new String(request.getRead().toArray())));
                break;

            case READ:
                cm.queueEvent(new ReadRequest(generateId(), client, txID, new String(request.getRead().toArray())));
                break;

            case COMMIT:
                Transaction tx = new Transaction(txID, request.getCommit());
                cm.queueEvent(new CommitRequest(generateId(), client, tx));
                break;

            case _NOT_IN_SCHEMA:
                logger.error("Received an incorrect request, ignoring...");
                break;
        }
    }

    private void recvInternalComm(String node, byte[] messageBytes) {
        Internal.InternalMessage.Reader message;
        try {
            message = SerializationUtils.getMessageFromBytes(messageBytes).getRoot(Internal.InternalMessage.factory);
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
                cm.queueEvent(new Prepare(generateId(), node, viewVersion, message.getPrepare().getOpNumber(), message.getPrepare().getCommitNumber()));
                break;
            case PREPARE_OK:
                cm.queueEvent(new PrepareOK(generateId(), node, viewVersion, message.getPrepareOk().getOpNumber()));
                break;
            case _NOT_IN_SCHEMA:
                logger.error("Received an incorrect internal message, ignoring...");
                break;
        }
    }
}
