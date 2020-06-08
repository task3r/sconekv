package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.StructList;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.MessageType;
import pt.ulisboa.tecnico.sconekv.server.db.Transaction;
import pt.ulisboa.tecnico.sconekv.server.events.external.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.transactions.*;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class SconeServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    private short id;
    private int eventCounter;
    private CommunicationManager cm;

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
                    recvInternalComm(message.getValue2());
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

        ClientRequest event = getClientRequest(request, client, generateId());
        if (event != null)
            cm.queueEvent(event);
    }

    private ClientRequest getClientRequest(External.Request.Reader request, String client, Pair<Short, Integer> eventId) {
        TransactionID txID = new TransactionID(request.getTxID());

        switch (request.which()) {
            case WRITE:
                return new WriteRequest(eventId, client, txID, new String(request.getRead().toArray()));
            case READ:
                return new ReadRequest(eventId, client, txID, new String(request.getRead().toArray()));
            case COMMIT:
                Transaction tx = new Transaction(txID, client, request.getCommit());
                return new CommitRequest(eventId, client, tx, request);
            case GET_DHT:
                return new GetDHTRequest(eventId, client);
            case _NOT_IN_SCHEMA:
            default:
                logger.error("Received an incorrect request, ignoring...");
                return null;
        }
    }

    private void recvInternalComm(byte[] messageBytes) {
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

        Version viewVersion = SerializationUtils.getVersionFromMesage(message.getViewVersion());

        Node node = null;
        try {
            node = SerializationUtils.getNodeFromMessage(message.getNode());
        } catch (UnknownHostException e) {
            logger.error("Unknown host in internal message, ignoring...");
            return;
        }

        Pair<Short, Integer> eventId = generateId();

        switch (message.which()) {
            case PREPARE:
                // prepare events have the client as null because using ZMQ only the master can respond to the client
                // if really needed, the client needs to be listening to requests as well, ans then this string could represent the address
                ClientRequest clientRequest = getClientRequest(message.getPrepare().getMessage(), null, generateId());
                if (clientRequest instanceof CommitRequest) {
                    cm.queueEvent(new Prepare(eventId, node, viewVersion, message.getPrepare().getOpNumber(), message.getPrepare().getCommitNumber(),
                            message.getPrepare().getBucket(), (CommitRequest) clientRequest));
                } else {
                    logger.error("Received incorrect replicated request of type {}", clientRequest != null ? clientRequest.getClass() : "NULL");
                }
                break;
            case PREPARE_OK:
                cm.queueEvent(new PrepareOK(eventId, node, viewVersion, message.getPrepareOk().getOpNumber(), message.getPrepareOk().getBucket()));
                break;
            case START_VIEW_CHANGE:
                cm.queueEvent(new StartViewChange(eventId, node, viewVersion));
                break;
            case DO_VIEW_CHANGE:
                cm.queueEvent(new DoViewChange(eventId, node, viewVersion, getLogFromMessage(message.getDoViewChange().getLog()),
                        SerializationUtils.getVersionFromMesage(message.getDoViewChange().getTerm()), message.getDoViewChange().getCommitNumber()));
                break;
            case START_VIEW:
                cm.queueEvent(new StartView(eventId, node, viewVersion, getLogFromMessage(message.getStartView().getLog()), message.getStartView().getCommitNumber()));
                break;
            case GET_STATE:
                cm.queueEvent(new GetState(eventId, node, viewVersion, message.getGetState().getOpNumber()));
                break;
            case NEW_STATE:
                cm.queueEvent(new NewState(eventId, node, viewVersion, getLogFromMessage(message.getNewState().getLogSegment()), message.getNewState().getOpNumber(), message.getNewState().getCommitNumber()));
                break;
            case LOCAL_DECISION_RESPONSE:
                cm.queueEvent(new LocalDecisionResponse(eventId, node, viewVersion, new TransactionID(message.getLocalDecisionResponse().getTxID()), message.getLocalDecisionResponse().getToCommit()));
                break;
            case REQUEST_ROLLBACK_LOCAL_DECISION:
                cm.queueEvent(new RequestRollbackLocalDecision(eventId, node, viewVersion, new TransactionID(message.getRequestRollbackLocalDecision())));
                break;
            case ROLLBACK_LOCAL_DECISION_RESPONSE:
                cm.queueEvent(new RollbackLocalDecisionResponse(eventId, node, viewVersion, new TransactionID(message.getRollbackLocalDecisionResponse())));
                break;
            case COMMIT_TRANSACTION:
                cm.queueEvent(new CommitTransaction(eventId, node, viewVersion, new TransactionID(message.getCommitTransaction())));
                break;
            case ABORT_TRANSACTION:
                cm.queueEvent(new AbortTransaction(eventId, node, viewVersion, new TransactionID(message.getAbortTransaction())));
                break;
            case _NOT_IN_SCHEMA:
            default:
                logger.error("Received an incorrect internal message, ignoring...");
                break;
        }
    }

    private List<LogEntry> getLogFromMessage(StructList.Reader<Internal.LoggedRequest.Reader> logReader) {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 0; i < logReader.size(); i++) {
            log.add(new LogEntry(new CommitRequest(null, null,
                    new Transaction(new TransactionID(logReader.get(i).getRequest().getTxID()), null,
                            logReader.get(i).getRequest().getCommit()), logReader.get(i).getRequest())));
        }
        return log;
    }
}
