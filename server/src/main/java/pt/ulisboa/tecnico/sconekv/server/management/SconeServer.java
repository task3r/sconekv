package pt.ulisboa.tecnico.sconekv.server.management;

import org.capnproto.StructList;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
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
    private CommunicationManager cm;

    public SconeServer(short id, CommunicationManager cm) {
        this.id = id;
        this.cm = cm;
    }

    @Override
    public void run() {
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

        ClientRequest event = getClientRequest(request, client);
        if (event != null)
            cm.queueEvent(event);
    }

    private ClientRequest getClientRequest(External.Request.Reader request, String client) {
        TransactionID txID = new TransactionID(request.getTxID());

        switch (request.which()) {
            case WRITE:
                return new WriteRequest(client, txID, new String(request.getRead().toArray()));
            case READ:
                return new ReadRequest(client, txID, new String(request.getRead().toArray()));
            case DELETE:
                return new DeleteRequest(client, txID, new String(request.getRead().toArray()));
            case COMMIT:
                Transaction tx = new Transaction(txID, client, request.getCommit());
                return new CommitRequest(client, tx, request);
            case GET_DHT:
                return new GetDHTRequest(client);
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

        switch (message.which()) {
            case PREPARE:
                // prepare events have the client as null because using ZMQ only the master can respond to the client
                // if really needed, the client needs to be listening to requests as well, ans then this string could represent the address
                LogEvent logEvent = getLogEvent(message.getPrepare().getEvent());
                if (logEvent != null) {
                    cm.queueEvent(new Prepare(node, viewVersion, message.getPrepare().getOpNumber(), message.getPrepare().getCommitNumber(),
                            message.getPrepare().getBucket(), logEvent));
                }
                break;
            case PREPARE_OK:
                cm.queueEvent(new PrepareOK(node, viewVersion, message.getPrepareOk().getOpNumber(), message.getPrepareOk().getBucket()));
                break;
            case START_VIEW_CHANGE:
                cm.queueEvent(new StartViewChange(node, viewVersion));
                break;
            case DO_VIEW_CHANGE:
                cm.queueEvent(new DoViewChange(node, viewVersion, getLogFromMessage(message.getDoViewChange().getLog()),
                        SerializationUtils.getVersionFromMesage(message.getDoViewChange().getTerm()), message.getDoViewChange().getCommitNumber()));
                break;
            case START_VIEW:
                cm.queueEvent(new StartView(node, viewVersion, getLogFromMessage(message.getStartView().getLog()), message.getStartView().getCommitNumber()));
                break;
            case GET_STATE:
                cm.queueEvent(new GetState(node, viewVersion, message.getGetState().getOpNumber()));
                break;
            case NEW_STATE:
                cm.queueEvent(new NewState(node, viewVersion, getLogFromMessage(message.getNewState().getLogSegment()), message.getNewState().getOpNumber(), message.getNewState().getCommitNumber()));
                break;
            case LOCAL_DECISION_RESPONSE:
                cm.queueEvent(new LocalDecisionResponse(node, viewVersion, new TransactionID(message.getLocalDecisionResponse().getTxID()), message.getLocalDecisionResponse().getToCommit()));
                break;
            case REQUEST_ROLLBACK_LOCAL_DECISION:
                cm.queueEvent(new RequestRollbackLocalDecision(node, viewVersion, new TransactionID(message.getRequestRollbackLocalDecision())));
                break;
            case ROLLBACK_LOCAL_DECISION_RESPONSE:
                cm.queueEvent(new RollbackLocalDecisionResponse(node, viewVersion, new TransactionID(message.getRollbackLocalDecisionResponse())));
                break;
            case COMMIT_TRANSACTION:
                cm.queueEvent(new CommitTransaction(node, viewVersion, new TransactionID(message.getCommitTransaction())));
                break;
            case ABORT_TRANSACTION:
                cm.queueEvent(new AbortTransaction(node, viewVersion, new TransactionID(message.getAbortTransaction())));
                break;
            case REQUEST_LOCAL_DECISION:
                cm.queueEvent(new RequestLocalDecision(node, viewVersion, new TransactionID(message.getAbortTransaction())));
                break;
            case REQUEST_GLOBAL_DECISION:
                cm.queueEvent(new RequestGlobalDecision(node, viewVersion, new TransactionID(message.getAbortTransaction())));
                break;
            case _NOT_IN_SCHEMA:
            default:
                logger.error("Received an incorrect internal message, ignoring...");
                break;
        }
    }

    private List<LogEntry> getLogFromMessage(StructList.Reader<Internal.LoggedEvent.Reader> logReader) {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 0; i < logReader.size(); i++) {
            LogEvent event = getLogEvent(logReader.get(i).getEvent());
            if (event != null)
                log.add(new LogEntry(event));
        }
        return log;
    }

    private LogEvent getLogEvent(Internal.LogEvent.Reader logReader) {
        LogEvent event = null;
        switch (logReader.which()) {
            case TRANSACTION:
                Transaction tx = new Transaction(new TransactionID(logReader.getTxID()), null,
                        logReader.getTransaction().getTransaction());
                tx.setState(logReader.getTransaction().getPrepared()? TransactionState.PREPARED : TransactionState.ABORTED);
                event = new LogTransaction(tx);
                break;
            case DECISION:
                event = new LogTransactionDecision(new TransactionID(logReader.getTxID()), logReader.getDecision());
                break;
            case ROLLBACK:
                event = new LogRollback(new TransactionID(logReader.getTxID()));
                break;
            case _NOT_IN_SCHEMA:
                logger.error("Received incorrect LoggedEvent, ignoring...");
                break;
        }
        return event;
    }
}
