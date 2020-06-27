package pt.ulisboa.tecnico.sconekv.server.communication;

import org.capnproto.MessageBuilder;
import org.capnproto.StructList;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionState;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.LogEvent;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.LogRollback;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.LogTransaction;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.LogTransactionDecision;
import pt.ulisboa.tecnico.sconekv.server.smr.LogEntry;

import java.util.List;

public class CommunicationUtils {
    private CommunicationUtils() {}

    public static MessageBuilder generateReadResponse(TransactionID txID, byte[] key, Value value) {
        MessageBuilder response = new MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        txID.serialize(rBuilder.getTxID());
        External.ReadResponse.Builder builder = rBuilder.initRead();
        builder.setKey(key);
        builder.setValue(value.getContent());
        builder.setVersion(value.getVersion());
        return response;
    }

    public static MessageBuilder generateWriteResponse(TransactionID txID, byte[] key, short version) {
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        txID.serialize(rBuilder.getTxID());
        External.WriteResponse.Builder builder = rBuilder.initWrite();
        builder.setKey(key);
        builder.setVersion(version);
        return response;
    }

    public static MessageBuilder generateDeleteResponse(TransactionID txID, byte[] key, short version) {
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        txID.serialize(rBuilder.getTxID());
        External.DeleteResponse.Builder builder = rBuilder.initDelete();
        builder.setKey(key);
        builder.setVersion(version);
        return response;
    }

    public static MessageBuilder generateCommitResponse(TransactionID txID, boolean wasSuccessful) {
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        txID.serialize(rBuilder.getTxID());
        External.CommitResponse.Builder builder = rBuilder.initCommit();
        if (wasSuccessful)
            builder.setResult(External.CommitResponse.Result.OK);
        else
            builder.setResult(External.CommitResponse.Result.NOK);
        return response;
    }

    public static MessageBuilder generateGetDHTResponse(DHT dht) {
        MessageBuilder response = new org.capnproto.MessageBuilder();
        External.Response.Builder rBuilder = response.initRoot(External.Response.factory);
        dht.serialize(rBuilder.initDht());
        return response;
    }

    public static MessageBuilder generateLocalDecisionResponse(Node sender, Version currentVersion, TransactionID txID, TransactionState state) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.LocalDecisionResponse.Builder builder = mBuilder.initLocalDecisionResponse();
        txID.serialize(builder.getTxID());
        builder.setToCommit(state != TransactionState.ABORTED);
        return message;
    }

    public static MessageBuilder generateCommitTransaction(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initCommitTransaction());
        return message;
    }

    public static MessageBuilder generateAbortTransaction(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initAbortTransaction());
        return message;
    }

    public static MessageBuilder generateRequestRollbackLocalDecision(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initRequestRollbackLocalDecision());
        return message;
    }

    public static MessageBuilder generateRollbackLocalDecisionResponse(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initRollbackLocalDecisionResponse());
        return message;
    }

    public static MessageBuilder generateRequestLocalDecision(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initRequestLocalDecision());
        return message;
    }

    public static MessageBuilder generateRequestGlobalDecision(Node sender, Version currentVersion, TransactionID txID) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        txID.serialize(mBuilder.initRequestGlobalDecision());
        return message;
    }

    public static MessageBuilder generatePrepare(LogEvent event, Node sender, Version currentVersion, short bucketId, int commitNumber, int opNumber) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.Prepare.Builder builder = mBuilder.initPrepare();
        builder.setBucket(bucketId);
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(opNumber);
        Internal.LogEvent.Builder eBuilder = builder.initEvent();
        serializeEvent(event, eBuilder);
        event.setReader(eBuilder.asReader());
        return message;
    }

    public static MessageBuilder generatePrepareOK(Node sender, Version currentVersion, short bucketId, int opNumber) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.PrepareOK.Builder builder = mBuilder.initPrepareOk();
        builder.setBucket(bucketId);
        builder.setOpNumber(opNumber);
        return message;
    }

    public static MessageBuilder generateStartViewChange(Node sender, Version currentVersion) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        mBuilder.setStartViewChange(null);
        return message;
    }

    public static MessageBuilder generateDoViewChange(Node sender, Version currentVersion, int commitNumber, List<LogEntry> log) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.DoViewChange.Builder builder = mBuilder.initDoViewChange();
        builder.setCommitNumber(commitNumber);
        SerializationUtils.serializeViewVersion(builder.getTerm(), currentVersion);
        serializeLog(log, builder.initLog(log.size()));
        return message;
    }

    public static MessageBuilder generateStartView(Node sender, Version currentVersion, int commitNumber, List<LogEntry> log) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.StartView.Builder builder = mBuilder.initStartView();
        builder.setCommitNumber(commitNumber);
        serializeLog(log, builder.initLog(log.size()));
        return message;
    }

    public static MessageBuilder generateGetState(Node sender, Version currentVersion, int opNumber) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.GetState.Builder builder = mBuilder.initGetState();
        builder.setOpNumber(opNumber);
        return message;
    }

    public static MessageBuilder generateNewState(Node sender, Version currentVersion, int opNumber, int commitNumber, List<LogEntry> logSegment) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.NewState.Builder builder = mBuilder.initNewState();
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(opNumber);
        serializeLog(logSegment, builder.initLogSegment(logSegment.size()));
        return message;
    }

    private static void serializeLog(List<LogEntry> log, StructList.Builder<Internal.LoggedEvent.Builder> logBuilder) {
        for (int i = 0; i < log.size(); i++) {
            LogEvent entry = log.get(i).getEvent();
            if (entry.getReader() != null) {
                logBuilder.get(i).setEvent(entry.getReader());
            } else {
                serializeEvent(entry, logBuilder.get(i).initEvent());
            }
        }
    }

    private static void serializeEvent(LogEvent event, Internal.LogEvent.Builder eBuilder) {
        event.getTxID().serialize(eBuilder.getTxID());
        if (event instanceof LogTransaction) {
            LogTransaction logTransaction = (LogTransaction) event;
            Internal.LoggedTransaction.Builder tBuilder = eBuilder.initTransaction();
            tBuilder.setTransaction(logTransaction.getTx().getReader());
            tBuilder.setPrepared(logTransaction.getTx().getState() != TransactionState.ABORTED);
        } else if (event instanceof  LogTransactionDecision){
            eBuilder.setDecision(((LogTransactionDecision) event).getDecision() == TransactionState.COMMITTED);
        } else if (event instanceof LogRollback) {
            eBuilder.setRollback(null);
        }
    }

}
