package pt.ulisboa.tecnico.sconekv.server.communication;

import org.capnproto.MessageBuilder;
import org.capnproto.StructList;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.Value;
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

    public static MessageBuilder generatePrepare(External.Request.Reader request, Node sender, Version currentVersion, short bucketId, int commitNumber, int opNumber) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.Prepare.Builder builder = mBuilder.initPrepare();
        builder.setMessage(request);
        builder.setBucket(bucketId);
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(opNumber);
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

    public static MessageBuilder generateStartView(Node sender, int commitNumber, List<LogEntry> log) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        Internal.StartView.Builder builder = mBuilder.initStartView();
        builder.setCommitNumber(commitNumber);
        serializeLog(log, builder.initLog(log.size()));
        return message;
    }

    public static MessageBuilder generateGetState(Node sender, int opNumber) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        Internal.GetState.Builder builder = mBuilder.initGetState();
        builder.setOpNumber(opNumber);
        return message;
    }

    public static MessageBuilder generateNewState(Node sender, int opNumber, int commitNumber, List<LogEntry> logSegment) {
        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), sender);
        Internal.NewState.Builder builder = mBuilder.initNewState();
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(opNumber);
        serializeLog(logSegment, builder.initLogSegment(logSegment.size()));
        return message;
    }

    private static void serializeLog(List<LogEntry> log, StructList.Builder<Internal.LoggedRequest.Builder> logBuilder) {
        for (int i = 0; i < log.size(); i++) {
            logBuilder.get(i).setRequest(log.get(i).getRequest().getRequest());
        }
    }
}
