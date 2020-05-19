package pt.ulisboa.tecnico.sconekv.server.smr;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.Prepare;
import pt.ulisboa.tecnico.sconekv.server.events.internal.PrepareOK;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StateMachineManager {
    private static final Logger logger = LoggerFactory.getLogger(StateMachineManager.class);

    private static StateMachineManager instance;

    private CommunicationManager cm;
    private MembershipManager mm;

    private long opNumber;
    private long commitNumber;

    private Map<Long, LogEntry> log;

    private StateMachineManager(CommunicationManager cm, MembershipManager mm) {
        this.cm = cm;
        this.mm = mm;
        this.log = new HashMap<>();
    }

    public static StateMachineManager getInstance() {
        if (instance == null) {
            logger.error("State machine was not initialized");
            return null;
        }
        return instance;
    }

    public static void init(CommunicationManager cm, MembershipManager mm) {
        if (instance == null)
            instance = new StateMachineManager(cm, mm);
    }

    private synchronized long newOpNumber() {
        return opNumber++;
    }

    public void prepareLogMaster(CommitRequest request) {
        logger.debug("Master logging request...");
        long requestOpNumber = newOpNumber();
        log.put(requestOpNumber, new LogEntry(request, opNumber));

        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
        Internal.Prepare.Builder builder = mBuilder.initPrepare();
        builder.setMessage(request.getRequest());
        builder.setBucket((short)0); // FIXME
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(requestOpNumber);
        try {
            cm.sendPrepare(SerializationUtils.getBytesFromMessage(message));
        } catch (IOException e) {
            logger.error("Error serializing prepare message");
            e.printStackTrace();
        }
    }

    public void prepareLogReplica(Prepare prepare) {
        // garantir que vem do master
        // garantir que tem as entries anteriores
        // fazer commit das entries com opNumber <= prepare.commitNumber
        this.opNumber = prepare.getOpNumber();
        this.commitNumber = prepare.getCommitNumber();

        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
        Internal.PrepareOK.Builder builder = mBuilder.initPrepareOk();
        builder.setBucket((short)0); // FIXME
        builder.setOpNumber(this.opNumber);
        try {
            cm.sendPrepareOK(SerializationUtils.getBytesFromMessage(message));
        } catch (IOException e) {
            logger.error("Error serializing prepareOK message");
            e.printStackTrace();
        }
        cm.queueEvent(prepare.getClientRequest()); //FIXME
    }

    public void prepareOK(PrepareOK prepareOK) {
        // in for loop from committed to prepareOk.opNum
        LogEntry entry = log.get(prepareOK.getOpNumber());
        entry.addOk(prepareOK.getNode());
        if (entry.isReady()) {
            entry.getRequest().setPrepared();
            cm.queueEvent(entry.getRequest());
        }
    }


}
