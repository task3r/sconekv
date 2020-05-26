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

    private CommunicationManager cm;
    private MembershipManager mm;

    private long opNumber;
    private long commitNumber;

    // Map opNumber -> logEntry
    private Map<Long, LogEntry> log; // maybe could change this map simply to a list
    // Map opNumber -> Prepare
    private Map<Long, Prepare> pendingEntries;

    public StateMachineManager(CommunicationManager cm, MembershipManager mm) {
        this.cm = cm;
        this.mm = mm;
        this.log = new HashMap<>();
        this.pendingEntries = new HashMap<>();
    }

    private synchronized long newOpNumber() {
        opNumber++;
        return opNumber;
    }

    public void prepareLogMaster(CommitRequest request) {
        logger.debug("Master replicating request...");
        long requestOpNumber = newOpNumber();
        log.put(requestOpNumber, new LogEntry(request));

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

    public synchronized void prepareLogReplica(Prepare prepare) {
        logger.debug("Replica received prepare message");
        // guarantee it comes from the master of the bucket

        // if this replica does not have all earlier entries, wait
        if (opNumber + 1 != prepare.getOpNumber()) {
            pendingEntries.put(prepare.getOpNumber(), prepare);
            return;
        }
        this.opNumber = prepare.getOpNumber();
        log.put(this.opNumber, new LogEntry(prepare.getClientRequest()));

        // if the next entry is pending, queue it
        if (pendingEntries.containsKey(this.opNumber + 1))
            cm.queueEvent(pendingEntries.remove(this.opNumber + 1));
        // TODO could save the queued opNumber and only send prepareOK to the highest value to reduce traffic

        // commit all entries with opNumber <= prepare.commitNumber
        for (long i = commitNumber + 1; i <= prepare.getCommitNumber(); i++) {
            LogEntry entry = log.get(i);
            entry.getRequest().setPrepared();
            cm.queueEvent(entry.getRequest());
        }
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
    }

    public synchronized void prepareOK(PrepareOK prepareOK) {
        logger.debug("Master received prepareOK from {} with opNum: {}", prepareOK.getNode(), prepareOK.getOpNumber());
        // maybe check bucket first
        for (long i = commitNumber + 1; i <= prepareOK.getOpNumber(); i++) {
            LogEntry entry = log.get(i);
            entry.addOk(prepareOK.getNode());
            if (entry.isReady()) {
                commitNumber = i;
                entry.getRequest().setPrepared();
                cm.queueEvent(entry.getRequest());
            }
        }
    }

    //state transfer / recovery
}
