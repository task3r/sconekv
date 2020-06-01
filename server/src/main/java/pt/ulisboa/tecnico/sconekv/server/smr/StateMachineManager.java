package pt.ulisboa.tecnico.sconekv.server.smr;

import org.capnproto.MessageBuilder;
import org.capnproto.StructList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.transport.Internal;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.*;

import java.io.IOException;
import java.util.*;

public class StateMachineManager {
    private static final Logger logger = LoggerFactory.getLogger(StateMachineManager.class);

    private CommunicationManager cm;
    private MembershipManager mm;
    private Bucket currentBucket;
    private Version term;
    private Version currentVersion;
    private Version futureVersion;
    private Node currentMaster;

    private int commitNumber;

    private int startViews;
    private List<DoViewChange> doViews;
    //status

    private List<LogEntry> log;
    // Map opNumber -> Prepare
    private Map<Integer, Prepare> pendingEntries;

    public StateMachineManager(CommunicationManager cm, MembershipManager mm) {
        this.cm = cm;
        this.mm = mm;
        this.log = new ArrayList<>();
        this.pendingEntries = new HashMap<>();
        this.doViews = new ArrayList<>();
    }

    public Node getCurrentMaster() {
        return currentMaster;
    }

    public synchronized void prepareLogMaster(CommitRequest request) {
        logger.debug("Master replicating request...");
        log.add(new LogEntry(request));

        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.Prepare.Builder builder = mBuilder.initPrepare();
        builder.setMessage(request.getRequest());
        builder.setBucket(currentBucket.getId());
        builder.setCommitNumber(commitNumber);
        builder.setOpNumber(log.size() - 1);
        try {
            cm.broadcastBucket(SerializationUtils.getBytesFromMessage(message));
        } catch (IOException e) {
            logger.error("Error serializing prepare message");
            e.printStackTrace();
        }
    }

    public synchronized void prepareLogReplica(Prepare prepare) {
        logger.debug("Replica received prepare message");

        if (prepare.getBucket() != currentBucket.getId() || !prepare.getNode().equals(currentMaster)) {
            logger.error("Received incorrect prepare request, ignoring");
            return;
        }

        // if this entry is not the immediately consecutive in the log, wait
        if (this.log.size() != prepare.getOpNumber()) {
            this.pendingEntries.put(prepare.getOpNumber(), prepare);
            return;
        }
        this.log.add(new LogEntry(prepare.getClientRequest()));

        // if the next entry is pending, queue it
        if (pendingEntries.containsKey(this.log.size()))
            cm.queueEvent(pendingEntries.remove(this.log.size()));

        // could save the queued opNumber and only send prepareOK to the highest value to reduce traffic

        // commit all entries with opNumber <= prepare.commitNumber
        for (int i = commitNumber + 1; i <= prepare.getCommitNumber(); i++) {
            LogEntry entry = log.get(i);
            entry.getRequest().setPrepared();
            cm.queueEvent(entry.getRequest());
        }
        this.commitNumber = prepare.getCommitNumber();

        MessageBuilder message = new MessageBuilder();
        Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
        SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
        SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
        Internal.PrepareOK.Builder builder = mBuilder.initPrepareOk();
        builder.setBucket(currentBucket.getId());
        builder.setOpNumber(this.log.size() - 1);
        try {
            cm.sendMaster(SerializationUtils.getBytesFromMessage(message));
        } catch (IOException e) {
            logger.error("Error serializing prepareOK message");
            e.printStackTrace();
        }
    }

    public synchronized void prepareOK(PrepareOK prepareOK) {
        logger.debug("Master received prepareOK from {} with opNum: {}", prepareOK.getNode(), prepareOK.getOpNumber());

        if (!currentMaster.equals(mm.getMyself()) || prepareOK.getBucket() != currentBucket.getId()) {
            logger.error("Received incorrect prepareOK request, ignoring");
            return;
        }

        for (int i = commitNumber + 1; i <= prepareOK.getOpNumber(); i++) {
            LogEntry entry = log.get(i);
            entry.addOk(prepareOK.getNode());
            if (entry.isReady()) {
                commitNumber = i;
                entry.getRequest().setPrepared();
                cm.queueEvent(entry.getRequest());
            }
        }
    }

    public synchronized void updateBucket(Bucket newBucket, Version newVersion) {
        logger.debug("Update bucket");
        this.currentVersion = newVersion;
        if (!newBucket.equals(this.currentBucket)) {
            this.currentBucket = newBucket;
            this.currentMaster = null;
            this.term = null;
            this.log.clear();
            this.commitNumber = 0;
        }
        if (newVersion.isGreater(this.futureVersion)) {
            this.futureVersion = newVersion;
            this.startViews = 0;
            this.doViews.clear();
        }
        Node newMaster = currentBucket.getMaster();
        if (newVersion.isEqual(this.futureVersion) && !newMaster.equals(this.currentMaster)) {
            MessageBuilder message = new MessageBuilder();
            Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
            SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
            SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
            mBuilder.setStartViewChange(null);
            try {
                cm.broadcastBucket(SerializationUtils.getBytesFromMessage(message));
            } catch (IOException e) {
                logger.error("Error serializing startViewChange message");
                e.printStackTrace();
            }
            // send to self
            cm.queueEvent(new StartViewChange(null, mm.getMyself(), currentVersion));
        }
    }

    public synchronized void startViewChange(StartViewChange event) {
        logger.debug("Received startViewChange");
        if (event.getViewVersion().isGreater(this.futureVersion)) {
            this.futureVersion = event.getViewVersion();
            this.startViews = 1;
            this.doViews.clear();
        } else if (event.getViewVersion().equals(this.futureVersion)) {
            this.startViews++;
        }

        if (event.getViewVersion().equals(this.currentVersion) && startViews == SconeConstants.FAILURES_PER_BUCKET + 1) {
            if (currentBucket.getMaster().equals(mm.getMyself())) {
                // send to self
                cm.queueEvent(new DoViewChange(null, mm.getMyself(), currentVersion, log, term, commitNumber));
            } else {
                MessageBuilder message = new MessageBuilder();
                Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
                SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
                SerializationUtils.serializeViewVersion(mBuilder.getViewVersion(), currentVersion);
                Internal.DoViewChange.Builder builder = mBuilder.initDoViewChange();
                builder.setCommitNumber(this.commitNumber);
                SerializationUtils.serializeViewVersion(builder.getTerm(), currentVersion);
                StructList.Builder<Internal.LoggedRequest.Builder> logBuilder = builder.initLog(log.size());
                for (int i = 0; i < log.size(); i++) {
                    logBuilder.get(i).setRequest(log.get(i).getRequest().getRequest());
                }
                try {
                    cm.sendMaster(SerializationUtils.getBytesFromMessage(message));
                } catch (IOException e) {
                    logger.error("Error serializing doViewChange message");
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized void doViewChange(DoViewChange event) {
        logger.debug("Received doViewChange");
        if (event.getViewVersion().isGreater(this.futureVersion)) {
            this.futureVersion = event.getViewVersion();
            this.startViews = 0;
            this.doViews.clear();
            this.doViews.add(event);
        } else if (event.getViewVersion().equals(this.futureVersion)) {
            this.doViews.add(event);

            if (event.getViewVersion().equals(this.currentVersion) && doViews.size() == SconeConstants.FAILURES_PER_BUCKET + 1) {
                this.currentMaster = mm.getMyself();
                this.term = new Version(this.currentVersion);
                //Selects as the new log the one with the largest term (largest opNum in case of tie).
                this.log = doViews.stream().max(Comparator.comparing(InternalMessage::getViewVersion)).get().getLog();
                //Sets the commitNum to the largest one received.
                this.commitNumber = doViews.stream().max(Comparator.comparing(DoViewChange::getCommitNumber)).get().getCommitNumber();
                MessageBuilder message = new MessageBuilder();
                Internal.InternalMessage.Builder mBuilder = message.initRoot(Internal.InternalMessage.factory);
                SerializationUtils.serializeNode(mBuilder.getNode(), mm.getMyself());
                Internal.StartView.Builder builder = mBuilder.initStartView();
                builder.setCommitNumber(this.commitNumber);
                StructList.Builder<Internal.LoggedRequest.Builder> logBuilder = builder.initLog(log.size());
                for (int i = 0; i < log.size(); i++) {
                    logBuilder.get(i).setRequest(log.get(i).getRequest().getRequest());
                }
                try {
                    cm.broadcastBucket(SerializationUtils.getBytesFromMessage(message));
                } catch (IOException e) {
                    logger.error("Error serializing startViewChange message");
                    e.printStackTrace();
                }
                cm.queueEvent(new StartView(null, mm.getMyself(), currentVersion, log, commitNumber));
            }
        }
    }

    public void startView(StartView event) {
        logger.debug("Received startView");
        this.term = event.getViewVersion();
        this.log = event.getLog();
        this.commitNumber = event.getCommitNumber();
        this.currentMaster = event.getNode();
    }

    //state transfer / recovery
}
