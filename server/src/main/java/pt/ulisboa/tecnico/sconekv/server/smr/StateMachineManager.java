package pt.ulisboa.tecnico.sconekv.server.smr;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.events.external.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.events.internal.*;

import java.util.*;

public class StateMachineManager {
    enum Status {
        NORMAL,
        VIEW_CHANGE
    }
    private static final Logger logger = LoggerFactory.getLogger(StateMachineManager.class);

    private CommunicationManager cm;
    private MembershipManager mm;
    private Bucket currentBucket;
    private Version term;
    private Version currentVersion;
    private Version futureVersion;
    private Node currentMaster;
    private Status status;

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
        this.status = Status.NORMAL;
        this.commitNumber = -1;
    }

    public Node getCurrentMaster() {
        return currentMaster;
    }

    private int getOpNumber() {
        return log.size() - 1;
    }

    public synchronized void updateBucket(Bucket newBucket, Version newVersion) {
        logger.debug("Update bucket");
        this.currentVersion = newVersion;
        Node newMaster = newBucket.getMaster();
        if (this.currentBucket == null) { // first view
            this.futureVersion = newVersion;
            this.currentMaster = newMaster;
        } else if (newBucket.getId() != this.currentBucket.getId()) {
            this.currentMaster = null;
            this.term = null;
            this.log.clear();
            this.commitNumber = -1;
        }
        this.currentBucket = newBucket;
        if (newVersion.isGreater(this.futureVersion)) {
            this.futureVersion = newVersion;
            this.startViews = 0;
            this.doViews.clear();
        }
        if (newVersion.isEqual(this.futureVersion) && !newMaster.equals(this.currentMaster)) {
            this.status = Status.VIEW_CHANGE;
            MessageBuilder message = CommunicationUtils.generateStartViewChange(mm.getMyself(), currentVersion);
            cm.broadcastBucket(message);
            // send to self
            cm.queueEvent(new StartViewChange(null, mm.getMyself(), currentVersion));
        }
    }

    public synchronized void prepareLogMaster(CommitRequest request) {
        if (status != Status.NORMAL) {
            cm.queueEvent(request);
            logger.info("CommitRequest event {} was not processed as status is {}", request.getTx().getId(), status);
            return;
        }
        logger.debug("Master replicating request...");
        log.add(new LogEntry(request));

        MessageBuilder message = CommunicationUtils.generatePrepare(request.getRequest(), mm.getMyself(), currentVersion,
                currentBucket.getId(), commitNumber, getOpNumber());
        cm.broadcastBucket(message);
    }

    public synchronized void prepareLogReplica(Prepare prepare) {
        if (status != Status.NORMAL) {
            cm.queueEvent(prepare);
            logger.info("Prepare event {} was not processed as status is {}", prepare.getClientRequest().getTx().getId(), status);
            return;
        }
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

        MessageBuilder message = CommunicationUtils.generatePrepareOK(mm.getMyself(), currentVersion, currentBucket.getId(), getOpNumber());
        cm.send(message, currentMaster);
    }

    public synchronized void prepareOK(PrepareOK prepareOK) {
        if (status != Status.NORMAL) {
            cm.queueEvent(prepareOK);
            logger.info("PrepareOK event from {} was not processed as status is {}", prepareOK.getNode(), status);
            return;
        }
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
            this.status = Status.VIEW_CHANGE; // in case it didn't detect in the update bucket
            if (currentBucket.getMaster().equals(mm.getMyself())) {
                // send to self
                cm.queueEvent(new DoViewChange(null, mm.getMyself(), currentVersion, log, term, commitNumber));
            } else {
                MessageBuilder message = CommunicationUtils.generateDoViewChange(mm.getMyself(), currentVersion, commitNumber, log);
                cm.send(message, currentBucket.getMaster());
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
                int oldCommitNumber = this.commitNumber;
                this.commitNumber = doViews.stream().max(Comparator.comparing(DoViewChange::getCommitNumber)).get().getCommitNumber();
                // set master up to date (execute requests that were committed but not here)
                for (int i = oldCommitNumber + 1; i <= commitNumber; i++) {
                    LogEntry entry = log.get(i);
                    entry.getRequest().setPrepared();
                    cm.queueEvent(entry.getRequest());
                }
                MessageBuilder message = CommunicationUtils.generateStartView(mm.getMyself(), commitNumber, log);
                cm.broadcastBucket(message);
            }
        }
    }

    public void startView(StartView event) {
        logger.debug("Received startView");
        this.term = event.getViewVersion();
        this.log = event.getLog();
        this.commitNumber = event.getCommitNumber();
        this.currentMaster = event.getNode();
        this.status = Status.NORMAL;
    }

    //state transfer / recovery
}
