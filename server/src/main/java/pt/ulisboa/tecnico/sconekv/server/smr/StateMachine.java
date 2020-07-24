package pt.ulisboa.tecnico.sconekv.server.smr;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationUtils;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.internal.*;
import pt.ulisboa.tecnico.sconekv.server.events.internal.smr.*;
import pt.ulisboa.tecnico.sconekv.server.events.local.CheckPendingTransactions;
import pt.ulisboa.tecnico.sconekv.server.exceptions.SMRStatusException;

import java.util.*;

public class StateMachine {
    private static final Logger logger = LoggerFactory.getLogger(StateMachine.class);

    enum Status {
        NORMAL,
        VIEW_CHANGE,
        MASTER_AFTER_VIEW_CHANGE
    }

    private CommunicationManager cm;
    private MembershipManager mm;
    private Bucket currentBucket;
    private Version currentVersion;
    private Version futureVersion;
    private Version term;
    private Version lastDoView;
    private Node currentMaster;
    private Status status;
    private List<LogEntry> log;
    private int commitNumber;
    private int startViews;
    private List<DoViewChange> doViews;
    private Map<Integer, Prepare> pendingEntries; // Map opNumber -> Prepare
    private List<SconeEvent> pendingEvents;

    public StateMachine(CommunicationManager cm, MembershipManager mm) {
        this.cm = cm;
        this.mm = mm;
        this.log = new ArrayList<>();
        this.pendingEntries = new HashMap<>();
        this.pendingEvents = new ArrayList<>();
        this.doViews = new ArrayList<>();
        this.status = Status.NORMAL;
        this.commitNumber = -1;
    }

    public Node getCurrentMaster() {
        return currentMaster;
    }

    public short getCurrentBucketId() {
        return currentBucket.getId();
    }

    public boolean isMaster() {
        return mm.getMyself().equals(currentMaster);
    }

    public Version getCurrentVersion() {
        return currentVersion;
    }

    private int getOpNumber() {
        return log.size() - 1;
    }

    public synchronized void updateBucket(Bucket newBucket, Version newVersion, short workerId) {
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
            setStatus(Status.VIEW_CHANGE);
            MessageBuilder message = CommunicationUtils.generateStartViewChange(mm.getMyself(), currentVersion);
            cm.broadcastBucket(message, workerId);
            logger.debug("Broadcasted startViewChange for {}", currentVersion);
        }
    }

    public synchronized void prepareLogMaster(LogEvent event, SconeEvent cause, short workerId) throws SMRStatusException {
        if (status != Status.NORMAL) {
            this.pendingEvents.add(cause);
            logger.info("LogEvent {} was not processed as status is {}", event.getTxID(), status);
            throw new SMRStatusException();
        }
        if (!currentMaster.equals(mm.getMyself())) {
            logger.error("PrepareLog but I am not the master, ignoring...");
            throw new SMRStatusException();
        }
        log.add(new LogEntry(event));

        MessageBuilder message = CommunicationUtils.generatePrepare(event, mm.getMyself(), currentVersion,
                currentBucket.getId(), commitNumber, getOpNumber());
        cm.broadcastBucket(message, workerId);
        logger.debug("Master replicated {}", getOpNumber());
    }

    public synchronized void prepareLogReplica(Prepare prepare, short workerId) {
        if (status != Status.NORMAL) {
            this.pendingEvents.add(prepare);
            logger.info("Prepare event {} was not processed as status is {}", prepare.getEvent().getTxID(), status);
            return;
        }
        logger.debug("Replica received prepare message op:{} commit:{}", prepare.getOpNumber(), prepare.getCommitNumber());

        if (prepare.getBucket() != currentBucket.getId() || !prepare.getNode().equals(currentMaster)) {
            logger.error("Received incorrect prepare request, ignoring");
            return;
        }

        if (this.log.size() == prepare.getOpNumber()) {
            this.log.add(new LogEntry(prepare.getEvent()));

            // if the next entry is pending, queue it
            if (pendingEntries.containsKey(this.log.size())) {
                cm.queueEvent(pendingEntries.remove(this.log.size()));
            }

            // could save the queued opNumber and only send prepareOK to the highest value to reduce traffic

            // commit all entries with opNumber <= prepare.commitNumber
            for (int i = commitNumber + 1; i <= prepare.getCommitNumber(); i++) {
                LogEntry entry = log.get(i);
                cm.queueEvent(entry.getEvent());
            }
            this.commitNumber = prepare.getCommitNumber();

            MessageBuilder message = CommunicationUtils.generatePrepareOK(mm.getMyself(), currentVersion, currentBucket.getId(), getOpNumber());
            cm.send(message, currentMaster, workerId);
            logger.debug("Replica sent prepareOK {}", getOpNumber());

        } else if (this.log.size() < prepare.getOpNumber()) {// if this entry is not the immediately consecutive in the log, wait
            logger.error("Received {} but am on {}", prepare.getOpNumber(), getOpNumber());
            this.pendingEntries.put(prepare.getOpNumber(), prepare);
            if (prepare.getOpNumber() - this.getOpNumber() >= SconeConstants.MAX_OP_NUMBER_HOLE) {
                logger.info("Detected op number hole, requesting state update");
                MessageBuilder message = CommunicationUtils.generateGetState(mm.getMyself(), this.currentVersion, getOpNumber());
                cm.send(message, currentMaster, workerId);
            }

        } else {
            logger.error("Received old entry {}, I am on {}", prepare.getOpNumber(), getOpNumber());
        }
    }

    public synchronized void prepareOK(PrepareOK prepareOK) {
        if (status == Status.VIEW_CHANGE) {
            this.pendingEvents.add(prepareOK);
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
                cm.queueEvent(entry.getEvent());
            }
        }

        if (this.status == Status.MASTER_AFTER_VIEW_CHANGE && (this.commitNumber == -1 || this.commitNumber == getOpNumber())) {
            cm.queueEvent(new CheckPendingTransactions());
            setStatus(Status.NORMAL);
        }
    }

    public synchronized void startViewChange(StartViewChange event, short workerId) {
        logger.debug("Received startViewChange from {}", event.getNode().getAddress().getHostAddress());
        if (event.getViewVersion().isGreater(this.futureVersion)) {
            logger.debug("StartViewChange is for new futureVersion {}", event.getViewVersion());
            this.futureVersion = event.getViewVersion();
            this.startViews = 1;
            this.doViews.clear();
        } else if (event.getViewVersion().equals(this.futureVersion)) {
            this.startViews++;
        }

        if (event.getViewVersion().equals(this.currentVersion) && startViews >= SconeConstants.FAILURES_PER_BUCKET
                && !this.currentVersion.equals(this.lastDoView)) { // avoid sending duplicate doViews
            setStatus(Status.VIEW_CHANGE); // in case it didn't detect in the update bucket
            this.lastDoView = new Version(this.currentVersion);
            if (currentBucket.getMaster().equals(mm.getMyself())) {
                // send to self
                cm.queueEvent(new DoViewChange(mm.getMyself(), currentVersion, log, term, commitNumber));
                logger.debug("Sent doView to myself");
            } else {
                MessageBuilder message = CommunicationUtils.generateDoViewChange(mm.getMyself(), currentVersion, commitNumber, log);
                cm.send(message, currentBucket.getMaster(), workerId);
                logger.debug("Sent doView to {}", currentBucket.getMaster().getAddress().getHostAddress());
            }
        }
    }

    public synchronized void doViewChange(DoViewChange event, short workerId) {
        logger.debug("Received doViewChange from {}", event.getNode().getAddress().getHostAddress());
        if (event.getViewVersion().isGreater(this.futureVersion)) {
            this.futureVersion = event.getViewVersion();
            this.startViews = 0;
            this.doViews.clear();
            this.doViews.add(event);
        } else if (event.getViewVersion().equals(this.futureVersion) && !event.getViewVersion().equals(term)) { // ignore old doViewChange i.e. after it is already done
            this.doViews.add(event);

            if (event.getViewVersion().equals(this.currentVersion) && doViews.size() >= SconeConstants.FAILURES_PER_BUCKET + 1) {
                this.currentMaster = mm.getMyself();
                this.term = new Version(this.currentVersion);
                //Selects as the new log the one with the largest term (largest opNum in case of tie).
                this.log = doViews.stream().max(Comparator.comparing(InternalEvent::getViewVersion)).get().getLog();
                //Sets the commitNum to the largest one received.
                int oldCommitNumber = this.commitNumber;
                this.commitNumber = doViews.stream().max(Comparator.comparing(DoViewChange::getCommitNumber)).get().getCommitNumber();
                // set master up to date (execute requests that were committed but not here)
                for (int i = oldCommitNumber + 1; i <= commitNumber; i++) {
                    LogEntry entry = log.get(i);
                    cm.queueEvent(entry.getEvent());
                }
                MessageBuilder message = CommunicationUtils.generateStartView(mm.getMyself(), currentVersion, commitNumber, log);
                cm.broadcastBucket(message, workerId);
                setStatus(Status.MASTER_AFTER_VIEW_CHANGE);
            }
        }
    }

    public synchronized void startView(StartView event, short workerId) {
        logger.debug("Received startView from {}", event.getNode().getAddress().getHostAddress());
        this.term = event.getViewVersion();
        this.log = event.getLog();
        for (int i = this.commitNumber + 1; i <= event.getCommitNumber(); i++) {
            LogEntry entry = log.get(i);
            cm.queueEvent(entry.getEvent());
        }
        this.commitNumber = event.getCommitNumber();
        this.currentMaster = event.getNode();
        MessageBuilder message = CommunicationUtils.generatePrepareOK(mm.getMyself(), currentVersion, currentBucket.getId(), getOpNumber());
        cm.send(message, currentMaster, workerId);
        setStatus(Status.NORMAL);
    }

    private void setStatus(Status status) {
        this.status = status;
        if (this.status == Status.NORMAL) {
            for (SconeEvent e : pendingEvents) {
                cm.queueEvent(e);
            }
            pendingEvents.clear();
            logger.info("Normal status, accepting requests");
        } else if (this.status == Status.VIEW_CHANGE) {
            logger.info("View-change status, delaying requests");
        } else if (this.status == Status.MASTER_AFTER_VIEW_CHANGE) {
            logger.info("New master after view-change status, awaiting prepareOks");
        }
    }

    public synchronized void getState(GetState event, short workerId) {
        logger.debug("Received getState");
        if (status != Status.NORMAL) {
            this.pendingEvents.add(event);
            logger.info("GetState event from {} was not processed as status is {}", event.getNode(), this.status);
            return;
        }
        if (currentVersion.equals(event.getViewVersion())) {
            MessageBuilder message = CommunicationUtils.generateNewState(mm.getMyself(), this.currentVersion, getOpNumber(), this.commitNumber, this.log.subList(Math.max(event.getOpNumber(),0), log.size()));
            cm.send(message, event.getNode(), workerId);
            logger.info("Responded to GetState from {}", event.getNode());
        } else {
            logger.info("Received GetState with version {} but I am on {} so I am not responding", event.getViewVersion(), this.currentVersion);
        }
    }

    public synchronized void newState(NewState event) {
        logger.debug("Received newState");
        if (event.getViewVersion().equals(this.currentVersion)) {
            // newState might be slightly outdated, should ignore those entries
            int missingEntriesCount = Math.max(event.getOpNumber() - getOpNumber(), 0); // avoid invalidIndex
            List<LogEntry> missingEntries = event.getLogSegment()
                    .subList(Math.max(event.getLogSegment().size() - missingEntriesCount,0), event.getLogSegment().size());
            log.addAll(missingEntries);
            for (int i = this.commitNumber + 1; i <= event.getCommitNumber(); i++) {
                LogEntry entry = log.get(i);
                cm.queueEvent(entry.getEvent());
            }
            this.commitNumber = event.getCommitNumber();
            // remove pending entries that were added to the log with this update
            TreeSet<Integer> pendingOpNumbers = new TreeSet<>(pendingEntries.keySet());
            pendingOpNumbers.tailSet(getOpNumber()).clear();
            for (Integer entryKey: pendingOpNumbers) {
                pendingEntries.remove(entryKey);
            }
        } else if (event.getViewVersion().isGreater(this.currentVersion)) {
            logger.info("Received newState from future view version, delaying...");
            pendingEvents.add(event);
        } else {
            logger.error("Received newState from older view version, discarding...");
        }
    }

}
