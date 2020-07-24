package pt.ulisboa.tecnico.sconekv.server.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.UpdateViewCallback;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.events.local.UpdateView;
import pt.ulisboa.tecnico.sconekv.server.smr.StateMachine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class SconeManager implements UpdateViewCallback {
    private static final Logger logger = LoggerFactory.getLogger(SconeManager.class);

    private CommunicationManager communicationManager;
    private MembershipManager membershipManager;
    private StateMachine stateMachine;
    private Store store;
    private DHT dht;
    private List<Thread> threads;

    public SconeManager() throws IOException, InterruptedException {
        this.store = new Store();
        joinMembership();
        this.communicationManager = new CommunicationManager(membershipManager.getMyself());
        this.stateMachine = new StateMachine(communicationManager, membershipManager);
    }

    private void joinMembership() throws IOException, InterruptedException {
        membershipManager = new MembershipManager(this);
        if (!membershipManager.isFirstNode()) {
            int sleepMs = ThreadLocalRandom.current().nextInt(20000);
            logger.info("[{}] - Sleeping {} ms", MembershipManager.myself, sleepMs);
            Thread.sleep(sleepMs);
        } else {
            logger.info("[{}] - Not sleeping", MembershipManager.myself);
        }
        membershipManager.join();
    }

    private void start() {
        threads = new ArrayList<>();
        for (short i = 0; i < SconeConstants.NUM_WORKERS; i++) {
            threads.add(new Thread(new SconeWorker(i, communicationManager, stateMachine, store, dht, membershipManager.getMyself())));
        }
        threads.add(new Thread(new SconeServer((short)0, communicationManager)));
        for (Thread t : threads) {
            t.start();
        }
        logger.info("Scone Node ready.");
    }

    public void shutdown() throws InterruptedException {
        logger.info("Shutdown handler");

        if (membershipManager != null) {
            membershipManager.leave();
        }

        if (communicationManager != null) {
            communicationManager.shutdown();
        }

        if (threads != null) {
            for (Thread t : threads) {
                t.interrupt();
                t.join();
            }
            logger.info("Scone threads terminated.");
        }

        logger.info("Scone node terminated.");
    }

    @Override
    public void onUpdateView(Ring ring) {
        logger.debug("New view! {}", ring);
        if (dht == null && ring.size() >= SconeConstants.BOOTSTRAP_NODE_NUMBER) {
            logger.debug("Constructing DHT...");
            dht = new DHT(ring, SconeConstants.NUM_BUCKETS, SconeConstants.MURMUR3_SEED);
            Bucket currentBucket = dht.getBucketOfNode(membershipManager.getMyself());
            logger.info("Belong to bucket {}, master: {}", currentBucket.getId(),
                    membershipManager.getMyself().equals(currentBucket.getMaster()) ? "myself" : currentBucket.getMaster());
            communicationManager.updateBucket(currentBucket);
            stateMachine.updateBucket(currentBucket, ring.getVersion(), (short) 0);
            start();
        } else if (dht != null) {
            // transformed into an event because of the possible need to send messages
            // thus it needing to be a worker thread that as a socket for communication
            // (zmq does not like different threads using the same socket, even if synchronized)
            communicationManager.queueEvent(new UpdateView(null, ring));
        }
    }

    @Override
    public void onWrongLeave() {
        logger.debug("I was wrongly removed!");
        System.exit(-1); // maybe this should be a normal termination
    }
}
