package pt.ulisboa.tecnico.sconekv.server.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.UpdateViewCallback;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.communication.CommunicationManager;
import pt.ulisboa.tecnico.sconekv.server.db.Store;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class SconeManager implements UpdateViewCallback {
    private static final Logger logger = LoggerFactory.getLogger(SconeManager.class);

    private CommunicationManager communicationManager;
    private MembershipManager membershipManager;
    private Store store;
    private DHT dht;
    private Thread worker;
    private Thread server;

    public SconeManager() throws IOException, InterruptedException {
        this.store = new Store();
        this.communicationManager = new CommunicationManager();

        //joinMembership();

        start();
    }

    private void joinMembership() throws IOException, InterruptedException {
        membershipManager = new MembershipManager(this);
        if (!membershipManager.isFirstNode()) {
            int sleepMs = ThreadLocalRandom.current().nextInt(15000);
            logger.info("[{}] - Sleeping {} ms", MembershipManager.myself, sleepMs);
            Thread.sleep(sleepMs);
        } else {
            logger.info("[{}] - Not sleeping", MembershipManager.myself);
        }
        membershipManager.join();
    }

    private void start() {
        logger.info("Scone Node starting...");
        server = new Thread(new SconeServer((short)0, communicationManager));
        worker = new Thread(new SconeWorker((short)1, communicationManager, store));
        server.start();
        worker.start();
    }

    public void shutdown() throws InterruptedException {
        logger.info("Shutdown handler");

        if (membershipManager != null) {
            logger.debug("before mm leave");
            membershipManager.leave();
        }
        if (worker != null) {
            logger.debug("before mm worker interrupt");
            worker.interrupt();
            worker.join();
        }
        if (server != null) {
            logger.debug("before mm server interrupt");
            server.interrupt();
            server.join();
        }

        communicationManager.shutdown();
    }

    @Override
    public void onUpdateView(Ring ring) {
        logger.debug("New view! {}", ring);
        if (this.dht == null & ring.size() >= SconeConstants.BOOTSTRAP_NODE_NUMBER) {
            logger.debug("Constructing DHT");
            this.dht = new DHT(ring, SconeConstants.NUM_BUCKETS, SconeConstants.MURMUR3_SEED);
            start();
        } else if (this.dht != null) {
            this.dht.applyView(ring);
        }
    }

    @Override
    public void onWrongLeave() {
        logger.debug("I was wrongly removed!");
        System.exit(-1); // maybe this should be a normal termination
    }
}
