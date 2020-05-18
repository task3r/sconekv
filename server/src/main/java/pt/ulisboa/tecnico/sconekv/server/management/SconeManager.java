package pt.ulisboa.tecnico.sconekv.server.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.tecnico.ulisboa.prime.MembershipManager;
import pt.tecnico.ulisboa.prime.UpdateViewCallback;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.db.Store;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class SconeManager implements UpdateViewCallback {
    private static final Logger logger = LoggerFactory.getLogger(SconeManager.class);

    MembershipManager membershipManager;
    ZContext context;
    ZMQ.Socket clientRequestSocket;
    ZMQ.Socket internalCommSocket;
    ZMQ.Poller poller;
    Store store;
    DHT dht;
    BlockingQueue<SconeEvent> eventQueue;
    Thread worker;
    Thread server;

    public SconeManager() throws IOException, InterruptedException {
        this.context = new ZContext();
        this.store = new Store();
        this.eventQueue = new LinkedBlockingQueue<>();

        joinMembership();

        initSockets();
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

    private void initSockets() {
        this.clientRequestSocket = context.createSocket(SocketType.ROUTER);
        this.clientRequestSocket.bind("tcp://*:" + SconeConstants.SERVER_REQUEST_PORT);

        this.internalCommSocket = context.createSocket(SocketType.ROUTER);
        this.internalCommSocket.bind("tcp://*:" + SconeConstants.SERVER_INTERNAL_PORT);

        poller = context.createPoller(2);
        poller.register(clientRequestSocket, ZMQ.Poller.POLLIN);
        poller.register(internalCommSocket, ZMQ.Poller.POLLIN);
    }

    private void start() {
        logger.info("Scone Node starting...");
        server = new Thread(new SconeServer((short)0, clientRequestSocket, internalCommSocket, poller, eventQueue));
        worker = new Thread(new SconeWorker((short)1, clientRequestSocket, store, eventQueue));
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
        context.destroy(); // FIXME this is not enough but alternatives also raised exceptions
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
