package pt.ulisboa.tecnico.sconekv.server.communication;

import org.capnproto.MessageBuilder;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.external.ClientRequest;
import zmq.ZError;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CommunicationManager {
    private static final Logger logger = LoggerFactory.getLogger(CommunicationManager.class);

    private Bucket currentBucket;
    private Node self;
    private ZContext context;
    private ZMQ.Socket clientRequestSocket;
    private ZMQ.Socket internalCommSocket;
    private ZMQ.Poller poller;
    private Map<Node, ZMQ.Socket> bucketSockets;
    private BlockingQueue<SconeEvent> eventQueue;
    private boolean running;

    public CommunicationManager(Bucket currentBucket, Node self) {
        this.currentBucket = currentBucket;
        this.self = self;
        this.context = new ZContext();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.bucketSockets = new HashMap<>();

        this.clientRequestSocket = context.createSocket(SocketType.ROUTER);
        this.clientRequestSocket.bind("tcp://*:" + SconeConstants.SERVER_REQUEST_PORT);

        this.internalCommSocket = context.createSocket(SocketType.ROUTER);
        this.internalCommSocket.bind("tcp://*:" + SconeConstants.SERVER_INTERNAL_PORT);

        this.poller = context.createPoller(2);
        this.poller.register(clientRequestSocket, ZMQ.Poller.POLLIN);
        this.poller.register(internalCommSocket, ZMQ.Poller.POLLIN);
        this.running = true;
    }

    public void updateBucket(Bucket newBucket) {
        if (newBucket != null && !newBucket.equals(currentBucket)) {
            logger.debug("Updating bucket");
            Map<Node, ZMQ.Socket> oldBucketSockets = bucketSockets;
            bucketSockets = new HashMap<>();
            for (Node n : newBucket.getNodesExceptSelf(self)) {
                if (oldBucketSockets.containsKey(n)) {
                    bucketSockets.put(n, oldBucketSockets.remove(n));
                } else {
                    logger.debug("New socket for {}", n.getAddress().getHostAddress());
                    ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
                    socket.setIdentity(UUID.randomUUID().toString().getBytes(ZMQ.CHARSET));
                    socket.connect(String.format("tcp://%s:%s",n.getAddress().getHostAddress(), SconeConstants.SERVER_INTERNAL_PORT));
                    bucketSockets.put(n, socket);
                }
            }
            // closing unused sockets
            for (ZMQ.Socket left : oldBucketSockets.values()) {
                left.setLinger(0);
                left.close();
            }
            currentBucket = newBucket;
        }
    }

    public void shutdown() {
        this.running = false;
        clientRequestSocket.setLinger(0);
        clientRequestSocket.close();
        internalCommSocket.setLinger(0);
        internalCommSocket.close();
        context.destroy();
        logger.info("Communication manager terminated.");
    }

    public Triplet<MessageType, String, byte[]> recvMessage() {
        if (running) {
            MessageType type = null;
            ZMQ.Socket socket = null;
            try {
                poller.poll();
                if (poller.pollin(0)) {
                    type = MessageType.EXTERNAL;
                    socket = clientRequestSocket;
                } else if (poller.pollin(1)) {
                    type = MessageType.INTERNAL;
                    socket = internalCommSocket;
                }
                if (socket != null) {
                    String client = socket.recvStr();
                    socket.recv(0); // delimiter
                    byte[] requestBytes = socket.recv(0);
                    return new Triplet<>(type, client, requestBytes);
                }
            } catch (ZError.IOException | ZMQException e) {
                logger.info("Probably due to termination");
                logger.error(e.toString());
            }
        }
        return null;
    }

    public void replyToClient(ClientRequest request, MessageBuilder response) {
        if (running) {
            try {
                clientRequestSocket.sendMore(request.getClient());
                clientRequestSocket.sendMore("");
                clientRequestSocket.send(SerializationUtils.getBytesFromMessage(response), 0);
            } catch (IOException e) {
                logger.error("IOException serializing response to {}", request);
            }
        }
    }

    public void sendPrepare(byte[] message) {
        for (Node n : currentBucket.getNodesExceptSelf(self)) { // should guarantee that I am the master and they are all replicas
            ZMQ.Socket socket = bucketSockets.get(n);
            socket.sendMore(""); // delimiter
            socket.send(message);
        }
    }

    public void sendPrepareOK(byte[] message) {
        ZMQ.Socket socket = bucketSockets.get(currentBucket.getMaster());
        socket.sendMore(""); // delimiter
        socket.send(message);
    }

    public SconeEvent takeEvent() throws InterruptedException {
        return eventQueue.take();
    }

    public void queueEvent(SconeEvent event) {
        this.eventQueue.add(event);
    }
}
