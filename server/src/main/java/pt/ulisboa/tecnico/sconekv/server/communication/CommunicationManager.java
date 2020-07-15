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
import pt.ulisboa.tecnico.sconekv.server.constants.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import zmq.ZError;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CommunicationManager {
    private static final Logger logger = LoggerFactory.getLogger(CommunicationManager.class);

    private Bucket currentBucket;
    private Node self;
    private ZContext context;
    private final ZMQ.Socket clientRequestSocket;
    private final ZMQ.Socket internalCommSocket;
    private ZMQ.Poller poller;
    private final Map<Node, ZMQ.Socket> sockets;
    private BlockingQueue<SconeEvent> eventQueue;
    private final List<ZMQ.Socket> workerReply;
    private final ZMQ.Socket workerReplySink;

    public CommunicationManager(Node self) {
        this.self = self;
        this.context = new ZContext();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.sockets = new HashMap<>();

        this.clientRequestSocket = context.createSocket(SocketType.ROUTER);
        this.clientRequestSocket.bind("tcp://*:" + SconeConstants.SERVER_REQUEST_PORT);

        this.internalCommSocket = context.createSocket(SocketType.ROUTER);
        this.internalCommSocket.bind("tcp://*:" + SconeConstants.SERVER_INTERNAL_PORT);

        this.workerReplySink = context.createSocket(SocketType.PULL);
        this.workerReplySink.bind("inproc://reply-sink");

        this.workerReply = new ArrayList<>();
        for (int i = 0; i < SconeConstants.NUM_WORKERS; i++) {
            ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
            socket.connect("inproc://reply-sink");
            this.workerReply.add(socket);
        }

        this.poller = context.createPoller(3);
        this.poller.register(clientRequestSocket, ZMQ.Poller.POLLIN);
        this.poller.register(internalCommSocket, ZMQ.Poller.POLLIN);
        this.poller.register(workerReplySink, ZMQ.Poller.POLLIN);
    }

    public void updateBucket(Bucket newBucket) {
        if (newBucket != null && !newBucket.equals(currentBucket)) {
            logger.debug("Updating bucket");
            currentBucket = newBucket;
        }
    }

    private ZMQ.Socket getSocket(Node node) {
        if (!sockets.containsKey(node)) {
            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            socket.setIdentity(UUID.randomUUID().toString().getBytes(ZMQ.CHARSET));
            socket.connect(String.format("tcp://%s:%s", node.getAddress().getHostAddress(), SconeConstants.SERVER_INTERNAL_PORT));
            sockets.put(node, socket);
        }
        return sockets.get(node);
    }

    public void shutdown() {
        clientRequestSocket.setLinger(0);
        clientRequestSocket.close();
        internalCommSocket.setLinger(0);
        internalCommSocket.close();
        context.destroy();
        logger.info("Communication manager terminated.");
    }

    public Triplet<MessageType, String, byte[]> recvMessage() {
        try {
            while (true) {
                poller.poll();

                if (poller.pollin(0)) {
                    String client = clientRequestSocket.recvStr();
                    clientRequestSocket.recv(); //delimiter
                    byte[] message = clientRequestSocket.recv();
                    return new Triplet<>(MessageType.EXTERNAL, client, message);

                } else if (poller.pollin(1)) {
                    String node = internalCommSocket.recvStr();
                    internalCommSocket.recv(); //delimiter
                    byte[] message = internalCommSocket.recv();
                    return new Triplet<>(MessageType.INTERNAL, node, message);

                } else if (poller.pollin(2)) {
                    String clientToReply = workerReplySink.recvStr();
                    workerReplySink.recv(); //delimiter
                    byte[] response = workerReplySink.recv();
                    clientRequestSocket.sendMore(clientToReply);
                    clientRequestSocket.sendMore(""); //delimiter
                    clientRequestSocket.send(response);
                }
            }
        } catch (ZError.IOException | ZMQException e) {
            logger.info("Probably due to termination");
            logger.error(e.toString());
        }
        return null;
    }

    public void replyToClient(String client, MessageBuilder response, short id) {
            try {
                ZMQ.Socket socket = workerReply.get(id - 1); // id 0 is reserved for the server thread
                socket.sendMore(client);
                socket.sendMore("");
                socket.send(SerializationUtils.getBytesFromMessage(response), 0);
            } catch (IOException e) {
                logger.error("IOException serializing response to {}", client);
            }
    }

    public void broadcastBucket(MessageBuilder message) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                for (Node n : currentBucket.getNodesExcept(self)) { // should guarantee that I am the master and they are all replicas
                    ZMQ.Socket socket = getSocket(n);
                    socket.sendMore(""); // delimiter
                    socket.send(messageBytes);
                }
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    public synchronized void broadcast(MessageBuilder message, Set<Node> recipients) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                for (Node node : recipients) {
                    send(messageBytes, node);
                }
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    public void send(MessageBuilder message, Node node) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                send(messageBytes, node);
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    private void send(byte[] message, Node node) {
        ZMQ.Socket socket = getSocket(node);
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
