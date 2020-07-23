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
    private final Map<String, ZMQ.Socket> sockets;
    private BlockingQueue<SconeEvent> eventQueue;
    private final List<ZMQ.Socket> workerChannel;
    private final ZMQ.Socket workerChannelSink;

    public CommunicationManager(Node self) {
        this.self = self;
        this.context = new ZContext();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.sockets = new HashMap<>();

        this.clientRequestSocket = context.createSocket(SocketType.ROUTER);
        this.clientRequestSocket.bind("tcp://*:" + SconeConstants.SERVER_REQUEST_PORT);

        this.internalCommSocket = context.createSocket(SocketType.ROUTER);
        this.internalCommSocket.bind("tcp://*:" + SconeConstants.SERVER_INTERNAL_PORT);

        this.workerChannelSink = context.createSocket(SocketType.PULL);
        this.workerChannelSink.bind("inproc://reply-sink");

        this.workerChannel = new ArrayList<>();
        for (int i = 0; i <= SconeConstants.NUM_WORKERS; i++) { // workerId 0 is saved for viewChange callbacks
            ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
            socket.connect("inproc://reply-sink");
            this.workerChannel.add(socket);
        }

        this.poller = context.createPoller(3);
        this.poller.register(workerChannelSink, ZMQ.Poller.POLLIN);
        this.poller.register(internalCommSocket, ZMQ.Poller.POLLIN);
        this.poller.register(clientRequestSocket, ZMQ.Poller.POLLIN);
    }

    public void updateBucket(Bucket newBucket) {
        if (newBucket != null && !newBucket.equals(currentBucket)) {
            logger.debug("Updating bucket");
            currentBucket = newBucket;
        }
    }

    private ZMQ.Socket getSocket(String address) {
        if (!sockets.containsKey(address)) {
            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            socket.setIdentity(UUID.randomUUID().toString().getBytes(ZMQ.CHARSET));
            socket.connect(String.format("tcp://%s:%s", address, SconeConstants.SERVER_INTERNAL_PORT));
            sockets.put(address, socket);
        }
        return sockets.get(address);
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
                    MessageType type = MessageType.valueOf(workerChannelSink.recvStr());
                    workerChannelSink.recv(); //delimiter
                    String target = workerChannelSink.recvStr();
                    workerChannelSink.recv(); //delimiter
                    byte[] message = workerChannelSink.recv();
                    if (type == MessageType.EXTERNAL) {
                        clientRequestSocket.sendMore(target);
                        clientRequestSocket.sendMore(""); //delimiter
                        clientRequestSocket.send(message);
                    } else if (type == MessageType.INTERNAL) {
                        send(message, target);
                    }

                } else if (poller.pollin(1)) {
                    String node = internalCommSocket.recvStr();
                    internalCommSocket.recv(); //delimiter
                    byte[] message = internalCommSocket.recv();
                    return new Triplet<>(MessageType.INTERNAL, node, message);

                } else if (poller.pollin(2)) {
                    String client = clientRequestSocket.recvStr();
                    clientRequestSocket.recv(); //delimiter
                    byte[] message = clientRequestSocket.recv();
                    return new Triplet<>(MessageType.EXTERNAL, client, message);
                }
            }
        } catch (ZError.IOException | ZMQException e) {
            logger.info("Probably due to termination");
            logger.error(e.toString());
        }
        return null;
    }

    private void workerChannelSend(byte[] messageBytes, ZMQ.Socket socket, MessageType type, String target) {
        socket.sendMore(type.name());
        socket.sendMore(""); // delimiter
        socket.sendMore(target);
        socket.sendMore(""); // delimiter
        socket.send(messageBytes, 0);
    }

    public void replyToClient(String client, MessageBuilder response, short workerId) {
            try {
                ZMQ.Socket socket = workerChannel.get(workerId);
                workerChannelSend(SerializationUtils.getBytesFromMessage(response), socket, MessageType.EXTERNAL, client);
            } catch (IOException e) {
                logger.error("IOException serializing response to {}", client);
            }
    }

    public void broadcastBucket(MessageBuilder message, short workerId) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                ZMQ.Socket socket = workerChannel.get(workerId);
                for (Node node : currentBucket.getNodesExcept(self)) { // should guarantee that I am the master and they are all replicas
                    workerChannelSend(messageBytes, socket, MessageType.INTERNAL, node.getAddress().getHostAddress());
                }
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    public synchronized void broadcast(MessageBuilder message, Set<Node> recipients, short workerId) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                ZMQ.Socket socket = workerChannel.get(workerId);
                for (Node node : recipients) {
                    workerChannelSend(messageBytes, socket, MessageType.INTERNAL, node.getAddress().getHostAddress());
                }
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    public void send(MessageBuilder message, Node node, short workerId) {
        synchronized (sockets) {
            try {
                byte[] messageBytes = SerializationUtils.getBytesFromMessage(message);
                ZMQ.Socket socket = workerChannel.get(workerId);
                workerChannelSend(messageBytes, socket, MessageType.INTERNAL, node.getAddress().getHostAddress());
            } catch (IOException e) {
                logger.error("IOException serializing internal message");
            }
        }
    }

    private void send(byte[] message, String target) {
        ZMQ.Socket socket = getSocket(target);
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
