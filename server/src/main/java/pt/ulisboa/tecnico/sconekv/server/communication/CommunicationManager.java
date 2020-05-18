package pt.ulisboa.tecnico.sconekv.server.communication;

import org.capnproto.MessageBuilder;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.events.external.ClientRequest;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CommunicationManager {
    private static final Logger logger = LoggerFactory.getLogger(CommunicationManager.class);

    private ZContext context;
    private ZMQ.Socket clientRequestSocket;
    private ZMQ.Socket internalCommSocket;
    private ZMQ.Poller poller;
    private BlockingQueue<SconeEvent> eventQueue;

    public CommunicationManager() {
        this.context = new ZContext();
        this.eventQueue = new LinkedBlockingQueue<>();

        this.clientRequestSocket = context.createSocket(SocketType.ROUTER);
        this.clientRequestSocket.bind("tcp://*:" + SconeConstants.SERVER_REQUEST_PORT);

        this.internalCommSocket = context.createSocket(SocketType.ROUTER);
        this.internalCommSocket.bind("tcp://*:" + SconeConstants.SERVER_INTERNAL_PORT);

        this.poller = context.createPoller(2);
        this.poller.register(clientRequestSocket, ZMQ.Poller.POLLIN);
        this.poller.register(internalCommSocket, ZMQ.Poller.POLLIN);
    }

    public void shutdown() {
        context.destroy(); // FIXME this is not enough but alternatives also raised exceptions
    }

    public Triplet<MessageType, String, byte[]> recvMessage() {
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
        } catch (ZMQException e) {
            logger.error(e.toString());
        }
        return null;
    }

    public void replyToClient(ClientRequest request, MessageBuilder response) {
        try {
            clientRequestSocket.sendMore(request.getClient());
            clientRequestSocket.sendMore("");
            clientRequestSocket.send(SerializationUtils.getBytesFromMessage(response), 0);
        } catch (IOException e) {
            logger.error("IOException serializing response to {}", request);
        }
    }

    public SconeEvent takeEvent() throws InterruptedException {
        return eventQueue.take();
    }

    public void queueEvent(SconeEvent event) {
        this.eventQueue.add(event);
    }
}
