package pt.ulisboa.tecnico.sconekv.server.db;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

import java.io.IOException;

public class SconeServer {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    ZContext context;
    ZMQ.Socket socket;

    public SconeServer(ZContext context) {
        this.context = context;

        initSockets();
    }

    private void initSockets() {
        this.socket = context.createSocket(SocketType.ROUTER);
        this.socket.bind("tcp://*:5555");
    }


    public void run() throws IOException {
        logger.info("Listening for requests...");
        while (!Thread.currentThread().isInterrupted()) {
            String client = socket.recvStr();
            socket.recv(0); // delimiter

            byte[] requestBytes = socket.recv(0);

            logger.info("Received request from {}", client);
            Message.Request.Reader request = SerializationUtils.getMessageFromBytes(requestBytes).getRoot(Message.Request.factory);

            assert  request.which() == Message.Request.Which.READ;

            logger.info("{} {}", request.which(), new String(request.getRead().toArray()));

            MessageBuilder message = new org.capnproto.MessageBuilder();
            Message.Response.Builder builder = message.initRoot(Message.Response.factory);
            builder.setTxID(request.getTxID());
            Message.ReadResponse.Builder rb = builder.initRead();
            rb.setKey(request.getRead());
            rb.setValue("bar".getBytes());
            rb.setVersion((short) 17);

            socket.sendMore(client);
            socket.sendMore("");
            socket.send(SerializationUtils.getBytesFromMessage(message), 0);
        }
        logger.info("Shutting down...");
    }
}
