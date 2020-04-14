package pt.ulisboa.tecnico.sconekv.server;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);

    public static void main(String[] args) throws Exception {
        logger.info("Launching server...");
        try (ZContext context = new ZContext()) {
            // Socket to talk to clients
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.bind("tcp://*:5555");
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
        }
    }
}
