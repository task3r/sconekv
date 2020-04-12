package pt.ulisboa.tecnico.sconekv.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.UUID;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);
    public static void main(String[] args) {
        logger.info("Client created.");
        try (ZContext context = new ZContext()) {
            //  Socket to talk to server
            logger.info("Connecting to hello world server");

            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            socket.setIdentity(UUID.randomUUID().toString().getBytes(ZMQ.CHARSET));
            socket.connect("tcp://localhost:5555");
//
//            for (int requestNbr = 0; requestNbr != 10; requestNbr++) {
//                logger.info("Sending Hello {}", requestNbr);
//                socket.send("hello", 0);
//
//                String reply = socket.recvStr();
//                logger.info("Received {} {}.", reply, requestNbr);
//            }

            socket.sendMore("");
            socket.send("NO", 0);
            logger.info("Sent NO");
            socket.sendMore("");
            socket.send("REQ",0);
            logger.info("Sent REQ");
            socket.recv(); // delimiter
            String response = socket.recvStr(0);
            logger.info("Received: {}", response);
        }
    }
}
