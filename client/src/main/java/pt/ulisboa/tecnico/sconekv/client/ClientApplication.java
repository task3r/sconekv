package pt.ulisboa.tecnico.sconekv.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);
    public static void main(String[] args) {
        logger.info("Client created.");
        try (ZContext context = new ZContext()) {
            //  Socket to talk to server
            logger.info("Connecting to hello world server");

            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5555");

            for (int requestNbr = 0; requestNbr != 10; requestNbr++) {
                String request = "Hello";
                logger.info("Sending Hello {}", requestNbr);
                socket.send(request.getBytes(ZMQ.CHARSET), 0);

                String reply = new String(socket.recv(0), ZMQ.CHARSET);
                logger.info("Received {} {}.", reply, requestNbr);
            }
        }
    }
}
