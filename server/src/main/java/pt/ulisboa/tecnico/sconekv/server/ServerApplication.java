package pt.ulisboa.tecnico.sconekv.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);

    public static void main(String[] args) throws Exception {
        logger.info("Server created.");
        try (ZContext context = new ZContext()) {
            // Socket to talk to clients
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.bind("tcp://*:5555");
            while (!Thread.currentThread().isInterrupted()) {
                String client = socket.recvStr();
                socket.recv(0); // delimiter
                String msg = socket.recvStr(0);
                logger.info("Received {} from {}", msg, client);

                if (msg.equals("REQ")) {
                    socket.sendMore(client);
                    socket.sendMore(""); // delimiter
                    socket.send("the response you asked for", 0);
                }

                Thread.sleep(1000); //  Do some 'work'
            }
        }
    }
}
