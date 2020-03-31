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
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            socket.bind("tcp://*:5555");

            while (!Thread.currentThread().isInterrupted()) {
                String reply = new String(socket.recv(0), ZMQ.CHARSET);
                logger.info("Received: [{}]", reply);

                String response = "world";
                socket.send(response.getBytes(ZMQ.CHARSET), 0);

                Thread.sleep(1000); //  Do some 'work'
            }
        }
    }
}
