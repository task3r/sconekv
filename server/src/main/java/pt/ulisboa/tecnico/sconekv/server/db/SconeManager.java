package pt.ulisboa.tecnico.sconekv.server.db;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.db.events.CommitRequest;
import pt.ulisboa.tecnico.sconekv.server.db.events.ReadRequest;
import pt.ulisboa.tecnico.sconekv.server.db.events.SconeEvent;
import pt.ulisboa.tecnico.sconekv.server.db.events.WriteRequest;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SconeManager {
    private static final Logger logger = LoggerFactory.getLogger(SconeManager.class);

    ZContext context;
    ZMQ.Socket socket;
    Store store;
    BlockingQueue<SconeEvent> eventQueue;
    Thread worker;

    public SconeManager(ZContext context) {
        this.context = context;
        this.store = new Store();
        this.eventQueue = new LinkedBlockingQueue<>();

        initSockets();
    }

    private void initSockets() {
        this.socket = context.createSocket(SocketType.ROUTER);
        this.socket.bind("tcp://*:5555");
    }


    public void run() throws IOException {
        logger.info("Listening for requests...");

        worker = new Thread(new SconeWorker(1, socket, store, eventQueue));
        worker.start();

        short id = 0;
        int eventCounter = 0;
        while (!Thread.currentThread().isInterrupted()) {
            String client = socket.recvStr();
            socket.recv(0); // delimiter

            byte[] requestBytes = socket.recv(0);

            Message.Request.Reader request = SerializationUtils.getMessageFromBytes(requestBytes).getRoot(Message.Request.factory);

            TransactionID txID = new TransactionID(request.getTxID());

            switch (request.which()) {
                case WRITE:
                    eventQueue.add(new WriteRequest(new Pair<>(id, eventCounter++), client, txID, new String(request.getRead().toArray())));
                    break;

                case READ:
                    eventQueue.add(new ReadRequest(new Pair<>(id, eventCounter++), client, txID, new String(request.getRead().toArray())));
                    break;

                case COMMIT:
                    Transaction tx = new Transaction(txID, request.getCommit());
                    eventQueue.add(new CommitRequest(new Pair<>(id, eventCounter++), client, tx));
                    break;

                case _NOT_IN_SCHEMA:
                    logger.error("Received an incorrect request, ignoring...");
                    break;
            }
        }
    }

    public void shutdown() {
        if (worker != null) {
            worker.interrupt();
        }
    }
}
