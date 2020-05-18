package pt.ulisboa.tecnico.sconekv.server.db;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class SconeServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    short id;
    int eventCounter;
    Store store;
    ZMQ.Socket socket;
    BlockingQueue<SconeEvent> eventQueue;

    public SconeServer(short id, ZMQ.Socket socket, Store store, BlockingQueue<SconeEvent> eventQueue) {
        this.id = id;
        this.socket = socket;
        this.store = store;
        this.eventQueue = eventQueue;
    }

    @Override
    public void run() {
        logger.info("Listening for requests...");
        while (!Thread.currentThread().isInterrupted()) {
            String client = socket.recvStr();
            socket.recv(0); // delimiter

            byte[] requestBytes = socket.recv(0);

            Message.Request.Reader request;
            try {
                request = SerializationUtils.getMessageFromBytes(requestBytes).getRoot(Message.Request.factory);
            } catch (IOException e) {
               logger.error("IOException deserializing client request. Continuing...");
               continue;
            }

            if (request == null) {
                logger.error("Request deserialized to null. Ignoring...");
                continue;
            }

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
}
