package pt.ulisboa.tecnico.sconekv.server.db;

import org.capnproto.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidOperationException;

import java.io.IOException;

public class SconeServer {
    private static final Logger logger = LoggerFactory.getLogger(SconeServer.class);

    ZContext context;
    ZMQ.Socket socket;
    Store store;

    public SconeServer(ZContext context) {
        this.context = context;
        this.store = new Store();

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

            //logger.info("Received request from {}", client);
            Message.Request.Reader request = SerializationUtils.getMessageFromBytes(requestBytes).getRoot(Message.Request.factory);

            TransactionID txID = new TransactionID(request.getTxID());

            MessageBuilder message = new org.capnproto.MessageBuilder();
            Message.Response.Builder response = message.initRoot(Message.Response.factory);
            response.setTxID(request.getTxID());

            switch (request.which()) {
                case WRITE:
                    performWrite(response, txID, new String(request.getRead().toArray()));
                    break;

                case READ:
                    performRead(response, txID, new String(request.getRead().toArray()));
                    break;

                case COMMIT:
                    Transaction tx = new Transaction(txID, request.getCommit());


                    performCommit(response, tx);
                    break;

                case _NOT_IN_SCHEMA:
                    logger.error("Received an incorrect request, ignoring...");
                    break;
            }


            socket.sendMore(client);
            socket.sendMore("");
            socket.send(SerializationUtils.getBytesFromMessage(message), 0);
        }
        logger.info("Shutting down...");
    }

    private void performRead(Message.Response.Builder response, TransactionID txID, String key) {
        logger.info("Read {} : {}", key, txID);

        Value value = store.get(key);

        Message.ReadResponse.Builder builder = response.initRead();
        builder.setKey(key.getBytes());
        builder.setValue(value.getContent());
        builder.setVersion(value.getVersion());
    }

    private void performWrite(Message.Response.Builder response, TransactionID txID, String key) {
        logger.info("Write {} : {}", key, txID);

        Value value = store.get(key);

        Message.WriteResponse.Builder builder = response.initWrite();
        builder.setKey(key.getBytes());
        builder.setVersion(value.getVersion());
    }

    private void performCommit(Message.Response.Builder response, Transaction tx) {
        logger.info("Commit : {}", tx.getId());
        Message.CommitResponse.Builder builder = response.initCommit();

        try {
            store.validate(tx);
            store.perform(tx);
            store.releaseLocks(tx);
            builder.setResult(Message.CommitResponse.Result.OK);
        } catch (InvalidOperationException e) {
            builder.setResult(Message.CommitResponse.Result.NOK);
        }

    }
}
