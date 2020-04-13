package pt.ulisboa.tecnico.sconekv.client;

import kotlin.Pair;
import org.capnproto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.common.Constants;
import pt.ulisboa.tecnico.sconekv.common.transaction.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Message.Request;
import pt.ulisboa.tecnico.sconekv.common.transport.Message.Response;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

import java.io.IOException;
import java.util.UUID;

public class SconeClient {
    private static final Logger logger = LoggerFactory.getLogger(SconeClient.class);

    private UUID clientID;
    private String server;
    private ZContext context;
    private ZMQ.Socket requester;
    private int transactionCounter = 0;

    public SconeClient(ZContext context, String server) {
        this.context = context;
        this.server = server;
        this.clientID = UUID.randomUUID();

        initSockets();

        logger.info("Created new client {}", this.clientID);
    }

    private void initSockets() {
        this.requester = this.context.createSocket(SocketType.DEALER);
        this.requester.setIdentity(UUID.randomUUID().toString().getBytes(ZMQ.CHARSET));
        this.requester.connect("tcp://" + server + ":" + Constants.SERVER_REQUEST_PORT);
        logger.info("Client {} connected to {}", clientID, server);
    }

    public Transaction newTransaction() {
        return new Transaction(this, new TransactionID(this.clientID, transactionCounter++));
    }

    protected Pair<byte[], Short> performRead(TransactionID txID, String key) throws IOException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        Request.Builder builder = message.initRoot(Request.factory);
        builder.setTxID(txID.serialize(builder.getTxID()));
        builder.initRead().setKey(key.getBytes());


        Response.Reader response = request(message).getRoot(Response.factory);
        assert response.which() == Response.Which.READ;

        return new Pair<>(response.getRead().getValue().toArray(), response.getRead().getVersion());
    }

    private MessageReader request(MessageBuilder message) throws IOException {
        this.requester.sendMore(""); // delimiter
        this.requester.send(SerializationUtils.getBytesFromMessage(message));
        this.requester.recv(); // delimiter
        return SerializationUtils.getMessageFromBytes(this.requester.recv(0));
    }
}
