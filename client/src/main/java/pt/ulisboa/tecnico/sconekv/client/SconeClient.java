package pt.ulisboa.tecnico.sconekv.client;

import org.capnproto.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
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
        this.requester.connect("tcp://" + server + ":" + SconeConstants.SERVER_REQUEST_PORT);
        logger.info("Client {} connected to {}", clientID, server);
    }

    public Transaction newTransaction() {
        return new Transaction(this, new TransactionID(this.clientID, transactionCounter++));
    }

    public Pair<byte[], Short> performRead(TransactionID txID, String key) throws IOException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setRead(key.getBytes());

        External.Response.Reader response = request(message).getRoot(External.Response.factory);
        assert response.which() == External.Response.Which.READ;

        return new Pair<>(response.getRead().getValue().toArray(), response.getRead().getVersion());
    }

    public Short performWrite(TransactionID txID, String key) throws IOException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setWrite(key.getBytes());

        External.Response.Reader response = request(message).getRoot(External.Response.factory);
        assert response.which() == External.Response.Which.WRITE;

        return response.getWrite().getVersion();
    }

    public boolean performCommit(TransactionID txID, List<Operation> ops) throws IOException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder rBuilder = message.initRoot(External.Request.factory);
        txID.serialize(rBuilder.getTxID());
        External.Commit.Builder cBuilder = rBuilder.initCommit();
        StructList.Builder<Common.Operation.Builder> opsBuilder = cBuilder.initOps(ops.size());
        ListIterator<Operation> it = ops.listIterator();
        while (it.hasNext()) {
            it.next().serialize(opsBuilder.get(it.nextIndex() - 1));
        }
        cBuilder.initBuckets(0); // insert buckets in message

        External.Response.Reader response = request(message).getRoot(External.Response.factory);
        assert response.which() == External.Response.Which.COMMIT;

        return response.getCommit().getResult() == External.CommitResponse.Result.OK;
    }

    private MessageReader request(MessageBuilder message) throws IOException {
        this.requester.sendMore(""); // delimiter
        this.requester.send(SerializationUtils.getBytesFromMessage(message));
        this.requester.recv(); // delimiter
        return SerializationUtils.getMessageFromBytes(this.requester.recv(0));

        // TODO falta garantir que é uma resposta à mesma mensagem que enviei
    }
}