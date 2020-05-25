package pt.ulisboa.tecnico.sconekv.client;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.capnproto.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import pt.tecnico.ulisboa.prime.network.DiscoveryResponseDto;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.MaxRetriesExceededException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.UnableToGetViewException;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;

public class SconeClient {
    private static final Logger logger = LoggerFactory.getLogger(SconeClient.class);
    private static final int RECV_TIMEOUT = 5000;
    private static final int MAX_REQUEST_RETRIES = 5;

    private UUID clientID;
    private ZContext context;
    private ZMQ.Socket requester;
    private DHT dht;
    private int transactionCounter = 0;

    public SconeClient() throws UnableToGetViewException, InvalidBucketException {
        this.context = new ZContext();
        this.clientID = UUID.randomUUID();

        this.dht = getDHT();

        initSockets();

        logger.info("Created new client {}", this.clientID);
    }

    private DHT getDHT() throws UnableToGetViewException {
        try {
            DiscoveryResponseDto discoveryResponseDto = getDiscoveryNodes();
            if (discoveryResponseDto == null)
                throw new UnableToGetViewException();

            MessageBuilder message = new org.capnproto.MessageBuilder();
            External.Request.Builder builder = message.initRoot(External.Request.factory);
            builder.setGetDht(null);
            byte[] rawRequest = SerializationUtils.getBytesFromMessage(message);

            try (ZMQ.Socket socket = this.context.createSocket(SocketType.DEALER)) {
                socket.setReceiveTimeOut(RECV_TIMEOUT);
                socket.setIdentity(UUID.randomUUID().toString().getBytes());
                for (String node : discoveryResponseDto.view) {
                    String address = "tcp://" + node + ":" + SconeConstants.SERVER_REQUEST_PORT;
                    socket.connect(address);
                    socket.sendMore(""); // delimiter
                    socket.send(rawRequest);
                    socket.recv(); // delimiter
                    byte[] rawResponse = socket.recv(0);
                    if (rawResponse != null) {
                        External.Response.Reader response = SerializationUtils.getMessageFromBytes(rawResponse)
                                .getRoot(External.Response.factory);
                        if (response.which() == External.Response.Which.DHT)
                            return new DHT(response.getDht());
                    }
                    socket.disconnect(address);
                }
            }
        } catch (IOException e) {
            throw new UnableToGetViewException();
        }

        throw new UnableToGetViewException(); // if it reached here then it did not receive any correct getDHT responses
    }

    private DiscoveryResponseDto getDiscoveryNodes() throws IOException {
        HttpGet get = new HttpGet(SconeConstants.TRACKER_URL + "/current");
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(get);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream stream = entity.getContent()) {
                    return new Gson().fromJson(new String(stream.readAllBytes()), DiscoveryResponseDto.class);
                }
            }
        }
        return null;
    }

    private void initSockets() throws InvalidBucketException {
        this.requester = createSocket(dht.getMasterOfBucket((short) 0).getAddress().getHostAddress()); //FIXME
    }

    private ZMQ.Socket createSocket(String address) {
        ZMQ.Socket socket = this.context.createSocket(SocketType.DEALER);
        socket.setIdentity(this.clientID.toString().getBytes(ZMQ.CHARSET));
        socket.connect("tcp://" + address + ":" + SconeConstants.SERVER_REQUEST_PORT);
        socket.setReceiveTimeOut(RECV_TIMEOUT);
        logger.info("Client {} connected to {}", clientID, address);
        return socket;
    }

    public Transaction newTransaction() {
        return new Transaction(this, new TransactionID(this.clientID, transactionCounter++));
    }

    public Pair<byte[], Short> performRead(TransactionID txID, String key) throws MaxRetriesExceededException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setRead(key.getBytes());

        External.Response.Reader response = request(message, External.Response.Which.READ);

        return new Pair<>(response.getRead().getValue().toArray(), response.getRead().getVersion());
    }

    public Short performWrite(TransactionID txID, String key) throws MaxRetriesExceededException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setWrite(key.getBytes());

        return request(message, External.Response.Which.WRITE).getWrite().getVersion();
    }

    public boolean performCommit(TransactionID txID, List<Operation> ops) throws MaxRetriesExceededException {
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

        return request(message, External.Response.Which.COMMIT).getCommit().getResult() == External.CommitResponse.Result.OK;
    }

    private External.Response.Reader request(MessageBuilder message, External.Response.Which requestType) throws MaxRetriesExceededException {
        int retries = 0;
        while (retries < MAX_REQUEST_RETRIES) {
            try {
                this.requester.sendMore(""); // delimiter
                this.requester.send(SerializationUtils.getBytesFromMessage(message));
                this.requester.recv(); // delimiter
                External.Response.Reader response = SerializationUtils.getMessageFromBytes(this.requester.recv(0)).getRoot(External.Response.factory);
                if (response.which() != requestType) {
                    if (response.which() == External.Response.Which.DHT) // request was sent to the wrong node
                        this.dht.applyView(response.getDht());
                    else
                        retries++;
                } else {
                    return response;
                }
            } catch (IOException e) {
                retries++;
            }
        }
        throw new MaxRetriesExceededException();
    }
}
