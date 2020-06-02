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
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.network.DiscoveryResponseDto;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.MaxRetriesExceededException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.RequestFailedException;
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
import java.util.*;

public class SconeClient {
    enum RequestMode {
        MASTER_ONLY,
        REPLICA_ONLY,
        RANDOM
    }
    private static final Logger logger = LoggerFactory.getLogger(SconeClient.class);
    private static final int RECV_TIMEOUT = 5000;
    private static final int MAX_REQUEST_RETRIES = 5;

    private UUID clientID;
    private ZContext context;
    private Map<Node, ZMQ.Socket> sockets;
    private DHT dht;
    private int transactionCounter = 0;
    private RequestMode mode;
    private Random random;

    public SconeClient() throws UnableToGetViewException {
        this.mode = RequestMode.MASTER_ONLY;
        this.context = new ZContext();
        this.clientID = UUID.randomUUID();
        this.sockets = new HashMap<>();
        this.random = new Random();

        this.dht = getDHT();

        logger.info("Created new client {}", this.clientID);
    }

    public SconeClient(RequestMode mode) throws UnableToGetViewException {
        this.mode = mode;
        this.context = new ZContext();
        this.clientID = UUID.randomUUID();
        this.dht = getDHT();

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

    private ZMQ.Socket getSocket(Node node) {
        if (!sockets.containsKey(node))
            sockets.put(node, createSocket(node.getAddress().getHostAddress()));
        return sockets.get(node);
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

    public Pair<byte[], Short> performRead(TransactionID txID, String key) throws RequestFailedException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setRead(key.getBytes());

        External.Response.Reader response = request(this.dht.getBucketForKey(key.getBytes()), message, External.Response.Which.READ);

        return new Pair<>(response.getRead().getValue().toArray(), response.getRead().getVersion());
    }

    public Short performWrite(TransactionID txID, String key) throws RequestFailedException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setWrite(key.getBytes());

        return request(this.dht.getBucketForKey(key.getBytes()), message, External.Response.Which.WRITE).getWrite().getVersion();
    }

    public boolean performCommit(TransactionID txID, List<Operation> ops) throws RequestFailedException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder rBuilder = message.initRoot(External.Request.factory);
        txID.serialize(rBuilder.getTxID());
        External.Commit.Builder cBuilder = rBuilder.initCommit();
        StructList.Builder<Common.Operation.Builder> opsBuilder = cBuilder.initOps(ops.size());
        for (int i = 0; i < ops.size(); i++) {
            ops.get(i).serialize(opsBuilder.get(i));
        }
        cBuilder.initBuckets(0); // insert buckets in message

        return request((short) 0, message, External.Response.Which.COMMIT).getCommit().getResult() == External.CommitResponse.Result.OK;
    }

    private ZMQ.Socket getSocketForRequest(short bucket) throws InvalidBucketException {
        List<Node> candidates;
        Node selected;
        Node masterOfBucket = this.dht.getMasterOfBucket(bucket);
        switch (this.mode) {
            case MASTER_ONLY:
                selected = masterOfBucket;
                break;
            case REPLICA_ONLY:
                candidates = new ArrayList<>(this.dht.getBucket(bucket).getNodesExcept(masterOfBucket));
                selected = candidates.get(random.nextInt(candidates.size()));
                break;
            case RANDOM:
                candidates = new ArrayList<>(this.dht.getBucket(bucket).getNodes());
                selected = candidates.get(random.nextInt(candidates.size()));
                break;
            default:
                //shouldn't reach here
                throw new InvalidBucketException();
        }
        return getSocket(selected);
    }

    private External.Response.Reader request(short bucket, MessageBuilder message, External.Response.Which requestType) throws RequestFailedException {
        int retries = 0;
        ZMQ.Socket requester;
        while (retries < MAX_REQUEST_RETRIES) {
            try {
                requester = getSocketForRequest(bucket);
                requester.sendMore(""); // delimiter
                requester.send(SerializationUtils.getBytesFromMessage(message));
                requester.recv(); // delimiter
                External.Response.Reader response = SerializationUtils.getMessageFromBytes(requester.recv(0)).getRoot(External.Response.factory);
                if (response.which() != requestType) {
                    if (response.which() == External.Response.Which.DHT) // request was sent to the wrong node
                        this.dht.applyView(response.getDht());
                    else
                        retries++;
                } else {
                    return response;
                }
            } catch (IOException | InvalidBucketException e) {
                try {
                    this.dht = getDHT();
                } catch (UnableToGetViewException ex) {
                    logger.error("Request failed, getDHT also failed. Is the system down? Retrying...");
                }
                retries++;
            }
        }
        throw new MaxRetriesExceededException();
    }
}
