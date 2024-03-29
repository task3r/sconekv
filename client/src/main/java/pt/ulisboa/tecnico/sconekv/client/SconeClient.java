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
import pt.ulisboa.tecnico.sconekv.common.db.Operation;
import pt.ulisboa.tecnico.sconekv.common.db.TransactionID;
import pt.ulisboa.tecnico.sconekv.common.dht.DHT;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.common.transport.External;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;
import zmq.ZError;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SconeClient {


    private static final Logger logger = LoggerFactory.getLogger(SconeClient.class);

    private SconeClientProperties properties;
    private UUID clientID;
    private ZContext context;
    private Map<Node, ZMQ.Socket> sockets;
    private DHT dht;
    private int transactionCounter = 0;
    private Random random;
    private List<String> discoveryNodes;

    public SconeClient() throws UnableToGetViewException, IOException {
        this("client-config.properties");
    }

    public SconeClient(String configFile) throws UnableToGetViewException, IOException {
        this.properties = new SconeClientProperties(configFile);
        this.sockets = new HashMap<>();
        this.random = new Random();
        this.context = new ZContext();
        this.clientID = UUID.randomUUID();

        this.discoveryNodes = getDiscoveryNodes();
        this.dht = new DHT(getDHT());

        logger.info("Created new client {}", this.clientID);
    }

    public SconeClient(List<String> nodes) throws UnableToGetViewException, IOException {
        this.properties = new SconeClientProperties("client-config.properties");
        this.sockets = new HashMap<>();
        this.random = new Random();
        this.context = new ZContext();
        this.clientID = UUID.randomUUID();
        this.discoveryNodes = nodes;
        this.dht = new DHT(getDHT());

        logger.info("Created new client {}", this.clientID);
    }


    private Common.DHT.Reader getDHT() throws UnableToGetViewException {
        try {
            MessageBuilder message = new org.capnproto.MessageBuilder();
            External.Request.Builder builder = message.initRoot(External.Request.factory);
            builder.setGetDht(null);
            byte[] rawRequest = SerializationUtils.getBytesFromMessage(message);

            try (ZMQ.Socket socket = this.context.createSocket(SocketType.DEALER)) {
                socket.setReceiveTimeOut(properties.RECV_TIMEOUT);
                socket.setIdentity(UUID.randomUUID().toString().getBytes());

                Collections.shuffle(this.discoveryNodes);
                for (String node : this.discoveryNodes) {
                    String address = "tcp://" + node + ":" + properties.SERVER_REQUEST_PORT;
                    socket.connect(address);
                    socket.sendMore(""); // delimiter
                    socket.send(rawRequest);
                    socket.recv(); // delimiter
                    byte[] rawResponse = socket.recv(0);
                    if (rawResponse != null) {
                        External.Response.Reader response = SerializationUtils.getMessageFromBytes(rawResponse)
                                .getRoot(External.Response.factory);
                        if (response.which() == External.Response.Which.DHT)
                            return response.getDht();
                    }
                    socket.disconnect(address);
                }
            }
        } catch (IOException e) {
            throw new UnableToGetViewException(e);
        }

        throw new UnableToGetViewException("Did not receive any correct GetDHT responses");
    }

    private List<String> getDiscoveryNodes() throws IOException, UnableToGetViewException {
        HttpGet get = new HttpGet(properties.TRACKER_URL + "/current");
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpClient.execute(get);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream stream = entity.getContent()) {
                    DiscoveryResponseDto discoveryResponseDto = new Gson().fromJson(new String(stream.readAllBytes()), DiscoveryResponseDto.class);

                    List<String> discoveryNodes = Arrays.asList(discoveryResponseDto.view);
                    return discoveryNodes;
                }
            }
        }
        throw new UnableToGetViewException("Did not receive any discovery nodes");
    }

    private ZMQ.Socket getSocket(Node node) {
        if (!sockets.containsKey(node))
            sockets.put(node, createSocket(node.getAddress().getHostAddress()));
        return sockets.get(node);
    }

    private ZMQ.Socket createSocket(String address) {
        ZMQ.Socket socket = this.context.createSocket(SocketType.DEALER);
        socket.setIdentity(this.clientID.toString().getBytes(ZMQ.CHARSET));
        socket.connect("tcp://" + address + ":" + properties.SERVER_REQUEST_PORT);
        socket.setReceiveTimeOut(properties.RECV_TIMEOUT);
        logger.debug("Client {} connected to {}", clientID, address);
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

        External.Response.Reader response = request(this.dht.getBucketForKey(key.getBytes()), message, txID, External.Response.Which.READ);

        return new Pair<>(response.getRead().getValue().toArray(), response.getRead().getVersion());
    }

    public Short performWrite(TransactionID txID, String key) throws RequestFailedException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setWrite(key.getBytes());

        return request(this.dht.getBucketForKey(key.getBytes()), message, txID, External.Response.Which.WRITE).getWrite().getVersion();
    }

    public Short performDelete(TransactionID txID, String key) throws RequestFailedException {
        MessageBuilder message = new org.capnproto.MessageBuilder();
        External.Request.Builder builder = message.initRoot(External.Request.factory);
        txID.serialize(builder.getTxID());
        builder.setDelete(key.getBytes());

        return request(this.dht.getBucketForKey(key.getBytes()), message, txID, External.Response.Which.DELETE).getDelete().getVersion();
    }

    public boolean performCommit(TransactionID txID, List<Operation> ops) throws RequestFailedException {
        Map<Short, List<Operation>> opsPerBucket = gerOpsPerBucket(ops);
        SortedSet<Short> buckets = new TreeSet<>(opsPerBucket.keySet()); // I want the coordinator to receive it first if possible

        Node coordinator = null;
        int retries = 0;
        while (retries < properties.MAX_REQUEST_RETRIES) {
            for (Short bucket : buckets) {
                MessageBuilder message = constructCommitMessage(txID, opsPerBucket.get(bucket), buckets);
                Node node = sendRequest(bucket, message, true);
                if (coordinator == null)
                    coordinator = node;
            }

            if (coordinator == null)
                throw new RequestFailedException("Unable to contact coordinator node");

            External.Response.Reader response = recvResponse(coordinator, External.Response.Which.COMMIT, txID, retries);
            if (response != null) {
                return response.getCommit().getResult() == External.CommitResponse.Result.OK;
            } else {
                retries++;
            }
        }
        throw new MaxRetriesExceededException("Exceeded the number o retries to commit transaction " + txID);
    }

    private MessageBuilder constructCommitMessage(TransactionID txID, List<Operation> bucketOps, SortedSet<Short> buckets) {
        MessageBuilder message = new MessageBuilder();
        External.Request.Builder rBuilder = message.initRoot(External.Request.factory);
        txID.serialize(rBuilder.getTxID());
        Common.Transaction.Builder cBuilder = rBuilder.initCommit();
        StructList.Builder<Common.Operation.Builder> opsBuilder = cBuilder.initOps(bucketOps.size());
        for (int i = 0; i < bucketOps.size(); i++) {
            bucketOps.get(i).serialize(opsBuilder.get(i));
        }

        // it is dumb to do this every single time, but I couldn't find a way, capnproto does not allow me to re-utilize the builder
        PrimitiveList.Short.Builder bucketsBuilder = cBuilder.initBuckets(buckets.size());
        int i = 0;
        for (Short b : buckets) {
            bucketsBuilder.set(i, b);
            i++;
        }
        return message;
    }


    private Map<Short, List<Operation>> gerOpsPerBucket(List<Operation> ops) {
        Map<Short, List<Operation>> opsPerBucket = new HashMap<>();
        for (Operation op : ops) {
            short bucket = this.dht.getBucketForKey(op.getKey().getBytes());
            if (!opsPerBucket.containsKey(bucket)) {
                opsPerBucket.put(bucket, new ArrayList<>());
            }
            opsPerBucket.get(bucket).add(op);
        }
        return opsPerBucket;
    }

    private Node getNodeForRequest(short bucket, boolean isCommit) throws InvalidBucketException {
        List<Node> candidates;
        Node selected;
        Node masterOfBucket = this.dht.getMasterOfBucket(bucket);
        if (isCommit) {
            selected = masterOfBucket;
        } else {
            switch (properties.MODE) {
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
                    throw new IllegalStateException("Unexpected request mode: " + properties.MODE);
            }
        }
        return selected;
    }

    private External.Response.Reader request(short bucket, MessageBuilder message, TransactionID txID, External.Response.Which requestType) throws RequestFailedException {
        int retries = 0;
        Node node;
        while (retries < properties.MAX_REQUEST_RETRIES) {
            node = sendRequest(bucket, message, false);
            External.Response.Reader response = recvResponse(node, requestType, txID, retries);
            if (response != null) {
                return response;
            } else {
                retries++;
            }
        }
        throw new MaxRetriesExceededException("Exceed the number of retries to " + requestType + " of transaction " + txID);
    }

    private Node sendRequest(short bucket, MessageBuilder message, boolean isCommit) throws RequestFailedException {
        ZMQ.Socket requester;
        try {
            Node node = getNodeForRequest(bucket, isCommit);
            requester = getSocket(node);
            requester.sendMore(""); // delimiter
            requester.send(SerializationUtils.getBytesFromMessage(message));
            return node;
        } catch (InvalidBucketException | IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private External.Response.Reader recvResponse(Node node, External.Response.Which requestType, TransactionID txID, int tryCount) {
        ZMQ.Socket socket = getSocket(node);
        socket.recv(); // waits for delimiter until timeout
        External.Response.Reader response;
        try {
            byte[] recvBytes = socket.recv(ZMQ.DONTWAIT); // already waited for the delimiter
            response = SerializationUtils.getMessageFromBytes(recvBytes).getRoot(External.Response.factory);
            if (response.which() != requestType || !txID.equals(new TransactionID(response.getTxID()))) {
                if (response.which() == External.Response.Which.DHT) // request was sent to the wrong node
                    this.dht.applyView(response.getDht());
                return null;
            } else {
                return response;
            }
        } catch (IOException e) {
            if (socket.errno() == ZError.EAGAIN) {
                logger.warn("Timeout recv [{}] {} - {}, try #{}", node.getAddress().getHostAddress(), requestType.name(), txID, tryCount);
            } else {
                logger.error("Error recv {}, but not a timeout", txID);
            }
            if (tryCount + 1 < properties.MAX_REQUEST_RETRIES) { // otherwise its pointless to getDHT
                try {
                    this.dht.applyView(getDHT());
                } catch (UnableToGetViewException ex) {
                    logger.error("Request failed, getDHT also failed. Is the system down? Retrying...");
                }
            }
            return null;
        }
    }
}
