package pt.ulisboa.tecnico.sconekv.common.dht;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.capnproto.StructList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;
import pt.ulisboa.tecnico.sconekv.common.utils.SerializationUtils;

import java.net.UnknownHostException;
import java.util.*;

public class DHT {
    private static final Logger logger = LoggerFactory.getLogger(DHT.class);

    private SortedSet<Node> nodes;
    private Version viewVersion;
    private short numBuckets;
    private HashFunction hashFunction;
    private Bucket[] buckets;
    private int murmurSeed;

    public DHT(SortedSet<Node> nodes, Version version, short numBuckets, int murmurSeed) {
        this.nodes = nodes;
        this.viewVersion = version;
        this.numBuckets = numBuckets;
        this.murmurSeed = murmurSeed;
        this.hashFunction = Hashing.murmur3_128(murmurSeed);
        defineBuckets();
    }

    public DHT(Ring ring, short numBuckets, int murmurSeed) {
        this(ring.asSet(), ring.getVersion(), numBuckets, murmurSeed);
    }

    public DHT(Common.DHT.Reader dht) {
        this(getNodes(dht), SerializationUtils.getVersionFromMesage(dht.getVersion()),
                dht.getNumBuckets(), dht.getMurmurSeed());
    }

    private static SortedSet<Node> getNodes(Common.DHT.Reader dht) {
        SortedSet<Node> nodes = new TreeSet<>();
        for (Common.Node.Reader node : dht.getNodes()) {
            try {
                nodes.add(SerializationUtils.getNodeFromMessage(node));
            } catch (UnknownHostException e) {
                logger.error("Received invalid node, ignoring {} {}", node.getId(), node.getAddress());
            }
        }
        return nodes;
    }

    public void serialize(Common.DHT.Builder builder) {
        StructList.Builder<Common.Node.Builder> nodesBuilder = builder.initNodes(this.nodes.size());
        int i = 0;
        for (Node node : this.nodes) {
            SerializationUtils.serializeNode(nodesBuilder.get(i), node);
            i++;
        }
        SerializationUtils.serializeViewVersion(builder.getVersion(), this.viewVersion);
        builder.setNumBuckets(this.numBuckets);
        builder.setMurmurSeed(this.murmurSeed);
    }

    public synchronized void applyView(Ring ring) {
        if (checkVersion(ring.getVersion()))
            changeView(ring.asSet(), ring.getVersion());
    }

    public synchronized void applyView(Common.DHT.Reader dht) {
        Version receivedVersion = SerializationUtils.getVersionFromMesage(dht.getVersion());
        if (checkVersion(receivedVersion)) {
            changeView(getNodes(dht), receivedVersion);
        }
    }

    private synchronized void changeView(SortedSet<Node> newView, Version newVersion) {
        this.nodes = newView;
        this.viewVersion = newVersion;
        defineBuckets();
    }

    private boolean checkVersion(Version newVersion) {
        return this.viewVersion.isLesser(newVersion);
    }

    private void defineBuckets() {
        buckets = new Bucket[numBuckets];
        int bucketSize = nodes.size() / numBuckets;
        ArrayList<Node> aux = new ArrayList<>(nodes);
        for (short b = 0; b < numBuckets; b++) {
            buckets[b] = new Bucket(b, new TreeSet<>(aux.subList(b*bucketSize, Math.min((b+1)*bucketSize, nodes.size()))));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Defined buckets");
            for (Bucket b : buckets) {
                logger.debug(b.toString());
            }
        }
    }

    public short getBucketForKey(byte[] key) {
        // this works because for now numBuckets doesn't change
        // otherwise we would need to apply consistent hashing
        return (short) Math.floorMod(hashFunction.hashBytes(key).asInt(), numBuckets);
    }

    public Node getMasterOfBucket(short bucketID) throws InvalidBucketException {
        validateBucket(bucketID);
        return buckets[bucketID].getMaster();
    }

    public Node getMasterForKey(byte[] key) {
        return buckets[getBucketForKey(key)].getMaster();
    }

    public Bucket getBucketOfNode(Node node) {
        for (Bucket b: buckets) {
            if (b.containsNode(node)) {
                return b;
            }
        }
        return null; //maybe raise exception?
    }

    public Bucket getBucket(short bucketID) throws InvalidBucketException {
        validateBucket(bucketID);
        return buckets[bucketID];
    }

    private void validateBucket(short bucket) throws InvalidBucketException {
        if (bucket < 0 || bucket >= numBuckets)
            throw new InvalidBucketException("Bucket out of bounds, DHT has " + numBuckets + " buckets, requested " + bucket);
    }

    public Set<Node> getMasters() {
        HashSet<Node> masters = new HashSet<>();
        for (Bucket b : this.buckets) {
            masters.add(b.getMaster());
        }
        return masters;
    }

    public Set<Node> getMastersOfBuckets(short[] buckets) throws InvalidBucketException {
        HashSet<Node> masters = new HashSet<>();
        for (short b : buckets) {
            masters.add(getMasterOfBucket(b));
        }
        return masters;
    }
}
