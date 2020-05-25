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
import java.util.ArrayList;
import java.util.TreeSet;

public class DHT {
    private static final Logger logger = LoggerFactory.getLogger(DHT.class);

    private TreeSet<Node> nodes;
    private Version viewVersion;
    private short numBuckets;
    private HashFunction hashFunction;
    private Bucket[] buckets;
    private int murmurSeed;

    public DHT(Ring ring, short numBuckets, int murmurSeed) {
        this.nodes = ring.asSet();
        this.viewVersion = ring.getVersion();
        this.numBuckets = numBuckets;
        this.murmurSeed = murmurSeed;
        this.hashFunction = Hashing.murmur3_128(murmurSeed);
        defineBuckets();
    }

    public DHT(TreeSet<Node> nodes, Version version, short numBuckets, int murmurSeed) {
        this.nodes = nodes;
        this.viewVersion = version;
        this.numBuckets = numBuckets;
        this.murmurSeed = murmurSeed;
        this.hashFunction = Hashing.murmur3_128(murmurSeed);
        defineBuckets();
    }

    public DHT(Common.DHT.Reader dht) {
        this.nodes = new TreeSet<>();
        for (Common.Node.Reader node : dht.getNodes()) {
            try {
                this.nodes.add(SerializationUtils.getNodeFromMessage(node));
            } catch (UnknownHostException e) {
                logger.error("Received invalid node, ignoring {} {}", node.getId(), node.getAddress());
            }
        }
        this.viewVersion = SerializationUtils.getVersionFromMesage(dht.getVersion());
        this.numBuckets = dht.getNumBuckets();
        this.murmurSeed = dht.getMurmurSeed();
        this.hashFunction = Hashing.murmur3_128(dht.getMurmurSeed());
        defineBuckets();
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

    public synchronized void applyView(TreeSet<Node> newView, Version newVersion) {
        if (this.viewVersion.isLesser(newVersion)) {
            this.nodes = newView;
            this.viewVersion = newVersion;
            defineBuckets();
        } else {
            logger.error("Tried adding a view with an earlier version. Ignored");
        }
    }

    public synchronized void applyView(Ring ring) {
        applyView(ring.asSet(), ring.getVersion());
    }

    public synchronized void applyView(Common.DHT.Reader dht) {
        DHT newDHT = new DHT(dht);
        this.applyView(newDHT.nodes, newDHT.viewVersion);
    }

    private void defineBuckets() {
        buckets = new Bucket[numBuckets];
        int bucketSize = nodes.size() / numBuckets;
        ArrayList<Node> aux = new ArrayList<>(nodes);
        logger.debug("numBuckets {} nodes {}", numBuckets, nodes);
        for (short b = 0; b < numBuckets; b++) {
            buckets[b] = new Bucket(b, new TreeSet<>(aux.subList(b*bucketSize, Math.min((b+1)*bucketSize, nodes.size()))));
        }

    }

    public short getBucketForKey(byte[] key) {
        // this works because for now numBuckets doesn't change
        // otherwise we would need to apply consistent hashing
        return (short) (hashFunction.hashBytes(key).asInt()  % numBuckets);
    }

    public Node getMasterOfBucket(short bucket) throws InvalidBucketException {
        if (bucket >= numBuckets) {
            throw new InvalidBucketException();
        }
        return buckets[bucket].getMaster();
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
}
