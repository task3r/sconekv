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

/**
 * SconeKV Distributed Hash Table representation
 *      - (server-side) it is built using the Ring provided by PRIME which contains all the nodes in the membership
 *      - (client-side) built using capnproto dht representation, given by a server
 *      - the ring is then divided into numBuckets sections, each becoming a Bucket
 */
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

    /**
     * Server-side constructor
     * @param ring obtained from PRIME
     * @param numBuckets number of entries in the DHT
     * @param murmurSeed used to hash the keys and determine the corresponding bucket
     */
    public DHT(Ring ring, short numBuckets, int murmurSeed) {
        this(ring.asSet(), ring.getVersion(), numBuckets, murmurSeed);
    }


    /**
     * Client-side constructor
     * @param dht capnproto representation obtained from a server message
     */
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

    public synchronized void serialize(Common.DHT.Builder builder) {
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

    /**
     * (server-side) Apply new view from the PRIME membership layer
     * @param ring to apply
     */
    public synchronized void applyView(Ring ring) {
        if (checkVersion(ring.getVersion()))
            changeView(ring.asSet(), ring.getVersion());
    }

    /**
     * (client-side) Apply the DHT received from a server node
     * @param dht to apply
     */
    public synchronized void applyView(Common.DHT.Reader dht) {
        Version receivedVersion = SerializationUtils.getVersionFromMesage(dht.getVersion());
        if (checkVersion(receivedVersion)) {
            changeView(getNodes(dht), receivedVersion);
        }
    }

    /**
     * Change the version a redefine the buckets according to the new view
     * @param newView to apply
     * @param newVersion of the newly applied view
     */
    private synchronized void changeView(SortedSet<Node> newView, Version newVersion) {
        this.nodes = newView;
        this.viewVersion = newVersion;
        defineBuckets();
    }

    /**
     * Determine if the received version is newer than the current version
     * @param newVersion received
     * @return true or false
     */
    private boolean checkVersion(Version newVersion) {
        return this.viewVersion.isLesser(newVersion);
    }

    /**
     * Define the buckets after a new view was applied
     * divides the ring into numBucket sections
     */
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

    /**
     * Obtain the bucket corresponding to a certain key
     * this direct hashing works because for now numBuckets does not change,
     * otherwise we would need to apply consistent hashing for better performance in case of changes in the membership
     * @param key to operate on
     * @return index of the corresponding bucket
     */
    public short getBucketForKey(byte[] key) {
        return (short) Math.floorMod(hashFunction.hashBytes(key).asInt(), numBuckets);
    }

    /**
     * Obtain the master node for a bucket
     * @param bucketID to get master of
     * @return master node
     * @throws InvalidBucketException if the index is out of bounds
     */
    public Node getMasterOfBucket(short bucketID) throws InvalidBucketException {
        validateBucket(bucketID);
        return buckets[bucketID].getMaster();
    }

    /**
     * Obtain the master node of the bucket corresponding to key
     * @param key to get master of
     * @return master node
     */
    public Node getMasterForKey(byte[] key) {
        return buckets[getBucketForKey(key)].getMaster();
    }

    /**
     * Get the bucket were the node belongs
     * @param node to get bucket of
     * @return bucket or null if the node is not part of the membership
     */
    public Bucket getBucketOfNode(Node node) {
        for (Bucket b: buckets) {
            if (b.containsNode(node)) {
                return b;
            }
        }
        return null; //maybe raise exception?
    }

    /**
     * Obtain the bucket indexed by the given id
     * @param bucketID to obtain
     * @return bucket
     * @throws InvalidBucketException if the index is outside of the bounds
     */
    public Bucket getBucket(short bucketID) throws InvalidBucketException {
        validateBucket(bucketID);
        return buckets[bucketID];
    }

    /**
     * Determine if the index received is inside of the bounds
     * @param bucket to validate
     * @throws InvalidBucketException if it is outside of the bounds
     */
    private void validateBucket(short bucket) throws InvalidBucketException {
        if (bucket < 0 || bucket >= numBuckets)
            throw new InvalidBucketException("Bucket out of bounds, DHT has " + numBuckets + " buckets, requested " + bucket);
    }

    /**
     * Obtain the masters of all DHT buckets
     * @return master nodes
     */
    public Set<Node> getMasters() {
        HashSet<Node> masters = new HashSet<>();
        for (Bucket b : this.buckets) {
            masters.add(b.getMaster());
        }
        return masters;
    }

    /**
     * Obtain masters for a selection of bucket indexes
     * @param buckets to get masters of
     * @return master nodes
     * @throws InvalidBucketException if any bucket index is outside of the bounds
     */
    public Set<Node> getMastersOfBuckets(short[] buckets) throws InvalidBucketException {
        HashSet<Node> masters = new HashSet<>();
        for (short b : buckets) {
            masters.add(getMasterOfBucket(b));
        }
        return masters;
    }
}
