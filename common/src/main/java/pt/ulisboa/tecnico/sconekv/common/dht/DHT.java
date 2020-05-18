package pt.ulisboa.tecnico.sconekv.common.dht;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Ring;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;

import java.util.TreeSet;

public class DHT {
    private static final Logger logger = LoggerFactory.getLogger(DHT.class);

    private Ring ring;
    private short numBuckets;
    private HashFunction hashFunction;
    private Bucket[] buckets;

    public DHT(Ring ring, short numBuckets, int murmurSeed) {
        this.ring = ring;
        this.numBuckets = numBuckets;
        this.hashFunction = Hashing.murmur3_128(murmurSeed);
        defineBuckets();
    }

    public synchronized void applyView(Ring ring) {
        if (this.ring.getVersion().isLesser(ring.getVersion())) {
            this.ring = ring;
            defineBuckets();
        } else {
            logger.error("Tried adding a view with an earlier version. Ignored");
        }
    }

    private void defineBuckets() {
        buckets = new Bucket[numBuckets];
        int bucketSize = ring.size() / numBuckets;
        for (short b = 0; b < numBuckets; b++) {
            buckets[b] = new Bucket(b, new TreeSet<>(ring.asList().subList(b*bucketSize, Math.min((b+1)*bucketSize, ring.size()))));
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
