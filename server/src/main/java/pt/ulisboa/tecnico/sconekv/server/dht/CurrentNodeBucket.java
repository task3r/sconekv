package pt.ulisboa.tecnico.sconekv.server.dht;

import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.ulisboa.tecnico.sconekv.common.dht.Bucket;

import java.util.TreeSet;

public class CurrentNodeBucket extends Bucket {

    public CurrentNodeBucket(short id, TreeSet<Node> nodes) {
        super(id, nodes);
    }
}
