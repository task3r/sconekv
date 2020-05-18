package pt.ulisboa.tecnico.sconekv.common.dht;

import pt.tecnico.ulisboa.prime.membership.ring.Node;

import java.util.TreeSet;

public class Bucket {
    private short id;
    private TreeSet<Node> nodes;

    public Bucket(short id, TreeSet<Node> nodes) {
        this.id = id;
        this.nodes = nodes;
    }

    public Node getMaster() {
        // avoid NoSuchElement
        // decided that master is the one with lowest id, subject to change
        return nodes.isEmpty() ? null : nodes.first();
    }
}
