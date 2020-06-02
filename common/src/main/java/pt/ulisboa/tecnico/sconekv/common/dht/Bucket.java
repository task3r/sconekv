package pt.ulisboa.tecnico.sconekv.common.dht;

import pt.tecnico.ulisboa.prime.membership.ring.Node;

import java.util.Objects;
import java.util.TreeSet;

public class Bucket {
    private short id;
    private TreeSet<Node> nodes;

    public Bucket(short id, TreeSet<Node> nodes) {
        this.id = id;
        this.nodes = nodes;
    }

    public short getId() {
        return id;
    }

    public TreeSet<Node> getNodes() {
        return nodes;
    }

    public TreeSet<Node> getNodesExcept(Node self) {
        TreeSet<Node> withoutSelf = new TreeSet<>(nodes);
        withoutSelf.remove(self);
        return withoutSelf;
    }

    public Node getMaster() {
        // avoid NoSuchElement
        // decided that master is the one with lowest id, subject to change
        return nodes.isEmpty() ? null : nodes.first();
    }

    public boolean containsNode(Node node) {
        return nodes.contains(node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bucket bucket = (Bucket) o;
        return id == bucket.id &&
                Objects.equals(nodes, bucket.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, nodes);
    }
}
