package pt.ulisboa.tecnico.sconekv.common.dht;

import pt.tecnico.ulisboa.prime.membership.ring.Node;

import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

public class Bucket {
    private short id;
    private SortedSet<Node> nodes;

    public Bucket(short id, SortedSet<Node> nodes) {
        this.id = id;
        this.nodes = nodes;
    }

    public short getId() {
        return id;
    }

    public SortedSet<Node> getNodes() {
        return nodes;
    }

    public SortedSet<Node> getNodesExcept(Node self) {
        SortedSet<Node> withoutSelf = new TreeSet<>(nodes);
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

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("Bucket{id=" + id + ", nodes=[");
        for (Node n : nodes)
            result.append(n).append(",");
        result.append("]}");
        return result.toString();
    }
}
