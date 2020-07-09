package pt.ulisboa.tecnico.sconekv.common.dht;

import pt.tecnico.ulisboa.prime.membership.ring.Node;

import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * SconeKV DHT Bucket representation, containing
 *      - bucket id in the DHT
 *      - section of the membership Ring as  a Set of Nodes
 */
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

    /**
     * Get nodes from bucket except one.
     * Typically used by a node to get the others in the same bucket.
     * @param node exception
     * @return the other nodes from bucket
     */
    public SortedSet<Node> getNodesExcept(Node node) {
        SortedSet<Node> others = new TreeSet<>(nodes);
        others.remove(node);
        return others;
    }

    /**
     * Get the master of the bucket.
     * Decided as the element with the lowest id.
     * **Subject to change, ideally would be the oldest node (in the membership) form this bucket**
     * @return master Node
     */
    public Node getMaster() {
        // avoid NoSuchElement
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
