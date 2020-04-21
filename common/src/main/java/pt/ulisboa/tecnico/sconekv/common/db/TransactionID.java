package pt.ulisboa.tecnico.sconekv.common.db;

import org.jetbrains.annotations.NotNull;
import pt.ulisboa.tecnico.sconekv.common.transport.Message;

import java.util.Objects;
import java.util.UUID;

public class TransactionID implements Comparable<TransactionID> {
    private UUID client;
    private int localID;

    public TransactionID(UUID client, int localID) {
        this.client = client;
        this.localID = localID;
    }

    public TransactionID(Message.TransactionID.Reader txID) {
        this.client = new UUID(txID.getMostSignificant(), txID.getLeastSignificant());
        this.localID = txID.getLocalID();
    }

    public UUID getClient() {
        return client;
    }

    public int getLocalID() {
        return localID;
    }

    public void serialize(Message.TransactionID.Builder builder) {
        builder.setMostSignificant(this.client.getMostSignificantBits());
        builder.setLeastSignificant(this.client.getLeastSignificantBits());
        builder.setLocalID(this.localID);
    }


    public boolean isLesser(TransactionID other) {
        return compareTo(other) < 0;
    }

    public boolean isEqual(TransactionID other) {
        return compareTo(other) == 0;
    }

    public boolean isGreater(TransactionID other) {
        return compareTo(other) > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionID that = (TransactionID) o;
        return getLocalID() == that.getLocalID() &&
                getClient().equals(that.getClient());
    }

    @Override
    public int compareTo(@NotNull TransactionID other) {
        if (this.localID == other.localID)
            return this.client.compareTo(other.client);
        return Integer.compare(this.localID, other.localID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClient(), getLocalID());
    }

    @Override
    public String toString() {
        return "<" + client + "," + localID + '>';
    }

}
