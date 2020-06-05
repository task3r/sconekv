package pt.ulisboa.tecnico.sconekv.common.utils;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.membership.ring.Node;
import pt.tecnico.ulisboa.prime.membership.ring.Version;
import pt.ulisboa.tecnico.sconekv.common.transport.Common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.util.UUID;

public final class SerializationUtils {
    private static final Logger logger = LoggerFactory.getLogger(SerializationUtils.class);

    private SerializationUtils() {}

    public static byte[] getBytesFromMessage(MessageBuilder message) throws IOException {
        if (message == null)
            throw new IOException("Received null message builder");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializePacked.writeToUnbuffered(Channels.newChannel(baos), message);
        return baos.toByteArray();
    }

    public static MessageReader getMessageFromBytes(byte[] message) throws IOException {
        if (message == null)
            throw new IOException("Received null byte array");
        ByteArrayInputStream bais = new ByteArrayInputStream(message);
        return SerializePacked.readFromUnbuffered(Channels.newChannel(bais));
    }


    public static Node getNodeFromMessage(Common.Node.Reader node) throws UnknownHostException {
        return new Node(InetAddress.getByAddress(node.getAddress().toArray()),
                new UUID(node.getId().getMostSignificant(), node.getId().getLeastSignificant()));
    }

    public static void serializeNode(Common.Node.Builder builder, Node node) {
        builder.getId().setMostSignificant(node.getId().getMostSignificantBits());
        builder.getId().setLeastSignificant(node.getId().getLeastSignificantBits());
        builder.setAddress(node.getAddress().getAddress());
    }

    public static void serializeViewVersion(Common.ViewVersion.Builder builder, Version version) {
        builder.getMessageId().setMostSignificant(version.getMessageId().getMostSignificantBits());
        builder.getMessageId().setLeastSignificant(version.getMessageId().getLeastSignificantBits());
        builder.setTimestamp(version.getTimestamp());
    }

    public static Version getVersionFromMesage(Common.ViewVersion.Reader version) {
        return new Version(version.getTimestamp(), new UUID(version.getMessageId().getMostSignificant(),
                                                            version.getMessageId().getLeastSignificant()));
    }
}
