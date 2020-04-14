package pt.ulisboa.tecnico.sconekv.common.utils;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.SerializePacked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

public final class SerializationUtils {
    private static final Logger logger = LoggerFactory.getLogger(SerializationUtils.class);

    private SerializationUtils() {}

    public static byte[] getBytesFromMessage(MessageBuilder message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializePacked.writeToUnbuffered(Channels.newChannel(baos), message);
        return baos.toByteArray();
    }

    public static MessageReader getMessageFromBytes(byte[] message) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(message);
        return SerializePacked.readFromUnbuffered(Channels.newChannel(bais));
    }

}
