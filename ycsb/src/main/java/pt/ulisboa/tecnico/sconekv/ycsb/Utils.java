package pt.ulisboa.tecnico.sconekv.ycsb;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for SconeKV YCSB client
 * adapted from RiakUtils, from the Riak KV YCSB binding
 */
public class Utils {
    private Utils() {}

    /**
     * Function that retrieves all the fields searched within a read or scan operation and puts them in the result
     * HashMap.
     *
     * @param fields        The list of fields to read, or null for all of them.
     * @param response      The byte array containing the value read from the database.
     * @param resultHashMap The HashMap to return as result.
     */
    static void createResultHashMap(Set<String> fields, byte[] response,
                                    Map<String, ByteIterator> resultHashMap) {
        // Deserialize the stored response table.
        HashMap<String, ByteIterator> deserializedTable = new HashMap<>();
        deserializeTable(response, deserializedTable);

        // If only specific fields are requested, then only these should be put in the result object!
        if (fields != null) {
            // Populate the HashMap to provide as result.
            for (String field : fields) {
                // Comparison between a requested field and the ones retrieved. If they're equal (i.e. the get() operation
                // DOES NOT return a null value), then  proceed to store the pair in the resultHashMap.
                ByteIterator value = deserializedTable.get(field);

                if (value != null) {
                    resultHashMap.put(field, value);
                }
            }
        } else {
            // If, instead, no field is specified, then all those retrieved must be provided as result.
            for (Map.Entry<String, ByteIterator> entry : deserializedTable.entrySet()) {
                resultHashMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Serializes a Map, transforming the contained list of (String, ByteIterator) couples into a byte array.
     *
     * @param table A Map to serialize.
     * @return A byte array containing the serialized table.
     */
    static byte[] serializeTable(Map<String, ByteIterator> table) {
        final Set<Map.Entry<String, ByteIterator>> entries = table.entrySet();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()){
            for (final Map.Entry<String, ByteIterator> entry : entries) {
                final byte[] column = entry.getKey().getBytes();

                baos.write(toBytes(column.length));
                baos.write(column);

                final byte[] value = entry.getValue().toArray();

                baos.write(toBytes(value.length));
                baos.write(value);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Deserializes an input byte array, transforming it into a list of (String, ByteIterator) pairs (i.e. a Map).
     *
     * @param bytes    A byte array containing the table to deserialize.
     * @param result   A Map containing the deserialized table.
     */
    private static void deserializeTable(final byte[] bytes, final Map<String, ByteIterator> result) {
        byte[] buffer = new byte[4];

        try (final ByteArrayInputStream anInputStream = new ByteArrayInputStream(bytes)){
            while (anInputStream.available() > 0) {
                anInputStream.read(buffer);
                final int columnLength = fromBytes(buffer);

                final byte[] column = new byte[columnLength];
                anInputStream.read(column);

                anInputStream.read(buffer);
                final int valueLength = fromBytes(buffer);

                final byte[] value = new byte[valueLength];
                anInputStream.read(value);

                result.put(new String(column), new ByteArrayByteIterator(value));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] toBytes(final int integer) {
        byte[] result = new byte[4];

        result[0] = (byte) (integer >> 24);
        result[1] = (byte) (integer >> 16);
        result[2] = (byte) (integer >> 8);
        result[3] = (byte) (integer /* >> 0 */);

        return result;
    }

    private static int fromBytes(final byte[] buffer) {
        if (buffer.length != 4)
            throw new IllegalArgumentException();

        return (buffer[0] << 24) | (buffer[1] & 0xFF) << 16 | (buffer[2] & 0xFF) << 8 | (buffer[3] & 0xFF);
    }
}
