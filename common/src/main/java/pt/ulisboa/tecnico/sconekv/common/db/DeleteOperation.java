package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Common;

/**
 * Represents a SconeKV Delete operation
 *     - the chosen behaviour is to remove the actual key from the system
 *     - consequent reads of the same key will obtain [] with version 0
 *     - next write on the same key will result in version 1
 */
public class DeleteOperation extends Operation {

    public DeleteOperation(String key, short version, byte[] value) {
        super(key, version, value, Type.WRITE);
    }

    public DeleteOperation(Common.Operation.Reader reader) {
        super(new String(reader.getKey().toArray()), reader.getVersion(), null, Type.WRITE);
    }

    @Override
    public void serialize(Common.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setDelete(null);
    }
}
