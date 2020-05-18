package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Common;

public class WriteOperation extends Operation {

    public WriteOperation(String key, short version, byte[] value) {
        super(key, version, value);
    }

    public WriteOperation(Common.Operation.Reader reader) {
        super(new String(reader.getKey().toArray()), reader.getVersion(), reader.getWrite().toArray());
    }

    @Override
    public void serialize(Common.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setWrite(getValue());
    }
}
