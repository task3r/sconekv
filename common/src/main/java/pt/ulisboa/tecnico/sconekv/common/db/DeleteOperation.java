package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Common;

public class DeleteOperation extends Operation {

    public DeleteOperation(String key, short version, byte[] value) {
        super(key, version, value);
    }

    public DeleteOperation(Common.Operation.Reader reader) {
        super(new String(reader.getKey().toArray()), reader.getVersion(), null);
    }

    @Override
    public void serialize(Common.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setDelete(null);
    }
}
