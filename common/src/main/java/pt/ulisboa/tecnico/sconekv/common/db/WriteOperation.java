package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class WriteOperation extends Operation {

    public WriteOperation(String key, short version, byte[] value) {
        super(key, version, value);
    }

    public WriteOperation(Message.Operation.Reader reader) {
        super(new String(reader.getKey().toArray()), reader.getVersion(), reader.getWrite().toArray());
    }

    @Override
    public void serialize(Message.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setWrite(getValue());
    }
}
