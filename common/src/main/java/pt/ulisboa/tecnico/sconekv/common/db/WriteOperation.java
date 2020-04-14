package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class WriteOperation extends Operation {
    byte[] value;

    public WriteOperation(String key, short version, byte[] value) {
        super(key, version);
        this.value = value;
    }

    public WriteOperation(Message.Operation.Reader reader) {
        super(reader.getKey().toString(), reader.getVersion());
        this.value = reader.getWrite().toArray();
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public void serialize(Message.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setWrite(value);
    }
}
