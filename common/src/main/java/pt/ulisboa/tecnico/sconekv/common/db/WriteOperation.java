package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class WriteOperation extends Operation {
    byte[] value;

    public WriteOperation(String key, short version, byte[] value) {
        super(key, version);
        this.value = value;
    }

    public WriteOperation(Message.Operation.Reader reader) {
        super(reader.getWrite().getKey().toString(), reader.getVersion());
        this.value = reader.getWrite().getValue().toArray();
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public void serialize(Message.Operation.Builder builder) {
        builder.setVersion(getVersion());
        Message.Write.Builder wBuilder = builder.initWrite();
        wBuilder.setKey(getKey().getBytes());
        wBuilder.setValue(value);
    }
}
