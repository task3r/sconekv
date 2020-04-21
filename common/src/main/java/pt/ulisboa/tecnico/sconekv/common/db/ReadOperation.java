package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class ReadOperation extends Operation {

    public ReadOperation(String key, short version) {
        super(key, version);
    }

    public ReadOperation(String key, short version, byte[] value) {
        super(key, version, value);
    }

    public ReadOperation(Message.Operation.Reader reader) {
        super(new String(reader.getKey().toArray()), reader.getVersion());
    }

    @Override
    public void serialize(Message.Operation.Builder builder) {
        builder.setKey(getKey().getBytes());
        builder.setVersion(getVersion());
        builder.setRead(null);
    }

}
