package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public class ReadOperation extends Operation {

    public ReadOperation(String key, short version) {
        super(key, version);
    }

    public ReadOperation(Message.Operation.Reader reader) {
        super(reader.getRead().getKey().toString(), reader.getVersion());
    }

    @Override
    public void serialize(Message.Operation.Builder builder) {
        builder.setVersion(getVersion());
        builder.initRead().setKey(getKey().getBytes());
    }

}
