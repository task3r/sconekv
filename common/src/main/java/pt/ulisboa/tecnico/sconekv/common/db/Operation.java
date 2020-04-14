package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public abstract class Operation {
    private String key;
    private short version;

    public Operation(String key, short version) {
        this.key = key;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public short getVersion() {
        return version;
    }

    public abstract void serialize(Message.Operation.Builder builder);
}
