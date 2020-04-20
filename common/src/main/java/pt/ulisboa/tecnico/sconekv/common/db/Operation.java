package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Message;

public abstract class Operation {
    private String key;
    private short version;

    public Operation(String key, short version) {
        this.key = key;
        this.version = version;
    }

    public static Operation unserialize(Message.Operation.Reader op) {
        switch (op.which()) {
            case WRITE:
                return new WriteOperation(op);
            case READ:
                return new ReadOperation(op);
            case _NOT_IN_SCHEMA:
                // TODO throw something
                break;
        }
        return null;
    }

    public String getKey() {
        return key;
    }

    public short getVersion() {
        return version;
    }

    public abstract void serialize(Message.Operation.Builder builder);
}
