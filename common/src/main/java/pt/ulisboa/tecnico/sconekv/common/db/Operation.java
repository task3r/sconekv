package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Common;

public abstract class Operation {
    private String key;
    private short version;
    private byte[] value;

    public Operation(String key, short version) {
        this.key = key;
        this.version = version;
    }

    public Operation(String key, short version, byte[] value) {
        this.key = key;
        this.version = version;
        this.value = value;
    }

    public static Operation unserialize(Common.Operation.Reader op) {
        switch (op.which()) {
            case WRITE:
                return new WriteOperation(op);
            case READ:
                return new ReadOperation(op);
            case DELETE:
                return new DeleteOperation(op);
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

    public byte[] getValue() {
        return value;
    }

    public abstract void serialize(Common.Operation.Builder builder);
}
