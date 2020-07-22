package pt.ulisboa.tecnico.sconekv.common.db;

import pt.ulisboa.tecnico.sconekv.common.transport.Common;

/*
    SconeKV Operation abstraction
 */
public abstract class Operation {
    public enum Type {
        READ,
        WRITE
    }

    private String key;
    private short version;
    private byte[] value;
    private Type type;

    public Operation(String key, short version, Type type) {
        this(key, version, null, type);
    }

    public Operation(String key, short version, byte[] value, Type type) {
        this.key = key;
        this.version = version;
        this.value = value;
        this.type = type;
    }

    public static Operation unserialize(Common.Operation.Reader op) {
        switch (op.which()) {
            case WRITE:
                return new WriteOperation(op);
            case READ:
                return new ReadOperation(op);
            case DELETE:
                return new DeleteOperation(op);
            default:
                throw new IllegalStateException("Unexpected operation: " + op.which());
        }
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

    public Type getType() {
        return type;
    }

    public abstract void serialize(Common.Operation.Builder builder);
}
