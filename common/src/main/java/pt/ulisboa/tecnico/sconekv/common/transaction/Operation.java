package pt.ulisboa.tecnico.sconekv.common.transaction;

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
}
