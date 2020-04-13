package pt.ulisboa.tecnico.sconekv.common.transaction;

public class WriteOperation extends Operation{
    byte[] value;

    public WriteOperation(String key, short version, byte[] value) {
        super(key, version);
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
}
