package pt.ulisboa.tecnico.sconekv.server.db;

import pt.ulisboa.tecnico.sconekv.server.exceptions.WriteOutdatedVersionException;

public class Value {

    private byte[] content;
    private short version;

    public Value() {
        this.content = new byte[0];
        this.version = 0;
    }
    public Value(byte[] content, short version) {
        this.content = content;
        this.version = version;
    }

    public void update(byte[] content, int version) throws WriteOutdatedVersionException {
        if (version > this.version)
            this.content = content;
        else
            throw new WriteOutdatedVersionException();
    }

    public byte[] getContent() {
        return content;
    }

    public short getVersion() {
        return version;
    }
}
