package pt.ulisboa.tecnico.sconekv.common.transaction;

public class ReadOperation extends Operation {

    public ReadOperation(String key, short version) {
        super(key, version);
    }

}
