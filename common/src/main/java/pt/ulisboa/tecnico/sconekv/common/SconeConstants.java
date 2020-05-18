package pt.ulisboa.tecnico.sconekv.server;

public class SconeConstants {

    public static short NUM_BUCKETS = 1;
    public static int REPLICATION = 1;
    public static int BOOTSTRAP_NODE_NUMBER = NUM_BUCKETS * REPLICATION;


    private SconeConstants() {}
}
