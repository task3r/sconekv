package pt.ulisboa.tecnico.sconekv.common;

public class SconeConstants {

    public static int SERVER_REQUEST_PORT = 5555;
    public static int SERVER_INTERNAL_PORT = 6666;
    public static short NUM_BUCKETS = 1;
    public static int REPLICATION = 1;
    public static int BOOTSTRAP_NODE_NUMBER = NUM_BUCKETS * REPLICATION;
    public static int MURMUR3_SEED = 42;
    public static String TRACKER_URL = "http://tracker:4321";


    private SconeConstants() {}
}
