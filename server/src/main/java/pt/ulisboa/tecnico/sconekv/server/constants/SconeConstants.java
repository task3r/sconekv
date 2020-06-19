package pt.ulisboa.tecnico.sconekv.server.constants;

public class SconeConstants {

    public static int SERVER_REQUEST_PORT = 5555;
    public static int SERVER_INTERNAL_PORT = 6666;
    public static short NUM_BUCKETS = 1;
    public static int BUCKET_SIZE = 4;
    public static int FAILURES_PER_BUCKET = 1;
    public static int BOOTSTRAP_NODE_NUMBER = NUM_BUCKETS * BUCKET_SIZE;
    public static int MURMUR3_SEED = 42;
    public static int MAX_OP_NUMBER_HOLE = 1;
    public static short NUM_WORKERS = 1;
    public static String TRACKER_URL = "http://tracker:4321";


    private SconeConstants() {}
}