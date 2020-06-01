package pt.ulisboa.tecnico.sconekv.server;

import epto.PSSParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.tecnico.ulisboa.prime.constants.EpTOConstants;
import pt.tecnico.ulisboa.prime.constants.PSSConstants;
import pt.tecnico.ulisboa.prime.constants.PrimeConstants;
import pt.ulisboa.tecnico.sconekv.common.SconeConstants;
import pt.ulisboa.tecnico.sconekv.server.exceptions.InvalidConfigurationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesConfigurator {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfigurator.class);

    private PropertiesConfigurator() {
    }

    public static void loadProperties(String filename) throws IOException, InvalidConfigurationException {
        Properties properties = new Properties();

        try (InputStream input = new FileInputStream(filename)) {
            properties.load(input);

            configurePSS(properties);
            configureEpTO(properties);
            configurePrime(properties);
            configureScone(properties);


        } catch (IOException e) {
            logger.error("Error loading properties.");
            throw e;
        }
    }

    private static void configureScone(Properties properties) throws InvalidConfigurationException {
        SconeConstants.NUM_BUCKETS = getShort(properties, "NUM_BUCKETS");
        SconeConstants.BUCKET_SIZE = getInt(properties, "BUCKET_SIZE");
        SconeConstants.FAILURES_PER_BUCKET = getInt(properties, "FAILURES_PER_BUCKET");
        SconeConstants.BOOTSTRAP_NODE_NUMBER = getInt(properties, "BOOTSTRAP_NODE_NUMBER");
        SconeConstants.MURMUR3_SEED = getInt(properties, "MURMUR3_SEED");
        SconeConstants.SERVER_REQUEST_PORT = getInt(properties, "SERVER_REQUEST_PORT");
        SconeConstants.SERVER_INTERNAL_PORT = getInt(properties, "SERVER_INTERNAL_PORT");
        SconeConstants.NUM_WORKERS = getShort(properties, "NUM_WORKERS");
        SconeConstants.TRACKER_URL = getString(properties, "TRACKER_URL");
        validateSconeConfiguration();
    }

    private static void validateSconeConfiguration() throws InvalidConfigurationException {
        if (SconeConstants.BUCKET_SIZE <= 2 * SconeConstants.FAILURES_PER_BUCKET)
            throw new InvalidConfigurationException("BUCKET_SIZE must be greater than 2 * F");
    }

    private static void configurePSS(Properties properties) {
        PSSConstants.GOSSIP_INTERVAL = getInt(properties, "GOSSIP_INTERVAL");
        PSSConstants.C = getInt(properties, "C");
        PSSConstants.H = getInt(properties, "H");
        PSSConstants.S = getInt(properties, "S");
        PSSConstants.EXCH = getInt(properties, "EXCH");
        PSSConstants.PORT = getInt(properties, "PORT");
        PSSConstants.BOOTSTRAP_PEER_NUMBER = getInt(properties, "BOOTSTRAP_PEER_NUMBER");
        PSSConstants.TRACKER_URL = getString(properties, "TRACKER_URL");
    }

    private static void configureEpTO(Properties properties) {
        EpTOConstants.FANOUT = getInt(properties, "FANOUT");
        EpTOConstants.FANIN = getInt(properties, "FANIN");
        EpTOConstants.DELTA = getLong(properties, "DELTA");
        EpTOConstants.GOSSIP_PORT = getInt(properties, "GOSSIP_PORT");
        EpTOConstants.PUSH_TTL = getInt(properties, "PUSH_TTL");
        EpTOConstants.PULL_TTL = getInt(properties, "PULL_TTL");
        EpTOConstants.SCALE = getFloat(properties, "SCALE");
        EpTOConstants.PSS_PARAMETERS = new PSSParams(PSSConstants.GOSSIP_INTERVAL, PSSConstants.C, PSSConstants.H,
                                                     PSSConstants.S, PSSConstants.EXCH, PSSConstants.PORT,
                                                     PSSConstants.BOOTSTRAP_PEER_NUMBER, PSSConstants.TRACKER_URL);
    }

    private static void configurePrime(Properties properties) {
        PrimeConstants.MAX_QUEUE_SIZE_TO_BATCH = getInt(properties, "MAX_QUEUE_SIZE_TO_BATCH");
        PrimeConstants.SO_TIMEOUT = getInt(properties, "SO_TIMEOUT");
        PrimeConstants.NUMBER_OF_PROTEGEES = getInt(properties, "NUMBER_OF_PROTEGEES");
        PrimeConstants.NUMBER_OF_GUARDIANS = getInt(properties, "NUMBER_OF_GUARDIANS");
        PrimeConstants.NUMBER_OF_EpTO_DELIVER_UNTIL_REQUEST_SNAPSHOT_AFTER_BOOTSTRAP = getInt(properties, "NUMBER_OF_EpTO_DELIVER_UNTIL_REQUEST_SNAPSHOT_AFTER_BOOTSTRAP");
        PrimeConstants.NUMBER_OF_EpTO_DELIVER_UNTIL_REQUEST_SNAPSHOT = getInt(properties, "NUMBER_OF_EpTO_DELIVER_UNTIL_REQUEST_SNAPSHOT");
        PrimeConstants.NUMBER_OF_JOINS_WITHOUT_ME_UNTIL_REJOIN_AFTER_BOOTSTRAP = getInt(properties, "NUMBER_OF_JOINS_WITHOUT_ME_UNTIL_REJOIN_AFTER_BOOTSTRAP");
        PrimeConstants.NUMBER_OF_JOINS_WITHOUT_ME_UNTIL_REJOIN = getInt(properties, "NUMBER_OF_JOINS_WITHOUT_ME_UNTIL_REJOIN");
        PrimeConstants.DELAY_TO_REQUEST_SNAPSHOT_AFTER_BOOTSTRAP = getInt(properties, "DELAY_TO_REQUEST_SNAPSHOT_AFTER_BOOTSTRAP");
        PrimeConstants.DELAY_TO_REQUEST_SNAPSHOT = getInt(properties, "DELAY_TO_REQUEST_SNAPSHOT");
        PrimeConstants.DELAY_TO_REJOIN_AFTER_BOOTSTRAP = getInt(properties, "DELAY_TO_REJOIN_AFTER_BOOTSTRAP");
        PrimeConstants.DELAY_TO_REJOIN = getInt(properties, "DELAY_TO_REJOIN");
        PrimeConstants.JOIN_MAX_ATTEMPTS = getInt(properties, "JOIN_MAX_ATTEMPTS");
        PrimeConstants.BOOTSTRAP_DURATION = getInt(properties, "BOOTSTRAP_DURATION");
        PrimeConstants.HEARTBEAT_INTERVAL_MAX = getInt(properties, "HEARTBEAT_INTERVAL_MAX");
        PrimeConstants.HEARTBEAT_INTERVAL_MIN = getInt(properties, "HEARTBEAT_INTERVAL_MIN");
        PrimeConstants.HEALTH_CHECKING_INTERVAL_MAX = getInt(properties, "HEALTH_CHECKING_INTERVAL_MAX");
        PrimeConstants.HEALTH_CHECKING_INTERVAL_MIN = getInt(properties, "HEALTH_CHECKING_INTERVAL_MIN");
        PrimeConstants.ACCEPTABLE_HEARTBEAT_PAUSE = getInt(properties, "ACCEPTABLE_HEARTBEAT_PAUSE");
        PrimeConstants.FIRST_HEARTBEAT_ESTIMATE = getInt(properties, "FIRST_HEARTBEAT_ESTIMATE");
        PrimeConstants.MAX_SAMPLE_SIZE_FD = getInt(properties, "MAX_SAMPLE_SIZE_FD");
        PrimeConstants.TRACKER_CONTACT_MAX_ATTEMPTS = getInt(properties, "TRACKER_CONTACT_MAX_ATTEMPTS");
        PrimeConstants.UDP_PORT = getInt(properties, "UDP_PORT");
        PrimeConstants.HEARTBEAT_PORT = getInt(properties, "HEARTBEAT_PORT");
        PrimeConstants.SNAPSHOT_PORT = getInt(properties, "SNAPSHOT_PORT");
        PrimeConstants.FAILURE_MANAGER_PORT = getInt(properties, "FAILURE_MANAGER_PORT");
        PrimeConstants.FIX_VIEW_PORT = getInt(properties, "FIX_VIEW_PORT");
        PrimeConstants.MURMUR3_SEED = getInt(properties, "MURMUR3_SEED");
        PrimeConstants.DELTA_REQUEST_TIMEOUT = getInt(properties, "DELTA_REQUEST_TIMEOUT");
        PrimeConstants.TRIES_DELTA = getInt(properties, "TRIES_DELTA");
        PrimeConstants.TIME_BETWEEN_TRACKER_TRIES = getLong(properties, "TIME_BETWEEN_TRACKER_TRIES");
        PrimeConstants.HISTORY_SIZE = getLong(properties, "HISTORY_SIZE");
        PrimeConstants.INTERVAL_BETWEEN_MONITOR_TRIES = getLong(properties, "INTERVAL_BETWEEN_MONITOR_TRIES");
    }

    private static int getInt(Properties properties, String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    private static short getShort(Properties properties, String key) {
        return Short.parseShort(properties.getProperty(key));
    }

    private static float getFloat(Properties properties, String key) {
        return Float.parseFloat(properties.getProperty(key));
    }

    private static long getLong(Properties properties, String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    private static String getString(Properties properties, String key) {
        return properties.getProperty(key);
    }
}
