package pt.ulisboa.tecnico.sconekv.client;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static pt.ulisboa.tecnico.sconekv.common.utils.PropertiesUtils.*;

public class SconeClientProperties {
    private static final Logger logger = LoggerFactory.getLogger(SconeClientProperties.class);

    public final int RECV_TIMEOUT;
    public final int MAX_REQUEST_RETRIES;
    public final int SERVER_REQUEST_PORT;
    public final String TRACKER_URL;

    public SconeClientProperties(String filename) throws IOException {
        Properties properties = new Properties();

            try (InputStream input = new FileInputStream(filename)) {
                properties.load(input);

                RECV_TIMEOUT = getInt(properties, "RECV_TIMEOUT");
                MAX_REQUEST_RETRIES = getInt(properties, "MAX_REQUEST_RETRIES");
                SERVER_REQUEST_PORT = getInt(properties, "SERVER_REQUEST_PORT");
                TRACKER_URL = getString(properties, "TRACKER_URL");
            } catch (IOException e) {
                logger.error("Error loading properties.");
                throw e;
        }
    }
}
