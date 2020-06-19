package pt.ulisboa.tecnico.sconekv.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.server.constants.PropertiesConfigurator;
import pt.ulisboa.tecnico.sconekv.server.management.SconeManager;

import java.util.concurrent.TimeUnit;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);
    private static final int AWAIT_DEBUGGER = 5;

    private static SconeManager sm;

    public static void main(String[] args) throws Exception {

        String debug = System.getenv("DEBUG");

        if (debug != null && debug.equals("REMOTE")) {
            logger.info("Sleeping for {}s waiting for remote debugger...", AWAIT_DEBUGGER);
            TimeUnit.SECONDS.sleep(AWAIT_DEBUGGER);
        }

        logger.info("Launching server...");

        // configure logger termination to ignore default shutdown hook
        final LoggerContextFactory factory = LogManager.getFactory();
        if (factory instanceof Log4jContextFactory) {
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;
            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }

        handleSigterm();
        PropertiesConfigurator.loadProperties("config.properties");

        sm = new SconeManager();
    }

    private static void handleSigterm() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (sm != null) {
                logger.info("Shutting down SconeKV node");
                try {
                    sm.shutdown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                logger.info("SconeManager was not created.");
            }
            LogManager.shutdown();
        }));
    }


}
