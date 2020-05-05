package pt.ulisboa.tecnico.sconekv.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.server.db.SconeManager;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);

    private static SconeManager sm;
    public static void main(String[] args) throws Exception {

        logger.info("Launching server...");

        final LoggerContextFactory factory = LogManager.getFactory();

        if (factory instanceof Log4jContextFactory) {
            logger.info("register shutdown hook");
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;

            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }

        handleSigterm();

        try (ZContext context = new ZContext()) {

            sm = new SconeManager(context);
            sm.run();
        }
    }

    private static void handleSigterm() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (sm != null) {
                logger.info("Shutting down SconeKV node");
                sm.shutdown();
            } else {
                logger.info("SconeManager was not created.");
            }
            logger.debug("kill logger");
            LogManager.shutdown();
        }));
    }
}
