package pt.ulisboa.tecnico.sconekv.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.server.db.SconeManager;

import java.util.concurrent.TimeUnit;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);
    private static final int AWAIT_DEBUGGER = 15;

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
            logger.info("register shutdown hook");
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;

            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }

        handleSigterm();
        PropertiesConfigurator.loadProperties("config.properties");

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
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.debug("kill logger");
            LogManager.shutdown();
        }));
    }


}
