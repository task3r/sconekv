package pt.ulisboa.tecnico.sconekv.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import epto.PSSParams;
import org.zeromq.ZContext;
import pt.tecnico.ulisboa.prime.constants.EpTOConstants;
import pt.tecnico.ulisboa.prime.constants.PSSConstants;
import pt.tecnico.ulisboa.prime.constants.PrimeConstants;
import pt.ulisboa.tecnico.sconekv.server.db.SconeManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);
    private static final int AWAIT_DEBUGGER = 15;

    private static SconeManager sm;

    public static void main(String[] args) throws Exception {

        String debug = System.getenv("DEBUG");

        if (debug.equals("REMOTE")) {
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
        loadProperties();

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

    private static void loadProperties() {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream("config.properties")) {

            // load a properties file
            prop.load(input);

            for (Field field : PrimeConstants.class.getFields()) {
                try {
                    setField(prop, field);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());
                }
            }

            for (Field field : EpTOConstants.class.getFields()) {
                try {
                    setField(prop, field);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());
                }
            }

            for (Field field : PSSConstants.class.getFields()) {
                try {
                    setField(prop, field);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void setField(Properties properties, Field field) throws IllegalAccessException {
        String property = properties.getProperty(field.getName());
        field.setAccessible(true);

        Class<?> t = field.getType();

        if (t == int.class) {
            field.setInt(null, Integer.valueOf(property));
        } else if (t == long.class) {
            field.setLong(null, Long.valueOf(property));

        } else if (t == float.class) {
            field.setFloat(null, Float.valueOf(property));

        } else if (t == String.class) {
            field.set(null, property);
        } else if (t == PSSParams.class){
            logger.debug("{} {}", t, field);
            field.set(null, new PSSParams(5000, 1, 3, 9, 12, 9877, 1, "http://tracker:4321"));
        }

    }
}
