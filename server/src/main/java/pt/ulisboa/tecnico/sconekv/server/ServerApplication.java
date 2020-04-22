package pt.ulisboa.tecnico.sconekv.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.server.db.SconeManager;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);

    private static SconeManager sm;
    public static void main(String[] args) throws Exception {

        logger.info("Launching server...");

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
        }));
    }
}
