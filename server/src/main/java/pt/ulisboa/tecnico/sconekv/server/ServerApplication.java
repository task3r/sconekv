package pt.ulisboa.tecnico.sconekv.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.server.db.SconeServer;

public class ServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ServerApplication.class);

    public static void main(String[] args) throws Exception {

        logger.info("Launching server...");

        try (ZContext context = new ZContext()) {

            SconeServer server = new SconeServer(context);
            server.run();
        }
    }
}
