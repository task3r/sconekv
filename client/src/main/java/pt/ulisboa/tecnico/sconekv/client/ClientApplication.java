package pt.ulisboa.tecnico.sconekv.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.client.db.SconeClient;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;

import java.io.IOException;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);

    public static void main(String[] args) throws IOException {
        logger.info("Launching client application...");

        try (ZContext context = new ZContext()) {

            SconeClient client = new SconeClient(context, "localhost");
            Transaction tx = client.newTransaction();

            byte[] response = tx.read("foo");

            logger.info("response: {}", new String(response));

        }
    }
}
