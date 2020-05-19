package pt.ulisboa.tecnico.sconekv.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.io.IOException;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);

    public static void main(String[] args) throws IOException {
        logger.info("Launching client application...");

        try (ZContext context = new ZContext()) {

            SconeClient client = new SconeClient(context, args[0]);

            Transaction tx1 = client.newTransaction();
            tx1.write("foo", "bar".getBytes());

            byte[] r1 = tx1.read("foo");
            logger.info("tx1 read foo: {}", new String(r1));

            tx1.commit();

            Transaction tx2 = client.newTransaction();

            byte[] response = tx2.read("foo");

            logger.info("tx2 read foo response: {}", new String(response));

            tx2.write("bar", response);
            tx2.write("bar", "barfoo".getBytes());

            tx2.commit();

        } catch (CommitFailedException | InvalidTransactionStateChangeException e) {
            e.printStackTrace();
        }
    }
}
