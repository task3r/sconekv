package pt.ulisboa.tecnico.sconekv.client;

import asg.cliche.Command;
import asg.cliche.Param;
import asg.cliche.ShellFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.MaxRetriesExceededException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.UnableToGetViewException;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidBucketException;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);

    SconeClient client;
    Map<String, Transaction> transactions;

    public ClientApplication() throws InvalidBucketException, UnableToGetViewException {
        client = new SconeClient();
        transactions = new HashMap();
    }

    public static void main(String[] args) throws IOException, InvalidBucketException, UnableToGetViewException {
        logger.info("Launching client application...");

        String shell = System.getenv("USE_SHELL");

        if (shell != null) {
            ShellFactory.createConsoleShell("", "", new ClientApplication()).commandLoop();
        } else {
            try {
                SconeClient client = new SconeClient();

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

            } catch (CommitFailedException | InvalidTransactionStateChangeException | UnableToGetViewException | InvalidBucketException | MaxRetriesExceededException e) {
                e.printStackTrace();
            }
        }
    }

    @Command
    public void createTransation(@Param(name = "txID") String id) {
        if (transactions.containsKey(id)) {
            logger.error("Transaction identifier {} already in use", id);
        } else {
            transactions.put(id, client.newTransaction());
            logger.info("[{}] Created tx", id);
        }
    }

    @Command
    public void write(@Param(name = "txID") String id, @Param(name = "key") String key, @Param(name = "value") String value) throws MaxRetriesExceededException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            transactions.get(id).write(key, value.getBytes());
            logger.info("[{}] Write {}: {}", id, key, value);
        }
    }

    @Command
    public void read(@Param(name = "txID") String id, @Param(name = "key") String key) throws MaxRetriesExceededException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            String value = new String(transactions.get(id).read(key));
            logger.info("[{}] Read {}: {}", id, key, value);
        }
    }

    @Command
    public void commit(@Param(name = "txID") String id) throws MaxRetriesExceededException, InvalidTransactionStateChangeException, CommitFailedException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            transactions.get(id).commit();
            logger.info("[{}] Committed", id);
        }
    }

    @Command
    public void abort(@Param(name = "txID") String id) throws InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            transactions.get(id).abort();
            logger.info("[{}] Aborted", id);
        }
    }

}
