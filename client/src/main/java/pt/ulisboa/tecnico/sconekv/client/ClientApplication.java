package pt.ulisboa.tecnico.sconekv.client;

import asg.cliche.Command;
import asg.cliche.Param;
import asg.cliche.ShellFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.CommitFailedException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.RequestFailedException;
import pt.ulisboa.tecnico.sconekv.client.exceptions.UnableToGetViewException;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ClientApplication {
    private static final Logger logger = LoggerFactory.getLogger(ClientApplication.class);

    SconeClient client;
    Map<String, Transaction> transactions;
    ExecutorService executorService;

    public ClientApplication() throws UnableToGetViewException, IOException {
        client = new SconeClient();
        transactions = new HashMap();
    }

    public static void main(String[] args) throws IOException, UnableToGetViewException {
        logger.info("Launching client application...");

        String shell = System.getenv("USE_SHELL");

        if (shell != null) {
            ShellFactory.createConsoleShell("", "", new ClientApplication()).commandLoop();
        } else {
            try {
                SconeClient client = new SconeClient();

                for (int i = 0; i < 100; i++) {
                    Transaction tx1 = client.newTransaction();
                    tx1.write(i + "a", "b".getBytes());
                    tx1.write(i + "b", "c".getBytes());
                    try {
                        tx1.commit();
                        logger.info("Commit {}", tx1.getId());
                    } catch (CommitFailedException e) {
                        logger.info("Abort {}", tx1.getId());
                    }
                }
            } catch (InvalidTransactionStateChangeException | UnableToGetViewException | RequestFailedException e) {
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
    public void write(@Param(name = "txID") String id, @Param(name = "key") String key, @Param(name = "value") String value) throws RequestFailedException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            transactions.get(id).write(key, value.getBytes());
            logger.info("[{}] Write {}: {}", id, key, value);
        }
    }

    @Command
    public void delete(@Param(name = "txID") String id, @Param(name = "key") String key) throws RequestFailedException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            transactions.get(id).delete(key);
            logger.info("[{}] Delete {}", id, key);
        }
    }


    @Command
    public void read(@Param(name = "txID") String id, @Param(name = "key") String key) throws RequestFailedException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            String value = new String(transactions.get(id).read(key));
            logger.info("[{}] Read {}: {}", id, key, value);
        }
    }

    @Command
    public void commit(@Param(name = "txID") String id) throws RequestFailedException, InvalidTransactionStateChangeException {
        if (!transactions.containsKey(id)) {
            logger.error("Transaction identifier {} does not exist", id);
        } else {
            try {
                transactions.get(id).commit();
                logger.info("[{}] Committed", id);
            } catch (CommitFailedException e) {
                logger.info("[{}] Aborted", id);
            }
        }
    }

    @Command
    public void concurrentCommit(@Param(name = "txIDs") String... txIds) throws InterruptedException {
        if (executorService == null)
            executorService = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 0L,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        List<Callable<String>> commits = new ArrayList<>();
        for (String id : txIds) {
            if (!transactions.containsKey(id)) {
                logger.error("Transaction identifier {} does not exist", id);
            } else {
                commits.add(() -> {
                    try {
                        transactions.get(id).commit();
                        return String.format("[%s] Committed", id);
                    } catch (CommitFailedException e) {
                        return String.format("[%s] Aborted", id);
                    } catch (InvalidTransactionStateChangeException e) {
                        return String.format("[%s] ERROR: Invalid transaction state change, already is %s", id, transactions.get(id).getState());
                    } catch (RequestFailedException e) {
                        return String.format("[%s] ERROR: Request failed" , id);
                    }
                });
            }
        }
        List<Future<String>> futures = executorService.invokeAll(commits);
        for (Future<String> f : futures) {
            try {
                logger.info(f.get());
            } catch (ExecutionException ignored) {}
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
