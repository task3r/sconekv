package pt.ulisboa.tecnico.sconekv.ycsb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.ulisboa.tecnico.sconekv.client.SconeClient;
import pt.ulisboa.tecnico.sconekv.client.db.Transaction;
import pt.ulisboa.tecnico.sconekv.client.exceptions.*;
import pt.ulisboa.tecnico.sconekv.common.exceptions.InvalidTransactionStateChangeException;
import site.ycsb.*;

import java.io.IOException;
import java.util.*;

public class SconeKVClient extends DB {
    private static final Logger logger = LoggerFactory.getLogger(SconeKVClient.class);
    private SconeClient sconeClient;
    private Transaction currentTransaction;
    private int transactionSize = 5;
    private int totalTransactions = 0;
    private int commits = 0;
    private int aborts = 0;

    @Override
    public void init() throws DBException {
        try {
            Properties props = getProperties();
            String configFile = props.getProperty("scone.config");
            String txSize = props.getProperty("scone.tx_size");

            if (configFile != null) {
                sconeClient = new SconeClient(configFile);
            } else {
                sconeClient = new SconeClient();
            }

            if (txSize != null) {
                transactionSize = Integer.parseInt(txSize);
            }

            currentTransaction = sconeClient.newTransaction();

        } catch (UnableToGetViewException | IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (currentTransaction.size() > 0) {
                totalTransactions++;
                currentTransaction.commit();
                commits++;
            }
        } catch (CommitFailedException e) {
            logger.error("Aborted tx {}", currentTransaction.getId());
            aborts++;
        } catch (RequestFailedException | InvalidTransactionStateChangeException ignored) {}

        logger.info("Ended SconeKV benchmark.");
        logger.info("Total transactions: {}", totalTransactions);
        logger.info("Committed: {} ({}%)", commits, commits/(float)totalTransactions*100);
        logger.info("Aborted: {} ({}%)", aborts, aborts/(float)totalTransactions*100);
    }

    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            byte[] response = currentTransaction.read(key);

            if (response.length == 0) {
                return Status.NOT_FOUND;
            }

            Utils.createResultHashMap(fields, response, result);
            // not considering reads for commit
            return Status.OK;

        } catch (InvalidTransactionStateChangeException e) { // should not happen
            currentTransaction = sconeClient.newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            currentTransaction = sconeClient.newTransaction();
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    public Status update(String table, String key, Map<String, ByteIterator> values) {
        try {
            byte[] value = Utils.serializeTable(values);
            currentTransaction.write(key, value);
            commit();
            return Status.OK;

        } catch (InvalidTransactionStateChangeException e) { // should not happen
            currentTransaction = sconeClient.newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            currentTransaction = sconeClient.newTransaction();
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return update(table, key, values); // SconeKV does not distinguish insertions from updates
    }

    public Status delete(String table, String key) {
        try {
            currentTransaction.delete(key);
            commit();
            return Status.OK;

        } catch (InvalidTransactionStateChangeException e) { // should not happen
            currentTransaction = sconeClient.newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            currentTransaction = sconeClient.newTransaction();
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    private void commit() throws RequestFailedException, InvalidTransactionStateChangeException {
        if (currentTransaction.size() >= transactionSize) {
            try {
                totalTransactions++;
                currentTransaction.commit();
                commits++;
            } catch (CommitFailedException e) {
                logger.error("Aborted tx {}", currentTransaction.getId());
                aborts++;
            }
            currentTransaction = sconeClient.newTransaction();
        }
    }

    public Status scan(String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.error("Scan command called, SconeKV has no scan implementation");
        return Status.NOT_IMPLEMENTED;
    }
}
