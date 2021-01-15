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
import java.util.concurrent.ThreadLocalRandom;

public class SconeKVClient extends DB {
    private static final Logger logger = LoggerFactory.getLogger(SconeKVClient.class);
    private SconeClient sconeClient;
    private Transaction currentTransaction;
    private int transactionSize;
    // TPC-C NewOrder [5..15]
    private int transactionSizeMin;
    private int transactionSizeMax;
    private int totalTransactions = 0;
    private int commits = 0;
    private int aborts = 0;
    private boolean randomTransactionSize;

    @Override
    public void init() throws DBException {
        try {
            Properties props = getProperties();
            String configFile = props.getProperty("scone.config");
            String txSize = props.getProperty("scone.tx_size");
            String txSizeMin = props.getProperty("scone.tx_min_size");
            String txSizeMax = props.getProperty("scone.tx_max_size");
            String nodes = props.getProperty("scone.nodes");

            if (nodes != null) {
                final String[] nodeArray = nodes.split(";");
                sconeClient = new SconeClient(Arrays.asList(nodeArray));
            } else {
                sconeClient = new SconeClient();
            }

            if (txSize != null) {
                transactionSize = Integer.parseInt(txSize);
                randomTransactionSize = false;
            }

            if (txSizeMin != null && txSizeMax != null) {
                transactionSizeMin = Integer.parseInt(txSizeMin);
                transactionSizeMax = Integer.parseInt(txSizeMax);
                randomTransactionSize = true;
            }

            Stats.getInstance().newClient();

            newTransaction();

        } catch (UnableToGetViewException | IOException e) {
            throw new DBException(e);
        }
    }

    private void newTransaction() {
        currentTransaction = sconeClient.newTransaction();
        if (randomTransactionSize) {
            // without +1 it's [min,max[ and not [min,max]
            transactionSize = ThreadLocalRandom.current().nextInt(transactionSizeMin, transactionSizeMax + 1);
        }
    }

    @Override
    public void cleanup() {
        try {
            commit();
        } catch (RequestFailedException | InvalidTransactionStateChangeException ignored) {}

//        logger.info("\nEnded SconeKV benchmark for client {}.\nTotal transactions: {}\nCommitted: {} ({}%)\nAborted: {} ({}%)",
//                currentTransaction.getId().getClient(), totalTransactions, commits, commits/(float)totalTransactions*100,
//                aborts, aborts/(float)totalTransactions*100);

        Stats.getInstance().clientFinished(totalTransactions, commits, aborts);
    }

    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            byte[] response = currentTransaction.read(key);

            if (response.length == 0) {
                return Status.NOT_FOUND;
            }

            Utils.createResultHashMap(fields, response, result);
            commit();
            return Status.OK;

        } catch (InvalidTransactionStateChangeException e) { // should not happen
            newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            totalTransactions++;
            newTransaction();
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
            newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            totalTransactions++;
            newTransaction();
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
            newTransaction();
            return Status.ERROR;
        } catch (RequestFailedException e) {
            totalTransactions++;
            newTransaction();
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    private void commit() throws RequestFailedException, InvalidTransactionStateChangeException {
        if (currentTransaction.size() >= transactionSize) {
            try {
                currentTransaction.commit();
                totalTransactions++;
                commits++;
                logger.debug("Committed {}", currentTransaction.getId());
            } catch (CommitFailedException e) {
                logger.debug("Aborted {}", currentTransaction.getId());
                totalTransactions++;
                aborts++;
            }
            newTransaction();
        }
    }

    public Status scan(String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.error("Scan command called, SconeKV has no scan implementation");
        return Status.NOT_IMPLEMENTED;
    }
}
