package pt.ulisboa.tecnico.sconekv.ycsb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats {
    public static class Holder {
        static Stats instance = new Stats();

        private Holder() {}
    }

    private static final Logger logger = LoggerFactory.getLogger(Stats.class);

    private int totalClients = 0;
    private int finishedClients = 0;
    private int totalTransactions = 0;
    private int totalCommits = 0;
    private int totalAborts = 0;

    private Stats() {}

    public static Stats getInstance() {
        return Holder.instance;
    }

    public synchronized void newClient() {
        this.totalClients++;
    }

    public synchronized void clientFinished(int clientTotalTransactions, int clientCommits, int clientAborts) {
        this.totalTransactions += clientTotalTransactions;
        this.totalCommits += clientCommits;
        this.totalAborts += clientAborts;
        this.finishedClients++;
        if (this.totalClients == this.finishedClients) {

            logger.info("\nEnded SconeKV benchmark.\nNumber of clients: {}\nTotal transactions: {}" +
                    "\nCommitted: {} ({}%)\nAborted: {} ({}%)",
                    this.totalClients, this.totalTransactions,
                    this.totalCommits, this.totalCommits/(float)this.totalTransactions*100,
                    this.totalAborts, this.totalAborts/(float)this.totalTransactions*100);
        }
    }
}
