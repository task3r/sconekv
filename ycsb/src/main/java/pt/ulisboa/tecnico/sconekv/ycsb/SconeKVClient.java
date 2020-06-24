package pt.ulisboa.tecnico.sconekv.ycsb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class SconeClient extends DB {
    private static final Logger logger = LoggerFactory.getLogger(SconeClient.class);

    @Override
    public void init() throws DBException {
        super.init();
    }

    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return null;
    }

    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.error("Scan command called, SconeKV has no scan implementation");
        return null;
    }

    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return null;
    }

    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return null;
    }

    public Status delete(String table, String key) {
        return null;
    }
}
