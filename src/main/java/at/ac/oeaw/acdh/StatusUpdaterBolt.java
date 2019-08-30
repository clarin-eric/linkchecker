package at.ac.oeaw.acdh;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.digitalpebble.stormcrawler.sql.SQLUtil;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

/**
 * Status updater for SQL backend. Discovered URLs are sent as a batch, whereas
 * updates are atomic.
 **/

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    public static final Logger LOG = LoggerFactory
            .getLogger(com.digitalpebble.stormcrawler.sql.StatusUpdaterBolt.class);

    private MultiCountMetric eventCounter;

    private Connection connection;
    private String tableName;
    private String resultTableName;

    private URLPartitioner partitioner;
    private int maxNumBuckets = -1;

    private int batchMaxSize = 1000;
    private float batchMaxIdleMsec = 2000;

    private int currentBatchSize = 0;

    private PreparedStatement insertPreparedStmt = null;

    private long lastInsertBatchTime = -1;

    private String updateQuery;
    private String insertQuery;

    private final Map<String, List<Tuple>> waitingAck = new HashMap<>();

    public StatusUpdaterBolt(int maxNumBuckets) {
        this.maxNumBuckets = maxNumBuckets;
    }

    /**
     * Does not shard based on the total number of queues
     **/
    public StatusUpdaterBolt() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        this.eventCounter = context.registerMetric("counter", new MultiCountMetric(), 10);

        tableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_TABLE_PARAM_NAME, "urls");
        resultTableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_RESULT_TABLE_PARAM_NAME, "status");

        batchMaxSize = ConfUtils.getInt(stormConf, Constants.SQL_UPDATE_BATCH_SIZE_PARAM_NAME, 1000);

        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        //Can code: extended query
        String query = resultTableName + " (url, status, nextfetchdate, metadata, host, statusCode, contentType, byteSize, duration, timestamp, redirectCount)"
                + " values (?, ?, ?, ?, ?, ?, ? ,? ,? ,? ,?)";

        updateQuery = "REPLACE INTO " + query;
        insertQuery = "INSERT IGNORE INTO " + query;

        try {
            insertPreparedStmt = connection.prepareStatement(insertQuery);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            try {
                checkExecuteBatch();
            } catch (SQLException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new RuntimeException(ex);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void store(String url, Status status,
                                   Metadata metadata, Date nextFetch, Tuple t) throws Exception {
        // check whether the batch needs sending
        checkExecuteBatch();

        boolean isUpdate = !status.equals(Status.DISCOVERED);

        // already have an entry for this DISCOVERED URL
        if (!isUpdate && waitingAck.containsKey(url)) {
            List<Tuple> list = waitingAck.get(url);
            // add the tuple to the list for that url
            list.add(t);
            return;
        }

        StringBuilder mdAsString = new StringBuilder();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }

        int partition = 0;
        String partitionKey = partitioner.getPartition(url, metadata);
        if (maxNumBuckets > 1) {
            // determine which shard to send to based on the host / domain /
            // IP
            partition = Math.abs(partitionKey.hashCode() % maxNumBuckets);
        }

        PreparedStatement preparedStmt = this.insertPreparedStmt;

        // create in table if does not already exist
        if (isUpdate) {
            preparedStmt = connection.prepareStatement(updateQuery);
        }


        //Can code, read this info from tuple, not to change fetcherbolt for now
        //because in the given parameter metadata, all this info is not there
        Metadata md = (Metadata) t.getValueByField("metadata");
        int statusCode = md.getFirstValue("fetch.statusCode") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.statusCode"));
        String contentType = md.getFirstValue("content-type");
        int byteLength = md.getFirstValue("byte-length") == null ? 0 : Integer.parseInt(md.getFirstValue("byte-length"));
        int loadingTime = md.getFirstValue("fetch.loadingTime") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.loadingTime"));
        String lastProcessedDate = md.getFirstValue("lastProcessedDate");
        Date timestamp = lastProcessedDate == null ? null : Date.from(Instant.parse(lastProcessedDate));

        preparedStmt.setString(1, url);
        preparedStmt.setString(2, status.toString());
        preparedStmt.setObject(3, nextFetch);
        preparedStmt.setString(4, mdAsString.toString());
        preparedStmt.setString(5, partitionKey);
        preparedStmt.setInt(6, statusCode);
        preparedStmt.setString(7, contentType);
        preparedStmt.setInt(8, byteLength);
        preparedStmt.setInt(9, loadingTime);
        preparedStmt.setObject(10, timestamp);
        preparedStmt.setInt(11, 0);//todo

        // updates are not batched
        if (isUpdate) {
            preparedStmt.executeUpdate();
            preparedStmt.close();
            eventCounter.scope("sql_updates_number").incrBy(1);
            super.ack(t, url);
            return;
        }

        // code below is for inserts i.e. DISCOVERED URLs
        preparedStmt.addBatch();

        if (lastInsertBatchTime == -1) {
            lastInsertBatchTime = System.currentTimeMillis();
        }

        // URL gets added to the cache in method ack
        // once this method has returned
        waitingAck.put(url, new LinkedList<Tuple>());

        currentBatchSize++;

        eventCounter.scope("sql_inserts_number").incrBy(1);
    }

    private synchronized void checkExecuteBatch() throws SQLException {
        if (currentBatchSize == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        // check whether the insert batches need executing
        if ((currentBatchSize == batchMaxSize)) {
            LOG.info("About to execute batch - triggered by size");
        } else if (lastInsertBatchTime + (long) batchMaxIdleMsec < System.currentTimeMillis()) {
            LOG.info("About to execute batch - triggered by time. Due {}, now {}",
                    lastInsertBatchTime + (long) batchMaxIdleMsec, now);
        } else {
            return;
        }

        try {
            long start = System.currentTimeMillis();
            insertPreparedStmt.executeBatch();
            long end = System.currentTimeMillis();

            LOG.info("Batched {} inserts executed in {} msec", currentBatchSize, end - start);
            waitingAck.forEach((k, v) -> {
                for (Tuple t : v) {
                    super.ack(t, k);
                }
            });
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            // fail the entire batch
            waitingAck.forEach((k, v) -> {
                for (Tuple t : v) {
                    super._collector.fail(t);
                }
            });
        }

        lastInsertBatchTime = System.currentTimeMillis();
        currentBatchSize = 0;
        waitingAck.clear();

        insertPreparedStmt.close();
        insertPreparedStmt = connection.prepareStatement(insertQuery);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            try {
                connection.close();
            } catch (SQLException e) {
            }
    }

}
