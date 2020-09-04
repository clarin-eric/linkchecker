/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * NOTICE: This code was modified in ACDH - Austrian Academy of Sciences, based on Stormcrawler source code.
 */

package at.ac.oeaw.acdh.stormychecker.bolt;

import at.ac.oeaw.acdh.stormychecker.config.Configuration;
import at.ac.oeaw.acdh.stormychecker.config.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.sql.SQLUtil;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private String urlTableName;
    private String statusTableName;
    private String historyTableName;

    private URLPartitioner partitioner;
    private int maxNumBuckets = -1;

    private int batchMaxSize = 1000;
    private float batchMaxIdleMsec = 10000;

    private int currentBatchSize = 0;

    private PreparedStatement insertHistoryPreparedStmt = null;
    private PreparedStatement replacePreparedStmt = null;
    private PreparedStatement updatePreparedStmt = null;

    private long lastInsertBatchTime = -1;

    private String updateURLTableQuery;
    private String replaceStatusTableQuery;
    private String insertHistoryTableQuery;

    //    private final Map<String, List<Tuple>> waitingAck = new HashMap<>();
    private final List<Tuple> waitingAck = new ArrayList<>();

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

        urlTableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_TABLE_PARAM_NAME, "urls");
        statusTableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_RESULT_TABLE_PARAM_NAME, "status");
        historyTableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_HISTORY_TABLE_PARAM_NAME, "history");

        batchMaxSize = ConfUtils.getInt(stormConf, Constants.SQL_UPDATE_BATCH_SIZE_PARAM_NAME, 1000);

        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }


        String fields = "url, statusCode, contentType, byteSize, duration, timestamp, redirectCount, record, collection, expectedMimeType, message, method, category";
        //insert into status table
        replaceStatusTableQuery = "REPLACE INTO " + statusTableName + "(" + fields + ")" +
                " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        insertHistoryTableQuery = "INSERT INTO " + historyTableName + " SELECT * FROM " + statusTableName + " WHERE url = ?";

        //update urls table for nextfetchdate

        //set the latestFetchdate if not set
        if (Configuration.latestFetchDate == null) {
            String latestFetchDateQuery = "SELECT MAX(nextfetchdate) as nextfetchdate FROM stormychecker.urls";
            try {
                Statement st = this.connection.createStatement();
                ResultSet rs = st.executeQuery(latestFetchDateQuery);
                if (rs.next()) {
                    Configuration.latestFetchDate = rs.getTimestamp("nextfetchdate");
                }

            } catch (SQLException e) {
                LOG.error("Couldn't set latest fetch date. Default of now+4 weeks will be used.");
            }
        }

        //couldn't get it from the database
        if (Configuration.latestFetchDate == null) {
            updateURLTableQuery = "UPDATE " + urlTableName + " SET nextfetchdate = NOW() + INTERVAL 4 WEEK, host = ? WHERE url = ?";
        } else {
            updateURLTableQuery = "UPDATE " + urlTableName + " SET nextfetchdate = ? + INTERVAL ? SECOND, host = ? WHERE url = ?";
        }


        try {
            insertHistoryPreparedStmt = connection.prepareStatement(insertHistoryTableQuery);
            replacePreparedStmt = connection.prepareStatement(replaceStatusTableQuery);
            updatePreparedStmt = connection.prepareStatement(updateURLTableQuery);
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
    public synchronized void store(String url,
                                   Metadata metadata, Tuple t) throws Exception {
        // check whether the batch needs sending
        checkExecuteBatch();


        StringBuilder mdAsString = new StringBuilder();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }

        String partitionKey = partitioner.getPartition(url, metadata);

        Metadata md = (Metadata) t.getValueByField("metadata");
        String fetchStatusCode = md.getFirstValue("fetch.statusCode");
        Integer statusCode = fetchStatusCode == null ? null : fetchStatusCode.equalsIgnoreCase("null") ? null : Integer.parseInt(fetchStatusCode);
        String contentType = md.getFirstValue("content-type");
        String fetchByteLength = md.getFirstValue("fetch.byteLength");
        Integer byteLength = fetchByteLength == null ? null : fetchByteLength.equalsIgnoreCase("null") ? null : Integer.parseInt(fetchByteLength);
        int loadingTime = md.getFirstValue("fetch.loadingTime") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.loadingTime"));
        String message = md.getFirstValue("fetch.message");
        String category = md.getFirstValue("fetch.category");

        Long timestampLong = md.getFirstValue("fetch.timestamp") == null ? System.currentTimeMillis() : Long.parseLong(md.getFirstValue("fetch.timestamp"));
        Timestamp timestamp = new Timestamp(timestampLong);

        String collection = t.getStringByField("collection");
        String record = t.getStringByField("record");
        String expectedMimeType = t.getStringByField("expectedMimeType");

        int redirectCount = md.getFirstValue("fetch.redirectCount") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.redirectCount"));

        String methodBool = md.getFirstValue("http.method.head");
        String method = methodBool == null ? "N/A" : methodBool.equalsIgnoreCase("true") ? "HEAD" : "GET";

        replacePreparedStmt.setString(1, url);
        if (statusCode == null) {
            replacePreparedStmt.setNull(2, Types.INTEGER);
        } else {
            replacePreparedStmt.setInt(2, statusCode);
        }
        replacePreparedStmt.setString(3, contentType);
        if (byteLength == null) {//if HEAD method and no content-length, then it needs to be null
            replacePreparedStmt.setNull(4, Types.INTEGER);
        } else {
            replacePreparedStmt.setInt(4, byteLength);
        }
        replacePreparedStmt.setInt(5, loadingTime);
        replacePreparedStmt.setTimestamp(6, timestamp);
        replacePreparedStmt.setInt(7, redirectCount);
        replacePreparedStmt.setString(8, record);
        replacePreparedStmt.setString(9, collection);
        replacePreparedStmt.setString(10, expectedMimeType);
        replacePreparedStmt.setString(11, message);
        replacePreparedStmt.setString(12, method);
        replacePreparedStmt.setString(13, category);

        replacePreparedStmt.addBatch();

        insertHistoryPreparedStmt.setString(1, url);
        insertHistoryPreparedStmt.addBatch();

        if (Configuration.latestFetchDate == null) {
            updatePreparedStmt.setString(1, partitionKey);
            updatePreparedStmt.setString(2, url);
        } else {
//          this is the query:
//          updateURLTableQuery = "UPDATE " + urlTableName + " SET nextfetchdate = ? + INTERVAL ? SECOND, host = ? WHERE url = ?";
            updatePreparedStmt.setTimestamp(1, Configuration.latestFetchDate);
            updatePreparedStmt.setInt(2, Configuration.sec.incrementAndGet());
            updatePreparedStmt.setString(3, partitionKey);
            updatePreparedStmt.setString(4, url);
        }

        updatePreparedStmt.addBatch();

        if (lastInsertBatchTime == -1) {
            lastInsertBatchTime = System.currentTimeMillis();
        }

        // URL gets added to the cache in method ack
        // once this method has returned
//        waitingAck.put(url, new LinkedList<Tuple>());
        waitingAck.add(t);

        currentBatchSize += 2;

        eventCounter.scope("sql_inserts_number").incrBy(2);
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
            //insert history should be done first because replace deletes old ones
            insertHistoryPreparedStmt.executeBatch();
            replacePreparedStmt.executeBatch();
            updatePreparedStmt.executeBatch();
            long end = System.currentTimeMillis();

            LOG.info("Batched {} inserts and updates executed in {} msec", currentBatchSize, end - start);
            waitingAck.forEach(tuple -> {
                _collector.ack(tuple);
            });
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            // fail the entire batch
            waitingAck.forEach(tuple -> {
                _collector.ack(tuple);
            });
        }

        lastInsertBatchTime = System.currentTimeMillis();
        currentBatchSize = 0;
        waitingAck.clear();

        replacePreparedStmt.close();
        replacePreparedStmt = connection.prepareStatement(replaceStatusTableQuery);
        updatePreparedStmt.close();
        updatePreparedStmt = connection.prepareStatement(updateURLTableQuery);
        insertHistoryPreparedStmt.close();
        insertHistoryPreparedStmt = connection.prepareStatement(insertHistoryTableQuery);
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
