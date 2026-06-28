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

package eu.clarin.linkchecker.bolt;

import jdk.jfr.Category;
import org.apache.stormcrawler.Metadata;


import eu.clarin.linkchecker.config.Configuration;

import lombok.extern.slf4j.Slf4j;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.*;
import java.time.LocalDateTime;

import java.util.Map;

import java.util.regex.Pattern;

/**
 * Status updater for SQL backend. Discovered URLs are sent as a batch, whereas
 * updates are atomic.
 **/

@SuppressWarnings("serial")
@Slf4j
public class StatusUpdaterBolt implements IRichBolt {

    private static final Pattern INT_PATTERN = Pattern.compile("\\d+");

    private OutputCollector collector;

    /**
     * Does not shard based on the total number of queues
     **/
    public StatusUpdaterBolt() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public synchronized void execute(Tuple t) {

        log.debug("tuple: {}", t);

        Metadata md = (Metadata) t.getValueByField("metadata");

        log.debug("metadata:\n{}", md.toString());

        String str = null;

        long urlId = Long.valueOf(md.getFirstValue("urlId"));

        Connection con = null;

        try {

            con = Configuration.dataSource.getConnection();

            con.setAutoCommit(false);

            Long statusId = null;
            Integer statusCode = null;
            String message = null;
            String category = null;
            String method = null;
            String contentType = null;
            Long contentLength = null;
            Integer duration = null;
            Timestamp checkingDate = null;
            Integer redirectCount = null;


            try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM status s WHERE s.url_id = ?")) {

                stmt.setLong(1, urlId);
                try (ResultSet rs = stmt.executeQuery()) {

                    if (rs.next()) {
                        statusId = rs.getLong("id");
                        statusCode = rs.getInt("status_code");
                        message = rs.getString("message");
                        category = rs.getString("category");
                        method = rs.getString("method");
                        contentType = rs.getString("content_type");
                        contentLength = rs.getLong("content_length");
                        duration = rs.getInt("duration");
                        checkingDate = rs.getTimestamp("checking_date");
                        redirectCount = rs.getInt("redirect_count");
                    }
                }
            }
            // copy old status to history table
            if (statusId != null) {

                try (PreparedStatement stmt = con.prepareStatement(
                        """
                                INSERT INTO history(url_id, status_code, message, category, method, content_type, content_length, duration, checking_date, redirect_count)
                                VALUES (?,?,?,?,?,?,?,?,?,?)
                                """
                )) {

                    stmt.setLong(1, urlId);
                    stmt.setInt(2, statusCode);
                    stmt.setString(3, message);
                    stmt.setString(4, category);
                    stmt.setString(5, method);
                    stmt.setString(6, contentType);
                    stmt.setLong(7, contentLength);
                    stmt.setInt(8, duration);
                    stmt.setTimestamp(9, checkingDate);
                    stmt.setInt(10, redirectCount);

                    stmt.execute();
                }

                try (PreparedStatement stmt = con.prepareStatement(
                     """
                        UPDATE status(status_code, message, category, method, content_type, content_length, duration, checking_date, redirect_count)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        WHERE status_code = ?
                        """
                )) {

                    if ((str = md.getFirstValue("fetch.statusCode")) != null && INT_PATTERN.matcher(str).matches()) {
                        stmt.setInt(1, Integer.parseInt(md.getFirstValue("fetch.statusCode")));
                    } else {
                        stmt.setNull(1, Types.INTEGER);
                    }
                    if ((str = md.getFirstValue("fetch.message")) != null) {

                        stmt.setString(2, str.length() < 1024 ? str : str.subSequence(0, 1017) + "[...]");
                    } else {
                        stmt.setNull(2, Types.VARCHAR);
                    }
                    if ((str = md.getFirstValue("fetch.category")) != null) {

                        stmt.setString(3, str);
                    } else {

                        stmt.setString(3, "Undetermined");
                    }
                    if ((str = md.getFirstValue("http.method.head")) != null) {
                        stmt.setString(4, str.equalsIgnoreCase("true") ? "HEAD" : "GET");
                    } else {
                        stmt.setNull(4, Types.VARCHAR);
                    }
                    if ((str = md.getFirstValue("fetch.contentType")) != null) {
                        stmt.setString(5, str.length() < 256 ? md.getFirstValue("fetch.contentType")
                                : md.getFirstValue("fetch.contentType").substring(0, 250) + "...");
                    } else {
                        stmt.setNull(5, Types.VARCHAR);
                    }
                    if (((str = md.getFirstValue("fetch.byteLength")) != null && INT_PATTERN.matcher(str).matches())) {
                        stmt.setLong(6, Long.parseLong(str));
                    } else {
                        stmt.setNull(6, Types.BIGINT);
                    }
                    if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
                        stmt.setInt(7, Integer.parseInt(str));
                    } else {
                        stmt.setNull(7, Types.INTEGER);
                    }
                    if ((str = md.getFirstValue("fetch.checkingDate")) != null) {
                        stmt.setTimestamp(8, Timestamp.valueOf(LocalDateTime.parse(str)));
                    } else {
                        stmt.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
                    }
                    if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
                        stmt.setInt(9, Integer.parseInt(str));
                    } else {
                        stmt.setNull(9, Types.INTEGER);
                    }
                    stmt.setLong(10, statusId);

                    stmt.executeUpdate();
                }
            }
            try (PreparedStatement stmt = con.prepareStatement(
                 """
                    INSERT INTO status(status_code, message, category, method, content_type, content_length, duration, checking_date, redirect_count, url_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """
            )) {
                if ((str = md.getFirstValue("fetch.statusCode")) != null && INT_PATTERN.matcher(str).matches()) {
                    stmt.setInt(1, Integer.parseInt(md.getFirstValue("fetch.statusCode")));
                } else {
                    stmt.setNull(1, Types.INTEGER);
                }
                if ((str = md.getFirstValue("fetch.message")) != null) {

                    stmt.setString(2, str.length() < 1024 ? str : str.subSequence(0, 1017) + "[...]");
                } else {
                    stmt.setNull(2, Types.VARCHAR);
                }
                if ((str = md.getFirstValue("fetch.category")) != null) {

                    stmt.setString(3, str);
                } else {

                    stmt.setString(3, "Undetermined");
                }
                if ((str = md.getFirstValue("http.method.head")) != null) {
                    stmt.setString(4, str.equalsIgnoreCase("true") ? "HEAD" : "GET");
                } else {
                    stmt.setNull(4, Types.VARCHAR);
                }
                if ((str = md.getFirstValue("fetch.contentType")) != null) {
                    stmt.setString(5, str.length() < 256 ? md.getFirstValue("fetch.contentType")
                            : md.getFirstValue("fetch.contentType").substring(0, 250) + "...");
                } else {
                    stmt.setNull(5, Types.VARCHAR);
                }
                if (((str = md.getFirstValue("fetch.byteLength")) != null && INT_PATTERN.matcher(str).matches())) {
                    stmt.setLong(6, Long.parseLong(str));
                } else {
                    stmt.setNull(6, Types.BIGINT);
                }
                if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
                    stmt.setInt(7, Integer.parseInt(str));
                } else {
                    stmt.setNull(7, Types.INTEGER);
                }
                if ((str = md.getFirstValue("fetch.checkingDate")) != null) {
                    stmt.setTimestamp(8, Timestamp.valueOf(LocalDateTime.parse(str)));
                } else {
                    stmt.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
                }
                if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
                    stmt.setInt(9, Integer.parseInt(str));
                } else {
                    stmt.setNull(9, Types.INTEGER);
                }
                stmt.setLong(10, urlId);

                stmt.execute();

            }

            con.commit();

            collector.emit(t, new Values(md));

            collector.ack(t);
        }

        catch (SQLException e) {
            log.error("can't save checked link \n{}\n{}", md.toString(), e);
            collector.fail(t);
            try {
                con.rollback();
            }
            catch (SQLException ex) {
                //
            }
        } finally {
            try {
                con.setAutoCommit(true);
            }
            catch (SQLException e) {
                //
            }

            try {
                con.close();
            }
            catch (SQLException e) {
                //
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("metadata"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void fillStatement(PreparedStatement stmt, Metadata md) {

    }
}
