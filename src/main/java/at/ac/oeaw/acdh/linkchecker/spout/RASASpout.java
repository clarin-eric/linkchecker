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

package at.ac.oeaw.acdh.linkchecker.spout;


import com.digitalpebble.stormcrawler.util.ConfUtils;

import at.ac.oeaw.acdh.linkchecker.config.Configuration;
import at.ac.oeaw.acdh.linkchecker.config.Constants;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("serial")
public class RASASpout extends AbstractQueryingSpout {
    public static final Logger LOG = LoggerFactory.getLogger(at.ac.oeaw.acdh.linkchecker.spout.RASASpout.class);

    /**
     * Used to distinguish between instances in the logs
     **/
    protected String logIdprefix = "";

    private int maxNumResults;

//    private Instant lastNextFetchDate = null;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        super.open(conf, context, collector);
        
        Configuration.setActive(conf, true);


        maxNumResults = ConfUtils.getInt(conf,
                Constants.SQL_MAXRESULTS_PARAM_NAME, 100);


        // determine bucket this spout instance will be in charge of
        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            logIdprefix = "[" + context.getThisComponentId() + " #"
                    + context.getThisTaskIndex() + "] ";
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("linkId, originalUrl", "url", "collection", "record", "expectedMimeType"));
    }

    @Override
    protected void populateBuffer() {

        if (maxNumResults != -1) {
        	maxNumResults = 100;
        	LOG.info("setting maxNumResults=100");
        }

        long timeStartQuery = System.currentTimeMillis();

        try {
        	Configuration
        		.linkToBeCheckedResource
        		.get(Optional.empty())
        		.limit(maxNumResults)
        		.filter(link -> !beingProcessed.containsKey(link.getUrl()))
        		.forEach(link -> buffer.add(
        				new Values(link.getLinkId(), link.getUrl(), link.getUrl()))
    				);

            long timeTaken = System.currentTimeMillis() - timeStartQuery;
            queryTimes.addMeasurement(timeTaken);

            LOG.info("{} SQL query returned {} URLs to check", logIdprefix, buffer.size());

        } 
        catch (SQLException e) {
            LOG.error("Exception while querying LinkToBeCheckedRessource.get()", e);
        } 
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("{}  Ack for {}", logIdprefix, msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("{}  Fail for {}", logIdprefix, msgId);
        super.fail(msgId);
    }

    @Override
    public void close() {
        super.close();
        
        Configuration.setActive(null, false);
    }
}
