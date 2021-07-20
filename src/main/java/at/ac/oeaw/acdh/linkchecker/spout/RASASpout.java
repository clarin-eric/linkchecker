/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.ac.oeaw.acdh.linkchecker.spout;
import java.sql.SQLException;
import java.util.Map;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.StringTabScheme;

import at.ac.oeaw.acdh.linkchecker.config.Configuration;
import at.ac.oeaw.acdh.linkchecker.config.Constants;
import eu.clarin.cmdi.rasa.filters.LinkToBeCheckedFilter;

@SuppressWarnings("serial")
public class RASASpout extends AbstractQueryingSpout {

    public static final Logger LOG = LoggerFactory.getLogger(RASASpout.class);

    private static final Scheme SCHEME = new StringTabScheme();


    /** Used to distinguish between instances in the logs **/
    protected String logIdprefix = "";


    private int maxNumResults;


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {

        super.open(conf, context, collector);

        maxNumResults = ConfUtils.getInt(conf, Constants.SQL_MAXRESULTS_PARAM_NAME, 100);

        Configuration.init(conf);
        Configuration.setActive(conf, true);
        
        int totalTasks = context
              .getComponentTasks(context.getThisComponentId()).size();
         if (totalTasks > 1) {
             logIdprefix = "[" + context.getThisComponentId() + " #"
                     + context.getThisTaskIndex() + "] ";
         }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(SCHEME.getOutputFields());
    }

    @Override
    protected void populateBuffer() {



        long timeStartQuery = System.currentTimeMillis();


        try {
           LinkToBeCheckedFilter filter = Configuration.linkToBeCheckedResource.getLinkToBeCheckedFilter().setIsActive(true).setDoOrder(true);
           
           Configuration
              .linkToBeCheckedResource
              .get(filter)
              .limit(maxNumResults)
              .filter(link -> !beingProcessed.containsKey(link.getUrl()))
              .forEach(link -> {
                 Metadata md = new Metadata();
                 md.setValue("urlId", String.valueOf(link.getUrlId()));
                 md.setValue("originalUrl", link.getUrl());
                 buffer.add(link.getUrl(), md);
              });

            long timeTaken = System.currentTimeMillis() - timeStartQuery;
            queryTimes.addMeasurement(timeTaken);

            LOG.info("{} SQL query returned {} hits in {} msec", logIdprefix, maxNumResults, timeTaken);

        } 
        catch (SQLException e) {
            LOG.error("Exception while querying table", e);
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
