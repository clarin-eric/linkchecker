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

package eu.clarin.linkchecker.spout;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.StringTabScheme;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.config.Constants;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("serial")
@Slf4j
public class RASAQuerySpout extends AbstractQueryingSpout {

   private static final Scheme SCHEME = new StringTabScheme();

   /** Used to distinguish between instances in the logs **/
   protected String logIdprefix = "";

   private int maxNumResults;
   
   private final String query;
   
   public RASAQuerySpout(String query) {
      this.query = query;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

      super.open(conf, context, collector);

      maxNumResults = ConfUtils.getInt(conf, Constants.RASA_MAXRESULTS_PARAM_NAME, 1000);

      Configuration.init(conf);
      Configuration.setActive(conf, true);

      int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
      if (totalTasks > 1) {
         logIdprefix = "[" + context.getThisComponentId() + " #" + context.getThisTaskIndex() + "] ";
      }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(SCHEME.getOutputFields());
   }

   @Override
   protected void populateBuffer() {

      log.debug("{} call LinkToBeCheckedRessource.getNextLinksToCheck()", logIdprefix);
      
      this.isInQuery.set(true);
      long timeStartQuery = System.currentTimeMillis();
      try (Stream<Map<String, Object>> stream = Configuration.linkToBeCheckedResource.get(this.query)) {         

         stream.limit(this.maxNumResults).forEach(map -> {
            Metadata md = new Metadata();
            md.setValue("urlId", map.get("id").toString());
            md.setValue("originalUrl", map.get("url").toString());
            md.setValue("http.method.head", "true");
            buffer.add(map.get("url").toString(), md);
         });
         
         this.markQueryReceivedNow();
         long timeTaken = System.currentTimeMillis() - timeStartQuery;
         queryTimes.addMeasurement(timeTaken);

         log.info("{} SQL query returned {} hits, distributed on {} queues in {} msec", logIdprefix, buffer.size(),
               buffer.numQueues(), timeTaken);

      } 
      catch (SQLException e) {
         log.error("Exception while executing query '{}'", this.query);
      }
   }

   @Override
   public void ack(Object msgId) {
      log.debug("{}  Ack for {}", logIdprefix, msgId);
      super.ack(msgId);
   }

   @Override
   public void fail(Object msgId) {
      log.info("{}  Fail for {}", logIdprefix, msgId);
      super.fail(msgId);
   }

   @Override
   public void close() {
      super.close();
   }
}