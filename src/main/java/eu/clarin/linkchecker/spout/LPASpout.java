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

import java.time.LocalDateTime;
import java.util.Map;

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
import eu.clarin.linkchecker.persistence.service.LinkService;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("serial")
@Slf4j
public class LPASpout extends AbstractQueryingSpout {

   private static final Scheme SCHEME = new StringTabScheme();

   /** Used to distinguish between instances in the logs **/
   protected String logIdprefix = "";

   private int maxNumResults;
   private int maxNumResultsPerGroup;

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

      super.open(conf, context, collector);

      maxNumResults = ConfUtils.getInt(conf, Constants.SPOUT_MAXRESULTS_PARAM_NAME, 1000);
      maxNumResultsPerGroup = ConfUtils.getInt(conf, Constants.SPOUT_GROUP_MAXRESULTS_PARAM_NAME, 100);

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
      
      LinkService lService = Configuration.ctx.getBean(LinkService.class);
      
      lService.getUrlsToCheck(maxNumResults, maxNumResultsPerGroup, LocalDateTime.now().minusDays(1))
         .stream()
         .filter(url -> !beingProcessed.containsKey(url.getName())).forEach(url -> {
            Metadata md = new Metadata();
            md.setValue("urlId", String.valueOf(url.getId()));
            md.setValue("originalUrl", url.getName());
            md.setValue("http.method.head", "true");
            buffer.add(url.getName(), md);
         });
      
         this.markQueryReceivedNow();
         long timeTaken = System.currentTimeMillis() - timeStartQuery;
         queryTimes.addMeasurement(timeTaken);

         log.info("{} SQL query returned {} hits, distributed on {} queues in {} msec", logIdprefix, buffer.size(),
               buffer.numQueues(), timeTaken);


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
