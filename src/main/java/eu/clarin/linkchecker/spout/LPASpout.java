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

import java.util.Map;
import java.util.stream.Stream;

import jakarta.persistence.Tuple;

import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.springframework.transaction.annotation.Transactional;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractQueryingSpout;
import com.digitalpebble.stormcrawler.util.StringTabScheme;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.repository.GenericRepository;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("serial")
@Slf4j
public class LPASpout extends AbstractQueryingSpout {

   private static final Scheme SCHEME = new StringTabScheme();
   
   private final String sql;

   /** Used to distinguish between instances in the logs **/
   protected String logIdprefix = "";
   
   private int counter = 0;
   private long lastCheckpoint = System.currentTimeMillis();
   
   
   public LPASpout(String sql) {
      super();
      this.sql = sql;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

      super.open(conf, context, collector);

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
   @Transactional(readOnly = true)
   protected void populateBuffer() {

      log.debug("{} call LinkToBeCheckedRessource.getNextLinksToCheck()", logIdprefix);
      
      this.isInQuery.set(true);
      long timeStartQuery = System.currentTimeMillis();
      
      GenericRepository gRep = Configuration.ctx.getBean(GenericRepository.class);

      
      try(Stream<Tuple> stream = gRep.findAll(sql, true).stream()){
         stream.filter(tuple -> !beingProcessed.containsKey(tuple.get("name"))).forEach(tuple -> {
   
            Metadata md = new Metadata();
            md.setValue("urlId", tuple.get("id").toString());
            md.setValue("originalUrl", tuple.get("name").toString());
            md.setValue("http.method.head", "true");
            buffer.add(tuple.get("name").toString(), md);
         });
      }
      
      this.markQueryReceivedNow();
      long timeTaken = System.currentTimeMillis() - timeStartQuery;
      queryTimes.addMeasurement(timeTaken);

      log.info("{} SQL query returned {} hits, distributed on {} queues in {} msec", logIdprefix, buffer.size(),
            buffer.numQueues(), timeTaken);


   }

   

   @Override
   public void ack(Object msgId) {
      if(++this.counter == 100) {

         log.info("Checked 100 links in {} ms", (System.currentTimeMillis() - this.lastCheckpoint));
         
         this.counter = 0;
         this.lastCheckpoint = System.currentTimeMillis();
      }
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
