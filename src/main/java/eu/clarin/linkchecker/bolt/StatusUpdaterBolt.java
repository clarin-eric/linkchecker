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

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.model.Url;
import eu.clarin.linkchecker.persistence.repository.UrlRepository;
import eu.clarin.linkchecker.persistence.service.StatusService;
import eu.clarin.linkchecker.persistence.utils.Category;
import lombok.extern.slf4j.Slf4j;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
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

   @SuppressWarnings({ "rawtypes", "unchecked" })
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
      
      UrlRepository uRep = Configuration.ctx.getBean(UrlRepository.class);
      StatusService sService = Configuration.ctx.getBean(StatusService.class);
      
      Url urlEntity = uRep.findById(Long.valueOf(md.getFirstValue("urlId"))).get();
      
      eu.clarin.linkchecker.persistence.model.Status statusEntity = new eu.clarin.linkchecker.persistence.model.Status(
            urlEntity, 
            Category.valueOf(md.getFirstValue("fetch.category")), 
            md.getFirstValue("fetch.message").length() < 1024?md.getFirstValue("fetch.message"): md.getFirstValue("fetch.message").subSequence(0, 1017) + "[...]",
            md.getFirstValue("fetch.chechingDate") != null? 
                  LocalDateTime.parse(md.getFirstValue("fetch.chechingDate"))
                  : LocalDateTime.now()
         );


      if ((str = md.getFirstValue("fetch.statusCode")) != null && INT_PATTERN.matcher(str).matches()) {
         statusEntity.setStatusCode(Integer.parseInt(md.getFirstValue("fetch.statusCode")));
      }
      if (md.getFirstValue("fetch.contentType") != null) {
         statusEntity.setContentType(
               (md.getFirstValue("fetch.contentType").length() < 256) ? md.getFirstValue("fetch.contentType")
                     : md.getFirstValue("fetch.contentType").substring(0, 250) + "...");
      }

      if ((str = md.getFirstValue("fetch.byteLength")) != null && INT_PATTERN.matcher(str).matches()) {
         statusEntity.setContentLength(Long.parseLong(md.getFirstValue("fetch.byteLength")));
      }

      if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
         statusEntity.setDuration(Integer.parseInt(md.getFirstValue("fetch.duration")));
      }

      statusEntity.setRedirectCount(md.getFirstValue("fetch.redirectCount") == null ? 0
            : Integer.parseInt(md.getFirstValue("fetch.redirectCount")));

      String methodBool = md.getFirstValue("http.method.head");
      String method = methodBool == null ? "N/A" : methodBool.equalsIgnoreCase("true") ? "HEAD" : "GET";
      statusEntity.setMethod(method);

      try {
         sService.save(statusEntity);
         collector.emit(t, new Values(md));

         collector.ack(t);
      }
      catch (Exception ex) {
         log.error("can't save checked link \n{}", statusEntity);
         log.error("metadata:\n" + md.toString());
         collector.fail(t);
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
}
