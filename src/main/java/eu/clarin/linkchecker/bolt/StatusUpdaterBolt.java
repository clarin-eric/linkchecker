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

import eu.clarin.linkchecker.persistence.model.History;
import eu.clarin.linkchecker.persistence.model.Status;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.model.Url;
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

      EntityManager entityManager = Configuration.emFactory.createEntityManager();

      EntityTransaction tx = entityManager.getTransaction();

      try{
         tx.begin();
         Url urlEntity = entityManager.find(Url.class, Long.valueOf(md.getFirstValue("urlId")));

         // resetting priority to 0
         if(urlEntity.getPriority() > 0) {
            urlEntity.setPriority(0);
            entityManager.merge(urlEntity);
         }

         Status statusEntity = urlEntity.getStatus();

         if(statusEntity != null){

            History historyEntity = new History(urlEntity, statusEntity.getCategory(), statusEntity.getCheckingDate());
            historyEntity.setStatusCode(statusEntity.getStatusCode());
            historyEntity.setContentLength(statusEntity.getContentLength());
            historyEntity.setDuration(statusEntity.getDuration());
            historyEntity.setMethod(statusEntity.getMethod());
            historyEntity.setMessage(statusEntity.getMessage());
            historyEntity.setRedirectCount(statusEntity.getRedirectCount());

            entityManager.persist(historyEntity);
         }
         else {

            statusEntity = new Status(
                    urlEntity,
                    Category.valueOf(md.getFirstValue("fetch.category")),
                    md.getFirstValue("fetch.message").length() < 1024?md.getFirstValue("fetch.message"): md.getFirstValue("fetch.message").subSequence(0, 1017) + "[...]",
                    md.getFirstValue("fetch.chechingDate") != null?
                            LocalDateTime.parse(md.getFirstValue("fetch.chechingDate"))
                            : LocalDateTime.now()
                 );
         }
         if ((str = md.getFirstValue("fetch.statusCode")) != null && INT_PATTERN.matcher(str).matches()) {
            statusEntity.setStatusCode(Integer.parseInt(md.getFirstValue("fetch.statusCode")));
         }
         else {
            statusEntity.setStatusCode(null);
         }
         if (md.getFirstValue("fetch.contentType") != null) {
            statusEntity.setContentType(
                  (md.getFirstValue("fetch.contentType").length() < 256) ? md.getFirstValue("fetch.contentType")
                        : md.getFirstValue("fetch.contentType").substring(0, 250) + "...");
         }
         else{
            statusEntity.setContentType(null);
         }
         if ((str = md.getFirstValue("fetch.byteLength")) != null && INT_PATTERN.matcher(str).matches()) {
            statusEntity.setContentLength(Long.parseLong(md.getFirstValue("fetch.byteLength")));
         }
         else{
            statusEntity.setContentLength(null);
         }
         if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
            statusEntity.setDuration(Integer.parseInt(md.getFirstValue("fetch.duration")));
         }
         else{
            statusEntity.setDuration(null);
         }
         if((str = md.getFirstValue("fetch.redirectCount"))!= null && INT_PATTERN.matcher(str).matches()){
            statusEntity.setRedirectCount(Integer.parseInt(md.getFirstValue("fetch.redirectCount")));
         }
         else {
            statusEntity.setRedirectCount(null);
         }

         String methodBool = md.getFirstValue("http.method.head");
         String method = methodBool == null ? "N/A" : methodBool.equalsIgnoreCase("true") ? "HEAD" : "GET";
         statusEntity.setMethod(method);

         if(statusEntity.getId() != null){
            entityManager.merge(statusEntity);
         }
         else{
            entityManager.persist(statusEntity);
         }

         tx.commit();

         collector.emit(t, new Values(md));

         collector.ack(t);
      }
      catch (Exception ex) {
         log.error("can't save checked link \n{}", md.toString(), ex);
         tx.rollback();
         collector.fail(t);
      }
      entityManager.close();
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
