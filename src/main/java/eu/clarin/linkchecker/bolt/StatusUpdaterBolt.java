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

import eu.clarin.cmdi.rasa.DAO.CheckedLink;

import eu.clarin.cmdi.rasa.helpers.statusCodeMapper.Category;
import eu.clarin.linkchecker.config.Configuration;
import lombok.extern.slf4j.Slf4j;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.sql.*;
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
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

   private static final Pattern INT_PATTERN = Pattern.compile("\\d+");


   /**
    * Does not shard based on the total number of queues
    **/
   public StatusUpdaterBolt() {
   }

   @SuppressWarnings({ "rawtypes" })
   @Override
   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      super.prepare(stormConf, context, collector);
   }

   @Override
   public synchronized void store(String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
         throws Exception {

      log.debug("url: {}", url);
      log.debug("metadata: {}", metadata);
      log.debug("tuple: {}", t);

      StringBuilder mdAsString = new StringBuilder();
      for (String mdKey : metadata.keySet()) {
         String[] vals = metadata.getValues(mdKey);
         for (String v : vals) {
            mdAsString.append("\t").append(mdKey).append("=").append(v);
         }
      }

      CheckedLink checkedLink = new CheckedLink();

      Metadata md = (Metadata) t.getValueByField("metadata");
      
      log.debug("metadata:\n" + md.toString());

      String str = null;

      checkedLink.setUrlId(Long.valueOf(md.getFirstValue("urlId")));

      checkedLink.setUrl(md.getFirstValue("originalUrl")); // just for the error message since urlId is first choice for
                                                           // storage

      if ((str = md.getFirstValue("fetch.statusCode")) != null && INT_PATTERN.matcher(str).matches()) {
         checkedLink.setStatus(Integer.parseInt(md.getFirstValue("fetch.statusCode")));
      }
      if (md.getFirstValue("fetch.contentType") != null) {
         checkedLink.setContentType((md.getFirstValue("fetch.contentType").length() < 256) ? md.getFirstValue("fetch.contentType")
               : md.getFirstValue("fetch.contentType").substring(0, 250) + "...");
      }

      if ((str = md.getFirstValue("fetch.byteLength")) != null && INT_PATTERN.matcher(str).matches()) {
         checkedLink.setByteSize(Long.parseLong(md.getFirstValue("fetch.byteLength")));
      }

      if ((str = md.getFirstValue("fetch.duration")) != null && INT_PATTERN.matcher(str).matches()) {
         checkedLink.setDuration(Integer.parseInt(md.getFirstValue("fetch.duration")));
      }
      if (md.getFirstValue("fetch.message") != null) {
         checkedLink.setMessage((md.getFirstValue("fetch.message").length() < 1024) ? md.getFirstValue("fetch.message")
               : md.getFirstValue("fetch.message").substring(0, 1020) + "...");
      }

      checkedLink.setCategory(Category.valueOf(md.getFirstValue("fetch.category")));

      checkedLink.setCheckingDate(new Timestamp(md.getFirstValue("fetch.startTime")!=null?Long.parseLong(md.getFirstValue("fetch.startTime")):System.currentTimeMillis()));

      checkedLink.setRedirectCount(md.getFirstValue("fetch.redirectCount") == null ? 0
            : Integer.parseInt(md.getFirstValue("fetch.redirectCount")));

      String methodBool = md.getFirstValue("http.method.head");
      String method = methodBool == null ? "N/A" : methodBool.equalsIgnoreCase("true") ? "HEAD" : "GET";
      checkedLink.setMethod(method);

      try {
         Configuration.checkedLinkResource.save(checkedLink);
         _collector.ack(t);
      } catch (SQLException ex) {
         log.error("can't save checked link \n{}", checkedLink);
         _collector.fail(t);
      }
   }

   @Override
   public void cleanup() {

   }
}