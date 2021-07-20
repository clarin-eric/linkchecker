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

package at.ac.oeaw.acdh.linkchecker.bolt;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;

import at.ac.oeaw.acdh.linkchecker.config.Configuration;
import eu.clarin.cmdi.rasa.DAO.CheckedLink;

import eu.clarin.cmdi.rasa.helpers.statusCodeMapper.Category;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.Map;
import java.util.Optional;


/**
 * Status updater for SQL backend. Discovered URLs are sent as a batch, whereas
 * updates are atomic.
 **/

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    public final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    /**
     * Does not shard based on the total number of queues
     **/
    public StatusUpdaterBolt() {
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    @Override
    public synchronized void store(String url, Status status,
          Metadata metadata, Optional<Date> nextFetch, Tuple t) throws Exception {
    	
    	LOG.debug("url: {}", url);
    	LOG.debug("metadata: {}", metadata);
    	LOG.debug("tuple: {}", t);

        StringBuilder mdAsString = new StringBuilder();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }
        
        CheckedLink checkedLink = new CheckedLink();
 

        Metadata md = (Metadata) t.getValueByField("metadata");
        
        checkedLink.setUrlId(Long.valueOf(md.getFirstValue("urlId")));
        
        String fetchStatusCode = md.getFirstValue("fetch.statusCode");
        checkedLink.setStatus(fetchStatusCode == null ? null : fetchStatusCode.equalsIgnoreCase("null") ? null : Integer.parseInt(fetchStatusCode));
        checkedLink.setContentType(md.getFirstValue("content-type"));
        String fetchByteLength = md.getFirstValue("fetch.byteLength");
        checkedLink.setByteSize(fetchByteLength == null ? null : fetchByteLength.equalsIgnoreCase("null") ? null : Integer.parseInt(fetchByteLength));
        checkedLink.setDuration(md.getFirstValue("fetch.loadingTime") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.loadingTime")));
        checkedLink.setMessage(md.getFirstValue("fetch.message"));
        checkedLink.setCategory(Category.valueOf(md.getFirstValue("fetch.category")));

        Long timestampLong = md.getFirstValue("fetch.timestamp") == null ? System.currentTimeMillis() : Long.parseLong(md.getFirstValue("fetch.timestamp"));
        Timestamp timestamp = new Timestamp(timestampLong);
        checkedLink.setCheckingDate(timestamp);



        checkedLink.setRedirectCount(md.getFirstValue("fetch.redirectCount") == null ? 0 : Integer.parseInt(md.getFirstValue("fetch.redirectCount")));

        String methodBool = md.getFirstValue("http.method.head");
        String method = methodBool == null ? "N/A" : methodBool.equalsIgnoreCase("true") ? "HEAD" : "GET";
        checkedLink.setMethod(method);
        
        try {
        	Configuration.checkedLinkResource.save(checkedLink);
        	_collector.ack(t);
        }
        catch(SQLException ex) {
        	LOG.error("can't save checked link \n{}", checkedLink);
        	_collector.fail(t);
        }
    }


    @Override
    public void cleanup() {
    	
    }
}
