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

import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * Status updater for SQL backend. Discovered URLs are sent as a batch, whereas
 * updates are atomic.
 **/

@SuppressWarnings("serial")
public class MetadataPrinterBolt extends AbstractStatusUpdaterBolt {



   /**
    * Does not shard based on the total number of queues
    **/
   public MetadataPrinterBolt() {
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      super.prepare(stormConf, context, collector);
   }

   @Override
   public synchronized void store(String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
         throws Exception {

      System.out.println(metadata.toString());


      _collector.ack(t);

   }

   @Override
   public void cleanup() {

   }
}
