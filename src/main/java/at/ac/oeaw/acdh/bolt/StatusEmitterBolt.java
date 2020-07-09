package at.ac.oeaw.acdh.bolt;

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
 * NOTICE: This code was modified in ACDH - Austrian Academy of Sciences, based on Stormcrawler source code.
 */

import at.ac.oeaw.acdh.config.Constants;
import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Provides common functionalities for Bolts which emit tuples to the status
 * stream, e.g. Fetchers, Parsers. Encapsulates the logic of URL filtering and
 * metadata transfer to outlinks.
 **/
public abstract class StatusEmitterBolt extends BaseRichBolt {

    private URLFilters urlFilters;

    private MetadataTransfer metadataTransfer;

    private boolean allowRedirs;

    protected OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        urlFilters = URLFilters.fromConf(stormConf);
        metadataTransfer = MetadataTransfer.getInstance(stormConf);
        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.stormcrawler.Constants.AllowRedirParamName,
                true);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                Constants.StatusStreamName,
                new Fields("originalUrl", "url", "metadata", "collection", "record", "expectedMimeType"));
        declarer.declareStream(
                Constants.RedirectStreamName,
                new Fields("originalUrl", "url", "metadata", "collection", "record", "expectedMimeType"));
    }

}

