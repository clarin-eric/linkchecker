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

package at.ac.oeaw.acdh.stormychecker.bolt;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Scheduler;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract bolt used to store the status of URLs. Uses the DefaultScheduler and
 * MetadataTransfer.
 **/
@SuppressWarnings("serial")
public abstract class AbstractStatusUpdaterBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt.class);

    /**
     * Parameter name to indicate whether the internal cache should be used for
     * discovered URLs. The value of the parameter is a boolean - true by
     * default.
     **/
    public static String useCacheParamName = "status.updater.use.cache";

    /**
     * Number of successive FETCH_ERROR before status changes to ERROR
     **/
    public static String maxFetchErrorsParamName = "max.fetch.errors";

    /**
     * Parameter name to configure the cache @see
     * http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default
     * value is "maximumSize=10000,expireAfterAccess=1h"
     **/
    public static String cacheConfigParamName = "status.updater.cache.spec";

    /**
     * Used for rounding nextFetchDates. Values are hour, minute or second, the
     * latter is the default value.
     **/
    public static String roundDateParamName = "status.updater.unit.round.date";

    /**
     * Key used to pass a preset Date to use as nextFetchDate. The value must
     * represent a valid instant in UTC and be parsable using
     * {@link DateTimeFormatter#ISO_INSTANT}. This also indicates that the
     * storage can be done directly on the metadata as-is.
     **/
    public static final String AS_IS_NEXTFETCHDATE_METADATA = "status.store.as.is.with.nextfetchdate";

    protected OutputCollector _collector;

    private Scheduler scheduler;
    private MetadataTransfer mdTransfer;

    private Cache<Object, Object> cache;
    private boolean useCache = true;

    private int maxFetchErrors = 3;

    private long cacheHits = 0;
    private long cacheMisses = 0;

    private int roundDateUnit = Calendar.SECOND;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        _collector = collector;

        scheduler = Scheduler.getInstance(stormConf);

        mdTransfer = MetadataTransfer.getInstance(stormConf);

        useCache = ConfUtils.getBoolean(stormConf, useCacheParamName, true);

        if (useCache) {
            String spec = ConfUtils.getString(stormConf, cacheConfigParamName);
            cache = CacheBuilder.from(spec).build();

            context.registerMetric("cache", new IMetric() {
                @Override
                public Object getValueAndReset() {
                    Map<String, Long> statsMap = new HashMap<>();
                    statsMap.put("hits", cacheHits);
                    statsMap.put("misses", cacheMisses);
                    statsMap.put("size", cache.size());
                    cacheHits = 0;
                    cacheMisses = 0;
                    return statsMap;
                }
            }, 30);
        }

        maxFetchErrors = ConfUtils
                .getInt(stormConf, maxFetchErrorsParamName, 3);

        String tmpdateround = ConfUtils.getString(stormConf,
                roundDateParamName, "SECOND");
        if (tmpdateround.equalsIgnoreCase("MINUTE")) {
            roundDateUnit = Calendar.MINUTE;
        } else if (tmpdateround.equalsIgnoreCase("HOUR")) {
            roundDateUnit = Calendar.HOUR;
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("originalUrl");

//        boolean potentiallyNew = status.equals(Status.DISCOVERED);
//
//        // if the URL is a freshly discovered one
//        // check whether it is already known in the cache
//        // if so we've already seen it and don't need to
//        // store it again
//        if (potentiallyNew && useCache) {
//            if (cache.getIfPresent(url) != null) {
//                // no need to add it to the queue
//                LOG.debug("URL {} already in cache", url);
//                cacheHits++;
//                return;
//            } else {
//                LOG.debug("URL {} not in cache", url);
//                cacheMisses++;
//            }
//        }

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // filter metadata just before storing it, so that non-persisted
        // metadata is available to fetch schedulers
        metadata = mdTransfer.filter(metadata);

        // extensions of this class will handle the storage
        // on a per document basis
        try {
            store(url, metadata, tuple);
        } catch (Exception e) {
            LOG.error("Exception caught when storing", e);
            _collector.fail(tuple);
        }
    }

    /**
     * Must be called by extending classes to store and collect in one go
     **/
    protected final void ack(Tuple t, String url) {
        // keep the URL in the cache
        if (useCache) {
            cache.put(url, "");
        }
    }

    protected abstract void store(String url, Metadata metadata, Tuple t) throws Exception;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constants.DELETION_STREAM_NAME, new Fields(
                "url", "metadata"));
    }
}

