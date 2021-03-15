package at.ac.oeaw.acdh.linkchecker.bolt;

import com.digitalpebble.stormcrawler.filtering.URLFilters;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.MetadataTransfer;

import at.ac.oeaw.acdh.linkchecker.config.Constants;

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

