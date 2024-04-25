/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
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

@Slf4j
public class LPASpout extends AbstractQueryingSpout {

   private static final Scheme SCHEME = new StringTabScheme();
   
   private final String sql;

   /** Used to distinguish between instances in the logs **/
   protected String logIdprefix = "";
   
   private int counter = 0;
   private long lastCheckpoint =  System.currentTimeMillis();

   private static long lastUncheckedLinks = 0;
   
   
   public LPASpout(String sql) {
      super();
      this.sql = sql;
   }

   @SuppressWarnings({"unchecked" })
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

      log.debug("{} call LinkToBeCheckedResource.getNextLinksToCheck()", logIdprefix);
      
      this.isInQuery.set(true);
      long timeStartQuery = System.currentTimeMillis();
      
      GenericRepository gRep = Configuration.ctx.getBean(GenericRepository.class);

      
      try(Stream<Tuple> stream = gRep.findAll(sql, true).stream()){
         //noinspection SuspiciousMethodCalls
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

      //we log the number of unchecked links
      LPASpout.logUncheckedLinks();
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
      
      Configuration.setActive(null, false);
   }

   private static synchronized void logUncheckedLinks(){
      if((LPASpout.lastUncheckedLinks + Configuration.logIntervalUncheckedLinks) < System.currentTimeMillis()){

         LPASpout.lastUncheckedLinks = System.currentTimeMillis();

         GenericRepository gRep = Configuration.ctx.getBean(GenericRepository.class);

         try(Stream<Tuple> stream = gRep.findAll("SELECT COUNT(*) FROM url u WHERE u.id NOT IN (SELECT s.url_id from status s)", true).stream()){

            stream.forEach(tuple -> log.info("number of unchecked links: {}", tuple.get(0)));
         }
      }
   }
}
