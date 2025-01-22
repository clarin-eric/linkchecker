/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class SimpleStackBolt implements IRichBolt {
   
   private static final long serialVersionUID = 1L;

   private OutputCollector collector;
   
   private ArrayDeque<Map<String, String[]>> deque = new ArrayDeque<Map<String, String[]>>();
   
   private String outputFileStr;
   
   private long lastSaveTimeInMs = System.currentTimeMillis();
   
   @Override
   public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

      this.collector = collector;

      this.outputFileStr = ConfUtils.getString(topoConf, "directory.share", "/tmp") + "/latestChecks.obj";
   }

   @Override
   public void execute(Tuple input) {
      // we have to create a new Map instance not to modify the instance which is underlying the Metadata instance
      Map<String,String[]> map = ((Metadata) input.getValueByField("metadata")).asMap();
      
      deque.addFirst(map);
      
      if(this.deque.size() > 100) {
         
         this.deque.removeLast();
      }
      
      if((System.currentTimeMillis() - this.lastSaveTimeInMs) > 10000) { //saving every 10 seconds
         
         try(FileOutputStream fileOutputStream = new FileOutputStream(outputFileStr)){
            
            ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
            
            out.writeObject(this.deque);  
            
            this.lastSaveTimeInMs = System.currentTimeMillis();

         }
         catch(IOException ex) {
            
            log.error("can't serialize stack to file {}", this.outputFileStr);
         }         
      }

      collector.ack(input);
   }

   @Override
   public void cleanup() {

   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      
   }

   @Override
   public Map<String, Object> getComponentConfiguration() {

      return null;
   }   
}
