/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class SimpleStackBolt implements IRichBolt {
   
   private static final long serialVersionUID = 1L;
   
   private Stack<Map<String, String[]>> stack = new Stack<Map<String, String[]>>();
   
   private String outputFileStr;
   
   @Override
   public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
      
      this.outputFileStr = ConfUtils.getString(topoConf, "directory.share", "/tmp") + "/latestChecks.obj";
   }

   @Override
   public void execute(Tuple input) {
      
      Metadata metadata = (Metadata) input.getValueByField("metadata");
      
      if(this.stack.size() >= 100) {
         
         try(FileOutputStream fileOutputStream = new FileOutputStream(outputFileStr)){
            
            ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
            
            out.writeObject(this.stack);  
            
            this.stack.clear();
         }
         catch(IOException ex) {
            
            log.error("can't serialize stack to file {}", this.outputFileStr);
         }
         
      }
      
      this.stack.push(metadata.asMap());
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
