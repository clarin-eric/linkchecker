package eu.clarin.linkchecker.spout;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.urlbuffer.SimpleURLBuffer;

public class QueueLimitURLBuffer extends SimpleURLBuffer {
   
   private static final int queueLimit = 100;
   @Override
   public synchronized boolean add(String URL, Metadata m) {
      String key = partitioner.getPartition(URL, m);
      if (key == null) {
          key = "_DEFAULT_";
      }
      if(!this.queues.containsKey(key) || this.queues.get(key).size() < queueLimit) {
         return super.add(URL, m, key);
      }
      return false;
   }
}
