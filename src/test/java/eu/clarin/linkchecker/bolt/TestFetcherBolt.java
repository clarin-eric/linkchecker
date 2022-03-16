package eu.clarin.linkchecker.bolt;

import org.apache.storm.tuple.Tuple;

import com.digitalpebble.stormcrawler.bolt.StatusEmitterBolt;

public class TestFetcherBolt extends StatusEmitterBolt {

   /**
    * 
    */
   private static final long serialVersionUID = 1L;

   @Override
   public void execute(Tuple input) {
      
      System.out.println(input);
      
      ack(input);

   }

}
