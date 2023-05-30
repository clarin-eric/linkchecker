/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.socket.PortFactory;

import com.digitalpebble.stormcrawler.Metadata;

import uk.org.webcompere.systemstubs.*;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.*;

import lombok.extern.slf4j.Slf4j;

import static org.mockito.Mockito.*;

/**
 *
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(SystemStubsExtension.class)
public class MectricsFetcherBoltTest {  

   @SystemStub
   private EnvironmentVariables environment;
   
   private TopologyDef def;
   
   private ClientAndServer cas;
   
   private MetricsFetcherBolt bolt;
   
   @BeforeAll
   public void setup() {      
      
      this.cas = startClientAndServer(PortFactory.findFreePort());      
   }
   
   @BeforeEach
   public void prepare() {
      
      this.bolt = new MetricsFetcherBolt();
      

      
      
   }
   
   @Test
   public void parallelizationTest() {
      
      Map<String, Object> conf = this.getConfiguration(5000, 100);       
      TopologyContext context = mock(TopologyContext.class);
      OutputCollector collector = mock(OutputCollector.class);
      bolt.prepare(conf, context, collector);

   }
   
   @AfterAll
   public void shutdown() {
      
      this.cas.close();
   }
   
   @SuppressWarnings("unchecked")
   private Map<String, Object> getConfiguration(int spoutMaxResults, int spoutGroupMaxResults) {
      
      this.environment.set("SPOUT_MAX_RESULTS", String.valueOf(spoutMaxResults));
      this.environment.set("SPOUT_GROUP_MAX_RESULTS", String.valueOf(spoutGroupMaxResults));
      this.environment.set("HTTP_REDIRECT_LIMIT", "");
      
      this.environment.set("LINKCHECKER_DIRECTORY", System.getProperty("user.dir"));
      this.environment.set("DATABASE_URI", "jdbc:h2:mem:linkcheckerTest;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
      this.environment.set("DATABASE_USER", "sa");
      this.environment.set("DATABASE_PASSWORD", "password");
      this.environment.set("HTTP_AGENT_NAME", "CLARIN Linkchecker (test)");
      this.environment.set("HTTP_AGENT_URL", "https://www.clarin.eu/linkchecker");
      this.environment.set("HTTP_REDIRECT_LIMIT", "5");

      
      try {
         TopologyDef def = FluxParser.parseFile("crawler.flux", false, true, null, true);
         
         //setting h2 driver for testing
         ((Map<String, Object>) def.getConfig().get("SPRING")).put("spring.datasource.driver-class-name", "org.h2.Driver");
         //setting MockServer proxy
         def.getConfig().put("http.proxy.host", "localhost");
         def.getConfig().put("http.proxy.port", this.cas.getPort());
         
         
         return def.getConfig();
      }
      catch (IOException e) {
         log.error("can't create TopoplogyDef from crawler.flux - using mock instead!", e);
         return mock(Map.class);
      }
   }
   
   private Stream<Tuple> getTuples(int numberOfHosts){
      
      AtomicInteger i = new AtomicInteger();
      
      return Stream.generate(() -> {
         Tuple tuple = mock(Tuple.class);
         
         when(tuple.getStringByField("url"))
         .thenReturn("http://www.wowasa" + i.get()%numberOfHosts + "/page" + i.getAndIncrement());
         
         Metadata metadata = new Metadata();
         
         when(tuple.getValueByField("metadata"))
         .thenReturn(metadata);
         
         return tuple;
      });
   }
}
