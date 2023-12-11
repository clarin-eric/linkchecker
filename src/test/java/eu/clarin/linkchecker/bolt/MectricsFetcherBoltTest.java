/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.socket.PortFactory;
import static org.mockserver.model.HttpRequest.*;
import static org.mockserver.model.HttpResponse.*;

import com.digitalpebble.stormcrawler.Metadata;

import eu.clarin.linkchecker.config.Configuration;
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
   
   @SuppressWarnings("unchecked")
   @BeforeAll
   public void setup() throws IOException {      
      
      this.cas = startClientAndServer(PortFactory.findFreePort());   
      
      this.environment.set("LINKCHECKER_DIRECTORY", System.getProperty("user.dir"));
      this.environment.set("DATABASE_URI", "jdbc:h2:mem:linkcheckerTest;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
      this.environment.set("DATABASE_USER", "sa");
      this.environment.set("DATABASE_PASSWORD", "password");
      
      this.environment.set("HTTP_AGENTS", "");
      this.environment.set("HTTP_AGENT_NAME", "CLARIN Linkchecker (test)");
      this.environment.set("HTTP_AGENT_DESCRIPTION", "CLARIN Linkchecker (test)");
      this.environment.set("HTTP_AGENT_URL", "https://www.clarin.eu/linkchecker");
      this.environment.set("HTTP_AGENT_EMAIL", "linkchecker@clarin.eu");
      
      this.environment.set("HTTP_REDIRECT_LIMIT", "15");
      this.environment.set("HTTP_TIMEOUT", "5000");
      
      this.def = FluxParser.parseResource("/linkchecker.flux", false, true, null, true);
      
      
      //setting h2 driver for testing
      ((Map<String, Object>) def.getConfig().get("SPRING")).put("spring.datasource.driver-class-name", "org.h2.Driver");
      //setting MockServer proxy
      def.getConfig().put("http.proxy.host", "localhost");
      def.getConfig().put("http.proxy.port", this.cas.getPort());
      
      Configuration.init(getConfiguration());
   }
   
   @BeforeEach
   public void prepare() throws IOException {
      
      this.bolt = new MetricsFetcherBolt();
      
   }
   
   @Test
   public void parallelizationTest() throws InterruptedException {
      
      Map<String, Object> conf = this.getConfiguration();       
      TopologyContext context = mock(TopologyContext.class);
      OutputCollector collector = mock(OutputCollector.class);
      
      ArgumentCaptor<String> streamId = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Tuple> anchor = ArgumentCaptor.forClass(Tuple.class);
      ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
      
      
      bolt.prepare(conf, context, collector);
      
      this.cas.when(request()).respond(response().withStatusCode(200));
      
      getTuples(10).limit(100).forEach(tuple -> bolt.execute(tuple));
      
      while(!bolt.isQueueEmpty()) {
         
         Thread.sleep(1000);
      }
      
      verify(collector, atLeastOnce()).emit(streamId.capture(), anchor.capture(), values.capture());
      
      values.getAllValues().forEach(System.out::println);
      
      

   }
   
   @AfterAll
   public void shutdown() {
      
      this.cas.close();
   }
   


   private Map<String, Object> getConfiguration() {
      
      HashMap<String, Object> map = new HashMap<String, Object>();
      
      map.putAll(this.def.getConfig());
      
      return map;
   }
   
   private Stream<Tuple> getTuples(int numberOfHosts){
      
      AtomicInteger i = new AtomicInteger();
      
      return Stream.generate(() -> {
         
         String urlString = "http://www.wowasa" + i.get()%numberOfHosts + ".com/page" + i.get();
         
         Tuple tuple = mock(Tuple.class);
         
         when(tuple.getStringByField("url"))
         .thenReturn(urlString);
         
         Metadata metadata = new Metadata();
         
         metadata.setValue("urlId", i.getAndIncrement() + "");
         metadata.setValue("originalUrl", urlString);
         metadata.setValue("http.method.head", "true");
         
         when(tuple.getValueByField("metadata"))
         .thenReturn(metadata);
         
         return tuple;
      });
   }
}
