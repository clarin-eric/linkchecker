/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockingDetails;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpStatusCode;
import org.mockserver.socket.PortFactory;
import static org.mockserver.model.HttpRequest.*;
import static org.mockserver.model.HttpResponse.*;
import static org.mockserver.model.HttpError.*;

import com.digitalpebble.stormcrawler.Metadata;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.utils.Category;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.*;

import static org.mockito.Mockito.*;

/**
 *
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(SystemStubsExtension.class)
@Slf4j
public class MectricsFetcherBoltTest {  

   @SystemStub
   private EnvironmentVariables environment;
   
   private TopologyDef def;
   
   private ClientAndServer cas;
   
   @SuppressWarnings("unchecked")
   @BeforeAll
   public void setup() throws IOException {      
      
      this.cas = startClientAndServer(PortFactory.findFreePort());   
      
      this.environment.set("LINKCHECKER_DIRECTORY", System.getProperty("user.dir"));
      this.environment.set("DATABASE_URI", "jdbc:h2:mem:linkcheckerTest;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
      this.environment.set("DATABASE_USER", "sa");
      this.environment.set("DATABASE_PASSWORD", "password");
      
      this.environment.set("HTTP_AGENTS", "");
      this.environment.set("HTTP_AGENT_NAME", "CLARIN Linkchecker");
      this.environment.set("HTTP_AGENT_DESCRIPTION", "");
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
   
   @Test
   public void redirectTest() {
      
      // expectation
      Configuration.redirectStatusCodes.forEach(redirectCode -> {
         
         this.cas
         .when(
               request()
            )
         .respond(
               response()
                  .withStatusCode(redirectCode)
                  .withHeader("Location", "http://devnull.org")
            ); 
      });
      
      StandardTestSet testSet = new StandardTestSet();
      
      createTuples(5).limit(Configuration.redirectStatusCodes.size()).forEach(tuple -> testSet.execute(tuple));
      
      testSet.verify();      
      
      // all tuples should be redirected
      assertTrue(testSet.getStreamId().getAllValues().stream().allMatch(streamId -> streamId.equals(eu.clarin.linkchecker.config.Constants.RedirectStreamName)));
      
      // the second value (index 1) must be an instance of Metadata with a setting http.method.head=true, since our first call to the new URL must be a HEAD request
      assertTrue(
            testSet
               .getValues()
               .getAllValues()
               .stream()
               .map(values -> values.get(1))
               .allMatch(value -> value instanceof Metadata && Metadata.class.cast(value).getFirstValue("http.method.head").equals("true"))
            );
      // the value for fetch.redirectCount must be 1, since it's a countable redirect
      assertTrue(
            testSet
               .getValues()
               .getAllValues()
               .stream()
               .map(values -> values.get(1))
               .allMatch(value -> value instanceof Metadata && "1".equals(Metadata.class.cast(value).getFirstValue("fetch.redirectCount")))
         );
      // all redirect URLs (in values.get(0)) must be "http://devnull.org"
      assertTrue(
            testSet
               .getValues()
               .getAllValues()
               .stream()
               .map(values -> values.get(0))
               .allMatch(value -> "http://devnull.org".equals(value))
         );
   }
   
   /*
    * Here we test, if the User-agent string is send
    */
   @Test
   public void userAgentTest() {
      
      this.cas
      .when(
            request()
         )
      .respond(
            response().withStatusCode(200)
         );
      
      SimpleTestSet testSet = new SimpleTestSet();
      
      testSet.execute(createRandomTuple(true));
      
      testSet.verify();
      
      assertTrue(this.cas.retrieveRecordedRequests(null)[0].getFirstHeader("User-agent").startsWith("CLARIN Linkchecker"));     
   }
   
   /*
    * whenever we don't get an ok-status code at a head request we send the Tuple to the redirect stream, means we repeat the request with a GET request
    */
   @Test
   public void headGetTest() {
      
      int[] errorCodes = {400, 401, 402, 403, 404, 500, 501, 502, 503};
      
      // creating expectations with the error codes
      Arrays.stream(errorCodes).forEach(errorCode -> {
         
         this.cas
            .when(
                  request(), 
                  Times.once()
               )
            .respond(
                  response().withStatusCode(errorCode)
               ); 
      });
      // creating an expectation to drop the connection
      this.cas
      .when(
            request(), 
            Times.once()
         )
      .error(
            error().withDropConnection(true)
         );             
      
      StandardTestSet testSet = new StandardTestSet();
      
      createTuples(5).limit(errorCodes.length +1).forEach(tuple -> testSet.execute(tuple));
      
      testSet.verify();      
      
      // all tuples should be redirected
      assertTrue(testSet.getStreamId().getAllValues().stream().allMatch(streamId -> streamId.equals(eu.clarin.linkchecker.config.Constants.RedirectStreamName)));
      
      // the second value (index 1) must be an instance of Metadata with a setting http.method.head=false, which will cause a GET request when injected next time to the bolt
      assertTrue(
            testSet
               .getValues()
               .getAllValues()
               .stream()
               .map(values -> values.get(1))
               .allMatch(value -> value instanceof Metadata && Metadata.class.cast(value).getFirstValue("http.method.head").equals("false"))
            );
      // the value for fetch.redirectCount must either not be set or 0, since it's not a countable redirect but a switch for a GET request
      assertTrue(
            testSet
               .getValues()
               .getAllValues()
               .stream()
               .map(values -> values.get(1))
               .allMatch(value -> value instanceof Metadata && (!Metadata.class.cast(value).containsKey("fetch.redirectCount") || Metadata.class.cast(value).getFirstValue("fetch.redirectCount").equals("0")))
         );
   }
   
   /*
    * We configure the server to respond with status code 200 but with a delay of 7 seconds. This is more than the general timeout of 5 seconds 
    * but for URLs with host »id.acdh.oeaw.ac.at« we have configured a timeout of 10 seconds. 
    */
   @Test
   public void individualTimeoutTest() {
      
      this.cas
      .when(
            request()
         )
      .respond(
            response()
               .withStatusCode(HttpStatusCode.OK_200.code())
               .withDelay(TimeUnit.SECONDS, 7)
         );
      
      StandardTestSet testSet = new StandardTestSet();
      
      // random tuple processed with a HEAD request
      testSet.execute(createRandomTuple(true));
      // random tuple, processed with a GET request
      testSet.execute(createRandomTuple(false));
      // tuple with individual timeout, processed with HEAD request
      testSet.execute(createTuple(1l, "http://id.acdh.oeaw.ac.at/test", true));
      
      testSet.verify();
      
      
      // the random tuple with head request should be redirected to be processed again with a GET request
      assertTrue(
            IntStream
            .range(0, 3)
            .filter(i -> testSet.getStreamId().getAllValues().get(i).equals(eu.clarin.linkchecker.config.Constants.RedirectStreamName))
            .mapToObj(i -> testSet.getValues().getAllValues().get(i))
            .anyMatch(values -> !values.get(0).equals("http://id.acdh.oeaw.ac.at/test") 
               && values.get(1) instanceof Metadata
               && "false".equals(Metadata.class.cast(values.get(1)).getFirstValue("http.method.head")))
            );
      
      // the tuple with the individual should go to status stream with category ok 
      assertTrue(
         IntStream
         .range(0, 3)
         .filter(i -> testSet.getStreamId().getAllValues().get(i).equals(com.digitalpebble.stormcrawler.Constants.StatusStreamName))
         .mapToObj(i -> testSet.getValues().getAllValues().get(i))
         .anyMatch(values -> values.get(0).equals("http://id.acdh.oeaw.ac.at/test") 
            && values.get(1) instanceof Metadata
            && Category.Ok.name().equals(Metadata.class.cast(values.get(1)).getFirstValue("fetch.category")))
         );
            

      // the random tuple processed with GET request should go to status stream with category broken 
      assertTrue(
            IntStream
            .range(0, 3)
            .filter(i -> testSet.getStreamId().getAllValues().get(i).equals(com.digitalpebble.stormcrawler.Constants.StatusStreamName))
            .mapToObj(i -> testSet.getValues().getAllValues().get(i))
            .anyMatch(values -> !values.get(0).equals("http://id.acdh.oeaw.ac.at/test") 
               && values.get(1) instanceof Metadata
               && Category.Broken.name().equals(Metadata.class.cast(values.get(1)).getFirstValue("fetch.category")))
            
         );
      
      
   }
   
   public void individialCrawlDelayTest() {
      
      this.cas
      .when(
            request()
         )
      .respond(
            response()
               .withStatusCode(HttpStatusCode.OK_200.code())
         );
   }
   
   @Test
   public void parallelizationTest() throws InterruptedException {
      
      StandardTestSet testSet = new StandardTestSet();
      
      this.cas
         .when(
               request()
            )
         .respond(
               response().withStatusCode(200)
            );
      
      createTuples(10).limit(100).forEach(tuple -> testSet.execute(tuple));
      
      testSet.verify();
      
      //we must have 100 results
      assertEquals(100, testSet.getStreamId().getAllValues().size());
      // and all results go to status stream
      assertTrue(testSet.getStreamId().getAllValues().stream().allMatch(streamId -> streamId.equals(com.digitalpebble.stormcrawler.Constants.StatusStreamName)));
      
   }
   
   @AfterEach
   public void reset() {
      // resetting the MockServer
      this.cas.reset();     
   }
   
   @AfterAll
   public void shutdown() {
      // closing the MockServer
      this.cas.close();
   }
   


   private Map<String, Object> getConfiguration() {
      
      HashMap<String, Object> map = new HashMap<String, Object>();
      
      map.putAll(this.def.getConfig());
      
      return map;
   }
   
   private Stream<Tuple> createTuples(int numberOfHosts){
      
      AtomicLong id = new AtomicLong();
      
      return Stream.generate(() -> {
         
         return createTuple(id.get(), "http://www.wowasa" + id.get()%numberOfHosts + ".com/page" + id.getAndIncrement(), true);         
      });
   }
   
   private Tuple createTuple(Long id, String urlString, boolean isHead) {
      
      Tuple tuple = mock(Tuple.class);
      
      when(tuple.getStringByField("url"))
      .thenReturn(urlString);
      
      Metadata metadata = new Metadata();
      
      metadata.setValue("urlId", id + "");
      metadata.setValue("originalUrl", urlString);
      metadata.setValue("http.method.head", String.valueOf(isHead));
      
      when(tuple.contains("metadata"))
      .thenReturn(true);
      
      when(tuple.getValueByField("metadata"))
      .thenReturn(metadata);
      
      return tuple;
   }
   
   private Tuple createRandomTuple(boolean isHead) {
      
      Long id = new Random().nextLong();
      
      return createTuple(id, "http://www.wowasa" +id + ".com/page" + id, isHead);      
   }
   
   /*
    * The StandardTestSet enlarges the SimpleTestSet for accessible instances of ArgumentCaptor
    * The verify-method not only waits for tuples to be processed but fills the captors.   
    */
   
   @Getter
   private class StandardTestSet extends SimpleTestSet{
      
      private final ArgumentCaptor<String> streamId;
      private final ArgumentCaptor<Tuple> anchor;
      private final ArgumentCaptor<Values> values;    
      
      public StandardTestSet() {
         
         super();
         
         this.streamId = ArgumentCaptor.forClass(String.class);
         this.anchor = ArgumentCaptor.forClass(Tuple.class);
         this.values = ArgumentCaptor.forClass(Values.class);         
      }
      
      
      
      public void verify() {

         super.verify();
         
         Mockito.verify(this.collector, times(this.invocations.get())).emit(this.streamId.capture(), this.anchor.capture(), this.values.capture());        
      }
   }
   
   /*
    * The SimpleTestSet creates an accessible instance of MetricsFetcherBolt and a mock instance of OutputCollector. 
    * The verify-method assures only that all input tuples have been processed  
    */
   
   private class SimpleTestSet {
      
      protected final MetricsFetcherBolt bolt;
      
      protected final OutputCollector collector;
      
      private final MockingDetails mockingDetails;
      
      protected final AtomicInteger invocations;
      
      public SimpleTestSet() {
         
         this.bolt = new MetricsFetcherBolt(); 
         
         this.collector = mock();
         
         this.mockingDetails = Mockito.mockingDetails(collector);
         
         this.invocations = new AtomicInteger();
         
         bolt.prepare(getConfiguration(), mock(TopologyContext.class), this.collector);          
      }
      
      public void execute(Tuple tuple) {
         
         this.bolt.execute(tuple);
         this.invocations.incrementAndGet();
      }
      
      public void verify() {

         while(this.mockingDetails.getInvocations().size() < this.invocations.get() *2) {
            
            try {
               Thread.sleep(500);
            }
            catch (InterruptedException e) {
               
               log.error("", e);
            }
         }
      }            
   }
}
