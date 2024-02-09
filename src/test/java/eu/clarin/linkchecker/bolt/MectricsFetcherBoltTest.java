/**
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.bolt;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.http.HttpStatus;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.shade.org.apache.commons.lang.RandomStringUtils;
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
import org.mockserver.model.MediaType;
import org.mockserver.socket.PortFactory;
import static org.mockserver.model.HttpRequest.*;
import static org.mockserver.model.HttpResponse.*;
import static org.mockserver.model.HttpError.*;
import org.mockserver.model.Not;
import org.mockserver.model.NottableString;

import com.digitalpebble.stormcrawler.Metadata;

import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.utils.Category;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.*;

import static org.mockito.Mockito.*;

/**
 * Attention: 
 * 1. since robots.txt settings are cached in static way, the createTuples method creates a set of new hosts each time the method is called
 * 2. if you create expectations for a fixed number of times you mustn't forget that the first call for a new host is an internal call for robots.txt
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
      // starting the MockServer on a default port
      this.cas = startClientAndServer(PortFactory.findFreePort());   
      // setting environment variables for replacement in linkchecker-conf.yaml
      this.environment.set("LINKCHECKER_DIRECTORY", System.getProperty("user.dir"));
      this.environment.set("DATABASE_URI", "jdbc:h2:mem:linkcheckerTest;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
      this.environment.set("DATABASE_USER", "sa");
      this.environment.set("DATABASE_PASSWORD", "password");
      
      this.environment.set("HTTP_AGENTS", "");
      this.environment.set("HTTP_AGENT_NAME", "CLARIN-Linkchecker");
      this.environment.set("HTTP_AGENT_DESCRIPTION", "");
      this.environment.set("HTTP_AGENT_URL", "https://www.clarin.eu/linkchecker");
      this.environment.set("HTTP_AGENT_EMAIL", "linkchecker@clarin.eu");
      
      this.environment.set("HTTP_REDIRECT_LIMIT", "15");
      this.environment.set("HTTP_TIMEOUT", "5000");
      // parsing linkchecker.flux, which includes linkchecker-conf.yaml
      this.def = FluxParser.parseResource("/linkchecker.flux", false, true, null, true);
      
      
      //setting h2 driver for testing
      ((Map<String, Object>) def.getConfig().get("SPRING")).put("spring.datasource.driver-class-name", "org.h2.Driver");
      //setting MockServer as proxy-server
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
            response().withStatusCode(HttpStatus.SC_OK)
         );
      
      SimpleTestSet testSet = new SimpleTestSet();
      
      testSet.execute(createTuples(1, true).limit(1).findFirst().get());
      
      testSet.verify();
      
      assertTrue(this.cas.retrieveRecordedRequests(null)[0].getFirstHeader("User-agent").startsWith("CLARIN-Linkchecker"));     
   }
   
   /*
    * Here we test if the robots.txt is respected and if it is overridden by an individual setting 
    * Generally we have set a crawl delay of 1 second but the robots.txt in our testcase requires a crawl delay of 2 seconds. 
    * For host "hdl.handle.net" we have configured an individual crawl delay of 0.1 seconds
    */
   
   @Test
   public void robotsTxtTest() {
      
      this.cas
      .when(
            request()
            .withPath("/robots.txt")
         )
      .respond(
            response()
               .withStatusCode(HttpStatus.SC_OK)
               .withBody("""
                     User-agent: CLARIN-Linkchecker
                     Allow: /
                     Crawl-delay: 2
                     """, MediaType.TEXT_PLAIN)

         );
      
      this.cas
      .when(
            request()
         )
      .respond(
            response()
               .withStatusCode(HttpStatus.SC_OK)
         );
      
      StandardTestSet testSet = new StandardTestSet();
      
      this.createTuples(1).limit(5).forEach(tuple -> testSet.execute(tuple));
      
      LongStream.range(0, 5).forEach(i -> testSet.execute(this.createTuple(i, "http://hdl.handle.net/handle" +i, true)));
      
      testSet.verify();
      
      AtomicLong previousDateInMs = new AtomicLong();
      
      // there must be no non handle link where the time difference between two successive requests on the same host is smaller than 2000 ms
      // since the robots.txt requires 2000ms
      assertFalse(
      
         testSet
         .getValues()
         .getAllValues()
         .stream()
         .filter(values -> !String.class.cast(values.get(0)).startsWith("http://hdl.handle.net/handle")) // filter for non handles
         .map(values -> Metadata.class.cast(values.get(1)).getFirstValue("fetch.checkingDate")) // get the date string from the metadata field
         .map(LocalDateTime::parse) // parse the date string to LocalDateTime
         .map(ldt -> ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()) // get the time in ms from LocalDateTime
         .map(dateInMs -> { // calculate the time delta between two successive request
            
            long dateDiffInMs = (previousDateInMs.get() == 0?9999:dateInMs - previousDateInMs.get());         
            previousDateInMs.set(dateInMs);         
            
            return dateDiffInMs;
         })
         .anyMatch(dateDiffInMs -> dateDiffInMs < 2000)
      );
      
      previousDateInMs.set(0);
      
      // for handle links the must be at least two successive requests with a time difference under 1000ms
      // since we have configured an individual crawl delay which dominates the crawl delay of 2000ms from robots.txt
      // and the general crawl delay of 1000ms
      assertTrue(
            
            testSet
            .getValues()
            .getAllValues()
            .stream()
            .filter(values -> String.class.cast(values.get(0)).startsWith("http://hdl.handle.net/handle")) // filter for handles
            .map(values -> Metadata.class.cast(values.get(1)).getFirstValue("fetch.checkingDate")) // get the date string from the metadata field
            .map(LocalDateTime::parse) // parse the date string to LocalDateTime
            .map(ldt -> ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()) // get the time in ms from LocalDateTime
            .map(dateInMs -> { // calculate the time delta between two successive request
               
               long dateDiffInMs = (previousDateInMs.get() == 0?9999:dateInMs - previousDateInMs.get());         
               previousDateInMs.set(dateInMs);         
               
               return dateDiffInMs;
            })
            .anyMatch(dateDiffInMs -> dateDiffInMs < 1000)
         );
      
   }
   
   /*
    * whenever we don't get an ok-status code at a head request we send the Tuple to the redirect stream, means we repeat the request with a GET request
    */
   @Test
   public void headGetTest() {
      
      int[] errorCodes = {400, 401, 402, 403, 404, 500, 501, 502, 503};
      
      this.cas
      .when(
            request()
            .withPath("/robots.txt")
         )
      .respond(
            response()
               .withStatusCode(HttpStatus.SC_NOT_FOUND)
         );
      
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
            request()
         )
      .error(
            error().withDropConnection(true)
         );  
             
      
      StandardTestSet testSet = new StandardTestSet();
      
      createTuples(10).limit(errorCodes.length +1).forEach(tuple -> testSet.execute(tuple));
      
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
               .withStatusCode(HttpStatus.SC_OK)
               .withDelay(TimeUnit.SECONDS, 7)
         );
      
      StandardTestSet testSet = new StandardTestSet();
      
      // random tuple processed with a HEAD request
      testSet.execute(createTuples(1, true).limit(1).findFirst().get());
      // random tuple, processed with a GET request
      testSet.execute(createTuples(1, false).limit(1).findFirst().get());
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
   @Test
   public void individialCrawlDelayTest() {
      
      this.cas
      .when(
            request()
         )
      .respond(
            response()
               .withStatusCode(HttpStatusCode.OK_200.code())
         );
      
      StandardTestSet testSet = new StandardTestSet();
      
      this.createTuples(1).limit(5).forEach(tuple -> testSet.execute(tuple));
      
      LongStream.range(0, 5).forEach(i -> testSet.execute(this.createTuple(i, "http://hdl.handle.net/handle" +i, true)));
      
      testSet.verify();
      
      AtomicLong previousDateInMs = new AtomicLong();
      
      // there must be no non handle link where the time difference between two successive requests on the same host is smaller than general delay of 1000 ms
      assertFalse(
      
         testSet
         .getValues()
         .getAllValues()
         .stream()
         .filter(values -> !String.class.cast(values.get(0)).startsWith("http://hdl.handle.net/handle")) // filter for non handles
         .map(values -> Metadata.class.cast(values.get(1)).getFirstValue("fetch.checkingDate")) // get the date string from the metadata field
         .map(LocalDateTime::parse) // parse the date string to LocalDateTime
         .map(ldt -> ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()) // get the time in ms from LocalDateTime
         .map(dateInMs -> { // calculate the time delta between two successive request
            
            long dateDiffInMs = (previousDateInMs.get() == 0?9999:dateInMs - previousDateInMs.get());         
            previousDateInMs.set(dateInMs);         
            
            return dateDiffInMs;
         })
         .anyMatch(dateDiffInMs -> dateDiffInMs < 1000)
      );
      
      previousDateInMs.set(0);
      
      // for handle links the must be at least two successive requests with a time difference under 1000ms
      // since we have configured an individual crawl delay which dominates the crawl delay of 2000ms from robots.txt
      // and the general crawl delay of 1000ms
      assertTrue(
            
            testSet
            .getValues()
            .getAllValues()
            .stream()
            .filter(values -> String.class.cast(values.get(0)).startsWith("http://hdl.handle.net/handle")) // filter for handles
            .map(values -> Metadata.class.cast(values.get(1)).getFirstValue("fetch.checkingDate")) // get the date string from the metadata field
            .map(LocalDateTime::parse) // parse the date string to LocalDateTime
            .map(ldt -> ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()) // get the time in ms from LocalDateTime
            .map(dateInMs -> { // calculate the time delta between two successive request
               
               long dateDiffInMs = (previousDateInMs.get() == 0?9999:dateInMs - previousDateInMs.get());         
               previousDateInMs.set(dateInMs);         
               
               return dateDiffInMs;
            })
            .anyMatch(dateDiffInMs -> dateDiffInMs < 1000)
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
   
   @Test
   public void categorizationTest() {
      
      int[] statusCodes = {200, 304, 301, 302, 303, 307, 308, 405, 429, 401, 403};
      
      this.cas
      .when(
            request()
               .withPath("/robots.txt")
         )
      .respond(
            response().withStatusCode(404)
         );
      
      
      Arrays.stream(statusCodes).forEach( statusCode -> {
         this.cas
         .when(
               request()
                  .withPath(NottableString.not("/robots.txt")),
               Times.once()
            )
         .respond(
               response().withStatusCode(statusCode)
            );
      });
      
      StandardTestSet testSet = new StandardTestSet();
      
      createTuples(1, false).limit(statusCodes.length).forEach(tuple -> testSet.execute(tuple));
      
      testSet.verify();
      
      
//      assertTrue(
         testSet.getValues().getAllValues().stream()
            .map(values -> Metadata.class.cast(values.get(1)))
            .forEach(System.out::println);
//            .anyMatch(statusCode -> "200".equals(statusCode))
//         );
      
   }
   
   @AfterEach
   public void reset() {
      // resetting the MockServer, basically to remove all expectations
      this.cas.reset();     
   }
   
   @AfterAll
   public void shutdown() {
      // closing the MockServer
      this.cas.close();
   }


   /**
    * @return a Map<String, Object> parsed from linkchecker-conf.yaml
    */
   private Map<String, Object> getConfiguration() {
      
      HashMap<String, Object> map = new HashMap<String, Object>();
      
      map.putAll(this.def.getConfig());
      
      return map;
   }
   
   /**
    * @param numberOfHosts the number of different hosts in the tuples of the stream 
    * @return a Stream<Tuple>, each Tuple processable by MetricsFetcherBolt.execute as a HEAD request
    */
   
   private Stream<Tuple> createTuples(int numberOfHosts){
      
      return createTuples(numberOfHosts, true);
   }

   
   /**
    * @param numberOfHosts the number of different hosts in the tuples of the stream 
    * @param isHeadRequest sets the property »http.method.head« to true or false which determinates, if the request is send as HEAD or GET
    * @return a Stream<Tuple>, each Tuple processable by MetricsFetcherBolt.execute 
    */
   private Stream<Tuple> createTuples(int numberOfHosts, boolean isHeadRequest){
      
      AtomicLong id = new AtomicLong();
      
      String hostName = RandomStringUtils.randomAlphabetic(10);
      
      return Stream.generate(() -> {
         
         return createTuple(id.get(), "http://www." + hostName + id.get()%numberOfHosts + ".com/page" + id.getAndIncrement(), isHeadRequest);         
      });
   }
   
   /**
    * @param id corresponds to the id field of the url table
    * @param urlString the complete URL as a String (f.e. http://www.wowasa.com/page1)
    * @param isHead sets the property »http.method.head« to true or false which determinates, if the request is send as HEAD or GET 
    * @return a Tuple processable by MetricsFetcherBolt.execute 
    */
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

         while(this.mockingDetails.getInvocations().size() < this.invocations.get()) {
            
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
