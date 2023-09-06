/**
 * Licensed to DigitalPebble Ltd under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.clarin.linkchecker.bolt;

import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.bolt.StatusEmitterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.protocol.HttpHeaders;
import com.digitalpebble.stormcrawler.protocol.Protocol;
import com.digitalpebble.stormcrawler.protocol.ProtocolFactory;
import com.digitalpebble.stormcrawler.protocol.ProtocolResponse;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.robots.BaseRobotRules;
import eu.clarin.linkchecker.config.Configuration;
import eu.clarin.linkchecker.persistence.utils.Category;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.net.ssl.SSLException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.RedirectException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */
@Slf4j
public class MetricsFetcherBolt extends StatusEmitterBolt {

   private static final long serialVersionUID = 1L;

   private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

   /**
    * Acks URLs which have spent too much time in the queue, should be set to a
    * value equals to the topology timeout
    */
   public static final String QUEUED_TIMEOUT_PARAM_KEY = "fetcher.timeout.queue";

   private final AtomicInteger activeThreads = new AtomicInteger(0);
   private final AtomicInteger spinWaiting = new AtomicInteger(0);

   private FetchItemQueues fetchQueues;

   private ProtocolFactory protocolFactory;

   private int taskID = -1;

   boolean sitemapsAutoDiscovery = false;

   private File debugfiletrigger;

   /** blocks the processing of new URLs if this value is reached * */
   private int maxNumberURLsInQueues = -1;

   private String[] beingFetched;

   // Clarin specific variables
   private int HTTP_REDIRECT_LIMIT;

   @Override
   public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      int tickFrequencyInSeconds = 5;
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
      return conf;
   }

   /** This class described the item to be fetched. */
   private static class FetchItem {

      String queueID;
      String url;
      Tuple t;
      long creationTime;

      private FetchItem(String url, Tuple t, String queueID) {
         this.url = url;
         this.queueID = queueID;
         this.t = t;
         this.creationTime = System.currentTimeMillis();
      }

      /**
       * Create an item. Queue id will be created based on <code>queueMode</code>
       * argument, either as a protocol + hostname pair, protocol + IP address pair or
       * protocol+domain pair.
       */
      public static FetchItem create(URL u, String url, Tuple t, String queueMode) {

         String queueID;

         String key = null;
         // reuse any key that might have been given
         // be it the hostname, domain or IP
         if (t.contains("key")) {
            key = t.getStringByField("key");
         }
         if (StringUtils.isNotBlank(key)) {
            queueID = key.toLowerCase(Locale.ROOT);
            return new FetchItem(url, t, queueID);
         }

         if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
            try {
               final InetAddress addr = InetAddress.getByName(u.getHost());
               key = addr.getHostAddress();
            }
            catch (final UnknownHostException e) {
               log.warn("Unable to resolve IP for {}, using hostname as key.", u.getHost());
               key = u.getHost();
            }
         }
         else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
            key = PaidLevelDomain.getPLD(u.getHost());
            if (key == null) {
               log.warn("Unknown domain for url: {}, using hostname as key", url);
               key = u.getHost();
            }
         }
         else {
            key = u.getHost();
         }

         if (key == null) {
            log.warn("Unknown host for url: {}, using URL string as key", url);
            key = u.toExternalForm();
         }

         queueID = key.toLowerCase(Locale.ROOT);
         return new FetchItem(url, t, queueID);
      }
   }

   /**
    * This class handles FetchItems which come from the same host ID (be it a
    * proto/hostname or proto/IP pair). It also keeps track of requests in progress
    * and elapsed time between requests.
    */
   private static class FetchItemQueue {
      final BlockingDeque<FetchItem> queue;

      private final AtomicInteger inProgress = new AtomicInteger();
      private final AtomicLong nextFetchTime = new AtomicLong();

      private final long minCrawlDelay;
      private final int maxThreads;

      long crawlDelay;

      public FetchItemQueue(int maxThreads, long crawlDelay, long minCrawlDelay, int maxQueueSize) {
         this.maxThreads = maxThreads;
         this.crawlDelay = crawlDelay;
         this.minCrawlDelay = minCrawlDelay;
         this.queue = new LinkedBlockingDeque<>(maxQueueSize);
         // ready to start
         setNextFetchTime(System.currentTimeMillis(), true);
      }

      public int getQueueSize() {
         return queue.size();
      }

      public int getInProgressSize() {
         return inProgress.get();
      }

      public void finishFetchItem(FetchItem it, boolean asap) {
         if (it != null) {
            inProgress.decrementAndGet();
            setNextFetchTime(System.currentTimeMillis(), asap);
         }
      }

      public boolean addFetchItem(FetchItem it) {
         return queue.offer(it);
      }

      public FetchItem getFetchItem() {
         if (inProgress.get() >= maxThreads)
            return null;
         if (nextFetchTime.get() > System.currentTimeMillis())
            return null;
         FetchItem it = queue.pollFirst();
         if (it != null) {
            inProgress.incrementAndGet();
         }
         return it;
      }

      private void setNextFetchTime(long endTime, boolean asap) {
         if (!asap)
            nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
         else
            nextFetchTime.set(endTime);
      }
   }

   /**
    * Convenience class - a collection of queues that keeps track of the total
    * number of items, and provides items eligible for fetching from any queue.
    */
   private static class FetchItemQueues {
      final Map<String, FetchItemQueue> queues = Collections.synchronizedMap(new LinkedHashMap<>());

      AtomicInteger inQueues = new AtomicInteger(0);

      final int defaultMaxThread;
      final long crawlDelay;
      final Map<String, Long> crawlDelayByServer;
      final long minCrawlDelay;

      int maxQueueSize;

      public static final String QUEUE_MODE_HOST = "byHost";
      public static final String QUEUE_MODE_DOMAIN = "byDomain";
      public static final String QUEUE_MODE_IP = "byIP";

      String queueMode;

      final Map<Pattern, Integer> customMaxThreads = new HashMap<>();

      public FetchItemQueues(Config conf) {

         this.defaultMaxThread = ConfUtils.getInt(conf, "fetcher.threads.per.queue", 1);
         queueMode = ConfUtils.getString(conf, "fetcher.queue.mode", QUEUE_MODE_HOST);
         // check that the mode is known
         if (!queueMode.equals(QUEUE_MODE_IP) && !queueMode.equals(QUEUE_MODE_DOMAIN)
               && !queueMode.equals(QUEUE_MODE_HOST)) {
            log.error("Unknown partition mode : {} - forcing to byHost", queueMode);
            queueMode = QUEUE_MODE_HOST;
         }
         log.debug("Using queue mode : {}", queueMode);

         this.crawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.delay", 1.0f) * 1000);
         
         this.crawlDelayByServer = new HashMap<String, Long>();
         
         // allow to set individual server delays by host
         if(conf.containsKey("fetcher.server.delay.individual")) {
            
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) conf.get("fetcher.server.delay.individual");
            
            map.keySet().forEach(key -> this.crawlDelayByServer.put(key, (long) (ConfUtils.getFloat(map, key, 0.0f) * 1000)));
         }

         this.minCrawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.min.delay", 0.0f) * 1000);
         this.maxQueueSize = ConfUtils.getInt(conf, "fetcher.max.queue.size", -1);
         if (this.maxQueueSize == -1) {
            this.maxQueueSize = Integer.MAX_VALUE;
         }

         // order is not guaranteed
         for (Entry<String, Object> e : conf.entrySet()) {
            String key = e.getKey();
            if (!key.startsWith("fetcher.maxThreads."))
               continue;
            Pattern patt = Pattern.compile(key.substring("fetcher.maxThreads.".length()));
            customMaxThreads.put(patt, ((Number) e.getValue()).intValue());
         }
      }

      /** @return true if the URL has been added, false otherwise * */
      public synchronized boolean addFetchItem(URL u, String url, Tuple input) {
         FetchItem it = FetchItem.create(u, url, input, queueMode);
         FetchItemQueue fiq = getFetchItemQueue(it.queueID);
         boolean added = fiq.addFetchItem(it);
         if (added) {
            inQueues.incrementAndGet();
         }

         log.debug("{} added to queue {}", url, it.queueID);

         return added;
      }

      public synchronized void finishFetchItem(FetchItem it, boolean asap) {
         FetchItemQueue fiq = queues.get(it.queueID);
         if (fiq == null) {
            log.warn("Attempting to finish item from unknown queue: {}", it.queueID);
            return;
         }
         fiq.finishFetchItem(it, asap);
      }

      public synchronized FetchItemQueue getFetchItemQueue(String id) {
         FetchItemQueue fiq = queues.get(id);
         if (fiq == null) {
            int customThreadVal = defaultMaxThread;
            // custom maxThread value?
            for (Entry<Pattern, Integer> p : customMaxThreads.entrySet()) {
               if (p.getKey().matcher(id).matches()) {
                  customThreadVal = p.getValue();
                  break;
               }
            }
            // initialize queue
            fiq = new FetchItemQueue(customThreadVal, crawlDelay, minCrawlDelay, maxQueueSize);
            queues.put(id, fiq);
         }
         return fiq;
      }

      public synchronized FetchItem getFetchItem() {
         if (queues.isEmpty()) {
            return null;
         }

         FetchItemQueue start = null;

         do {
            Iterator<Entry<String, FetchItemQueue>> i = queues.entrySet().iterator();

            if (!i.hasNext()) {
               return null;
            }

            Map.Entry<String, FetchItemQueue> nextEntry = i.next();

            if (nextEntry == null) {
               return null;
            }

            FetchItemQueue fiq = nextEntry.getValue();

            // We remove the entry and put it at the end of the map
            i.remove();

            // reap empty queues
            if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
               continue;
            }

            // Put the entry at the end no matter the result
            queues.put(nextEntry.getKey(), nextEntry.getValue());

            // In case of we are looping
            if (start == null) {
               start = fiq;
            }
            else if (fiq == start) {
               return null;
            }

            FetchItem fit = fiq.getFetchItem();

            if (fit != null) {
               inQueues.decrementAndGet();
               return fit;
            }

         }
         while (!queues.isEmpty());

         return null;
      }
   }

   /** This class picks items from queues and fetches the pages. */
   private class FetcherThread extends Thread {

      // max. delay accepted from robots.txt
      private final long maxCrawlDelay;
      // whether maxCrawlDelay overwrites the longer value in robots.txt
      // (otherwise URLs in this queue are skipped)
      private final boolean maxCrawlDelayForce;
      // whether the default delay is used even if the robots.txt
      // specifies a shorter crawl-delay
      private final boolean crawlDelayForce;
      private final int threadNum;

      private long timeoutInQueues = -1;

      // by default remains as is-pre 1.17
      private String protocolMDprefix = "";

      public FetcherThread(Config conf, int num) {
         this.setDaemon(true); // don't hang JVM on exit
         this.setName("FetcherThread #" + num); // use an informative name

         this.maxCrawlDelay = ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30) * 1000L;
         this.maxCrawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false);
         this.crawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.server.delay.force", false);
         this.threadNum = num;
         timeoutInQueues = ConfUtils.getLong(conf, QUEUED_TIMEOUT_PARAM_KEY, timeoutInQueues);
         protocolMDprefix = ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMDprefix);
      }

      @Override
      public void run() {
         while (true) {
            FetchItem fit = fetchQueues.getFetchItem();
            if (fit == null) {
               log.trace("{} spin-waiting ...", getName());
               // spin-wait.
               spinWaiting.incrementAndGet();
               try {
                  Thread.sleep(100);
               }
               catch (InterruptedException e) {
                  log.error("{} caught interrupted exception", getName());
                  Thread.currentThread().interrupt();
               }
               spinWaiting.decrementAndGet();
               continue;
            }

            activeThreads.incrementAndGet(); // count threads

            beingFetched[threadNum] = fit.url;

            log.debug("[Fetcher #{}] {}  => activeThreads={}, spinWaiting={}, queueID={}", taskID, getName(),
                  activeThreads, spinWaiting, fit.queueID);

            log.debug("[Fetcher #{}] {} : Fetching {}", taskID, getName(), fit.url);

            Metadata metadata = null;           

            if (fit.t.contains("metadata")) {
               metadata = (Metadata) fit.t.getValueByField("metadata");
            }
            if (metadata == null) {
               metadata = new Metadata();
            }
            metadata.setValue("fetch.checkingDate", LocalDateTime.now().toString());

            // https://github.com/DigitalPebble/storm-crawler/issues/813
            metadata.remove("fetch.exception");

            String originalUrl = metadata.getFirstValue("originalUrl");

            String redirectCountStr = metadata.getFirstValue("fetch.redirectCount");

            int redirectCount = (redirectCountStr != null ? Integer.valueOf(redirectCountStr) : 0);

            boolean asap = false;

            try {
               
               String correctedUrlString = fit.url.replace(" ", "%20");
               URL url = new URL(correctedUrlString); //whitespace replacement

               
               Protocol protocol = protocolFactory.getProtocol(url);

               if (protocol == null)
                  throw new RuntimeException("No protocol implementation found for " + fit.url);

               BaseRobotRules rules = protocol.getRobotRules(url.toString());

               //checking for robots.txt
               if (!rules.isAllowed(correctedUrlString)) {
                  metadata.setValue("fetch.message", "Blocked by robots.txt");
                  // pass the info about denied by robots
                  metadata.setValue("fetch.category", Category.Blocked_By_Robots_txt.name());
                  collector.emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName, fit.t,
                        new Values(fit.url, metadata, Status.DISCOVERED));
                  // no need to wait next time as we won't request from
                  // that site
                  asap = true;
                  
                  continue;
               }
               
               //checking for crawl delays
               FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
               
               // a host specific crawl delay is set it dominates any other setting! 
               if(MetricsFetcherBolt.this.fetchQueues.crawlDelayByServer.containsKey(url.getHost())) {
                  
                  fiq.crawlDelay = MetricsFetcherBolt.this.fetchQueues.crawlDelayByServer.get(url.getHost());
               }               
               // if crawl delay from robots.txt >0 and not the same as the server.crawl.delay
               else if (rules.getCrawlDelay() > 0 && rules.getCrawlDelay() != fiq.crawlDelay) {
                  
                  // if crawl delay from robots.txt > max.crawl.delay and max.crawl.delay> 0
                  if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                     boolean force = false;
                     String msg = "skipping";
                     if (maxCrawlDelayForce) {
                        force = true;
                        msg = "using value of fetcher.max.crawl.delay instead";
                     }
                     log.debug("Crawl-Delay for {} too long ({}), {}", fit.url, rules.getCrawlDelay(), msg);
                     if (force) {
                        fiq.crawlDelay = maxCrawlDelay;
                     }
                     else {
                        // pass the info about crawl delay
                        metadata.setValue("fetch.message", "Crawl delay too long.");
                        metadata.setValue("fetch.category", Category.Blocked_By_Robots_txt.name());

                        collector.emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName, fit.t,
                              new Values(fit.url, metadata, Status.DISCOVERED));
                        // no need to wait next time as we won't request
                        // from that site
                        asap = true;
                        
                        continue;
                     }
                  }
                  else if (rules.getCrawlDelay() < fetchQueues.crawlDelay && crawlDelayForce) {
                     fiq.crawlDelay = fetchQueues.crawlDelay;
                     log.debug("Crawl delay for {} too short ({}), set to fetcher.server.delay", fit.url,
                           rules.getCrawlDelay());
                  }
                  else {
                     if(fiq.crawlDelay > 0 && rules.getCrawlDelay() > fiq.crawlDelay) {
                        
                        log.info("Crawl delay increased by robots.txt from {} to {} ms for URL '{}'", fiq.crawlDelay, rules.getCrawlDelay(), fit.url);
                     }
                     fiq.crawlDelay = rules.getCrawlDelay();
                     log.debug("Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}", fit.queueID,
                           fiq.crawlDelay, fit.url);
                  }
               }
               
               // checking for know login pages
               if (Configuration.loginPageUrls.contains(correctedUrlString)) {
                  // this next if check means that the harvested page was not the login page.
                  // if the login page is harvested as a url, then it should be handled normally

                  if (!fit.url.equals(metadata.getFirstValue("originalUrl"))) {
                     metadata.setValue("fetch.message", metadata.getFirstValue("originalUrl")
                           + " points to a login page, therefore has restricted access.");
                     metadata.setValue("fetch.category", Category.Restricted_Access.name());
                     
                     collector.emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName, fit.t,
                           new Values(fit.url, metadata, Status.DISCOVERED));
                     
                     continue;
                  }
               }
               

               long start = System.currentTimeMillis();
               long timeInQueues = start - fit.creationTime;

               // been in the queue far too long and already failed
               // by the timeout - let's not fetch it
               if (timeoutInQueues != -1 && timeInQueues > timeoutInQueues * 1000) {
                  log.debug("[Fetcher #{}] Waited in queue for too long - {}", taskID, fit.url);
                  // no need to wait next time as we won't request from
                  // that site
                  asap = true;
                  continue;
               }

               ProtocolResponse response = protocol.getProtocolOutput(correctedUrlString, metadata);               
               
               if (Configuration.redirectStatusCodes.contains(response.getStatusCode())) {

                  if (++redirectCount > HTTP_REDIRECT_LIMIT) {

                     metadata.setValue("fetch.message",
                           "Redirects exceeded " + HTTP_REDIRECT_LIMIT + " redirects for " + originalUrl);
                     metadata.setValue("fetch.category", Category.Undetermined.name());

                  }
                  else {
                     
                     String redirectUrl = convertRelativeToAbsolute(fit.url,
                           response.getMetadata().getFirstValue(HttpHeaders.LOCATION));
                     metadata.setValue("fetch.redirectCount", Integer.toString(redirectCount));

                     collector.emit(eu.clarin.linkchecker.config.Constants.RedirectStreamName, fit.t, new Values(redirectUrl, metadata));
                     continue;
                  }
               }
               
               // LPASpout sets HEAD request as default
               // if the head request is unsuccessful, the tuple is returned to the Partitioner 
               // with the information that we wont to replay it with a GET request
               else if ("true".equals(metadata.getFirstValue("http.method.head")) && !List.of(200, 304).contains(response.getStatusCode())) {                  
                  metadata.setValue("http.method.head", "false");
                  collector.emit(eu.clarin.linkchecker.config.Constants.RedirectStreamName, fit.t, new Values(fit.url, metadata));
                  continue;
               }               
               else if (Configuration.okStatusCodes.contains(response.getStatusCode())) {
                  metadata.setValue("fetch.message", Category.Ok.name());
                  metadata.setValue("fetch.category", Category.Ok.name());
               }
               else if (Configuration.undeterminedStatusCodes.contains(response.getStatusCode())) {
                  metadata.setValue("fetch.message", "Undetermined, Status code: " + response.getStatusCode());
                  metadata.setValue("fetch.category", Category.Undetermined.name());
               }
               else if (Configuration.restrictedAccessStatusCodes.contains(response.getStatusCode())) {
                  metadata.setValue("fetch.message", "Restricted access, Status code: " + response.getStatusCode());
                  metadata.setValue("fetch.category", Category.Restricted_Access.name());
               }
               else {
                  metadata.setValue("fetch.message", "Broken, Status code: " + response.getStatusCode());
                  metadata.setValue("fetch.category", Category.Broken.name());
               }

               long timeFetching = System.currentTimeMillis() - start;


               log.debug("[Fetcher #{}] Fetched {} with status {} in msec {}", taskID, fit.url, response.getStatusCode(),
                     timeFetching);

               // merges the original MD and the ones returned by the
               // protocol
               Metadata mergedMD = new Metadata();
               mergedMD.putAll(metadata);

               // add a prefix to avoid confusion, preserve protocol
               // metadata persisted or transferred from previous fetches
               mergedMD.putAll(response.getMetadata(), protocolMDprefix);

               mergedMD.setValue("fetch.statusCode", Integer.toString(response.getStatusCode()));

               mergedMD.setValue("fetch.byteLength", response.getMetadata().getFirstValue(HttpHeaders.CONTENT_LENGTH));
               
               mergedMD.setValue("fetch.contentType", response.getMetadata().getFirstValue(HttpHeaders.CONTENT_TYPE));

               mergedMD.setValue("fetch.duration", Long.toString(timeFetching));
               
               mergedMD.setValue("fetch.redirectCount", Integer.toString(redirectCount));

               collector.emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName, fit.t,
                     new Values(fit.url, mergedMD, Status.DISCOVERED));

            }
            catch (Exception exece) {
               // recheck with GET request if the failed check was a GET request
               if("true".equals(metadata.getFirstValue("http.method.head"))){
                  metadata.setValue("http.method.head", "false");
                  collector.emit(eu.clarin.linkchecker.config.Constants.RedirectStreamName, fit.t, new Values(fit.url, metadata));
                  continue;                  
               }
               
               String message = exece.getMessage();                

               // common exceptions for which we log only a short message
               if (exece.getCause() instanceof java.util.concurrent.TimeoutException
                     || (message != null && message.contains(" timed out"))) {
                  log.debug("Socket timeout fetching {}", fit.url);
                  message = "Socket timeout fetching";
               }
               else if (exece.getCause() instanceof java.net.UnknownHostException
                     || exece instanceof java.net.UnknownHostException) {
                  log.debug("Unknown host {}", fit.url);
                  message = "Unknown host";
               }
               else if(StringUtils.isBlank(message)){
                  
                  message = exece.getClass().getName();
               }

               if (metadata.size() == 0) {
                  metadata = new Metadata();
               }
               
               metadata.setValue("fetch.category", getCategoryFromException(exece, fit.url).name());

               metadata.setValue("fetch.message", message);

               // send to status stream
               collector.emit(Constants.StatusStreamName, fit.t, new Values(fit.url, metadata, Status.DISCOVERED));

            }
            finally {
               fetchQueues.finishFetchItem(fit, asap);
               activeThreads.decrementAndGet(); // count threads
               // ack it whatever happens
               collector.ack(fit.t);
               beingFetched[threadNum] = "";
            }
         }
      }
   }

   private void checkConfiguration(Config stormConf) {

      // ensure that a value has been set for the agent name and that that
      // agent name is the first value in the agents we advertise for robot
      // rules parsing
      String agentName = (String) stormConf.get("http.agent.name");
      if (agentName == null || agentName.trim().length() == 0) {
         String message = "Fetcher: No agents listed in 'http.agent.name'" + " property.";
         log.error(message);
         throw new IllegalArgumentException(message);
      }
      
      HTTP_REDIRECT_LIMIT = ConfUtils.getInt(stormConf, eu.clarin.linkchecker.config.Constants.HTTP_REDIRECT_LIMIT, 5);
   }

   @Override
   public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

      super.prepare(stormConf, context, collector);

      Config conf = new Config();
      conf.putAll(stormConf);

      checkConfiguration(conf);

      log.debug("[Fetcher #{}] : starting at {}", taskID, Instant.now());

      protocolFactory = ProtocolFactory.getInstance(conf);

      this.fetchQueues = new FetchItemQueues(conf);

      this.taskID = context.getThisTaskId();

      int threadCount = ConfUtils.getInt(conf, "fetcher.threads.number", 10);
      for (int i = 0; i < threadCount; i++) { // spawn threads
         new FetcherThread(conf, i).start();
      }

      // keep track of the URLs in fetching
      beingFetched = new String[threadCount];
      Arrays.fill(beingFetched, "");

      sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf, SITEMAP_DISCOVERY_PARAM_KEY, false);

      maxNumberURLsInQueues = ConfUtils.getInt(conf, "fetcher.max.urls.in.queues", -1);

      /*
       * If set to a valid path e.g. /tmp/fetcher-dump-{port} on a worker node, the
       * content of the queues will be dumped to the logs for debugging. The port
       * number needs to match the one used by the FetcherBolt instance.
       */
      String debugfiletriggerpattern = ConfUtils.getString(conf, "fetcherbolt.queue.debug.filepath");

      if (StringUtils.isNotBlank(debugfiletriggerpattern)) {
         debugfiletrigger = new File(
               debugfiletriggerpattern.replaceAll("\\{port\\}", Integer.toString(context.getThisWorkerPort())));
      }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream(
            com.digitalpebble.stormcrawler.Constants.StatusStreamName, 
            new Fields("url", "metadata", "status")
         );
      declarer.declareStream(
            eu.clarin.linkchecker.config.Constants.RedirectStreamName, 
            new Fields("url", "metadata")
         );
   }


   @Override
   public void cleanup() {
      protocolFactory.cleanup();
   }

   @Override
   public void execute(Tuple input) {

      if (TupleUtils.isTick(input)) {
         // detect whether there is a file indicating that we should
         // dump the content of the queues to the log
         if (debugfiletrigger != null && debugfiletrigger.exists()) {
            log.debug("Found trigger file {}", debugfiletrigger);
            logQueuesContent();
            debugfiletrigger.delete();
         }
         return;
      }

      if (this.maxNumberURLsInQueues != -1) {
         while (this.activeThreads.get() + this.fetchQueues.inQueues.get() >= maxNumberURLsInQueues) {
            try {
               Thread.sleep(500);
            }
            catch (InterruptedException e) {
               log.error("Interrupted exception caught in execute method");
               Thread.currentThread().interrupt();
            }
            log.debug("[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}", taskID, this.activeThreads.get(),
                  this.fetchQueues.queues.size(), this.fetchQueues.inQueues.get());
         }
      }

      final String urlString = input.getStringByField("url");
      if (StringUtils.isBlank(urlString)) {
         log.debug("[Fetcher #{}] Missing value for field url in tuple {}", taskID, input);
         // ignore silently
         collector.ack(input);
         return;
      }

      log.debug("Received in Fetcher {}", urlString);

      URL url;

      try {
         url = new URL(urlString);
      }
      catch (MalformedURLException e) {
         log.error("{} is a malformed URL", urlString);

         Metadata metadata = (Metadata) input.getValueByField("metadata");
         if (metadata == null) {
            metadata = new Metadata();
         }
         // Report to status stream and ack
         metadata.setValue(Constants.STATUS_ERROR_CAUSE, "malformed URL");
         
         metadata.setValue("fetch.category", getCategoryFromException(e, urlString).name());
         metadata.setValue("fetch.message", e.getClass().getName());
         metadata.setValue("fetch.checkingDate", LocalDateTime.now().toString());
         
         collector.emit(com.digitalpebble.stormcrawler.Constants.StatusStreamName, input,
               new Values(urlString, metadata, Status.ERROR));
         collector.ack(input);
         return;
      }

      boolean added = fetchQueues.addFetchItem(url, urlString, input);
      if (!added) {
         collector.fail(input);
      }
   }

   private void logQueuesContent() {
      StringBuilder sb = new StringBuilder();
      synchronized (fetchQueues.queues) {
         sb.append("\nNum queues : ").append(fetchQueues.queues.size());
         Iterator<Entry<String, FetchItemQueue>> iterator = fetchQueues.queues.entrySet().iterator();
         while (iterator.hasNext()) {
            Entry<String, FetchItemQueue> entry = iterator.next();
            sb.append("\nQueue ID : ").append(entry.getKey());
            FetchItemQueue fiq = entry.getValue();
            sb.append("\t size : ").append(fiq.getQueueSize());
            sb.append("\t in progress : ").append(fiq.getInProgressSize());
            Iterator<FetchItem> urlsIter = fiq.queue.iterator();
            while (urlsIter.hasNext()) {
               sb.append("\n\t").append(urlsIter.next().url);
            }
         }
         log.debug("Dumping queue content {}", sb.toString());

         StringBuilder sb2 = new StringBuilder("\n");
         // dump the list of URLs being fetched
         for (int i = 0; i < beingFetched.length; i++) {
            if (beingFetched[i].length() > 0) {
               sb2.append("\n\tThread #").append(i).append(": ").append(beingFetched[i]);
            }
         }
         log.debug("URLs being fetched {}", sb2.toString());
      }
   }

   private String convertRelativeToAbsolute(String url, String locationHeader)
         throws URISyntaxException, MalformedURLException {
      if (locationHeader == null) {
         throw new MalformedURLException("Location Header in a redirect shouldn't be null.");
      }
      
      if (locationHeader.startsWith(".")) {
         // remove query parameters
         url = url.split("\\?")[0];
         int lastIndex = url.lastIndexOf("/");
         return (url.substring(0, lastIndex) + locationHeader.substring(1));

      }
      else if (locationHeader.startsWith("/")) {
         URI uri = new URI(url);
         String scheme = uri.getScheme();
         String domain = uri.getHost();
         return (scheme + "://" + domain + locationHeader);
      }
      else {
         return locationHeader.matches("(http|ftp).+")?locationHeader: convertRelativeToAbsolute(url, "." + locationHeader);
      }
   }
   private Category getCategoryFromException(Exception e, String url) {
      if (e instanceof MalformedURLException) {
         return Category.Broken;
      }
      else if (e instanceof IllegalArgumentException) {
         return Category.Broken;
      }
      else if (e instanceof RedirectException) {
         return Category.Undetermined;
      }
      else if (e instanceof ConnectException) {
         return Category.Broken;
      }
      else if (e instanceof SocketTimeoutException) {
         return Category.Broken;
      }
      else if (e instanceof NoHttpResponseException) {
         return Category.Broken;
      }
      else if (e instanceof ConnectTimeoutException) {
         return Category.Broken;
      }
      else if (e instanceof UnknownHostException) {
         return Category.Broken;
      }
      else if (e instanceof SSLException) {
         return Category.Undetermined;
      }
      else if (e instanceof NoRouteToHostException) {
         return Category.Broken;
      }
      else if (e instanceof SocketException) {
         return Category.Undetermined;
      }
      else if (e instanceof ConnectionClosedException) {
         return Category.Broken;
      }
      else {
         log.debug("For the URL: \"" + url + "\" there was a yet undefined exception: " + e.getClass().toString()
               + " with the message: " + e.getMessage() + ". Please add this new exception into the code");
         return Category.Undetermined; // we dont know the exception, then we can't determine it: Undetermined.
      }
   } 
}
