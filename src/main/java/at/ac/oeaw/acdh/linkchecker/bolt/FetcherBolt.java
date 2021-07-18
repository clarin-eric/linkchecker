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
 * limitations under the License. NOTICE: This code was modified in ACDH - Austrian Academy of
 * Sciences, based on Stormcrawler source code.
 */
package at.ac.oeaw.acdh.linkchecker.bolt;

import at.ac.oeaw.acdh.linkchecker.config.Configuration;
import at.ac.oeaw.acdh.linkchecker.config.Constants;
import at.ac.oeaw.acdh.linkchecker.exception.CategoryException;
import at.ac.oeaw.acdh.linkchecker.exception.CrawlDelayTooLongException;
import at.ac.oeaw.acdh.linkchecker.exception.DeniedByRobotsException;
import at.ac.oeaw.acdh.linkchecker.exception.LoginPageException;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.protocol.*;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.PerSecondReducer;
import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.robots.BaseRobotRules;
import eu.clarin.cmdi.rasa.helpers.statusCodeMapper.Category;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.RedirectException;
import org.apache.storm.Config;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */
@SuppressWarnings("serial")
public class FetcherBolt extends StatusEmitterBolt {

   private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

   private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

   private final AtomicInteger activeThreads = new AtomicInteger(0);
   private final AtomicInteger spinWaiting = new AtomicInteger(0);

   private FetchItemQueues fetchQueues;

   private MultiCountMetric eventCounter;
   private MultiReducedMetric averagedMetrics;

   private ProtocolFactory protocolFactory;

   private int HTTP_REDIRECT_LIMIT;

   private int taskID = -1;

   boolean sitemapsAutoDiscovery = false;

   private MultiReducedMetric perSecMetrics;

   private File debugfiletrigger;

   /** blocks the processing of new URLs if this value is reached */
   private int maxNumberURLsInQueues = -1;

   private String[] beingFetched;

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
            } catch (final UnknownHostException e) {
               LOG.warn("Unable to resolve IP for {}, using hostname as key.", u.getHost());
               key = u.getHost();
            }
         } else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
            key = PaidLevelDomain.getPLD(u.getHost());
            if (key == null) {
               LOG.warn("Unknown domain for url: {}, using hostname as key", url);
               key = u.getHost();
            }
         } else {
            key = u.getHost();
         }

         if (key == null) {
            LOG.warn("Unknown host for url: {}, using URL string as key", url);
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
      Map<String, FetchItemQueue> queues = Collections.synchronizedMap(new LinkedHashMap<String, FetchItemQueue>());

      AtomicInteger inQueues = new AtomicInteger(0);

      final int defaultMaxThread;
      final long crawlDelay;
      final long minCrawlDelay;

      int maxQueueSize;

      final Config conf;

      public static final String QUEUE_MODE_HOST = "byHost";
      public static final String QUEUE_MODE_DOMAIN = "byDomain";
      public static final String QUEUE_MODE_IP = "byIP";

      String queueMode;

      public FetchItemQueues(Config conf) {
         this.conf = conf;
         this.defaultMaxThread = ConfUtils.getInt(conf, "fetcher.threads.per.queue", 1);
         queueMode = ConfUtils.getString(conf, "fetcher.queue.mode", QUEUE_MODE_HOST);
         // check that the mode is known
         if (!queueMode.equals(QUEUE_MODE_IP) && !queueMode.equals(QUEUE_MODE_DOMAIN)
               && !queueMode.equals(QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
            queueMode = QUEUE_MODE_HOST;
         }
         LOG.info("Using queue mode : {}", queueMode);

         this.crawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.delay", 1.0f) * 1000);
         this.minCrawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.min.delay", 0.0f) * 1000);
         this.maxQueueSize = ConfUtils.getInt(conf, "fetcher.max.queue.size", -1);
         if (this.maxQueueSize == -1) {
            this.maxQueueSize = Integer.MAX_VALUE;
         }
      }

      /** @return true if the URL has been added, false otherwise */
      public synchronized boolean addFetchItem(URL u, String url, Tuple input) {
         FetchItem it = FetchItem.create(u, url, input, queueMode);
         FetchItemQueue fiq = getFetchItemQueue(it.queueID);
         boolean added = fiq.addFetchItem(it);
         if (added) {
            inQueues.incrementAndGet();
         }
         return added;
      }

      public synchronized void finishFetchItem(FetchItem it, boolean asap) {
         FetchItemQueue fiq = queues.get(it.queueID);
         if (fiq == null) {
            LOG.warn("Attempting to finish item from unknown queue: {}", it.queueID);
            return;
         }
         fiq.finishFetchItem(it, asap);
      }

      public synchronized FetchItemQueue getFetchItemQueue(String id) {
         FetchItemQueue fiq = queues.get(id);
         if (fiq == null) {
            // custom maxThread value?
            final int customThreadVal = ConfUtils.getInt(conf, "fetcher.maxThreads." + id, defaultMaxThread);
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
            } else if (fiq == start) {
               return null;
            }

            FetchItem fit = fiq.getFetchItem();

            if (fit != null) {
               inQueues.decrementAndGet();
               return fit;
            }

         } while (!queues.isEmpty());

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
      private int threadNum;

      public FetcherThread(Config conf, int num) {
         this.setDaemon(true); // don't hang JVM on exit
         this.setName("FetcherThread #" + num); // use an informative name

         this.maxCrawlDelay = ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30) * 1000;
         this.maxCrawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false);
         this.crawlDelayForce = ConfUtils.getBoolean(conf, "fetcher.server.delay.force", false);
         this.threadNum = num;
      }

      @Override
      public void run() {
         try {
            Thread.sleep(5000);
         } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
         }
         LOG.debug("Configuration.isActive(): {}", Configuration.isActive());
         while (Configuration.isActive()) {
            FetchItem fit = fetchQueues.getFetchItem();
            if (fit == null) {
               LOG.debug("{} spin-waiting ...", getName());
               // spin-wait.
               spinWaiting.incrementAndGet();
               try {
                  Thread.sleep(100);
               } catch (InterruptedException e) {
                  LOG.error("{} caught interrupted exception", getName());
                  Thread.currentThread().interrupt();
               }
               spinWaiting.decrementAndGet();
               continue;
            }

            activeThreads.incrementAndGet(); // count threads

            beingFetched[threadNum] = fit.url;

            LOG.debug("[Fetcher #{}] {}  => activeThreads={}, spinWaiting={}, queueID={}", taskID, getName(),
                  activeThreads, spinWaiting, fit.queueID);

            LOG.debug("[Fetcher #{}] {} : Fetching {}", taskID, getName(), fit.url);

            Metadata metadata = new Metadata();

            if (fit.t.contains("metadata") && fit.t.getValueByField("metadata") != null) {
               metadata.putAll((Metadata) fit.t.getValueByField("metadata"));
            }

            boolean asap = false;
            String message;
            String category;

            String originalUrl = fit.t.getStringByField("originalUrl");

            Integer redirectCount = fit.t.getIntegerByField("redirectCount");

            Long urlId = fit.t.getLongByField("urlId");

            String url = fit.url;

            try {
               int statusCode = 0;
               ProtocolResponse response = null;

               long start = System.currentTimeMillis();
               long timeInQueues = start - fit.creationTime;

               if (redirectCount == null) {
                  redirectCount = 0;
               }

               URL u = new URL(url); // here it checks for malformed url and throws an exception, which is
               // handled below

               Protocol protocol = protocolFactory.getProtocol(u);

               if (protocol == null) {
                  throw new RuntimeException("No protocol implementation found for " + url);
               }
               BaseRobotRules rules = protocol.getRobotRules(url);
               if (rules instanceof RobotRules && ((RobotRules) rules).getContentLengthFetched().length == 0) {
                  eventCounter.scope("robots.fromCache").incrBy(1);
               } else {
                  eventCounter.scope("robots.fetched").incrBy(1);
               }

               if (!rules.isAllowed(url)) {
                  LOG.info("Denied by robots.txt: {}", url);
                  // pass the info about denied by robots
                  metadata.setValue(Constants.STATUS_ERROR_CAUSE, "robots.txt");

                  // no need to wait next time as we won't request from
                  // that site
                  asap = true;
                  throw new DeniedByRobotsException("Denied by robots.txt: " + url);
               }

               FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
               if (rules.getCrawlDelay() > 0 && rules.getCrawlDelay() != fiq.crawlDelay) {
                  if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                     boolean force = false;
                     String msg = "skipping";
                     if (maxCrawlDelayForce) {
                        force = true;
                        msg = "using value of fetcher.max.crawl.delay instead";
                     }
                     LOG.info("Crawl-Delay for {} too long ({}), {}", url, rules.getCrawlDelay(), msg);
                     if (force) {
                        fiq.crawlDelay = maxCrawlDelay;
                     } else {
                        // pass the info about crawl delay
                        metadata.setValue(Constants.STATUS_ERROR_CAUSE, "crawl_delay");

                        // no need to wait next time as we won't request
                        // from that site
                        asap = true;
                        throw new CrawlDelayTooLongException("Crawl delay too long.");
                     }
                  } else if (rules.getCrawlDelay() < fetchQueues.crawlDelay && crawlDelayForce) {
                     fiq.crawlDelay = fetchQueues.crawlDelay;
                     LOG.info("Crawl delay for {} too short ({}), set to fetcher.server.delay", url,
                           rules.getCrawlDelay());
                  } else {
                     fiq.crawlDelay = rules.getCrawlDelay();
                     LOG.info("Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}", fit.queueID,
                           fiq.crawlDelay, url);
                  }
               }

               while (Configuration.loginPagesLock.isLocked()) {
                  // do nothing, wait until it is unlocked to read
               }
               if (Configuration.loginPageUrls.contains(url)) {
                  // this next if check means that the harvested page was not the login page.
                  // if the login page is harvested as a url, then it should be handled normally
                  if (!url.equals(originalUrl)) {
                     throw new LoginPageException(
                           originalUrl + " points to a login page, therefore has restricted access.");
                  }
               }

               Long timestamp = System.currentTimeMillis();

               // first try HEAD
               metadata.setValue("http.method.head", "true");
               response = protocol.getProtocolOutput(url, metadata);
               statusCode = response.getStatusCode();

               // if its unsuccessful, try GET
               // GET is the only reliable method because some servers implement HEAD wrong
               // https://stackoverflow.com/questions/7351249/reliability-of-using-head-request-to-check-web-page-status
               if (!Configuration.redirectStatusCodes.contains(statusCode) && statusCode != 200 && statusCode != 304) {
                  metadata.setValue("http.method.head", "false");
                  response = protocol.getProtocolOutput(url, metadata);
                  statusCode = response.getStatusCode();
               }

               boolean redirect = false;
               String redirectUrl = null;
               if (Configuration.redirectStatusCodes.contains(statusCode)) {
                  redirect = true;
                  redirectCount++;
                  if (redirectCount >= HTTP_REDIRECT_LIMIT) {
                     throw new RedirectException(
                           "Redirects exceeded " + HTTP_REDIRECT_LIMIT + " redirects for " + originalUrl);
                  }
                  redirectUrl = convertRelativeToAbsolute(url,
                        response.getMetadata().getFirstValue(HttpHeaders.LOCATION));
               }

               // this doesn't take redirects into account, but i dont think thats a problem
               long timeFetching = System.currentTimeMillis() - start;

               Integer byteLength;
               try {
                  byteLength = Integer.parseInt(response.getMetadata().getFirstValue(HttpHeaders.CONTENT_LENGTH));
               } catch (NumberFormatException e) {
                  byteLength = null;
               }

               averagedMetrics.scope("fetch_time").update(timeFetching);
               averagedMetrics.scope("time_in_queues").update(timeInQueues);
               averagedMetrics.scope("bytes_fetched").update(byteLength == null ? 0 : byteLength);
               perSecMetrics.scope("bytes_fetched_perSec").update(byteLength == null ? 0 : byteLength);
               perSecMetrics.scope("fetched_perSec").update(1);
               eventCounter.scope("fetched").incrBy(1);
               eventCounter.scope("bytes_fetched").incrBy(byteLength == null ? 0 : byteLength);

               if (Configuration.okStatusCodes.contains(statusCode)) {
                  message = Category.Ok.name();
                  category = message;
               } 
               else if (Configuration.undeterminedStatusCodes.contains(statusCode)) {
                  message = "Undetermined, Status code: " + statusCode;
                  category = Category.Undetermined.name();
               } 
               else {
                  message = "Broken, Status code: " + statusCode;
                  category = Category.Broken.name();
               }

               LOG.info("[Fetcher #{}] Fetched {} with status {} in msec {}", taskID, url, response.getStatusCode(),
                     timeFetching);

               // merges the original MD and the ones returned by the
               // protocol
               Metadata mergedMD = new Metadata();
               mergedMD.putAll(metadata);

               mergedMD.putAll(response.getMetadata());

               mergedMD.setValue("fetch.statusCode", Integer.toString(response.getStatusCode()));

               mergedMD.setValue("fetch.byteLength", byteLength == null ? null : Integer.toString(byteLength));

               mergedMD.setValue("fetch.loadingTime", Long.toString(timeFetching));

               mergedMD.setValue("fetch.timeInQueues", Long.toString(timeInQueues));

               mergedMD.setValue("fetch.redirectCount", Integer.toString(redirectCount));

               mergedMD.setValue("fetch.message", message);

               mergedMD.setValue("fetch.category", category);

               mergedMD.setValue("fetch.timestamp", timestamp.toString());

               // if redirect, go back to partitioner bolt, if not pass it to status updater
               // bolt
               String streamName;
               if (redirect) {
                  streamName = Constants.RedirectStreamName;
                  url = redirectUrl;
               } else {
                  streamName = Constants.StatusStreamName;
               }

               final Values tupleToSend = new Values(originalUrl, url, mergedMD, urlId);

               collector.emit(streamName, fit.t, tupleToSend);

            } catch (Exception e) {
               if (metadata.size() == 0) {
                  metadata = new Metadata();
               }

               metadata.setValue("fetch.statusCode", null);

               metadata.setValue("fetch.byteLength", null);

               metadata.setValue("fetch.content-type", null);

               metadata.setValue("fetch.category", CategoryException.getCategoryFromException(e, url).name());

               metadata.setValue("fetch.message", e.getMessage());

               final Values tupleToSend = new Values(originalUrl, url, metadata, urlId);

               collector.emit(Constants.StatusStreamName, fit.t, tupleToSend);

               eventCounter.scope("exception").incrBy(1);
            } finally {
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
         LOG.error(message);
         throw new IllegalArgumentException(message);
      }

      HTTP_REDIRECT_LIMIT = ConfUtils.getInt(stormConf, Constants.HTTP_REDIRECT_LIMIT, 5);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

      super.prepare(stormConf, context, collector);

      Config conf = new Config();
      conf.putAll(stormConf);

      checkConfiguration(conf);

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
      long start = System.currentTimeMillis();
      LOG.info("[Fetcher #{}] : starting at {}", taskID, sdf.format(start));

      int metricsTimeBucketSecs = ConfUtils.getInt(conf, "fetcher.metrics.time.bucket.secs", 10);

      // Register a "MultiCountMetric" to count different events in this bolt
      // Storm will emit the counts every n seconds to a special bolt via a
      // system stream
      // The data can be accessed by registering a "MetricConsumer" in the
      // topology
      this.eventCounter = context.registerMetric("fetcher_counter", new MultiCountMetric(), metricsTimeBucketSecs);

      // create gauges
      context.registerMetric("activethreads", () -> {
         return activeThreads.get();
      }, metricsTimeBucketSecs);

      context.registerMetric("in_queues", () -> {
         return fetchQueues.inQueues.get();
      }, metricsTimeBucketSecs);

      context.registerMetric("num_queues", () -> {
         return fetchQueues.queues.size();
      }, metricsTimeBucketSecs);

      this.averagedMetrics = context.registerMetric("fetcher_average_perdoc", new MultiReducedMetric(new MeanReducer()),
            metricsTimeBucketSecs);

      this.perSecMetrics = context.registerMetric("fetcher_average_persec",
            new MultiReducedMetric(new PerSecondReducer()), metricsTimeBucketSecs);

      protocolFactory = new ProtocolFactory(conf);

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

      /**
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
      super.declareOutputFields(declarer);
      declarer.declare(new Fields("originalUrl", "url", "metadata", "urlId"));
   }

   @Override
   public void cleanup() {
      protocolFactory.cleanup();
   }

  @Override
  public void execute(Tuple input) {
    LOG.debug("tuple: {}", input);  
    if (this.maxNumberURLsInQueues != -1) {
      while (this.activeThreads.get() + this.fetchQueues.inQueues.get() >= maxNumberURLsInQueues) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOG.error("Interrupted exception caught in execute method");
          Thread.currentThread().interrupt();
        }
        LOG.debug(
            "[Fetcher #{}] Threads : {}\tqueues : {}\tin_queues : {}",
            taskID,
            this.activeThreads.get(),
            this.fetchQueues.queues.size(),
            this.fetchQueues.inQueues.get());
      }
    }

    // detect whether there is a file indicating that we should
    // dump the content of the queues to the log
    if (debugfiletrigger != null && debugfiletrigger.exists()) {
      LOG.info("Found trigger file {}", debugfiletrigger);
      logQueuesContent();
      debugfiletrigger.delete();
    }

    Long urlId = input.getLongByField("urlId");
    String originalUrl = input.getStringByField("originalUrl");
    final String url = input.getStringByField("url");

    if (StringUtils.isBlank(url)) {
      LOG.info("[Fetcher #{}] Missing value for field url in tuple {}", taskID, input);
      // ignore silently
      collector.ack(input);
      return;
    }

    URL u;

    try {
      u = new URL(url);
    } catch (MalformedURLException e) {

      Metadata metadata = (Metadata) input.getValueByField("metadata");
      if (metadata == null) {
        metadata = new Metadata();
      }

      metadata.setValue(
          "fetch.category", CategoryException.getCategoryFromException(e, url).name());

      metadata.setValue("fetch.message", e.getMessage());

      final Values tupleToSend = new Values(originalUrl, url, metadata, urlId);

      collector.emit(Constants.StatusStreamName, input, tupleToSend);

      collector.ack(input);
      return;
    }

    boolean added = fetchQueues.addFetchItem(u, url, input);
    if (!added) {
      collector.fail(input);
    }
  }

   private String convertRelativeToAbsolute(String url, String locationHeader)
         throws URISyntaxException, MalformedURLException {
      if (locationHeader == null) {
         throw new MalformedURLException("Location Header in a redirect shouldn't be null.");
      }
      String result;
      if (locationHeader.startsWith(".")) {
         // remove query parameters
         url = url.split("\\?")[0];
         int lastIndex = url.lastIndexOf("/");
         result = url.substring(0, lastIndex) + locationHeader.substring(1);

      } else if (locationHeader.startsWith("/")) {
         URI uri = new URI(url);
         String scheme = uri.getScheme();
         String domain = uri.getHost();
         result = scheme + "://" + domain + locationHeader;
      } else {
         return locationHeader;
      }
      return result;
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
         LOG.info("Dumping queue content {}", sb.toString());

         StringBuilder sb2 = new StringBuilder("\n");
         // dump the list of URLs being fetched
         for (int i = 0; i < beingFetched.length; i++) {
            if (beingFetched[i].length() > 0) {
               sb2.append("\n\tThread #").append(i).append(": ").append(beingFetched[i]);
            }
         }
         LOG.info("URLs being fetched {}", sb2.toString());
      }
   }
}
