package eu.clarin.linkchecker.config;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

import com.digitalpebble.stormcrawler.util.ConfUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * This class is for global variables. Since they are variables, I can't put
 * them under constants.
 */
public class Configuration {

   private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Configuration.class);

   public static AtomicInteger sec = new AtomicInteger(0);
   public static Timestamp latestFetchDate;

   public static ReentrantLock loginPagesLock = new ReentrantLock();
   public static List<String> loginPageUrls = new ArrayList<>();

   // private static String loginListURL =
   // "https://raw.githubusercontent.com/clarin-eric/login-pages/master/list.txt";
   private static String loginListContent;

   private static boolean isInitialized = false;
   private static boolean isActive = false;

   public static List<Integer> okStatusCodes;

   public static List<Integer> redirectStatusCodes;

   // this determines what status codes will not be considered broken links. urls
   // with these codes will also not factor into the url-scores
   public static List<Integer> undeterminedStatusCodes;

   public static List<Integer> restrictedAccessStatusCodes;
   
   public static final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();

//    @SuppressWarnings("unchecked")
   @SuppressWarnings("unchecked")
   public static synchronized void init(Map<String, Object> conf) {
      if (!isInitialized) {
         fillLoginPageUrls(ConfUtils.getString(conf, Constants.LOGIN_LIST_URL));

         // update the list once a day at 1 am
         Runnable loginPageUrlUpdater = () -> {
            Configuration.fillLoginPageUrls(ConfUtils.getString(conf, Constants.LOGIN_LIST_URL));
         };

         long oneAM = LocalDateTime.now().until(LocalDate.now().plusDays(1).atTime(1, 0), ChronoUnit.MINUTES);
         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
         scheduler.scheduleAtFixedRate(loginPageUrlUpdater, oneAM, TimeUnit.DAYS.toMinutes(1), MINUTES);

         okStatusCodes = getIntegerList(Constants.OK_STATUS_CODES, conf);
         redirectStatusCodes = getIntegerList(Constants.REDIRECT_STATUS_CODES, conf);
         undeterminedStatusCodes = getIntegerList(Constants.UNDETERMINED_STATUS_CODES, conf);
         restrictedAccessStatusCodes = getIntegerList(Constants.RESTRICTED_ACCESS_STATUS_CODES, conf);
         
         ConfigurableEnvironment environment = new StandardEnvironment();
         MutablePropertySources propertySources = environment.getPropertySources();
         
         propertySources.addFirst(new MapPropertySource("MY_MAP", (Map<String, Object>) conf.get("SPRING")));
         
         ctx.setEnvironment(environment);
         ctx.register(ApplicationConfig.class);
         
         ctx.refresh();
         
         isInitialized = true;        
      }
   }

   public static synchronized void setActive(Map<String, Object> conf, boolean active) {
      if (active) {

         isActive = true;
      }
      else if (isActive) {

         isActive = false;
      }
   }

   public static boolean isActive() {
      return isActive;
   }

   private static void fillLoginPageUrls(String loginListURL) {

      CloseableHttpClient httpclient = HttpClients.createDefault();
      HttpGet httpGet = new HttpGet(loginListURL);
      try (CloseableHttpResponse response = httpclient.execute(httpGet);) {

         String newContent = EntityUtils.toString(response.getEntity(), "UTF-8");

         // do nothing if the new list is the same as old, meaning newContent equals
         // loginListContent
         if (!newContent.equals(loginListContent)) {
            loginListContent = newContent;

            loginPagesLock.lock();
            // fill loginPageUrls

            loginPageUrls.clear();
            try (BufferedReader reader = new BufferedReader(new StringReader(loginListContent))) {
               String line = reader.readLine();
               while (line != null) {
                  loginPageUrls.add(line);
                  line = reader.readLine();
               }
            }
            catch (IOException e) {
               // quit
            }

            loginPagesLock.unlock();
         }
      }
      catch (IOException e) {
         e.printStackTrace();
      }

      LOG.info("login page urls: " + loginPageUrls);

   }

   private static List<Integer> getIntegerList(String key, Map<String, Object> conf) {
      List<Integer> list = new ArrayList<Integer>();
      Object ret = conf.get(key);

      if (ret != null && JSONArray.class.isInstance(ret)) {
         JSONArray array = JSONArray.class.cast(ret);

         for (Object obj : array) {
            if (Number.class.isInstance(obj)) {
               list.add(Number.class.cast(obj).intValue());
            }
         }
      }

      return list;
   }
}
