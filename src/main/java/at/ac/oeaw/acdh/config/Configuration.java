package at.ac.oeaw.acdh.config;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * This class is for global variables. Since they are variables, I can't put them under constants.
 */
public class Configuration {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(Configuration.class);

    public static AtomicInteger sec = new AtomicInteger(0);
    public static Timestamp latestFetchDate;

    public static ReentrantLock loginPagesLock = new ReentrantLock();
    public static List<String> loginPageUrls = new ArrayList<>();

    private static String loginListURL = "https://raw.githubusercontent.com/clarin-eric/login-pages/master/list.txt";
    private static String loginListContent;

    static {
        fillLoginPageUrls();

        //update the list once a day at 1 am
        Runnable loginPageUrlUpdater = Configuration::fillLoginPageUrls;

        long oneAM = LocalDateTime.now().until(LocalDate.now().plusDays(1).atTime(1, 0), ChronoUnit.MINUTES);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(loginPageUrlUpdater, oneAM, TimeUnit.DAYS.toMinutes(1), MINUTES);
    }

    private static void fillLoginPageUrls() {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(loginListURL);
        try (CloseableHttpResponse response = httpclient.execute(httpGet);) {

            String newContent = EntityUtils.toString(response.getEntity(), "UTF-8");

            //do nothing if the new list is the same as old, meaning newContent equals loginListContent
            if (!newContent.equals(loginListContent)) {
                loginListContent = newContent;

                loginPagesLock.lock();
                //fill loginPageUrls

                loginPageUrls.clear();
                try (BufferedReader reader = new BufferedReader(new StringReader(loginListContent))) {
                    String line = reader.readLine();
                    while (line != null) {
                        loginPageUrls.add(line);
                        line = reader.readLine();
                    }
                } catch (IOException e) {
                    // quit
                }

                loginPagesLock.unlock();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info("login page urls: "+loginPageUrls);


    }

}


