package at.ac.oeaw.acdh.config;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is for global variables. Since they are variables, I can't put them under constants.
 */
public class Configuration {

    public static AtomicInteger sec = new AtomicInteger(0);
    public static Timestamp latestFetchDate;
}
