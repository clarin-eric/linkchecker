/**
 * The class takes org.apache.stormcrawler.protocol.ProtocolFactory as a sample, adds the capacity of host specific http timeouts and simplifies the code 
 * 
 * @author Wolfgang Walter SAUER (wowasa) &lt;clarin@wowasa.com&gt;
 *
 */
package eu.clarin.linkchecker.extension;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;

import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolFactory;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;

/**
 *
 */
public class HostProtocolFactory {
   
   private final HashMap<String, HashMap<String, Protocol>> cache;
   
   private final Config conf;
   
   private HostProtocolFactory(Config conf) {
      
      this.cache = new HashMap<String, HashMap<String, Protocol>>();
      this.conf = conf;
      
   }

   private static volatile HostProtocolFactory single_instance = null;

   public static HostProtocolFactory getInstance(Config conf) {

       // https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java

       HostProtocolFactory temp = single_instance;

       if (temp == null) {
           // Synchronize on class-level.
           synchronized (ProtocolFactory.class) {
               temp = single_instance;
               if (temp == null) {
                   temp = new HostProtocolFactory(conf);
                   single_instance = temp;
               }
           }
       }

       return single_instance;
   }

   private Protocol newProtocol(String protocol, Config conf) {

           String paramName = protocol + ".protocol.implementation";
           String protocolimplementation = ConfUtils.getString(conf, paramName);
           
           if (StringUtils.isBlank(protocolimplementation)) {
              
              if(protocol.matches("https?")) {
                 
                 protocolimplementation = "org.apache.stormcrawler.protocol.httpclient.HttpProtocol";
              }
              else {
                 
                 throw new RuntimeException("the '" + protocol + "'-protocol is not supported by the HostProtocolFactory");
              }
           }
           
           Protocol protocolInstance = InitialisationUtil.initializeFromQualifiedName(protocolimplementation, Protocol.class);
           protocolInstance.configure(conf);
           
           return protocolInstance;
   }
   
   private synchronized Protocol getProtocol(String protocol, String host, Config conf) {
      
      return cache
               .computeIfAbsent(host, k -> new HashMap<String, Protocol>())
               .computeIfAbsent(protocol, k -> newProtocol(protocol, conf));      
   }

   public synchronized void cleanup() {
       
      cache.forEach((host, map) -> map.forEach((protocol, protocolObject) -> protocolObject.cleanup()));         
   }

   /** Returns an instance of the protocol to use for a given URL */
   public synchronized Protocol getProtocol(URL url) {
      
      if(this.conf.get("http.timeout.individual") instanceof Map && ((Map<?,?>) this.conf.get("http.timeout.individual")).containsKey(url.getHost())) {
         
         Config conf = (Config) this.conf.clone();
         conf.put("http.timeout", ((Map<?,?>) this.conf.get("http.timeout.individual")).get(url.getHost()));
         
         return getProtocol(url.getProtocol(), url.getHost(), conf);        
      }
      
      return getProtocol(url.getProtocol(), url.getHost(), this.conf);
   }

   /**
    * Returns instance(s) of the implementation for the protocol passed as argument.
    *
    * @param protocol representation of the protocol e.g. http
    */
   public synchronized Protocol getProtocol(String protocol) {
       // get the protocol
       return getProtocol("default", protocol, this.conf);
   }
}
