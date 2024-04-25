# version 3.4.0
- logging number of unchecked links (issue #84)
- dependency upgrade for linkchecker-persistence

# version 3.3.0
- upgrading dependencies to Storm 2.6.1 and Storm Crawler 2.11  
  => requirement to change user agent string (issues #78, #79, #80)
- removing acknowledgement from MetricsFetcherBolt (issue #81)

# version 3.2.0
- adding functionality for host specific http.timeout
- upgrading dependencies to Storm 2.5.0 and Storm Crawler 2.10
- moving flux and conf files to maven resource directory ([requires additional command line option -R](https://storm.apache.org/releases/2.6.0/flux.html))

# version 3.1.3
- set http.timeout via environment variable

# version 3.1.2
- activating http.agent.email and http.agent.descripton again

# version 3.1.1
- allow to configure a host specific crawl delay

# version 3.1.0
- redesign of LPASpout: takes now a native SQL query as constructor parameter from crawler.flux file
- modification of crawler.flux to have two instances of LPASpout for handle- and non handle URLs
- logging crawl delays from robots.txt in MetricsFetcherBolt which are exceeding the configured fetcher.server.delay
- writing latest checking results in a Object file to be used by curation-web
- removing redundant settings from configuration file
- bug fixes   

# version 3.0.4
- configuring okhttp.HttpProtocol (issue #52)
- shifting status logging from MetricsFetcherBolt to LPASpout (issue #59)
- bug fix for issue #58

# version 3.0.3
- upgrade of storm crawler dependeny (issue #53)
- bug fix for issue #57

# version 3.0.2
- adding missing PartitionerBolt again (bug fix!)

# version 3.0.1
- bugfix in class MetricsFetcherBolt to prevent null message
- bugfix in dependency linkchecker-persistence

# version 3.0.0
- replacement of the persistence layer: the [resource availability status API (RASA)](https://github.com/clarin-eric/resource-availability-status-api) 
is replaced by [curation-persistence](https://github.com/clarin-eric/curation-persistence)
- inclusion of maven wrapper
- deletion of template_crawler-conf.yaml and use of environment variables in crawler-conf.yaml

# version 2.4.0
- upgrade to storm 2.4.0 and storm crawler 2.4
 
# version 2.3.0
- improved algorithm for next links to check
- trimming URLs (done by RASA) and escaping white spaces in URLs used for request

# version 2.2.0
- checking time taken now at the start of the checking instead of the end
- accurate control flow instead of using exceptions
- fixing bug of doubled log messages in RASASpout
- reducing log messages of MetricsFetcherBolt to one message on info-level per 100 checks  

# version 2.1.1
- increase size of content-length/byteSize from int to long in Java and from int to bigint in mysql/maria db
- increase size of message from varchar(256) to varchar(1024) with a truncation in Java, if the message is longer that 1024 characters

# version 2.1.0
- Java version upgrade to Java 11
- dependency upgerade to storm 2.2
- dependency upgrade to storm crawler 2.1
- using resource-availability-status-api for db access in RASASpout and StatusUpdaterBolt instead of direct db access 
- storing originalUrl in metadata which allows the use of unmodified storm crawler Abstract super-classes
- using storm crawler's SimpleURLBuffer instead of LinkedList to handle tuples