# Link Checker
## Introduction
The Link checker is a [StormCrawler](https://github.com/DigitalPebble/storm-crawler) 
adaptation for URL checking. Instead of crawling, it checks the status of URLs and
persists them in a database (currently MariaDB/MySQL). 

**Important note**  
The Link Checker is not a stand-alone application but storm topology which is running inside a cluster. 
Only for testing we provide a class which runs as a stand-alone application, preferably in your IDE. But this 
should not be run in production. 

For more information on storm topologies, have a look at the documentation of the [apache storm](https://storm.apache.org/releases/2.6.0/Concepts.html) project, please. 

## Building and running the Link Checker topology
### Building the Link Checker topology
1. Clone this repository to your workspace
1. Go inside the Link Checker directory and build a jar by calling the Maven wrapper with the command  
`./mvnw clean install`

You may use your own Maven instead of the Maven wrapper for building the topology but the wrapper is the safe way,
since it is tested.
Therefore, if anything goes wrong at build time, make sure at first that you were using the Maven wrapper.

### Setting up a storm cluster
For remote cluster setup, have a look at the documentation of the [apache storm](https://storm.apache.org/releases/2.6.0/Setting-up-a-Storm-cluster.html) project, please.

### Deploying the Link Checker topology to the cluster
To deploy your Link Checker topology to the cluster, use the command  
`<storm directory>/bin/storm" jar <Link Checker directory>/target/linkchecker-<version>.jar org.apache.storm.flux.Flux -e -r -R linkchecker.flux`

For more information on the parameters, have a look at the [Flux](https://storm.apache.org/releases/2.6.0/flux.html) chapter 
of the apache storm documentation. 

## Testing in local mode in your IDE
As mentioned before the Link Checker project provides a class to test the Link Checker in your favorite IDE in
local mode without any necessity to set up a cluster.    
1. Clone this repository into an IDE workspace
1. Set environment the variables used in src/test/resources/linkchecker-test-conf.yaml in the IDEs application running configuration
1. Execute class eu.clarin.linkchecker.LinkcheckerTestApp (under src/test/java)
  
# Simple Explanation of the current implementation

Our SQL database has got these tables:
1. **url:** This is the table that linkchecker reads the URLs to check from. So this will be populated by another application (in our case curation-module or linkchecker-api).
1. **status:** This is the table that linkchecker saves the results into.
1. **history:** If a URL is checked more than once, the previous checking result is saved in the history table and the record in the status table is updated.   
1. **obsolete** A flat table which keeps the records still for a while after purging the from the other tables 
1. **providerGroup**
1. **context**: The table saves the context (the file or the upload) in which the link is found 
1. **url_context**: Joins url-table n-n to the context table, so that each URL might appear in different contexts. Moreover the table contains the last time when the link was ingested and and a boolean flag which indicates if the join is still active. Only URLs which have at least one active join are considered to be checked!
1. **client** The table is basically used to identify the link source

The creation script is available in the [linkchecker-persictence API](https://github.com/clarin-eric/linkchecker-persistence/blob/main/src/main/resources/schema.sql) project. 

*linkchecker.flux* defines the components(spouts, bolts and streams) if our topology and loads the configuration file *linkchecker-conf.yaml*.
1. `eu.clarin.linkchecker.spout.LPASpout` uses the [linkchecker-persistence API](https://github.com/clarin-eric/linkchecker-persistence) to fill up a buffer with URLs to check.
1. `org.apache.stormcrawler.bolt.URLPartitionerBolt` partitions the URLs by a configured criteria
1. `eu.clarin.linkchecker.bolt.MetricsFetcherBolt` fetches the urls. It sends redirects back to URLPartitionerBolt and sends the rest onwards down the stream to StatusUpdaterBolt. Modification of  `org.apache.stormcrawler.bolt.FetcherBolt`
1. `eu.clarin.linkchecker.bolt.StatusUpdaterBolt` persists the results in the status table of the database via the [linkchecker-persistence API](https://github.com/clarin-eric/linkchecker-persistence).
1. `eu.clarin.linkchecker.bolt.SimpleStackBolt` persists the latest checking results into a Java Object file for use in curation-web
