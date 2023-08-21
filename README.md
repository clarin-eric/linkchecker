# Link Checker
The Link checker is a [StormCrawler](https://github.com/DigitalPebble/storm-crawler) 
adaptation for URL checking. Instead of crawling, it checks the status of URLs and
persists them in a database (currently MariaDB/MySQL). 
**Important** for understanding is the fact, that the linkchecker is not a stand alone application 
but storm topology which is running inside a cluster. For more information on storm topologies, 
have a look at the documentation of the [apache storm](https://storm.apache.org/releases/2.4.0/Concepts.html) project, please.   

# How to setup and run

## In your IDE
1. Clone this repository into an IDE workspace
2. Create either a file crawler-test.flux or change the name in class at.oeaw.acdh.linkchecker.LinkcheckerTest, line 13 to point to a valid flux file
3. Adapt the settings in crawler.flux and crawler-conf.yaml (or whatever you call these files in your test-environment) as described in the cluster setup.
4. Execute class at.oeaw.acdh.linkchecker.LinkcheckerTest

## In a local cluster
1. Before you can run linkchecker, you need to install [Apache Storm](https://storm.apache.org/):
Download Apache Storm 2.4.0 (current supported version) from this link: https://archive.apache.org/dist/storm/apache-storm-2.4.0/apache-storm-2.4.0.tar.gz

2. Clone this repository.

3. Run `mvn install` in the working directory

4. Point to your crawler-conf.yaml file in *crawler.flux*:
  
  ```
  includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: {path to your crawler-conf.yaml file}
      override: true
  ```
  Note: If you set it "crawler-conf.yaml", then you can directly use the crawler-conf.yaml in this repository.

5. To start the link checker on local mode, run `apache-storm-2.4.0/bin/storm storm local path/to/this/repository/target/linkchecker-<current version>.jar  org.apache.storm.flux.Flux --local path/to/this/repository/crawler.flux --local-ttl 3600`

**For remote cluster setup, have a look at the documentation of the [apache storm](https://storm.apache.org/releases/2.4.0/Setting-up-a-Storm-cluster.html) project, please.**
  
# Simple Explanation of Current Implementation

Our SQL database has got these tables:
1. **url:** This is the table that linkchecker reads the URLs to check from. So this will be populated by another application (in our case curation-module or linkchecker-api).
1. **status:** This is the table that linkchecker saves the results into.
1. **history:** If a URL is checked more than once, the previous checking result is saved in the history table and the record in the status table is updated.   
1. **obsolete** A flat table which keeps the records still for a while after purging the from the other tables 
1. **providerGroup**
1. **context**: The table saves the context (the file or the upload) in which the link is found 
1. **url_context**: Joins url-table n-n to the context table, so that each URL might appear in different contexts. Moreover the table contains the last time when the link was ingested and and a boolean flag which indicates if the join is still active. Only URLs which have at least one active join are considered to be checked!
1. **client** The table is basically used to identify the link source

*crawler.flux* defines our topology. It defines all the spouts, bolts and streams.
1. `eu.clarin.linkchecker.spout.LPASpout` uses the [linkchecker-persistence API](https://github.com/clarin-eric/linkchecker-persistence) to fill up a buffer with URLs to check.
1. `com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt` partitions the URLs by a configured criteria
1. `eu.clarin.linkchecker.bolt.MetricsFetcherBolt` fetches the urls. It sends redirects back to URLPartitionerBolt and sends the rest onwards down the stream to StatusUpdaterBolt. Modification of  `com.digitalpebble.stormcrawler.bolt.FetcherBolt`
1. `eu.clarin.linkchecker.bolt.StatusUpdaterBolt` persists the results in the status table of the database via [https://github.com/clarin-eric/linkchecker-persistence).
1. `eu.clarin.linkchecker.bolt.SimpleStackBolt` persists the latest checking results into a Java Object file for use in curation-web
