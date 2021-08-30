# Link Checker
The Link checker is a [StormCrawler](https://github.com/DigitalPebble/storm-crawler) 
adaptation for URL checking. Instead of crawling, it checks the status of URLs and
persists them in a database (currently MariaDB/MySQL)

# How to setup and run

1. Before you can run linkchecker, you need to install [Apache Storm](https://storm.apache.org/):
Download Apache Storm 2.2.0 (current supported version) from this link: https://archive.apache.org/dist/storm/apache-storm-2.2.0/apache-storm-2.2.0.tar.gz

2. Clone this repository.

3. Run `mvn install` in the working directory

4. Add your hikari connection pool propiertes to *crawler-conf.yaml* (and change any other parameters you wish, ex: http.agent):
  
  ```
  HIKARI:
   driverClassName: com.mysql.cj.jdbc.Driver
   jdbcUrl: {your database url, ex: "jdbc:mysql://localhost:3307/stormychecker"}
   username: {your database username}
   password: {your database password}
  ```
5. Point to your crawler-conf.yaml file in *crawler.flux*:
  
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

6. To start the link checker on local mode, run `apache-storm-2.2.0/bin/storm storm local path/to/this/repository/target/linkchecker-2.1.0.jar  org.apache.storm.flux.Flux --local path/to/this/repository/crawler.flux --local-ttl 3600`

  
  
# Simple Explanation of Current Implementation

Our SQL database has 6 tables:
1. **url:** This is the table that linkchecker reads the URLs to check from. So this will be populated by another application(in our case curation-module).
2. **status:** This is the table that linkchecker saves the results into.
3. **history:** If a URL is checked more than once, the previous checking result is saved in the history table and the record in the status table is updated.   
4. **providerGroup**
5. **context**: The table saves the context in which
6. **url_context**: joins url-table n-n to the context table, so that each URL might appear in different contexts. Moreover the table contains the last time when the link was ingested and and a boolean flag which indicates if the join is still active. Only URLs which have at least one active join are considered to be checked!

*crawler.flux* defines our topology. It defines all the spouts, bolts and streams.
1. `at.ac.oeaw.acdh.linkchecker.spout.RASASpout` uses the [resource availability status API](https://github.com/clarin-eric/resource-availability-status-api) to fill up a buffer with URLs to check.
2. `com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt` partitions the URLs by a configured criteria
3. `at.ac.oeaw.acdh.linkchecker.bolt.MetricsFetcherBolt` fetches the urls. It sends redirects back to URLPartitionerBolt and sends the rest onwards down the stream to StatusUpdaterBolt. Modification of  `com.digitalpebble.stormcrawler.bolt.FetcherBolt`
4. `at.ac.oeaw.acdh.linkchecker.bolt.StatusUpdaterBolt` persists the results in the status table of the database via [resource availability status API](https://github.com/clarin-eric/resource-availability-status-api).

