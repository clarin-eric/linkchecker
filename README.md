# Link Checker
The Link checker is a [StormCrawler](https://github.com/DigitalPebble/storm-crawler) 
adaptation for URL checking. Instead of crawling, it checks the status of URLs and
persists them in a database (currently MariaDB/MySQL)

# How to setup and run

0. Before you can run linkchecker, you need to install [Apache Storm](https://storm.apache.org/):
Download Apache Storm 2.1.0 (current supported version) from this link: https://archive.apache.org/dist/storm/apache-storm-2.1.0/apache-storm-2.1.0.tar.gz

1. Clone this repository.

2. Run `mvn install` in the working directory

3. ...

4. Add your database url and login parameters to *crawler-conf.yaml* (and change any other parameters you wish, ex: http.agent):
  ```
  sql.connection:
  url: {your database url, ex: "jdbc:mysql://localhost:3307/stormychecker"}
  user: {your database username}
  password: {your database password}
  rewriteBatchedStatements: "true"
  useBatchMultiSend: "true"
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

Our SQL database has 3 tables:
1. **urls:** This is the table that linkchecker reads from. So this will be populated by another application(in our case curation-module).
2. **status:** This is the table that linkchecker saves the results into.
3. **metrics:** This table is filled by default storm-crawler behaviour in FetcherBolt and has some statistics information.

*crawler.flux* defines our topology. It defines all the spouts, bolts and streams.
1. `com.digitalpebble.stormcrawler.sql.SQLSpout` reads from the urls table in the database and sends it to URLPartitionerBolt. It reads only when nextfetchdate is in the future and it orders the reads based on that column.
2. `com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt` partitions the URLS to host, path, parameter etc.
3. `FetcherBolt` fetches the urls. It sends redirects back to URLPartitionerBolt and sends the rest onwards down the stream to StatusUpdaterBolt. Modification of  `com.digitalpebble.stormcrawler.bolt.FetcherBolt`
4. `StatusUpdaterBolt` persists the results in the status table in the database. It also persists nextfetchdate and host columns in the urls table. Modification of `com.digitalpebble.stormcrawler.sql.StatusUpdaterBolt`.

Note: For now streams just forward the tuples between the bolts. Parallelism is currently set to 1, so streams are not fully used to their potential right now.
