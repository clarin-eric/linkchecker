# Stormychecker
Stormychecker is a Storm Crawler adaptation for URL checking. Instead of crawling, it checks the status of URLs and persists them in a database (currently mysql)

# How to setup and run

0. Before you can run stormychecker, you need to install ApacheStorm: Download Apache Storm 1.2.2 (current supported version) from this link: https://archive.apache.org/dist/storm/apache-storm-1.2.2/apache-storm-1.2.2.tar.gz

1. Clone this repository.

2. Run `mvn install` in the working directory

3. Run *tableCreation.script* on your mysql database. It requires a database with the name *stormychecker*. You can change the database name and the table names in the script but then you would have to change the *crawler-conf.yaml* configuration for those parameters as well.

4. Add your mysql url and login parameters to *crawler-conf.yaml* (and change any other parameters you wish, ex: http.agent):
  ```
  sql.connection:
  url: {your mysql url, ex: "jdbc:mysql://localhost:3307/stormychecker"}
  user: {your mysql username}
  password: {your mysql password}
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

6. To start stormychecker on local mode, run `apache-storm-1.2.2/bin/storm jar path/to/this/repository/target/stormychecker-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --local path/to/this/repository/crawler.flux --sleep 86400000`
  Note: For now, it is on SNAPSHOT level because this repository containst just a very basic implementation.
  
  
# Simple Explanation of Current Implementation

Our MYSQL database has 3 tables:
1. **urls:** This is the table that stormychecker reads from. So this will be populated by another application(in our case curation-module).
2. **status:** This is the table that stormychecker saves the results into.
3. **metrics:** This table is filled by default storm-crawler behaviour in FetcherBolt and has some statistics information.

*crawler.flux* defines our topology. It defines all the spouts, bolts and streams.
1. `com.digitalpebble.stormcrawler.sql.SQLSpout` reads from the urls table in the database and sends it to URLPartitionerBolt. It reads only when nextfetchdate is in the future and it orders the reads based on that column.
2. `com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt` partitions the URLS to host, path, parameter etc.
3. `at.ac.oeaw.acdh.RedirectFetcherBolt` fetches the urls. It follows redirects and passes all results onwards down the stream to StatusUpdaterBolt. Modification of  `com.digitalpebble.stormcrawler.bolt.FetcherBolt`
4. `at.ac.oeaw.acdh.StatusUpdaterBolt` persists the results in the status table in the database. It also persists nextfetchdate and host columns in the urls table. Modification of `com.digitalpebble.stormcrawler.sql.StatusUpdaterBolt`.

Note: For now streams just forward the tuples between the bolts. Parallelism is currently set to 1, so streams are not fully used to their potential right now.
