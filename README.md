# Stormychecker
Stormychecker is a Storm Crawler adaptation for URL checking. Instead of crawling, it checks the status of URLs and persists them in a database (currently mysql)

# How to setup and run

1. Clone this repository.

2. Run *tableCreation.script* on your mysql database. It requires a database with the name *stormcrawler*. You can change the database name and the table names in the script but then you would have to change the *crawler-conf.yaml* configuration for those parameters as well.

3. Add your mysql url and login parameters to *crawler-conf.yaml* (and change any other parameters you wish, ex: http.agent):
  ```
  sql.connection:
  url: {your mysql url, ex: "jdbc:mysql://localhost:3307/stormcrawler"}
  user: {your mysql username}
  password: {your mysql password}
  rewriteBatchedStatements: "true"
  useBatchMultiSend: "true"
  ```
4. Point to your crawler-conf.yaml file in *crawlerSQL.flux*:
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

5. Run `mvn install` in the working directory

6. Download Apache Storm 1.2.2 (current supported version) from this link: https://archive.apache.org/dist/storm/apache-storm-1.2.2/apache-storm-1.2.2.tar.gz

7. To start stormychecker, run `apache-storm-1.2.2/bin/storm jar path/to/this/repository/target/stormychecker-1.0-SNAPSHOT.jar  org.apache.storm.flux.Flux --local path/to/this/repository/crawlerSQL.flux --sleep 86400000`
  Note: For now, it is on SNAPSHOT level because this repository containst just a very basic implementation.
  
  
# Simple Explanation of Current Implementation

Our MYSQL database has 3 tables:
1. **urls:** This is the table that stormychecker reads from. So this will be populated by another application(in our case curation-module).
2. **status:** This is the table that stormychecker saves the results into.
3. **metrics:** This table is filled by default storm-crawler behaviour in FetcherBolt and has some statistics information.

*crawlerSQL.flux* defines our topology. It defines all the spouts, bolts and streams.
2. `com.digitalpebble.stormcrawler.sql.SQLSpout` reads from the urls table in the database and sends it to URLPartitionerBolt.
3. `com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt` partitions the URLS to host, path, parameter etc.
4. `com.digitalpebble.stormcrawler.bolt.FetcherBolt` fetches the urls. [Here](https://github.com/DigitalPebble/storm-crawler/wiki/FetcherBolt%28s%29) is how it works. This is the default implementation but we might need to adapt it in the future. 
5. `at.ac.oeaw.acdh.StatusUpdaterBolt` persists the results in the status table in the database. This is our own adaptation of `com.digitalpebble.stormcrawler.sql.StatusUpdaterBolt`.

Note: For now streams just forward the tuples between the bolts. Parallelism is currently set to 1, so streams are not fully used to their potential right now.
