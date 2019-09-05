# Stormychecker
Stormychecker is a Storm Crawler adaptation for URL checking. Instead of crawling, it checks the status of URLs and persists them in a database (currently mysql)

# How to setup and run

1. Clone this repository.

2. Run *tableCreation.script* on your mysql database. It requires a database with the name *stormcrawler*. You can change the database name and the table names in the script but then you would have to change the *crawler-conf.yaml* configuration for those parameters as well.

3. Add your mysql url and login parameters to *crawler-conf.yaml*:
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
