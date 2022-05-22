name: "linkchecker-test"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "./crawler-test-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "eu.clarin.linkchecker.spout.RASAQuerySpout"
    constructorArgs: 
      - >
       SELECT u.* FROM url u 
       WHERE u.valid=true 
       ORDER BY RAND() 
       LIMIT 1000
    parallelism: 1

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "eu.clarin.linkchecker.bolt.MetricsFetcherBolt"
    parallelism: 1
  - id: "status"
    className: "eu.clarin.linkchecker.bolt.StatusUpdaterBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "partitioner"
    grouping:
      type: SHUFFLE
  - from: "partitioner"
    to: "fetcher"
    grouping:
      type: FIELDS
      args: ["key"]
  - from: "fetcher"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"        
  - from: "fetcher"
    to: "partitioner"
    grouping:
      type: SHUFFLE
      streamId: "redirect"
