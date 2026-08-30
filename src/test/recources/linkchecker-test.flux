name: "linkchecker-test"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: true
      file: "/linkchecker-test-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "eu.clarin.linkchecker.spout.LPASpout"
    parallelism: 1
    constructorArgs:
      - >
         SELECT * FROM url_to_check
            LIMIT 10000
bolts:
  - id: "partitioner"
    className: "org.apache.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "eu.clarin.linkchecker.bolt.MetricsFetcherBolt"
    parallelism: 1
  - id: "status"
    className: "eu.clarin.linkchecker.bolt.StatusUpdaterBolt"
    parallelism: 1
  - id: "stack"
    className: "eu.clarin.linkchecker.bolt.SimpleStackBolt"
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
      type: SHUFFLE
      streamId: "status"        
  - from: "fetcher"
    to: "partitioner"
    grouping:
      type: SHUFFLE
      streamId: "redirect"
  - from: "status"
    to: "stack"
    grouping:
      type: SHUFFLE