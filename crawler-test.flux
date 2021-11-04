name: "linkchecker"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "./crawler-test-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "at.ac.oeaw.acdh.linkchecker.spout.RASASpout"
    parallelism: 1

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "at.ac.oeaw.acdh.linkchecker.bolt.MetricsFetcherBolt"
    parallelism: 1
  - id: "status"
    className: "at.ac.oeaw.acdh.linkchecker.bolt.StatusUpdaterBolt"
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
