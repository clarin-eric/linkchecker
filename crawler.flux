name: "crawler"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "/usr/local/stormychecker/conf/crawler-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "at.ac.oeaw.acdh.SQLSpout"
    parallelism: 1

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "at.ac.oeaw.acdh.RedirectFetcherBolt"
    parallelism: 10
  - id: "status"
    className: "at.ac.oeaw.acdh.StatusUpdaterBolt"
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

