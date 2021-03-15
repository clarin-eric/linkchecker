name: "linkchecker"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: true
      file: "/crawler-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "at.ac.oeaw.acdh.linkchecker.spout.SQLSpout"
    parallelism: 1

bolts:
  - id: "partitioner"
    className: "at.ac.oeaw.acdh.linkchecker.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "at.ac.oeaw.acdh.linkchecker.bolt.FetcherBolt"
    parallelism: 10
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


