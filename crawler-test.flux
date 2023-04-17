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
    className: "eu.clarin.linkchecker.spout.LPASpout"
    parallelism: 1

bolts:
  - id: "fetcher"
    className: "eu.clarin.linkchecker.bolt.MetricsFetcherBolt"
    parallelism: 1
  - id: "status"
    className: "eu.clarin.linkchecker.bolt.StatusUpdaterBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "fetcher"
    grouping:
      type: SHUFFLE
  - from: "fetcher"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"        
  - from: "fetcher"
    to: "fetcher"
    grouping:
      type: SHUFFLE
      streamId: "redirect"
