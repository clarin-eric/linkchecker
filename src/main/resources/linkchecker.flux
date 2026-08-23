name: "linkchecker"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: true
      file: "/linkchecker-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "eu.clarin.linkchecker.spout.LPASpout"
    parallelism: 1
    constructorArgs:
      - >
         SELECT id, name FROM
               (SELECT ROW_NUMBER() OVER (PARTITION BY u.group_key ORDER BY u.priority DESC, s.checking_date) AS order_nr, u.id, u.name, u.group_key, u.valid, u.priority, s.checking_date
               FROM url u
               LEFT JOIN status s ON s.url_id = u.id
               WHERE u.valid IS TRUE
               AND u.exclude_checking IS NOT TRUE
               AND u.id IN (SELECT uc.url_id FROM url_context uc WHERE uc.active = true)
               AND (s.checking_date IS NULL OR DATEDIFF(NOW(), s.checking_date) > 1)
               ORDER BY u.group_key, u.priority DESC, s.checking_date) tab1
            ORDER by order_nr
            LIMIT 10000

bolts:
  - id: "partitioner"
    className: "org.apache.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "eu.clarin.linkchecker.bolt.MetricsFetcherBolt"
    parallelism: 5
  - id: "status"
    className: "eu.clarin.linkchecker.bolt.StatusUpdaterBolt"
    parallelism: 5
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
      type: FIELDS
      args: ["url"]
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