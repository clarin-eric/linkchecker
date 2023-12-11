name: "linkchecker-test"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: true
      file: "/crawler-test-conf.yaml"
      override: true

spouts:
  - id: "nonHdlSpout"
    className: "eu.clarin.linkchecker.spout.LPASpout"
    parallelism: 1
    constructorArgs:
      - >
         SELECT id, name FROM 
               (SELECT ROW_NUMBER() OVER (PARTITION BY u.group_key ORDER BY u.priority DESC, s.checking_date) AS order_Nr, u.id, u.name, u.group_key, u.valid, u.priority, s.checking_date 
               FROM url u 
               LEFT JOIN status s ON s.url_id = u.id 
               WHERE u.valid=true 
               AND u.group_key != 'hdl.handle.net'
               AND u.id IN (SELECT uc.url_id FROM url_context uc WHERE uc.active = true) 
               AND (s.checking_date IS NULL OR DATEDIFF(NOW(), s.checking_date) > 1)
               ORDER BY u.group_key, u.priority DESC, s.checking_date) tab1
            WHERE order_nr <= 100 
            ORDER by tab1.priority DESC, tab1.checking_date 
            LIMIT 3000
         
  - id: "hdlSpout"
    className: "eu.clarin.linkchecker.spout.LPASpout"
    parallelism: 1
    constructorArgs:
      - >
         SELECT id, name FROM 
            (SELECT ROW_NUMBER() OVER (PARTITION BY handle_prefix ORDER BY u.priority DESC, s.checking_date) AS order_Nr, u.id, u.name, REGEXP_SUBSTR(u.name, 'hdl.handle.net/\\K([^/]*)') AS handle_prefix, u.valid, u.priority, s.checking_date 
            FROM url u 
            LEFT JOIN status s ON s.url_id = u.id 
            WHERE u.valid=true 
            AND u.group_key = 'hdl.handle.net'
            AND u.id IN (SELECT uc.url_id FROM url_context uc WHERE uc.active = true) 
            AND (s.checking_date IS NULL OR DATEDIFF(NOW(), s.checking_date) > 1)
            ORDER BY handle_prefix, u.priority DESC, s.checking_date) tab1
         WHERE order_nr <= 20 
         ORDER by tab1.priority DESC, tab1.checking_date
         LIMIT 500   

bolts:
  - id: "partitioner"
    className: "com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt"
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
  - from: "nonHdlSpout"
    to: "partitioner"
    grouping:
      type: SHUFFLE
  - from: "hdlSpout"
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