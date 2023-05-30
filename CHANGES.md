# version 3.0.3
- fix for issue #57

# version 3.0.2
- adding missing PartitionerBolt again (bug fix!)

# version 3.0.1
- bugfix in class MetricsFetcherBolt to prevent null message
- bugfix in dependency linkchecker-persistence

# version 3.0.0
- replacement of the persistence layer: the [resource availability status API (RASA)](https://github.com/clarin-eric/resource-availability-status-api) 
is replaced by [curation-persistence](https://github.com/clarin-eric/curation-persistence)
- inclusion of maven wrapper
- deletion of template_crawler-conf.yaml and use of environment variables in crawler-conf.yaml

# version 2.4.0
- upgrade to storm 2.4.0 and storm crawler 2.4
 
# version 2.3.0
- improved algorithm for next links to check
- trimming URLs (done by RASA) and escaping white spaces in URLs used for request

# version 2.2.0
- checking time taken now at the start of the checking instead of the end
- accurate control flow instead of using exceptions
- fixing bug of doubled log messages in RASASpout
- reducing log messages of MetricsFetcherBolt to one message on info-level per 100 checks  

# version 2.1.1
- increase size of content-length/byteSize from int to long in Java and from int to bigint in mysql/maria db
- increase size of message from varchar(256) to varchar(1024) with a truncation in Java, if the message is longer that 1024 characters

# version 2.1.0
- Java version upgrade to Java 11
- dependency upgerade to storm 2.2
- dependency upgrade to storm crawler 2.1
- using resource-availability-status-api for db access in RASASpout and StatusUpdaterBolt instead of direct db access 
- storing originalUrl in metadata which allows the use of unmodified storm crawler Abstract super-classes
- using storm crawler's SimpleURLBuffer instead of LinkedList to handle tuples