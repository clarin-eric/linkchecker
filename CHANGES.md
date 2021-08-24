# version 2.1
- Java version upgrade to Java 11
- dependency upgerade to storm 2.2
- dependency upgrade to storm crawler 2.1
- using resource-availability-status-api for db access in RASASpout and StatusUpdaterBolt instead of direct db access 
- storing originalUrl in metadata which allows the use of unmodified storm crawler Abstract super-classes
- using storm crawler's SimpleURLBuffer instead of LinkedList to handle tuples