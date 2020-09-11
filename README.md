# Couchbase N1QL JDBC Driver

This driver supports the Couchbase N1QL query language with some extensions for basic bucket management. 

## How to build jar
```
# Linux, MacOs
./gradlew jar

# Windows
gradlew.bat jar
```

You'll find it in build/libs.

## JDBC connection string

```
jdbc:couchbase:<host1[:port1],host2[:port2],...>[/defaultBucket][?<property1>=<value>&<property2>=<value>&...]
```

The driver supports a custom Couchbase port mapping, and the specified port should be a key-value service port [11210 by default].

If you are connecting to a pre Couchbase 6.5 cluster, a `defaultBucket` must be specified in order to properly initialize the connection.

Recognized properties are the following:
  * Recognized by the driver itself
      * `user=<username>` [required parameter]
      * `password=<password>` [required parameter]
      * `sslenabled=true/false`
      * `meta.sampling.size=<integer>` specifies a number of documents fetched in order to infer a database schema
      * `query.scan.consistency=not_bounded/request_plus` specifies a query scan consistency (RYW consistency) [default value is `not_bounded`]
  * Propagated to a Couchbase cluster
      * The full list of recognized parameters is documented in the Couchbase [Client-Settings Documentation](https://docs.couchbase.com/java-sdk/current/ref/client-settings.html).
      Any client setting with a system property name may also be specified as a connection string parameter (without the com.couchbase.env. prefix).


## Extended N1QL statements

This driver supports some extra statements for the N1QL query language. These are for a bucket (keyspace) management (create and drop). 
Important note: create bucket statement is valid for Couchbase Enterprise edition only.

#### Create bucket statement
```
create-bucket ::= CREATE ( BUCKET | TABLE ) [ WITH PRIMARY INDEX ] keyspace-ref [ bucket-with ] 
keyspace-ref ::= [ namespace ':' ] keyspace
bucket-with ::= WITH expr
```
Where `expr` is a json object with optional bucket settings:
  * `flushEnabled` true/false [default is false]
  * `ramQuotaMB` number [default is 100]
  * `replicaNumber` number [default is 1]
  * `replicaIndexes` true/false [default is false]
  * `maxTTL` number of seconds [default is 0]
  * `compressionMode` string, one of "off"/"passive"/"active" [default is passive]
  * `bucketType` string, one of "membase"/"memcached"/"ephemeral" [default is membase]
  * `conflictResolutionType` string, one of "lww"/"seqno" [default is seqno]
  * `evictionPolicy` string, one of "fullEviction"/"valueOnly"/"nruEviction"/"noEviction" [default is based on a bucket type]

If `WITH PRIMARY INDEX` clause is present, a default primary index will be built for the newly created bucket.

Example:
```
create bucket with primary index bucket_name 
    with { ramQuotaMB: 128, bucketType: "ephemeral" }
```
#### Drop bucket statement
```
drop-bucket ::= DROP ( BUCKET | TABLE ) keyspace-ref
keyspace-ref ::= [ namespace ':' ] keyspace
```

Example:
```
drop bucket bucket_name
```
