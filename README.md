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
jdbc:couchbase:<host1,host2,...>?<property1>=<value>&<property2>=<value>&...
```

The driver supports the default Couchbase port mapping only.

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
