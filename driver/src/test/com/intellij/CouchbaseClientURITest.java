package com.intellij;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author Liudmila Kornilova
 **/
public class CouchbaseClientURITest {

    @Test(expected = IllegalArgumentException.class)
    public void testUriForDifferentDb() {
        new CouchbaseClientURI("jdbc:postgresql://localhost:54332/guest", null);
    }

    @Test
    public void testSimpleUri() {
        CouchbaseClientURI uri = new CouchbaseClientURI("jdbc:couchbase:localhost:9042", null);
        List<String> hosts = Arrays.asList(uri.getHosts().split(","));
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
    }

    @Test
    public void testUriWithUserName() {
        CouchbaseClientURI uri = new CouchbaseClientURI("jdbc:couchbase:localhost:9042?user=cassandra", null);
        List<String> hosts = Arrays.asList(uri.getHosts().split(","));
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
        assertEquals("cassandra", uri.getUsername());
    }

    @Test
    public void testUriConnectionString() {
        String uriSting = "jdbc:couchbase:localhost:9042?user=cass&kv.timeout=123s&retry=exponential&password=pass";
        CouchbaseClientURI uri = new CouchbaseClientURI(uriSting, null);
        assertEquals("cass", uri.getUsername());
        assertEquals("pass", uri.getPassword());
        assertEquals("localhost:9042?kv.timeout=123s&retry=exponential", uri.getConnectionString());
    }

    @Test
    public void testUriConnectionStringWithoutOptionsPart() {
        String uriSting = "jdbc:couchbase:127.0.0.1:9042?user=p&password=p";
        CouchbaseClientURI uri = new CouchbaseClientURI(uriSting, null);
        assertEquals("127.0.0.1:9042?", uri.getConnectionString());
    }

    @Test
    public void testOptionsInProperties() {
        Properties properties = new Properties();
        properties.put("user", "NameFromProperties");
        properties.put("password", "PasswordFromProperties");
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042?user=cassandra&password=cassandra",
                properties);
        List<String> hosts = Arrays.asList(uri.getHosts().split(","));
        assertEquals(1, hosts.size());
        assertEquals("localhost:9042", hosts.get(0));
        assertEquals("NameFromProperties", uri.getUsername());
        assertEquals("PasswordFromProperties", uri.getPassword());
    }


    @Test
    public void testSslEnabledOptionTrue() {
        Properties properties = new Properties();
        properties.put("sslenabled", "true");
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042?name=cassandra&password=cassandra",
                properties);
        assertTrue(uri.getSslEnabled());
    }

    @Test
    public void testSslEnabledOptionFalse() {
        Properties properties = new Properties();
        properties.put("sslenabled", "false");
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042?name=cassandra&password=cassandra",
                properties);
        assertFalse(uri.getSslEnabled());
    }

    @Test
    public void testNullSslEnabledOptionFalse() {
        Properties properties = new Properties();
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042?name=cassandra&password=cassandra",
                properties);
        assertFalse(uri.getSslEnabled());
    }

    @Test
    public void testDefaultBucket() {
        Properties properties = new Properties();
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042/default",
                properties);
        assertEquals("default", uri.getDefaultBucket());
    }

    @Test
    public void testDefaultBucketWithOptions() {
        Properties properties = new Properties();
        CouchbaseClientURI uri = new CouchbaseClientURI(
                "jdbc:couchbase:localhost:9042/default?kv.timeout=123s&retry=exponential",
                properties);
        assertEquals("default", uri.getDefaultBucket());
        assertEquals("localhost:9042?kv.timeout=123s&retry=exponential", uri.getConnectionString());
    }
}