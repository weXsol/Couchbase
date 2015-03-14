package org.exist.couchbase.test.java;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author wessels
 */
public class GeneralTests {

    @BeforeClass
    public static void setUpClass() {
        //BasicConfigurator.configure();
    }

    @AfterClass
    public static void tearDownClass() {
    }

    Cluster cluster = null;

    public GeneralTests() {
    }

    @Before
    public void setUp() {
        // Connect to localhost
        cluster = CouchbaseCluster.create();
    }

    @After
    public void tearDown() {
        // Disconnect and clear all allocated resources
        cluster.disconnect();
    }

    @Test
    public void basicConnect() {

        // Open the default bucket and the "beer-sample" one
        Bucket defaultBucket = cluster.openBucket();
        assertEquals("default", defaultBucket.name());

        Bucket beerSampleBucket = cluster.openBucket("beer-sample");
        assertEquals("beer-sample", beerSampleBucket.name());

    }

}
