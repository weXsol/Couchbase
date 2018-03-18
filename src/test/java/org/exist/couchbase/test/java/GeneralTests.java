package org.exist.couchbase.test.java;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import org.junit.*;

import static org.junit.Assert.assertEquals;

/**
 * @author wessels
 */
public class GeneralTests {

    Cluster cluster = null;

    public GeneralTests() {
    }

    @BeforeClass
    public static void setUpClass() {
        //BasicConfigurator.configure();
    }

    @AfterClass
    public static void tearDownClass() {
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
        final Bucket defaultBucket = cluster.openBucket();
        assertEquals("default", defaultBucket.name());

        final Bucket beerSampleBucket = cluster.openBucket("beer-sample");
        assertEquals("beer-sample", beerSampleBucket.name());

    }

}
