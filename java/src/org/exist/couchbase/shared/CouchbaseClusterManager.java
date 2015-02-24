package org.exist.couchbase.shared;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import static org.exist.couchbase.xquery.CouchbaseModule.COBA0001;

import org.exist.xquery.XPathException;

/**
 * Class for managing CouchbaseCluster objects
 *
 * @author Dannes Wessels
 */
public class CouchbaseClusterManager {

    protected final static Logger LOG = Logger.getLogger(CouchbaseClusterManager.class);

    /**
     * Singleton
     */
    private static CouchbaseClusterManager instance = null;

    /**
     * Needed for optimizing connections
     */
    private static CouchbaseEnvironment cbEnvironment = null;

    /**
     * Actual storage of cluster objects
     */
    private final Map<String, CouchbaseCluster> clusters = new HashMap<>();

    /**
     *   Get instance of object, initialize when needed.
     * 
     * @return Instance of class.
     */
    public static synchronized CouchbaseClusterManager getInstance() {
        if (instance == null) {
            instance = new CouchbaseClusterManager();
            cbEnvironment = DefaultCouchbaseEnvironment.create();
        }
        return instance;
    }

    private void add(String id, CouchbaseCluster cluster) {
        clusters.put(id, cluster);
    }

    public void remove(String clusterId) {
        
        // Close connection
        CouchbaseCluster c = get(clusterId);
        c.disconnect();
        
        // Remove
        clusters.remove(clusterId);
    }

    public Set<String> list() {
        return clusters.keySet();
    }

    public CouchbaseCluster get(String clusterId) {
        return clusters.get(clusterId);
    }

    public boolean isValid(String clusterId) {
        return get(clusterId) != null;
    }

    public String create(String connectionString) {

        // Create new cb cluster with the connection string.
        CouchbaseCluster cluster = CouchbaseCluster.create(cbEnvironment, connectionString);

        // Register the cluster
        String clusterId = UUID.randomUUID().toString();
        add(clusterId, cluster);

        LOG.info(String.format("%s - %s", clusterId, cluster.toString()));

        return clusterId;
    }

    public CouchbaseCluster validate(String clusterId) throws XPathException {
        if (clusterId == null || !isValid(clusterId)) {
            try {
                // introduce a delay
                Thread.sleep(1000l);

            } catch (InterruptedException ex) {
                LOG.error(ex);
            }
            throw new XPathException(COBA0001, "The provided Couchbase clusterId is not valid.");
        }

        return clusters.get(clusterId);

    }
    
    /**
     * Disconnect all cluster connections and shutdown the environment.
     */
    public void shutdownAll(){
            // Stopping clusters
            for(CouchbaseCluster c : clusters.values()){
                c.disconnect();
            }
            
            // Shut it down, all
            cbEnvironment.shutdown();
    }

}
