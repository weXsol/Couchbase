/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2015 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.couchbase.shared;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.exist.couchbase.xquery.CouchbaseModule.COBA0001;
import org.exist.xquery.XPathException;

/**
 * Class for managing CouchbaseCluster objects
 *
 * @author Dannes Wessels
 */
public class CouchbaseClusterManager {

    protected final static Logger LOG = LogManager.getLogger(CouchbaseClusterManager.class);

    /**
     * Singleton
     */
    private static CouchbaseClusterManager instance = null;

    /**
     * Needed for optimizing connections
     */
    private static CouchbaseEnvironment cbEnvironment = null;

    /**
     * Storage of connections
     */
    private final Map<String, CouchbaseClusterConnection> clusters = new HashMap<>();

    /**
     * Get instance of object, initialize when needed.
     *
     * @return Instance of class.
     */
    public static synchronized CouchbaseClusterManager getInstance() {
        if (instance == null) {
            instance = new CouchbaseClusterManager();
            cbEnvironment = DefaultCouchbaseEnvironment.builder().build(); // .queryEnabled(true)
        }
        return instance;
    }

    private void add(String id, CouchbaseClusterConnection connection) {
        clusters.put(id, connection);
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
        return clusters.get(clusterId).getCluster();
    }

    public String getBucketPassword(String clusterId) {
        CouchbaseClusterConnection ccc = clusters.get(clusterId);
        if (ccc == null) {
            LOG.debug(String.format("No bucket password for '%s'", clusterId));
            return null;
        }
        return ccc.getBucketPassword();
    }
    
    public Collection<CouchbaseClusterConnection> getClusterConnections(){
        return clusters.values();
    }

    public boolean isValid(String clusterId) {
        return get(clusterId) != null;
    }

    public String create(String connectionString) {
        return create(connectionString, null, null);
    }

    public String create(String connectionString, String username, String defaultBucketPassword) {

        // Create new cb cluster with the connection string.
        CouchbaseCluster cluster = CouchbaseCluster.fromConnectionString(cbEnvironment, connectionString);

        // Create random identifier
        UUID clusterId = UUID.randomUUID();

        // Register the cluster
        CouchbaseClusterConnection ccc = new CouchbaseClusterConnection(cluster, username, defaultBucketPassword, connectionString, clusterId);
        add(clusterId.toString(), ccc);

        LOG.info(String.format("%s - %s", clusterId, cluster.toString()));

        return clusterId.toString();
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

        return clusters.get(clusterId).getCluster();

    }

    /**
     * Disconnect all cluster connections and shutdown the environment.
     * 
     * @return 
     */
    public List<String> shutdownAll() {
        
        List<String> ids = new ArrayList<>();

        // Stopping clusters
        clusters.values().stream().forEach((connection) -> {
            try {         
                String id = connection.getConnectionId().toString();
                remove(id);
                ids.add(id);
                
            } catch (Throwable ex) {
                LOG.error(ex.getMessage());
            }
        });

        // Shut it down, all
        cbEnvironment.shutdown();
        
        return ids;
    }

}
