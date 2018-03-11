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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.xquery.XPathException;

import java.util.*;

import static org.exist.couchbase.xquery.CouchbaseModule.COBA0001;

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
    private final Map<String, CouchbaseClusterConnection> clusterConnections = new HashMap<>();

    /**
     * Get instance of object, initialize when needed.
     *
     * @return Instance of class.
     */
    public static synchronized CouchbaseClusterManager getInstance() {
        if (instance == null) {
            instance = new CouchbaseClusterManager();
            cbEnvironment = DefaultCouchbaseEnvironment.builder()
                    .connectTimeout(DefaultCouchbaseEnvironment.MAX_REQUEST_LIFETIME)
                    .kvTimeout(DefaultCouchbaseEnvironment.MAX_REQUEST_LIFETIME)
                    .build(); // Hardcode timeout
        }
        return instance;
    }

    private void add(final String clusterConnectionId, final CouchbaseClusterConnection connection) {
        clusterConnections.put(clusterConnectionId, connection);
    }

    public void remove(final String clusterConnectionId) {

        // Close connection
        final CouchbaseCluster c = get(clusterConnectionId);

        // If no connectionID is available, silently ignore
        if (c == null) {
            LOG.debug("clusterConnectionId does not exist, ignoring.", clusterConnectionId);
            return;
        }

        c.disconnect();

        // Remove
        clusterConnections.remove(clusterConnectionId);
    }

    public Set<String> list() {
        return clusterConnections.keySet();
    }

    /**
     * Get reference to cluster when present, or NULL.
     *
     * @param clusterConnectionId ID of connection
     * @return Cluster reference.
     */
    public CouchbaseCluster get(final String clusterConnectionId) {
        CouchbaseClusterConnection couchbaseClusterConnection = clusterConnections.get(clusterConnectionId);

        if (couchbaseClusterConnection == null) {
            return null;
        }

        return couchbaseClusterConnection.getCluster();
    }

    public String getBucketPassword(final String clusterConnectionId) {
        final CouchbaseClusterConnection ccc = clusterConnections.get(clusterConnectionId);
        if (ccc == null) {
            LOG.debug(String.format("No bucket password for '%s'", clusterConnectionId));
            return null;
        }
        return ccc.getBucketPassword();
    }

    public Collection<CouchbaseClusterConnection> getClusterConnections() {
        return clusterConnections.values();
    }

    public boolean isValid(final String clusterId) {
        return get(clusterId) != null;
    }

    public String create(final String connectionString) {
        return create(connectionString, null, null);
    }

    public String create(final String connectionString, final String username, final String defaultBucketPassword) {

        // Create new cb cluster with the connection string.
        final CouchbaseCluster cluster = CouchbaseCluster.fromConnectionString(cbEnvironment, connectionString);

        // Create random identifier
        final UUID clusterConnectionId = UUID.randomUUID();

        // Register the cluster
        final CouchbaseClusterConnection ccc = new CouchbaseClusterConnection(cluster, username, defaultBucketPassword, connectionString, clusterConnectionId);
        add(clusterConnectionId.toString(), ccc);

        LOG.info(String.format("%s - %s", clusterConnectionId, cluster.toString()));

        return clusterConnectionId.toString();
    }

    public CouchbaseCluster validate(final String clusterConnectionId) throws XPathException {
        if (clusterConnectionId == null || !isValid(clusterConnectionId)) {
            try {
                // introduce a delay
                Thread.sleep(1000L);

            } catch (final InterruptedException ex) {
                LOG.error(ex);
            }
            throw new XPathException(COBA0001, "The provided Couchbase clusterConnectionId is not valid.");
        }

        return clusterConnections.get(clusterConnectionId).getCluster();

    }

    /**
     * Disconnect all cluster connections and shutdown the environment.
     *
     * @return List of connections that have been shutdown.
     */
    public List<String> shutdownAll() {

        final List<String> ids = new ArrayList<>();

        // Stopping clusterConnections
        clusterConnections.values().forEach((connection) -> {
            try {
                final String id = connection.getConnectionId().toString();
                remove(id);
                ids.add(id);

            } catch (final Throwable ex) {
                LOG.error(ex.getMessage());
            }
        });

        // Shut it down, all
        cbEnvironment.shutdown();

        return ids;
    }

}
