package org.exist.couchbase.shared;

import com.couchbase.client.java.CouchbaseCluster;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;

/**
 *
 * @author wessels
 */
public class CouchbaseClusterConnection {

    private static final Object LOCK = new Object();

    private final String username;

    private final String bucketPassword;

    private final String connectionString;

    private final Calendar creation;

    private final CouchbaseCluster cluster;

    private long invokes = 0l;

    private final UUID connectionId;

    public CouchbaseClusterConnection(CouchbaseCluster cluster, String username, String bucketPassword, String connectionString, UUID connectionId) {
        this.cluster = cluster;
        this.username = username;
        this.bucketPassword = bucketPassword;
        this.connectionString = connectionString;
        this.creation = new GregorianCalendar();
        this.connectionId = connectionId;
    }

    public UUID getConnectionId() {
        return connectionId;
    }

    public String getBucketPassword() {
        return bucketPassword;
    }

    public CouchbaseCluster getCluster() {
        return cluster;
    }

    public String getUsername() {
        return username;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public Calendar getCreation() {
        return creation;
    }

    /**
     * Get number of invocations
     *
     * @return Number of invocations.
     */
    public long getInvokes() {
        return invokes;
    }

    public long increaseInvokes() {
        synchronized (LOCK) {
            return (invokes++);
        }
    }

    public void getReport(MemTreeBuilder builder) {

        builder.startElement("", "connection", "connection", null);

        builder.startElement("", "id", "id", null);
        builder.characters(getConnectionId().toString());
        builder.endElement();

        builder.startElement("", "username", "username", null);
        builder.characters(getUsername());
        builder.endElement();

        builder.startElement("", "has-bucket-password", "has-bucket-password", null);
        builder.characters((getBucketPassword() == null) ? "false" : "true");
        builder.endElement();

        builder.startElement("", "url", "url", null);
        builder.characters(getConnectionString());
        builder.endElement();

        builder.startElement("", "created", "created", null);
        builder.characters(DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(creation));
        builder.endElement();

        builder.endElement();
    }

}
