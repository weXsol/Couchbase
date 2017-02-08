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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.exist.dom.memtree.MemTreeBuilder;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;

/**
 * Container for a Couchbase cluster connection.
 *
 * @author Dannes Wessels
 */
public class CouchbaseClusterConnection {

    private static final Object LOCK = new Object();

    private final String username;

    private final String bucketPassword;

    private final String connectionString;

    private final Calendar creation;

    private final CouchbaseCluster cluster;
    private final UUID connectionId;
    private long invokes = 0L;

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
