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

import com.couchbase.client.java.CouchbaseAsyncCluster;

/**
 * Misc constants
 * <p>
 * Dannes Wessels
 */


public class Constants {
    public final static String COUCHBASE_GROUP = "couchbase";
    public static final String DEFAULT_BUCKET = CouchbaseAsyncCluster.DEFAULT_BUCKET;
    public static final String DEFAULT_HOST = CouchbaseAsyncCluster.DEFAULT_HOST;
}
