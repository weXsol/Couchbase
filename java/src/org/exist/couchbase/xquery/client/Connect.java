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
package org.exist.couchbase.xquery.client;

import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.shared.GenericExceptionHandler;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * Connect to couchbase cluster
 *
 * @author Dannes Wessels
 */

public class Connect extends BasicFunction {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("connect", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
                    "Connect to Couchbase server",
                    new SequenceType[]{
                            new FunctionParameterSequenceType("connection", Type.STRING, Cardinality.ONE, "Server connection string")
                    },
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The identifier for the cluster connection")
            ),
            new FunctionSignature(
                    new QName("connect", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
                    "Connect to Couchbase server",
                    new SequenceType[]{
                            new FunctionParameterSequenceType("connection", Type.STRING, Cardinality.ONE, "Server connection string"),
                            new FunctionParameterSequenceType("password", Type.STRING, Cardinality.ONE, "Bucket passsword")
                    },
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ONE, "The identifier for the cluster connection")
            ),
    };

    public Connect(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the correct group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.COUCHBASE_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.COUCHBASE_GROUP);
            LOG.error(txt);
            throw new XPathException(this, CouchbaseModule.COBA0003, txt);
        }

        try {
            // Get connection string URL
            final String connectionString = args[0].itemAt(0).getStringValue();

            // Get password for bucket, when available
            final String password = (getArgumentCount() > 1) ? args[1].itemAt(0).getStringValue() : null;

            // Username is only used for reporting.
            final String username = context.getEffectiveUser().getUsername();

            // Register connection
            final String clusterId = CouchbaseClusterManager.getInstance().create(connectionString, username, password);

            // Return id
            return new StringValue(clusterId);

        } catch (final Throwable ex) {
            return GenericExceptionHandler.handleException(this, ex);
        }
    }
}
