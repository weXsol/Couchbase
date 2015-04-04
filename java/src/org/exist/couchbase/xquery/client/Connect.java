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
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 *  Connect to couchbase cluster
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

    public Connect(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the correct group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.COUCHBASE_GROUP)) {
            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.COUCHBASE_GROUP);
            LOG.error(txt);
            throw new XPathException(this, txt);
        }

        // Get connection string URL
        String connectionString = args[0].itemAt(0).getStringValue();

        // Get password for bucket, when available
        String password = (getArgumentCount() > 1) ? args[1].itemAt(0).getStringValue() : null;

        // Register connection
        String clusterId = CouchbaseClusterManager.getInstance().create(connectionString, context.getRealUser().getUsername(), password);

        // Return id
        return new StringValue(clusterId);
    }   
}
