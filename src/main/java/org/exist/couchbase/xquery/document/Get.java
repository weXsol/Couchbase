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
package org.exist.couchbase.xquery.document;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.*;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.*;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Retrieve document
 *
 * @author Dannes Wessels
 */
public class Get extends BasicFunction {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("get", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
                    "Retrieve document from bucket",
                    new SequenceType[]{
                            new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                            new FunctionParameterSequenceType("documentName", Type.STRING, Cardinality.ONE, "Name of document"),},
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The Json document, or Empty sequence when not found.")
            ),
            new FunctionSignature(
                    new QName("get", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
                    "Retrieve document from bucket",
                    new SequenceType[]{
                            new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                            new FunctionParameterSequenceType("documentName", Type.STRING, Cardinality.ONE, "Name of document"),
                            new FunctionParameterSequenceType("parameters", Type.MAP, Cardinality.ZERO_OR_ONE, "Query parameters")
                    },
                    new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The Json document, or Empty sequence when not found.")
            ),};

    public Get(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        final CouchbaseClusterManager cmm = CouchbaseClusterManager.getInstance();

        // Get connection details
        final String clusterId = args[0].itemAt(0).getStringValue();

        // Get reference to cluster
        final CouchbaseCluster cluster = cmm.validate(clusterId);

        // Retrieve other parameters             
        final String bucketName = (args[1].isEmpty()) ? Constants.DEFAULT_BUCKET : args[1].itemAt(0).getStringValue();
        final String bucketPassword = cmm.getBucketPassword(clusterId);

        final String docName = args[2].itemAt(0).getStringValue();

        final Map<String, Object> parameters = (getArgumentCount() > 3)
                ? ConversionTools.convert((AbstractMapType) args[3].itemAt(0))
                : null;

        try {
            // Perform action
            final JsonDocument result = (parameters == null)
                    ? get(cluster, bucketName, bucketPassword, docName)
                    : get(cluster, bucketName, bucketPassword, docName, parameters);

            if (result == null) {
                return EmptySequence.EMPTY_SEQUENCE;
            }

            // Return results
            return JsonToMap.convert(result.content(), context);

        } catch (final Throwable ex) {
            return GenericExceptionHandler.handleException(this, ex);
        }

    }

    /* Retrieve document */
    private JsonDocument get(final CouchbaseCluster cluster, final String bucketName, final String bucketPassword, final String docName) {
        return cluster.openBucket(bucketName, bucketPassword).get(docName);
    }

    /**
     * Retrieve documents with additional parameters
     */
    private JsonDocument get(final CouchbaseCluster cluster, final String bucketName, final String docName, final String bucketPassword, final Map<String, Object> parameters) {
        final long timeout = ConversionTools.getLongValue("timeout", parameters.get("timeout"));
        final TimeUnit timeUnit = TimeUnit.valueOf(parameters.get("timeUnit").toString().toUpperCase(Locale.US));

        return cluster.openBucket(bucketName, bucketPassword).get(docName, timeout, timeUnit);
    }

}
