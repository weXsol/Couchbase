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
package org.exist.couchbase.xquery.design;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.DesignDocument;
import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.shared.GenericExceptionHandler;
import org.exist.couchbase.shared.JsonToMap;
import org.exist.couchbase.shared.MapToJson;
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
import org.exist.xquery.value.Type;

/**
 * Insert or upsert design document
 *
 * @author Dannes Wessels
 */
public class InsertUpsertDesignDocument extends BasicFunction {

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName("upsert-design-document", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
        "Upsert design document with views.",
        new SequenceType[]{
            new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
            new FunctionParameterSequenceType("design-document-name", Type.STRING, Cardinality.ONE, "Name of design document"),
            new FunctionParameterSequenceType("view-data", Type.ITEM, Cardinality.ONE, "JSON formatted view data.")},
        new FunctionReturnSequenceType(Type.MAP, Cardinality.ONE, "The upserted document.")
        ),
        new FunctionSignature(
        new QName("insert-design-document", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
        "Insert design document with views.",
        new SequenceType[]{
            new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
            new FunctionParameterSequenceType("design-document-name", Type.STRING, Cardinality.ONE, "Name of design document"),
            new FunctionParameterSequenceType("view-data", Type.ITEM, Cardinality.ONE, "JSON formatted view data.")},
        new FunctionReturnSequenceType(Type.MAP, Cardinality.ZERO_OR_ONE, "The inserted document")
        )
    };

    public InsertUpsertDesignDocument(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        final CouchbaseClusterManager cmm = CouchbaseClusterManager.getInstance();

        // Get connection details
        String clusterId = args[0].itemAt(0).getStringValue();

        // Get reference to cluster
        CouchbaseCluster cluster = cmm.validate(clusterId);

        // Retrieve other parameters             
        String bucketName = (args[1].isEmpty()) ? Constants.DEFAULT_BUCKET : args[1].itemAt(0).getStringValue();
        String designName = args[2].itemAt(0).getStringValue();

        String bucketPassword = cmm.getBucketPassword(clusterId);

        try {
            // Get access to bucketmanager
            BucketManager bucketManager = cluster.openBucket(bucketName, bucketPassword).bucketManager();

            // Convert to JSonObject
            JsonObject jsonObject = (JsonObject) MapToJson.convert(args[3]);

            // Convert JSON to design document
            DesignDocument input = DesignDocument.from(designName, jsonObject);

            // Retrieve all design documents
            DesignDocument designDocument = (isCalledAs("upsert-design-document"))
                    ? bucketManager.upsertDesignDocument(input)
                    : bucketManager.insertDesignDocument(input);

            if (designDocument == null) {
                return Sequence.EMPTY_SEQUENCE;
            } else {
                return JsonToMap.convert(designDocument.toJsonObject(), context);
            }

        } catch (Throwable ex) {
            return GenericExceptionHandler.handleException(this, ex);
        }

    }

}
