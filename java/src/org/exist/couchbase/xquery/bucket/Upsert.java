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
package org.exist.couchbase.xquery.bucket;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.exist.couchbase.shared.ConversionTools;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.xquery.CouchbaseModule;
import static org.exist.couchbase.xquery.CouchbaseModule.COBA0010;
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
 *  Upsert document into bucket
 *
 * @author Dannes Wessels
 */
public class Upsert extends BasicFunction {
    
    private static final String UPSERT = "upsert";
    private static final String INSERT = "insert";
    

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName(UPSERT, CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
            "Upsert document into database",
            new SequenceType[]{
                new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                new FunctionParameterSequenceType("documentName", Type.STRING, Cardinality.ONE, "Name of document"),
                new FunctionParameterSequenceType("payload", Type.STRING, Cardinality.ONE, "JSon document content"),
                
            },
            new FunctionReturnSequenceType(Type.EMPTY, Cardinality.ZERO, "Empty sequence")
        ),
        
        new FunctionSignature(
            new QName(INSERT, CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
            "Insert document into database",
            new SequenceType[]{
                new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                new FunctionParameterSequenceType("documentName", Type.STRING, Cardinality.ONE, "Name of document"),
                new FunctionParameterSequenceType("payload", Type.STRING, Cardinality.ONE, "JSon document content"),
                
            },
            new FunctionReturnSequenceType(Type.EMPTY, Cardinality.ZERO, "Empty sequence")
        ),
    };

    public Upsert(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // Get connection details
        String clusterId = args[0].itemAt(0).getStringValue();
        CouchbaseClusterManager.getInstance().validate(clusterId);
        
        // Retrieve other parameters
        String bucketName = (args[1].isEmpty()) 
                ? null 
                : args[1].itemAt(0).getStringValue();
        
        String docName = args[2].itemAt(0).getStringValue();
        String payload = args[3].itemAt(0).getStringValue();
            
        // Retrieve access to cluster
        CouchbaseCluster cluster = CouchbaseClusterManager.getInstance().get(clusterId);
           
        try {
            // Prepare input
            JsonObject jsonObject = ConversionTools.convert(payload);
            JsonDocument jsonDocument = JsonDocument.create(docName, jsonObject);
            
            // Perform action
            JsonDocument result = isCalledAs(UPSERT) 
                    ? upsert(cluster, bucketName, jsonDocument) 
                    : insert(cluster, bucketName, jsonDocument);
            
            // Return results
            return new StringValue(ConversionTools.convert(result.content()));
            
        } catch(CouchbaseException ex){
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, COBA0010, ex.getMessage());
        
        } catch (Throwable ex){
            // TODO detailed error handling
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);
        }
        
    }
    
    private JsonDocument upsert(CouchbaseCluster cluster, String bucketName, JsonDocument jsonDocument){
        return StringUtils.isBlank(bucketName) 
                    ? cluster.openBucket().upsert(jsonDocument)
                    : cluster.openBucket(bucketName).upsert(jsonDocument);
    }
    
     private JsonDocument insert(CouchbaseCluster cluster, String bucketName, JsonDocument jsonDocument){
        return StringUtils.isBlank(bucketName) 
                    ? cluster.openBucket().insert(jsonDocument)
                    : cluster.openBucket(bucketName).insert(jsonDocument);
    }
}
