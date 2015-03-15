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


import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.exist.couchbase.shared.ConversionTools;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.shared.GenericExceptionHandler;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.EmptySequence;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 *  Retrieve document
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
                new FunctionParameterSequenceType("documentName", Type.STRING, Cardinality.ONE, "Name of document"),               
            },
            new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The document, or Empty sequence when not found.")
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
            new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_ONE, "The document, or Empty sequence when not found.")
        ),
    };

    public Get(XQueryContext context, FunctionSignature signature) {
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
        
        Map<String, Object> parameters = (getArgumentCount() > 3)
                ? ConversionTools.convert((AbstractMapType) args[4].itemAt(0))
                : null;
            
        // Retrieve access to cluster
        CouchbaseCluster cluster = CouchbaseClusterManager.getInstance().get(clusterId);
           
        try {           
            // Perform action
            JsonDocument result = (parameters == null)
                    ? get(cluster, bucketName, docName)
                    : get(cluster, bucketName, docName, parameters);
            
            
            if(result == null){
                return EmptySequence.EMPTY_SEQUENCE;
            }
            
            // Return results
            return new StringValue(ConversionTools.convert(result.content()));
        
        } catch (Throwable ex){
            return GenericExceptionHandler.handleException(this, ex);           
        }
        
    }
    
    private JsonDocument get(CouchbaseCluster cluster, String bucketName, String docName){
        return StringUtils.isBlank(bucketName) 
                    ? cluster.openBucket().get(docName)
                    : cluster.openBucket(bucketName).get(docName);
    }
    
     private JsonDocument get(CouchbaseCluster cluster, String bucketName, String docName, Map<String, Object> parameters){
         
        long timeout = ConversionTools.getLongValue("timeout", parameters.get("timeout"));
        TimeUnit timeUnit = TimeUnit.valueOf( parameters.get("timeUnit").toString().toUpperCase());
         
        return StringUtils.isBlank(bucketName) 
                    ? cluster.openBucket().get(docName, timeout, timeUnit)
                    : cluster.openBucket(bucketName).get(docName,  timeout, timeUnit);
    }
     
}
