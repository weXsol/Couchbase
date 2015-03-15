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
package org.exist.couchbase.xquery.clustermanager;


import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.cluster.DefaultBucketSettings.Builder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 *  Insert bucket
 *
 * @author Dannes Wessels
 */
public class InsertBucket extends BasicFunction {
    

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("insert-bucket", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
            "Remove bucket",
            new SequenceType[]{
                new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                new FunctionParameterSequenceType("username", Type.STRING, Cardinality.ONE, "Clustermanager username"),    
                new FunctionParameterSequenceType("password", Type.STRING, Cardinality.ONE, "Clusermanager password"),  
                new FunctionParameterSequenceType("parameters", Type.MAP, Cardinality.ZERO_OR_ONE, "Bucket parameters: enableFlush indexReplicas password port quota replicas type.")
            },
            new FunctionReturnSequenceType(Type.BOOLEAN, Cardinality.ONE, "true() if the removal was successful, false() otherwise")
        ),
    };

    public InsertBucket(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
        // Get connection details
        String clusterId = args[0].itemAt(0).getStringValue();
        CouchbaseClusterManager.getInstance().validate(clusterId);
        
        // Get additional parameters
        String bucketId = args[1].itemAt(0).getStringValue();
        String username = args[2].itemAt(0).getStringValue();
        String password = args[3].itemAt(0).getStringValue();
        Map<String, Object> parameters = (args[4].isEmpty())
                ? new HashMap<String, Object>()
                : ConversionTools.convert((AbstractMapType) args[4].itemAt(0));
           
        try {
            // Get reference to cluster manager
            ClusterManager clusterManager = CouchbaseClusterManager.getInstance().get(clusterId).clusterManager(username, password);
            
            // Get configuaration
            BucketSettings bucketSettings = parseParameters(bucketId, parameters);
            
            // Execute
            BucketSettings insertBucket = clusterManager.insertBucket(bucketSettings);
            
            // Return results
            return new StringValue(insertBucket.toString());
            
     
        } catch (Throwable ex){
            return GenericExceptionHandler.handleException(this, ex);           
        }
    }
        
    private BucketSettings parseParameters(String bucketName, Map<String, Object> parameters) throws XPathException {
        
        Builder builder = DefaultBucketSettings.builder().name(bucketName);
        
        for (Entry<String, Object> entry : parameters.entrySet()) {
            
            String key = entry.getKey();
            Object value = entry.getValue(); // check for empty sequence?
            
            switch (key) {
                case "enableFlush":
                    builder.enableFlush(ConversionTools.getBooleanValue(key, value, false));
                    break;
                case "indexReplicas":
                    builder.indexReplicas(ConversionTools.getBooleanValue(key, value, false));
                    break;
                case "password":
                    builder.password(value.toString());
                    break;
                case "port":
                    builder.port(ConversionTools.getIntegerValue(key, value, 0));
                    break;
                case "quota":
                    builder.quota(ConversionTools.getIntegerValue(key, value, 0));
                    break;
                case "replicas":
                    builder.replicas(ConversionTools.getIntegerValue(key, value, 0));
                    break;
                case "type":
                    builder.type(BucketType.valueOf(value.toString().toUpperCase()));
                    break;
                default:
                    throw new IllegalArgumentException(String.format("'%s' is not a valid parameter.", key));
            }
            
        } 
        
        return builder;
        
    
    }
}
