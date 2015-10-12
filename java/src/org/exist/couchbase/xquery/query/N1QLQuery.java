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
package org.exist.couchbase.xquery.query;

import com.couchbase.client.java.CouchbaseCluster; 
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.shared.GenericExceptionHandler;
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
import org.exist.xquery.value.ValueSequence;


/**
 * Implementation of the Couchbase N1QL query (experimental!)
 *
 * @author Dannes Wessels
 */
public class N1QLQuery extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
        new QName("n1ql", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
        "Execute a N1QL query (experimental).",
        new SequenceType[]{
            new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
            new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
            new FunctionParameterSequenceType("query", Type.STRING, Cardinality.ONE, "N1QL query")        
        },
        new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "Results of query, JSON formatted.")
        ),};
    
    public N1QLQuery(XQueryContext context, FunctionSignature signature) {
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
        String bucketPassword = cmm.getBucketPassword(clusterId);
        
        String query = args[2].itemAt(0).getStringValue();
        
        try {
            // Prepare query
            N1qlQuery viewQuery = N1qlQuery.simple(query);
            
            // Perform action
            N1qlQueryResult result = cluster.openBucket(bucketName, bucketPassword).query(viewQuery);
            
            LOG.info(result.info().asJsonObject().toString());

            // Return results
            ValueSequence retVal = new ValueSequence();
            
            for(N1qlQueryRow row : result.allRows()){
                retVal.add(new StringValue(row.value().toString()));               
            }
            
            return retVal;
            
        } catch (Throwable ex){
            return GenericExceptionHandler.handleException(this, ex);           
        }
        
    }
    
 
    
}
