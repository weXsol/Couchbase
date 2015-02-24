/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
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
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.lang3.StringUtils;
import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.ConversionTools;
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
import org.exist.xquery.value.ValueSequence;

/**
 *  Retrieve document
 *
 * @author Dannes Wessels
 */
public class Query extends BasicFunction {
    

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("query", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
            "Query a view with the default view timeout.",
            new SequenceType[]{
                new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                new FunctionParameterSequenceType("design", Type.STRING, Cardinality.ONE, "Name of design document"),   
                new FunctionParameterSequenceType("view", Type.STRING, Cardinality.ONE, "Name of view"),   
            },
            new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "Results of query, JSON formatted.")
        ),
    };

    public Query(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the c group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.COUCHBASE_GROUP)) {
            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.COUCHBASE_GROUP);
            LOG.error(txt);
            throw new XPathException(this, txt);
        }
        
        // Get connection details
        String clusterId = args[0].itemAt(0).getStringValue();
        CouchbaseClusterManager.getInstance().validate(clusterId);
        
        // Retrieve other parameters             
        String bucketName = (args[1].isEmpty()) 
                ? null 
                : args[1].itemAt(0).getStringValue();
        
        String design = args[2].itemAt(0).getStringValue();
        String view = args[3].itemAt(0).getStringValue();
            
        // Retrieve access to cluster
        CouchbaseCluster cluster = CouchbaseClusterManager.getInstance().get(clusterId);
           
        try {           
            // Prepare query
            ViewQuery viewQuery = ViewQuery.from(design, view);
            
            // Perform action
            ViewResult result = StringUtils.isBlank(bucketName) 
                    ? cluster.openBucket().query(viewQuery)
                    : cluster.openBucket(bucketName).query(viewQuery);
            
            // Return results
            ValueSequence retVal = new ValueSequence();
            
            for(ViewRow row : result){
                retVal.add(new StringValue(ConversionTools.convert(row.document().content())));
            }
            
            return retVal;
        
        } catch (Exception ex){
            // TODO detailed error handling
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);
        }
        
    }
}
