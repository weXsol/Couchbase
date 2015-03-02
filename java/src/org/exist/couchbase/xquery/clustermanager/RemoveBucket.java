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


import com.couchbase.client.java.cluster.ClusterManager;
import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 *  Remove bucket
 *
 * @author Dannes Wessels
 */
public class RemoveBucket extends BasicFunction {
    

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("remove-bucket", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
            "Remove bucket",
            new SequenceType[]{
                new FunctionParameterSequenceType("clusterId", Type.STRING, Cardinality.ONE, "Couchbase clusterId"),
                new FunctionParameterSequenceType("bucket", Type.STRING, Cardinality.ZERO_OR_ONE, "Name of bucket, empty sequence for default bucket"),
                new FunctionParameterSequenceType("username", Type.STRING, Cardinality.ONE, "Username"),    
                new FunctionParameterSequenceType("password", Type.STRING, Cardinality.ONE, "Password"),   
            },
            new FunctionReturnSequenceType(Type.BOOLEAN, Cardinality.ONE, "true() if the removal was successful, false() otherwise")
        ),
    };

    public RemoveBucket(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

//        // User must either be DBA or in the c group
//        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.COUCHBASE_GROUP)) {
//            String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
//                    context.getSubject().getName(), Constants.COUCHBASE_GROUP);
//            LOG.error(txt);
//            throw new XPathException(this, txt);
//        }
        
        // Get connection details
        String clusterId = args[0].itemAt(0).getStringValue();
        CouchbaseClusterManager.getInstance().validate(clusterId);
        
        // Get additional parameters
        String bucketId = args[1].itemAt(0).getStringValue();
        String username = args[2].itemAt(0).getStringValue();
        String password = args[3].itemAt(0).getStringValue();
           
        try {
            // Get reference to cluster manager
            ClusterManager clusterManager = CouchbaseClusterManager.getInstance().get(clusterId).clusterManager(username, password);
            
            // Execute
            Boolean retVal = clusterManager.removeBucket(bucketId);
            
            // Return results
            return new BooleanValue(retVal);
        
        } catch (Exception ex){
            // TODO detailed error handling
            LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex.getMessage(), ex);
        }
        
    }
}
