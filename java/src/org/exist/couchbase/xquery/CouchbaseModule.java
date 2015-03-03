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
package org.exist.couchbase.xquery;

import java.util.List;
import java.util.Map;
import org.exist.couchbase.xquery.bucket.Get;
import org.exist.couchbase.xquery.bucket.Query;
import org.exist.couchbase.xquery.bucket.Remove;
import org.exist.couchbase.xquery.bucket.Upsert;
import org.exist.couchbase.xquery.client.Close;
import org.exist.couchbase.xquery.client.Connect;
import org.exist.couchbase.xquery.client.ListClusterIds;
import org.exist.couchbase.xquery.clustermanager.InsertBucket;
import org.exist.couchbase.xquery.clustermanager.ListBuckets;
import org.exist.couchbase.xquery.clustermanager.RemoveBucket;
import org.exist.dom.QName;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

public class CouchbaseModule extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/couchbase/db";
    public final static String PREFIX = "couchbase";
    public final static String INCLUSION_DATE = "2015-03-01";
    public final static String RELEASED_IN_VERSION = "eXist-2.2";

    public final static FunctionDef[] functions = { 
        new FunctionDef(Close.signatures[0], Close.class),
        new FunctionDef(Connect.signatures[0], Connect.class),
        new FunctionDef(ListClusterIds.signatures[0], ListClusterIds.class), 
        new FunctionDef(Get.signatures[0], Get.class), 
        new FunctionDef(Remove.signatures[0], Remove.class), 
        new FunctionDef(Upsert.signatures[0], Upsert.class), 
        new FunctionDef(Upsert.signatures[1], Upsert.class), 
        new FunctionDef(Query.signatures[0], Query.class), 
        new FunctionDef(InsertBucket.signatures[0], InsertBucket.class), 
        new FunctionDef(RemoveBucket.signatures[0], RemoveBucket.class), 
        new FunctionDef(ListBuckets.signatures[0], ListBuckets.class), 
    };
    
    public final static ErrorCode COBA0001 = new CouchbaseErrorCode("COBA0001", "Forbidden");
//    public final static ErrorCode MONG0002 = new MongodbErrorCode("COBA0002", "Mongodb exception");
//    public final static ErrorCode MONG0003 = new MongodbErrorCode("COBA0003", "Generic exception");
//    public final static ErrorCode MONG0004 = new MongodbErrorCode("COBA0004", "JSON Syntax exception");
//    public final static ErrorCode MONG0005 = new MongodbErrorCode("COBA0005", "Command exception");
    
    public final static ErrorCode COBA0010 = new CouchbaseErrorCode("COBA0001", "Document already exists");

    public final static QName EXCEPTION_QNAME
            = new QName("exception", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX);

    public final static QName EXCEPTION_MESSAGE_QNAME
            = new QName("exception-message", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX);


    public CouchbaseModule(Map<String, List<? extends Object>> parameters) throws XPathException {
        super(functions, parameters);
    }

    @Override
    public String getNamespaceURI() {
        return NAMESPACE_URI;
    }

    @Override
    public String getDefaultPrefix() {
        return PREFIX;
    }

    @Override
    public String getDescription() {
        return "CouchBase module";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
    
    protected final static class CouchbaseErrorCode extends ErrorCode {

        public CouchbaseErrorCode(String code, String description) {
            super(new QName(code, NAMESPACE_URI, PREFIX), description);
        }

    }
}
