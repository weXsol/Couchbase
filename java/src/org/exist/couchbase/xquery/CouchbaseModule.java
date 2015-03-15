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
    
    /** Generic error */
    public final static ErrorCode COBA0000 
            = new CouchbaseErrorCode("COBA0000", "Generic exception.");  
     
    /** Forbidden: The provided Couchbase clusterId is not valid */
    public final static ErrorCode COBA0001 
            = new CouchbaseErrorCode("COBA0001", "Forbidden: the clusterId is not valid.");    
    
    /** IllegalArgumentException: invalid value */
    public final static ErrorCode COBA0002 
            = new CouchbaseErrorCode("COBA0002", "IllegalArgumentException: invalid value.");
    
    /*
     *  Below are the Couchbase specific errors/exceptions
     */
    
    /** CouchbaseException: the underlying resources could not be enabled properly */
    public final static ErrorCode COBA0010 
            = new CouchbaseErrorCode("COBA0010", "CouchbaseException: generic Couchbase exception.");
    
    /** TimeoutException: the timeout is exceeded */
    public final static ErrorCode COBA0011 
            = new CouchbaseErrorCode("COBA0011", "TimeoutException: the timeout is exceeded.");
    
    /** BackpressureException: the incoming request rate is too high to be processed */
    public final static ErrorCode COBA0012 
            = new CouchbaseErrorCode("COBA0012", "BackpressureException: the incoming request rate is too high to be processed.");
  
    /** RequestCancelledException: The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of retrying */
    public final static ErrorCode COBA0013 
            = new CouchbaseErrorCode("COBA0013", "RequestCancelledException: The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of retrying.");

    /** TemporaryFailureException: The server is currently not able to process the request, retrying may help */
    public final static ErrorCode COBA0014 
            = new CouchbaseErrorCode("COBA0014", "TemporaryFailureException: The server is currently not able to process the request, retrying may help.");
    
    /** CouchbaseOutOfMemoryException: The server is out of memory */
    public final static ErrorCode COBA0015 
            = new CouchbaseErrorCode("COBA0015", "CouchbaseOutOfMemoryException: The server is out of memory.");
    
     /** ViewDoesNotExistException: the design document or view is not found */
    public final static ErrorCode COBA0016 
            = new CouchbaseErrorCode("COBA0016", "ViewDoesNotExistException: the design document or view is not found.");
 
    /** DocumentAlreadyExistsException: the document already exists */
    public final static ErrorCode COBA0017
            = new CouchbaseErrorCode("COBA0017", "DocumentAlreadyExistsException: the document already exists.");
     
    /** DocumentDoesNotExistException: the document does not exist */
    public final static ErrorCode COBA0018 
            = new CouchbaseErrorCode("COBA0018", "DocumentDoesNotExistException: the document does not exist.");
    
    /** RequestTooBigException: the request content is too big */
    public final static ErrorCode COBA0019 
            = new CouchbaseErrorCode("COBA0019", "RequestTooBigException: the request content is too big.");
    
    /** TranscodingException: the server response could not be decoded */
    public final static ErrorCode COBA0020
            = new CouchbaseErrorCode("COBA0020", "TranscodingException: the server response could not be decoded.");
    

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
