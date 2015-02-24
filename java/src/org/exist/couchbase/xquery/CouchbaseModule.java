
package org.exist.couchbase.xquery;

import java.util.List;
import java.util.Map;
import org.exist.couchbase.xquery.client.Close;
import org.exist.couchbase.xquery.client.Connect;
import org.exist.couchbase.xquery.client.ListClusterIds;
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
    };
    
    public final static ErrorCode COBA0001 = new CouchbaseErrorCode("COBA0001", "Forbidden");
//    public final static ErrorCode MONG0002 = new MongodbErrorCode("COBA0002", "Mongodb exception");
//    public final static ErrorCode MONG0003 = new MongodbErrorCode("COBA0003", "Generic exception");
//    public final static ErrorCode MONG0004 = new MongodbErrorCode("COBA0004", "JSON Syntax exception");
//    public final static ErrorCode MONG0005 = new MongodbErrorCode("COBA0005", "Command exception");

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
