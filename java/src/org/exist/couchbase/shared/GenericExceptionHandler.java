package org.exist.couchbase.shared;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.java.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.error.ViewDoesNotExistException;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.xquery.Expression;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Sequence;

/**
 *  Handle couchbase exceptions in a generic way by translating them into an existdb XpathExeption
 * @author Dannes Wessels
 */


public class GenericExceptionHandler {
    
    protected final static Logger LOG = Logger.getLogger(GenericExceptionHandler.class);
    
    /**
     *  Process the exception thrown by the Couchbase driver.
     * 
     * @param expression The current xpath expression
     * @param throwable The Exception
     * @throws XPathException The translated eXistdb exception
     */
    public static Sequence handleException(Expression expression, Throwable throwable) throws XPathException {

        LOG.error(throwable.getMessage(), throwable);

        if(throwable instanceof XPathException){
            throw (XPathException) throwable;
            
        } else if(throwable instanceof IllegalArgumentException){
            throw new XPathException(expression, CouchbaseModule.COBA0013, throwable);
            
        } else if (throwable instanceof BackpressureException) {

            throw new XPathException(expression, CouchbaseModule.COBA0005, throwable);

        } else if (throwable instanceof RequestCancelledException) {

            throw new XPathException(expression, CouchbaseModule.COBA0006, throwable);

        } else if (throwable instanceof TemporaryFailureException) {

            throw new XPathException(expression, CouchbaseModule.COBA0007, throwable);

        } else if (throwable instanceof CouchbaseOutOfMemoryException) {

            throw new XPathException(expression, CouchbaseModule.COBA0008, throwable);

        } else if (throwable instanceof ViewDoesNotExistException) {

            throw new XPathException(expression, CouchbaseModule.COBA0009, throwable);

        } else if (throwable instanceof DocumentAlreadyExistsException) {

            throw new XPathException(expression, CouchbaseModule.COBA0010, throwable);

        } else if (throwable instanceof DocumentDoesNotExistException) {

            throw new XPathException(expression, CouchbaseModule.COBA0011, throwable);

        } else if (throwable instanceof RequestTooBigException) {

            throw new XPathException(expression, CouchbaseModule.COBA0012, throwable);

        } else if (throwable instanceof TimeoutException) {

            throw new XPathException(expression, CouchbaseModule.COBA0002, throwable);

        } else if (throwable instanceof TranscodingException) {

            throw new XPathException(expression, CouchbaseModule.COBA0003, throwable);

        } else if (throwable instanceof CouchbaseException) {

            throw new XPathException(expression, CouchbaseModule.COBA0004, throwable);

        } else {

            throw new XPathException(expression, CouchbaseModule.COBA0000, throwable);
        }
        
    }
    
}
