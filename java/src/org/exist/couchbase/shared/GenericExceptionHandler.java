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
package org.exist.couchbase.shared;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.java.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.InvalidPasswordException;
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
 * 
 * @author Dannes Wessels
 */


public class GenericExceptionHandler {
    
    protected final static Logger LOG = Logger.getLogger(GenericExceptionHandler.class);
    
    /**
     *  Process the exception thrown by the Couchbase driver.
     * 
     * @param expr The current xpath expression
     * @param throwable The Exception
     * @throws XPathException The translated eXistdb exception
     * 
     * @return Nothing, there will always be an exception thrown.
     */
    public static Sequence handleException(Expression expr, Throwable throwable) throws XPathException {

        if(LOG.isDebugEnabled()){
            LOG.error(throwable.getMessage());
        } else {
            LOG.error(throwable.getMessage(), throwable);
        }

        if(throwable instanceof XPathException){
            throw (XPathException) throwable;
            
        } else if(throwable instanceof IllegalArgumentException){
            throw new XPathException(expr, CouchbaseModule.COBA0002, throwable.getMessage());
            
        } else if (throwable instanceof TimeoutException) {

            throw new XPathException(expr, CouchbaseModule.COBA0011, throwable);
            
        } else if (throwable instanceof BackpressureException) {

            throw new XPathException(expr, CouchbaseModule.COBA0012, throwable);

        } else if (throwable instanceof RequestCancelledException) {

            throw new XPathException(expr, CouchbaseModule.COBA0013, throwable);

        } else if (throwable instanceof TemporaryFailureException) {

            throw new XPathException(expr, CouchbaseModule.COBA0014, throwable);

        } else if (throwable instanceof CouchbaseOutOfMemoryException) {

            throw new XPathException(expr, CouchbaseModule.COBA0015, throwable);

        } else if (throwable instanceof ViewDoesNotExistException) {

            throw new XPathException(expr, CouchbaseModule.COBA0016, throwable);

        } else if (throwable instanceof DocumentAlreadyExistsException) {

            throw new XPathException(expr, CouchbaseModule.COBA0017, throwable);

        } else if (throwable instanceof DocumentDoesNotExistException) {

            throw new XPathException(expr, CouchbaseModule.COBA0018, throwable);

        } else if (throwable instanceof RequestTooBigException) {

            throw new XPathException(expr, CouchbaseModule.COBA0019, throwable);

        } else if (throwable instanceof TranscodingException) {

            throw new XPathException(expr, CouchbaseModule.COBA0020, throwable);
            
        } else if (throwable instanceof InvalidPasswordException) {

            throw new XPathException(expr, CouchbaseModule.COBA0021, throwable.getMessage());
            
        } else if (throwable instanceof CouchbaseException) {

            throw new XPathException(expr, CouchbaseModule.COBA0010, throwable);

        } else {

            throw new XPathException(expr, CouchbaseModule.COBA0000, throwable);
        }
        
    }
    
}
