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
import com.couchbase.client.java.error.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.xquery.Expression;
import org.exist.xquery.XPathException;
import org.exist.xquery.util.ExpressionDumper;
import org.exist.xquery.value.Sequence;

import java.util.concurrent.TimeoutException;

/**
 * Handle couchbase exceptions in a generic way by translating them into an existdb XpathExeption
 *
 * @author Dannes Wessels
 */


public class GenericExceptionHandler {

    protected final static Logger LOG = LogManager.getLogger(GenericExceptionHandler.class);

    /**
     * Process the exception thrown by the Couchbase driver.
     *
     * @param expr      The current xpath expression
     * @param throwable The Exception
     * @return Nothing, there will always be an exception thrown.
     * @throws XPathException The translated eXistdb exception
     */
    public static Sequence handleException(Expression expr, Throwable throwable) throws XPathException {

        if (LOG.isDebugEnabled()) {
            LOG.error("Expression='{}' Source={}#{}", ExpressionDumper.dump(expr), expr.getSource(), expr.getLine(), throwable);
        } else {
            LOG.error("{} Origin={}  Expression='{}'  Source={}#{}", throwable.toString(), throwable.getStackTrace()[0].toString(), ExpressionDumper.dump(expr), expr.getSource(), expr.getLine());
        }

        if (throwable instanceof XPathException) {
            throw (XPathException) throwable;

        } else if (throwable instanceof IllegalArgumentException) {

            throw new XPathException(expr, CouchbaseModule.COBA0002, throwable.getMessage());

        } else if (throwable instanceof TimeoutException) {

            throw new XPathException(expr, CouchbaseModule.COBA0011, throwable.getMessage());

        } else if (throwable instanceof BackpressureException) {

            throw new XPathException(expr, CouchbaseModule.COBA0012, throwable.getMessage());

        } else if (throwable instanceof RequestCancelledException) {

            throw new XPathException(expr, CouchbaseModule.COBA0013, throwable.getMessage());

        } else if (throwable instanceof TemporaryFailureException) {

            throw new XPathException(expr, CouchbaseModule.COBA0014, throwable.getMessage());

        } else if (throwable instanceof CouchbaseOutOfMemoryException) {

            throw new XPathException(expr, CouchbaseModule.COBA0015, throwable.getMessage());

        } else if (throwable instanceof ViewDoesNotExistException) {

            throw new XPathException(expr, CouchbaseModule.COBA0016, throwable.getMessage());

        } else if (throwable instanceof DocumentAlreadyExistsException) {

            throw new XPathException(expr, CouchbaseModule.COBA0017, throwable.getMessage() /*, throwable */);

        } else if (throwable instanceof DocumentDoesNotExistException) {

            throw new XPathException(expr, CouchbaseModule.COBA0018, throwable.getMessage());

        } else if (throwable instanceof RequestTooBigException) {

            throw new XPathException(expr, CouchbaseModule.COBA0019, throwable.getMessage());

        } else if (throwable instanceof TranscodingException) {

            throw new XPathException(expr, CouchbaseModule.COBA0020, throwable.getMessage());

        } else if (throwable instanceof InvalidPasswordException) {

            throw new XPathException(expr, CouchbaseModule.COBA0021, throwable.getMessage());

        } else if (throwable instanceof DesignDocumentAlreadyExistsException) {

            throw new XPathException(expr, CouchbaseModule.COBA0030, throwable.getMessage());

        } else if (throwable instanceof DesignDocumentDoesNotExistException) {

            throw new XPathException(expr, CouchbaseModule.COBA0032, throwable.getMessage());

        } else if (throwable instanceof DesignDocumentException) {

            throw new XPathException(expr, CouchbaseModule.COBA0031, throwable.getMessage());

        } else if (throwable instanceof CouchbaseException) {

            throw new XPathException(expr, CouchbaseModule.COBA0010, throwable);

        } else {

            if (!LOG.isDebugEnabled()) {
                LOG.error("Generic issue", throwable);
            }

            throw new XPathException(expr, CouchbaseModule.COBA0000, throwable);
        }

    }

}
