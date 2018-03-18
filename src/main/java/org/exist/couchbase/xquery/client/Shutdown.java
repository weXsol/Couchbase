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
package org.exist.couchbase.xquery.client;


import org.exist.couchbase.shared.Constants;
import org.exist.couchbase.shared.CouchbaseClusterManager;
import org.exist.couchbase.shared.GenericExceptionHandler;
import org.exist.couchbase.xquery.CouchbaseModule;
import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.util.List;

/**
 * Shutdown all connections
 *
 * @author Dannes Wessels
 */
public class Shutdown extends BasicFunction {


    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("shutdown", CouchbaseModule.NAMESPACE_URI, CouchbaseModule.PREFIX),
                    "Close and shutdown all Couchbase connections.",
                    new SequenceType[]{

                    },
                    new FunctionReturnSequenceType(Type.STRING, Cardinality.ZERO_OR_MORE, "IDs of closed down connections")
            ),
    };

    public Shutdown(final XQueryContext context, final FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(final Sequence[] args, final Sequence contextSequence) throws XPathException {

        // User must either be DBA or in the c group
        if (!context.getSubject().hasDbaRole() && !context.getSubject().hasGroup(Constants.COUCHBASE_GROUP)) {
            final String txt = String.format("Permission denied, user '%s' must be a DBA or be in group '%s'",
                    context.getSubject().getName(), Constants.COUCHBASE_GROUP);
            LOG.error(txt);
            throw new XPathException(this, CouchbaseModule.COBA0003, txt);
        }


        try {
            // Perform action
            final List<String> ids = CouchbaseClusterManager.getInstance().shutdownAll();

            // Bundle results
            final Sequence retVal = new ValueSequence();
            for (final String id : ids) {

                retVal.add(new StringValue(id));
            }

            // Return IDs
            return retVal;

        } catch (final Throwable ex) {
            return GenericExceptionHandler.handleException(this, ex);
        }

    }
}
