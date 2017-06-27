/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2015 The eXist Project
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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

import static org.exist.couchbase.xquery.CouchbaseModule.COBA0051;

/**
 * Convert Couchbase's JSON object to eXist-db's Map
 *
 * @author wessels
 */
public class JsonToMap {

    /**
     * Convert the CouchBase JSON object into an eXistdb MapType
     *
     * @param json    The couchbase JSON object
     * @param context XQuery context
     * @return eXist-db map representing the JSON object
     * @throws XPathException The conversion failed.
     */
    public static MapType convert(final JsonObject json, final XQueryContext context) throws XPathException {

        return convertJsonObject(json, context);
    }

    /**
     * Convert the Couchbase JSON array into an eXist-db Array type,
     *
     * @param jsonObject The Json Object
     * @param context    Xquery context
     * @return The eXist-db representation of the jsonObject
     * @throws XPathException The conversion can not be performed.
     */
    private static MapType convertJsonObject(final JsonObject jsonObject, final XQueryContext context) throws XPathException {

        final MapType result = new MapType(context);

        for (final String name : jsonObject.getNames()) {
            final Object obj = jsonObject.get(name);

            if (obj == null) {
                result.add(new StringValue(name), Sequence.EMPTY_SEQUENCE);

            } else if (obj instanceof JsonObject) {
                final JsonObject jo = (JsonObject) obj;
                result.add(new StringValue(name), convertJsonObject(jo, context));

            } else if (obj instanceof JsonArray) {
                final JsonArray ja = (JsonArray) obj;
                final ArrayType array = convertJsonArray(ja, context);
                result.add(new StringValue(name), array);

            } else if (obj instanceof HashMap) {
                final HashMap map = (HashMap) obj;
                final JsonObject jo = JsonObject.from(map);
                result.add(new StringValue(name), convertJsonObject(jo, context));

            } else {
                result.add(new StringValue(name), convertToSequence(obj, context));
            }
        }

        return result;

    }

    /**
     * Convert the Couchbase JSON array into an eXist-db Array type,
     *
     * @param ja      The Json Array
     * @param context Xquery context
     * @return The eXist-db representation of the array
     * @throws XPathException The conversion can not be performed.
     */
    private static ArrayType convertJsonArray(final JsonArray ja, final XQueryContext context) throws XPathException {

        final Sequence sequence = new ValueSequence();

        for (final Object obj : ja.toList()) {

            if (obj instanceof JsonObject) {
                final JsonObject jo = (JsonObject) obj;
                sequence.add(convertJsonObject(jo, context));

            } else if (obj instanceof JsonArray) {
                final JsonArray newarray = (JsonArray) obj;
                sequence.add(convertJsonArray(newarray, context));

            } else if (obj instanceof HashMap) {
                final HashMap map = (HashMap) obj;
                final JsonObject jo = JsonObject.from(map);
                sequence.add(convertJsonObject(jo, context));

            } else if (obj instanceof ArrayList) {
                final Sequence tmpSequence = new ValueSequence();
                final ArrayList<Object> al = (ArrayList) obj;

                for (final Object o : al.toArray()) {
                    tmpSequence.addAll(convertToSequence(o, context));
                }

                sequence.add(new ArrayType(context, tmpSequence));

            } else if (obj == null) {
                sequence.add(AtomicValue.EMPTY_VALUE);

            } else {
                sequence.addAll(convertToSequence(obj, context));
            }

        }

        return new ArrayType(context, sequence);

    }

    /**
     * Convert an object to the eXist-db equivalent, when possible.
     *
     * @param obj     The to be converted java object
     * @param context Xquery context
     * @return The eXist-db representation of the object
     * @throws XPathException The conversion can not be performed.
     */
    private static Sequence convertToSequence(final Object obj, final XQueryContext context) throws XPathException {

        if (obj instanceof String) {
            return new StringValue((String) obj);

        } else if (obj instanceof Integer) {
            return new IntegerValue((Integer) obj);

        } else if (obj instanceof Long) {
            return new IntegerValue((Long) obj);

        } else if (obj instanceof Double) {
            return new DoubleValue((Double) obj);

        } else if (obj instanceof Boolean) {
            return new BooleanValue((Boolean) obj);

        } else if (obj instanceof BigInteger) {
            return new IntegerValue((BigInteger) obj);

        } else if (obj instanceof BigDecimal) {
            return new DecimalValue((BigDecimal) obj);

        } else if (obj instanceof Float) {
            return new FloatValue((Float) obj);

//        } else if (obj instanceof Map) {


        } else if (obj instanceof ArrayList) {
            // Special case
            final Sequence sequence = new ValueSequence();

            final ArrayList<Object> al = (ArrayList) obj;

            for (final Object o : al.toArray()) {
                sequence.addAll(convertToSequence(o, context));
            }

            return new ArrayType(context, sequence);

        } else if (obj == null) {
            return Sequence.EMPTY_SEQUENCE;
        }

        throw new XPathException(COBA0051, String.format("Cannot convert '%s' to an eXistdb type. %s", obj, obj.getClass().getCanonicalName()));

    }

}
