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
import com.couchbase.client.java.document.json.JsonNull;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.*;

import java.math.BigDecimal;

import static org.exist.couchbase.xquery.CouchbaseModule.COBA0051;

/**
 * @author wessels
 */
public class MapToJson {

    protected final static Logger LOG = LogManager.getLogger(MapToJson.class);

    private static final JsonTranscoder transcoder = new JsonTranscoder();

    /**
     * Convert JSON as Item to object
     *
     * @param seq JSON formatted text document
     * @return JSON Object representation
     * @throws Exception When something bad happens during the JSON conversion.
     */
    public static JsonValue convert(final Sequence seq) throws Exception {

        final JsonValue result;

        switch (seq.getItemType()) {
            case Type.STRING:
                result = transcoder.stringToJsonObject(seq.getStringValue());
                break;

            case Type.MAP:
                result = convertMap((MapType) seq);
                break;

            default:
                throw new IllegalArgumentException(
                        String.format("Can only convert String or a Map to a Couchbase JSON object. Got type `%s` with value `%s`.",
                                seq.getItemType(), seq.getStringValue()));
        }

        return result;

    }


    /*
     *  ArrayType to JSON Array conversion
     */
    private static JsonArray convertArray(final ArrayType xqueryArray) throws XPathException {

        final JsonArray jsonArray = JsonValue.ja();

        for (final Sequence subSeq : xqueryArray.toArray()) {
            jsonArray.add(convertSequence(subSeq));

        }

        return jsonArray;
    }


    /*
     * Need Object here because of conversion
     */
    private static Object convertSequence(final Sequence seq) throws XPathException {
        switch (seq.getItemType()) {

            case Type.MAP:
                return convertMap((MapType) seq);

            case Type.ARRAY:
                return convertArray((ArrayType) seq);

            case Type.EMPTY:
                return JsonNull.INSTANCE;

            default:
                // try to convert value
                return convertSequenceToJavaObject(seq);
        }

    }


    /*
     *  MapType to JSON object conversion
     */
    private static JsonObject convertMap(final MapType map) throws XPathException {

        final JsonObject jo = JsonValue.jo();

        // Get all keys
        final Sequence keys = map.keys();

        // Iterate over all keys
        for (final SequenceIterator i = keys.iterate(); i.hasNext(); ) {

            // Get next item
            final Item key = i.nextItem();

            // Only use Strings as key, as required by JMS
            final String keyValue = key.getStringValue();

            // Get values
            final Sequence sequence = map.get((AtomicValue) key);

            jo.put(keyValue, convertSequence(sequence));

        }
        return jo;
    }

    /**
     * Actual conversion of item to a JSON value.
     *
     * @param sequence The sequence item to be converted.
     * @return The raw Java object of the sequence item.
     * @throws XPathException The conversion failed.
     */
    static Object convertSequenceToJavaObject(final Sequence sequence) throws XPathException {

        final Object retVal;

        switch (sequence.getItemType()) {
            case Type.STRING:
                retVal = ((StringValue) sequence).getStringValue();
                break;
            case Type.INTEGER:
            case Type.INT:
                retVal = sequence.toJavaObject(Integer.class);
                break;
            case Type.DOUBLE:
                retVal = sequence.toJavaObject(Double.class);
                break;
            case Type.BOOLEAN:
                retVal = sequence.toJavaObject(Boolean.class);
                break;
            case Type.FLOAT:
                retVal = sequence.toJavaObject(Float.class);
                break;
            case Type.LONG:
                retVal = sequence.toJavaObject(Long.class);
                break;
            case Type.DECIMAL:
                retVal = sequence.toJavaObject(BigDecimal.class);
                break;
            default:
                final String msg = String.format("Unable to convert '%s' of type '%d' to a Java object.", sequence.getStringValue(), sequence.getItemType());
                LOG.error(msg);
                throw new XPathException(COBA0051, msg);
        }
        return retVal;
    }

}
