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
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

/**
 *
 * @author wessels
 */
public class MapToJson {

    protected final static Logger LOG = LogManager.getLogger(MapToJson.class);

    private static final JsonTranscoder transcoder = new JsonTranscoder();

    /**
     * Convert JSON as Item to object
     *
     * @param document JSON formatted text document
     * @return JSON Object representation
     * @throws Exception When something bad happens during the JSON conversion.
     */
    public static JsonValue convert(Sequence document) throws Exception {

        JsonValue result;

        switch (document.getItemType()) {
            case Type.STRING:
                result = transcoder.stringToJsonObject(document.getStringValue());
                break;
            case Type.MAP:
                JsonObject jo = JsonValue.jo();
                result = convertItem(document, jo);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Can only convert String, Map or Array. Got type `%s` with value `%s`.",
                                document.getItemType(), document.getStringValue()));
        }

        return result;

    }

    private static JsonValue convertItem(Sequence sequence, JsonValue jsonValue) throws XPathException {

        JsonValue retVal = null;

        switch (sequence.getItemType()) {
            case Type.MAP:
                retVal = convertMap(jsonValue, sequence);
                break;
            case Type.ARRAY:
                retVal = convertArray(sequence);
                break;
        }
        return retVal;

    }

    private static JsonArray convertArray(Sequence sequence) throws XPathException {

        ArrayType xqueryArray = (ArrayType) sequence;
        JsonArray jsonArray = JsonValue.ja();

        for (Sequence subSeq : xqueryArray.toArray()) {

            switch (subSeq.getItemType()) {
                case Type.STRING:
                case Type.INTEGER:
                case Type.DOUBLE:
                case Type.BOOLEAN:
                    jsonArray.add(convertSequenceToJavaObject(sequence));
                    break;
                case Type.MAP:
                    JsonObject newObject = JsonValue.jo();
                    JsonValue newMap = convertItem((MapType) subSeq, newObject);
                    jsonArray.add(newMap);
                    break;
                case Type.ARRAY:
                    JsonArray newObject1 = JsonValue.ja();
                    JsonValue newMap1 = convertItem((ArrayType) subSeq, newObject1);
                    jsonArray.add(newMap1);
                    break;
                case Type.EMPTY:
                    jsonArray.addNull();
                    break;
                default:
                    LOG.error(String.format("Unable to convert '%s'", subSeq.getStringValue()));
            }

        }
        return jsonArray;
    }

    private static JsonObject convertMap(JsonValue in, Sequence seq) throws XPathException {

        JsonObject jo = (JsonObject) in;
        MapType map = (MapType) seq;

        // Get all keys
        Sequence keys = map.keys();

        // Iterate over all keys
        for (final SequenceIterator i = keys.iterate(); i.hasNext();) {

            // Get next item
            Item key = i.nextItem();

            // Only use Strings as key, as required by JMS
            String keyValue = key.getStringValue();

            // Get values
            Sequence sequence = map.get((AtomicValue) key);

            switch (sequence.getItemType()) {
                case Type.STRING:
                case Type.INTEGER:
                case Type.DOUBLE:
                case Type.BOOLEAN:
                    jo.put(keyValue, convertSequenceToJavaObject(sequence));
                    break;
                case Type.MAP:
                    JsonObject newObject = JsonValue.jo();
                    JsonValue newMap = convertItem((MapType) sequence, newObject);
                    jo.put(keyValue, newMap);
                    break;
                case Type.ARRAY:
                    JsonArray newObject1 = JsonValue.ja();
                    JsonValue newMap1 = convertItem((ArrayType) sequence, newObject1);
                    jo.put(keyValue, newMap1);
                    break;
                case Type.EMPTY:
                    jo.putNull(keyValue);
                    break;
                default:
                    LOG.error(String.format("Unable to convert '%s' with value '%s'", keyValue, sequence.getStringValue()));
            }
        }
        return jo;
    }

    static Object convertSequenceToJavaObject(Sequence sequence) throws XPathException {

        Object retVal = null;

        switch (sequence.getItemType()) {
            case Type.STRING:
                retVal = ((StringValue) sequence).getStringValue();
                break;
            case Type.INTEGER:
                retVal = ((IntegerValue) sequence).toJavaObject(Integer.class);
                break;
            case Type.DOUBLE:
                retVal = ((DoubleValue) sequence).toJavaObject(Double.class);
                break;
            case Type.BOOLEAN:
                retVal = ((BooleanValue) sequence).toJavaObject(Boolean.class);
                break;
            default:
                String msg = String.format("Unable to convert '%s'", sequence.getStringValue());
                LOG.error(msg);
                throw new XPathException(msg);
        }
        return retVal;
    }

}
