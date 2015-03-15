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

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.xquery.XPathException;
import org.exist.xquery.functions.map.AbstractMapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.FloatValue;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.ValueSequence;

/**
 * Helper class for converting JSON documents
 *
 * @author Dannes Wessels
 */
public class ConversionTools {
    
    protected final static Logger LOG = Logger.getLogger(ConversionTools.class);

    private static final JsonTranscoder transcoder = new JsonTranscoder();

    /**
     * Convert JSON string to object
     *
     * @param document JSON formatted text document
     * @return JSON Object representation
     * @throws Exception When something bad happens during the JSON conversion.
     */
    public static JsonObject convert(String document) throws Exception {
        return transcoder.stringToJsonObject(document);
    }

    /**
     * Convert JSON string to object
     *
     * @param document JSON object representation
     * @return JSON formatted text document
     * @throws Exception When something bad happens during the JSON conversion.
     */
    public static String convert(JsonObject document) throws Exception {
        return transcoder.jsonObjectToString(document);
    }

   /**
    *  Convert an MAPtype into a easy to use HashMap
    * 
    * @param map The xquery map
    * @return Java hashmap containing the map values
    * 
    * @throws XPathException Something happened during the value conversion
    */
    public static Map<String, Object> convert(AbstractMapType map) throws XPathException {
        
        // Results are stored here
        Map<String, Object> retVal = new HashMap<>();

        // Get all keys
        Sequence keys = map.keys();

        // Iterate over all keys
        for (final SequenceIterator i = keys.unorderedIterator(); i.hasNext();) {

            // Get next item
            Item key = i.nextItem();

            // Only use Strings as key, as required by JMS
            String keyValue = key.getStringValue();

            // Get values
            Sequence values = map.get((AtomicValue) key);

            // Parse data only if the key is a String
            if (values instanceof StringValue) {
                StringValue singlevalue = (StringValue) values;
                retVal.put(keyValue, singlevalue.getStringValue());

            } else if (values instanceof IntegerValue) {
                IntegerValue singleValue = (IntegerValue) values;
                retVal.put(keyValue, singleValue.toJavaObject(Integer.class));

            } else if (values instanceof DoubleValue) {
                DoubleValue singleValue = (DoubleValue) values;
                retVal.put(keyValue, singleValue.toJavaObject(Double.class));

            } else if (values instanceof BooleanValue) {
                BooleanValue singleValue = (BooleanValue) values;
                retVal.put(keyValue, singleValue.toJavaObject(Boolean.class));

            } else if (values instanceof FloatValue) {
                FloatValue singleValue = (FloatValue) values;
                retVal.put(keyValue, singleValue.toJavaObject(Float.class));

            } else if (values instanceof ValueSequence) {
                LOG.error(String.format("Cannot convert a sequence of values for key '%s'", keyValue));

            } else {
                LOG.error(String.format("Cannot convert map entry '%s'/'%s'", keyValue, values.getStringValue()));
            }

        }
        
        return retVal;
    }

    public static int getIntValue(String key, Object obj, int def) throws IllegalArgumentException {
        if (obj == null) {
            return def;
        }
        if (!(obj instanceof Integer)) {
            throw new IllegalArgumentException(String.format("Map item '%s' is not a Integer value (%s)", key, obj.toString()));
        }
        return (Integer) obj;
    }

    public static boolean getBooleanValue(String key, Object obj, boolean def) throws IllegalArgumentException {
        if (obj == null) {
            return def;
        }
        if (!(obj instanceof Boolean)) {
            throw new IllegalArgumentException(String.format("Map item '%s' is not a Boolean value (%s)", key, obj.toString()));
        }
        return (Boolean) obj;
    }

}
