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
import java.util.HashMap;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.array.ArrayType;
import org.exist.xquery.functions.map.MapType;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.DoubleValue;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.ValueSequence;

/**
 * Convert Couchbase's JSON object to eXist-db's Map
 *
 * @author wessels
 */
public class JsonToMap {

    public static MapType convert(JsonObject json, XQueryContext context) throws Exception {

        if (json instanceof JsonObject) {
            return convertJsonObject((JsonObject) json, context);

        } else {
            throw new IllegalArgumentException(String.format("Unable to convert " + json));
        }

    }

    static MapType convertJsonObject(JsonObject jsonObject, XQueryContext context) throws XPathException {

        MapType result = new MapType(context);

        for (String name : jsonObject.getNames()) {
            Object obj = jsonObject.get(name);

            if (obj instanceof JsonObject) {
                JsonObject jo = (JsonObject) obj;
                result.add(new StringValue(name), convertJsonObject(jo, context));

            } else if (obj instanceof JsonArray) {
                JsonArray ja = (JsonArray) obj;
                ArrayType array = convertJsonArray(ja, context);
                result.add(new StringValue(name), array);

            } else if (obj instanceof HashMap) {
                HashMap map = (HashMap) obj;
                JsonObject jo = JsonObject.from(map);
                result.add(new StringValue(name), convertJsonObject(jo, context));

            } else {
                result.add(new StringValue(name), convertToSequence(obj));
            }
        }

        return result;

    }

    private static ArrayType convertJsonArray(JsonArray ja, XQueryContext context) throws XPathException {

        Sequence sequence = new ValueSequence();

        for (Object obj : ja.toList()) {

            if (obj instanceof JsonObject) {
                JsonObject jo = (JsonObject) obj;
                sequence.add(convertJsonObject(jo, context));

            } else if (obj instanceof JsonArray) {
                JsonArray newarray = (JsonArray) obj;
                sequence.add(convertJsonArray(newarray, context));

            } else if (obj instanceof HashMap) {
                HashMap map = (HashMap) obj;
                JsonObject jo = JsonObject.from(map);
                sequence.add(convertJsonObject(jo, context));

            } else {
                sequence.add(convertToSequence(obj));
            }

        }

        return new ArrayType(context, sequence);

    }

    static AtomicValue convertToSequence(Object obj) throws XPathException {

        if (obj instanceof String) {
            return new StringValue((String) obj);
        } else if (obj instanceof Integer) {
            return new IntegerValue((Integer) obj);
        } else if (obj instanceof Double) {
            return new DoubleValue((Double) obj);
        } else if (obj instanceof Boolean) {
            return new BooleanValue((Boolean) obj);
        }

        throw new XPathException(String.format("Cannot convert %s. %s", obj, obj.toString()));

    }

}
