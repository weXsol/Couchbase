package org.exist.couchbase.shared;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.transcoder.JsonTranscoder;

/**
 *  Helper class for converting JSON documents
 * 
 * @author Dannes Wessels
 */
public class ConversionTools {

    private static final JsonTranscoder transcoder = new JsonTranscoder();

    /**
     * Convert JSON string to object
     *
     * @param document JSON formatted text document
     * @return JSON Object representation
     * @throws Exception When something bad happens during the JSON
     * conversion.
     */
    public static JsonObject convert(String document) throws Exception {
        return transcoder.stringToJsonObject(document);
    }

    /**
     * Convert JSON string to object
     *
     * @param document JSON object representation
     * @return JSON formatted text document
     * @throws Exception When something bad happens during the JSON
     * conversion.
     */
    public static String convert(JsonObject document) throws Exception {
        return transcoder.jsonObjectToString(document);
    }

}
