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
package org.exist.couchbase.test.java;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author wessels
 */
public class JSonTests {

    public JSonTests() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }


    @Test
    public void basicJSONtest() {

        JsonObject user = JsonObject.empty()
                .put("firstname", "Walter")
                .put("lastname", "White")
                .put("job", "chemistry teacher")
                .put("age", 50);

        assertEquals("{\"firstname\":\"Walter\",\"job\":\"chemistry teacher\",\"age\":50,\"lastname\":\"White\"}", user.toString());

        JsonDocument doc = JsonDocument.create("walter", user);

        assertEquals("JsonDocument{id='walter', cas=0, expiry=0, content={\"firstname\":\"Walter\",\"job\":\"chemistry teacher\",\"age\":50,\"lastname\":\"White\"}, mutationToken=null}\n" +
                "", doc.toString());

    }

    @Test
    public void jsonDocument() throws IOException {
        String doc = IOUtils.toString(this.getClass().getResourceAsStream("address.json"));
        JsonObject jo = JsonObject.fromJson(doc);
        System.out.println(jo.toString());
    }

}
