package org.exist.couchbase.test.xquery;

import org.apache.commons.lang3.StringUtils;
import xquery.TestRunner;

public class CouchbaseTests extends TestRunner {

    @Override
    protected String getDirectory() {

        final ClassLoader loader = this.getClass().getClassLoader();
        final String className = this.getClass().getCanonicalName().replaceAll("\\.", "\\/");

        final String fullPath = loader.getResource(className + ".class").getFile();

        return StringUtils.substringBeforeLast(fullPath, "/");
    }
}
