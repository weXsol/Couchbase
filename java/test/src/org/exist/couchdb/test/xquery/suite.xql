xquery version "3.0";

import module namespace test="http://exist-db.org/xquery/xqsuite"
at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

test:suite((
    inspect:module-functions(xs:anyURI("connection.xql")),
    inspect:module-functions(xs:anyURI("query.xql")),
    inspect:module-functions(xs:anyURI("upsert.xql")),
    inspect:module-functions(xs:anyURI("bucket.xql"))
))
