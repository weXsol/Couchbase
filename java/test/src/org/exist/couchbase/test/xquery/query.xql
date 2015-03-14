xquery version "3.0";

module namespace query="http://exist-db.org/couchbase/test/query";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : query and count
 :)

declare 
    %test:assertEquals(7303)
function query:count() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")

    let $result := couchbase:query($clusterId, "beer-sample", "beer", "brewery_beers", ())

    let $close := couchbase:close($clusterId)

    return count($result)
    
};

declare 
    %test:assertEquals(10)
function query:count() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")

    let $parameters :=
        map { 
            "limit" := 10
        }

    let $result := couchbase:query($clusterId, "beer-sample", "beer", "brewery_beers", $parameters)

    let $close := couchbase:close($clusterId)

    return count($result)
    
};

declare 
   %test:assertError("java.lang.IllegalArgumentException")
function query:count_fail() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")

    let $parameters :=
        map { 
            "fail" := 10
        }

    let $result := couchbase:query($clusterId, "beer-sample", "beer", "brewery_beers", $parameters)

    let $close := couchbase:close($clusterId)

    return count($result)
    
};