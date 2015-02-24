xquery version "3.0";

module namespace query="http://exist-db.org/couchbase/test/upsert";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : upsert and get
 :)

declare 
    %test:assertEquals('{"a":1}')
function query:upsert_get() {
    
    let $clusterId := couchbase:connect("couchdb://localhost")
    let $documentName := "testDocument"
    let $json := '{ "a" : 1 }'

    let $upsert := couchbase:upsert($clusterId, (), $documentName, $json)

    let $get := couchbase:get($clusterId, (), $documentName)

    let $close := couchbase:close($clusterId)

    return $get
    
};


