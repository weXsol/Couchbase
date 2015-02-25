xquery version "3.0";

module namespace upsert="http://exist-db.org/couchbase/test/upsert";

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
function upsert:upsert_get() {
    
    let $clusterId := couchbase:connect("couchdb://localhost")
    let $documentName := "testDocument"
    let $json := '{ "a" : 1 }'

    let $upsert := couchbase:upsert($clusterId, (), $documentName, $json)

    let $get := couchbase:get($clusterId, (), $documentName)

    let $close := couchbase:close($clusterId)

    return $get
    
};

(: 
 : upsert and get
 :)

declare 
    %test:assertEquals('{"b":1}')
function upsert:insert_get() {
    
    let $clusterId := couchbase:connect("couchdb://localhost")
    let $documentName := "testInsertDocument"
    let $json := '{ "b" : 1 }'

    let $upsert := couchbase:insert($clusterId, (), $documentName, $json)

    let $get := couchbase:get($clusterId, (), $documentName)

    let $close := couchbase:close($clusterId)

    return $get
    
};

declare 
    %test:assertEquals('{"b":1}')
function upsert:insert_get_conflict() {
    
    let $clusterId := couchbase:connect("couchdb://localhost")
    let $documentName := "testInsertDocument"
    let $json := '{ "b" : 1 }'

    let $upsert := couchbase:insert($clusterId, (), $documentName, $json)

    let $get := couchbase:get($clusterId, (), $documentName)

    let $close := couchbase:close($clusterId)

    return $get
    
};
