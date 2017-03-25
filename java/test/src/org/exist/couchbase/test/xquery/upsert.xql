xquery version "3.0";

module namespace upsert="http://exist-db.org/couchbase/test/upsert";

declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";



declare variable $upsert:testBucket := "testBucket";

(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : upsert and get
 :)

declare 
    %test:assertEquals('{"a":1}')
function upsert:upsert_get() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{ "a" : 1 }'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)
    
};

(: 
 : upsert and get
 :)

declare 
    %test:assertEquals('{"b":1}')
function upsert:insert_get() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testInsertDocument"
    let $json := '{ "b" : 1 }'

    let $upsert := couchbase:insert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)
    
};

declare 
       %test:assertError("couchbase:COBA0017")
function upsert:insert_get_conflict() {
    
    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testInsertDocumentFail"
    let $json := '{ "b" : 1 }'

    let $upsert1 := couchbase:insert($clusterId, $upsert:testBucket, $documentName, $json)
    let $upsert2 := couchbase:insert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return $get
    
};


declare %test:setUp function upsert:setup()
{
    let $username := "Administrator"
    let $password := "passwd!"
    let $params := map { 
        "quota" := 100
    }
        
    let $clusterId := couchbase:connect("couchbase://localhost")
    
    let $tmp := couchbase:remove-bucket($clusterId, $upsert:testBucket, $username, $password) 
    let $tmp := couchbase:insert-bucket($clusterId, $upsert:testBucket, $username, $password, $params)

    return couchbase:close($clusterId)

    
    
};

declare %test:tearDown function upsert:teardown()
{
    let $username := "Administrator"
    let $password := "passwd!"
    let $params := map { 
        "quota" := 100
    }
        
    let $clusterId := couchbase:connect("couchbase://localhost")
    
    let $tmp := couchbase:remove-bucket($clusterId, $upsert:testBucket, $username, $password) 

    return couchbase:close($clusterId)

};