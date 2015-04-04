xquery version "3.0";

module namespace bucket="http://exist-db.org/couchbase/test/bucket";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";

declare variable $bucket:testBucket := "testBucket";

(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : Add and remove buckets
 :)
declare 
    %test:assertEquals(3,4,3)
function bucket:count() {
    
    let $username := "Administrator"
    let $password := "passwd!"
    let $bucket := "test"
    
    let $params := map { 
        "quota" := 100
    }
    
    let $clusterId := couchbase:connect("couchbase://localhost")
                        
    let $start := count( couchbase:list-buckets($clusterId, $username, $password) )
    
    let $tmp := couchbase:insert-bucket($clusterId, $bucket, $username, $password, $params)
    
    let $added := count( couchbase:list-buckets($clusterId, $username, $password) )
    
    let $tmp := couchbase:remove-bucket($clusterId, $bucket, $username, $password)
    
    let $stop := count( couchbase:list-buckets($clusterId, $username, $password) )
    
    let $close := couchbase:close($clusterId)
                       
    return ($start, $added, $stop)
    
};

declare %test:assertError("couchbase:COBA0021") function bucket:passwords()
{
    let $username := "Administrator"
    let $password := "passwd!"
    let $params := map { 
        "quota" := 100,
        "password" := "foobar"
    }
        
    let $clusterId := couchbase:connect("couchbase://localhost")
    
    let $tmp := couchbase:remove-bucket($clusterId, $bucket:testBucket, $username, $password) 
    let $tmp := couchbase:insert-bucket($clusterId, $bucket:testBucket, $username, $password, $params)

    let $close := couchbase:close($clusterId)

    let $clusterId := couchbase:connect("couchbase://localhost", "wrongpassword")

    let $json := '{ "b" : 1 }'

    return couchbase:insert($clusterId, $bucket:testBucket, "documentName", $json)
  
};