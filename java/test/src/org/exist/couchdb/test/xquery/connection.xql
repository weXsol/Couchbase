xquery version "3.0";

module namespace connection="http://exist-db.org/couchbase/test/connection";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";


(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)

(: 
 : connect and disconnect
 :)
declare 
    %test:assertEquals(10,10,0)
function connection:count() {
    
    let $connectIds := for $i in (1 to 10)
                        return couchbase:connect("couchdb://localhost")
                        
    let $countBefore :=  count( couchbase:list-cluster-ids() )
    let $countDistinct :=  count( distinct-values(couchbase:list-cluster-ids()) )
    
    let $disconnect := for $clusterId in couchbase:list-cluster-ids()
                       return couchbase:close($clusterId)
                       
    let $countAfter := count( couchbase:list-cluster-ids() )
                       
    return ($countBefore, $countDistinct, $countAfter)
    
};

