xquery version "3.0";

module namespace design="http://exist-db.org/couchbase/test/design";

import module namespace test="http://exist-db.org/xquery/xqsuite" 
                at "resource:org/exist/xquery/lib/xqsuite/xqsuite.xql";

import module namespace couchbase = "http://exist-db.org/couchbase/db" 
                at "java:org.exist.couchbase.xquery.CouchbaseModule";

declare variable $design:bucket := "beer-sample";
declare variable $design:view := "testview";
declare variable $design:designdoc := '{
 "views": {
 "byloc": {
 "map": "function (doc, meta) { if (meta.type == \"json\") { emit(doc.city, doc.sales);  } else { emit([\"blob\"]);  } }"
 } } }';

(: ----------------------------
 : Actual tests below this line  
 : ----------------------------:)


declare %test:assertEquals(1, 2, "true", "false", 1) 
function design:insert_delete()
{
    let $clusterId := couchbase:connect("couchbase://localhost")
    (: clean up to be sure :)
    (:let $result0 := couchbase:delete-design-document($clusterId, $design:bucket, $design:view):)
    let $count1 := count(couchbase:list-design-documents($clusterId, $design:bucket))
    let $result1 :=
        couchbase:insert-design-document($clusterId, $design:bucket, $design:view, $design:designdoc)
    let $count2 := count(couchbase:list-design-documents($clusterId, $design:bucket))
    let $result2 := couchbase:delete-design-document($clusterId, $design:bucket, $design:view)
    (:let $result3 := couchbase:delete-design-document($clusterId, $design:bucket, $design:view):)
let $result3 := false()
    let $count3 := count(couchbase:list-design-documents($clusterId, $design:bucket))
    let $close := couchbase:close($clusterId)
    return ($count1, $count2, $result2, $result3, $count3)
};

declare %test:assertError("couchbase:COBA0030")
function design:upsert_insert()
{
    let $clusterId := couchbase:connect("couchbase://localhost")
    (: clean up to be sure :)
    (:let $result0 := couchbase:delete-design-document($clusterId, $design:bucket, $design:view):)
    let $count1 := count(couchbase:list-design-documents($clusterId, $design:bucket))
    let $result1 := couchbase:upsert-design-document($clusterId, $design:bucket, $design:view, $design:designdoc)
    let $result2 := couchbase:insert-design-document($clusterId, $design:bucket, $design:view, $design:designdoc)
    return ($count1, $result1, $result2)
};
