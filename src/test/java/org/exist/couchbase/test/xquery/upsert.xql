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
function upsert:upsert_get_simple() {
    
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

declare
    %test:assertEquals('{"key":["a","b","c"]}')
function upsert:upsert_simple_2() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{ "key": [ "a","b","c"] }'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)

};

declare
    %test:assertEquals('{"key":[{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"],"g":[{"a":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}},{"b":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}}]},{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"],"g":[{"a":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}},{"b":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}}]},{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"],"g":[{"a":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}},{"b":{"a":"1","b":2,"c":true,"d":false,"e":null,"f":["a","b","c"]}}]}]}')
function upsert:upsert_complex_1() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{"key": [
                      {
                          "a": "1",
                          "b": 2,
                          "c": true,
                          "d": false,
                          "e": null,
                          "f": [
                              "a",
                              "b",
                              "c"
                          ],
                          "g": [
                              {"a": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }},
                              {"b": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }}
                          ]
                      },
                      {
                          "a": "1",
                          "b": 2,
                          "c": true,
                          "d": false,
                          "e": null,
                          "f": [
                              "a",
                              "b",
                              "c"
                          ],
                          "g": [
                              {"a": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }},
                              {"b": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }}
                          ]
                      },
                      {
                          "a": "1",
                          "b": 2,
                          "c": true,
                          "d": false,
                          "e": null,
                          "f": [
                              "a",
                              "b",
                              "c"
                          ],
                          "g": [
                              {"a": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }},
                              {"b": {
                                  "a": "1",
                                  "b": 2,
                                  "c": true,
                                  "d": false,
                                  "e": null,
                                  "f": [
                                      "a",
                                      "b",
                                      "c"
                                  ]
                              }}
                          ]
                      }
                  ]}'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)

};

declare
    %test:assertEquals('{"megaArray":[[["a","b","c","d"],["a","b","c","d"],["a","b","c","d"],["a","b","c","d"]],[["a","b","c","d"],["a","b","c","d"],["a","b","c","d"],["a","b","c","d"]],[["a","b","c","d"],["a","b","c","d"],["a","b","c","d"],["a","b","c","d"]]]}')
function upsert:upsert_mega_array() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{"megaArray": [
                      [
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ]
                      ],
                      [
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ]
                      ],
                      [
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ],
                          [
                              "a",
                              "b",
                              "c",
                              "d"
                          ]
                      ]
                  ]}'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)

};


declare
    %test:assertEquals('{"a":{"b":{"c":{"d":{"e":{"f":"1"}}}}}}')
function upsert:upsert_recursive_object() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{"a": {"b": {"c": {"d": {"e": { "f" : "1"}}}}}}'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)

};


declare
    %test:assertEquals('{"summary":"","use":"read","created":"2017-02-22T14:21:43+02:00","model":{"pending":{"period":{"label":"","type":"","id":""},"validDate":"","data":{"appProfile":{"systemVariablesRepeat":{"summary":"","header":"","systemVariablesData":{"systemVariables":[{"name":"","value":"","key":""}]}},"Name":"I4S Logical Framework","refNo":"","description":""}},"user":{"userId":"38","name":"GIZ User","username":"GIZ"},"status":"Authorised","revision":"","seq":1},"approved":{"period":{"label":"","type":"","id":""},"validDate":"","data":{"appProfile":{"systemVariablesRepeat":{"summary":"","header":"","systemVariablesData":{"systemVariables":[{"name":"","value":"","key":""}]}},"Name":"I4S Logical Framework","refNo":"","description":""}},"user":{"userId":"38","name":"GIZ User","username":"GIZ"},"status":"Authorised","revision":"","seq":1},"version":""},"gps":{},"author":{"userId":"38","name":"GIZ User","username":"GIZ"},"attachments":[{"href":"","label":"","type":"","id":""}],"channels":["community_b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c","profile_53b65c40-798a-4adc-bc84-5fb9dff38ce8","application_407b6ee2-c682-41e0-a165-e7d9bc566a31","community_b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c_application_407b6ee2-c682-41e0-a165-e7d9bc566a31"],"meta-data":{"profileId":"53b65c40-798a-4adc-bc84-5fb9dff38ce8","subprofileId":"","communityId":"b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c","indicatorId":"","applicationId":"407b6ee2-c682-41e0-a165-e7d9bc566a31","setId":"appProfile"},"workflows":[{"instance":"53b65c40-798a-4adc-bc84-5fb9dff38ce8:processes","id":"407b6ee2-c682-41e0-a165-e7d9bc566a31:1:processDefinition","processes":[{"subProcessUUID":"5ee0a690-c75c-416d-edf6-1200e17e65ce","subProcessId":"spProfileRegistration","step":{"message":"Workflow Complete","assignedTo":{"userId":"","name":""},"endDate":"","startDate":"","status":"Complete","complete":false,"id":"completion","seq":4,"comment":""},"id":"profileRegistration"}]}],"title":"I4S Logical Framework","contributors":[{"userId":"","name":""},{"userId":"38","name":"GIZ User","username":"GIZ"}],"tags":[{"label":"","type":"","id":""}],"links":[{"applicationId":"407b6ee2-c682-41e0-a165-e7d9bc566a31","label":"Related","bookmark":"","Class":"P","type":"ParentProfile","uuid":"53b65c40-798a-4adc-bc84-5fb9dff38ce8","setId":"appProfile"},{"applicationId":"1023","label":"Related","bookmark":"","Class":"C","type":"ParentCommunity","uuid":"b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c","setId":"appProfile"},{"applicationId":"1023","label":"Related","bookmark":"","Class":"R","type":"RootCommunity”","uuid":"29000","setId":"appProfile"},{"applicationId":"407b6ee2-c682-41e0-a165-e7d9bc566a31","label":"Related","bookmark":"","Class":"I","type":"Indicator","uuid":"plan-bb59e7db-7d32-4323-887b-246dbaee98ba","setId":"plan"}],"_id":"53b65c40-798a-4adc-bc84-5fb9dff38ce8","type":"indicator","source":"remote","updated":"2017-02-22T14:21:43+02:00","control":{"draft":false,"moderation":{"reviewer":{"userId":"","name":"","dateTime":"","comment":""},"status":"","type":"","required":true},"active":{"details":{"userId":"","dateTime":""},"status":true},"privacy":{"update":{"roles":[{"label":"","id":""}]},"delete":{"roles":[{"label":"","id":""}]},"read":{"roles":[{"label":"","id":""}]}},"lock":{"details":{"userId":"","dateTime":""},"status":false,"expires":""},"deleted":{"details":{"userId":"","dateTime":""},"status":false}},"processes":[{"status":"","id":""}],"category":{"term":"appProfile","label":"Entries"}}')
function upsert:upsert_complex_object() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := '{
                      "_id": "53b65c40-798a-4adc-bc84-5fb9dff38ce8",
                      "attachments": [{
                          "href": "",
                          "id": "",
                          "label": "",
                          "type": ""
                      }],
                      "author": {
                          "name": "GIZ User",
                          "userId": "38",
                          "username": "GIZ"
                      },
                      "category": {
                          "label": "Entries",
                          "term": "appProfile"
                      },
                      "channels": [
                          "community_b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c",
                          "profile_53b65c40-798a-4adc-bc84-5fb9dff38ce8",
                          "application_407b6ee2-c682-41e0-a165-e7d9bc566a31",
                          "community_b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c_application_407b6ee2-c682-41e0-a165-e7d9bc566a31"
                      ],
                      "contributors": [
                          {
                              "name": "",
                              "userId": ""
                          },
                          {
                              "name": "GIZ User",
                              "userId": "38",
                              "username": "GIZ"
                          }
                      ],
                      "control": {
                          "active": {
                              "details": {
                                  "dateTime": "",
                                  "userId": ""
                              },
                              "status": true
                          },
                          "deleted": {
                              "details": {
                                  "dateTime": "",
                                  "userId": ""
                              },
                              "status": false
                          },
                          "draft": false,
                          "lock": {
                              "details": {
                                  "dateTime": "",
                                  "userId": ""
                              },
                              "expires": "",
                              "status": false
                          },
                          "moderation": {
                              "required": true,
                              "reviewer": {
                                  "comment": "",
                                  "dateTime": "",
                                  "name": "",
                                  "userId": ""
                              },
                              "status": "",
                              "type": ""
                          },
                          "privacy": {
                              "delete": {"roles": [{
                                  "id": "",
                                  "label": ""
                              }]},
                              "read": {"roles": [{
                                  "id": "",
                                  "label": ""
                              }]},
                              "update": {"roles": [{
                                  "id": "",
                                  "label": ""
                              }]}
                          }
                      },
                      "created": "2017-02-22T14:21:43+02:00",
                      "gps": {},
                      "links": [
                          {
                              "Class": "P",
                              "applicationId": "407b6ee2-c682-41e0-a165-e7d9bc566a31",
                              "bookmark": "",
                              "label": "Related",
                              "setId": "appProfile",
                              "type": "ParentProfile",
                              "uuid": "53b65c40-798a-4adc-bc84-5fb9dff38ce8"
                          },
                          {
                              "Class": "C",
                              "applicationId": "1023",
                              "bookmark": "",
                              "label": "Related",
                              "setId": "appProfile",
                              "type": "ParentCommunity",
                              "uuid": "b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c"
                          },
                          {
                              "Class": "R",
                              "applicationId": "1023",
                              "bookmark": "",
                              "label": "Related",
                              "setId": "appProfile",
                              "type": "RootCommunity\u201d",
                              "uuid": "29000"
                          },
                          {
                              "Class": "I",
                              "applicationId": "407b6ee2-c682-41e0-a165-e7d9bc566a31",
                              "bookmark": "",
                              "label": "Related",
                              "setId": "plan",
                              "type": "Indicator",
                              "uuid": "plan-bb59e7db-7d32-4323-887b-246dbaee98ba"
                          }
                      ],
                      "meta-data": {
                          "applicationId": "407b6ee2-c682-41e0-a165-e7d9bc566a31",
                          "communityId": "b3a29735-5ced-48d3-b9f3-c6b2f38d0e6c",
                          "indicatorId": "",
                          "profileId": "53b65c40-798a-4adc-bc84-5fb9dff38ce8",
                          "setId": "appProfile",
                          "subprofileId": ""
                      },
                      "model": {
                          "approved": {
                              "data": {"appProfile": {
                                  "Name": "I4S Logical Framework",
                                  "description": "",
                                  "refNo": "",
                                  "systemVariablesRepeat": {
                                      "header": "",
                                      "summary": "",
                                      "systemVariablesData": {"systemVariables": [{
                                          "key": "",
                                          "name": "",
                                          "value": ""
                                      }]}
                                  }
                              }},
                              "period": {
                                  "id": "",
                                  "label": "",
                                  "type": ""
                              },
                              "revision": "",
                              "seq": 1,
                              "status": "Authorised",
                              "user": {
                                  "name": "GIZ User",
                                  "userId": "38",
                                  "username": "GIZ"
                              },
                              "validDate": ""
                          },
                          "pending": {
                              "data": {"appProfile": {
                                  "Name": "I4S Logical Framework",
                                  "description": "",
                                  "refNo": "",
                                  "systemVariablesRepeat": {
                                      "header": "",
                                      "summary": "",
                                      "systemVariablesData": {"systemVariables": [{
                                          "key": "",
                                          "name": "",
                                          "value": ""
                                      }]}
                                  }
                              }},
                              "period": {
                                  "id": "",
                                  "label": "",
                                  "type": ""
                              },
                              "revision": "",
                              "seq": 1,
                              "status": "Authorised",
                              "user": {
                                  "name": "GIZ User",
                                  "userId": "38",
                                  "username": "GIZ"
                              },
                              "validDate": ""
                          },
                          "version": ""
                      },
                      "processes": [{
                          "id": "",
                          "status": ""
                      }],
                      "source": "remote",
                      "summary": "",
                      "tags": [{
                          "id": "",
                          "label": "",
                          "type": ""
                      }],
                      "title": "I4S Logical Framework",
                      "type": "indicator",
                      "updated": "2017-02-22T14:21:43+02:00",
                      "use": "read",
                      "workflows": [{
                          "id": "407b6ee2-c682-41e0-a165-e7d9bc566a31:1:processDefinition",
                          "instance": "53b65c40-798a-4adc-bc84-5fb9dff38ce8:processes",
                          "processes": [{
                              "id": "profileRegistration",
                              "step": {
                                  "assignedTo": {
                                      "name": "",
                                      "userId": ""
                                  },
                                  "comment": "",
                                  "complete": false,
                                  "endDate": "",
                                  "id": "completion",
                                  "message": "Workflow Complete",
                                  "seq": 4,
                                  "startDate": "",
                                  "status": "Complete"
                              },
                              "subProcessId": "spProfileRegistration",
                              "subProcessUUID": "5ee0a690-c75c-416d-edf6-1200e17e65ce"
                          }]
                      }]
                  }'

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

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

declare
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

declare
    %test:assertEquals('{"a":1,"b":2,"c":3,"d":4.1,"e":5}')
function upsert:valued_map() {

    let $clusterId := couchbase:connect("couchbase://localhost")
    let $documentName := "testUpsertDocument"
    let $json := map {
                         "a" : 1,
                         "b" : xs:int("2"),
                         "c" : xs:integer("3"),
                         "d" : xs:double("4.1"),
                         "e" : xs:long("5")

                     }

    let $upsert := couchbase:upsert($clusterId, $upsert:testBucket, $documentName, $json)

    let $get := couchbase:get($clusterId, $upsert:testBucket, $documentName)

    let $close := couchbase:close($clusterId)

    return serialize($get,
            <output:serialization-parameters>
                <output:method>json</output:method>
            </output:serialization-parameters>)

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