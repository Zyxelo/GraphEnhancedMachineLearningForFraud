CREATE CONSTRAINT ON (c:Customer) ASSERT c.name IS UNIQUE;

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'file:///PS_20174392719_1491204439457_log.csv' AS line
WITH line


MERGE (custOrig:Customer {name: line.nameOrig})

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'file:///PS_20174392719_1491204439457_log.csv' AS line
WITH line

MATCH (custOrig: Customer {name: line.nameOrig})
WITH custOrig

MATCH (custDest:Customer {name: line.nameDest})
WITH custDest

MERGE (transaction:Transaction {amount: toFloat(line.amount), fraud: toInteger(line.isFraud), flaggedFraud: toInteger(line.isFlaggedFraud),
                                 type: line.type, step: toInteger(line.step), oldbalanceOrig: toFloat(line.oldbalanceOrig), newbalanceOrig: toFloat(line.newbalanceOrig),
                                 oldbalanceDest: toFloat(line.oldbalanceDest), newbalanceDest: toFloat(line.newbalanceDest)})-[:WITH]->(custDest)
MERGE (custOrig)-[:MAKE]->(transaction)
;





//











// find node with most transsactions
Match (t:Transaction)
WITH t, SIZE(()-[:MAKE]->(t)) as customerCnt
ORDER BY customerCnt DESC LIMIT 10
MATCH (a:Customer)-[:MAKE]->(t)
RETURN t, a



Match (c:Customer)
WITH c, SIZE((c)-[:MAKE]->(:Transaction {fraud: "1"})) as fraudCnt, SIZE((c)-[:MAKE]->(:Transaction)) as totCnt
SET c.fraudRatio = (1.0*fraudCnt/totCnt)
//RETURN  c, (1.0*fraudCnt/totCnt)


Match (c:Customer)
WITH c, SIZE((c)-[:MAKE]->(:Transaction {fraud: "1"})) as fraudCnt
ORDER BY fraudCnt DESC LIMIT 10
RETURN c

// Test query
MATCH (c:Customer)-[r1:MAKE]->(t1:Transaction)-[r2:WITH]->(c1:Customer)-[r3:MAKE]->(t2:Transaction)-[r4:WITH]->(c2:Customer)-[r5:MAKE]-(t3:Transaction)
RETURN c,r1, t1, r2, c1, r3, t2, r4, c2, r5,t3
limit 2

// Create unipartite graph for Page Rank
MATCH (c1:Customer)-[:MAKE]->(t1:Transaction)-[:WITH]->(b1:Bank)
WITH c1, b1, count(*) as count
Create (:Placeholder {id: c1.id})-[p:Payes]->(:Placeholder {id: b1.id})


MATCH (c1:Customer)-[:MAKE]->(t1:Transaction)-[:WITH]->(b1:Bank)
WITH c1, b1, count(*) as cnt
MATCH (p1:Placeholder {id:c1.id})
WITH c1, b1, p1, cnt
MATCH (p2:Placeholder {id: b1.id})
WITH c1, b1, p1, p2, cnt
CREATE (p1)-[:Payes {cnt: cnt}]->(p2)