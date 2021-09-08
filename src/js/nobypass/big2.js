/**
 * The 'forEachCollectionFromDb' CollectionCatalog helper should abandon its snapshot before it
 * performs the user provided callback on each collection.
 * Any newly added collections have the chance to be seen as 'forEachCollectionFromDb' iterates over
 * the remaining collections. This could lead to inconsistencies, such as seeing indexes in the
 * IndexCatalog of the new collection but not seeing them on-disk due to using an older snapshot
 * (BF-13133).
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const db = rst.getPrimary().getDB(dbName);
assert.commandWorked(db.createCollection(collName));

const failpoint = 'hangBeforeGettingNextCollection';

// Hang 'forEachCollectionFromDb' after iterating through the first collection.
assert.commandWorked(db.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));

TestData.failpoint = failpoint;
const awaitCreateCollections = startParallelShell(() => {
    // The 'forEachCollectionFromDb' helper doesn't iterate in collection name order, so we need
    // to insert multiple collections to have at least one next collection when the
    // CollectionCatalog iterator is incremented.
    for (let i = 0; i < 25; i++) {
        const collName = "a".repeat(i + 1);
        assert.commandWorked(db.createCollection(collName));
    }

    // Let 'forEachCollectionFromDb' iterate to the next collection.
    assert.commandWorked(db.adminCommand({configureFailPoint: TestData.failpoint, mode: "off"}));
}, rst.getPrimary().port);

assert.commandWorked(db.stats());
awaitCreateCollections();

rst.stopSet();
}());

/**
 * Ensures that the 'ns' field for index specs is absent with its removal in SERVER-41696.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

const dbName = 'test';
const collName = 'absent_ns';

let replSet = new ReplSetTest({name: 'absentNsField', nodes: 2});
replSet.startSet();
replSet.initiate();

const primary = replSet.getPrimary();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB.getCollection(collName);

const secondary = replSet.getSecondary();
const secondaryDB = secondary.getDB(dbName);

assert.commandWorked(primaryColl.insert({x: 100}));
assert.commandWorked(primaryColl.createIndex({x: 1}));

replSet.awaitReplication();

let specPrimary =
    assert.commandWorked(primaryDB.runCommand({listIndexes: collName})).cursor.firstBatch[1];
let specSecondary =
    assert.commandWorked(secondaryDB.runCommand({listIndexes: collName})).cursor.firstBatch[1];

assert.eq(false, specPrimary.hasOwnProperty('ns'));
assert.eq(false, specSecondary.hasOwnProperty('ns'));

replSet.stopSet(/*signal=*/null, /*forRestart=*/true);

// Both nodes should have no 'ns' field in the index spec on restart.
const options = {
    dbpath: primary.dbpath,
    noCleanData: true
};
let conn = MongoRunner.runMongod(options);
assert.neq(null, conn, 'mongod was unable to start up with options: ' + tojson(options));

let db = conn.getDB(dbName);
let spec = assert.commandWorked(db.runCommand({listIndexes: collName})).cursor.firstBatch[1];

assert.eq(false, spec.hasOwnProperty('ns'));

MongoRunner.stopMongod(conn);

options.dbpath = secondary.dbpath;
conn = MongoRunner.runMongod(options);
assert.neq(null, conn, 'mongod was unable to start up with options: ' + tojson(options));

db = conn.getDB(dbName);
spec = assert.commandWorked(db.runCommand({listIndexes: collName})).cursor.firstBatch[1];

assert.eq(false, spec.hasOwnProperty('ns'));

MongoRunner.stopMongod(conn);
}());

// This test verifies readConcern:afterClusterTime behavior on a standalone mongod.
// @tags: [requires_replication, requires_majority_read_concern]
(function() {
"use strict";
var standalone =
    MongoRunner.runMongod({enableMajorityReadConcern: "", storageEngine: "wiredTiger"});

var testDB = standalone.getDB("test");

assert.commandWorked(testDB.runCommand({insert: "after_cluster_time", documents: [{x: 1}]}));

// Majority reads without afterClusterTime succeed.
assert.commandWorked(
    testDB.runCommand({find: "after_cluster_time", readConcern: {level: "majority"}}),
    "expected majority read without afterClusterTime to succeed on standalone mongod");

// afterClusterTime reads without a level fail.
assert.commandFailedWithCode(
    testDB.runCommand(
        {find: "after_cluster_time", readConcern: {afterClusterTime: Timestamp(0, 0)}}),
    ErrorCodes.InvalidOptions,
    "expected non-majority afterClusterTime read to fail on standalone mongod");

// afterClusterTime reads with null timestamps are rejected.
assert.commandFailedWithCode(
    testDB.runCommand({
        find: "after_cluster_time",
        readConcern: {level: "majority", afterClusterTime: Timestamp(0, 0)}
    }),
    ErrorCodes.InvalidOptions,
    "expected afterClusterTime read with null timestamp to fail on standalone mongod");

// Standalones don't support any operations with clusterTime.
assert.commandFailedWithCode(testDB.runCommand({
    find: "after_cluster_time",
    readConcern: {level: "majority", afterClusterTime: Timestamp(0, 1)}
}),
                             ErrorCodes.IllegalOperation,
                             "expected afterClusterTime read to fail on standalone mongod");
MongoRunner.stopMongod(standalone);

var rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
var adminDBRS = rst.getPrimary().getDB("admin");

var res = adminDBRS.runCommand({ping: 1});
assert.commandWorked(res);
assert(res.hasOwnProperty("$clusterTime"), tojson(res));
assert(res.$clusterTime.hasOwnProperty("clusterTime"), tojson(res));
var clusterTime = res.$clusterTime.clusterTime;
// afterClusterTime is not allowed in  ping command.
assert.commandFailedWithCode(
    adminDBRS.runCommand({ping: 1, readConcern: {afterClusterTime: clusterTime}}),
    ErrorCodes.InvalidOptions,
    "expected afterClusterTime fail in ping");

// afterClusterTime is not allowed in serverStatus command.
assert.commandFailedWithCode(
    adminDBRS.runCommand({serverStatus: 1, readConcern: {afterClusterTime: clusterTime}}),
    ErrorCodes.InvalidOptions,
    "expected afterClusterTime fail in serverStatus");

// afterClusterTime is not allowed in currentOp command.
assert.commandFailedWithCode(
    adminDBRS.runCommand({currentOp: 1, readConcern: {afterClusterTime: clusterTime}}),
    ErrorCodes.InvalidOptions,
    "expected afterClusterTime fail in serverStatus");

rst.stopSet();
}());

// Test that causally consistent majority-committed read-only transactions will wait for the
// majority commit point to move past 'afterClusterTime' before they can commit.
// @tags: [
//   requires_majority_read_concern,
//   uses_transactions,
// ]
(function() {
"use strict";

load("jstests/libs/write_concern_util.js");  // For stopReplicationOnSecondaries.

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const session = rst.getPrimary().getDB(dbName).getMongo().startSession({causalConsistency: false});
const primaryDB = session.getDatabase(dbName);

let txnNumber = 0;

function testReadConcernLevel(level) {
    // Stop replication.
    stopReplicationOnSecondaries(rst);

    // Perform a write and get its op time.
    const res = assert.commandWorked(primaryDB.runCommand({insert: collName, documents: [{}]}));
    assert(res.hasOwnProperty("opTime"), tojson(res));
    assert(res.opTime.hasOwnProperty("ts"), tojson(res));
    const clusterTime = res.opTime.ts;

    // A majority-committed read-only transaction on the primary after the new cluster time
    // should time out at commit time waiting for the cluster time to be majority committed.
    assert.commandWorked(primaryDB.runCommand({
        find: collName,
        txnNumber: NumberLong(++txnNumber),
        startTransaction: true,
        autocommit: false,
        readConcern: {level: level, afterClusterTime: clusterTime}
    }));
    assert.commandFailedWithCode(primaryDB.adminCommand({
        commitTransaction: 1,
        txnNumber: NumberLong(txnNumber),
        autocommit: false,
        writeConcern: {w: "majority"},
        maxTimeMS: 1000
    }),
                                 ErrorCodes.MaxTimeMSExpired);

    // Restart replication.
    restartReplicationOnSecondaries(rst);

    // A majority-committed read-only transaction on the primary after the new cluster time now
    // succeeds.
    assert.commandWorked(primaryDB.runCommand({
        find: collName,
        txnNumber: NumberLong(++txnNumber),
        startTransaction: true,
        autocommit: false,
        readConcern: {level: level, afterClusterTime: clusterTime}
    }));
    assert.commandWorked(primaryDB.adminCommand({
        commitTransaction: 1,
        txnNumber: NumberLong(txnNumber),
        autocommit: false,
        writeConcern: {w: "majority"}
    }));
}

testReadConcernLevel("majority");
testReadConcernLevel("snapshot");

rst.stopSet();
}());

/**
 * Tests for using collStats to retrieve count information.
 *
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

const dbName = jsTestName();
const collName = "test";

function getShardCount(counts, shardName) {
    for (let i = 0; i < counts.length; i++) {
        if (counts[i]["shard"] == shardName)
            return counts[i];
    }
    return {count: null};
}

/* Accepts a dbName, collName, and shardDistribution (array of positive integers or nulls).
 * Creates a sharded cluster with shardDistribution.length shards and shardDistribution[i] documents
 * on the i-th shard or no chunks assigned to that shard if shardDistribution[i] is null.
 */
function runShardingTestExists(shardDistribution) {
    const st = ShardingTest({shards: shardDistribution.length});

    const mongos = st.s0;
    const admin = mongos.getDB("admin");
    const config = mongos.getDB("config");
    const shards = config.shards.find().toArray();
    const namespace = dbName + "." + collName;

    /* Shard the collection. */
    assert.commandWorked(admin.runCommand({enableSharding: dbName}));
    assert.commandWorked(admin.runCommand({movePrimary: dbName, to: shards[0]._id}));
    assert.commandWorked(admin.runCommand({shardCollection: namespace, key: {a: 1}}));

    const coll = mongos.getCollection(namespace);

    const length = shardDistribution.length;
    let curr = 0;
    let startChunk = curr;

    for (let i = 0; i < length; i++) {
        for (startChunk = curr;
             shardDistribution[i] != null && curr < startChunk + shardDistribution[i];
             curr++) {
            /* Insert shardDistribution[i] documents into the current chunk.*/
            assert.commandWorked(coll.insert({a: curr}));
        }

        /* We need to ensure that we don't split at the same location as we spit previously.  */
        if (shardDistribution[i] == 0) {
            curr++;
        }

        /* If the i-th shard is supposed to have documents then split the chunk to the right of
         * where it is supposed to end. Otherwise do not split the chunk. */
        if (shardDistribution[i] != null) {
            assert.commandWorked(st.splitAt(namespace, {a: curr}));
        }

        /* Move the "next" chunk to the next shard */
        assert.commandWorked(admin.runCommand(
            {moveChunk: namespace, find: {a: curr + 1}, to: shards[(i + 1) % length]._id}));
    }

    /* Move the remaining chunk to the first shard which is supposed to have documents. */
    for (let j = 0; shardDistribution[j] == null && j < length; j++)
        assert.commandWorked(
            admin.runCommand({moveChunk: namespace, find: {a: curr + 1}, to: shards[j + 1]._id}));

    const counts = coll.aggregate([{"$collStats": {"count": {}}}]).toArray();

    for (let i = 0; i < shards.length; i++) {
        assert.eq(getShardCount(counts, shards[i]._id)["count"], shardDistribution[i]);
    }

    st.stop();
}

function runUnshardedCollectionShardTestExists(shardNum, docsNum) {
    const st = ShardingTest({shards: shardNum});

    const mongos = st.s0;
    const admin = mongos.getDB("admin");
    const namespace = dbName + "." + collName;
    const coll = mongos.getCollection(namespace);

    /* Shard the collection. */
    assert.commandWorked(admin.runCommand({enableSharding: dbName}));

    for (let i = 0; i < docsNum; i++) {
        assert.commandWorked(coll.insert({a: i}));
    }

    const counts = coll.aggregate([{"$collStats": {"count": {}}}]).toArray();

    assert.eq(counts.length, 1);
    assert.eq(counts[0]["count"], docsNum);
    assert.eq(counts[0].hasOwnProperty("shard"), true);

    st.stop();
}

function runReplicaSetTestExists(nodesNum, docsNum) {
    const namespace = dbName + '.' + collName;
    const rst = ReplSetTest({nodes: nodesNum});

    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const coll = primary.getCollection(namespace);

    for (let i = 0; i < docsNum; i++) {
        assert.commandWorked(coll.insert({a: i}));
    }

    const counts = coll.aggregate([{"$collStats": {"count": {}}}]).toArray();

    assert.eq(counts.length, 1);
    assert.eq(counts[0]["count"], docsNum);
    assert.eq(counts[0].hasOwnProperty("shard"), false);

    rst.stopSet();
}

function runStandaloneTestExists(docsNum) {
    const namespace = dbName + '.' + collName;
    const conn = MongoRunner.runMongod({});

    const coll = conn.getCollection(namespace);

    for (let i = 0; i < docsNum; i++) {
        assert.commandWorked(coll.insert({a: i}));
    }

    const counts = coll.aggregate([{"$collStats": {"count": {}}}]).toArray();

    assert.eq(counts.length, 1);
    assert.eq(counts[0]["count"], docsNum);

    MongoRunner.stopMongod(conn);
}

runShardingTestExists([1, 2]);
runShardingTestExists([null, 1, 2]);
runShardingTestExists([null, 0, 2]);
runShardingTestExists([null, 2, 0]);

runReplicaSetTestExists(1, 4);

runStandaloneTestExists(4);
runStandaloneTestExists(6);

runUnshardedCollectionShardTestExists(3, 4);
runUnshardedCollectionShardTestExists(2, 3);

const doesNotExistName = "dne";

/* Test that if a collection does not exist that the database throws NamespaceNotFound. */
const st = ShardingTest({shards: 3});
const mongos = st.s0;
const stDb = mongos.getDB(dbName);

assert.commandFailedWithCode(
    stDb.runCommand(
        {aggregate: doesNotExistName, pipeline: [{"$collStats": {"count": {}}}], cursor: {}}),
    ErrorCodes.NamespaceNotFound);

assert.commandFailedWithCode(
    stDb.runCommand(
        {aggregate: doesNotExistName, pipeline: [{"$collStats": {"unknown": {}}}], cursor: {}}),
    40415);

assert.commandFailedWithCode(stDb.runCommand({
    aggregate: doesNotExistName,
    pipeline: [{"$collStats": {"queryExecStats": {}}}],
    cursor: {}
}),
                             ErrorCodes.NamespaceNotFound);

st.stop();

const rst = ReplSetTest({nodes: 3});
rst.startSet();
rst.initiate();
const rstDb = rst.getPrimary().getDB(dbName);

assert.commandFailedWithCode(
    rstDb.runCommand(
        {aggregate: doesNotExistName, pipeline: [{"$collStats": {"count": {}}}], cursor: {}}),
    ErrorCodes.NamespaceNotFound);

rst.stopSet();

const conn = MongoRunner.runMongod({});
const standaloneDb = conn.getDB(dbName);

assert.commandFailedWithCode(
    standaloneDb.runCommand(
        {aggregate: doesNotExistName, pipeline: [{"$collStats": {"count": {}}}], cursor: {}}),
    ErrorCodes.NamespaceNotFound);

assert.commandFailedWithCode(
    standaloneDb.runCommand(
        {aggregate: doesNotExistName, pipeline: [{"$collStats": {"unknown": {}}}], cursor: {}}),
    40415);

assert.commandFailedWithCode(standaloneDb.runCommand({
    aggregate: doesNotExistName,
    pipeline: [{"$collStats": {"queryExecStats": {}}}],
    cursor: {}
}),
                             ErrorCodes.NamespaceNotFound);

MongoRunner.stopMongod(conn);
})();

// Tests that certain aggregation operators have configurable memory limits.
(function() {
"use strict";

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");
const db = conn.getDB("test");
const coll = db.agg_configurable_memory_limit;

const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 100; i++) {
    bulk.insert({_id: i, x: i, y: ["string 1", "string 2", "string 3", "string 4", "string " + i]});
}
assert.commandWorked(bulk.execute());

// Test that pushing a bunch of strings to an array does not exceed the default 100MB memory limit.
assert.doesNotThrow(
    () => coll.aggregate([{$unwind: "$y"}, {$group: {_id: null, strings: {$push: "$y"}}}]));

// Now lower the limit to test that it's configuration is obeyed.
assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: 100}));
assert.throwsWithCode(
    () => coll.aggregate([{$unwind: "$y"}, {$group: {_id: null, strings: {$push: "$y"}}}]),
    ErrorCodes.ExceededMemoryLimit);

// Test that using $addToSet behaves similarly.
assert.doesNotThrow(
    () => coll.aggregate([{$unwind: "$y"}, {$group: {_id: null, strings: {$addToSet: "$y"}}}]));

assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxAddToSetBytes: 100}));
assert.throwsWithCode(
    () => coll.aggregate([{$unwind: "$y"}, {$group: {_id: null, strings: {$addToSet: "$y"}}}]),
    ErrorCodes.ExceededMemoryLimit);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that an aggregation cursor is killed when it is timed out by the ClientCursorMonitor.
 *
 * This test was designed to reproduce SERVER-25585.
 */
(function() {
'use strict';

// Cursor timeout on mongod is handled by a single thread/timer that will sleep for
// "clientCursorMonitorFrequencySecs" and add the sleep value to each operation's duration when
// it wakes up, timing out those whose "now() - last accessed since" time exceeds. A cursor
// timeout of 2 seconds with a monitor frequency of 1 second means an effective timeout period
// of 1 to 2 seconds.
const cursorTimeoutMs = 2000;
const cursorMonitorFrequencySecs = 1;

const options = {
    setParameter: {
        internalDocumentSourceCursorBatchSizeBytes: 1,
        // We use the "cursorTimeoutMillis" server parameter to decrease how long it takes for a
        // non-exhausted cursor to time out. We use the "clientCursorMonitorFrequencySecs"
        // server parameter to make the ClientCursorMonitor that cleans up the timed out cursors
        // run more often. The combination of these server parameters reduces the amount of time
        // we need to wait within this test.
        cursorTimeoutMillis: cursorTimeoutMs,
        clientCursorMonitorFrequencySecs: cursorMonitorFrequencySecs,
    }
};
const conn = MongoRunner.runMongod(options);
assert.neq(null, conn, 'mongod was unable to start up with options: ' + tojson(options));

const testDB = conn.getDB('test');

// We use a batch size of 2 to ensure that the mongo shell does not exhaust the cursor on its
// first batch.
const batchSize = 2;
const numMatches = 5;

function assertCursorTimesOutImpl(collName, pipeline) {
    const res = assert.commandWorked(testDB.runCommand({
        aggregate: collName,
        pipeline: pipeline,
        cursor: {
            batchSize: batchSize,
        },
    }));

    let serverStatus = assert.commandWorked(testDB.serverStatus());
    const expectedNumTimedOutCursors = serverStatus.metrics.cursor.timedOut + 1;

    const cursor = new DBCommandCursor(testDB, res, batchSize);

    // Wait until the idle cursor background job has killed the aggregation cursor.
    assert.soon(
        function() {
            serverStatus = assert.commandWorked(testDB.serverStatus());
            return +serverStatus.metrics.cursor.timedOut === expectedNumTimedOutCursors;
        },
        function() {
            return "aggregation cursor failed to time out: " + tojson(serverStatus.metrics.cursor);
        });

    assert.eq(0, serverStatus.metrics.cursor.open.total, tojson(serverStatus));

    // We attempt to exhaust the aggregation cursor to verify that sending a getMore returns an
    // error due to the cursor being killed.
    let err = assert.throws(function() {
        cursor.itcount();
    });
    assert.eq(ErrorCodes.CursorNotFound, err.code, tojson(err));
}

function assertCursorTimesOut(collName, pipeline) {
    // Confirm that cursor timeout occurs outside of sessions.
    TestData.disableImplicitSessions = true;
    assertCursorTimesOutImpl(collName, pipeline);
    TestData.disableImplicitSessions = false;

    // Confirm that cursor timeout occurs within sessions when the
    // `enableTimeoutOfInactiveSessionCursors` parameter is set to true. If false, we rely on
    // session expiration to cleanup outstanding cursors.
    assert.commandWorked(
        testDB.adminCommand({setParameter: 1, enableTimeoutOfInactiveSessionCursors: true}));
    assertCursorTimesOutImpl(collName, pipeline);
    assert.commandWorked(
        testDB.adminCommand({setParameter: 1, enableTimeoutOfInactiveSessionCursors: false}));
}

assert.commandWorked(testDB.source.insert({local: 1}));
for (let i = 0; i < numMatches; ++i) {
    assert.commandWorked(testDB.dest.insert({foreign: 1}));
}

// Test that a regular aggregation cursor is killed when the timeout is reached.
assertCursorTimesOut('dest', []);

// Test that an aggregation cursor with a $lookup stage is killed when the timeout is reached.
assertCursorTimesOut('source', [
        {
          $lookup: {
              from: 'dest',
              localField: 'local',
              foreignField: 'foreign',
              as: 'matches',
          }
        },
        {
          $unwind: "$matches",
        },
    ]);

// Test that an aggregation cursor with nested $lookup stages is killed when the timeout is
// reached.
assertCursorTimesOut('source', [
        {
          $lookup: {
              from: 'dest',
              let : {local1: "$local"},
              pipeline: [
                  {$match: {$expr: {$eq: ["$foreign", "$$local1"]}}},
                  {
                    $lookup: {
                        from: 'source',
                        let : {foreign1: "$foreign"},
                        pipeline: [{$match: {$expr: {$eq: ["$local", "$$foreign1"]}}}],
                        as: 'matches2'
                    }
                  },
                  {
                    $unwind: "$matches2",
                  },
              ],
              as: 'matches1',
          }
        },
        {
          $unwind: "$matches1",
        },
    ]);

MongoRunner.stopMongod(conn);
})();

/**
 * Test that explained aggregation commands behave correctly with the readConcern option.
 * @tags: [requires_majority_read_concern]
 */
(function() {
"use strict";

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

const rst = new ReplSetTest(
    {name: "aggExplainReadConcernSet", nodes: 1, nodeOptions: {enableMajorityReadConcern: ""}});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const session = primary.getDB("test").getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase("test");
const coll = sessionDB.agg_explain_read_concern;

// Test that explain is legal with readConcern "local".
assert.commandWorked(coll.explain().aggregate([], {readConcern: {level: "local"}}));
assert.commandWorked(sessionDB.runCommand(
    {aggregate: coll.getName(), pipeline: [], explain: true, readConcern: {level: "local"}}));
assert.commandWorked(sessionDB.runCommand({
    explain: {aggregate: coll.getName(), pipeline: [], cursor: {}},
    readConcern: {level: "local"}
}));

// Test that explain is illegal with other readConcern levels.
const nonLocalReadConcerns = ["majority", "available", "linearizable"];
nonLocalReadConcerns.forEach(function(readConcernLevel) {
    let aggCmd = {
        aggregate: coll.getName(),
        pipeline: [],
        explain: true,
        readConcern: {level: readConcernLevel}
    };
    let explainCmd = {
        explain: {aggregate: coll.getName(), pipeline: [], cursor: {}},
        readConcern: {level: readConcernLevel}
    };

    assert.throws(() => coll.explain().aggregate([], {readConcern: {level: readConcernLevel}}));

    let cmdRes = sessionDB.runCommand(aggCmd);
    assert.commandFailedWithCode(cmdRes, ErrorCodes.InvalidOptions, tojson(cmdRes));
    let expectedErrStr = "aggregate command cannot run with a readConcern other than 'local'";
    assert.neq(cmdRes.errmsg.indexOf(expectedErrStr), -1, tojson(cmdRes));

    cmdRes = sessionDB.runCommand(explainCmd);
    assert.commandFailedWithCode(cmdRes, ErrorCodes.InvalidOptions, tojson(cmdRes));
    expectedErrStr = "read concern not supported";
    assert.neq(cmdRes.errmsg.indexOf(expectedErrStr), -1, tojson(cmdRes));
});

session.endSession();
rst.stopSet();
}());

/**
 * Tests command output from the $operationMetrics aggregation stage.
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

var rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {setParameter: {"aggregateOperationResourceConsumptionMetrics": true}}
});
rst.startSet();
rst.initiate();

const isLinux = getBuildInfo().buildEnvironment.target_os == "linux";

let assertMetricsExist = function(metrics) {
    try {
        assert.neq(metrics, undefined);
        assert(metrics.hasOwnProperty("db"));
        assert(metrics.hasOwnProperty("localTime"));
        let primaryMetrics = metrics.primaryMetrics;
        let secondaryMetrics = metrics.secondaryMetrics;
        [primaryMetrics, secondaryMetrics].forEach((readMetrics) => {
            assert.gte(readMetrics.docBytesRead, 0);
            assert.gte(readMetrics.docUnitsRead, 0);
            assert.gte(readMetrics.idxEntryBytesRead, 0);
            assert.gte(readMetrics.idxEntryUnitsRead, 0);
            assert.gte(readMetrics.keysSorted, 0);
            assert.gte(readMetrics.sorterSpills, 0);
            assert.gte(readMetrics.docUnitsReturned, 0);
            assert.gte(readMetrics.cursorSeeks, 0);
        });

        assert.gte(metrics.cpuNanos, 0);
        assert.gte(metrics.docBytesWritten, 0);
        assert.gte(metrics.docUnitsWritten, 0);
        assert.gte(metrics.idxEntryBytesWritten, 0);
        assert.gte(metrics.idxEntryUnitsWritten, 0);
        assert.gte(metrics.totalUnitsWritten, 0);
    } catch (e) {
        print("caught exception while checking metrics output: " + tojson(metrics));
        throw e;
    }
};

let getDBMetrics = (adminDB) => {
    let cursor = adminDB.aggregate([{$operationMetrics: {}}]);

    // Merge all returned documents into a single object keyed by database name.
    let allMetrics = {};
    while (cursor.hasNext()) {
        let doc = cursor.next();
        allMetrics[doc.db] = doc;
    }

    return allMetrics;
};

let getServerStatusMetrics = (db) => {
    let ss = db.serverStatus();
    assert(ss.hasOwnProperty('resourceConsumption'), ss);
    return ss.resourceConsumption;
};

const primary = rst.getPrimary();

// $operationMetrics may only be run against the admin database and in a 'collectionless' form.
assert.commandFailedWithCode(primary.getDB('invalid').runCommand({
    aggregate: 1,
    pipeline: [{$operationMetrics: {}}],
    cursor: {},
}),
                             ErrorCodes.InvalidNamespace);
assert.commandFailedWithCode(primary.getDB('admin').runCommand({
    aggregate: 'test',
    pipeline: [{$operationMetrics: {}}],
    cursor: {},
}),
                             ErrorCodes.InvalidNamespace);

// Perform very basic reads and writes on two different databases.
const db1Name = 'db1';
const db1 = primary.getDB(db1Name);
assert.commandWorked(db1.coll1.insert({a: 1}));
assert.commandWorked(db1.coll2.insert({a: 1}));

const db2Name = 'db2';
const db2 = primary.getDB(db2Name);
assert.commandWorked(db2.coll1.insert({a: 1}));
assert.commandWorked(db2.coll2.insert({a: 1}));

const secondary = rst.getSecondary();
[primary, secondary].forEach(function(node) {
    jsTestLog("Testing node: " + node);

    // Clear metrics after waiting for replication to ensure we are not observing metrics from
    // a previous loop iteration.
    rst.awaitReplication();
    const adminDB = node.getDB('admin');

    let initialCpuTime = getServerStatusMetrics(adminDB).cpuNanos;

    adminDB.aggregate([{$operationMetrics: {clearMetrics: true}}]);

    assert.eq(node.getDB(db1Name).coll1.find({a: 1}).itcount(), 1);
    assert.eq(node.getDB(db1Name).coll2.find({a: 1}).itcount(), 1);
    assert.eq(node.getDB(db2Name).coll1.find({a: 1}).itcount(), 1);
    assert.eq(node.getDB(db2Name).coll2.find({a: 1}).itcount(), 1);

    // Run an aggregation with a batch size of 1.
    let cursor = adminDB.aggregate([{$operationMetrics: {}}], {cursor: {batchSize: 1}});
    assert(cursor.hasNext());

    // Merge all returned documents into a single object keyed by database name.
    let allMetrics = {};
    let doc = cursor.next();
    allMetrics[doc.db] = doc;
    assert.eq(cursor.objsLeftInBatch(), 0);

    // Trigger a getMore to retrieve metrics for the other database.
    assert(cursor.hasNext());
    doc = cursor.next();
    allMetrics[doc.db] = doc;
    assert(!cursor.hasNext());

    // Ensure the two user database have present metrics.
    assertMetricsExist(allMetrics[db1Name]);
    assertMetricsExist(allMetrics[db2Name]);

    let ssMetrics = getServerStatusMetrics(adminDB);
    assert.eq(ssMetrics.numMetrics, 2);
    assert.gt(ssMetrics.memUsage, 0);

    // Ensure read metrics are attributed to the correct replication state.
    let lastDocBytesRead;
    if (node === primary) {
        [db1Name, db2Name].forEach((db) => {
            assert.gt(allMetrics[db].primaryMetrics.docBytesRead, 0);
            assert.gt(allMetrics[db].primaryMetrics.docUnitsRead, 0);
            assert.eq(allMetrics[db].primaryMetrics.cursorSeeks, 0);
            assert.eq(allMetrics[db].secondaryMetrics.docBytesRead, 0);
            assert.eq(allMetrics[db].secondaryMetrics.docUnitsRead, 0);
            assert.eq(allMetrics[db].secondaryMetrics.cursorSeeks, 0);
        });
        assert.eq(allMetrics[db1Name].primaryMetrics.docBytesRead,
                  allMetrics[db2Name].primaryMetrics.docBytesRead);
        lastDocBytesRead = allMetrics[db1Name].primaryMetrics.docBytesRead;
    } else {
        [db1Name, db2Name].forEach((db) => {
            assert.gt(allMetrics[db].secondaryMetrics.docBytesRead, 0);
            assert.gt(allMetrics[db].secondaryMetrics.docUnitsRead, 0);
            assert.eq(allMetrics[db].secondaryMetrics.cursorSeeks, 0);
            assert.eq(allMetrics[db].primaryMetrics.docBytesRead, 0);
            assert.eq(allMetrics[db].primaryMetrics.docUnitsRead, 0);
            assert.eq(allMetrics[db].primaryMetrics.cursorSeeks, 0);
        });
        assert.eq(allMetrics[db1Name].secondaryMetrics.docBytesRead,
                  allMetrics[db2Name].secondaryMetrics.docBytesRead);
        lastDocBytesRead = allMetrics[db1Name].secondaryMetrics.docBytesRead;
    }

    // CPU time aggregation is only supported on Linux.
    if (isLinux) {
        // Ensure the CPU time is increasing.
        let lastCpuTime = getServerStatusMetrics(adminDB).cpuNanos;
        assert.gt(lastCpuTime, initialCpuTime);

        // Ensure the global CPU time matches the aggregated time for both databases.
        assert.eq(lastCpuTime - initialCpuTime,
                  allMetrics[db1Name].cpuNanos + allMetrics[db2Name].cpuNanos);
    }

    // Metrics for these databases should not be collected or reported.
    assert.eq(allMetrics['admin'], undefined);
    assert.eq(allMetrics['local'], undefined);
    assert.eq(allMetrics['config'], undefined);

    // Ensure this stage can be composed with other pipeline stages.
    const newDbName = "newDB";
    const newCollName = "metrics_out";
    cursor = adminDB.aggregate([
        {$operationMetrics: {}},
        {$project: {db: 1}},
        {$out: {db: newDbName, coll: newCollName}},
    ]);

    // No results from the aggregation because of the $out.
    assert.eq(cursor.itcount(), 0);

    // There are no additional metrics for the new database because the command was run on the
    // 'admin' database and it does not collect metrics.
    cursor = adminDB.aggregate([{$operationMetrics: {}}]);
    assert.eq(cursor.itcount(), 2);

    // Metrics should not have changed.
    allMetrics = getDBMetrics(adminDB);
    if (node === primary) {
        assert.eq(allMetrics[db1Name].primaryMetrics.docBytesRead, lastDocBytesRead);
        assert.eq(allMetrics[db2Name].primaryMetrics.docBytesRead, lastDocBytesRead);
    } else {
        assert.eq(allMetrics[db1Name].secondaryMetrics.docBytesRead, lastDocBytesRead);
        assert.eq(allMetrics[db2Name].secondaryMetrics.docBytesRead, lastDocBytesRead);
    }

    // Ensure the output collection has the 2 databases that existed at the start of the operation.
    rst.awaitReplication();
    cursor = node.getDB(newDbName)[newCollName].find({});
    assert.eq(cursor.itcount(), 2);

    primary.getDB(newDbName).dropDatabase();

    // Fetch and don't clear metrics.
    cursor = adminDB.aggregate([{$operationMetrics: {clearMetrics: false}}]);
    assert.eq(cursor.itcount(), 3);

    // Fetch and clear metrics.
    cursor = adminDB.aggregate([{$operationMetrics: {clearMetrics: true}}]);
    assert.eq(cursor.itcount(), 3);

    // Ensure no metrics are reported.
    cursor = adminDB.aggregate([{$operationMetrics: {}}]);
    assert.eq(cursor.itcount(), 0);

    // Ensure the serverStatus metrics are cleared except for cpuNanos.
    ssMetrics = getServerStatusMetrics(adminDB);
    assert.eq(0, ssMetrics.numMetrics);
    assert.eq(0, ssMetrics.memUsage);
    if (isLinux) {
        assert.neq(0, ssMetrics.cpuNanos);
    } else {
        assert.eq(0, ssMetrics.cpuNanos);
    }

    // Insert something and ensure metrics are still reporting.
    assert.commandWorked(db1.coll3.insert({a: 1}));
    rst.awaitReplication();

    // On the primary, this insert's metrics should be recorded, but not on the secondary. Since it
    // is applied by the batch applier on the secondary, it is not a user operation and should not
    // count toward any metrics.
    cursor = adminDB.aggregate([{$operationMetrics: {}}]);
    if (node === primary) {
        assert.eq(cursor.itcount(), 1);
    } else {
        assert.eq(cursor.itcount(), 0);
    }
    db1.coll3.drop();
});

rst.stopSet();
}());

/**
 * Tests that using an aggregation cursor when the underlying PlanExecutor has been killed results
 * in an error. Also tests that if the pipeline has already read all results from a collection
 * before the collection is dropped, the aggregation should succeed.
 *
 * This test issues getMores on aggregation cursors and expects the getMore to cause the aggregation
 * to request more documents from the collection. If the pipeline is wrapped in a $facet stage, all
 * results will be computed in the initial request and buffered in the results array, preventing the
 * pipeline from requesting more documents.
 * @tags: [
 *   do_not_wrap_aggregations_in_facets,
 *   requires_capped,
 * ]
 */
(function() {
'use strict';

load("jstests/libs/curop_helpers.js");  // For waitForCurOpByFailPoint().

// This test runs a getMore in a parallel shell, which will not inherit the implicit session of
// the cursor establishing command.
TestData.disableImplicitSessions = true;

// The DocumentSourceCursor which wraps PlanExecutors will batch results internally. We use the
// 'internalDocumentSourceCursorBatchSizeBytes' parameter to disable this behavior so that we
// can easily pause a pipeline in a state where it will need to request more results from the
// PlanExecutor.
const options = {
    setParameter: 'internalDocumentSourceCursorBatchSizeBytes=1'
};
const conn = MongoRunner.runMongod(options);
assert.neq(null, conn, 'mongod was unable to start up with options: ' + tojson(options));

const testDB = conn.getDB('test');

// Make sure the number of results is greater than the batchSize to ensure the results
// cannot all fit in one batch.
const batchSize = 2;
const numMatches = batchSize + 1;
const sourceCollection = testDB.source;
const foreignCollection = testDB.foreign;

/**
 * Populates both 'sourceCollection' and 'foreignCollection' with values of 'local' and
 * 'foreign' in the range [0, 'numMatches').
 */
function setup() {
    sourceCollection.drop();
    foreignCollection.drop();
    for (let i = 0; i < numMatches; ++i) {
        assert.commandWorked(sourceCollection.insert({_id: i, local: i}));

        // We want to be able to pause a $lookup stage in a state where it has returned some but
        // not all of the results for a single lookup, so we need to insert at least
        // 'numMatches' matches for each source document.
        for (let j = 0; j < numMatches; ++j) {
            assert.commandWorked(foreignCollection.insert({_id: numMatches * i + j, foreign: i}));
        }
    }
}

// Check that there are no cursors still open on the source collection. If any are found, the
// test will fail and print a list of idle cursors. This should be called each time we
// expect a cursor to have been destroyed.
function assertNoOpenCursorsOnSourceCollection() {
    const cursors = testDB.getSiblingDB("admin")
                        .aggregate([
                            {"$currentOp": {"idleCursors": true}},
                            {
                                "$match": {ns: sourceCollection.getFullName(), "type": "idleCursor"}

                            }
                        ])
                        .toArray();
    assert.eq(
        cursors.length, 0, "Did not expect to find any cursors, but found " + tojson(cursors));
}

const defaultAggregateCmdSmallBatch = {
    aggregate: sourceCollection.getName(),
    pipeline: [],
    cursor: {
        batchSize: batchSize,
    },
};

// Test that dropping the source collection between an aggregate and a getMore will cause an
// aggregation pipeline to fail during the getMore if it needs to fetch more results from the
// collection.
setup();
let res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));

sourceCollection.drop();

let getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    [ErrorCodes.QueryPlanKilled, ErrorCodes.NamespaceNotFound],
    'expected getMore to fail because the source collection was dropped');

// Make sure the cursors were cleaned up.
assertNoOpenCursorsOnSourceCollection();

// Test that dropping the source collection between an aggregate and a getMore will *not* cause
// an aggregation pipeline to fail during the getMore if it *does not need* to fetch more
// results from the collection.
//
// The test expects that the $sort will execute in the agg layer, and will not be pushed down into
// the PlanStage layer. We add an $_internalInhibitOptimization stage to enforce this.
setup();
res = assert.commandWorked(testDB.runCommand({
    aggregate: sourceCollection.getName(),
    pipeline: [{$_internalInhibitOptimization: {}}, {$sort: {x: 1}}],
    cursor: {
        batchSize: batchSize,
    },
}));

sourceCollection.drop();

getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
assert.commandWorked(testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}));

// Test that dropping a $lookup stage's foreign collection between an aggregate and a getMore
// will *not* cause an aggregation pipeline to fail during the getMore if it needs to fetch more
// results from the foreign collection. It will instead return no matches for subsequent
// lookups, as if the foreign collection was empty.
setup();
res = assert.commandWorked(testDB.runCommand({
        aggregate: sourceCollection.getName(),
        pipeline: [
            {
              $lookup: {
                  from: foreignCollection.getName(),
                  localField: 'local',
                  foreignField: 'foreign',
                  as: 'results',
              }
            },
        ],
        cursor: {
            batchSize: batchSize,
        },
    }));

foreignCollection.drop();
getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
res = testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName});
assert.commandWorked(res,
                     'expected getMore to succeed despite the foreign collection being dropped');
res.cursor.nextBatch.forEach(function(aggResult) {
    assert.eq(aggResult.results,
              [],
              'expected results of $lookup into non-existent collection to be empty');
});

// Make sure the cursors were cleaned up.
assertNoOpenCursorsOnSourceCollection();

// Test that a $lookup stage will properly clean up its cursor if it becomes invalidated between
// batches of a single lookup. This is the same scenario as above, but with the $lookup stage
// left in a state where it has returned some but not all of the matches for a single lookup.
setup();
res = assert.commandWorked(testDB.runCommand({
        aggregate: sourceCollection.getName(),
        pipeline: [
            {
              $lookup: {
                  from: foreignCollection.getName(),
                  localField: 'local',
                  foreignField: 'foreign',
                  as: 'results',
              }
            },
            // Use an $unwind stage to allow the $lookup stage to return some but not all of the
            // results for a single lookup.
            {$unwind: '$results'},
        ],
        cursor: {
            batchSize: batchSize,
        },
    }));

foreignCollection.drop();
getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    [ErrorCodes.QueryPlanKilled, ErrorCodes.NamespaceNotFound],
    'expected getMore to fail because the foreign collection was dropped');

// Make sure the cursors were cleaned up.
assertNoOpenCursorsOnSourceCollection();

// Test that dropping a $graphLookup stage's foreign collection between an aggregate and a
// getMore will *not* cause an aggregation pipeline to fail during the getMore if it needs to
// fetch more results from the foreign collection. It will instead return no matches for
// subsequent lookups, as if the foreign collection was empty.
setup();
res = assert.commandWorked(testDB.runCommand({
        aggregate: sourceCollection.getName(),
        pipeline: [
            {
              $graphLookup: {
                  from: foreignCollection.getName(),
                  startWith: '$local',
                  connectFromField: '_id',
                  connectToField: 'foreign',
                  as: 'results',
              }
            },
        ],
        cursor: {
            batchSize: batchSize,
        },
    }));

foreignCollection.drop();
getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
res = testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName});
assert.commandWorked(res,
                     'expected getMore to succeed despite the foreign collection being dropped');

// Make sure the cursors were cleaned up.
assertNoOpenCursorsOnSourceCollection();

// Test that the getMore still succeeds if the $graphLookup is followed by an $unwind on the
// 'as' field and the collection is dropped between the initial request and a getMore.
setup();
res = assert.commandWorked(testDB.runCommand({
        aggregate: sourceCollection.getName(),
        pipeline: [
            {
              $graphLookup: {
                  from: foreignCollection.getName(),
                  startWith: '$local',
                  connectFromField: '_id',
                  connectToField: 'foreign',
                  as: 'results',
              }
            },
            {$unwind: '$results'},
        ],
        cursor: {
            batchSize: batchSize,
        },
    }));

foreignCollection.drop();
getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
res = testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName});
assert.commandWorked(res,
                     'expected getMore to succeed despite the foreign collection being dropped');

// Make sure the cursors were cleaned up.
assertNoOpenCursorsOnSourceCollection();

// Test that dropping the database will kill an aggregation's cursor, causing a subsequent
// getMore to fail.
setup();
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));

assert.commandWorked(sourceCollection.getDB().dropDatabase());
getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);

assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    [ErrorCodes.QueryPlanKilled, ErrorCodes.NamespaceNotFound],
    'expected getMore to fail because the database was dropped');

assertNoOpenCursorsOnSourceCollection();

// Test that killing an aggregation's cursor by inserting enough documents to force a truncation
// of a capped collection will cause a subsequent getMore to fail.
sourceCollection.drop();
foreignCollection.drop();
const maxCappedSizeBytes = 64 * 1024;
const maxNumDocs = 10;
assert.commandWorked(testDB.runCommand(
    {create: sourceCollection.getName(), capped: true, size: maxCappedSizeBytes, max: maxNumDocs}));
// Fill up about half of the collection.
for (let i = 0; i < maxNumDocs / 2; ++i) {
    assert.commandWorked(sourceCollection.insert({_id: i}));
}
// Start an aggregation.
assert.gt(maxNumDocs / 2, batchSize);
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));
// Insert enough to force a truncation.
for (let i = maxNumDocs / 2; i < 2 * maxNumDocs; ++i) {
    assert.commandWorked(sourceCollection.insert({_id: i}));
}
assert.eq(maxNumDocs, sourceCollection.count());
assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    ErrorCodes.CappedPositionLost,
    'expected getMore to fail because the capped collection was truncated');

// Test that killing an aggregation's cursor via the killCursors command will cause a subsequent
// getMore to fail.
setup();
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));

const killCursorsNamespace = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
assert.commandWorked(
    testDB.runCommand({killCursors: killCursorsNamespace, cursors: [res.cursor.id]}));

assertNoOpenCursorsOnSourceCollection();

assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    ErrorCodes.CursorNotFound,
    'expected getMore to fail because the cursor was killed');

// Test that killing an aggregation's operation via the killOp command will cause a getMore to
// fail.
setup();
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));

// Use a failpoint to cause a getMore to hang indefinitely.
assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'waitAfterPinningCursorBeforeGetMoreBatch', mode: 'alwaysOn'}));
const curOpFilter = {
    'command.getMore': res.cursor.id
};
assert.eq(0, testDB.currentOp(curOpFilter).inprog.length);

getMoreCollName = res.cursor.ns.substr(res.cursor.ns.indexOf('.') + 1);
const parallelShellCode = 'assert.commandFailedWithCode(db.getSiblingDB(\'' + testDB.getName() +
    '\').runCommand({getMore: ' + res.cursor.id.toString() + ', collection: \'' + getMoreCollName +
    '\'}), ErrorCodes.Interrupted, \'expected getMore command to be interrupted by killOp\');';

// Start a getMore and wait for it to hang.
const awaitParallelShell = startParallelShell(parallelShellCode, conn.port);
assert.soon(function() {
    return assert.commandWorked(testDB.currentOp(curOpFilter)).inprog.length === 1;
}, 'expected getMore operation to remain active');

// Wait until we know the failpoint has been reached.
waitForCurOpByFailPointNoNS(testDB, "waitAfterPinningCursorBeforeGetMoreBatch");

// Kill the operation.
const opId = assert.commandWorked(testDB.currentOp(curOpFilter)).inprog[0].opid;
assert.commandWorked(testDB.killOp(opId));
assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'waitAfterPinningCursorBeforeGetMoreBatch', mode: 'off'}));
assert.eq(0, awaitParallelShell());

assertNoOpenCursorsOnSourceCollection();

assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    ErrorCodes.CursorNotFound,
    'expected getMore to fail because the cursor was killed');

// Test that a cursor timeout of an aggregation's cursor will cause a subsequent getMore to
// fail.
setup();
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));

let serverStatus = assert.commandWorked(testDB.serverStatus());
const expectedNumTimedOutCursors = serverStatus.metrics.cursor.timedOut + 1;

// Wait until the idle cursor background job has killed the aggregation cursor.
assert.commandWorked(testDB.adminCommand({setParameter: 1, cursorTimeoutMillis: 10}));
const cursorTimeoutFrequencySeconds = 1;
assert.commandWorked(testDB.adminCommand(
    {setParameter: 1, clientCursorMonitorFrequencySecs: cursorTimeoutFrequencySeconds}));
assert.soon(
    function() {
        serverStatus = assert.commandWorked(testDB.serverStatus());
        return serverStatus.metrics.cursor.timedOut == expectedNumTimedOutCursors;
    },
    function() {
        return 'aggregation cursor failed to time out, expected ' + expectedNumTimedOutCursors +
            ' timed out cursors: ' + tojson(serverStatus.metrics.cursor);
    });

assertNoOpenCursorsOnSourceCollection();
assert.commandFailedWithCode(
    testDB.runCommand({getMore: res.cursor.id, collection: getMoreCollName}),
    ErrorCodes.CursorNotFound,
    'expected getMore to fail because the cursor was killed');

// Test that a cursor will properly be cleaned up on server shutdown.
setup();
res = assert.commandWorked(testDB.runCommand(defaultAggregateCmdSmallBatch));
assert.eq(0, MongoRunner.stopMongod(conn), 'expected mongod to shutdown cleanly');
})();

// Tests that a source collection namespace is correctly logged in the global log for an aggregate
// command when a pipeline contains a stage that can write into an output collection.
// @tags: [requires_profiling]
(function() {
'use strict';

load("jstests/aggregation/extras/merge_helpers.js");  // For withEachKindOfWriteStage.
load("jstests/libs/logv2_helpers.js");

// Runs the given 'pipeline' and verifies that the namespace is correctly logged in the global
// log for the aggregate command. The 'comment' parameter is used to match a log entry against
// the aggregate command.
function verifyLoggedNamespace({pipeline, comment}) {
    assert.commandWorked(db.runCommand(
        {aggregate: source.getName(), comment: comment, pipeline: pipeline, cursor: {}}));
    if (isJsonLogNoConn()) {
        checkLog.containsWithCount(
            conn,
            `"appName":"MongoDB Shell",` +
                `"command":{"aggregate":"${source.getName()}","comment":"${comment}"`,
            1);

    } else {
        checkLog.containsWithCount(
            conn,
            `command ${source.getFullName()} appName: "MongoDB Shell" ` +
                `command: aggregate { aggregate: "${source.getName()}", comment: "${comment}"`,
            1);
    }
}

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);
assert.neq(null, conn, `mongod failed to start with options ${tojson(mongodOptions)}`);

const db = conn.getDB(`${jsTest.name()}_db`);
const source = db.getCollection(`${jsTest.name()}_source`);
source.drop();
const target = db.getCollection(`${jsTest.name()}_target`);
target.drop();

// Make sure each command gets logged.
assert.commandWorked(db.setProfilingLevel(1, {slowms: 0}));

// Test stages that can write into an output collection.
withEachKindOfWriteStage(
    target, (stage) => verifyLoggedNamespace({pipeline: [stage], comment: Object.keys(stage)[0]}));

// Test each $merge mode.
withEachMergeMode(({whenMatchedMode, whenNotMatchedMode}) => verifyLoggedNamespace({
                      pipeline: [{
                          $merge: {
                              into: target.getName(),
                              whenMatched: whenMatchedMode,
                              whenNotMatched: whenNotMatchedMode
                          }
                      }],
                      comment: `merge_${whenMatchedMode}_${whenNotMatchedMode}`
                  }));

MongoRunner.stopMongod(conn);
})();

/**
 * Basic test to verify that an aggregation pipeline using $out targeting a replica set secondary
 * performs the reads on the secondaries and executes any write commands against the replica set
 * primary.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */

(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // for anyEq.

const nDocs = 100;
const readCollName = "readColl";
const outCollName = "outColl";
const dbName = "out_on_secondary_db";
let rs = new ReplSetTest({nodes: 2});
rs.startSet();
rs.initiate();
rs.awaitReplication();

const primary = rs.getPrimary();
const primaryDB = primary.getDB(dbName);
const readColl = primaryDB[readCollName];

const secondary = rs.getSecondary();
const secondaryDB = secondary.getDB(dbName);
const replSetConn = new Mongo(rs.getURL());
replSetConn.setReadPref("secondary");
const db = replSetConn.getDB(dbName);
// Keeps track of values which we expect the aggregate command to return.
let expectedResults = [];
// Insert some documents which our pipeline will eventually read from.
for (let i = 0; i < nDocs; i++) {
    assert.commandWorked(
        readColl.insert({_id: i, groupKey: i % 10, num: i}, {writeConcern: {w: 2}}));
    if (i < 10) {
        expectedResults.push({_id: i, sum: i});
    } else {
        expectedResults[i % 10].sum += i;
    }
}

assert.commandWorked(primaryDB.setProfilingLevel(2));
assert.commandWorked(secondaryDB.setProfilingLevel(2));

const pipeline = [{$group: {_id: "$groupKey", sum: {$sum: "$num"}}}, {$out: outCollName}];
const comment = "$out issued to secondary";
assert.eq(db[readCollName].aggregate(pipeline, {comment: comment}).itcount(), 0);

// Verify that $out wrote to the primary and that query is correct.
assert(anyEq(primaryDB[outCollName].find().toArray(), expectedResults));

// Verify that $group was executed on the secondary.
const secondaryProfile =
    secondaryDB.system.profile.find({"command.aggregate": "readColl", "command.comment": comment})
        .itcount();
assert.eq(1, secondaryProfile);

// Verify $out's operations were executed on the primary.
const primaryProfile =
    primaryDB.system.profile
        .find({"command.internalRenameIfOptionsAndIndexesMatch": 1, "command.comment": comment})
        .itcount();
assert.eq(1, primaryProfile);

rs.stopSet();
}());
/**
 * Tests that a batch size of zero can be used for aggregation commands, and all data can be
 * retrieved via getMores.
 */
(function() {
"use strict";

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);
assert.neq(null, conn, "mongod failed to start with options " + tojson(mongodOptions));

const testDB = conn.getDB("test");
const coll = testDB[jsTest.name];
coll.drop();

// Test that an aggregate is successful on a non-existent collection.
assert.eq(0,
          coll.aggregate([]).toArray().length,
          "expected no results from an aggregation on an empty collection");

// Test that an aggregate is successful on a non-existent collection with a batchSize of 0, and
// that a getMore will succeed with an empty result set.
let res = assert.commandWorked(
    testDB.runCommand({aggregate: coll.getName(), pipeline: [], cursor: {batchSize: 0}}));

let cursor = new DBCommandCursor(testDB, res);
assert.eq(
    0, cursor.itcount(), "expected no results from getMore of aggregation on empty collection");

// Test that an aggregation can return *all* matching data via getMores if the initial aggregate
// used a batchSize of 0.
const nDocs = 1000;
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < nDocs; i++) {
    bulk.insert({_id: i, stringField: "string"});
}
assert.commandWorked(bulk.execute());

res = assert.commandWorked(
    testDB.runCommand({aggregate: coll.getName(), pipeline: [], cursor: {batchSize: 0}}));
cursor = new DBCommandCursor(testDB, res);
assert.eq(nDocs, cursor.itcount(), "expected all results to be returned via getMores");

// Test that an error in a getMore will destroy the cursor.
function assertNumOpenCursors(nExpectedOpen) {
    let serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    assert.eq(nExpectedOpen,
              serverStatus.metrics.cursor.open.total,
              "expected to find " + nExpectedOpen +
                  " open cursor(s): " + tojson(serverStatus.metrics.cursor));
}

// Issue an aggregate command that will fail *at runtime*, so the error will happen in a
// getMore.
assertNumOpenCursors(0);
res = assert.commandWorked(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$project: {invalidComputation: {$add: [1, "$stringField"]}}}],
    cursor: {batchSize: 0}
}));
cursor = new DBCommandCursor(testDB, res);
assertNumOpenCursors(1);

assert.throws(() => cursor.itcount(), [], "expected getMore to fail");
assertNumOpenCursors(0);

// Test that an error in a getMore using a $out stage will destroy the cursor. This test is
// intended to reproduce SERVER-26608.

// Issue an aggregate command that will fail *at runtime*, so the error will happen in a
// getMore.
res = assert.commandWorked(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$out: "validated_collection"}],
    cursor: {batchSize: 0}
}));
cursor = new DBCommandCursor(testDB, res);
assertNumOpenCursors(1);

// Add a document validation rule to the $out collection so that insertion will fail.
assert.commandWorked(
    testDB.runCommand({create: "validated_collection", validator: {stringField: {$type: "int"}}}));

assert.throws(() => cursor.itcount(), [], "expected getMore to fail");
assertNumOpenCursors(0);

MongoRunner.stopMongod(conn);
}());

// Tests for whether the query solution correctly used an AND_HASH for index intersection.
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertArrayEq.
load("jstests/libs/analyze_plan.js");  // For planHasStage helper to analyze explain() output.

const conn = MongoRunner.runMongod();
const db = conn.getDB("test");

// Enable and force AND_HASH query solution to be used to evaluate index intersections.
assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryForceIntersectionPlans: true}));
assert.commandWorked(
    db.adminCommand({setParameter: 1, internalQueryPlannerEnableHashIntersection: true}));

const coll = db.and_hash;
coll.drop();

assert.commandWorked(coll.createIndex({a: 1}));
assert.commandWorked(coll.createIndex({b: 1}));
assert.commandWorked(coll.createIndex({c: -1}));
assert.commandWorked(coll.createIndex({e: 1}));
assert.commandWorked(coll.createIndex({f: 1}));

assert.commandWorked(coll.insertMany([
    {_id: 0, a: 1, b: "A", c: 22, d: "field"},
    {_id: 1, a: 1, b: "B", c: 22, d: "field"},
    {_id: 2, a: 1, b: "C", c: 22, d: "field"},
    {_id: 3, a: 2, b: 2, c: null},
    {_id: 4, a: 2, b: "D", c: 22},
    {_id: 5, a: 2, b: "E", c: 22, d: 99},
    {_id: 6, a: 3, b: "b", c: 23, d: "field"},
    {_id: 7, a: 3, b: "abc", c: 22, d: 23},
    {_id: 8, a: 3, b: "A", c: 22, d: "field"},
    {_id: 9, a: 4, b: "a", c: {x: 1, y: 2}, d: "field"},
    {_id: 10, a: 5, b: "ABC", d: 22, c: "field"},
    {_id: 11, a: 6, b: "ABC", d: 22, c: "field"},
    {_id: 12, a: 4, b: "a", c: {x: 1, y: 2}, d: "field", e: [1, 2, 3]},
    {_id: 13, a: 5, b: "ABC", d: 22, c: "field", e: [4, 5, 6]},
    {_id: 14, a: 6, b: "ABC", d: 22, c: "field", e: [7, 8, 9]},
    {_id: 15, a: 1, e: [7, 8, 9], f: [-1, -2, -3]},
]));

// Helper to check the result returned by the query and to check whether the
// query solution correctly did or did not use an AND_HASH for index
// intersection.
function assertAndHashUsed({query, expectedResult, shouldUseAndHash} = {}) {
    const queryResult = coll.find(query);
    const expl = queryResult.explain();

    assertArrayEq({actual: queryResult.toArray(), expected: expectedResult});
    assert.eq(shouldUseAndHash, planHasStage(db, getWinningPlan(expl.queryPlanner), "AND_HASH"));
}

// Test basic index intersection where we expect AND_HASH to be used.
assertAndHashUsed({
    query: {a: {$gt: 1}, c: null},
    expectedResult: [{_id: 3, a: 2, b: 2, c: null}],
    shouldUseAndHash: true
});
assertAndHashUsed({
    query: {a: {$gt: 3}, c: {x: 1, y: 2}},
    expectedResult: [
        {_id: 9, a: 4, b: "a", c: {x: 1, y: 2}, d: "field"},
        {_id: 12, a: 4, b: "a", c: {x: 1, y: 2}, d: "field", e: [1, 2, 3]}
    ],
    shouldUseAndHash: true
});
assertAndHashUsed({
    query: {a: {$lt: 5}, b: {$in: ["A", "abc"]}},
    expectedResult: [
        {_id: 0, a: 1, b: "A", c: 22, d: "field"},
        {_id: 7, a: 3, b: "abc", c: 22, d: 23},
        {_id: 8, a: 3, b: "A", c: 22, d: "field"},
    ],
    shouldUseAndHash: true
});
assertAndHashUsed({
    query: {a: {$gt: 1}, e: {$elemMatch: {$lt: 7}}},
    expectedResult: [
        {_id: 12, a: 4, b: "a", c: {x: 1, y: 2}, d: "field", e: [1, 2, 3]},
        {_id: 13, a: 5, b: "ABC", d: 22, c: "field", e: [4, 5, 6]},
    ],
    shouldUseAndHash: true
});
assertAndHashUsed({query: {a: {$gt: 5}, c: {$lt: 3}}, expectedResult: [], shouldUseAndHash: true});
assertAndHashUsed({query: {a: {$gt: 5}, c: null}, expectedResult: [], shouldUseAndHash: true});
assertAndHashUsed({query: {a: {$gt: 1}, c: {$lt: 3}}, expectedResult: [], shouldUseAndHash: true});

// Test queries that should not use AND_HASH.
assertAndHashUsed({
    query: {a: 6},
    expectedResult: [
        {_id: 11, a: 6, b: "ABC", d: 22, c: "field"},
        {_id: 14, a: 6, b: "ABC", d: 22, c: "field", e: [7, 8, 9]}
    ],
    shouldUseAndHash: false
});
assertAndHashUsed({query: {fieldDoesNotExist: 1}, expectedResult: [], shouldUseAndHash: false});
assertAndHashUsed({
    query: {$or: [{a: 6}, {fieldDoesNotExist: 1}]},
    expectedResult: [
        {_id: 11, a: 6, b: "ABC", d: 22, c: "field"},
        {_id: 14, a: 6, b: "ABC", d: 22, c: "field", e: [7, 8, 9]}
    ],
    shouldUseAndHash: false
});
assertAndHashUsed({query: {$or: [{a: 7}, {a: 8}]}, expectedResult: [], shouldUseAndHash: false});

// Test intersection with a compound index.
assert.commandWorked(coll.createIndex({c: 1, d: -1}));
assertAndHashUsed({
    query: {a: {$gt: 1}, c: {$gt: 0}, d: {$gt: 90}},
    expectedResult: [{_id: 5, a: 2, b: "E", c: 22, d: 99}],
    shouldUseAndHash: true
});
// The query on only 'd' field should not be able to use the index and thus the intersection
// since it is not using a prefix of the compound index {c:1, d:-1}.
assertAndHashUsed({
    query: {a: {$gt: 1}, d: {$gt: 90}},
    expectedResult: [{_id: 5, a: 2, b: "E", c: 22, d: 99}],
    shouldUseAndHash: false
});

// Test that deduplication is correctly performed on top of the index scans that comprise the
// hash join -- predicate matching multiple elements of array should not return the same
// document more than once.
assertAndHashUsed({
    query: {e: {$gt: 0}, a: 6},
    expectedResult: [{_id: 14, a: 6, b: "ABC", d: 22, c: "field", e: [7, 8, 9]}],
    shouldUseAndHash: true
});
assertAndHashUsed({
    query: {e: {$gt: 0}, f: {$lt: 0}},
    expectedResult: [{_id: 15, a: 1, e: [7, 8, 9], f: [-1, -2, -3]}],
    shouldUseAndHash: true
});

MongoRunner.stopMongod(conn);
})();

// Tests for whether the query solution correctly used an AND_SORTED stage for index intersection.
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertArrayEq.
load("jstests/libs/analyze_plan.js");  // For planHasStage helper to analyze explain() output.

const conn = MongoRunner.runMongod();
const db = conn.getDB("test");

// Enable and force index intersections plans.
assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryForceIntersectionPlans: true}));

function runAndSortedTests() {
    const coll = db.and_sorted;
    coll.drop();

    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.createIndex({b: 1}));
    assert.commandWorked(coll.createIndex({c: -1}));

    assert.commandWorked(coll.insertMany([
        {_id: 0, a: 1, b: 1, c: 1},
        {_id: 1, a: 2, b: -1, c: 1},
        {_id: 2, a: 0, b: 0, c: 10},
        {_id: 3, a: 10, b: 1, c: -1}
    ]));

    // Helper to check the result returned by the query and to check whether the query solution
    // correctly did or did not use an AND_SORTED for index intersection.
    function assertAndSortedUsed({query, expectedResult, shouldUseAndSorted} = {}) {
        const queryResult = coll.find(query);
        const expl = queryResult.explain();

        assertArrayEq({actual: queryResult.toArray(), expected: expectedResult});
        assert.eq(shouldUseAndSorted,
                  planHasStage(db, getWinningPlan(expl.queryPlanner), "AND_SORTED"));
    }

    // Test basic index intersection where we expect AND_SORTED to be used.
    assertAndSortedUsed({
        query: {a: 1, b: 1},
        expectedResult: [{_id: 0, a: 1, b: 1, c: 1}],
        shouldUseAndSorted: true
    });

    assert.commandWorked(coll.insertMany([
        {_id: 4, a: 100, b: 100, c: 1},
        {_id: 5, a: 100, b: 100, c: 2},
        {_id: 6, a: 100, b: 100, c: 3},
        {_id: 7, a: 100, b: 100, c: 1}
    ]));

    assertAndSortedUsed({
        query: {a: 100, c: 1},
        expectedResult: [{_id: 4, a: 100, b: 100, c: 1}, {_id: 7, a: 100, b: 100, c: 1}],
        shouldUseAndSorted: true
    });
    assertAndSortedUsed({
        query: {a: 100, b: 100, c: 1},
        expectedResult: [{_id: 4, a: 100, b: 100, c: 1}, {_id: 7, a: 100, b: 100, c: 1}],
        shouldUseAndSorted: true
    });
    assertAndSortedUsed({
        query: {a: 100, b: 100, c: 2},
        expectedResult: [{_id: 5, a: 100, b: 100, c: 2}],
        shouldUseAndSorted: true
    });

    assert.commandWorked(coll.insertMany(
        [{_id: 8, c: 1, d: 1, e: 1}, {_id: 9, c: 1, d: 2, e: 2}, {_id: 10, c: 1, d: 2, e: 3}]));
    assert.commandWorked(coll.createIndex({e: 1}));

    // Test where shouldn't use AND_SORTED since no index exists on one of the fields or the query
    // is on a single field.
    assertAndSortedUsed({
        query: {c: 1, d: 2},
        expectedResult: [{_id: 9, c: 1, d: 2, e: 2}, {_id: 10, c: 1, d: 2, e: 3}],
        shouldUseAndSorted: false
    });
    assertAndSortedUsed(
        {query: {e: 1}, expectedResult: [{_id: 8, c: 1, d: 1, e: 1}], shouldUseAndSorted: false});

    // Test on an empty collection.
    assert(coll.drop());
    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.createIndex({b: 1}));

    assertAndSortedUsed({query: {a: 1, b: 1}, expectedResult: [], shouldUseAndSorted: true});

    // Test more than two branches.
    assert(coll.drop());
    assert.commandWorked(coll.insertMany([
        {_id: 1, a: 1, b: 2, c: 5, d: 9, e: 5},
        {_id: 2, a: 1, b: 2, c: 3, d: 4, e: 5},
        {_id: 3, a: 1, b: 2, c: 3, d: 4, e: 6},
        {_id: 4, a: 1, b: 4, c: 3, d: 4, e: 5}
    ]));
    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.createIndex({b: 1}));
    assert.commandWorked(coll.createIndex({c: 1}));
    assert.commandWorked(coll.createIndex({d: 1}));
    assert.commandWorked(coll.createIndex({e: 1}));

    assertAndSortedUsed({
        query: {a: 1, b: 2, c: 3, d: 4, e: 5},
        expectedResult: [{_id: 2, a: 1, b: 2, c: 3, d: 4, e: 5}],
        shouldUseAndSorted: true
    });

    // Test with arrays, strings, and non-scalar predicates.
    assert(coll.drop());
    assert.commandWorked(coll.insertMany([
        {_id: 1, a: 1, b: [1, 2, 3], c: "c"},
        {_id: 2, a: [1, 2, 3], b: 2, c: "c"},
        {_id: 3, a: 2, b: "b", c: ["a", "b", "c"]}
    ]));
    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.createIndex({b: 1}));
    assert.commandWorked(coll.createIndex({c: 1}));

    assertAndSortedUsed({
        query: {a: 1, c: "c"},
        expectedResult:
            [{_id: 1, a: 1, b: [1, 2, 3], c: "c"}, {_id: 2, a: [1, 2, 3], b: 2, c: "c"}],
        shouldUseAndSorted: true
    });
    assertAndSortedUsed({
        query: {a: 1, b: 2},
        expectedResult:
            [{_id: 1, a: 1, b: [1, 2, 3], c: "c"}, {_id: 2, a: [1, 2, 3], b: 2, c: "c"}],
        shouldUseAndSorted: true
    });
    assertAndSortedUsed({
        query: {a: 2, c: "c"},
        expectedResult:
            [{_id: 2, a: [1, 2, 3], b: 2, c: "c"}, {_id: 3, a: 2, b: "b", c: ["a", "b", "c"]}],
        shouldUseAndSorted: true
    });
    assertAndSortedUsed({
        query: {a: 2, c: {"$size": 3}},
        expectedResult: [{_id: 3, a: 2, b: "b", c: ["a", "b", "c"]}],
        shouldUseAndSorted: false
    });
}

runAndSortedTests();

// Re-run the tests now with 'internalQueryExecYieldIterations' set to '1' such that yield happens
// after each document is returned.
assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 1}));
runAndSortedTests();

MongoRunner.stopMongod(conn);
})();

/**
 * Checks that API version 2 will behave correctly with mongod/mongos.
 *
 * @tags: [
 *   requires_journaling,
 * ]
 */

(function() {
"use strict";

const runTest = testDB => {
    // Test command in V2 but not V1.
    assert.commandWorked(testDB.runCommand({testVersion2: 1, apiVersion: "2", apiStrict: true}));
    assert.commandFailedWithCode(
        testDB.runCommand({testVersion2: 1, apiVersion: "1", apiStrict: true}),
        ErrorCodes.APIStrictError,
        "testVersion2 is not in API V1");

    // Test command in both V1 and V2.
    assert.commandWorked(
        testDB.runCommand({testVersions1And2: 1, apiVersion: "1", apiStrict: true}));
    assert.commandWorked(
        testDB.runCommand({testVersions1And2: 1, apiVersion: "2", apiStrict: true}));

    // Test command in V1, deprecated in V2.
    assert.commandWorked(testDB.runCommand({
        testDeprecationInVersion2: 1,
        apiVersion: "1",
        apiStrict: true,
        apiDeprecationErrors: true
    }));
    assert.commandFailedWithCode(
        testDB.runCommand({
            testDeprecationInVersion2: 1,
            apiVersion: "2",
            apiStrict: true,
            apiDeprecationErrors: true
        }),
        ErrorCodes.APIDeprecationError,
        "Provided apiDeprecationErrors: true, but testDeprecationInVersion2 is deprecated in V2");

    // Test command in V1, removed in V2.
    assert.commandWorked(testDB.runCommand({testRemoval: 1, apiVersion: "1", apiStrict: true}));
    assert.commandFailedWithCode(
        testDB.runCommand({testRemoval: 1, apiVersion: "2", apiStrict: true}),
        ErrorCodes.APIStrictError,
        "testRemoval is not in API V2");
};

const conn = MongoRunner.runMongod({setParameter: {acceptApiVersion2: true}});
const db = conn.getDB(jsTestName());
runTest(db);
MongoRunner.stopMongod(conn);

const st = new ShardingTest({mongosOptions: {setParameter: {acceptApiVersion2: true}}});
runTest(st.s0.getDB(jsTestName()));
st.stop();
})();

/**
 * Checks that the API version metrics are properly stored and returned.
 *
 * @tags: [
 * ]
 */

(function() {
"use strict";

const apiVersionsFieldName = "apiVersions";
const appName = "apiVersionMetricsTest";
const defaultAppName = "MongoDB Shell";

const conn = MongoRunner.runMongod();
const uri = "mongodb://" + conn.host + "/test";

const testDB = new Mongo(uri + `?appName=${appName}`).getDB(jsTestName());

jsTestLog("Issuing cmd with no API version");
assert.commandWorked(testDB.runCommand({ping: 1}));

let serverStatus = testDB.serverStatus().metrics;
assert(serverStatus.hasOwnProperty(apiVersionsFieldName),
       () => `serverStatus should have an '${apiVersionsFieldName}' field: ${serverStatus}`);

let apiVersionMetrics = serverStatus[apiVersionsFieldName][appName];
assert.eq(["default"], apiVersionMetrics);

jsTestLog("Issuing cmd with API version 1");
assert.commandWorked(testDB.runCommand({ping: 1, apiVersion: "1"}));

serverStatus = testDB.serverStatus().metrics;
assert(serverStatus.hasOwnProperty(apiVersionsFieldName),
       () => `serverStatus should have an '${apiVersionsFieldName}' field: ${serverStatus}`);

apiVersionMetrics = serverStatus[apiVersionsFieldName][appName];
assert.eq(["default", "1"], apiVersionMetrics);

const testDBDefaultAppName = conn.getDB(jsTestName());

jsTestLog("Issuing cmd with default app name");
assert.commandWorked(testDBDefaultAppName.runCommand({ping: 1}));

serverStatus = testDB.serverStatus().metrics;
assert(serverStatus.hasOwnProperty(apiVersionsFieldName),
       () => `serverStatus should have an '${apiVersionsFieldName}' field: ${serverStatus}`);
assert(serverStatus[apiVersionsFieldName].hasOwnProperty(appName),
       () => `serverStatus should store metrics for '${appName}': ${serverStatus}`);
assert(serverStatus[apiVersionsFieldName].hasOwnProperty(defaultAppName),
       () => `serverStatus should store metrics for '${defaultAppName}': ${serverStatus}`);

MongoRunner.stopMongod(conn);
})();

/**
 * Test the shell's --apiVersion and other options related to the MongoDB Versioned API, and
 * test passing API parameters to the Mongo() constructor.
 *
 * @tags: [
 *   requires_journaling,
 * ]
 */

(function() {
'use strict';

const testCases = [
    // [requireApiVersion server parameter, expect success, command, API parameters]
    [false, true, {ping: 1}, {}],
    [false, true, {ping: 1}, {version: '1'}],
    [false, true, {count: 'collection'}, {version: '1'}],
    [false, true, {getLog: 'global'}, {version: '1'}],
    [false, true, {getLog: 'global'}, {version: '1', deprecationErrors: true}],
    // getLog isn't in API Version 1, so it's banned with strict: true.
    [false, false, {getLog: 'global'}, {version: '1', strict: true}],
    [false, true, {ping: 1}, {version: '1', strict: true}],
    [false, true, {testDeprecation: 1}, {version: '1', strict: true}],
    [false, false, {testDeprecation: 1}, {version: '1', deprecationErrors: true}],
    [false, false, {testDeprecation: 1}, {version: '1', strict: true, deprecationErrors: true}],
    // tests with setParameter requireApiVersion: true.
    [true, false, {count: 'collection'}, {version: '1', strict: true}],
    [true, true, {count: 'collection'}, {version: '1'}],
    [true, false, {ping: 1}, {}],
    [true, true, {ping: 1}, {version: '1'}],
];

function runShellWithScript(port, requireApiVersion, expectSuccess, script, api) {
    let shellArgs = [];
    if (api.version) {
        shellArgs.push('--apiVersion', api.version);
    }

    if (api.strict) {
        shellArgs.push('--apiStrict');
    }

    if (api.deprecationErrors) {
        shellArgs.push('--apiDeprecationErrors');
    }

    jsTestLog(`Run shell with script ${script} and args ${tojson(shellArgs)},` +
              ` requireApiVersion = ${requireApiVersion}, expectSuccess = ${expectSuccess}`);

    const result = runMongoProgram.apply(
        null, ['mongo', '--port', port, '--eval', script].concat(shellArgs || []));

    if (expectSuccess) {
        assert.eq(
            result,
            0,
            `Error running shell with script ${tojson(script)} and args ${tojson(shellArgs)}`);
    } else {
        assert.neq(result,
                   0,
                   `Unexpected success running shell with` +
                       ` script ${tojson(script)} and args ${tojson(shellArgs)}`);
    }
}

function runShellWithCommand(port, requireApiVersion, expectSuccess, command, api) {
    // Test runCommand and runReadCommand.
    const scripts = [
        `assert.commandWorked(db.getSiblingDB("admin").runCommand(${tojson(command)}))`,
        `assert.commandWorked(db.getSiblingDB("admin").runReadCommand(${tojson(command)}))`,
    ];

    for (const script of scripts) {
        runShellWithScript(port, requireApiVersion, expectSuccess, script, api);
    }
}

function newMongo(port, requireApiVersion, expectSuccess, command, api) {
    jsTestLog(`Construct Mongo object with command ${tojson(command)} and args ${tojson(api)},` +
              ` requireApiVersion = ${requireApiVersion}, expectSuccess = ${expectSuccess}`);
    if (expectSuccess) {
        const m = new Mongo(
            `mongodb://localhost:${port}`, undefined /* encryptedDBClientCallback */, {api: api});
        const reply = m.adminCommand(command);
        assert.commandWorked(reply, command);
    } else {
        let m;
        try {
            m = new Mongo(`mongodb://localhost:${port}`,
                          undefined /* encryptedDBClientCallback */,
                          {api: api});
        } catch (e) {
            // The constructor threw, but we expected failure.
            print(e);
            return;
        }
        const reply = m.adminCommand(command);
        assert.commandFailed(reply, command);
    }
}

const mongod = MongoRunner.runMongod({verbose: 2});

for (let [requireApiVersion, successExpected, command, api] of testCases) {
    const m = new Mongo(`localhost:${mongod.port}`, undefined, {api: {version: '1'}});
    assert.commandWorked(
        m.getDB("admin").runCommand({setParameter: 1, requireApiVersion: requireApiVersion}));

    runShellWithCommand(mongod.port, requireApiVersion, successExpected, command, api);
    newMongo(mongod.port, requireApiVersion, successExpected, command, api);
}

let m = new Mongo(`localhost:${mongod.port}`, undefined, {api: {version: '1'}});
assert.commandWorked(m.getDB('admin').runCommand(
    {insert: 'collection', documents: [{}, {}, {}, {}, {}, {}], apiVersion: '1'}));

for (let requireApiVersion of [false, true]) {
    // Omit api = {}, that's tested elsewhere.
    for (let api of [{version: '1'},
                     {version: '1', strict: true},
                     {version: '1', deprecationErrors: true},
                     {version: '1', strict: true, deprecationErrors: true},
    ]) {
        assert.commandWorked(
            m.getDB("admin").runCommand({setParameter: 1, requireApiVersion: requireApiVersion}));

        /*
         * Test getMore. Create a cursor with the right API version parameters. Use an explicit lsid
         * to override implicit session creation.
         */
        const lsid = UUID();
        const versionedConn = new Mongo(`localhost:${mongod.port}`, undefined, {api: api});
        const findReply = assert.commandWorked(versionedConn.getDB('admin').runCommand(
            {find: 'collection', batchSize: 1, lsid: {id: lsid}}));
        const getMoreCmd = {
            getMore: findReply.cursor.id,
            collection: 'collection',
            lsid: {id: lsid},
            batchSize: 1
        };

        runShellWithCommand(
            mongod.port, requireApiVersion, true /* expectSuccess */, getMoreCmd, api);
        newMongo(mongod.port, requireApiVersion, true /* expectSuccess */, getMoreCmd, api);

        /*
         * Test unacknowledged writes (OP_MSG with moreToCome).
         */
        versionedConn.getDB('admin')['collection2'].insertMany([{}, {}]);
        const deleteScript = "db.getSiblingDB('admin').collection2.deleteOne({}, {w: 0});";
        runShellWithScript(
            mongod.port, requireApiVersion, true /* expectSuccess */, deleteScript, api);
        // The delete succeeds, collection2 had 2 records, it soon has 1. Use assert.soon since the
        // write is unacknowledged. Use countDocuments which is apiStrict-compatible.
        assert.soon(() => {
            return 1 === versionedConn.getDB('admin')['collection2'].countDocuments({});
        });
        const deleteCmd = {
            delete: 'collection2',
            deletes: [{q: {}, limit: 1}],
            writeConcern: {w: 0}
        };
        newMongo(mongod.port, requireApiVersion, true /* expectSuccess */, deleteCmd, api);
        // The delete succeeds, collection2 soon has 0 records.
        assert.soon(() => {
            return 0 === versionedConn.getDB('admin')['collection2'].countDocuments({});
        });
    }
}

/*
 * Shell-specific tests.
 */

// Version 2 is not supported.
runShellWithCommand(mongod.port, false, false, {ping: 1}, {version: '2'});
// apiVersion is required if strict or deprecationErrors is included
runShellWithCommand(mongod.port, false, false, {ping: 1}, {strict: true});
runShellWithCommand(mongod.port, false, false, {ping: 1}, {deprecationErrors: true});
runShellWithCommand(mongod.port, false, false, {ping: 1}, {strict: true, deprecationErrors: true});

if (m.adminCommand('buildinfo').modules.indexOf('enterprise') > -1) {
    /*
     * Test that we can call buildinfo while assembling the shell prompt, in order to determine that
     * this is MongoDB Enterprise, although buildinfo is not in API Version 1 and the shell is
     * running with --apiStrict.
     */
    const testPrompt = "assert(RegExp('MongoDB Enterprise').test(defaultPrompt()))";
    const result = runMongoProgram(
        'mongo', '--port', mongod.port, '--apiVersion', '1', '--apiStrict', '--eval', testPrompt);
    assert.eq(result, 0, `Error running shell with script '${testPrompt}'`);
}

/*
 * Mongo-specific tests.
 */
assert.throws(() => {
    new Mongo(mongod.port, null, "not an object");
}, [], "Mongo() constructor should check that options argument is an object");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: "not an object"});
}, [], "Mongo() constructor should check that 'api' is an object");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: 1}});
}, [], "Mongo() constructor should reject API version 1 (as a number)");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: '2'}});
}, [], "Mongo() constructor should reject API version 2");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: '1', strict: 1}});
}, [], "Mongo() constructor should reject strict: 1");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: '1', strict: 'asdf'}});
}, [], "Mongo() constructor should reject strict: 'asdf");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: '1', deprecationErrors: 1}});
}, [], "Mongo() constructor should reject deprecationErrors: 1");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {version: '1', deprecationErrors: 'asdf'}});
}, [], "Mongo() constructor should reject deprecationErrors: 'asdf'");

// apiVersion is required if strict or deprecationErrors is included
assert.throws(() => {
    new Mongo(mongod.port, null, {api: {strict: true}});
}, [], "Mongo() constructor should reject 'strict' without 'version'");

assert.throws(() => {
    new Mongo(mongod.port, null, {api: {deprecationErrors: true}});
}, [], "Mongo() constructor should reject 'deprecationErrors' without 'version'");

MongoRunner.stopMongod(mongod);

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const primaryPort = rst.getPrimary().port;

/*
 * Test that we can call replSetGetStatus while assembling the shell prompt, although
 * replSetGetStatus is not in API Version 1 and the shell is running with --apiStrict.
 */
const testPrompt = "assert(RegExp('PRIMARY').test(defaultPrompt()))";
const result = runMongoProgram(
    'mongo', '--port', primaryPort, '--apiVersion', '1', '--apiStrict', '--eval', testPrompt);
assert.eq(result, 0, `Error running shell with script '${testPrompt}'`);

/*
 * Test transaction-continuing commands with API parameters.
 */
m = new Mongo(`localhost:${primaryPort}`, undefined, {api: {version: '1'}});
for (let requireApiVersion of [false, true]) {
    // Omit api = {}, that's tested elsewhere.
    for (let api of [{version: '1'},
                     {version: '1', strict: true},
                     {version: '1', deprecationErrors: true},
                     {version: '1', strict: true, deprecationErrors: true},
    ]) {
        assert.commandWorked(
            m.getDB("admin").runCommand({setParameter: 1, requireApiVersion: requireApiVersion}));

        // Start a transaction with the right API version parameters. Use an explicit lsid to
        // override implicit session creation.
        const lsid = UUID();
        const versionedConn = new Mongo(`localhost:${primaryPort}`, undefined, {api: api});
        assert.commandWorked(versionedConn.getDB('admin').runCommand({
            find: 'collection',
            lsid: {id: lsid},
            txnNumber: NumberLong(1),
            autocommit: false,
            startTransaction: true
        }));

        const continueCmd =
            {find: 'collection', lsid: {id: lsid}, txnNumber: NumberLong(1), autocommit: false};

        runShellWithCommand(
            primaryPort, requireApiVersion, true /* expectSuccess */, continueCmd, api);
        newMongo(primaryPort, requireApiVersion, true /* expectSuccess */, continueCmd, api);
    }
}

assert.commandWorked(m.getDB('admin').runCommand({setParameter: 1, requireApiVersion: false}));

rst.stopSet();
})();

/**
 * Test that applying DDL operation on secondary does not take a global X lock.
 *
 * @tags: [
 *   requires_replication,
 *   requires_snapshot_read,
 * ]
 */

(function() {
'use strict';

const testDBName = 'test';
const readDBName = 'read';
const readCollName = 'readColl';
const testCollName = 'testColl';
const renameCollName = 'renameColl';

const rst = new ReplSetTest({name: jsTestName(), nodes: 2});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const secondary = rst.getSecondary();

assert.commandWorked(
    primary.getDB(readDBName)
        .runCommand({insert: readCollName, documents: [{x: 1}], writeConcern: {w: 2}}));

// The find will hang and holds a global IS lock.
assert.commandWorked(secondary.getDB("admin").runCommand(
    {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "alwaysOn"}));

const findWait = startParallelShell(function() {
    db.getMongo().setSecondaryOk();
    assert.eq(
        db.getSiblingDB('read').getCollection('readColl').find().comment('read hangs').itcount(),
        1);
}, secondary.port);

assert.soon(function() {
    let findOp = secondary.getDB('admin')
                     .aggregate([{$currentOp: {}}, {$match: {'command.comment': 'read hangs'}}])
                     .toArray();
    return findOp.length == 1;
});

{
    // Run a series of DDL commands, none of which should take the global X lock.
    const testDB = primary.getDB(testDBName);
    assert.commandWorked(testDB.runCommand({create: testCollName, writeConcern: {w: 2}}));

    assert.commandWorked(
        testDB.runCommand({collMod: testCollName, validator: {v: 1}, writeConcern: {w: 2}}));

    assert.commandWorked(testDB.runCommand({
        createIndexes: testCollName,
        indexes: [{key: {x: 1}, name: 'x_1'}],
        writeConcern: {w: 2}
    }));

    assert.commandWorked(
        testDB.runCommand({dropIndexes: testCollName, index: 'x_1', writeConcern: {w: 2}}));

    assert.commandWorked(primary.getDB('admin').runCommand({
        renameCollection: testDBName + '.' + testCollName,
        to: testDBName + '.' + renameCollName,
        writeConcern: {w: 2}
    }));

    assert.commandWorked(testDB.runCommand({drop: renameCollName, writeConcern: {w: 2}}));
}

assert.commandWorked(secondary.getDB("admin").runCommand(
    {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "off"}));
findWait();

rst.stopSet();
})();

/**
 * Tests that applyOps correctly respects the 'oplogApplicationMode' and 'alwaysUpsert' flags.
 * 'alwaysUpsert' defaults to true and 'oplogApplicationMode' defaults to 'ApplyOps'. We test
 * that these default values do not lead to command failure.
 */

(function() {
'use strict';

var standalone = MongoRunner.runMongod();
var db = standalone.getDB("test");

var coll = db.getCollection("apply_ops_mode1");

// ------------ Testing normal updates ---------------

var id = ObjectId();
for (let updateOp of [
         // An update with a modifier.
         {op: 'u', ns: coll.getFullName(), o: {$set: {x: 1}}, o2: {_id: id}},
         // A full-document replace.
         {op: 'u', ns: coll.getFullName(), o: {_id: id, x: 1}, o2: {_id: id}},
]) {
    coll.drop();
    assert.writeOK(coll.insert({_id: 1}));

    jsTestLog(`Test applyOps with the following op:\n${tojson(updateOp)}`);
    assert.commandFailed(db.adminCommand({applyOps: [updateOp], alwaysUpsert: false}));
    assert.eq(coll.count({x: 1}), 0);

    // Test that 'InitialSync' does not override 'alwaysUpsert: false'.
    assert.commandFailed(db.adminCommand(
        {applyOps: [updateOp], alwaysUpsert: false, oplogApplicationMode: "InitialSync"}));
    assert.eq(coll.count({x: 1}), 0);

    // Test parsing failure.
    assert.commandFailedWithCode(
        db.adminCommand({applyOps: [updateOp], oplogApplicationMode: "BadMode"}),
        ErrorCodes.FailedToParse);
    assert.commandFailedWithCode(db.adminCommand({applyOps: [updateOp], oplogApplicationMode: 5}),
                                 ErrorCodes.TypeMismatch);

    // Test default succeeds.
    assert.commandWorked(db.adminCommand({applyOps: [updateOp]}));
    assert.eq(coll.count({x: 1}), 1);

    coll.drop();
    assert.commandWorked(coll.insert({_id: 1}));

    // Test default succeeds in 'InitialSync' mode.
    assert.commandWorked(
        db.adminCommand({applyOps: [updateOp], oplogApplicationMode: "InitialSync"}));
    assert.eq(coll.count({x: 1}), 1);
}

// ------------ Testing fCV updates ---------------

var adminDB = db.getSiblingDB("admin");
const systemVersionColl = adminDB.getCollection("system.version");

var updateOp = {
    op: 'u',
    ns: systemVersionColl.getFullName(),
    o: {_id: "featureCompatibilityVersion", version: lastLTSFCV},
    o2: {_id: "featureCompatibilityVersion"}
};
assert.commandFailed(db.adminCommand({applyOps: [updateOp], oplogApplicationMode: "InitialSync"}));

assert.commandWorked(db.adminCommand({applyOps: [updateOp], oplogApplicationMode: "ApplyOps"}));

// Test default succeeds.
updateOp.o.targetVersion = latestFCV;
assert.commandWorked(db.adminCommand({
    applyOps: [updateOp],
}));

// ------------ Testing commands on the fCV collection ---------------

var collModOp = {
    op: 'c',
    ns: systemVersionColl.getDB() + ".$cmd",
    o: {collMod: systemVersionColl.getName(), validationLevel: "off"},
};
assert.commandFailed(db.adminCommand({applyOps: [collModOp], oplogApplicationMode: "InitialSync"}));

assert.commandWorked(db.adminCommand({applyOps: [collModOp], oplogApplicationMode: "ApplyOps"}));

// Test default succeeds.
assert.commandWorked(db.adminCommand({
    applyOps: [collModOp],
}));

MongoRunner.stopMongod(standalone);
})();

(function() {
"use strict";
var standalone = MongoRunner.runMongod();
var adminDB = standalone.getDB("admin");

// Get the uuid of the original admin.system.version.
var res = adminDB.runCommand({listCollections: 1, filter: {name: "system.version"}});
assert.commandWorked(res, "failed to list collections");
assert.eq(1, res.cursor.firstBatch.length);
var originalUUID = res.cursor.firstBatch[0].info.uuid;
var newUUID = UUID();

// Create new collection, insert new FCV document and then delete the
// original collection.
var createNewAdminSystemVersionCollection =
    {op: "c", ns: "admin.$cmd", ui: newUUID, o: {create: "system.version"}};
var insertFCVDocument = {
    op: "i",
    ns: "admin.system.version",
    o: {_id: "featureCompatibilityVersion", version: latestFCV}
};
var dropOriginalAdminSystemVersionCollection =
    {op: "c", ns: "admin.$cmd", ui: originalUUID, o: {drop: "admin.tmp_system_version"}};
var cmd = {
    applyOps: [
        createNewAdminSystemVersionCollection,
        insertFCVDocument,
        dropOriginalAdminSystemVersionCollection
    ]
};
assert.commandWorked(adminDB.runCommand(cmd), "failed command " + tojson(cmd));

// Now admin.system.version is overwritten with the new entry.
res = adminDB.runCommand({listCollections: 1, filter: {name: "system.version"}});
assert.commandWorked(res, "failed to list collections");
assert.eq(1, res.cursor.firstBatch.length);
assert.eq(newUUID, res.cursor.firstBatch[0].info.uuid);

MongoRunner.stopMongod(standalone);
})();

// @tags: [requires_replication,tenant_migration_incompatible]
(function() {
// SERVER-28285 When renameCollection drops the target collection, it should just generate
// a single oplog entry, so we cannot end up in a state where the drop has succeeded, but
// the rename didn't.
let rs = new ReplSetTest({nodes: 1});
rs.startSet();
rs.initiate();

let prim = rs.getPrimary();
let first = prim.getDB("first");
let second = prim.getDB("second");
let local = prim.getDB("local");

// Test both for rename within a database as across databases.
const tests = [
    {
        source: first.x,
        target: first.y,
        expectedOplogEntries: 1,
    },
    {
        source: first.x,
        target: second.x,
        expectedOplogEntries: 4,
    }
];
tests.forEach((test) => {
    test.source.drop();
    assert.commandWorked(test.source.insert({}));
    assert.commandWorked(test.target.insert({}));
    // Other things may be going on in the system; look only at oplog entries affecting the
    // particular databases under test.
    const dbregex =
        "^(" + test.source.getDB().getName() + ")|(" + test.target.getDB().getName() + ")\\.";

    let ts = local.oplog.rs.find().sort({$natural: -1}).limit(1).next().ts;
    let cmd = {
        renameCollection: test.source.toString(),
        to: test.target.toString(),
        dropTarget: true
    };
    assert.commandWorked(local.adminCommand(cmd), tojson(cmd));
    ops =
        local.oplog.rs.find({ts: {$gt: ts}, ns: {'$regex': dbregex}}).sort({$natural: 1}).toArray();
    assert.eq(ops.length,
              test.expectedOplogEntries,
              "renameCollection was supposed to only generate " + test.expectedOplogEntries +
                  " oplog entries: " + tojson(ops));
});
rs.stopSet();
})();

/**
 * Verifies mismatching cluster time objects are rejected by a sharded cluster when auth is on. In
 * noPassthrough because auth is manually set.
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

// Given a valid cluster time object, returns one with the same signature, but a mismatching
// cluster time.
function mismatchingLogicalTime(lt) {
    return Object.merge(lt, {clusterTime: Timestamp(lt.clusterTime.getTime() + 100, 0)});
}

function assertRejectsMismatchingLogicalTime(db) {
    let validTime = db.runCommand({hello: 1}).$clusterTime;
    let mismatchingTime = mismatchingLogicalTime(validTime);

    assert.commandFailedWithCode(
        db.runCommand({hello: 1, $clusterTime: mismatchingTime}),
        ErrorCodes.TimeProofMismatch,
        "expected command with mismatching cluster time and signature to be rejected");
}

function assertAcceptsValidLogicalTime(db) {
    let validTime = db.runCommand({hello: 1}).$clusterTime;
    assert.commandWorked(testDB.runCommand({hello: 1, $clusterTime: validTime}),
                         "expected command with valid cluster time and signature to be accepted");
}

// Start the sharding test with auth on.
const st =
    new ShardingTest({mongos: 1, manualAddShard: true, other: {keyFile: "jstests/libs/key1"}});

// Create admin user and authenticate as them.
st.s.getDB("admin").createUser({user: "foo", pwd: "bar", roles: jsTest.adminUserRoles});
st.s.getDB("admin").auth("foo", "bar");

// Add shard with auth enabled.
const rst = new ReplSetTest({nodes: 2});
rst.startSet({keyFile: "jstests/libs/key1", shardsvr: ""});

rst.initiateWithAnyNodeAsPrimary(
    null, "replSetInitiate", {doNotWaitForStableRecoveryTimestamp: true});
assert.commandWorked(st.s.adminCommand({addShard: rst.getURL()}));

const testDB = st.s.getDB("test");

// Unsharded collections reject mismatching cluster times and accept valid ones.
assertRejectsMismatchingLogicalTime(testDB);
assertAcceptsValidLogicalTime(testDB);

// Initialize sharding.
assert.commandWorked(testDB.adminCommand({enableSharding: "test"}));
assert.commandWorked(
    testDB.adminCommand({shardCollection: testDB.foo.getFullName(), key: {_id: 1}}));

// Sharded collections reject mismatching cluster times and accept valid ones.
assertRejectsMismatchingLogicalTime(testDB);
assertAcceptsValidLogicalTime(testDB);

// Shards and config servers also reject mismatching times and accept valid ones.
assertRejectsMismatchingLogicalTime(rst.getPrimary().getDB("test"));
assertAcceptsValidLogicalTime(rst.getPrimary().getDB("test"));
assertRejectsMismatchingLogicalTime(st.configRS.getPrimary().getDB("admin"));
assertAcceptsValidLogicalTime(st.configRS.getPrimary().getDB("admin"));

st.stop();
rst.stopSet();
})();

/**
 * Tests that the auto_retry_on_network_error.js override automatically retries commands on network
 * errors for commands run under a session.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

TestData.networkErrorAndTxnOverrideConfig = {
    retryOnNetworkErrors: true
};

function getThreadName(db) {
    let myUri = db.adminCommand({whatsmyuri: 1}).you;
    return db.getSiblingDB("admin")
        .aggregate([{$currentOp: {localOps: true}}, {$match: {client: myUri}}])
        .toArray()[0]
        .desc;
}

function failNextCommand(db, command) {
    let threadName = getThreadName(db);

    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: {times: 1},
        data: {
            closeConnection: true,
            failCommands: [command],
            threadName: threadName,
        }
    }));
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();

// awaitLastStableRecoveryTimestamp runs an 'appendOplogNote' command which is not retryable.
rst.initiateWithAnyNodeAsPrimary(
    null, "replSetInitiate", {doNotWaitForStableRecoveryTimestamp: true});

// We require the 'setParameter' command to initialize a replica set, and the command will fail
// due to the override below. As a result, we must initiate our replica set before we load these
// files.
load('jstests/libs/override_methods/network_error_and_txn_override.js');
load("jstests/replsets/rslib.js");

const dbName = "test";
const collName = "auto_retry";

// The override requires the connection to be run under a session. Use the replica set URL to
// allow automatic re-targeting of the primary on NotWritablePrimary errors.
const db = new Mongo(rst.getURL()).startSession({retryWrites: true}).getDatabase(dbName);

// Commands with no disconnections should work as normal.
assert.commandWorked(db.runCommand({ping: 1}));
assert.commandWorked(db.runCommandWithMetadata({ping: 1}, {}).commandReply);

// Read commands are automatically retried on network errors.
failNextCommand(db, "find");
assert.commandWorked(db.runCommand({find: collName}));

failNextCommand(db, "find");
assert.commandWorked(db.runCommandWithMetadata({find: collName}, {}).commandReply);

// Retryable write commands that can be retried succeed.
failNextCommand(db, "insert");
assert.commandWorked(db[collName].insert({x: 1}));

failNextCommand(db, "insert");
assert.commandWorked(db.runCommandWithMetadata({
                           insert: collName,
                           documents: [{x: 2}, {x: 3}],
                           txnNumber: NumberLong(10),
                           lsid: {id: UUID()}
                       },
                                               {})
                         .commandReply);

// Retryable write commands that cannot be retried (i.e. no transaction number, no session id,
// or are unordered) throw.
failNextCommand(db, "insert");
assert.throws(function() {
    db.runCommand({insert: collName, documents: [{x: 1}, {x: 2}], ordered: false});
});

// The previous command shouldn't have been retried, so run a command to successfully re-target
// the primary, so the connection to it can be closed.
assert.commandWorked(db.runCommandWithMetadata({ping: 1}, {}).commandReply);

failNextCommand(db, "insert");
assert.throws(function() {
    db.runCommandWithMetadata({insert: collName, documents: [{x: 1}, {x: 2}], ordered: false}, {});
});

// getMore commands can't be retried because we won't know whether the cursor was advanced or
// not.
let cursorId = assert.commandWorked(db.runCommand({find: collName, batchSize: 0})).cursor.id;
failNextCommand(db, "getMore");
assert.throws(function() {
    db.runCommand({getMore: cursorId, collection: collName});
});

cursorId = assert.commandWorked(db.runCommand({find: collName, batchSize: 0})).cursor.id;
failNextCommand(db, "getMore");
assert.throws(function() {
    db.runCommandWithMetadata({getMore: cursorId, collection: collName}, {});
});

rst.stopSet();
})();

/**
 * Test that the 'reconfig' helper function correctly executes reconfigs between configs that have
 * the maximum number of allowed voting nodes.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");

// Make secondaries unelectable. Add 7 voting nodes, which is the maximum allowed.
const replTest = new ReplSetTest({
    nodes: [
        {},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0}},
        {rsConfig: {priority: 0, votes: 0}}
    ]
});
replTest.startSet();
let conf = replTest.getReplSetConfig();
conf.settings = {
    // Speed up config propagation.
    heartbeatIntervalMillis: 100,
};
replTest.initiate(conf);

// Start out with config {n0,n1,n2}
let config = replTest.getReplSetConfigFromNode();
let origConfig = Object.assign({}, config);
let [m0, m1, m2, m3, m4, m5, m6, m7] = origConfig.members;

//
// Test max voting constraint.
//

jsTestLog("Test max voting constraint.");

// Test making one node non voting and the other voting.
m6.votes = 0;
m6.priority = 0;
m7.votes = 1;
m7.priority = 1;
config.members = [m0, m1, m2, m3, m4, m5, m6, m7];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// And test switching the vote back.
m6.votes = 1;
m6.priority = 0;
m7.votes = 0;
m7.priority = 0;
config.members = [m0, m1, m2, m3, m4, m5, m6, m7];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Test swapping out a voting member.
m6.votes = 1;
m6.priority = 0;
config.members = [m0, m1, m2, m3, m4, m5, m6];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

m7.votes = 1;
m7.priority = 1;
config.members = [m0, m1, m2, m3, m4, m5, m7];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Restore the original config before shutting down.
m7.votes = 0;
m7.priority = 0;
config.members = [m0, m1, m2, m3, m4, m5, m6, m7];
reconfig(replTest, config);
replTest.stopSet();
})();

/**
 * Test that the 'reconfig' helper function correctly executes arbitrary reconfigs.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");

// Make secondaries unelectable.
const replTest =
    new ReplSetTest({nodes: [{}, {rsConfig: {priority: 0}}, {rsConfig: {priority: 0}}]});
replTest.startSet();
let conf = replTest.getReplSetConfig();
conf.settings = {
    // Speed up config propagation.
    heartbeatIntervalMillis: 100,
};
replTest.initiate(conf);

// Start out with config {n0,n1,n2}
let config = replTest.getReplSetConfigFromNode();
let origConfig = Object.assign({}, config);
let [m0, m1, m2] = origConfig.members;

//
// Test reconfigs that only change config settings but not the member set.
//

jsTestLog("Testing reconfigs that don't modify the member set.");

// Change the 'electionTimeoutMillis' setting.
config.settings.electionTimeoutMillis = config.settings.electionTimeoutMillis + 1;
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Do a reconfig that leaves out a config setting that will take on a default.
delete config.settings.electionTimeoutMillis;
reconfig(replTest, config);
// The installed config should be the same as the given config except for the default value.
let actualConfig = replTest.getReplSetConfigFromNode();
assert(actualConfig.settings.hasOwnProperty("electionTimeoutMillis"));
config.settings.electionTimeoutMillis = actualConfig.settings.electionTimeoutMillis;
assertSameConfigContent(actualConfig, config);

// Change a member config parameter.
config.members[0].priority = 2;
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

//
// Test member set changes.
//

jsTestLog("Testing member set changes.");

// Start in the original config and reset the config object.
reconfig(replTest, origConfig);
config = replTest.getReplSetConfigFromNode();

// Remove 2 nodes, {n1, n2}.
config.members = [m0];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Add 2 nodes, {n1, n2}.
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Remove one node so we can test swapping a node out.
config.members = [m0, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Remove n2 and add n1 simultaneously (swap a node).
config.members = [m0, m1];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Remove both existing nodes (n0, n1) and add a new node (n2). Removing a node that is executing
// the reconfig shouldn't be allowed, but we test it here to make sure it fails in an expected way.
m2.priority = 1;
config.members = [m2];
try {
    reconfig(replTest, config);
} catch (e) {
    assert.eq(e.code, ErrorCodes.NewReplicaSetConfigurationIncompatible, tojson(e));
}

// Reset the member's priority.
m2.priority = 0;

//
// Test voting set changes that don't change the member set.
//

jsTestLog("Testing voting set changes.");

// Start in the original config.
reconfig(replTest, origConfig);

// Remove two nodes, {n1,n2}, from the voting set.
m1.votes = 0;
m2.votes = 0;
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Add two nodes, {n1,n2}, to the voting set.
m1.votes = 1;
m2.votes = 1;
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Remove one node n1 from the voting set.
m1.votes = 0;
m2.votes = 1;
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Add one node (n1) and remove one node (n2) from the voting set.
m1.votes = 1;
m2.votes = 0;
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Make n2 voting by omitting a 'votes' field, which is allowed.
delete m2.votes;
config.members = [m0, m1, m2];
reconfig(replTest, config);
actualConfig = replTest.getReplSetConfigFromNode();
assert.eq(actualConfig.members[2].votes, 1);
config.members[2].votes = 1;
assertSameConfigContent(actualConfig, config);

// Remove the primary (n0) from the voting set and remove n2. We expect this to fail.
m0.votes = 0;
m0.priority = 0;
m1.priority = 1;
m1.votes = 1;
m2.priority = 0;
m2.votes = 0;
config.members = [m0, m1, m2];
try {
    reconfig(replTest, config);
} catch (e) {
    assert.eq(e.code, ErrorCodes.NewReplicaSetConfigurationIncompatible, tojson(e));
}

//
// Test simultaneous voting set and member set changes.
//

jsTestLog("Testing simultaneous voting set and member set changes.");

// Start in the original config and reset vote counts.
m0.votes = 1;
m0.priority = 1;
m1.votes = 1;
m1.priority = 0;
m2.votes = 1;
m2.priority = 0;
reconfig(replTest, origConfig);

// Remove voting node n2 and make n1 non voting.
m1.votes = 0;
m2.votes = 1;
config.members = [m0, m1];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Add voting node n2 and make n1 voting.
m1.votes = 1;
m2.votes = 1;
config.members = [m0, m1, m2];
reconfig(replTest, config);
assertSameConfigContent(replTest.getReplSetConfigFromNode(), config);

// Restore the original config before shutting down.
reconfig(replTest, origConfig);
replTest.stopSet();
})();

/**
 * Tests the maxAwaitTimeMS and topologyVersion parameters of the hello command, and its aliases,
 * isMaster and ismaster.
 * @tags: [requires_replication]
 */
(function() {
"use strict";
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

// ErrorCodes
const kIDLParserComparisonError = 51024;

// runTest takes in the hello command or its aliases, isMaster and ismaster.
function runTest(db, cmd, logFailpoint) {
    // Check the command response contains a topologyVersion even if maxAwaitTimeMS and
    // topologyVersion are not included in the request.
    const res = assert.commandWorked(db.runCommand(cmd));
    assert(res.hasOwnProperty("topologyVersion"), tojson(res));

    const topologyVersionField = res.topologyVersion;
    assert(topologyVersionField.hasOwnProperty("processId"), tojson(topologyVersionField));
    assert(topologyVersionField.hasOwnProperty("counter"), tojson(topologyVersionField));

    // Check that the command succeeds when passed a valid topologyVersion and maxAwaitTimeMS. In
    // this case, use the topologyVersion from the previous command response. The topologyVersion
    // field is expected to be of the form {processId: <ObjectId>, counter: <long>}.
    assert.commandWorked(
        db.runCommand({[cmd]: 1, topologyVersion: topologyVersionField, maxAwaitTimeMS: 0}));

    // Ensure the command waits for at least maxAwaitTimeMS before returning, and doesn't appear in
    // slow query log even if it takes many seconds.
    assert.commandWorked(db.adminCommand({clearLog: 'global'}));
    let now = new Date();
    jsTestLog(`Running slow ${cmd}`);

    // Get the slow query log failpoint for the command, to know the current timesEntered before
    // the command runs.
    const timesEnteredBeforeRunningCommand = configureFailPoint(db, logFailpoint).timesEntered;

    assert.commandWorked(
        db.runCommand({[cmd]: 1, topologyVersion: topologyVersionField, maxAwaitTimeMS: 20000}));
    let commandDuration = new Date() - now;
    // Allow for some clock imprecision between the server and the jstest.
    assert.gte(
        commandDuration,
        10000,
        cmd + ` command should have taken at least 10000ms, but completed in ${commandDuration}ms`);

    // Get the slow query log failpoint for the command, to make sure that it didn't get hit during
    // the command run by checking that timesEntered is the same as before.
    const timesEnteredAfterRunningCommand = configureFailPoint(db, logFailpoint).timesEntered;
    assert(timesEnteredBeforeRunningCommand == timesEnteredAfterRunningCommand);

    // Check that the command appears in the slow query log if it's unexpectedly slow.
    function runHelloCommand(cmd, topologyVersionField) {
        assert.commandWorked(
            db.runCommand({[cmd]: 1, topologyVersion: topologyVersionField, maxAwaitTimeMS: 1}));
        jsTestLog(`${cmd} completed in parallel shell`);
    }

    assert.commandWorked(db.adminCommand({clearLog: 'global'}));

    // Use a skip of 1, since the parallel shell runs hello when it starts.
    const helloFailpoint = configureFailPoint(db, "waitInHello", {}, {skip: 1});
    const logFailPoint = configureFailPoint(db, logFailpoint);
    const awaitHello = startParallelShell(funWithArgs(runHelloCommand, cmd, topologyVersionField),
                                          db.getMongo().port);
    helloFailpoint.wait();
    sleep(1000);  // Make the command hang for a second in the parallel shell.
    helloFailpoint.off();

    // Wait for the parallel shell to finish.
    awaitHello();

    // Wait for the command to be logged.
    logFailPoint.wait();

    // Check that when a different processId is given, the server responds immediately.
    now = new Date();
    assert.commandWorked(db.runCommand({
        [cmd]: 1,
        topologyVersion: {processId: ObjectId(), counter: topologyVersionField.counter},
        maxAwaitTimeMS: 2000
    }));
    commandDuration = new Date() - now;
    assert.lt(
        commandDuration,
        1000,
        cmd + ` command should have taken at most 1000ms, but completed in ${commandDuration}ms`);

    // Check that when a different processId is given, a higher counter is allowed.
    assert.commandWorked(db.runCommand({
        [cmd]: 1,
        topologyVersion:
            {processId: ObjectId(), counter: NumberLong(topologyVersionField.counter + 1)},
        maxAwaitTimeMS: 0
    }));

    // Check that when the same processId is given, a higher counter is not allowed.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {
            processId: topologyVersionField.processId,
            counter: NumberLong(topologyVersionField.counter + 1)
        },
        maxAwaitTimeMS: 0
    }),
                                 [31382, 51761, 51764]);

    // Check that passing a topologyVersion not of type object fails.
    assert.commandFailedWithCode(
        db.runCommand({[cmd]: 1, topologyVersion: "topology_version_string", maxAwaitTimeMS: 0}),
        ErrorCodes.TypeMismatch);

    // Check that a topologyVersion with an invalid processId and valid counter fails.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {processId: "pid1", counter: topologyVersionField.counter},
        maxAwaitTimeMS: 0
    }),
                                 ErrorCodes.TypeMismatch);

    // Check that a topologyVersion with a valid processId and invalid counter fails.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {processId: topologyVersionField.processId, counter: 0},
        maxAwaitTimeMS: 0
    }),
                                 ErrorCodes.TypeMismatch);

    // Check that a topologyVersion with a valid processId but missing counter fails.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {processId: topologyVersionField.processId},
        maxAwaitTimeMS: 0
    }),
                                 40414);

    // Check that a topologyVersion with a missing processId and valid counter fails.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {counter: topologyVersionField.counter},
        maxAwaitTimeMS: 0
    }),
                                 40414);

    // Check that a topologyVersion with a valid processId and negative counter fails.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {processId: topologyVersionField.processId, counter: NumberLong("-1")},
        maxAwaitTimeMS: 0
    }),
                                 [31372, 51758]);

    // Check that the command fails if there is an extra field in its topologyVersion.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: {
            processId: topologyVersionField.processId,
            counter: topologyVersionField.counter,
            randomField: "I should cause an error"
        },
        maxAwaitTimeMS: 0
    }),
                                 40415);

    // A client following the awaitable hello/isMaster protocol must include topologyVersion in
    // their request if and only if they include maxAwaitTimeMS. Check that the command fails if
    // there is a topologyVersion but no maxAwaitTimeMS field.
    assert.commandFailedWithCode(db.runCommand({[cmd]: 1, topologyVersion: topologyVersionField}),
                                 [31368, 51760]);

    // Check that the command fails if there is a maxAwaitTimeMS field but no topologyVersion.
    assert.commandFailedWithCode(db.runCommand({[cmd]: 1, maxAwaitTimeMS: 0}), [31368, 51760]);

    // Check that the command fails if there is a valid topologyVersion but invalid maxAwaitTimeMS
    // type.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: topologyVersionField,
        maxAwaitTimeMS: "stringMaxAwaitTimeMS"
    }),
                                 ErrorCodes.TypeMismatch);

    // Check that the command fails if there is a valid topologyVersion but negative maxAwaitTimeMS.
    assert.commandFailedWithCode(db.runCommand({
        [cmd]: 1,
        topologyVersion: topologyVersionField,
        maxAwaitTimeMS: -1,
    }),
                                 [31373, 51759, kIDLParserComparisonError]);
}

// Set command log verbosity to 0 to avoid logging *all* commands in the "slow query" log.
const conn = MongoRunner.runMongod({setParameter: {logComponentVerbosity: tojson({command: 0})}});
assert.neq(null, conn, "mongod was unable to start up");
runTest(conn.getDB("admin"), "hello", "waitForHelloCommandLogged");
runTest(conn.getDB("admin"), "isMaster", "waitForIsMasterCommandLogged");
runTest(conn.getDB("admin"), "ismaster", "waitForIsMasterCommandLogged");
MongoRunner.stopMongod(conn);

const replTest = new ReplSetTest(
    {nodes: 1, nodeOptions: {setParameter: {logComponentVerbosity: tojson({command: 0})}}});
replTest.startSet();
replTest.initiate();
runTest(replTest.getPrimary().getDB("admin"), "hello", "waitForHelloCommandLogged");
runTest(replTest.getPrimary().getDB("admin"), "isMaster", "waitForIsMasterCommandLogged");
runTest(replTest.getPrimary().getDB("admin"), "ismaster", "waitForIsMasterCommandLogged");
replTest.stopSet();

const st = new ShardingTest({
    mongos: 1,
    shards: [{nodes: 1}],
    config: 1,
    other: {mongosOptions: {setParameter: {logComponentVerbosity: tojson({command: 0})}}}
});
runTest(st.s.getDB("admin"), "hello", "waitForHelloCommandLogged");
runTest(st.s.getDB("admin"), "isMaster", "waitForIsMasterCommandLogged");
runTest(st.s.getDB("admin"), "ismaster", "waitForIsMasterCommandLogged");
st.stop();
})();

/**
 * Tests the server status metrics of awaitable hello/isMaster.
 * @tags: [requires_replication]
 */
(function() {
"use strict";
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/fail_point_util.js");

function runAwaitCmd(cmd, maxAwaitTimeMS) {
    const res = assert.commandWorked(db.runCommand({[cmd]: 1}));
    assert(res.hasOwnProperty("topologyVersion"), res);
    const topologyVersionField = res.topologyVersion;

    assert.commandWorked(db.runCommand(
        {[cmd]: 1, topologyVersion: topologyVersionField, maxAwaitTimeMS: maxAwaitTimeMS}));
}

function runTest(db, cmd, failPoint) {
    const res = assert.commandWorked(db.runCommand({[cmd]: 1}));
    assert(res.hasOwnProperty("topologyVersion"), res);

    const topologyVersionField = res.topologyVersion;
    assert(topologyVersionField.hasOwnProperty("processId"), topologyVersionField);
    assert(topologyVersionField.hasOwnProperty("counter"), topologyVersionField);

    // Test that metrics are properly updated when there are command requests that are waiting.
    let awaitCmdFailPoint = configureFailPoint(failPoint.conn, failPoint.failPointName);
    let singleAwaitCmd =
        startParallelShell(funWithArgs(runAwaitCmd, cmd, 100), failPoint.conn.port);

    // Ensure the command requests have started waiting before checking the metrics.
    awaitCmdFailPoint.wait();
    // awaitingTopologyChanges should increment once.
    let numAwaitingTopologyChange = db.serverStatus().connections.awaitingTopologyChanges;
    assert.eq(1, numAwaitingTopologyChange);
    configureFailPoint(failPoint.conn, failPoint.failPointName, {}, "off");

    // The awaitingTopologyChanges metric should decrement once the waiting command has returned.
    singleAwaitCmd();
    numAwaitingTopologyChange = db.serverStatus().connections.awaitingTopologyChanges;
    assert.eq(0, numAwaitingTopologyChange);

    // Refresh the number of times we have entered the failpoint.
    awaitCmdFailPoint = configureFailPoint(failPoint.conn, failPoint.failPointName);
    let firstAwaitCmd = startParallelShell(funWithArgs(runAwaitCmd, cmd, 100), failPoint.conn.port);
    let secondAwaitCmd =
        startParallelShell(funWithArgs(runAwaitCmd, cmd, 100), failPoint.conn.port);
    assert.commandWorked(db.runCommand({
        waitForFailPoint: failPoint.failPointName,
        // Each failpoint will be entered twice. Once for the 'shouldFail' check and again for the
        // 'pauseWhileSet'.
        timesEntered: awaitCmdFailPoint.timesEntered + 4,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    }));

    numAwaitingTopologyChange = db.serverStatus().connections.awaitingTopologyChanges;
    assert.eq(2, numAwaitingTopologyChange);
    configureFailPoint(failPoint.conn, failPoint.failPointName, {}, "off");

    firstAwaitCmd();
    secondAwaitCmd();
    numAwaitingTopologyChange = db.serverStatus().connections.awaitingTopologyChanges;
    assert.eq(0, numAwaitingTopologyChange);
}

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");
// A failpoint signalling that the standalone server has received the command request and is
// waiting for maxAwaitTimeMS.
let failPoint = configureFailPoint(conn, "hangWaitingForHelloResponseOnStandalone");
runTest(conn.getDB("admin"), "hello", failPoint);
runTest(conn.getDB("admin"), "isMaster", failPoint);
runTest(conn.getDB("admin"), "ismaster", failPoint);
MongoRunner.stopMongod(conn);

const replTest = new ReplSetTest({nodes: 1});
replTest.startSet();
replTest.initiate();
const primary = replTest.getPrimary();
// A failpoint signalling that the server has received the command request and is waiting for a
// topology change or maxAwaitTimeMS.
failPoint = configureFailPoint(primary, "hangWhileWaitingForHelloResponse");
runTest(primary.getDB("admin"), "hello", failPoint);
runTest(primary.getDB("admin"), "isMaster", failPoint);
runTest(primary.getDB("admin"), "ismaster", failPoint);
replTest.stopSet();

const st = new ShardingTest({mongos: 1, shards: [{nodes: 1}], config: 1});
failPoint = configureFailPoint(st.s, "hangWhileWaitingForHelloResponse");
runTest(st.s.getDB("admin"), "hello", failPoint);
runTest(st.s.getDB("admin"), "isMaster", failPoint);
runTest(st.s.getDB("admin"), "ismaster", failPoint);
st.stop();
})();

/**
 * Tests the validate command with {background:true} in a replica set.
 *
 * Checks that {full:true} cannot be run with {background:true}.
 * Checks that {background:true} runs.
 * Checks that {background:true} can run concurrently with CRUD ops on the same collection.
 *
 * @tags: [
 *   # Background validation is only supported by WT.
 *   requires_wiredtiger,
 *   # inMemory does not have checkpoints; background validation only runs on a checkpoint.
 *   requires_persistence,
 *   # A failpoint is set that only exists on the mongod.
 *   assumes_against_mongod_not_mongos,
 *   # A failpoint is set against the primary only.
 *   does_not_support_stepdowns,
 *   # Checkpoint cursors cannot be open in lsm.
 *   does_not_support_wiredtiger_lsm,
 *   requires_replication,
 * ]
 */

(function() {
'use strict';

load("jstests/libs/fail_point_util.js");
load("jstests/core/txns/libs/prepare_helpers.js");

const dbName = "db_background_validation_repl";
const collName = "coll_background_validation_repl";

// Starts and returns a replica set.
const initTest = () => {
    const replSet = new ReplSetTest({nodes: 1, name: "rs"});
    replSet.startSet();
    replSet.initiate();
    const primary = replSet.getPrimary();

    let testColl = primary.getDB(dbName)[collName];
    testColl.drop();
    return replSet;
};

const doTest = replSet => {
    /*
     * Create some indexes and insert some data, so we can validate them more meaningfully.
     */
    const testDB = replSet.getPrimary().getDB(dbName);
    const testColl = testDB[collName];
    assert.commandWorked(testColl.createIndex({a: 1}));
    assert.commandWorked(testColl.createIndex({b: 1}));
    assert.commandWorked(testColl.createIndex({c: 1}));

    const numDocs = 100;
    for (let i = 0; i < numDocs; ++i) {
        assert.commandWorked(testColl.insert({a: i, b: i, c: i}));
    }

    /**
     * Ensure {full:true} and {background:true} cannot be run together.
     */
    assert.commandFailedWithCode(testColl.validate({background: true, full: true}),
                                 ErrorCodes.CommandNotSupported);

    assert.commandWorked(testDB.adminCommand({fsync: 1}));

    // Check that {backround:true} is successful.
    let res = testColl.validate({background: true});
    assert.commandWorked(res);
    assert(res.valid, "Validate cmd with {background:true} failed: " + tojson(res));

    /*
     * Test background validation with concurrent CRUD operations.
     */

    // Set a failpoint in the background validation code to pause validation while holding a
    // collection lock.
    let failPoint = configureFailPoint(testDB, "pauseCollectionValidationWithLock");

    jsTest.log(`Starting parallel shell on port ${replSet.getPrimary().port}`);
    // Start an asynchronous thread to run collection validation with {background:true}.
    // Ensure we can perform multiple collection validations on the same collection
    // concurrently.
    let awaitValidateCommand = startParallelShell(function() {
        const asyncTestDB = db.getSiblingDB("db_background_validation_repl");
        const asyncTestColl = asyncTestDB.coll_background_validation_repl;
        const validateRes = asyncTestColl.validate({background: true});
        assert.commandWorked(
            validateRes, "asynchronous background validate command failed: " + tojson(validateRes));
        assert(validateRes.valid,
               "asynchronous background validate command was not valid: " + tojson(validateRes));
    }, replSet.getPrimary().port);

    // Wait for background validation command to start.
    jsTest.log("Waiting for failpoint to hit...");
    failPoint.wait();

    // Check that CRUD ops are succesful while validation is in progress.
    assert.commandWorked(testColl.remove({a: 1, b: 1, c: 1}));
    assert.commandWorked(testColl.insert({a: 1, b: 1, c: 1, d: 100}));
    assert.commandWorked(testColl.update({d: 100}, {"e": "updated"}));
    let docRes = testColl.find({"e": "updated"});
    assert.eq(1,
              docRes.toArray().length,
              "expected to find a single document, found: " + tojson(docRes.toArray()));

    // Clear the failpoint and make sure the validate command was successful.
    failPoint.off();
    awaitValidateCommand();

    /**
     * Verify everything is still OK by running foreground validation.
     */
    res = testColl.validate({background: false});
    assert.commandWorked(res);
    assert(res.valid, "Validate cmd with {background:true} failed: " + tojson(res));
    assert.eq(res.nIndexes, 4, "Expected 4 indexes: " + tojson(res));
    assert.eq(res.nrecords, numDocs, "Expected " + numDocs + " collection records:" + tojson(res));
    assert.eq(res.keysPerIndex._id_,
              numDocs,
              "Expected " + numDocs + " _id index records: " + tojson(res));
    assert.eq(res.keysPerIndex.a_1,
              numDocs,
              "Expected " + numDocs + " a_1 index records: " + tojson(res));
    assert.eq(res.keysPerIndex.b_1,
              numDocs,
              "Expected " + numDocs + " b_1 index records: " + tojson(res));
    assert.eq(res.keysPerIndex.c_1,
              numDocs,
              "Expected " + numDocs + " c_1 index records: " + tojson(res));
};

const replSet = initTest();
doTest(replSet);
replSet.stopSet();
})();

/**
 * Test the backup/restore process:
 * - 3 node replica set
 * - Mongo CRUD client
 * - Mongo FSM client
 * - fsyncLock Secondary
 * - cp DB files
 * - fsyncUnlock Secondary
 * - Start mongod as hidden secondary
 * - Wait until new hidden node becomes secondary
 *
 * Some methods for backup used in this test checkpoint the files in the dbpath. This technique will
 * not work for ephemeral storage engines, as they do not store any data in the dbpath.
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

load("jstests/noPassthrough/libs/backup_restore.js");

(function() {
"use strict";

// Run the fsyncLock test. Will return before testing for any engine that doesn't
// support fsyncLock
new BackupRestoreTest({backup: 'fsyncLock'}).run();
}());

/**
 * Test the backup/restore process:
 * - 3 node replica set
 * - Mongo CRUD client
 * - Mongo FSM client
 * - fsyncLock (or stop) Secondary
 * - cp (or rsync) DB files
 * - fsyncUnlock (or start) Secondary
 * - Start mongod as hidden secondary
 * - Wait until new hidden node becomes secondary
 *
 * Some methods for backup used in this test checkpoint the files in the dbpath. This technique will
 * not work for ephemeral storage engines, as they do not store any data in the dbpath.
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

load("jstests/noPassthrough/libs/backup_restore.js");

(function() {
"use strict";

// Windows doesn't guarantee synchronous file operations.
if (_isWindows()) {
    print("Skipping test on windows");
    return;
}

// Grab the storage engine, default is wiredTiger
var storageEngine = jsTest.options().storageEngine || "wiredTiger";

// Skip this test if not running with the "wiredTiger" storage engine.
if (storageEngine !== 'wiredTiger') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger"');
    return;
}

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

// if rsync is not available on the host, then this test is skipped
if (!runProgram('bash', '-c', 'which rsync')) {
    new BackupRestoreTest({backup: 'rolling', clientTime: 30000}).run();
} else {
    jsTestLog("Skipping test for " + storageEngine + ' rolling');
}
}());

/**
 * Test the backup/restore process:
 * - 3 node replica set
 * - Mongo CRUD client
 * - Mongo FSM client
 * - Stop Secondary
 * - cp DB files
 * - Start Secondary
 * - Start mongod as hidden secondary
 * - Wait until new hidden node becomes secondary
 *
 * Some methods for backup used in this test checkpoint the files in the dbpath. This technique will
 * not work for ephemeral storage engines, as they do not store any data in the dbpath.
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

load("jstests/noPassthrough/libs/backup_restore.js");

(function() {
"use strict";

new BackupRestoreTest({backup: 'stopStart', clientTime: 30000}).run();
}());

/**
 * Verifies that readPrefMode param works for find/fineOne/query ops in benchRun().
 *
 * @tags: [requires_replication]
 */

(function() {
"use strict";

const rs = new ReplSetTest({nodes: 2});
rs.startSet();
rs.initiate();

const primary = rs.getPrimary();
const secondary = rs.getSecondary();
const collName = primary.getDB(jsTestName()).getCollection("coll").getFullName();

const verifyNoError = res => {
    assert.eq(res.errCount, 0);
    assert.gt(res.totalOps, 0);
};

const benchArgArray = [
    {
        ops: [{op: "find", readCmd: true, query: {}, ns: collName, readPrefMode: "primary"}],
        parallel: 1,
        host: primary.host
    },
    {
        ops: [{
            op: "findOne",
            readCmd: true,
            query: {},
            ns: collName,
            readPrefMode: "primaryPreferred"
        }],
        parallel: 1,
        host: primary.host
    },
    {
        ops: [{op: "find", readCmd: true, query: {}, ns: collName, readPrefMode: "secondary"}],
        parallel: 1,
        host: secondary.host
    },
    {
        ops: [{
            op: "findOne",
            readCmd: true,
            query: {},
            ns: collName,
            readPrefMode: "secondaryPreferred"
        }],
        parallel: 1,
        host: secondary.host
    },
    {
        ops: [{op: "query", readCmd: true, query: {}, ns: collName, readPrefMode: "nearest"}],
        parallel: 1,
        host: secondary.host
    },
];

benchArgArray.forEach(benchArg => verifyNoError(benchRun(benchArg)));

const invalidArgAndError = [
    {
        benchArg: {
            ops: [{op: "find", readCmd: true, query: {}, ns: collName, readPrefMode: 1}],
            parallel: 1,
            host: primary.host
        },
        error: ErrorCodes.BadValue
    },
    {
        benchArg: {
            ops:
                [{op: "find", readCmd: true, query: {}, ns: collName, readPrefMode: "invalidPref"}],
            parallel: 1,
            host: primary.host
        },
        error: ErrorCodes.BadValue
    },
    {
        benchArg: {
            ops: [
                {op: "insert", writeCmd: true, doc: {a: 1}, ns: collName, readPrefMode: "primary"}
            ],
            parallel: 1,
            host: primary.host
        },
        error: ErrorCodes.InvalidOptions
    },
];

invalidArgAndError.forEach(
    argAndError => assert.throwsWithCode(() => benchRun(argAndError.benchArg), argAndError.error));

rs.stopSet();
})();

// Startup with --bind_ip_all and --ipv6 should not fail with address already in use.

(function() {
'use strict';

const mongo = MongoRunner.runMongod({ipv6: "", bind_ip_all: ""});
assert(mongo !== null, "Database is not running");
assert.commandWorked(mongo.getDB("test").hello(), "hello failed");
MongoRunner.stopMongod(mongo);
}());

// Startup with --bind_ip_all should override net.bindIp and vice versa.

(function() {
'use strict';

const port = allocatePort();
const BINDIP = 'jstests/noPassthrough/libs/net.bindIp_localhost.yaml';
const BINDIPALL = 'jstests/noPassthrough/libs/net.bindIpAll.yaml';

function runTest(config, opt, expectStar, expectLocalhost) {
    clearRawMongoProgramOutput();
    const mongod =
        runMongoProgram('mongod', '--port', port, '--config', config, opt, '--outputConfig');
    assert.eq(mongod, 0);
    const output = rawMongoProgramOutput();
    assert.eq(output.search(/bindIp: "\*"/) >= 0, expectStar, output);
    assert.eq(output.search(/bindIp: localhost/) >= 0, expectLocalhost, output);
    assert.eq(output.search(/bindIpAll:/) >= 0, false, output);
}

runTest(BINDIP, '--bind_ip_all', true, false);
runTest(BINDIPALL, '--bind_ip=localhost', false, true);
}());

// Log bound addresses at startup.

(function() {
'use strict';

const mongo = MongoRunner.runMongod({ipv6: '', bind_ip: 'localhost', useLogFiles: true});
assert.neq(mongo, null, "Database is not running");
const log = cat(mongo.fullOptions.logFile);
print(log);
assert(log.match(/Listening on.*127.0.0.1/), "Not listening on AF_INET");
if (!_isWindows()) {
    assert(log.match(/Listening on.*\.sock/), "Not listening on AF_UNIX");
}
MongoRunner.stopMongod(mongo);
}());

/**
 * Tests using different combinations of --wiredTigerCollectionBlockCompressor and
 * --wiredTigerJournalCompressor.
 *
 * Using the collection block compressor option will result in all new collections made during
 * that process lifetime to use that compression setting. WiredTiger perfectly supports different
 * tables using different block compressors. This test will start up MongoDB once for each block
 * compressor setting and a create a new collection. Then after all collections are created, check
 * creation string passed to WT via the collStats command.
 *
 * WiredTiger also supports changing the compression setting for writes to the journal. This tests
 * that the setting can be changed between clean restarts, but otherwise does not verify the
 * journal compression behavior.
 *
 * @tags: [requires_persistence,requires_wiredtiger]
 */
(function() {
'use strict';

// On the first iteration, start a mongod. Subsequent iterations will close and restart on the
// same dbpath.
let firstIteration = true;
let compressors = ['none', 'snappy', 'zlib', 'zstd'];
let mongo;
for (let compressor of compressors) {
    jsTestLog({"Starting with compressor": compressor});
    if (firstIteration) {
        mongo = MongoRunner.runMongod({
            wiredTigerCollectionBlockCompressor: compressor,
            wiredTigerJournalCompressor: compressor
        });
        firstIteration = false;
    } else {
        MongoRunner.stopMongod(mongo);
        mongo = MongoRunner.runMongod({
            restart: true,
            dbpath: mongo.dbpath,
            cleanData: false,
            wiredTigerCollectionBlockCompressor: compressor
        });
    }
    mongo.getDB('db')[compressor].insert({});
}

for (let compressor of compressors) {
    jsTestLog({"Asserting collection compressor": compressor});
    let stats = mongo.getDB('db')[compressor].stats();
    assert(stats['wiredTiger']['creationString'].search('block_compressor=' + compressor) > -1);
}

MongoRunner.stopMongod(mongo);
}());

/**
 * Tests that capped deletes occur during rollback on documents inserted earlier in rollback.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/replsets/libs/rollback_test.js');

const rollbackTest = new RollbackTest(jsTestName());

const testDB = function() {
    return rollbackTest.getPrimary().getDB('test');
};

const coll = function() {
    return testDB().getCollection(jsTestName());
};

assert.commandWorked(
    testDB().createCollection(coll().getName(), {capped: true, size: 100, max: 1}));
assert.commandWorked(coll().insert({a: 1}));

rollbackTest.transitionToRollbackOperations();
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();

assert.commandWorked(coll().insert([{b: 1}, {b: 2}]));

rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

// Stopping the test fixture runs validate with {enforceFastCount: true}. This will cause collection
// validation to fail if rollback did not perform capped deletes on documents that were inserted
// earlier in rollback.
rollbackTest.stop();
})();

/**
 * Tests that capped deletes occur during statup recovery on documents inserted earlier in startup
 * recovery.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/libs/fail_point_util.js');

const replTest = new ReplSetTest({nodes: 1});
replTest.startSet();
replTest.initiate();

const primary = replTest.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection(jsTestName());

assert.commandWorked(testDB.createCollection(coll.getName(), {capped: true, size: 100, max: 1}));

const ts = assert.commandWorked(testDB.runCommand({insert: coll.getName(), documents: [{a: 1}]}))
               .operationTime;
configureFailPoint(primary, 'holdStableTimestampAtSpecificTimestamp', {timestamp: ts});

assert.commandWorked(coll.insert([{b: 1}, {b: 2}]));
replTest.restart(primary);

// Stopping the test fixture runs validate with {enforceFastCount: true}. This will cause collection
// validation to fail if startup recovery did not perform capped deletes on documents that were
// inserted earlier in startup recovery.
replTest.stopSet();
})();

// Tests that concurrent change streams requests that would create the database will take locks in
// an order that avoids a deadlock.
// This test was designed to reproduce SERVER-34333.
// This test uses the WiredTiger storage engine, which does not support running without journaling.
// @tags: [requires_replication, requires_journaling, requires_majority_read_concern]
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const db = rst.getPrimary().getDB("test");

let unique_dbName = jsTestName();
const sleepShell = startParallelShell(() => {
    assert.commandFailedWithCode(db.adminCommand({sleep: 1, lock: "w", seconds: 600}),
                                 ErrorCodes.Interrupted);
}, rst.getPrimary().port);
assert.soon(
    () =>
        db.getSiblingDB("admin").currentOp({"command.sleep": 1, active: true}).inprog.length === 1);
const sleepOps = db.getSiblingDB("admin").currentOp({"command.sleep": 1, active: true}).inprog;
assert.eq(sleepOps.length, 1);
const sleepOpId = sleepOps[0].opid;

// Start two concurrent shells which will both attempt to create the database which does not yet
// exist.
const openChangeStreamCode = `const cursor = db.getSiblingDB("${unique_dbName}").test.watch();`;
const changeStreamShell1 = startParallelShell(openChangeStreamCode, rst.getPrimary().port);
const changeStreamShell2 = startParallelShell(openChangeStreamCode, rst.getPrimary().port);

// Wait until we can see both change streams have started and are waiting to acquire the lock
// held by the sleep command.
assert.soon(
    () => db.currentOp({"command.aggregate": "test", waitingForLock: true}).inprog.length === 2);
assert.commandWorked(db.adminCommand({killOp: 1, op: sleepOpId}));

sleepShell();

// Before the fix for SERVER-34333, the operations in these shells would be deadlocked with each
// other and never complete.
changeStreamShell1();
changeStreamShell2();

rst.stopSet();
}());

/**
 * Test that a change stream pipeline which encounters a retryable exception responds to the client
 * with an error object that includes the "ResumableChangeStreamError" label.
 * @tags: [
 *   requires_journaling,
 *   requires_replication,
 *   uses_change_streams,
 * ]
 */
(function() {
"use strict";

// Create a two-node replica set so that we can issue a request to the Secondary.
const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();
rst.awaitSecondaryNodes();

// Disable "secondaryOk" on the connection so that we are not allowed to run on the Secondary.
const testDB = rst.getSecondary().getDB(jsTestName());
testDB.getMongo().setSecondaryOk(false);
const coll = testDB.test;

// Issue a change stream. We should fail with a NotPrimaryNoSecondaryOk error.
const err = assert.throws(() => coll.watch([]));
assert.commandFailedWithCode(err, ErrorCodes.NotPrimaryNoSecondaryOk);

// Confirm that the response includes the "ResumableChangeStreamError" error label.
assert("errorLabels" in err, err);
assert.contains("ResumableChangeStreamError", err.errorLabels, err);

// Now verify that the 'failGetMoreAfterCursorCheckout' failpoint can effectively exercise the
// error label generation logic for change stream getMores.
function testFailGetMoreAfterCursorCheckoutFailpoint({errorCode, expectedLabel}) {
    // Re-enable "secondaryOk" on the test connection.
    testDB.getMongo().setSecondaryOk();

    // Activate the failpoint and set the exception that it will throw.
    assert.commandWorked(testDB.adminCommand({
        configureFailPoint: "failGetMoreAfterCursorCheckout",
        mode: "alwaysOn",
        data: {"errorCode": errorCode}
    }));

    // Now open a valid $changeStream cursor...
    const aggCmdRes = assert.commandWorked(
        coll.runCommand("aggregate", {pipeline: [{$changeStream: {}}], cursor: {}}));

    // ... run a getMore using the cursorID from the original command response, and confirm that the
    // expected error was thrown...
    const getMoreRes = assert.commandFailedWithCode(
        testDB.runCommand({getMore: aggCmdRes.cursor.id, collection: coll.getName()}), errorCode);

    /// ... and confirm that the label is present or absent depending on the "expectedLabel" value.
    const errorLabels = (getMoreRes.errorLabels || []);
    assert.eq("errorLabels" in getMoreRes, expectedLabel, getMoreRes);
    assert.eq(errorLabels.includes("ResumableChangeStreamError"), expectedLabel, getMoreRes);

    // Finally, disable the failpoint.
    assert.commandWorked(
        testDB.adminCommand({configureFailPoint: "failGetMoreAfterCursorCheckout", mode: "off"}));
}
// Test the expected output for both resumable and non-resumable error codes.
testFailGetMoreAfterCursorCheckoutFailpoint(
    {errorCode: ErrorCodes.ShutdownInProgress, expectedLabel: true});
testFailGetMoreAfterCursorCheckoutFailpoint(
    {errorCode: ErrorCodes.FailedToParse, expectedLabel: false});

rst.stopSet();
}());
// Test resuming a change stream on a node other than the one it was started on. Accomplishes this
// by triggering a stepdown.
// This test uses the WiredTiger storage engine, which does not support running without journaling.
// @tags: [
//   requires_journaling,
//   requires_majority_read_concern,
//   requires_replication,
// ]
(function() {
"use strict";
load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

const rst = new ReplSetTest({nodes: 3});
rst.startSet();
rst.initiate();

for (let key of Object.keys(ChangeStreamWatchMode)) {
    const watchMode = ChangeStreamWatchMode[key];
    jsTestLog("Running test for mode " + watchMode);

    const primary = rst.getPrimary();
    const primaryDB = primary.getDB("test");
    const coll = assertDropAndRecreateCollection(primaryDB, "change_stream_failover");

    // Be sure we'll only read from the primary.
    primary.setReadPref("primary");

    // Open a changeStream on the primary.
    const cst = new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, primaryDB));

    let changeStream = cst.getChangeStream({watchMode: watchMode, coll: coll});

    // Be sure we can read from the change stream. Use {w: "majority"} so that we're still
    // guaranteed to be able to read after the failover.
    assert.commandWorked(coll.insert({_id: 0}, {writeConcern: {w: "majority"}}));
    assert.commandWorked(coll.insert({_id: 1}, {writeConcern: {w: "majority"}}));
    assert.commandWorked(coll.insert({_id: 2}, {writeConcern: {w: "majority"}}));

    const firstChange = cst.getOneChange(changeStream);
    assert.docEq(firstChange.fullDocument, {_id: 0});

    // Make the primary step down
    assert.commandWorked(primaryDB.adminCommand({replSetStepDown: 30}));

    // Now wait for another primary to be elected.
    const newPrimary = rst.getPrimary();
    // Be sure we got a different node that the previous primary.
    assert.neq(newPrimary.port, primary.port);

    cst.assertNextChangesEqual({
        cursor: changeStream,
        expectedChanges: [{
            documentKey: {_id: 1},
            fullDocument: {_id: 1},
            ns: {db: primaryDB.getName(), coll: coll.getName()},
            operationType: "insert",
        }]
    });

    // Now resume using the resume token from the first change (before the failover).
    const resumeCursor =
        cst.getChangeStream({watchMode: watchMode, coll: coll, resumeAfter: firstChange._id});

    // Be sure we can read the 2nd and 3rd changes.
    cst.assertNextChangesEqual({
        cursor: resumeCursor,
        expectedChanges: [
            {
                documentKey: {_id: 1},
                fullDocument: {_id: 1},
                ns: {db: primaryDB.getName(), coll: coll.getName()},
                operationType: "insert",
            },
            {
                documentKey: {_id: 2},
                fullDocument: {_id: 2},
                ns: {db: primaryDB.getName(), coll: coll.getName()},
                operationType: "insert",
            }
        ]
    });

    // Unfreeze the original primary so that it can stand for election again.
    assert.commandWorked(primaryDB.adminCommand({replSetFreeze: 0}));
}

rst.stopSet();
}());

/**
 * Tests resource consumption metrics aggregate for change streams.
 *
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {setParameter: {"aggregateOperationResourceConsumptionMetrics": true}}
});
rst.startSet();
rst.initiate();

const dbName = 'test';
const collName = 'test';
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const secondaryDB = secondary.getDB(dbName);

const nDocs = 100;

const clearMetrics = (conn) => {
    conn.getDB('admin').aggregate([{$operationMetrics: {clearMetrics: true}}]);
};

// Get aggregated metrics keyed by database name.
const getMetrics = (conn) => {
    const cursor = conn.getDB('admin').aggregate([{$operationMetrics: {}}]);

    let allMetrics = {};
    while (cursor.hasNext()) {
        let doc = cursor.next();
        allMetrics[doc.db] = doc;
    }
    return allMetrics;
};

const assertMetrics = (conn, assertFn) => {
    let metrics = getMetrics(conn);
    try {
        assertFn(metrics);
    } catch (e) {
        print("caught exception while checking metrics on " + tojson(conn) +
              ", metrics: " + tojson(metrics));

        // Since change streams read from the oplog, print the last oplog entry to provide some
        // insight into why the test's expectations differed from reality.
        try {
            let cur = conn.getDB('local').oplog.rs.find({}).sort({$natural: -1});
            print('top of oplog: ' + tojson(cur.next()));
        } catch (e2) {
            print('failed to print the top of oplog' + e2);
        }
        throw e;
    }
};

assert.commandWorked(primaryDB.createCollection(collName));

/**
 * Load documents into the collection. Expect that metrics are reasonable and only reported on the
 * primary node.
 */
(function loadCollection() {
    clearMetrics(primary);

    let bulk = primaryDB[collName].initializeUnorderedBulkOp();
    for (let i = 0; i < nDocs; i++) {
        bulk.insert({_id: i, a: i});
    }
    assert.commandWorked(bulk.execute());

    assertMetrics(primary, (metrics) => {
        // Each document is 29 bytes and we do not count oplog writes.
        assert.eq(metrics[dbName].docBytesWritten, 29 * nDocs);
        assert.eq(metrics[dbName].docUnitsWritten, nDocs);

        // With batch inserts, the index updates are all performed together after all the documents
        // are inserted, so this has the effect of associating all the index bytes for the batch
        // with one document, for the purposes of totalUnitsWritten.  This effect causes the last
        // document to have 3 units instead of 1 like the first 99.
        assert.eq(metrics[dbName].totalUnitsWritten, nDocs + 2);

        // The inserted keys will vary in size from 2 to 4 bytes depending on their value. Assert
        // that the number of bytes fall within that range.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 2 * nDocs);
        assert.lt(metrics[dbName].idxEntryBytesWritten, 4 * nDocs);
        assert.eq(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // The secondary should not collect metrics for replicated index builds.
    rst.awaitReplication();

    assertMetrics(secondary, (metrics) => {
        assert.eq(undefined, metrics[dbName]);
    });
})();

let nextId = nDocs;

(function changeStreamPrimary() {
    clearMetrics(primary);
    const cur = primaryDB[collName].watch([], {fullDocument: "updateLookup"});

    assertMetrics(primary, (metrics) => {
        // The first aggregate operation will read from the top of the oplog, size not guaranteed.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 0);
    });

    // Ensure that while nothing is returned from the change stream, the server still measures read
    // activity.
    clearMetrics(primary);
    assert(!cur.hasNext());
    assertMetrics(primary, (metrics) => {
        // Calling hasNext may perform many reads from the oplog. The oplog entry size is not
        // guaranteed.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 0);
    });

    // Insert a document and ensure its metrics are aggregated.
    clearMetrics(primary);
    const doc = {_id: nextId, a: nextId};
    nextId += 1;
    assert.commandWorked(primaryDB[collName].insert(doc));
    assertMetrics(primary, (metrics) => {
        assert.eq(metrics[dbName].docBytesWritten, 29);
        assert.eq(metrics[dbName].docUnitsWritten, 1);
        assert.eq(metrics[dbName].idxEntryBytesWritten, 3);
        assert.eq(metrics[dbName].idxEntryUnitsWritten, 1);
        assert.eq(metrics[dbName].totalUnitsWritten, 1);
        assert.eq(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsRead, 0);
        assert.eq(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 0);
    });

    clearMetrics(primary);

    // Ensure that the inserted document eventually comes through the change stream.
    assert.soon(() => {
        if (cur.hasNext()) {
            return true;
        }
        print("Change stream returned no data. Clearing metrics and retrying.");
        clearMetrics(primary);
        return false;
    });
    assert.eq(doc, cur.next().fullDocument);
    assertMetrics(primary, (metrics) => {
        // Will read at least one document from the oplog.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 0);
        assert.gt(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        // Returns one large document
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 3);
    });

    // Update the document and ensure the metrics are aggregated.
    clearMetrics(primary);
    assert.commandWorked(primaryDB[collName].update(doc, {$set: {b: 0}}));
    assertMetrics(primary, (metrics) => {
        assert.eq(metrics[dbName].docBytesWritten, 40);
        assert.eq(metrics[dbName].docUnitsWritten, 1);
        assert.eq(metrics[dbName].totalUnitsWritten, 1);
        assert.eq(metrics[dbName].primaryMetrics.docBytesRead, 29);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsRead, 1);
        assert.eq(metrics[dbName].primaryMetrics.idxEntryBytesRead, 3);
        assert.eq(metrics[dbName].primaryMetrics.idxEntryUnitsRead, 1);
        assert.gt(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 0);
    });

    clearMetrics(primary);

    // Ensure that the updated document eventually comes through the change stream.
    assert.soon(() => {
        if (cur.hasNext()) {
            return true;
        }
        print("Change stream returned no data. Clearing metrics and retrying.");
        clearMetrics(primary);
        return false;
    });
    const newDoc = Object.assign({b: 0}, doc);
    let res = cur.next();
    assert.docEq(newDoc, res.fullDocument, res);
    assertMetrics(primary, (metrics) => {
        // Performs at least three seeks (oplog, _id index, collection), reads at least one entry
        // from the oplog, once from the collection, and then returns one large response document.
        assert.gte(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.gte(metrics[dbName].primaryMetrics.docUnitsRead, 2);
        assert.eq(metrics[dbName].primaryMetrics.idxEntryBytesRead, 3);
        assert.eq(metrics[dbName].primaryMetrics.idxEntryUnitsRead, 1);
        assert.gt(metrics[dbName].primaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].primaryMetrics.docUnitsReturned, 4);
    });
})();

(function changeStreamSecondary() {
    clearMetrics(secondary);
    const cur = secondaryDB[collName].watch([], {fullDocument: "updateLookup"});

    assertMetrics(secondary, (metrics) => {
        // The first aggregate operation will read one document from the oplog, size not guaranteed.
        assert.gt(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.docUnitsRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].secondaryMetrics.docUnitsReturned, 0);
    });

    // Ensure that while nothing is returned from the change stream, the server still measures read
    // activity.
    clearMetrics(secondary);
    assert(!cur.hasNext());
    assertMetrics(secondary, (metrics) => {
        // Calling hasNext may perform many reads from the oplog, and the size is not guaranteed.
        assert.gt(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.docUnitsRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.cursorSeeks, 0);
        assert.eq(metrics[dbName].secondaryMetrics.docUnitsReturned, 0);
    });

    // Insert a document and ensure the secondary collects no metrics.
    clearMetrics(secondary);
    const doc = {_id: nextId, a: nextId};
    assert.commandWorked(primaryDB[collName].insert(doc));
    rst.awaitReplication();
    assertMetrics(secondary, (metrics) => {
        assert(!metrics[dbName]);
    });

    // Ensure that the inserted document eventually comes through the change stream.
    assert.soon(() => {
        if (cur.hasNext()) {
            return true;
        }
        print("Change stream returned no data. Clearing metrics and retrying.");
        clearMetrics(secondary);
        return false;
    });
    assert.eq(doc, cur.next().fullDocument);
    assertMetrics(secondary, (metrics) => {
        // Performs one seek on the oplog, read at least one entry, and then returns one large
        // response document.
        assert.gt(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.docUnitsRead, 0);
        assert.gte(metrics[dbName].secondaryMetrics.cursorSeeks, 1);
        assert.gte(metrics[dbName].secondaryMetrics.docUnitsReturned, 3);
    });

    // Update the document and ensure the secondary collects no metrics.
    clearMetrics(secondary);
    assert.commandWorked(primaryDB[collName].update(doc, {$set: {b: 0}}));
    rst.awaitReplication();
    assertMetrics(secondary, (metrics) => {
        assert(!metrics[dbName]);
    });

    // Ensure that the updated document eventually comes through the change stream.
    assert.soon(() => {
        if (cur.hasNext()) {
            return true;
        }
        print("Change stream returned no data. Clearing metrics and retrying.");
        clearMetrics(secondary);
        return false;
    });
    const newDoc = Object.assign({b: 0}, doc);
    let res = cur.next();
    assert.docEq(newDoc, res.fullDocument, res);
    assertMetrics(secondary, (metrics) => {
        // Performs at least three seeks (oplog, _id index, collection), reads at least one entry
        // from the oplog and once from the collection, and then returns one large response
        // document.
        assert.gt(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.gt(metrics[dbName].secondaryMetrics.docUnitsRead, 0);
        assert.eq(metrics[dbName].secondaryMetrics.idxEntryBytesRead, 3);
        assert.eq(metrics[dbName].secondaryMetrics.idxEntryUnitsRead, 1);
        assert.gte(metrics[dbName].secondaryMetrics.cursorSeeks, 3);
        assert.gte(metrics[dbName].secondaryMetrics.docUnitsReturned, 4);
    });
})();
rst.stopSet();
}());

/**
 * Tests that a whole-db or whole-cluster change stream can succeed when the
 * "fullDocumentBeforeChange" option is set to "required", so long as the user
 * specifies a pipeline that filters out changes to any collections which do not
 * have pre-images enabled.
 *
 * @tags: [uses_change_streams, requires_replication]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const testDB = rst.getPrimary().getDB(jsTestName());
const adminDB = rst.getPrimary().getDB("admin");

// Create one collection that has pre-image recording enabled...
const collWithPreImages = testDB.coll_with_pre_images;
assert.commandWorked(testDB.createCollection(collWithPreImages.getName(), {recordPreImages: true}));

//... and one collection which has pre-images disabled.
const collWithNoPreImages = testDB.coll_with_no_pre_images;
assert.commandWorked(
    testDB.createCollection(collWithNoPreImages.getName(), {recordPreImages: false}));

//... and a collection that will hold the sentinal document that marks the end of changes
const sentinelColl = testDB.sentinelColl;

// Insert one document as a starting point and extract its resume token.
const resumeToken = (() => {
    const csCursor = collWithNoPreImages.watch();
    assert.commandWorked(collWithNoPreImages.insert({_id: -1}));
    assert.soon(() => csCursor.hasNext());
    return csCursor.next()._id;
})();

// Write a series of interleaving operations to each collection.
assert.commandWorked(collWithNoPreImages.insert({_id: 0}));
assert.commandWorked(collWithPreImages.insert({_id: 0}));

assert.commandWorked(collWithNoPreImages.update({_id: 0}, {foo: "bar"}));
assert.commandWorked(collWithPreImages.update({_id: 0}, {foo: "bar"}));

assert.commandWorked(collWithNoPreImages.update({_id: 0}, {$set: {foo: "baz"}}));
assert.commandWorked(collWithPreImages.update({_id: 0}, {$set: {foo: "baz"}}));

assert.commandWorked(collWithNoPreImages.remove({_id: 0}));
assert.commandWorked(collWithPreImages.remove({_id: 0}));

// This will generate an insert change event we can wait for on the change stream that indicates
// we have reached the end of changes this test is interested in.
assert.commandWorked(sentinelColl.insert({_id: "last_change_sentinel"}));

// Confirm that attempting to open a whole-db stream on this database with mode "required" fails.
assert.throwsWithCode(function() {
    const wholeDBStream =
        testDB.watch([], {fullDocumentBeforeChange: "required", resumeAfter: resumeToken});

    return assert.soon(() => wholeDBStream.hasNext() &&
                           wholeDBStream.next().documentKey._id === "last_change_sentinel");
}, 51770);

// Confirm that attempting to open a whole-cluster stream on with mode "required" fails.
assert.throwsWithCode(function() {
    const wholeClusterStream = adminDB.watch([], {
        fullDocumentBeforeChange: "required",
        resumeAfter: resumeToken,
        allChangesForCluster: true,
    });

    return assert.soon(() => wholeClusterStream.hasNext() &&
                           wholeClusterStream.next().documentKey._id == "last_change_sentinel");
}, 51770);

// However, if we open a whole-db or whole-cluster stream that filters for only the namespace with
// pre-images, then the cursor can proceed. This is because the $match gets moved ahead of the
// pre-image lookup stage, so no events from 'collWithNoPreImages' ever reach it, and therefore
// don't trip the validation checks for the existence of the pre-image.
for (let runOnDB of [testDB, adminDB]) {
    // Open a whole-db or whole-cluster stream that filters for the 'collWithPreImages' namespace.
    const csCursor = runOnDB.watch(
        [{$match: {$or: [{_id: resumeToken}, {"ns.coll": collWithPreImages.getName()}]}}], {
            fullDocumentBeforeChange: "required",
            resumeAfter: resumeToken,
            allChangesForCluster: (runOnDB === adminDB)
        });

    // The list of events and pre-images that we expect to see in the stream.
    const expectedPreImageEvents = [
        {opType: "insert", fullDocumentBeforeChange: null},
        {opType: "replace", fullDocumentBeforeChange: {_id: 0}},
        {opType: "update", fullDocumentBeforeChange: {_id: 0, foo: "bar"}},
        {opType: "delete", fullDocumentBeforeChange: {_id: 0, foo: "baz"}}
    ];

    // Confirm that the expected events are all seen, and in the expected order.
    for (let expectedEvent of expectedPreImageEvents) {
        assert.soon(() => csCursor.hasNext());
        const observedEvent = csCursor.next();
        assert.eq(observedEvent.operationType, expectedEvent.opType);
        assert.eq(observedEvent.fullDocumentBeforeChange, expectedEvent.fullDocumentBeforeChange);
    }
}

rst.stopSet();
})();

/**
 * Test that mongoS rejects change streams which request 'fullDocumentBeforeChange' pre-images.
 *
 * @tags: [uses_change_streams, requires_replication]
 */
(function() {
'use strict';

const st = new ShardingTest({
    shards: 1,
    mongos: 1,
    config: 1,
});

const shard = st.shard0;
const mongos = st.s;

// Test that we cannot create a collection with pre-images enabled in a sharded cluster.
assert.commandFailed(shard.getDB("test").runCommand({create: "test", recordPreImages: true}));

// Test that attempting to run $changeStream with {fullDocumentBeforeChange: "whenAvailable"} fails.
assert.commandFailedWithCode(mongos.getDB("test").runCommand({
    aggregate: 1,
    pipeline: [{$changeStream: {fullDocumentBeforeChange: "whenAvailable"}}],
    cursor: {}
}),
                             51771);

// Test that attempting to run $changeStream with {fullDocumentBeforeChange: "required"} fails.
assert.commandFailedWithCode(mongos.getDB("test").runCommand({
    aggregate: 1,
    pipeline: [{$changeStream: {fullDocumentBeforeChange: "required"}}],
    cursor: {}
}),
                             51771);

st.stop();
}());

/**
 * Tests that a change stream can be resumed from a point in time before a new shard was added to
 * the cluster. Exercises the fix for SERVER-42232.
 * @tags: [
 *   requires_sharding,
 *   uses_change_streams,
 * ]
 */
(function() {
"use strict";

const rsNodeOptions = {
    setParameter: {writePeriodicNoops: true, periodicNoopIntervalSecs: 1}
};
const st =
    new ShardingTest({shards: 1, mongos: 1, rs: {nodes: 1}, other: {rsOptions: rsNodeOptions}});

const mongosDB = st.s.getDB(jsTestName());
const coll = mongosDB.test;

// Helper function to confirm that a stream sees an expected sequence of documents. This
// function also pushes all observed changes into the supplied 'eventList' array.
function assertAllEventsObserved(changeStream, expectedDocs, eventList) {
    for (let expectedDoc of expectedDocs) {
        assert.soon(() => changeStream.hasNext());
        const nextEvent = changeStream.next();
        assert.eq(nextEvent.fullDocument, expectedDoc);
        if (eventList) {
            eventList.push(nextEvent);
        }
    }
}

// Helper function to add a new ReplSetTest shard into the cluster. Using single-node shards
// ensures that the "initiating set" entry cannot be rolled back.
function addShardToCluster(shardName) {
    const replTest = new ReplSetTest({name: shardName, nodes: 1, nodeOptions: rsNodeOptions});
    replTest.startSet({shardsvr: ""});
    replTest.initiate();
    assert.commandWorked(st.s.adminCommand({addShard: replTest.getURL(), name: shardName}));

    // Verify that the new shard's first oplog entry contains the string "initiating set". This
    // is used by change streams as a sentinel to indicate that no writes have occurred on the
    // replica set before this point.
    const firstOplogEntry = replTest.getPrimary().getCollection("local.oplog.rs").findOne();
    assert.docEq(firstOplogEntry.o, {msg: "initiating set"});
    assert.eq(firstOplogEntry.op, "n");

    return replTest;
}

// Helper function to resume from each event in a given list and confirm that the resumed stream
// sees the subsequent events in the correct expected order.
function assertCanResumeFromEachEvent(eventList) {
    for (let i = 0; i < eventList.length; ++i) {
        const resumedStream = coll.watch([], {resumeAfter: eventList[i]._id});
        for (let j = i + 1; j < eventList.length; ++j) {
            assert.soon(() => resumedStream.hasNext());
            assert.docEq(resumedStream.next(), eventList[j]);
        }
        resumedStream.close();
    }
}

// Open a change stream on the unsharded test collection.
const csCursor = coll.watch();
assert(!csCursor.hasNext());
const changeList = [];

// Insert some docs into the unsharded collection, and obtain a change stream event for each.
const insertedDocs = [{_id: 1}, {_id: 2}, {_id: 3}];
assert.commandWorked(coll.insert(insertedDocs));
assertAllEventsObserved(csCursor, insertedDocs, changeList);

// Verify that, for a brand new shard, we can start at an operation time before the set existed.
let startAtDawnOfTimeCursor = coll.watch([], {startAtOperationTime: Timestamp(1, 1)});
assertAllEventsObserved(startAtDawnOfTimeCursor, insertedDocs);
startAtDawnOfTimeCursor.close();

// Add a new shard into the cluster. Wait three seconds so that its initiation time is
// guaranteed to be later than any of the events in the existing shard's oplog.
const newShard1 = sleep(3000) || addShardToCluster("newShard1");

// .. and confirm that we can resume from any point before the shard was added.
assertCanResumeFromEachEvent(changeList);

// Now shard the collection on _id and move one chunk to the new shard.
st.shardColl(coll, {_id: 1}, {_id: 3}, false);
assert.commandWorked(st.s.adminCommand(
    {moveChunk: coll.getFullName(), find: {_id: 3}, to: "newShard1", _waitForDelete: true}));

// Insert some new documents into the new shard and verify that the original stream sees them.
const newInsertedDocs = [{_id: 4}, {_id: 5}];
assert.commandWorked(coll.insert(newInsertedDocs));
assertAllEventsObserved(csCursor, newInsertedDocs, changeList);

// Add a third shard into the cluster...
const newShard2 = sleep(3000) || addShardToCluster("newShard2");

// ... and verify that we can resume the stream from any of the preceding events.
assertCanResumeFromEachEvent(changeList);

// Now drop the collection, and verify that we can still resume from any point.
assert(coll.drop());
for (let expectedEvent of ["drop", "invalidate"]) {
    assert.soon(() => csCursor.hasNext());
    assert.eq(csCursor.next().operationType, expectedEvent);
}
assertCanResumeFromEachEvent(changeList);

// Verify that we can start at an operation time before the cluster existed and see all events.
startAtDawnOfTimeCursor = coll.watch([], {startAtOperationTime: Timestamp(1, 1)});
assertAllEventsObserved(startAtDawnOfTimeCursor, insertedDocs.concat(newInsertedDocs));
startAtDawnOfTimeCursor.close();

st.stop();

// Stop the new shards manually since the ShardingTest doesn't know anything about them.
newShard1.stopSet();
newShard2.stopSet();
})();

// Attempt to resume a change stream from the resume token for an "invalidate" event when the "drop"
// event that caused the invalidation is the last thing in the primary shard's oplog. There should
// be no error creating the new change stream, which should initially see no events. Reproduces the
// bug described in SERVER-41196.
// @tags: [
//   requires_sharding,
//   uses_change_streams,
// ]
(function() {
"use strict";

// The edge case we are testing occurs on an unsharded collection in a sharded cluster. We
// create a cluster with just one shard to ensure the test never blocks for another shard.
const st = new ShardingTest(
    {shards: 1, mongos: 1, rs: {nodes: 1, setParameter: {writePeriodicNoops: false}}});

const mongosDB = st.s0.getDB(jsTestName());
const mongosColl = mongosDB[jsTestName()];

(function testStartAfterInvalidate() {
    // Start a change stream that matches on the invalidate event.
    const changeStream = mongosColl.watch([{'$match': {'operationType': 'invalidate'}}]);

    // Create the collection by inserting into it and then drop the collection, thereby generating
    // an invalidate event.
    assert.commandWorked(mongosColl.insert({_id: 1}));
    assert(mongosColl.drop());
    assert.soon(() => changeStream.hasNext());
    const invalidateEvent = changeStream.next();

    // Resuming the change stream using the invalidate event allows us to see events after the drop.
    const resumeStream = mongosColl.watch([], {startAfter: invalidateEvent["_id"]});

    // The PBRT returned with the first (empty) batch should match the resume token we supplied.
    assert.eq(bsonWoCompare(resumeStream.getResumeToken(), invalidateEvent["_id"]), 0);

    // Initially, there should be no events visible after the drop.
    assert(!resumeStream.hasNext());

    // Add one last event and make sure the change stream sees it.
    assert.commandWorked(mongosColl.insert({_id: 2}));
    assert.soon(() => resumeStream.hasNext());
    const afterDrop = resumeStream.next();
    assert.eq(afterDrop.operationType, "insert");
    assert.eq(afterDrop.fullDocument, {_id: 2});
})();

// Drop the collection before running the subsequent test.
assert(mongosColl.drop());

(function testStartAfterInvalidateOnEmptyCollection() {
    // Start a change stream on 'mongosColl', then create and drop the collection.
    const changeStream = mongosColl.watch([]);
    assert.commandWorked(mongosDB.createCollection(jsTestName()));
    assert(mongosColl.drop());

    // Wait until we see the invalidation, and store the associated resume token.
    assert.soon(() => {
        return changeStream.hasNext() && changeStream.next().operationType === "invalidate";
    });

    const invalidateResumeToken = changeStream.getResumeToken();

    // Recreate and then immediately drop the collection again to make sure that change stream when
    // opened with the invalidate resume token sees this invalidate event.
    assert.commandWorked(mongosDB.createCollection(jsTestName()));
    assert(mongosColl.drop());

    // Open the change stream using the invalidate resume token and verify that the change stream
    // sees the invalidated event from the second collection drop.
    const resumeStream = mongosColl.watch([], {startAfter: invalidateResumeToken});

    assert.soon(() => {
        return resumeStream.hasNext() && resumeStream.next().operationType === "invalidate";
    });
})();

st.stop();
})();

/**
 * Confirms that change streams only see committed operations for prepared transactions.
 * @tags: [
 *   requires_majority_read_concern,
 *   uses_change_streams,
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */
(function() {
"use strict";

load("jstests/core/txns/libs/prepare_helpers.js");  // For PrepareHelpers.

const dbName = "test";
const collName = "change_stream_transaction";

/**
 * This test sets an internal parameter in order to force transactions with more than 4
 * operations to span multiple oplog entries, making it easier to test that scenario.
 */
const maxOpsInOplogEntry = 4;

/**
 * Asserts that the expected operation type and documentKey are found on the change stream
 * cursor. Returns the change stream document.
 */
function assertWriteVisible(cursor, operationType, documentKey) {
    assert.soon(() => cursor.hasNext());
    const changeDoc = cursor.next();
    assert.eq(operationType, changeDoc.operationType, changeDoc);
    assert.eq(documentKey, changeDoc.documentKey, changeDoc);
    return changeDoc;
}

/**
 * Asserts that the expected operation type and documentKey are found on the change stream
 * cursor. Pushes the corresponding resume token and change stream document to an array.
 */
function assertWriteVisibleWithCapture(cursor, operationType, documentKey, changeList) {
    const changeDoc = assertWriteVisible(cursor, operationType, documentKey);
    changeList.push(changeDoc);
}

/**
 * Asserts that there are no changes waiting on the change stream cursor.
 */
function assertNoChanges(cursor) {
    assert(!cursor.hasNext(), () => {
        return "Unexpected change set: " + tojson(cursor.toArray());
    });
}

function runTest(conn) {
    const db = conn.getDB(dbName);
    const coll = db.getCollection(collName);
    const unwatchedColl = db.getCollection(collName + "_unwatched");
    let changeList = [];

    // Collections must be created outside of any transaction.
    assert.commandWorked(db.createCollection(coll.getName()));
    assert.commandWorked(db.createCollection(unwatchedColl.getName()));

    //
    // Start transaction 1.
    //
    const session1 = db.getMongo().startSession();
    const sessionDb1 = session1.getDatabase(dbName);
    const sessionColl1 = sessionDb1[collName];
    session1.startTransaction({readConcern: {level: "majority"}});

    //
    // Start transaction 2.
    //
    const session2 = db.getMongo().startSession();
    const sessionDb2 = session2.getDatabase(dbName);
    const sessionColl2 = sessionDb2[collName];
    session2.startTransaction({readConcern: {level: "majority"}});

    //
    // Start transaction 3.
    //
    const session3 = db.getMongo().startSession();
    const sessionDb3 = session3.getDatabase(dbName);
    const sessionColl3 = sessionDb3[collName];
    session3.startTransaction({readConcern: {level: "majority"}});

    // Open a change stream on the test collection.
    const changeStreamCursor = coll.watch();

    // Insert a document and confirm that the change stream has it.
    assert.commandWorked(coll.insert({_id: "no-txn-doc-1"}, {writeConcern: {w: "majority"}}));
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "no-txn-doc-1"}, changeList);

    // Insert two documents under each transaction and confirm no change stream updates.
    assert.commandWorked(sessionColl1.insert([{_id: "txn1-doc-1"}, {_id: "txn1-doc-2"}]));
    assert.commandWorked(sessionColl2.insert([{_id: "txn2-doc-1"}, {_id: "txn2-doc-2"}]));
    assertNoChanges(changeStreamCursor);

    // Update one document under each transaction and confirm no change stream updates.
    assert.commandWorked(sessionColl1.update({_id: "txn1-doc-1"}, {$set: {"updated": 1}}));
    assert.commandWorked(sessionColl2.update({_id: "txn2-doc-1"}, {$set: {"updated": 1}}));
    assertNoChanges(changeStreamCursor);

    // Update and then remove the second doc under each transaction and confirm no change stream
    // events are seen.
    assert.commandWorked(
        sessionColl1.update({_id: "txn1-doc-2"}, {$set: {"update-before-delete": 1}}));
    assert.commandWorked(
        sessionColl2.update({_id: "txn2-doc-2"}, {$set: {"update-before-delete": 1}}));
    assert.commandWorked(sessionColl1.remove({_id: "txn1-doc-2"}));
    assert.commandWorked(sessionColl2.remove({_id: "txn2-doc-2"}));
    assertNoChanges(changeStreamCursor);

    // Perform a write to the 'session1' transaction in a collection that is not being watched
    // by 'changeStreamCursor'. We do not expect to see this write in the change stream either
    // now or on commit.
    assert.commandWorked(
        sessionDb1[unwatchedColl.getName()].insert({_id: "txn1-doc-unwatched-collection"}));
    assertNoChanges(changeStreamCursor);

    // Perform a write to the 'session3' transaction in a collection that is not being watched
    // by 'changeStreamCursor'. We do not expect to see this write in the change stream either
    // now or on commit.
    assert.commandWorked(
        sessionDb3[unwatchedColl.getName()].insert({_id: "txn3-doc-unwatched-collection"}));
    assertNoChanges(changeStreamCursor);

    // Perform a write outside of a transaction and confirm that the change stream sees only
    // this write.
    assert.commandWorked(coll.insert({_id: "no-txn-doc-2"}, {writeConcern: {w: "majority"}}));
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "no-txn-doc-2"}, changeList);
    assertNoChanges(changeStreamCursor);

    let prepareTimestampTxn1;
    prepareTimestampTxn1 = PrepareHelpers.prepareTransaction(session1);
    assertNoChanges(changeStreamCursor);

    assert.commandWorked(coll.insert({_id: "no-txn-doc-3"}, {writeConcern: {w: "majority"}}));
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "no-txn-doc-3"}, changeList);

    //
    // Commit first transaction and confirm expected changes.
    //
    assert.commandWorked(PrepareHelpers.commitTransaction(session1, prepareTimestampTxn1));
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "txn1-doc-1"}, changeList);
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "txn1-doc-2"}, changeList);
    assertWriteVisibleWithCapture(changeStreamCursor, "update", {_id: "txn1-doc-1"}, changeList);
    assertWriteVisibleWithCapture(changeStreamCursor, "update", {_id: "txn1-doc-2"}, changeList);
    assertWriteVisibleWithCapture(changeStreamCursor, "delete", {_id: "txn1-doc-2"}, changeList);
    assertNoChanges(changeStreamCursor);

    // Transition the second transaction to prepared. We skip capturing the prepare
    // timestamp it is not required for abortTransaction_forTesting().
    PrepareHelpers.prepareTransaction(session2);
    assertNoChanges(changeStreamCursor);

    assert.commandWorked(coll.insert({_id: "no-txn-doc-4"}, {writeConcern: {w: "majority"}}));
    assertWriteVisibleWithCapture(changeStreamCursor, "insert", {_id: "no-txn-doc-4"}, changeList);

    //
    // Abort second transaction.
    //
    session2.abortTransaction_forTesting();
    assertNoChanges(changeStreamCursor);

    //
    // Start transaction 4.
    //
    const session4 = db.getMongo().startSession();
    const sessionDb4 = session4.getDatabase(dbName);
    const sessionColl4 = sessionDb4[collName];
    session4.startTransaction({readConcern: {level: "majority"}});

    // Perform enough writes to fill up one applyOps.
    const txn4Inserts = Array.from({length: maxOpsInOplogEntry},
                                   (_, index) => ({_id: {name: "txn4-doc", index: index}}));
    txn4Inserts.forEach(function(doc) {
        sessionColl4.insert(doc);
        assertNoChanges(changeStreamCursor);
    });

    // Perform enough writes to an unwatched collection to fill up a second applyOps. We
    // specifically want to test the case where a multi-applyOps transaction has no relevant
    // updates in its final applyOps.
    txn4Inserts.forEach(function(doc) {
        assert.commandWorked(sessionDb4[unwatchedColl.getName()].insert(doc));
        assertNoChanges(changeStreamCursor);
    });

    //
    // Start transaction 5.
    //
    const session5 = db.getMongo().startSession();
    const sessionDb5 = session5.getDatabase(dbName);
    const sessionColl5 = sessionDb5[collName];
    session5.startTransaction({readConcern: {level: "majority"}});

    // Perform enough writes to span 3 applyOps entries.
    const txn5Inserts = Array.from({length: 3 * maxOpsInOplogEntry},
                                   (_, index) => ({_id: {name: "txn5-doc", index: index}}));
    txn5Inserts.forEach(function(doc) {
        assert.commandWorked(sessionColl5.insert(doc));
        assertNoChanges(changeStreamCursor);
    });

    //
    // Prepare and commit transaction 5.
    //
    const prepareTimestampTxn5 = PrepareHelpers.prepareTransaction(session5);
    assertNoChanges(changeStreamCursor);
    assert.commandWorked(PrepareHelpers.commitTransaction(session5, prepareTimestampTxn5));
    txn5Inserts.forEach(function(doc) {
        assertWriteVisibleWithCapture(changeStreamCursor, "insert", doc, changeList);
    });

    //
    // Commit transaction 4 without preparing.
    //
    session4.commitTransaction();
    txn4Inserts.forEach(function(doc) {
        assertWriteVisibleWithCapture(changeStreamCursor, "insert", doc, changeList);
    });
    assertNoChanges(changeStreamCursor);

    changeStreamCursor.close();

    // Test that change stream resume returns the expected set of documents at each point
    // captured by this test.
    for (let i = 0; i < changeList.length; ++i) {
        const resumeCursor = coll.watch([], {startAfter: changeList[i]._id});

        for (let x = (i + 1); x < changeList.length; ++x) {
            const expectedChangeDoc = changeList[x];
            assertWriteVisible(
                resumeCursor, expectedChangeDoc.operationType, expectedChangeDoc.documentKey);
        }

        assertNoChanges(resumeCursor);
        resumeCursor.close();
    }

    //
    // Prepare and commit the third transaction and confirm that there are no visible changes.
    //
    let prepareTimestampTxn3;
    prepareTimestampTxn3 = PrepareHelpers.prepareTransaction(session3);
    assertNoChanges(changeStreamCursor);

    assert.commandWorked(PrepareHelpers.commitTransaction(session3, prepareTimestampTxn3));
    assertNoChanges(changeStreamCursor);

    assert.commandWorked(db.dropDatabase());
}

let replSetTestDescription = {nodes: 1};
if (!jsTest.options().setParameters.hasOwnProperty(
        "maxNumberOfTransactionOperationsInSingleOplogEntry")) {
    // Configure the replica set to use our value for maxOpsInOplogEntry.
    replSetTestDescription.nodeOptions = {
        setParameter: {maxNumberOfTransactionOperationsInSingleOplogEntry: maxOpsInOplogEntry}
    };
} else {
    // The test is executing in a build variant that already defines its own override value for
    // maxNumberOfTransactionOperationsInSingleOplogEntry. Even though the build variant's
    // choice for this override won't test the same edge cases, the test should still succeed.
}
const rst = new ReplSetTest(replSetTestDescription);
rst.startSet();
rst.initiate();

runTest(rst.getPrimary());

rst.stopSet();
})();

/**
 * Tests that a change stream on a sharded collection with a non-simple default collation is not
 * erroneously invalidated upon chunk migration. Reproduction script for the bug in SERVER-33944.
 * @tags: [
 *   requires_journaling,
 *   requires_replication,
 * ]
 */
(function() {
load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.
load("jstests/libs/change_stream_util.js");        // For 'ChangeStreamTest'.

const st = new ShardingTest({
    shards: 2,
    mongos: 1,
    rs: {
        nodes: 1,
    },
});

const testDB = st.s.getDB(jsTestName());

// Enable sharding on the test database and ensure that the primary is shard0.
assert.commandWorked(testDB.adminCommand({enableSharding: testDB.getName()}));
st.ensurePrimaryShard(testDB.getName(), st.shard0.shardName);

const caseInsensitiveCollectionName = "change_stream_case_insensitive";
const caseInsensitive = {
    locale: "en_US",
    strength: 2
};

// Create the collection with a case-insensitive collation, then shard it on {shardKey: 1}.
const caseInsensitiveCollection = assertDropAndRecreateCollection(
    testDB, caseInsensitiveCollectionName, {collation: caseInsensitive});
assert.commandWorked(
    caseInsensitiveCollection.createIndex({shardKey: 1}, {collation: {locale: "simple"}}));
assert.commandWorked(testDB.adminCommand({
    shardCollection: caseInsensitiveCollection.getFullName(),
    key: {shardKey: 1},
    collation: {locale: "simple"}
}));

// Verify that the collection does not exist on shard1.
assert(!st.shard1.getCollection(caseInsensitiveCollection.getFullName()).exists());

// Now open a change stream on the collection.
const cst = new ChangeStreamTest(testDB);
const csCursor = cst.startWatchingChanges({
    pipeline: [{$changeStream: {}}, {$project: {docId: "$documentKey.shardKey"}}],
    collection: caseInsensitiveCollection
});

// Insert some documents into the collection.
assert.commandWorked(caseInsensitiveCollection.insert({shardKey: 0, text: "aBc"}));
assert.commandWorked(caseInsensitiveCollection.insert({shardKey: 1, text: "abc"}));

// Move a chunk from shard0 to shard1. This will create the collection on shard1.
assert.commandWorked(testDB.adminCommand({
    moveChunk: caseInsensitiveCollection.getFullName(),
    find: {shardKey: 1},
    to: st.rs1.getURL(),
    _waitForDelete: false
}));

// Attempt to read from the change stream. We should see both inserts, without an invalidation.
cst.assertNextChangesEqual({cursor: csCursor, expectedChanges: [{docId: 0}, {docId: 1}]});

st.stop();
})();

// Tests the behaviour of change streams on an oplog which rolls over.
// @tags: [
//   requires_journaling,
//   requires_majority_read_concern,
//   uses_change_streams,
// ]
(function() {
"use strict";

load('jstests/replsets/rslib.js');           // For getLatestOp, getFirstOplogEntry.
load('jstests/libs/change_stream_util.js');  // For ChangeStreamTest.

const oplogSize = 1;  // size in MB
const rst = new ReplSetTest({nodes: 1, oplogSize: oplogSize});

rst.startSet();
rst.initiate();

const testDB = rst.getPrimary().getDB(jsTestName());
const testColl = testDB[jsTestName()];

const cst = new ChangeStreamTest(testDB);

// Write a document to the test collection.
assert.commandWorked(testColl.insert({_id: 1}, {writeConcern: {w: "majority"}}));

let changeStream = cst.startWatchingChanges(
    {pipeline: [{$changeStream: {}}], collection: testColl.getName(), includeToken: true});

// We awaited the replication of the insert, so the change stream shouldn't return them.
assert.commandWorked(testColl.update({_id: 1}, {$set: {updated: true}}));

// Record current time to resume a change stream later in the test.
const resumeTimeFirstUpdate = testDB.runCommand({hello: 1}).$clusterTime.clusterTime;

assert.commandWorked(testColl.update({_id: 1}, {$set: {updated: true}}));

// Test that we see the the update, and remember its resume tokens.
let next = cst.getOneChange(changeStream);
assert.eq(next.operationType, "update");
assert.eq(next.documentKey._id, 1);
const resumeTokenFromFirstUpdate = next._id;

// Write some additional documents, then test we can resume after the first update.
assert.commandWorked(testColl.insert({_id: 2}, {writeConcern: {w: "majority"}}));
assert.commandWorked(testColl.insert({_id: 3}, {writeConcern: {w: "majority"}}));

changeStream = cst.startWatchingChanges({
    pipeline: [{$changeStream: {resumeAfter: resumeTokenFromFirstUpdate}}],
    aggregateOptions: {cursor: {batchSize: 0}},
    collection: testColl.getName()
});

for (let nextExpectedId of [2, 3]) {
    assert.eq(cst.getOneChange(changeStream).documentKey._id, nextExpectedId);
}

// Test that the change stream can see additional inserts into the collection.
assert.commandWorked(testColl.insert({_id: 4}, {writeConcern: {w: "majority"}}));
assert.commandWorked(testColl.insert({_id: 5}, {writeConcern: {w: "majority"}}));

for (let nextExpectedId of [4, 5]) {
    assert.eq(cst.getOneChange(changeStream).documentKey._id, nextExpectedId);
}

// Confirm that we can begin a stream at a timestamp that precedes the start of the oplog, if
// the first entry in the oplog is the replica set initialization message.
const firstOplogEntry = getFirstOplogEntry(rst.getPrimary());
assert.eq(firstOplogEntry.o.msg, "initiating set");
assert.eq(firstOplogEntry.op, "n");

const startAtDawnOfTimeStream = cst.startWatchingChanges({
    pipeline: [{$changeStream: {startAtOperationTime: Timestamp(1, 1)}}],
    aggregateOptions: {cursor: {batchSize: 0}},
    collection: testColl.getName()
});

// The first entry we see should be the initial insert into the collection.
const firstStreamEntry = cst.getOneChange(startAtDawnOfTimeStream);
assert.eq(firstStreamEntry.operationType, "insert");
assert.eq(firstStreamEntry.documentKey._id, 1);

// Test that the stream can't resume if the resume token is no longer present in the oplog.

// Roll over the entire oplog such that none of the events are still present.
const primaryNode = rst.getPrimary();
const mostRecentOplogEntry = getLatestOp(primaryNode);
assert.neq(mostRecentOplogEntry, null);
const largeStr = new Array(4 * 1024 * oplogSize).join('abcdefghi');

function oplogIsRolledOver() {
    // The oplog has rolled over if the op that used to be newest is now older than the
    // oplog's current oldest entry. Said another way, the oplog is rolled over when
    // everything in the oplog is newer than what used to be the newest entry.
    return bsonWoCompare(mostRecentOplogEntry.ts,
                         getFirstOplogEntry(primaryNode, {readConcern: "majority"}).ts) < 0;
}

while (!oplogIsRolledOver()) {
    assert.commandWorked(testColl.insert({long_str: largeStr}, {writeConcern: {w: "majority"}}));
}

// Confirm that attempting to continue reading an existing change stream throws CappedPositionLost.
assert.commandFailedWithCode(
    testDB.runCommand({getMore: startAtDawnOfTimeStream.id, collection: testColl.getName()}),
    ErrorCodes.CappedPositionLost);

// Now confirm that attempting to resumeAfter or startAtOperationTime fails.
ChangeStreamTest.assertChangeStreamThrowsCode({
    db: testDB,
    collName: testColl.getName(),
    pipeline: [{$changeStream: {resumeAfter: resumeTokenFromFirstUpdate}}],
    expectedCode: ErrorCodes.ChangeStreamHistoryLost
});

ChangeStreamTest.assertChangeStreamThrowsCode({
    db: testDB,
    collName: testColl.getName(),
    pipeline: [{$changeStream: {startAtOperationTime: resumeTimeFirstUpdate}}],
    expectedCode: ErrorCodes.ChangeStreamHistoryLost
});

// We also can't start a stream from the "dawn of time" any more, since the first entry in the
// oplog is no longer the replica set initialization message.
ChangeStreamTest.assertChangeStreamThrowsCode({
    db: testDB,
    collName: testColl.getName(),
    pipeline: [{$changeStream: {startAtOperationTime: Timestamp(1, 1)}}],
    expectedCode: ErrorCodes.ChangeStreamHistoryLost
});

cst.cleanUp();
rst.stopSet();
})();

// Tests that the $changeStream requires read concern majority.
// @tags: [
//   requires_majority_read_concern,
//   uses_change_streams,
// ]
(function() {
"use strict";

load("jstests/libs/change_stream_util.js");  // For ChangeStreamTest.
load("jstests/libs/namespace_utils.js");     // For getCollectionNameFromFullNamespace.
load("jstests/libs/write_concern_util.js");  // For stopReplicationOnSecondaries.

const rst = new ReplSetTest({nodes: 2, nodeOptions: {enableMajorityReadConcern: ""}});

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

rst.startSet();
rst.initiate();

const name = "change_stream_require_majority_read_concern";
const db = rst.getPrimary().getDB(name);

// Use ChangeStreamTest to verify that the pipeline returns expected results.
const cst = new ChangeStreamTest(db);

// Attempts to get a document from the cursor with awaitData disabled, and asserts if a
// document is present.
function assertNextBatchIsEmpty(cursor) {
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "disableAwaitDataForGetMoreCmd", mode: "alwaysOn"}));
    let res = assert.commandWorked(db.runCommand({
        getMore: cursor.id,
        collection: getCollectionNameFromFullNamespace(cursor.ns),
        batchSize: 1
    }));
    assert.eq(res.cursor.nextBatch.length, 0);
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "disableAwaitDataForGetMoreCmd", mode: "off"}));
}

// Test read concerns other than "majority" are not supported.
const primaryColl = db.foo;
assert.commandWorked(primaryColl.insert({_id: 1}, {writeConcern: {w: "majority"}}));
let res = primaryColl.runCommand({
    aggregate: primaryColl.getName(),
    pipeline: [{$changeStream: {}}],
    cursor: {},
    readConcern: {level: "local"},
});
assert.commandFailedWithCode(res, ErrorCodes.InvalidOptions);
res = primaryColl.runCommand({
    aggregate: primaryColl.getName(),
    pipeline: [{$changeStream: {}}],
    cursor: {},
    readConcern: {level: "linearizable"},
});
assert.commandFailedWithCode(res, ErrorCodes.InvalidOptions);

// Test that explicit read concern "majority" works.
res = primaryColl.runCommand({
    aggregate: primaryColl.getName(),
    pipeline: [{$changeStream: {}}],
    cursor: {},
    readConcern: {level: "majority"},
});
assert.commandWorked(res);

// Test not specifying readConcern defaults to "majority" read concern.
stopReplicationOnSecondaries(rst);
// Verify that the document just inserted cannot be returned.
let cursor = cst.startWatchingChanges({pipeline: [{$changeStream: {}}], collection: primaryColl});
assert.eq(cursor.firstBatch.length, 0);

// Insert a document on the primary only.
assert.commandWorked(primaryColl.insert({_id: 2}, {writeConcern: {w: 1}}));
assertNextBatchIsEmpty(cursor);

// Restart data replicaiton and wait until the new write becomes visible.
restartReplicationOnSecondaries(rst);
rst.awaitLastOpCommitted();

// Verify that the expected doc is returned because it has been committed.
let doc = cst.getOneChange(cursor);
assert.docEq(doc.operationType, "insert");
assert.docEq(doc.fullDocument, {_id: 2});
rst.stopSet();
}());

// Tests that a change stream requires the correct privileges to be run.
// This test uses the WiredTiger storage engine, which does not support running without journaling.
// @tags: [
//   requires_journaling,
//   requires_majority_read_concern,
//   requires_replication,
// ]
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const password = "test_password";
rst.getPrimary().getDB("admin").createUser(
    {user: "userAdmin", pwd: password, roles: [{db: "admin", role: "userAdminAnyDatabase"}]});
rst.restart(0, {auth: '', keyFile: 'jstests/libs/key1'});

const db = rst.getPrimary().getDB("test");
const coll = db.coll;
const adminDB = db.getSiblingDB("admin");

// Wrap different sections of the test in separate functions to make the scoping clear.
(function createRoles() {
    assert(adminDB.auth("userAdmin", password));
    // Create some collection-level roles.
    db.createRole({
        role: "write",
        roles: [],
        privileges: [{
            resource: {db: db.getName(), collection: coll.getName()},
            actions: ["insert", "update", "remove"]
        }]
    });
    db.createRole({
        role: "find_only",
        roles: [],
        privileges: [{resource: {db: db.getName(), collection: coll.getName()}, actions: ["find"]}]
    });
    db.createRole({
        role: "find_and_change_stream",
        roles: [],
        privileges: [{
            resource: {db: db.getName(), collection: coll.getName()},
            actions: ["find", "changeStream"]
        }]
    });
    db.createRole({
        role: "change_stream_only",
        roles: [],
        privileges:
            [{resource: {db: db.getName(), collection: coll.getName()}, actions: ["changeStream"]}]
    });

    // Create some privileges at the database level.
    db.createRole({
        role: "db_write",
        roles: [],
        privileges: [
            {resource: {db: db.getName(), collection: ""},
             actions: ["insert", "update", "remove"]}
        ]
    });
    db.createRole({
        role: "db_find_only",
        roles: [],
        privileges: [{resource: {db: db.getName(), collection: ""}, actions: ["find"]}]
    });
    db.createRole({
        role: "db_find_and_change_stream",
        roles: [],
        privileges:
            [{resource: {db: db.getName(), collection: ""}, actions: ["find", "changeStream"]}]
    });
    db.createRole({
        role: "db_change_stream_only",
        roles: [],
        privileges: [{resource: {db: db.getName(), collection: ""}, actions: ["changeStream"]}]
    });

    // Create some privileges at the admin database level.
    adminDB.createRole({
        role: "admin_db_write",
        roles: [],
        privileges: [
            {resource: {db: db.getName(), collection: ""},
             actions: ["insert", "update", "remove"]}
        ]
    });
    adminDB.createRole({
        role: "admin_db_find_only",
        roles: [],
        privileges: [{resource: {db: "admin", collection: ""}, actions: ["find"]}]
    });
    adminDB.createRole({
        role: "admin_db_find_and_change_stream",
        roles: [],
        privileges: [{resource: {db: "admin", collection: ""}, actions: ["find", "changeStream"]}]
    });
    adminDB.createRole({
        role: "admin_db_change_stream_only",
        roles: [],
        privileges: [{resource: {db: "admin", collection: ""}, actions: ["changeStream"]}]
    });

    // Create some roles at the any-db, any-collection level.
    adminDB.createRole({
        role: "any_db_find_only",
        roles: [],
        privileges: [{resource: {db: "", collection: ""}, actions: ["find"]}]
    });
    adminDB.createRole({
        role: "any_db_find_and_change_stream",
        roles: [],
        privileges: [{resource: {db: "", collection: ""}, actions: ["find", "changeStream"]}]
    });
    adminDB.createRole({
        role: "any_db_change_stream_only",
        roles: [],
        privileges: [{resource: {db: "", collection: ""}, actions: ["changeStream"]}]
    });

    // Create some roles at the cluster level.
    adminDB.createRole({
        role: "cluster_find_only",
        roles: [],
        privileges: [{resource: {cluster: true}, actions: ["find"]}]
    });
    adminDB.createRole({
        role: "cluster_find_and_change_stream",
        roles: [],
        privileges: [{resource: {cluster: true}, actions: ["find", "changeStream"]}]
    });
    adminDB.createRole({
        role: "cluster_change_stream_only",
        roles: [],
        privileges: [{resource: {cluster: true}, actions: ["changeStream"]}]
    });
}());

(function createUsers() {
    // Create some users for a specific collection. Use the name of the role as the name of the
    // user.
    for (let role of ["write", "find_only", "find_and_change_stream", "change_stream_only"]) {
        db.createUser({user: role, pwd: password, roles: [role]});
    }

    // Create some users at the database level. Use the name of the role as the name of the
    // user, except for the built-in roles.
    for (let role of
             ["db_write", "db_find_only", "db_find_and_change_stream", "db_change_stream_only"]) {
        db.createUser({user: role, pwd: password, roles: [role]});
    }
    db.createUser({user: "db_read", pwd: password, roles: ["read"]});

    // Create some users on the admin database. Use the name of the role as the name of the
    // user, except for the built-in roles.
    for (let role of ["admin_db_write",
                      "admin_db_find_only",
                      "admin_db_find_and_change_stream",
                      "admin_db_change_stream_only"]) {
        adminDB.createUser({user: role, pwd: password, roles: [role]});
    }
    adminDB.createUser({user: "admin_db_read", pwd: password, roles: ["read"]});

    // Create some users with privileges on all databases. Use the name of the role as the name
    // of the user, except for the built-in roles.
    for (let role of ["any_db_find_only",
                      "any_db_find_and_change_stream",
                      "any_db_change_stream_only"]) {
        adminDB.createUser({user: role, pwd: password, roles: [role]});
    }

    // Create some users on the whole cluster. Use the name of the role as the name of the user.
    for (let role of ["cluster_find_only",
                      "cluster_find_and_change_stream",
                      "cluster_change_stream_only"]) {
        adminDB.createUser({user: role, pwd: password, roles: [role]});
    }
}());

(function testPrivilegesForSingleCollection() {
    // Test that users without the required privileges cannot open a change stream. A user
    // needs both the 'find' and 'changeStream' action on the collection. Note in particular
    // that the whole-cluster privileges (specified with {cluster: true}) is not enough to open
    // a change stream on any particular collection.
    for (let userWithoutPrivileges of [{db: db, name: "find_only"},
                                       {db: db, name: "change_stream_only"},
                                       {db: db, name: "write"},
                                       {db: db, name: "db_find_only"},
                                       {db: db, name: "db_change_stream_only"},
                                       {db: db, name: "db_write"},
                                       {db: adminDB, name: "admin_db_find_only"},
                                       {db: adminDB, name: "admin_db_find_and_change_stream"},
                                       {db: adminDB, name: "admin_db_change_stream_only"},
                                       {db: adminDB, name: "admin_db_read"},
                                       {db: adminDB, name: "any_db_find_only"},
                                       {db: adminDB, name: "any_db_change_stream_only"},
                                       {db: adminDB, name: "cluster_find_only"},
                                       {db: adminDB, name: "cluster_find_and_change_stream"},
                                       {db: adminDB, name: "cluster_change_stream_only"}]) {
        jsTestLog(`Testing user ${tojson(userWithoutPrivileges)} cannot open a change stream ` +
                  `on a collection`);
        const db = userWithoutPrivileges.db;
        assert(db.auth(userWithoutPrivileges.name, password));

        assert.commandFailedWithCode(
            coll.getDB().runCommand(
                {aggregate: coll.getName(), pipeline: [{$changeStream: {}}], cursor: {}}),
            ErrorCodes.Unauthorized);

        db.logout();
    }

    // Test that a user with the required privileges can open a change stream.
    for (let userWithPrivileges of [{db: db, name: "find_and_change_stream"},
                                    {db: db, name: "db_find_and_change_stream"},
                                    {db: db, name: "db_read"},
                                    {db: adminDB, name: "any_db_find_and_change_stream"}]) {
        jsTestLog(`Testing user ${tojson(userWithPrivileges)} _can_ open a change stream on a` +
                  ` collection`);
        const db = userWithPrivileges.db;
        assert(db.auth(userWithPrivileges.name, password));

        assert.doesNotThrow(() => coll.watch());

        db.logout();
    }
}());

(function testPrivilegesForWholeDB() {
    // Test that users without the required privileges cannot open a change stream. A user needs
    // both the 'find' and 'changeStream' action on the database. Note in particular that the
    // whole-cluster privileges (specified with {cluster: true}) is not enough to open a change
    // stream on the whole database.
    for (let userWithoutPrivileges of [{db: db, name: "find_only"},
                                       {db: db, name: "change_stream_only"},
                                       {db: db, name: "find_and_change_stream"},
                                       {db: db, name: "write"},
                                       {db: db, name: "db_find_only"},
                                       {db: db, name: "db_change_stream_only"},
                                       {db: db, name: "db_write"},
                                       {db: adminDB, name: "admin_db_find_only"},
                                       {db: adminDB, name: "admin_db_find_and_change_stream"},
                                       {db: adminDB, name: "admin_db_change_stream_only"},
                                       {db: adminDB, name: "admin_db_read"},
                                       {db: adminDB, name: "any_db_find_only"},
                                       {db: adminDB, name: "any_db_change_stream_only"},
                                       {db: adminDB, name: "cluster_find_only"},
                                       {db: adminDB, name: "cluster_find_and_change_stream"},
                                       {db: adminDB, name: "cluster_change_stream_only"}]) {
        jsTestLog(`Testing user ${tojson(userWithoutPrivileges)} cannot open a change stream` +
                  ` on the whole database`);
        const db = userWithoutPrivileges.db;
        assert(db.auth(userWithoutPrivileges.name, password));

        assert.commandFailedWithCode(
            coll.getDB().runCommand({aggregate: 1, pipeline: [{$changeStream: {}}], cursor: {}}),
            ErrorCodes.Unauthorized);

        db.logout();
    }

    // Test that a user with the required privileges can open a change stream.
    for (let userWithPrivileges of [{db: db, name: "db_find_and_change_stream"},
                                    {db: db, name: "db_read"},
                                    {db: adminDB, name: "any_db_find_and_change_stream"}]) {
        jsTestLog(`Testing user ${tojson(userWithPrivileges)} _can_ open a change stream on` +
                  ` the whole database`);
        const db = userWithPrivileges.db;
        assert(db.auth(userWithPrivileges.name, password));

        assert.doesNotThrow(() => coll.getDB().watch());

        db.logout();
    }
}());

(function testPrivilegesForWholeCluster() {
    // Test that users without the required privileges cannot open a change stream. A user needs
    // both the 'find' and 'changeStream' action on _any_ resource. Note in particular that the
    // whole-cluster privileges (specified with {cluster: true}) is not enough to open a change
    // stream on the whole cluster.
    for (let userWithoutPrivileges of [{db: db, name: "find_only"},
                                       {db: db, name: "change_stream_only"},
                                       {db: db, name: "find_and_change_stream"},
                                       {db: db, name: "write"},
                                       {db: db, name: "db_find_only"},
                                       {db: db, name: "db_find_and_change_stream"},
                                       {db: db, name: "db_change_stream_only"},
                                       {db: db, name: "db_read"},
                                       {db: db, name: "db_write"},
                                       {db: adminDB, name: "admin_db_find_only"},
                                       {db: adminDB, name: "admin_db_find_and_change_stream"},
                                       {db: adminDB, name: "admin_db_change_stream_only"},
                                       {db: adminDB, name: "admin_db_read"},
                                       {db: adminDB, name: "any_db_find_only"},
                                       {db: adminDB, name: "any_db_change_stream_only"},
                                       {db: adminDB, name: "cluster_find_only"},
                                       {db: adminDB, name: "cluster_change_stream_only"},
                                       {db: adminDB, name: "cluster_find_and_change_stream"}]) {
        jsTestLog(`Testing user ${tojson(userWithoutPrivileges)} cannot open a change stream` +
                  ` on the whole cluster`);
        const db = userWithoutPrivileges.db;
        assert(db.auth(userWithoutPrivileges.name, password));

        assert.commandFailedWithCode(adminDB.runCommand({
            aggregate: 1,
            pipeline: [{$changeStream: {allChangesForCluster: true}}],
            cursor: {}
        }),
                                     ErrorCodes.Unauthorized);

        db.logout();
    }

    // Test that a user with the required privileges can open a change stream.
    for (let userWithPrivileges of [{db: adminDB, name: "any_db_find_and_change_stream"}]) {
        jsTestLog(`Testing user ${tojson(userWithPrivileges)} _can_ open a change stream` +
                  ` on the whole cluster`);
        const db = userWithPrivileges.db;
        assert(db.auth(userWithPrivileges.name, password));

        assert.doesNotThrow(() => db.getMongo().watch());

        db.logout();
    }
}());
rst.stopSet();
}());

/**
 * Tests that a change stream can be resumed from the higher of two tokens on separate shards whose
 * clusterTime is identical, differing only by documentKey, without causing the PBRT sent to mongoS
 * to go back-in-time.
 * @tags: [
 *   requires_journaling,
 *   requires_majority_read_concern,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const st = new ShardingTest({shards: 2, rs: {nodes: 1, setParameter: {writePeriodicNoops: false}}});

const mongosDB = st.s.startSession({causalConsistency: true}).getDatabase(jsTestName());
const mongosColl = mongosDB.test;

// Enable sharding on the test DB and ensure its primary is shard0.
assert.commandWorked(mongosDB.adminCommand({enableSharding: mongosDB.getName()}));
st.ensurePrimaryShard(mongosDB.getName(), st.rs0.getURL());

// Shard on {_id:1}, split at {_id:0}, and move the upper chunk to shard1.
st.shardColl(mongosColl, {_id: 1}, {_id: 0}, {_id: 1}, mongosDB.getName(), true);

// Write one document to each shard.
assert.commandWorked(mongosColl.insert({_id: -10}));
assert.commandWorked(mongosColl.insert({_id: 10}));

// Open a change stream cursor to listen for subsequent events.
let csCursor = mongosColl.watch([], {cursor: {batchSize: 1}});

// Update both documents in the collection, such that the events are likely to have the same
// clusterTime. We update twice to ensure that the PBRT for both shards moves past the first two
// updates.
assert.commandWorked(mongosColl.update({}, {$set: {updated: 1}}, {multi: true}));
assert.commandWorked(mongosColl.update({}, {$set: {updatedAgain: 1}}, {multi: true}));

// Retrieve the first two events and confirm that they are in order with non-descending
// clusterTime. Unfortunately we cannot guarantee that clusterTime will be identical, since it
// is based on each shard's local value and there are operations beyond noop write that can
// bump the oplog timestamp. We expect however that they will be identical for most test runs,
// so there is value in testing.
let clusterTime = null, updateEvent = null;
for (let x = 0; x < 2; ++x) {
    assert.soon(() => csCursor.hasNext());
    updateEvent = csCursor.next();
    clusterTime = (clusterTime || updateEvent.clusterTime);
    assert.gte(updateEvent.clusterTime, clusterTime);
    assert.eq(updateEvent.updateDescription.updatedFields.updated, 1);
}
assert.soon(() => csCursor.hasNext());

// Update both documents again, so that we will have something to observe after resuming.
assert.commandWorked(mongosColl.update({}, {$set: {updatedYetAgain: 1}}, {multi: true}));

// Resume from the second update, and confirm that we only see events starting with the third
// and fourth updates. We use batchSize:1 to induce mongoD to send each individual event to the
// mongoS when resuming, rather than scanning all the way to the most recent point in its oplog.
csCursor = mongosColl.watch([], {resumeAfter: updateEvent._id, cursor: {batchSize: 1}});
clusterTime = updateEvent = null;
for (let x = 0; x < 2; ++x) {
    assert.soon(() => csCursor.hasNext());
    updateEvent = csCursor.next();
    clusterTime = (clusterTime || updateEvent.clusterTime);
    assert.gte(updateEvent.clusterTime, clusterTime);
    assert.eq(updateEvent.updateDescription.updatedFields.updatedAgain, 1);
}
assert.soon(() => csCursor.hasNext());

st.stop();
})();

/**
 * Confirms that resuming from an event which has the same clusterTime but a different UUID than on
 * another shard does not cause the resume attempt to be prematurely rejected. Reproduction script
 * for the bug described in SERVER-40094.
 * @tags: [
 *   requires_sharding,
 *   uses_change_streams,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/fixture_helpers.js");  // For runCommandOnEachPrimary.

// Asserts that the expected operation type and documentKey are found on the change stream
// cursor. Returns the change stream document.
function assertWriteVisible({cursor, opType, docKey}) {
    assert.soon(() => cursor.hasNext());
    const changeDoc = cursor.next();
    assert.eq(opType, changeDoc.operationType, changeDoc);
    assert.eq(docKey, changeDoc.documentKey, changeDoc);
    return changeDoc;
}

// Create a new cluster with 2 shards. Disable periodic no-ops to ensure that we have control
// over the ordering of events across the cluster.
const st = new ShardingTest({
    shards: 2,
    rs: {nodes: 1, setParameter: {writePeriodicNoops: false, periodicNoopIntervalSecs: 1}}
});

// Create two databases. We will place one of these on each shard.
const mongosDB0 = st.s.getDB(`${jsTestName()}_0`);
const mongosDB1 = st.s.getDB(`${jsTestName()}_1`);
const adminDB = st.s.getDB("admin");

// Enable sharding on mongosDB0 and ensure its primary is shard0.
assert.commandWorked(mongosDB0.adminCommand({enableSharding: mongosDB0.getName()}));
st.ensurePrimaryShard(mongosDB0.getName(), st.rs0.getURL());

// Enable sharding on mongosDB1 and ensure its primary is shard1.
assert.commandWorked(mongosDB1.adminCommand({enableSharding: mongosDB1.getName()}));
st.ensurePrimaryShard(mongosDB1.getName(), st.rs1.getURL());

// Open a connection to a different collection on each shard. We use direct connections to
// ensure that the oplog timestamps across the shards overlap.
const coll0 = st.rs0.getPrimary().getCollection(`${mongosDB0.getName()}.test`);
const coll1 = st.rs1.getPrimary().getCollection(`${mongosDB1.getName()}.test`);

// Open a change stream on the test cluster. We will capture events in 'changeList'.
const changeStreamCursor = adminDB.aggregate([{$changeStream: {allChangesForCluster: true}}]);
const changeList = [];

// Insert ten documents on each shard, alternating between the two collections.
for (let i = 0; i < 20; ++i) {
    const coll = (i % 2 ? coll1 : coll0);
    assert.commandWorked(coll.insert({shard: (i % 2)}));
}

// Verify that each shard now has ten total documents present in the associated collection.
assert.eq(st.rs0.getPrimary().getCollection(coll0.getFullName()).count(), 10);
assert.eq(st.rs1.getPrimary().getCollection(coll1.getFullName()).count(), 10);

// Re-enable 'writePeriodicNoops' to ensure that all change stream events are returned.
FixtureHelpers.runCommandOnEachPrimary(
    {db: adminDB, cmdObj: {setParameter: 1, writePeriodicNoops: true}});

// Read the stream of events, capture them in 'changeList', and confirm that all events occurred
// at or later than the clusterTime of the first event. Unfortunately, we cannot guarantee that
// corresponding events occurred at the same clusterTime on both shards; we expect, however,
// that this will be true in the vast majority of runs, and so there is value in testing.
for (let i = 0; i < 20; ++i) {
    assert.soon(() => changeStreamCursor.hasNext());
    changeList.push(changeStreamCursor.next());
}
const clusterTime = changeList[0].clusterTime;
for (let event of changeList) {
    assert.gte(event.clusterTime, clusterTime);
}

// Test that resuming from each event returns the expected set of subsequent documents.
for (let i = 0; i < changeList.length; ++i) {
    const resumeCursor = adminDB.aggregate(
        [{$changeStream: {allChangesForCluster: true, resumeAfter: changeList[i]._id}}]);

    // Confirm that the first event in the resumed stream matches the next event recorded in
    // 'changeList' from the original stream. The order of the events should be stable across
    // resumes from any point.
    for (let x = (i + 1); x < changeList.length; ++x) {
        const expectedChangeDoc = changeList[x];
        assertWriteVisible({
            cursor: resumeCursor,
            opType: expectedChangeDoc.operationType,
            docKey: expectedChangeDoc.documentKey
        });
    }
    resumeCursor.close();
}

st.stop();
})();

/**
 * Confirms that resuming from an event which has the same clusterTime as a transaction on another
 * shard does not cause the resume attempt to be prematurely rejected. Reproduction script for the
 * bug described in SERVER-40094.
 * @tags: [
 *   requires_sharding,
 *   uses_multi_shard_transaction,
 *   uses_transactions,
 * ]
 */
(function() {
"use strict";

// Asserts that the expected operation type and documentKey are found on the change stream
// cursor. Returns the change stream document.
function assertWriteVisible({cursor, opType, docKey}) {
    assert.soon(() => cursor.hasNext());
    const changeDoc = cursor.next();
    assert.eq(opType, changeDoc.operationType, changeDoc);
    assert.eq(docKey, changeDoc.documentKey, changeDoc);
    return changeDoc;
}

// Create a new cluster with 2 shards. Enable 1-second period no-ops to ensure that all relevant
// events eventually become available.
const st = new ShardingTest({
    shards: 2,
    rs: {nodes: 1, setParameter: {writePeriodicNoops: true, periodicNoopIntervalSecs: 1}}
});

const mongosDB = st.s.getDB(jsTestName());
const mongosColl = mongosDB.test;

// Enable sharding on the test DB and ensure its primary is shard0.
assert.commandWorked(mongosDB.adminCommand({enableSharding: mongosDB.getName()}));
st.ensurePrimaryShard(mongosDB.getName(), st.rs0.getURL());

// Shard on {shard:1}, split at {shard:1}, and move the upper chunk to shard1.
st.shardColl(mongosColl, {shard: 1}, {shard: 1}, {shard: 1}, mongosDB.getName(), true);

// Seed each shard with one document.
assert.commandWorked(
    mongosColl.insert([{shard: 0, _id: "initial_doc"}, {shard: 1, _id: "initial doc"}]));

// Start a transaction which will be used to write documents across both shards.
const session = mongosDB.getMongo().startSession();
const sessionDB = session.getDatabase(mongosDB.getName());
const sessionColl = sessionDB[mongosColl.getName()];
session.startTransaction({readConcern: {level: "majority"}});

// Open a change stream on the test collection. We will capture events in 'changeList'.
const changeStreamCursor = mongosColl.watch();
const changeList = [];

// Insert four documents on each shard under the transaction.
assert.commandWorked(
    sessionColl.insert([{shard: 0, _id: "txn1-doc-0"}, {shard: 1, _id: "txn1-doc-1"}]));
assert.commandWorked(
    sessionColl.insert([{shard: 0, _id: "txn1-doc-2"}, {shard: 1, _id: "txn1-doc-3"}]));
assert.commandWorked(
    sessionColl.insert([{shard: 0, _id: "txn1-doc-4"}, {shard: 1, _id: "txn1-doc-5"}]));
assert.commandWorked(
    sessionColl.insert([{shard: 0, _id: "txn1-doc-6"}, {shard: 1, _id: "txn1-doc-7"}]));

// Commit the transaction.
assert.commandWorked(session.commitTransaction_forTesting());

// Read the stream of events, capture them in 'changeList', and confirm that all events occurred
// at or later than the clusterTime of the first event. Unfortunately, we cannot guarantee that
// all events occurred at the same clusterTime on both shards, even in the case where all events
// occur within a single transaction. We expect, however, that this will be true in the vast
// majority of test runs, and so there is value in retaining this test.
for (let i = 0; i < 8; ++i) {
    assert.soon(() => changeStreamCursor.hasNext());
    changeList.push(changeStreamCursor.next());
}
const clusterTime = changeList[0].clusterTime;
for (let event of changeList) {
    assert.gte(event.clusterTime, clusterTime);
}

// Test that resuming from each event returns the expected set of subsequent documents.
for (let i = 0; i < changeList.length; ++i) {
    const resumeCursor = mongosColl.watch([], {startAfter: changeList[i]._id});

    // Confirm that the first event in the resumed stream matches the next event recorded in
    // 'changeList' from the original stream. The order of the events should be stable across
    // resumes from any point.
    for (let x = (i + 1); x < changeList.length; ++x) {
        const expectedChangeDoc = changeList[x];
        assertWriteVisible({
            cursor: resumeCursor,
            opType: expectedChangeDoc.operationType,
            docKey: expectedChangeDoc.documentKey
        });
    }
    assert(!resumeCursor.hasNext(), () => `Unexpected event: ${tojson(resumeCursor.next())}`);
    resumeCursor.close();
}

st.stop();
})();

/**
 * Tests that the cursor.getResumeToken() shell helper behaves as expected, tracking the resume
 * token with each document and returning the postBatchResumeToken as soon as each batch is
 * exhausted.
 * @tags: [
 *   requires_journaling,
 *   requires_majority_read_concern,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

// Create a new single-node replica set, and ensure that it can support $changeStream.
const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const db = rst.getPrimary().getDB(jsTestName());
const collName = "change_stream_shell_helper_resume_token";
const csCollection = assertDropAndRecreateCollection(db, collName);
const otherCollection = assertDropAndRecreateCollection(db, "unrelated_" + collName);

const batchSize = 5;
let docId = 0;

// Test that getResumeToken() returns the postBatchResumeToken when an empty batch is received.
const csCursor = csCollection.watch([], {cursor: {batchSize: batchSize}});
assert(!csCursor.hasNext());
let curResumeToken = csCursor.getResumeToken();
assert.neq(undefined, curResumeToken);

// Test that advancing the oplog time updates the postBatchResumeToken, even with no results.
assert.commandWorked(otherCollection.insert({}));
let prevResumeToken = curResumeToken;
assert.soon(() => {
    assert(!csCursor.hasNext());  // Causes a getMore to be dispatched.
    prevResumeToken = curResumeToken;
    curResumeToken = csCursor.getResumeToken();
    assert.neq(undefined, curResumeToken);
    return bsonWoCompare(curResumeToken, prevResumeToken) > 0;
});

// Insert 9 documents into the collection, followed by a write to the unrelated collection.
for (let i = 0; i < 9; ++i) {
    assert.commandWorked(csCollection.insert({_id: ++docId}));
}
assert.commandWorked(otherCollection.insert({}));

// Retrieve the first batch of events from the cursor.
assert.soon(() => csCursor.hasNext());  // Causes a getMore to be dispatched.

// We have not yet iterated any of the events. Verify that the resume token is unchanged.
assert.docEq(curResumeToken, csCursor.getResumeToken());

// For each event in the first batch, the resume token should match the document's _id.
let currentDoc = null;
while (csCursor.objsLeftInBatch()) {
    currentDoc = csCursor.next();
    prevResumeToken = curResumeToken;
    curResumeToken = csCursor.getResumeToken();
    assert.docEq(curResumeToken, currentDoc._id);
    assert.gt(bsonWoCompare(curResumeToken, prevResumeToken), 0);
}

// Retrieve the second batch of events from the cursor.
assert.soon(() => csCursor.hasNext());  // Causes a getMore to be dispatched.

// We haven't pulled any events out of the cursor yet, so the resumeToken should be unchanged.
assert.docEq(curResumeToken, csCursor.getResumeToken());

// For all but the final event, the resume token should match the document's _id.
while ((currentDoc = csCursor.next()).fullDocument._id < docId) {
    assert.soon(() => csCursor.hasNext());
    prevResumeToken = curResumeToken;
    curResumeToken = csCursor.getResumeToken();
    assert.docEq(curResumeToken, currentDoc._id);
    assert.gt(bsonWoCompare(curResumeToken, prevResumeToken), 0);
}
// When we reach here, 'currentDoc' is the final document in the batch, but we have not yet
// updated the resume token. Assert that this resume token sorts before currentDoc's.
prevResumeToken = curResumeToken;
assert.gt(bsonWoCompare(currentDoc._id, prevResumeToken), 0);

// After we have pulled the final document out of the cursor, the resume token should be the
// postBatchResumeToken rather than the document's _id. Because we inserted an item into the
// unrelated collection to push the oplog past the final event returned by the change stream,
// this will be strictly greater than the final document's _id.
assert.soon(() => {
    curResumeToken = csCursor.getResumeToken();
    assert(!csCursor.hasNext(), () => tojson(csCursor.next()));
    return bsonWoCompare(curResumeToken, currentDoc._id) > 0;
});

rst.stopSet();
}());

// Tests that the update lookup of an unsharded change stream will use the collection-default
// collation, regardless of the collation on the change stream.
//
// @tags: [
//   requires_majority_read_concern,
//   uses_change_streams,
// ]
(function() {
"use strict";

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const db = rst.getPrimary().getDB("test");
const coll = db[jsTestName()];
const caseInsensitive = {
    locale: "en_US",
    strength: 2
};
assert.commandWorked(db.createCollection(coll.getName(), {collation: caseInsensitive}));

// Insert some documents that have similar _ids, but differ by case and diacritics. These _ids
// would all match the collation on the strengthOneChangeStream, but should not be confused
// during the update lookup using the strength 2 collection default collation.
assert.commandWorked(coll.insert({_id: "abc", x: "abc"}));
assert.commandWorked(coll.insert({_id: "abç", x: "ABC"}));
assert.commandWorked(coll.insert({_id: "åbC", x: "AbÇ"}));

const changeStreamDefaultCollation = coll.aggregate(
    [{$changeStream: {fullDocument: "updateLookup"}}, {$match: {"fullDocument.x": "abc"}}],
    {collation: caseInsensitive});

// Strength one will consider "ç" equal to "c" and "C".
const strengthOneCollation = {
    locale: "en_US",
    strength: 1
};
const strengthOneChangeStream = coll.aggregate(
    [{$changeStream: {fullDocument: "updateLookup"}}, {$match: {"fullDocument.x": "abc"}}],
    {collation: strengthOneCollation});

assert.commandWorked(coll.update({_id: "abc"}, {$set: {updated: true}}));

// Track the number of _id index usages to prove that the update lookup uses the _id index (and
// therefore is using the correct collation for the lookup).
function numIdIndexUsages() {
    return coll.aggregate([{$indexStats: {}}, {$match: {name: "_id_"}}]).toArray()[0].accesses.ops;
}
const idIndexUsagesBeforeIteration = numIdIndexUsages();

// Both cursors should produce a document describing this update, since the "x" value of the
// first document will match both filters.
assert.soon(() => changeStreamDefaultCollation.hasNext());
assert.docEq(changeStreamDefaultCollation.next().fullDocument,
             {_id: "abc", x: "abc", updated: true});
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 1);
assert.docEq(strengthOneChangeStream.next().fullDocument, {_id: "abc", x: "abc", updated: true});
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 2);

assert.commandWorked(coll.update({_id: "abç"}, {$set: {updated: true}}));
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 3);

// Again, both cursors should produce a document describing this update.
assert.soon(() => changeStreamDefaultCollation.hasNext());
assert.docEq(changeStreamDefaultCollation.next().fullDocument,
             {_id: "abç", x: "ABC", updated: true});
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 4);
assert.docEq(strengthOneChangeStream.next().fullDocument, {_id: "abç", x: "ABC", updated: true});
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 5);

assert.commandWorked(coll.update({_id: "åbC"}, {$set: {updated: true}}));
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 6);

// Both $changeStream stages will see this update and both will look up the full document using
// the foreign collection's default collation. However, the changeStreamDefaultCollation's
// subsequent $match stage will reject the document because it does not consider "AbÇ" equal to
// "abc". Only the strengthOneChangeStream will output the final document.
assert.soon(() => strengthOneChangeStream.hasNext());
assert.docEq(strengthOneChangeStream.next().fullDocument, {_id: "åbC", x: "AbÇ", updated: true});
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 7);
assert(!changeStreamDefaultCollation.hasNext());
assert.eq(numIdIndexUsages(), idIndexUsagesBeforeIteration + 8);

changeStreamDefaultCollation.close();
strengthOneChangeStream.close();
rst.stopSet();
}());

/**
 * Checks that the oplog cap maintainer thread is started and the oplog stones calculation is
 * performed under normal startup circumstances. Both of these operations should not be done when
 * starting up with any of the following modes:
 *     - readonly
 *     - repair
 *     - recoverFromOplogAsStandalone
 *
 * @tags: [requires_replication, requires_persistence]
 */
(function() {
"use strict";

// Verify that the oplog cap maintainer thread is running under normal circumstances.
jsTestLog("Testing single node replica set mode");
const rst = new ReplSetTest({nodes: 1, nodeOptions: {setParameter: {logLevel: 1}}});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
checkLog.containsJson(primary, 5295000);  // OplogCapMaintainerThread started.
checkLog.containsJson(primary, 22382);    // Oplog stones calculated.

rst.stopSet(/*signal=*/null, /*forRestart=*/true);

// A subset of startup options prevent the oplog cap maintainer thread from being started. These
// startup options are currently limited to readOnly, recoverFromOplogAsStandalone and repair.
function verifyOplogCapMaintainerThreadNotStarted(log) {
    const threadRegex = new RegExp("\"id\":5295000");
    const oplogStonesRegex = new RegExp("\"id\":22382");

    assert(!threadRegex.test(log));
    assert(!oplogStonesRegex.test(log));
}

jsTestLog("Testing readOnly mode");
clearRawMongoProgramOutput();
let conn = MongoRunner.runMongod({
    dbpath: primary.dbpath,
    noCleanData: true,
    queryableBackupMode: "",  // readOnly
    setParameter: {logLevel: 1},
});
assert(conn);
MongoRunner.stopMongod(conn);
verifyOplogCapMaintainerThreadNotStarted(rawMongoProgramOutput());

jsTestLog("Testing recoverFromOplogAsStandalone mode");
clearRawMongoProgramOutput();
conn = MongoRunner.runMongod({
    dbpath: primary.dbpath,
    noCleanData: true,
    setParameter: {recoverFromOplogAsStandalone: true, logLevel: 1},
});
assert(conn);
MongoRunner.stopMongod(conn);
verifyOplogCapMaintainerThreadNotStarted(rawMongoProgramOutput());

jsTestLog("Testing repair mode");
clearRawMongoProgramOutput();
conn = MongoRunner.runMongod({
    dbpath: primary.dbpath,
    noCleanData: true,
    repair: "",
    setParameter: {logLevel: 1},
});
assert(!conn);
verifyOplogCapMaintainerThreadNotStarted(rawMongoProgramOutput());
}());

/**
 * Confirms that a parent operation correctly inherits 'numYields' from each of its child operations
 * as the latter are popped off the CurOp stack.
 */
(function() {
"use strict";

load("jstests/libs/curop_helpers.js");  // For waitForCurOpByFailPoint().

// Start a single mongoD using MongoRunner.
const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");

// Create the test DB and collection.
const testDB = conn.getDB("currentop_yield");
const adminDB = conn.getDB("admin");
const testColl = testDB.test;

// Executes a bulk remove using the specified 'docsToRemove' array, captures the 'numYields'
// metrics from each child op, and confirms that the parent op's 'numYields' total is equivalent
// to the sum of the child ops.
function runYieldTest(docsToRemove) {
    // Sets parameters such that all operations will yield & the operation hangs on the server
    // when we need to test.
    assert.commandWorked(
        testDB.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 1}));
    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: "hangBeforeChildRemoveOpFinishes", mode: "alwaysOn"}));
    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: "hangAfterAllChildRemoveOpsArePopped", mode: "alwaysOn"}));

    // Starts parallel shell to run the command that will hang.
    const awaitShell = startParallelShell(`{
            const testDB = db.getSiblingDB("currentop_yield");
            const bulkRemove = testDB.test.initializeOrderedBulkOp();
            for(let doc of ${tojsononeline(docsToRemove)}) {
                bulkRemove.find(doc).removeOne();
            }
            bulkRemove.execute();
        }`,
                                              testDB.getMongo().port);

    let childOpId = null;
    let childYields = 0;

    // Get child operations and sum yields. Each child op encounters two failpoints while
    // running: 'hangBeforeChildRemoveOpFinishes' followed by 'hangBeforeChildRemoveOpIsPopped'.
    // We use these two failpoints as an 'airlock', hanging at the first while we enable the
    // second, then hanging at the second while we enable the first, to ensure that each child
    // op is caught and their individual 'numYields' recorded.
    for (let childCount = 0; childCount < docsToRemove.length; childCount++) {
        // Wait for the child op to hit the first of two failpoints.
        let childCurOp = waitForCurOpByFailPoint(
            testDB, testColl.getFullName(), "hangBeforeChildRemoveOpFinishes")[0];

        // Add the child's yield count to the running total, and record the opid.
        assert(childOpId === null || childOpId === childCurOp.opid);
        assert.gt(childCurOp.numYields, 0);
        childYields += childCurOp.numYields;
        childOpId = childCurOp.opid;

        // Enable the subsequent 'hangBeforeChildRemoveOpIsPopped' failpoint, just after the
        // child op finishes but before it is popped from the stack.
        assert.commandWorked(testDB.adminCommand(
            {configureFailPoint: "hangBeforeChildRemoveOpIsPopped", mode: "alwaysOn"}));

        // Let the operation proceed to the 'hangBeforeChildRemoveOpIsPopped' failpoint.
        assert.commandWorked(testDB.adminCommand(
            {configureFailPoint: "hangBeforeChildRemoveOpFinishes", mode: "off"}));
        waitForCurOpByFailPoint(testDB, testColl.getFullName(), "hangBeforeChildRemoveOpIsPopped");

        // If this is not the final child op, re-enable the 'hangBeforeChildRemoveOpFinishes'
        // failpoint from earlier so that we don't miss the next child.
        if (childCount + 1 < docsToRemove.length) {
            assert.commandWorked(testDB.adminCommand(
                {configureFailPoint: "hangBeforeChildRemoveOpFinishes", mode: "alwaysOn"}));
        }

        // Finally, allow the operation to continue.
        assert.commandWorked(testDB.adminCommand(
            {configureFailPoint: "hangBeforeChildRemoveOpIsPopped", mode: "off"}));
    }

    // Wait for the operation to hit the 'hangAfterAllChildRemoveOpsArePopped' failpoint, then
    // take the total number of yields recorded by the parent op.
    const parentCurOp = waitForCurOpByFailPointNoNS(
        testDB, "hangAfterAllChildRemoveOpsArePopped", {opid: childOpId})[0];

    // Verify that the parent's yield count equals the sum of the child ops' yields.
    assert.eq(parentCurOp.numYields, childYields);
    assert.eq(parentCurOp.opid, childOpId);

    // Allow the parent operation to complete.
    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: "hangAfterAllChildRemoveOpsArePopped", mode: "off"}));

    // Wait for the parallel shell to complete.
    awaitShell();
}

// Test that a parent remove op inherits the sum of its children's yields for a single remove.
assert.commandWorked(testDB.test.insert({a: 2}));
runYieldTest([{a: 2}]);

// Test that a parent remove op inherits the sum of its children's yields for multiple removes.
const docsToTest = [{a: 1}, {a: 2}, {a: 3}, {a: 4}, {a: 5}];
assert.commandWorked(testDB.test.insert(docsToTest));
runYieldTest(docsToTest);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that mongos reacts properly to a client disconnecting while the logical time is being
 * signed as a part of appending fields to a command response.
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

const st = new ShardingTest({mongos: 1, shards: 0, keyFile: "jstests/libs/key1"});

assert.commandFailedWithCode(st.s.adminCommand({
    configureFailPoint: "throwClientDisconnectInSignLogicalTimeForExternalClients",
    mode: {times: 1}
}),
                             ErrorCodes.ClientDisconnect);

st.stop();
})();
/**
 * Test that verifies client metadata is logged into log file on new connections.
 * @tags: [
 *   requires_sharding,
 * ]
 */
load("jstests/libs/logv2_helpers.js");

(function() {
'use strict';

let checkLog = function(conn) {
    let coll = conn.getCollection("test.foo");
    assert.commandWorked(coll.insert({_id: 1}));

    print(`Checking ${conn.fullOptions.logFile} for client metadata message`);
    let log = cat(conn.fullOptions.logFile);

    let predicate = null;
    if (isJsonLog(conn)) {
        predicate =
            /"id":51800,.*"msg":"client metadata","attr":.*"doc":{"application":{"name":".*"},"driver":{"name":".*","version":".*"},"os":{"type":".*","name":".*","architecture":".*","version":".*"}}/;
    } else {
        predicate =
            /received client metadata from .*: {"application":{"name":".*"},"driver":{"name":".*","version":".*"},"os":{"type":".*","name":".*","architecture":".*","version":".*"}}/;
    }

    assert(predicate.test(log),
           "'client metadata' log line missing in log file!\n" +
               "Log file contents: " + conn.fullOptions.logFile +
               "\n************************************************************\n" + log +
               "\n************************************************************");
};

// Test MongoD
let testMongoD = function() {
    let conn = MongoRunner.runMongod({useLogFiles: true});
    assert.neq(null, conn, 'mongod was unable to start up');

    checkLog(conn);

    MongoRunner.stopMongod(conn);
};

// Test MongoS
let testMongoS = function() {
    let options = {
        mongosOptions: {useLogFiles: true},
    };

    let st = new ShardingTest({shards: 1, mongos: 1, other: options});

    checkLog(st.s0);

    // Validate db.currentOp() contains mongos information
    let curOp = st.s0.adminCommand({currentOp: 1});
    print(tojson(curOp));

    var inprogSample = null;
    for (let inprog of curOp.inprog) {
        if (inprog.hasOwnProperty("clientMetadata") &&
            inprog.clientMetadata.hasOwnProperty("mongos")) {
            inprogSample = inprog;
            break;
        }
    }

    assert.neq(inprogSample.clientMetadata.mongos.host, "unknown");
    assert.neq(inprogSample.clientMetadata.mongos.client, "unknown");
    assert.neq(inprogSample.clientMetadata.mongos.version, "unknown");

    st.stop();
};

testMongoD();
testMongoS();
})();

/**
 * Test that verifies client metadata is logged as part of slow query logging in MongoD.
 */
load("jstests/libs/logv2_helpers.js");

(function() {
'use strict';

let conn = MongoRunner.runMongod({useLogFiles: true});
assert.neq(null, conn, 'mongod was unable to start up');

let coll = conn.getCollection("test.foo");
assert.commandWorked(coll.insert({_id: 1}));

// Do a really slow query beyond the 100ms threshold
let count = coll.count({
    $where: function() {
        sleep(1000);
        return true;
    }
});
assert.eq(count, 1, "expected 1 document");

print(`Checking ${conn.fullOptions.logFile} for client metadata message`);
let log = cat(conn.fullOptions.logFile);
let predicate = null;
if (isJsonLog(conn)) {
    predicate =
        /Slow query.*test.foo.*"appName":"MongoDB Shell".*"command":{"count":"foo","query":{"\$where":{"\$code":"function\(\)/;
} else {
    predicate =
        /COMMAND .* command test.foo appName: "MongoDB Shell" command: count { count: "foo", query: { \$where: function\(\)/;
}

// Dump the log line by line to avoid log truncation
for (var a of log.split("\n")) {
    print("LOG_FILE_ENTRY: " + a);
}

assert(predicate.test(log),
       "'Slow query' log line missing in mongod log file!\n" +
           "Log file contents: " + conn.fullOptions.logFile);
MongoRunner.stopMongod(conn);
})();

/**
 * Test that verifies client metadata is logged as part of slow query logging in MongoD in a replica
 * set.
 * @tags: [
 *   requires_replication,
 * ]
 */
load("jstests/libs/logv2_helpers.js");

(function() {
'use strict';

const numNodes = 2;
const rst = new ReplSetTest({nodes: numNodes});

rst.startSet();
rst.initiate();
rst.awaitReplication();

// Build a new connection based on the replica set URL
var conn = new Mongo(rst.getURL());

let coll = conn.getCollection("test.foo");
assert.commandWorked(coll.insert({_id: 1}, {writeConcern: {w: numNodes, wtimeout: 5000}}));

const predicate =
    /Slow query.*test.foo.*"appName":"MongoDB Shell".*"command":{"find":"foo","filter":{"\$where":{"\$code":"function\(\)/;

// Do a really slow query beyond the 100ms threshold
let count = coll.find({
                    $where: function() {
                        sleep(1000);
                        return true;
                    }
                })
                .readPref('primary')
                .toArray();
assert.eq(count.length, 1, "expected 1 document");
assert(checkLog.checkContainsOnce(rst.getPrimary(), predicate));

// Do a really slow query beyond the 100ms threshold
count = coll.find({
                $where: function() {
                    sleep(1000);
                    return true;
                }
            })
            .readPref('secondary')
            .toArray();
assert.eq(count.length, 1, "expected 1 document");
assert(checkLog.checkContainsOnce(rst.getSecondary(), predicate));

rst.stopSet();
})();

// SERVER-30665 Ensure that a non-empty collMod with a nonexistent UUID is not applied
// in applyOps.

(function() {
"use strict";
const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up with empty options");

let dbCollModName = "db_coll_mod";
const dbCollMod = conn.getDB(dbCollModName);
dbCollMod.dropDatabase();
let collName = "collModTest";
let coll = dbCollMod[collName];

// Generate a random UUID that is distinct from collModTest's UUID.
const randomUUID = UUID();
assert.neq(randomUUID, coll.uuid);

// Perform a collMod to initialize validationLevel to "off".
assert.commandWorked(dbCollMod.createCollection(collName));
let cmd = {"collMod": collName, "validationLevel": "off"};
let res = dbCollMod.runCommand(cmd);
assert.commandWorked(res, 'could not run ' + tojson(cmd));
let collectionInfosOriginal = dbCollMod.getCollectionInfos()[0];
assert.eq(collectionInfosOriginal.options.validationLevel, "off");

// Perform an applyOps command with a nonexistent UUID and the same name as an existing
// collection. applyOps should succeed because of idempotency but a NamespaceNotFound
// uassert should be thrown during collMod application.
let collModApplyOpsEntry = {
    "v": 2,
    "op": "c",
    "ns": dbCollModName + ".$cmd",
    "ui": randomUUID,
    "o2": {"collectionOptions_old": {"uuid": randomUUID}},
    "o": {"collMod": collName, "validationLevel": "moderate"}
};
assert.commandWorked(dbCollMod.adminCommand({"applyOps": [collModApplyOpsEntry]}));

// Ensure the collection options of the existing collection were not affected.
assert.eq(dbCollMod.getCollectionInfos()[0].name, collName);
assert.eq(dbCollMod.getCollectionInfos()[0].options.validationLevel, "off");
MongoRunner.stopMongod(conn);
}());

/**
 * Tests that the expired data is deleted correctly after an index is converted to TTL using the
 * collMod command.
 *
 * @tags: [
 *   assumes_no_implicit_collection_creation_after_drop,
 *   multiversion_incompatible,
 *   requires_ttl_index,
 * ]
 */
(function() {
"use strict";

// Runs TTL monitor constantly to speed up this test.
const conn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
const coll = testDB.getCollection('coll');
assert.commandWorked(testDB.createCollection(coll.getName()));
coll.createIndex({a: 1});
const expireAfterSeconds = 5;

const waitForTTL = () => {
    // The 'ttl.passes' metric is incremented when the TTL monitor starts processing the indexes, so
    // we wait for it to be incremented twice to know that the TTL monitor finished processing the
    // indexes at least once.
    const ttlPasses = testDB.serverStatus().metrics.ttl.passes;
    assert.soon(function() {
        return testDB.serverStatus().metrics.ttl.passes > ttlPasses + 1;
    });
};

// Inserts a measurement older than the TTL expiry. The data should be found.
const expired = new Date((new Date()).getTime() - (1000 * 10));
assert.commandWorked(coll.insert({a: expired}));

waitForTTL();
assert.eq(1, coll.find().itcount());

// Converts to a TTL index and checks the data is deleted.
assert.commandWorked(testDB.runCommand({
    collMod: coll.getName(),
    index: {
        keyPattern: {a: 1},
        expireAfterSeconds: expireAfterSeconds,
    }
}));

waitForTTL();
assert.eq(0, coll.find().itcount());

MongoRunner.stopMongod(conn);
})();

// validate command line parameter parsing

var baseName = "jstests_slowNightly_command_line_parsing";

// test notablescan
var m = MongoRunner.runMongod({notablescan: ""});
m.getDB(baseName).getCollection(baseName).save({a: 1});
assert.throws(function() {
    m.getDB(baseName).getCollection(baseName).find({a: 1}).toArray();
});
MongoRunner.stopMongod(m);

// test config file
var m2 = MongoRunner.runMongod({config: "jstests/libs/testconfig"});

var m2expected = {
    "parsed": {
        "config": "jstests/libs/testconfig",
        "storage": {"dbPath": m2.dbpath},
        "net": {"bindIp": "0.0.0.0", "port": m2.port},
        "help": false,
        "version": false,
        "sysinfo": false
    }
};
var m2result = m2.getDB("admin").runCommand("getCmdLineOpts");
MongoRunner.stopMongod(m2);

// remove variables that depend on the way the test is started.
delete m2result.parsed.net.transportLayer;
delete m2result.parsed.setParameter;
delete m2result.parsed.storage.engine;
delete m2result.parsed.storage.inMemory;
delete m2result.parsed.storage.journal;
delete m2result.parsed.storage.rocksdb;
delete m2result.parsed.storage.wiredTiger;
delete m2result.parsed.replication;  // Removes enableMajorityReadConcern setting.
assert.docEq(m2expected.parsed, m2result.parsed);

// test JSON config file
var m3 = MongoRunner.runMongod({config: "jstests/libs/testconfig"});

var m3expected = {
    "parsed": {
        "config": "jstests/libs/testconfig",
        "storage": {"dbPath": m3.dbpath},
        "net": {"bindIp": "0.0.0.0", "port": m3.port},
        "help": false,
        "version": false,
        "sysinfo": false
    }
};
var m3result = m3.getDB("admin").runCommand("getCmdLineOpts");
MongoRunner.stopMongod(m3);

// remove variables that depend on the way the test is started.
delete m3result.parsed.net.transportLayer;
delete m3result.parsed.setParameter;
delete m3result.parsed.storage.engine;
delete m3result.parsed.storage.inMemory;
delete m3result.parsed.storage.journal;
delete m3result.parsed.storage.rocksdb;
delete m3result.parsed.storage.wiredTiger;
delete m3result.parsed.replication;  // Removes enableMajorityReadConcern setting.
assert.docEq(m3expected.parsed, m3result.parsed);

// Tests that commands properly handle their underlying plan executor failing or being killed.
(function() {
'use strict';
const dbpath = MongoRunner.dataPath + jsTest.name();
resetDbpath(dbpath);
const mongod = MongoRunner.runMongod({dbpath: dbpath});
const db = mongod.getDB("test");
const collName = jsTest.name();
const coll = db.getCollection(collName);

// How many works it takes to yield.
const yieldIterations = 2;
assert.commandWorked(
    db.adminCommand({setParameter: 1, internalQueryExecYieldIterations: yieldIterations}));
const nDocs = yieldIterations + 2;

/**
 * Asserts that 'commandResult' indicates a command failure, and returns the error message.
 */
function assertContainsErrorMessage(commandResult) {
    assert(commandResult.ok === 0 ||
               (commandResult.ok === 1 && commandResult.writeErrors !== undefined),
           'expected command to fail: ' + tojson(commandResult));
    if (commandResult.ok === 0) {
        return commandResult.errmsg;
    } else {
        return commandResult.writeErrors[0].errmsg;
    }
}

function setupCollection() {
    coll.drop();
    let bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < nDocs; i++) {
        bulk.insert({_id: i, a: i});
    }
    assert.commandWorked(bulk.execute());
    assert.commandWorked(coll.createIndex({a: 1}));
}

/**
 * Asserts that the command given by 'cmdObj' will propagate a message from a PlanExecutor
 * failure back to the user.
 */
function assertCommandPropogatesPlanExecutorFailure(cmdObj) {
    // Make sure the command propagates failure messages.
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "planExecutorAlwaysFails", mode: "alwaysOn"}));
    let res = db.runCommand(cmdObj);
    let errorMessage = assertContainsErrorMessage(res);
    assert.neq(errorMessage.indexOf("planExecutorAlwaysFails"),
               -1,
               "Expected error message to include 'planExecutorAlwaysFails', instead found: " +
                   errorMessage);
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "planExecutorAlwaysFails", mode: "off"}));
}

/**
 * Asserts that the command properly handles failure scenarios while using its PlanExecutor.
 * Asserts that the appropriate error message is propagated if the is a failure during
 * execution, or if the plan was killed during execution. If 'options.commandYields' is false,
 * asserts that the PlanExecutor cannot be killed, and succeeds when run concurrently with any
 * of 'invalidatingCommands'.
 *
 * @param {Object} cmdObj - The command to run.
 * @param {Boolean} [options.commandYields=true] - Whether or not this command can yield during
 *   execution.
 * @param {Object} [options.curOpFilter] - The query to use to find this operation in the
 *   currentOp output. The default checks that all fields of cmdObj are in the curOp command.
 * @param {Function} [options.customSetup=undefined] - A callback to do any necessary setup
 *   before the command can be run, like adding a geospatial index before a geoNear command.
 * @param {Boolean} [options.usesIndex] - True if this command should scan index {a: 1}, and
 *   therefore should be killed if this index is dropped.
 */
function assertCommandPropogatesPlanExecutorKillReason(cmdObj, options) {
    options = options || {};

    var curOpFilter = options.curOpFilter;
    if (!curOpFilter) {
        curOpFilter = {};
        for (var arg in cmdObj) {
            curOpFilter['command.' + arg] = {$eq: cmdObj[arg]};
        }
    }

    // These are commands that will cause all running PlanExecutors to be invalidated, and the
    // error messages that should be propagated when that happens.
    const invalidatingCommands = [
        {command: {dropDatabase: 1}, message: 'collection dropped'},
        {command: {drop: collName}, message: 'collection dropped'},
    ];

    if (options.usesIndex) {
        invalidatingCommands.push(
            {command: {dropIndexes: collName, index: {a: 1}}, message: 'index \'a_1\' dropped'});
    }

    for (let invalidatingCommand of invalidatingCommands) {
        setupCollection();
        if (options.customSetup !== undefined) {
            options.customSetup();
        }

        // Enable a failpoint that causes PlanExecutors to hang during execution.
        assert.commandWorked(
            db.adminCommand({configureFailPoint: "setYieldAllLocksHang", mode: "alwaysOn"}));

        const canYield = options.commandYields === undefined || options.commandYields;
        // Start a parallel shell to run the command. This should hang until we unset the
        // failpoint.
        let awaitCmdFailure = startParallelShell(`
let assertContainsErrorMessage = ${ assertContainsErrorMessage.toString() };
let res = db.runCommand(${ tojson(cmdObj) });
if (${ canYield }) {
    let errorMessage = assertContainsErrorMessage(res);
    assert.neq(errorMessage.indexOf(${ tojson(invalidatingCommand.message) }),
               -1,
                "Expected error message to include '" +
                    ${ tojson(invalidatingCommand.message) } +
                    "', instead found: " + errorMessage);
} else {
    assert.commandWorked(
        res,
        'expected non-yielding command to succeed: ' + tojson(${ tojson(cmdObj) })
    );
}
`,
                                                     mongod.port);

        // Wait until we can see the command running.
        assert.soon(
            function() {
                if (!canYield) {
                    // The command won't yield, so we won't necessarily see it in currentOp.
                    return true;
                }
                return db.currentOp({
                             $and: [
                                 {
                                     ns: coll.getFullName(),
                                     numYields: {$gt: 0},
                                 },
                                 curOpFilter,
                             ]
                         }).inprog.length > 0;
            },
            function() {
                return 'expected to see command yielded in currentOp output. Command: ' +
                    tojson(cmdObj) + '\n, currentOp output: ' + tojson(db.currentOp().inprog);
            });

        // Run the command that invalidates the PlanExecutor, then allow the PlanExecutor to
        // proceed.
        jsTestLog("Running invalidating command: " + tojson(invalidatingCommand.command));
        assert.commandWorked(db.runCommand(invalidatingCommand.command));
        assert.commandWorked(
            db.adminCommand({configureFailPoint: "setYieldAllLocksHang", mode: "off"}));
        awaitCmdFailure();
    }

    setupCollection();
    if (options.customSetup !== undefined) {
        options.customSetup();
    }
    assertCommandPropogatesPlanExecutorFailure(cmdObj);
}

// Disable aggregation's batching behavior, since that can prevent the PlanExecutor from being
// active during the command that would have caused it to be killed.
assert.commandWorked(
    db.adminCommand({setParameter: 1, internalDocumentSourceCursorBatchSizeBytes: 1}));
assertCommandPropogatesPlanExecutorKillReason({aggregate: collName, pipeline: [], cursor: {}});
assertCommandPropogatesPlanExecutorKillReason(
    {aggregate: collName, pipeline: [{$match: {a: {$gte: 0}}}], cursor: {}}, {usesIndex: true});

assertCommandPropogatesPlanExecutorKillReason({dataSize: coll.getFullName()},
                                              {commandYields: false});

assertCommandPropogatesPlanExecutorKillReason("dbHash", {commandYields: false});

assertCommandPropogatesPlanExecutorKillReason({count: collName, query: {a: {$gte: 0}}},
                                              {usesIndex: true});

assertCommandPropogatesPlanExecutorKillReason(
    {distinct: collName, key: "_id", query: {a: {$gte: 0}}}, {usesIndex: true});

assertCommandPropogatesPlanExecutorKillReason(
    {findAndModify: collName, query: {fakeField: {$gt: 0}}, update: {$inc: {a: 1}}});

assertCommandPropogatesPlanExecutorKillReason(
    {
        aggregate: collName,
        cursor: {},
        pipeline: [{
            $geoNear:
                {near: {type: "Point", coordinates: [0, 0]}, spherical: true, distanceField: "dis"}
        }]
    },
    {
        customSetup: function() {
            assert.commandWorked(coll.createIndex({geoField: "2dsphere"}));
        }
    });

assertCommandPropogatesPlanExecutorKillReason({find: coll.getName(), filter: {}});
assertCommandPropogatesPlanExecutorKillReason({find: coll.getName(), filter: {a: {$gte: 0}}},
                                              {usesIndex: true});

assertCommandPropogatesPlanExecutorKillReason(
    {update: coll.getName(), updates: [{q: {a: {$gte: 0}}, u: {$set: {a: 1}}}]},
    {curOpFilter: {op: 'update'}, usesIndex: true});

assertCommandPropogatesPlanExecutorKillReason(
    {delete: coll.getName(), deletes: [{q: {a: {$gte: 0}}, limit: 0}]},
    {curOpFilter: {op: 'remove'}, usesIndex: true});
MongoRunner.stopMongod(mongod);
})();

// Tests that an error encountered during PlanExecutor execution will be propagated back to the user
// with the original error code. This is important for retryable errors like
// 'InterruptedDueToReplStateChange',
// and also to ensure that the error is not swallowed and the diagnostic info is not lost.
(function() {
"use strict";

const kPlanExecAlwaysFailsCode = 4382101;

const mongod = MongoRunner.runMongod({});
assert.neq(mongod, null, "mongod failed to start up");
const db = mongod.getDB("test");
const coll = db.commands_preserve_exec_error_code;
coll.drop();

assert.commandWorked(coll.insert([{_id: 0}, {_id: 1}, {_id: 2}]));
assert.commandWorked(coll.createIndex({geo: "2d"}));

assert.commandWorked(
    db.adminCommand({configureFailPoint: "planExecutorAlwaysFails", mode: "alwaysOn"}));

function assertFailsWithExpectedError(fn) {
    const error = assert.throws(fn);
    assert.eq(error.code, kPlanExecAlwaysFailsCode, tojson(error));
    assert.neq(-1,
               error.message.indexOf("planExecutorAlwaysFails"),
               "Expected error message to be preserved");
}
function assertCmdFailsWithExpectedError(cmd) {
    const res =
        assert.commandFailedWithCode(db.runCommand(cmd), kPlanExecAlwaysFailsCode, tojson(cmd));
    assert.neq(-1,
               res.errmsg.indexOf("planExecutorAlwaysFails"),
               "Expected error message to be preserved");
}

assertFailsWithExpectedError(() => coll.find().itcount());
assertFailsWithExpectedError(() => coll.updateOne({_id: 1}, {$set: {x: 2}}));
assertFailsWithExpectedError(() => coll.deleteOne({_id: 1}));
assertFailsWithExpectedError(() => coll.count({_id: 1}));
assertFailsWithExpectedError(() => coll.aggregate([]).itcount());
assertFailsWithExpectedError(
    () => coll.aggregate([{$geoNear: {near: [0, 0], distanceField: "d"}}]).itcount());
assertCmdFailsWithExpectedError({distinct: coll.getName(), key: "_id"});
assertCmdFailsWithExpectedError(
    {findAndModify: coll.getName(), query: {_id: 1}, update: {$set: {x: 2}}});

const cmdRes = db.runCommand({find: coll.getName(), batchSize: 0});
assert.commandWorked(cmdRes);
assertCmdFailsWithExpectedError(
    {getMore: cmdRes.cursor.id, collection: coll.getName(), batchSize: 1});

assert.commandWorked(db.adminCommand({configureFailPoint: "planExecutorAlwaysFails", mode: "off"}));
MongoRunner.stopMongod(mongod);
}());

/**
 * Verify that adding 'comment' field to any command shouldn't cause unexpected failures.
 * @tags: [
 *   requires_capped,
 *   requires_journaling,
 *   requires_persistence,
 *   requires_replication,
 *   requires_sharding,
 *   requires_wiredtiger,
 * ]
 */
(function() {

"use strict";

load("jstests/auth/lib/commands_lib.js");  // Provides an exhaustive list of commands.
load("jstests/libs/fail_point_util.js");   // Helper to enable/disable failpoints easily.

const tests = authCommandsLib.tests;

// The following commands require additional start up configuration and hence need to be skipped.
const denylistedTests =
    ["startRecordingTraffic", "stopRecordingTraffic", "addShardToZone", "removeShardFromZone"];

function runTests(tests, conn, impls) {
    const firstDb = conn.getDB(firstDbName);
    const secondDb = conn.getDB(secondDbName);
    const isMongos = authCommandsLib.isMongos(conn);
    for (const test of tests) {
        if (!denylistedTests.includes(test.testname)) {
            authCommandsLib.runOneTest(conn, test, impls, isMongos);
        }
    }
}

const impls = {
    runOneTest: function(conn, testObj) {
        // Some tests requires mongot, however, setting this failpoint will make search queries to
        // return EOF, that way all the hassle of setting it up can be avoided.
        let disableSearchFailpoint;
        if (testObj.disableSearch) {
            disableSearchFailpoint = configureFailPoint(conn.rs0 ? conn.rs0.getPrimary() : conn,
                                                        'searchReturnEofImmediately');
        }
        const testCase = testObj.testcases[0];

        const runOnDb = conn.getDB(testCase.runOnDb);
        const state = testObj.setup && testObj.setup(runOnDb);

        const command = (typeof (testObj.command) === "function")
            ? testObj.command(state, testCase.commandArgs)
            : testObj.command;
        command['comment'] = {comment: true};
        const res = runOnDb.runCommand(command);
        assert(res.ok == 1 || testCase.expectFail || res.code == ErrorCodes.CommandNotSupported,
               tojson(res));

        if (testObj.teardown) {
            testObj.teardown(runOnDb, res);
        }

        if (disableSearchFailpoint) {
            disableSearchFailpoint.off();
        }
    }
};

let conn = MongoRunner.runMongod();

// Test with standalone mongod.
runTests(tests, conn, impls);

MongoRunner.stopMongod(conn);

// Test with a sharded cluster. Some tests require the first shard's name acquired from the
// auth commands library to be up-to-date in order to set up correctly.
conn = new ShardingTest({shards: 1, mongos: 2});
shard0name = conn.shard0.shardName;
runTests(tests, conn, impls);

conn.stop();
})();

/**
 * Tests that the commit quorum can be changed during a two-phase index build.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
load("jstests/noPassthrough/libs/index_build.js");

const replSet = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
        },
    ]
});

// Allow the createIndexes command to use the index builds coordinator in single-phase mode.
replSet.startSet();
replSet.initiate();

const primary = replSet.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.twoPhaseIndexBuild;

const bulk = coll.initializeUnorderedBulkOp();
const numDocs = 1000;
for (let i = 0; i < numDocs; i++) {
    bulk.insert({a: i, b: i});
}
assert.commandWorked(bulk.execute());

const collName = "createIndexes";
// This test depends on using the IndexBuildsCoordinator to build this index, which as of
// SERVER-44405, will not occur in this test unless the collection is created beforehand.
assert.commandWorked(testDB.runCommand({create: collName}));

// Use createIndex(es) to build indexes and check the commit quorum default.
let res = assert.commandWorked(testDB[collName].createIndex({x: 1}));
assert.eq("votingMembers", res.commitQuorum);

res = assert.commandWorked(testDB[collName].createIndex({y: 1}, {}, 1));
assert.eq(1, res.commitQuorum);

// Use createIndex(es) to build indexes and check the commit quorum default.
res = assert.commandWorked(testDB[collName].createIndexes([{i: 1}]));
assert.eq("votingMembers", res.commitQuorum);

res = assert.commandWorked(testDB[collName].createIndexes([{j: 1}], {}, 1));
assert.eq(1, res.commitQuorum);

replSet.awaitReplication();

let awaitShell;
try {
    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: "hangAfterIndexBuildFirstDrain", mode: "alwaysOn"}));

    // Starts parallel shell to run the command that will hang.
    awaitShell = startParallelShell(function() {
        // Use the index builds coordinator for a two-phase index build.
        assert.commandWorked(db.runCommand({
            createIndexes: 'twoPhaseIndexBuild',
            indexes: [{key: {a: 1}, name: 'a_1'}],
            commitQuorum: "majority"
        }));
    }, testDB.getMongo().port);

    checkLog.containsWithCount(
        replSet.getPrimary(), "Index build: waiting for index build to complete", 5);

    // Test setting various commit quorums on the index build in our two node replica set.
    assert.commandFailed(testDB.runCommand(
        {setIndexCommitQuorum: 'twoPhaseIndexBuild', indexNames: ['a_1'], commitQuorum: 3}));
    assert.commandFailed(testDB.runCommand({
        setIndexCommitQuorum: 'twoPhaseIndexBuild',
        indexNames: ['a_1'],
        commitQuorum: "someTag"
    }));
    // setIndexCommitQuorum should fail as it is illegal to disable commit quorum for in-progress
    // index builds with commit quorum enabled.
    assert.commandFailed(testDB.runCommand(
        {setIndexCommitQuorum: 'twoPhaseIndexBuild', indexNames: ['a_1'], commitQuorum: 0}));

    assert.commandWorked(testDB.runCommand(
        {setIndexCommitQuorum: 'twoPhaseIndexBuild', indexNames: ['a_1'], commitQuorum: 2}));
    assert.commandWorked(testDB.runCommand({
        setIndexCommitQuorum: 'twoPhaseIndexBuild',
        indexNames: ['a_1'],
        commitQuorum: "majority"
    }));
} finally {
    assert.commandWorked(
        testDB.adminCommand({configureFailPoint: "hangAfterIndexBuildFirstDrain", mode: "off"}));
}

// Wait for the parallel shell to complete.
awaitShell();

IndexBuildTest.assertIndexes(coll, 2, ["_id_", "a_1"]);

replSet.stopSet();
})();

/**
 * Initial syncing a node with two phase index builds should immediately build all ready indexes
 * from the sync source and only setup the index builder threads for any unfinished index builds
 * grouped by their buildUUID.
 *
 * Previously, an initial syncing node would start and finish the index build when it applied the
 * "commitIndexBuild" oplog entry, but the primary will no longer send that oplog entry until the
 * commit quorum is satisfied, which may depend on the initial syncing nodes vote.
 *
 * Take into consideration the following scenario where the primary could not achieve the commit
 * quorum without the initial syncing nodes vote:
 * 1. Node A (primary) starts a two-phase index build "x_1" with commit quorum "votingMembers".
 * 2. Node B (secondary) shuts down while building the "x_1" index, preventing the node from sending
 *    its vote to the primary.
 * 3. Node A cannot achieve the commit quorum and is stuck. The "commitIndexBuild" oplog entry does
 *    not get sent to any other nodes.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = jsTest.name();
const collName = "commitQuorumWithInitialSync";

const rst = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
            },
        },
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
            },
        },
    ]
});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const db = primary.getDB(dbName);
const coll = db.getCollection(collName);

assert.commandWorked(coll.insert({a: 1, b: 1, c: 1, d: 1, e: 1, f: 1, g: 1}));
assert.commandWorked(coll.createIndex({a: 1}, {}, "votingMembers"));
rst.awaitReplication();

// Start multiple index builds using a commit quorum of "votingMembers", but pause the index build
// on the secondary, preventing it from voting to commit the index build.
jsTest.log("Pausing index builds on the secondary");
let secondary = rst.getSecondary();
IndexBuildTest.pauseIndexBuilds(secondary);

TestData.dbName = dbName;
TestData.collName = collName;
const awaitFirstIndexBuild = startParallelShell(() => {
    const coll = db.getSiblingDB(TestData.dbName).getCollection(TestData.collName);
    assert.commandWorked(coll.createIndex({b: 1}, {}, "votingMembers"));
}, primary.port);

const awaitSecondIndexBuild = startParallelShell(() => {
    const coll = db.getSiblingDB(TestData.dbName).getCollection(TestData.collName);
    assert.commandWorked(coll.createIndexes([{c: 1}, {d: 1}], {}, "votingMembers"));
}, primary.port);

const awaitThirdIndexBuild = startParallelShell(() => {
    const coll = db.getSiblingDB(TestData.dbName).getCollection(TestData.collName);
    assert.commandWorked(coll.createIndexes([{e: 1}, {f: 1}, {g: 1}], {}, "votingMembers"));
}, primary.port);

// Wait for all the indexes to start building on the primary.
IndexBuildTest.waitForIndexBuildToStart(db, collName, "b_1");
IndexBuildTest.waitForIndexBuildToStart(db, collName, "c_1");
IndexBuildTest.waitForIndexBuildToStart(db, collName, "d_1");
IndexBuildTest.waitForIndexBuildToStart(db, collName, "e_1");
IndexBuildTest.waitForIndexBuildToStart(db, collName, "f_1");
IndexBuildTest.waitForIndexBuildToStart(db, collName, "g_1");

// Restart the secondary with a clean data directory to start the initial sync process.
secondary = rst.restart(1, {
    startClean: true,
    setParameter: 'failpoint.initialSyncHangAfterDataCloning=' + tojson({mode: 'alwaysOn'}),
});

// The secondary node will start any in-progress two-phase index builds from the primary before
// starting the oplog replay phase. This ensures that the secondary will send its vote to the
// primary when it is ready to commit the index build. The index build on the secondary will get
// committed once the primary sends the "commitIndexBuild" oplog entry after the commit quorum is
// satisfied with the secondaries vote.
checkLog.containsJson(secondary, 21184);

// Cannot use IndexBuildTest helper functions on the secondary during initial sync.
function checkForIndexes(indexes) {
    for (let i = 0; i < indexes.length; i++) {
        checkLog.containsJson(secondary, 20384, {
            "properties": function(obj) {
                return obj.name === indexes[i];
            }
        });
    }
}
checkForIndexes(["b_1", "c_1", "d_1", "e_1", "f_1", "g_1"]);

assert.commandWorked(
    secondary.adminCommand({configureFailPoint: "initialSyncHangAfterDataCloning", mode: "off"}));

rst.awaitReplication();
rst.awaitSecondaryNodes();

awaitFirstIndexBuild();
awaitSecondIndexBuild();
awaitThirdIndexBuild();

let indexes = secondary.getDB(dbName).getCollection(collName).getIndexes();
assert.eq(8, indexes.length);

indexes = coll.getIndexes();
assert.eq(8, indexes.length);
rst.stopSet();
}());

/**
 * Test that commitQuorum option is not supported on standalones for index creation.
 * Note: noPassthrough/commit_quorum.js - Verifies the commitQuorum behavior for replica sets.
 */

(function() {
'use strict';

const standalone = MongoRunner.runMongod();
const db = standalone.getDB("test");

jsTestLog("Create index");
assert.commandFailedWithCode(
    db.runCommand(
        {createIndexes: "coll", indexes: [{name: "x_1", key: {x: 1}}], commitQuorum: "majority"}),
    ErrorCodes.BadValue);

MongoRunner.stopMongod(standalone);
})();

/**
 * Checks that the compact command exits cleanly on EBUSY.
 *
 * @tags: [requires_wiredtiger, requires_persistence]
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod({});
const db = conn.getDB("test");
const coll = db.getCollection(jsTest.name());

for (let i = 0; i < 10; i++) {
    assert.commandWorked(coll.insert({x: i}));
}

const failPoints = ["WTCompactRecordStoreEBUSY", "WTCompactIndexEBUSY"];
for (const failPoint of failPoints) {
    assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "alwaysOn"}));
    assert.commandFailedWithCode(db.runCommand({compact: jsTest.name()}), ErrorCodes.Interrupted);
    assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "off"}));
}

MongoRunner.stopMongod(conn);
}());

// Tests --networkMessageCompressors options.

(function() {
'use strict';

var runTest = function(optionValue, expected) {
    jsTest.log("Testing with --networkMessageCompressors=\"" + optionValue +
               "\" expecting: " + expected);
    var mongo = MongoRunner.runMongod({networkMessageCompressors: optionValue});
    assert.commandWorked(mongo.adminCommand({hello: 1}));
    clearRawMongoProgramOutput();
    assert.eq(runMongoProgram("mongo",
                              "--eval",
                              "tostrictjson(db.hello());",
                              "--port",
                              mongo.port,
                              "--networkMessageCompressors=snappy"),
              0);

    var output = rawMongoProgramOutput()
                     .split("\n")
                     .map(function(str) {
                         str = str.replace(/^sh[0-9]+\| /, "");
                         if (!/^{.*isWritablePrimary/.test(str)) {
                             return "";
                         }
                         return str;
                     })
                     .join("\n")
                     .trim();

    output = JSON.parse(output);

    assert.eq(output.compression, expected);
    MongoRunner.stopMongod(mongo);
};

assert.throws(() => MongoRunner.runMongod({networkMessageCompressors: "snappy,disabled"}));

runTest("snappy", ["snappy"]);
runTest("disabled", undefined);
}());

/* Test that snapshot reads and afterClusterTime majority reads are not allowed on
 * config.transactions.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const replSet = new ReplSetTest({nodes: 1});

replSet.startSet();
replSet.initiate();

const primary = replSet.getPrimary();
const primaryDB = primary.getDB('config');

const operationTime =
    assert.commandWorked(primaryDB.runCommand({find: "transactions"})).operationTime;
assert.commandWorked(
    primaryDB.runCommand({find: "transactions", readConcern: {level: "majority"}}));
assert.commandFailedWithCode(
    primaryDB.runCommand(
        {find: "transactions", readConcern: {level: "majority", afterClusterTime: operationTime}}),
    5557800);
assert.commandFailedWithCode(
    primaryDB.runCommand({find: "transactions", readConcern: {level: "snapshot"}}), 5557800);
assert.commandFailedWithCode(
    primaryDB.runCommand(
        {find: "transactions", readConcern: {level: "snapshot", atClusterTime: operationTime}}),
    5557800);

replSet.stopSet();
})();

// Test config file expansion using EXEC with digests.

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

// hash === SHA256HMAC('12345', 'secret')
const hash = 'f88c7ebe4740db59c873cecf5e1f18e3726a1ad64068a13d764b79028430ab0e';

// Simple positive case.
configExpandSuccess({
    setParameter: {
        scramIterationCount:
            {__exec: makeReflectionCmd('12345'), digest: hash, digest_key: '736563726574'}
    }
});

// Invalid digest length.
configExpandFailure({
    setParameter: {
        scramIteratorCount:
            {__exec: makeReflectionCmd('12345'), digest: '123', digest_key: '736563726574'}
    }
},
                    /digest: Not a valid, even length hex string/);

// Invalid characters.
configExpandFailure({
    setParameter: {
        scramIteratorCount:
            {__exec: makeReflectionCmd('12345'), digest: hash, digest_key: '736563X26574'}
    }
},
                    /digest_key: Not a valid, even length hex string/);

// Digest without key.
configExpandFailure(
    {setParameter: {scramIteratorCount: {__exec: makeReflectionCmd('12345'), digest: hash}}},
    /digest requires digest_key/);

// Empty digest_key.
configExpandFailure({
    setParameter:
        {scramIteratorCount: {__exec: makeReflectionCmd('12345'), digest: hash, digest_key: ''}}
},
                    /digest_key must not be empty/);

// Mismatched digests.
configExpandFailure({
    setParameter: {
        scramIteratorCount:
            {__exec: makeReflectionCmd('12345'), digest: hash, digest_key: '736563726575'}
    }
},
                    /does not match expected digest/);
})();

// Test config file expansion using EXEC.

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

// Unexpected elements.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__exec: makeReflectionCmd('12345'), foo: 'bar'},
    }
},
                    /expansion block must contain only '__exec'/);

const sicReflect = {
    setParameter: {scramIterationCount: {__exec: makeReflectionCmd('12345')}}
};

// Positive test just to be sure this works in a basic case before testing negatives.
configExpandSuccess(sicReflect);

// Expansion not enabled.
configExpandFailure(sicReflect, /__exec support has not been enabled/, {configExpand: 'none'});

// Expansion enabled, but not recursively.
configExpandFailure({__exec: makeReflectionCmd(jsToYaml(sicReflect)), type: 'yaml'},
                    /__exec support has not been enabled/);
})();

// Test config file expansion using EXEC when permissions are too loose.
// Ideally, we'd also check for foreign ownership here,
// but that's impractical in a test suite where we're not running as root.

(function() {
'use strict';

if (_isWindows()) {
    print("Skipping test on windows");
    return;
}

load('jstests/noPassthrough/libs/configExpand/lib.js');

const sicReflect = {
    setParameter: {scramIterationCount: {__exec: makeReflectionCmd('12345')}}
};

// Positive test just to be sure this works in a basic case before testing negatives.
configExpandSuccess(sicReflect, null, {configExpand: 'exec', chmod: 0o600});

// Still successful if readable by others, but not writable.
configExpandSuccess(sicReflect, null, {configExpand: 'exec', chmod: 0o644});

// Fail if writable by others.
const expect = /is writable by non-owner users/;
configExpandFailure(sicReflect, expect, {configExpand: 'exec', chmod: 0o666});
configExpandFailure(sicReflect, expect, {configExpand: 'exec', chmod: 0o622});
configExpandFailure(sicReflect, expect, {configExpand: 'exec', chmod: 0o660});
configExpandFailure(sicReflect, expect, {configExpand: 'exec', chmod: 0o606});

// Explicitly world-readable/writable config file without expansions should be fine.
configExpandSuccess({}, null, {configExpand: 'none', chmod: 0o666});
})();

// Test config file expansion using EXEC.

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

assert.eq(runNonMongoProgram.apply(null, makeReflectionCmd('12345', {sleep: 0}).split(" ")), 0);

// Sleep 10 seconds during request.
configExpandSuccess({
    setParameter: {
        scramIterationCount: {__exec: makeReflectionCmd('12345', {sleep: 10})},
    }
});

// Sleep 40 seconds during request, with default 30 second timeout.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__exec: makeReflectionCmd('12345', {sleep: 40})},
    }
},
                    /Timeout expired/);

// Sleep 10 seconds during request, with custom 5 second timeout.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__exec: makeReflectionCmd('12345', {sleep: 10})},
    }
},
                    /Timeout expired/,
                    {configExpandTimeoutSecs: 5});
})();

// Test config file expansion using EXEC.

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

// Basic success case
configExpandSuccess(
    {
        setParameter: {
            scramIterationCount: {__exec: makeReflectionCmd('12345')},
            scramSHA256IterationCount:
                {__exec: makeReflectionCmd("23456\n"), type: 'string', trim: 'whitespace'}
        }
    },
    function(admin) {
        const response = assert.commandWorked(admin.runCommand(
            {getParameter: 1, scramIterationCount: 1, scramSHA256IterationCount: 1}));
        assert.eq(response.scramIterationCount,
                  12345,
                  "Incorrect derived config value for scramIterationCount");
        assert.eq(response.scramSHA256IterationCount,
                  23456,
                  "Incorrect derived config value scramSHA256IterationCount");
    });
})();

// Test config file expansion using EXEC at top level.

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

const yamlConfig = jsToYaml({setParameter: {scramIterationCount: 12345}});
configExpandSuccess({__exec: makeReflectionCmd(yamlConfig), type: 'yaml'}, function(admin) {
    const response =
        assert.commandWorked(admin.runCommand({getParameter: 1, scramIterationCount: 1}));
    assert.eq(response.scramIterationCount, 12345, "Incorrect derived config value");
});
})();

// Test config file expansion using REST at top level.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

const web = new ConfigExpandRestServer();
web.start();

// Unexpected elements.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__rest: web.getStringReflectionURL('12345'), foo: 'bar'},
    }
},
                    /expansion block must contain only '__rest'/);

const sicReflect = {
    setParameter: {scramIterationCount: {__rest: web.getStringReflectionURL('12345')}}
};

// Positive test just to be sure this works in a basic case before testing negatives.
configExpandSuccess(sicReflect);

// Expansion not enabled.
configExpandFailure(sicReflect, /__rest support has not been enabled/, {configExpand: 'none'});

// Expansion enabled, but not recursively.
configExpandFailure(
    {__rest: web.getURL() + '/reflect/yaml?yaml=' + encodeURI(jsToYaml(sicReflect)), type: 'yaml'},
    /__rest support has not been enabled/);

web.stop();
})();

// Test config file expansion using REST when permissions are too loose.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

if (_isWindows()) {
    print("Skipping test on windows");
    return;
}

const web = new ConfigExpandRestServer();
web.start();

const sicReflect = {
    setParameter: {scramIterationCount: {__rest: web.getStringReflectionURL('12345')}}
};

// Positive test just to be sure this works in a basic case before testing negatives.
configExpandSuccess(sicReflect, null, {configExpand: 'rest', chmod: 0o600});

// Still successful if writable by others, but not readable.
configExpandSuccess(sicReflect, null, {configExpand: 'rest', chmod: 0o622});

// Fail if readable by others.
const expect = /is readable by non-owner users/;
configExpandFailure(sicReflect, expect, {configExpand: 'rest', chmod: 0o666});
configExpandFailure(sicReflect, expect, {configExpand: 'rest', chmod: 0o644});
configExpandFailure(sicReflect, expect, {configExpand: 'rest', chmod: 0o660});
configExpandFailure(sicReflect, expect, {configExpand: 'rest', chmod: 0o606});

web.stop();
})();

// Test config file expansion using REST at top level.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

const web = new ConfigExpandRestServer();
web.start();

// Sleep 10 seconds during request.
configExpandSuccess({
    setParameter: {
        scramIterationCount: {__rest: web.getStringReflectionURL('12345', {sleep: 10})},
    }
});

// Sleep 40 seconds during request, with default 30 second timeout.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__rest: web.getStringReflectionURL('12345', {sleep: 40})},
    }
},
                    /Timeout was reached/);

// Sleep 10 seconds during request, with custom 5 second timeout.
configExpandFailure({
    setParameter: {
        scramIterationCount: {__rest: web.getStringReflectionURL('12345', {sleep: 10})},
    }
},
                    /Timeout was reached/,
                    {configExpandTimeoutSecs: 5});

web.stop();
})();

// Test config file expansion using REST at top level.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

const web = new ConfigExpandRestServer();
web.start();

// Basic success case
configExpandSuccess(
    {
        setParameter: {
            scramIterationCount: {__rest: web.getStringReflectionURL('12345')},
            scramSHA256IterationCount:
                {__rest: web.getStringReflectionURL('23456'), type: 'string', trim: 'whitespace'}
        }
    },
    function(admin) {
        const response = assert.commandWorked(admin.runCommand(
            {getParameter: 1, scramIterationCount: 1, scramSHA256IterationCount: 1}));
        assert.eq(response.scramIterationCount,
                  12345,
                  "Incorrect derived config value for scramIterationCount");
        assert.eq(response.scramSHA256IterationCount,
                  23456,
                  "Incorrect derived config value scramSHA256IterationCount");
    });

// With digest
// SHA256HMAC('12345', 'secret')
const hash = 'f88c7ebe4740db59c873cecf5e1f18e3726a1ad64068a13d764b79028430ab0e';
configExpandSuccess({
    setParameter: {
        scramIterationCount:
            {__rest: web.getStringReflectionURL('12345'), digest: hash, digest_key: '736563726574'}
    }
});

web.stop();
})();

// Test config file expansion using REST at top level.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

const web = new ConfigExpandRestServer();
web.start();

const yamlConfig = jsToYaml({setParameter: {scramIterationCount: 12345}});
configExpandSuccess(
    {__rest: web.getURL() + '/reflect/yaml?yaml=' + encodeURI(yamlConfig), type: 'yaml'},
    function(admin) {
        const response =
            assert.commandWorked(admin.runCommand({getParameter: 1, scramIterationCount: 1}));
        assert.eq(response.scramIterationCount, 12345, "Incorrect derived config value");
    });

web.stop();
})();

// Tests that the read preference set on the connection is used when we call the count helper.
(function() {
"use strict";

var commandsRan = [];

// Create a new DB object backed by a mock connection.
function MockMongo() {
    this.getMinWireVersion = function getMinWireVersion() {
        return 0;
    };

    this.getMaxWireVersion = function getMaxWireVersion() {
        return 0;
    };
}
MockMongo.prototype = Mongo.prototype;
MockMongo.prototype.runCommand = function(db, cmd, opts) {
    commandsRan.push({db: db, cmd: cmd, opts: opts});
    return {ok: 1, n: 100};
};

const mockMongo = new MockMongo();
var db = new DB(mockMongo, "test");

// Attach a dummy implicit session because the mock connection cannot create sessions.
db._session = new _DummyDriverSession(mockMongo);

assert.eq(commandsRan.length, 0);

// Run a count with no readPref.
db.getMongo().setReadPref(null);
db.foo.count();

// Check that there is no readPref on the command document.
assert.eq(commandsRan.length, 1);
assert.docEq(commandsRan[0].cmd, {count: "foo", query: {}});

commandsRan = [];

// Run with readPref secondary.
db.getMongo().setReadPref("secondary");
db.foo.count();

// Check that we have wrapped the command and attached the read preference.
assert.eq(commandsRan.length, 1);
assert.docEq(commandsRan[0].cmd,
             {query: {count: "foo", query: {}}, $readPreference: {mode: "secondary"}});
})();

/**
 * Ensures that a createIndexes command request inside a transaction immediately errors if an
 * existing index build of a duplicate index is already in progress outside of the transaction.
 * @tags: [
 *     uses_transactions,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/parallel_shell_helpers.js");
load('jstests/libs/test_background_ops.js');

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "create_indexes_waits_for_already_in_progress";
const primary = rst.getPrimary();
const testDB = primary.getDB(dbName);
const testColl = testDB.getCollection(collName);
const indexSpecB = {
    key: {b: 1},
    name: "the_b_1_index"
};
const indexSpecC = {
    key: {c: 1},
    name: "the_c_1_index"
};

assert.commandWorked(testDB.runCommand({create: collName}));

const runSuccessfulIndexBuild = function(dbName, collName, indexSpec, requestNumber) {
    jsTest.log("Index build request " + requestNumber + " starting...");
    const res = db.getSiblingDB(dbName).runCommand({createIndexes: collName, indexes: [indexSpec]});
    jsTest.log("Index build request " + requestNumber +
               ", expected to succeed, result: " + tojson(res));
    assert.commandWorked(res);
};

const runFailedIndexBuildInTxn = function(dbName, collName, indexSpec, requestNumber) {
    const session = db.getMongo().startSession();

    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB[collName];
    jsTest.log("Index build request " + requestNumber + " starting in a transaction...");
    session.startTransaction();
    const res = sessionColl.runCommand({createIndexes: collName, indexes: [indexSpec]});
    jsTest.log("Index build request " + requestNumber +
               ", expected to fail, result: " + tojson(res));
    assert.commandFailedWithCode(res, ErrorCodes.IndexBuildAlreadyInProgress);
    assert.commandFailedWithCode(session.abortTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);
};

// Insert document into collection to avoid optimization for index creation on an empty collection.
// This allows us to pause index builds on the collection using a fail point.
assert.commandWorked(testColl.insert({a: 1}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'alwaysOn'}));
let joinFirstIndexBuild;
let joinSecondIndexBuild;
try {
    jsTest.log("Starting a parallel shell to run first index build request...");
    joinFirstIndexBuild = startParallelShell(
        funWithArgs(runSuccessfulIndexBuild, dbName, collName, indexSpecB, 1), primary.port);

    jsTest.log("Waiting for first index build to get started...");
    checkLog.contains(primary,
                      "Hanging index build due to failpoint 'hangAfterSettingUpIndexBuild'");

    jsTest.log(
        "Starting a parallel shell to run a transaction with a second index build request...");
    joinSecondIndexBuild = startParallelShell(
        funWithArgs(runFailedIndexBuildInTxn, dbName, collName, indexSpecB, 2), primary.port);

} finally {
    assert.commandWorked(
        testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'off'}));
}

joinFirstIndexBuild();
joinSecondIndexBuild();

// We should have the _id index and the 'the_b_1_index' index just built.
assert.eq(testColl.getIndexes().length, 2);
rst.stopSet();
})();

/**
 * Test that create view only takes database IX lock.
 *
 * @tags: [uses_transactions, requires_db_locking]
 */

(function() {
"use strict";

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let db = rst.getPrimary().getDB("test");

assert.commandWorked(db.runCommand({insert: "a", documents: [{x: 1}]}));

const session = db.getMongo().startSession();
const sessionDb = session.getDatabase("test");

session.startTransaction();
// This holds a database IX lock and a collection IX lock on "a".
assert.commandWorked(sessionDb.a.insert({y: 1}));

// This only requires database IX lock.
assert.commandWorked(db.createView("view", "a", []));

assert.eq(db.view.find().toArray().length, 1);

assert.commandWorked(session.commitTransaction_forTesting());

rst.stopSet();
})();

// Test that a user is not allowed to getMore a cursor they did not create, and that such a failed
// getMore will leave the cursor unaffected, so that a subsequent getMore by the original author
// will work.
// @tags: [requires_sharding]

(function() {
const st = new ShardingTest({shards: 2, config: 1, other: {keyFile: "jstests/libs/key1"}});
const kDBName = "test";
const adminDB = st.s.getDB('admin');
const testDB = st.s.getDB(kDBName);

jsTest.authenticate(st.shard0);

const adminUser = {
    db: "admin",
    username: "foo",
    password: "bar"
};
const userA = {
    db: "test",
    username: "a",
    password: "pwd"
};
const userB = {
    db: "test",
    username: "b",
    password: "pwd"
};

function login(userObj) {
    st.s.getDB(userObj.db).auth(userObj.username, userObj.password);
}

function logout(userObj) {
    st.s.getDB(userObj.db).runCommand({logout: 1});
}

adminDB.createUser(
    {user: adminUser.username, pwd: adminUser.password, roles: jsTest.adminUserRoles});

login(adminUser);

let coll = testDB.security_501;
coll.drop();

for (let i = 0; i < 100; i++) {
    assert.commandWorked(coll.insert({_id: i}));
}

// Create our two users.
for (let user of [userA, userB]) {
    testDB.createUser({
        user: user.username,
        pwd: user.password,
        roles: [{role: "readWriteAnyDatabase", db: "admin"}]
    });
}
logout(adminUser);

// As userA, run a find and get a cursor.
login(userA);
const cursorID =
    assert.commandWorked(testDB.runCommand({find: coll.getName(), batchSize: 2})).cursor.id;
logout(userA);

// As userB, attempt to getMore the cursor ID.
login(userB);
assert.commandFailed(testDB.runCommand({getMore: cursorID, collection: coll.getName()}));
logout(userB);

// As user A again, try to getMore the cursor.
login(userA);
assert.commandWorked(testDB.runCommand({getMore: cursorID, collection: coll.getName()}));
logout(userA);

st.stop();
})();

// @tags: [requires_replication, uses_transactions, uses_atclustertime]

// Test the correct timestamping of insert, update, and delete writes along with their accompanying
// index updates.
//

(function() {
"use strict";

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const testDB = rst.getPrimary().getDB(dbName);
const coll = testDB.getCollection(collName);

if (!testDB.serverStatus().storageEngine.supportsSnapshotReadConcern) {
    rst.stopSet();
    return;
}

// Turn off timestamp reaping.
assert.commandWorked(testDB.adminCommand({
    configureFailPoint: "WTPreserveSnapshotHistoryIndefinitely",
    mode: "alwaysOn",
}));

const session = testDB.getMongo().startSession({causalConsistency: false});
const sessionDb = session.getDatabase(dbName);
const response = assert.commandWorked(testDB.createCollection("coll"));
const startTime = response.operationTime;

function check(atClusterTime, expected) {
    session.startTransaction({readConcern: {level: "snapshot", atClusterTime: atClusterTime}});
    // Check both a collection scan and scanning the _id index.
    [{$natural: 1}, {_id: 1}].forEach(sort => {
        let response = assert.commandWorked(
            sessionDb.runCommand({find: collName, sort: sort, singleBatch: true}));
        assert.eq(expected, response.cursor.firstBatch);
    });
    assert.commandWorked(session.commitTransaction_forTesting());
}

// insert

let request = {insert: coll.getName(), documents: [{_id: 1}, {_id: 2}], ordered: false};
assert.commandWorked(coll.runCommand(request));

const oplog = rst.getPrimary().getDB("local").getCollection("oplog.rs");
let ts1 = oplog.findOne({o: {_id: 1}}).ts;
let ts2 = oplog.findOne({o: {_id: 2}}).ts;

check(startTime, []);
check(ts1, [{_id: 1}]);
check(ts2, [{_id: 1}, {_id: 2}]);

// upsert

request = {
    update: coll.getName(),
    updates: [
        {q: {_id: 3, a: 1}, u: {$set: {a: 2}}, upsert: true},
        {q: {_id: 4, a: 1}, u: {$set: {a: 3}}, upsert: true}
    ],
    ordered: true
};
assert.commandWorked(coll.runCommand(request));

ts1 = oplog.findOne({o: {_id: 3, a: 2}}).ts;
ts2 = oplog.findOne({o: {_id: 4, a: 3}}).ts;

check(ts1, [{_id: 1}, {_id: 2}, {_id: 3, a: 2}]);
check(ts2, [{_id: 1}, {_id: 2}, {_id: 3, a: 2}, {_id: 4, a: 3}]);

// update

request = {
    update: coll.getName(),
    updates: [{q: {_id: 3, a: 2}, u: {$set: {a: 4}}}, {q: {_id: 4, a: 3}, u: {$set: {a: 5}}}],
    ordered: true
};
assert.commandWorked(coll.runCommand(request));

ts1 = oplog.findOne({op: 'u', o2: {_id: 3}}).ts;
ts2 = oplog.findOne({op: 'u', o2: {_id: 4}}).ts;

check(ts1, [{_id: 1}, {_id: 2}, {_id: 3, a: 4}, {_id: 4, a: 3}]);
check(ts2, [{_id: 1}, {_id: 2}, {_id: 3, a: 4}, {_id: 4, a: 5}]);

// delete

request = {
    delete: coll.getName(),
    deletes: [{q: {}, limit: 0}],
    ordered: false
};

assert.commandWorked(coll.runCommand(request));

ts1 = oplog.findOne({op: 'd', o: {_id: 1}}).ts;
ts2 = oplog.findOne({op: 'd', o: {_id: 2}}).ts;
let ts3 = oplog.findOne({op: 'd', o: {_id: 3}}).ts;
let ts4 = oplog.findOne({op: 'd', o: {_id: 4}}).ts;

check(ts1, [{_id: 2}, {_id: 3, a: 4}, {_id: 4, a: 5}]);
check(ts2, [{_id: 3, a: 4}, {_id: 4, a: 5}]);
check(ts3, [{_id: 4, a: 5}]);
check(ts4, []);

session.endSession();
rst.stopSet();
}());

/**
 * Verifies that the 'dataThroughputLastSecond' and 'dataThroughputAverage' fields appear in the
 * currentOp output while running validation.
 *
 * ephemeralForTest does not support background validation, which is needed to report the
 * 'dataThroughputLastSecond' and 'dataThroughputAverage' fields in currentOp.
 *
 * @tags: [incompatible_with_eft]
 */
(function() {
const dbName = "test";
const collName = "currentOpValidation";

const conn = MongoRunner.runMongod();

let db = conn.getDB(dbName);
let coll = db.getCollection(collName);

coll.drop();

assert.commandWorked(coll.createIndex({a: 1}));
for (let i = 0; i < 5; i++) {
    assert.commandWorked(coll.insert({a: i}));
}

// The throttle is off by default.
assert.commandWorked(db.adminCommand({setParameter: 1, maxValidateMBperSec: 1}));

// Simulate each record being 512KB.
assert.commandWorked(db.adminCommand(
    {configureFailPoint: "fixedCursorDataSizeOf512KBForDataThrottle", mode: "alwaysOn"}));

// This fail point comes after we've traversed the record store, so currentOp should have some
// validation statistics once we hit this fail point.
assert.commandWorked(
    db.adminCommand({configureFailPoint: "pauseCollectionValidationWithLock", mode: "alwaysOn"}));

TestData.dbName = dbName;
TestData.collName = collName;
const awaitValidation = startParallelShell(() => {
    assert.commandWorked(
        db.getSiblingDB(TestData.dbName).getCollection(TestData.collName).validate({
            background: true
        }));
}, conn.port);

checkLog.containsJson(conn, 20304);

const curOpFilter = {
    'command.validate': collName
};

let curOp = assert.commandWorked(db.currentOp(curOpFilter));
assert(curOp.inprog.length == 1);
assert(curOp.inprog[0].hasOwnProperty("dataThroughputLastSecond") &&
       curOp.inprog[0].hasOwnProperty("dataThroughputAverage"));

curOp = conn.getDB("admin").aggregate([{$currentOp: {}}, {$match: curOpFilter}]).toArray();
assert(curOp.length == 1);
assert(curOp[0].hasOwnProperty("dataThroughputLastSecond") &&
       curOp[0].hasOwnProperty("dataThroughputAverage"));

// Finish up validating the collection.
assert.commandWorked(db.adminCommand(
    {configureFailPoint: "fixedCursorDataSizeOf512KBForDataThrottle", mode: "off"}));
assert.commandWorked(
    db.adminCommand({configureFailPoint: "pauseCollectionValidationWithLock", mode: "off"}));

// Setting this to 0 turns off the throttle.
assert.commandWorked(db.adminCommand({setParameter: 1, maxValidateMBperSec: 0}));

awaitValidation();

MongoRunner.stopMongod(conn);
}());

// Test whether a pinned cursor does not show up as an idle cursor in curOp.
// Then test and make sure a pinned cursor shows up in the operation object.
// @tags: [
//   requires_sharding,
// ]
(function() {
"use strict";
load("jstests/libs/pin_getmore_cursor.js");  // for "withPinnedCursor"

function runTest(cursorId, coll) {
    const db = coll.getDB();
    const adminDB = db.getSiblingDB("admin");
    // Test that active cursors do not show up as idle cursors.
    const idleCursors =
        adminDB
            .aggregate([
                {"$currentOp": {"localOps": true, "idleCursors": true, "allUsers": false}},
                {"$match": {"type": "idleCursor"}}
            ])
            .toArray();
    assert.eq(idleCursors.length, 0, tojson(idleCursors));
    // Test that an active cursor shows up in currentOp.
    const activeCursors =
        adminDB
            .aggregate([
                {"$currentOp": {"localOps": true, "idleCursors": false, "allUsers": false}},
                {"$match": {"cursor": {"$exists": true}}}
            ])
            .toArray();
    assert.eq(activeCursors.length, 1, tojson(activeCursors));
    const cursorObject = activeCursors[0].cursor;
    assert.eq(cursorObject.originatingCommand.find, coll.getName(), tojson(activeCursors));
    assert.eq(cursorObject.nDocsReturned, 2, tojson(activeCursors));
    assert.eq(cursorObject.tailable, false, tojson(activeCursors));
    assert.eq(cursorObject.awaitData, false, tojson(activeCursors));
}
const conn = MongoRunner.runMongod({});
let failPointName = "waitWithPinnedCursorDuringGetMoreBatch";
withPinnedCursor({
    conn: conn,
    sessionId: null,
    db: conn.getDB("test"),
    assertFunction: runTest,
    runGetMoreFunc: function() {
        const response =
            assert.commandWorked(db.runCommand({getMore: cursorId, collection: collName}));
    },
    failPointName: failPointName,
    assertEndCounts: true
});
MongoRunner.stopMongod(conn);

// Sharded test
failPointName = "waitAfterPinningCursorBeforeGetMoreBatch";
let st = new ShardingTest({shards: 2, mongos: 1});
withPinnedCursor({
    conn: st.s,
    sessionId: null,
    db: st.s.getDB("test"),
    assertFunction: runTest,
    runGetMoreFunc: function() {
        const response =
            assert.commandWorked(db.runCommand({getMore: cursorId, collection: collName}));
    },
    failPointName: failPointName,
    assertEndCounts: true
});
st.stop();
})();

/**
 * Confirms inclusion of a 'transaction' object containing lsid and txnNumber in
 * currentOp() for a prepared transaction and an active non-prepared transaction.
 * @tags: [uses_transactions, uses_prepare_transaction]
 */

(function() {
'use strict';
load("jstests/libs/parallel_shell_helpers.js");

function transactionFn(isPrepared) {
    const collName = 'currentop_active_transaction';
    const session = db.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase('test');

    session.startTransaction({readConcern: {level: 'snapshot'}});
    sessionDB[collName].update({}, {x: 2});
    if (isPrepared) {
        // Load the prepare helpers to be called in the parallel shell.
        load('jstests/core/txns/libs/prepare_helpers.js');
        const prepareTimestamp = PrepareHelpers.prepareTransaction(session);
        PrepareHelpers.commitTransaction(session, prepareTimestamp);
    } else {
        assert.commandWorked(session.commitTransaction_forTesting());
    }
}

function checkCurrentOpFields(currentOp,
                              isPrepared,
                              operationTime,
                              timeBeforeTransactionStarts,
                              timeAfterTransactionStarts,
                              timeBeforeCurrentOp) {
    const transactionDocument = currentOp[0].transaction;
    assert.eq(transactionDocument.parameters.autocommit,
              false,
              "Expected 'autocommit' to be false but got " +
                  transactionDocument.parameters.autocommit +
                  " instead: " + tojson(transactionDocument));
    assert.eq(transactionDocument.parameters.readConcern.level,
              'snapshot',
              "Expected 'readConcern' level to be snapshot but got " +
                  tojson(transactionDocument.parameters.readConcern) +
                  " instead: " + tojson(transactionDocument));
    assert.gte(transactionDocument.readTimestamp,
               operationTime,
               "Expected 'readTimestamp' to be at least " + tojson(operationTime) + " but got " +
                   tojson(transactionDocument.readTimestamp) +
                   " instead: " + tojson(transactionDocument));
    assert.gte(ISODate(transactionDocument.startWallClockTime),
               timeBeforeTransactionStarts,
               "Expected 'startWallClockTime' to be at least" +
                   tojson(timeBeforeTransactionStarts) + " but got " +
                   transactionDocument.startWallClockTime +
                   " instead: " + tojson(transactionDocument));
    const expectedTimeOpen = (timeBeforeCurrentOp - timeAfterTransactionStarts) * 1000;
    assert.gt(transactionDocument.timeOpenMicros,
              expectedTimeOpen,
              "Expected 'timeOpenMicros' to be at least" + expectedTimeOpen + " but got " +
                  transactionDocument.timeOpenMicros + " instead: " + tojson(transactionDocument));
    assert.gte(transactionDocument.timeActiveMicros,
               0,
               "Expected 'timeActiveMicros' to be at least 0: " + tojson(transactionDocument));
    assert.gte(transactionDocument.timeInactiveMicros,
               0,
               "Expected 'timeInactiveMicros' to be at least 0: " + tojson(transactionDocument));
    const actualExpiryTime = ISODate(transactionDocument.expiryTime).getTime();
    const expectedExpiryTime =
        ISODate(transactionDocument.startWallClockTime).getTime() + transactionLifeTime * 1000;
    assert.eq(expectedExpiryTime,
              actualExpiryTime,
              "Expected 'expiryTime' to be " + expectedExpiryTime + " but got " + actualExpiryTime +
                  " instead: " + tojson(transactionDocument));
    if (isPrepared) {
        assert.gte(
            transactionDocument.timePreparedMicros,
            0,
            "Expected 'timePreparedMicros' to be at least 0: " + tojson(transactionDocument));
    }
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const collName = 'currentop_active_transaction';
const testDB = rst.getPrimary().getDB('test');
const adminDB = rst.getPrimary().getDB('admin');
testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
assert.commandWorked(testDB[collName].insert({x: 1}, {writeConcern: {w: "majority"}}));

// Run an operation prior to starting the transaction and save its operation time. We will use
// this later to assert that our subsequent transaction's readTimestamp is greater than or equal
// to this operation time.
let res = assert.commandWorked(testDB.runCommand({insert: collName, documents: [{x: 1}]}));

// Set and save the transaction's lifetime. We will use this later to assert that our
// transaction's expiry time is equal to its start time + lifetime.
const transactionLifeTime = 10;
assert.commandWorked(
    testDB.adminCommand({setParameter: 1, transactionLifetimeLimitSeconds: transactionLifeTime}));

// This will make the transaction hang.
assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'hangAfterSettingPrepareStartTime', mode: 'alwaysOn'}));

let timeBeforeTransactionStarts = new ISODate();
let isPrepared = true;
const joinPreparedTransaction =
    startParallelShell(funWithArgs(transactionFn, isPrepared), rst.ports[0]);

const prepareTransactionFilter = {
    active: true,
    'lsid': {$exists: true},
    'transaction.parameters.txnNumber': {$eq: 0},
    'transaction.parameters.autocommit': {$eq: false},
    'transaction.timePreparedMicros': {$exists: true}
};

// Keep running currentOp() until we see the transaction subdocument.
assert.soon(function() {
    return 1 ===
        adminDB.aggregate([{$currentOp: {}}, {$match: prepareTransactionFilter}]).itcount();
});

let timeAfterTransactionStarts = new ISODate();
// Sleep here to allow some time between timeAfterTransactionStarts and timeBeforeCurrentOp to
// elapse.
sleep(100);
let timeBeforeCurrentOp = new ISODate();
// Check that the currentOp's transaction subdocument's fields align with our expectations.
let currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: prepareTransactionFilter}]).toArray();
checkCurrentOpFields(currentOp,
                     isPrepared,
                     res.operationTime,
                     timeBeforeTransactionStarts,
                     timeAfterTransactionStarts,
                     timeBeforeCurrentOp);

// Now the transaction can proceed.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingPrepareStartTime', mode: 'off'}));
joinPreparedTransaction();

// Conduct the same test but with a non-prepared transaction.
res = assert.commandWorked(testDB.runCommand({insert: collName, documents: [{x: 1}]}));

// This will make the transaction hang.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangDuringBatchUpdate', mode: 'alwaysOn'}));

timeBeforeTransactionStarts = new ISODate();
isPrepared = false;
const joinTransaction = startParallelShell(funWithArgs(transactionFn, isPrepared), rst.ports[0]);

const transactionFilter = {
    active: true,
    'lsid': {$exists: true},
    'transaction.parameters.txnNumber': {$eq: 0},
    'transaction.parameters.autocommit': {$eq: false},
    'transaction.timePreparedMicros': {$exists: false}
};

// Keep running currentOp() until we see the transaction subdocument.
assert.soon(function() {
    return 1 === adminDB.aggregate([{$currentOp: {}}, {$match: transactionFilter}]).itcount();
});

timeAfterTransactionStarts = new ISODate();
// Sleep here to allow some time between timeAfterTransactionStarts and timeBeforeCurrentOp to
// elapse.
sleep(100);
timeBeforeCurrentOp = new ISODate();
// Check that the currentOp's transaction subdocument's fields align with our expectations.
currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: transactionFilter}]).toArray();
checkCurrentOpFields(currentOp,
                     isPrepared,
                     res.operationTime,
                     timeBeforeTransactionStarts,
                     timeAfterTransactionStarts,
                     timeBeforeCurrentOp);

// Now the transaction can proceed.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangDuringBatchUpdate', mode: 'off'}));
joinTransaction();

rst.stopSet();
})();

/**
 * Tests that the object returned by currentOp() for an inactive transaction includes information
 * about the last client that has run an operation against this transaction.
 *
 * @tags: [uses_transactions]
 */

(function() {
'use strict';

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const collName = 'currentop_last_client_info';
const dbName = 'test';
const testDB = rst.getPrimary().getDB(dbName);
const adminDB = rst.getPrimary().getDB('admin');
testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
assert.commandWorked(testDB[collName].insert({x: 1}, {writeConcern: {w: "majority"}}));

// Start a new Session.
const lsid = assert.commandWorked(testDB.runCommand({startSession: 1})).id;
const txnNumber = NumberLong(0);
assert.commandWorked(testDB.runCommand({
    find: collName,
    lsid: lsid,
    txnNumber: txnNumber,
    readConcern: {level: "snapshot"},
    startTransaction: true,
    autocommit: false
}));

const currentOpFilter = {
    active: false,
    'lsid.id': {$eq: lsid.id},
    'client': {$exists: true}
};

let currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: currentOpFilter}]).toArray();
assert.eq(currentOp.length, 1);

let currentOpEntry = currentOp[0];
const connectionId = currentOpEntry.connectionId;
// Check that the currentOp object contains information about the last client that has run an
// operation and that its values align with our expectations.
assert.eq(currentOpEntry.appName, "MongoDB Shell");
assert.eq(currentOpEntry.clientMetadata.application.name, "MongoDB Shell");
assert.eq(currentOpEntry.clientMetadata.driver.name, "MongoDB Internal Client");

// Create a new Client and run another operation on the same session.
const otherClient = new Mongo(rst.getPrimary().host);
assert.commandWorked(otherClient.getDB(dbName).runCommand(
    {find: collName, lsid: lsid, txnNumber: txnNumber, autocommit: false}));

currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: currentOpFilter}]).toArray();
currentOpEntry = currentOp[0];
// Check that the last client that has ran an operation against this session has a different
// connectionId than the previous client.
assert.neq(currentOpEntry.connectionId, connectionId);

assert.commandWorked(testDB.adminCommand({
    commitTransaction: 1,
    lsid: lsid,
    txnNumber: txnNumber,
    autocommit: false,
    writeConcern: {w: 'majority'}
}));

rst.stopSet();
})();

/**
 * Test that the operation latencies reported in current op for a getMore on an awaitData cursor
 * include time spent blocking for the await time.
 * @tags: [requires_capped]
 */
(function() {
"use test";

// This test runs a getMore in a parallel shell, which will not inherit the implicit session of
// the cursor establishing command.
TestData.disableImplicitSessions = true;

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");
const testDB = conn.getDB("test");
const coll = testDB.currentop_includes_await_time;

coll.drop();
assert.commandWorked(testDB.createCollection(coll.getName(), {capped: true, size: 1024}));
assert.commandWorked(coll.insert({_id: 1}));

let cmdRes = assert.commandWorked(
    testDB.runCommand({find: coll.getName(), tailable: true, awaitData: true}));

TestData.commandResult = cmdRes;
let cleanupShell = startParallelShell(function() {
    db.getSiblingDB("test").runCommand({
        getMore: TestData.commandResult.cursor.id,
        collection: "currentop_includes_await_time",
        maxTimeMS: 5 * 60 * 1000,
    });
}, conn.port);

assert.soon(function() {
    // This filter ensures that the getMore 'secs_running' and 'microsecs_running' fields are
    // sufficiently large that they appear to include time spent blocking waiting for capped
    // inserts.
    let ops = testDB.currentOp({
        "command.getMore": {$exists: true},
        "ns": coll.getFullName(),
        secs_running: {$gte: 2},
        microsecs_running: {$gte: 2 * 1000 * 1000}
    });
    return ops.inprog.length === 1;
}, printjson(testDB.currentOp()));

// A capped insertion should unblock the getMore, allowing the test to complete before the
// getMore's awaitData time expires.
assert.commandWorked(coll.insert({_id: 2}));

cleanupShell();
MongoRunner.stopMongod(conn);
}());

/**
 * Confirms inclusion of query, command object and planSummary in currentOp() for CRUD operations.
 * This test should not be run in the parallel suite as it sets fail points.
 * @tags: [
 *    requires_replication,
 *    requires_sharding,
 * ]
 */
(function() {
"use strict";

// This test runs manual getMores using different connections, which will not inherit the
// implicit session of the cursor establishing command.
TestData.disableImplicitSessions = true;

load("jstests/libs/fixture_helpers.js");  // For FixtureHelpers.

// Set up a 2-shard cluster. Configure 'internalQueryExecYieldIterations' on both shards such
// that operations will yield on each PlanExecuter iteration.
const st = new ShardingTest({
    name: jsTestName(),
    shards: 2,
    rs: {nodes: 1, setParameter: {internalQueryExecYieldIterations: 1}}
});

// Obtain one mongoS connection and a second direct to the shard.
const rsConn = st.rs0.getPrimary();
const mongosConn = st.s;

const mongosDB = mongosConn.getDB("currentop_query");
const mongosColl = mongosDB.currentop_query;

// Enable sharding on the the test database and ensure that the primary is on shard0.
assert.commandWorked(mongosDB.adminCommand({enableSharding: mongosDB.getName()}));
st.ensurePrimaryShard(mongosDB.getName(), rsConn.name);

// On a sharded cluster, aggregations which are dispatched to multiple shards first establish
// zero-batch cursors and only hit the failpoints on the following getMore. This helper takes a
// generic command object and creates an appropriate filter given the use-case.
function commandOrOriginatingCommand(cmdObj, isRemoteShardCurOp) {
    const cmdFieldName = (isRemoteShardCurOp ? "cursor.originatingCommand" : "command");
    const cmdFilter = {};
    for (let subFieldName in cmdObj) {
        cmdFilter[`${cmdFieldName}.${subFieldName}`] = cmdObj[subFieldName];
    }
    return cmdFilter;
}

// Drops and re-creates the sharded test collection.
function dropAndRecreateTestCollection() {
    assert(mongosColl.drop());
    assert.commandWorked(
        mongosDB.adminCommand({shardCollection: mongosColl.getFullName(), key: {_id: "hashed"}}));
}

/**
 * @param {connection} conn - The connection through which to run the test suite.
 * @params {function} currentOp - Function which takes a database object and a filter, and
 * returns an array of matching current operations. This allows us to test output for both the
 * currentOp command and the $currentOp aggregation stage.
 * @params {boolean} truncatedOps - if true, we expect operations that exceed the maximum
 * currentOp size to be truncated in the output 'command' field, and we run only a subset of
 * tests designed to exercise that scenario. If false, we expect the entire operation to be
 * returned.
 * @params {boolean} localOps - if true, we expect currentOp to return operations running on a
 * mongoS itself rather than on the shards.
 */
function runTests({conn, currentOp, truncatedOps, localOps}) {
    const testDB = conn.getDB("currentop_query");
    const coll = testDB.currentop_query;
    dropAndRecreateTestCollection();

    for (let i = 0; i < 5; ++i) {
        assert.commandWorked(coll.insert({_id: i, a: i}));
    }

    const isLocalMongosCurOp = (FixtureHelpers.isMongos(testDB) && localOps);
    const isRemoteShardCurOp = (FixtureHelpers.isMongos(testDB) && !localOps);

    // If 'truncatedOps' is true, run only the subset of tests designed to validate the
    // truncation behaviour. Otherwise, run the standard set of tests which assume that
    // truncation will not occur.
    if (truncatedOps) {
        runTruncationTests();
    } else {
        runStandardTests();
    }

    /**
     * Captures currentOp() for a given test command/operation and confirms that namespace,
     * operation type and planSummary are correct.
     *
     *  - 'testObj' - Contains test arguments.
     *  - 'testObj.test' - A function that runs the desired test op/cmd.
     *  - 'testObj.planSummary' - A string containing the expected planSummary.
     *  - 'testObj.currentOpFilter' - A filter to be used to narrow currentOp() output to only the
     *  relevant operation or command.
     *  - 'testObj.command]' - The command to test against. Will look for this to be a key in the
     *  currentOp().query object.
     *  - 'testObj.operation' - The operation to test against. Will look for this to be the value
     *  of the currentOp().op field.
     *  - 'testObj.skipMongosLocalOps' - True if this test should not be run against a mongos with
     *  localOps=true.
     */
    function confirmCurrentOpContents(testObj) {
        const skipMongosLocalOps = testObj.skipMongosLocalOps || false;

        if (isLocalMongosCurOp && skipMongosLocalOps) {
            return;
        }

        // Force queries to hang on yield to allow for currentOp capture.
        FixtureHelpers.runCommandOnEachPrimary({
            db: conn.getDB("admin"),
            cmdObj: {
                configureFailPoint: "setYieldAllLocksHang",
                mode: "alwaysOn",
                data: {namespace: mongosColl.getFullName()}
            }
        });

        // Set the test configuration in TestData for the parallel shell test.
        TestData.currentOpTest = testObj.test;
        TestData.currentOpCollName = "currentop_query";

        // Wrapper function which sets DB before running the test function
        // found at TestData.currentOpTest.
        function doTest() {
            const testDB = db.getSiblingDB(TestData.currentOpCollName);
            TestData.currentOpTest(testDB);
        }

        // Run the operation in the background.
        var awaitShell = startParallelShell(doTest, testDB.getMongo().port);

        // Augment the currentOpFilter with additional known predicates.
        if (!testObj.currentOpFilter.ns) {
            testObj.currentOpFilter.ns = coll.getFullName();
        }
        if (!isLocalMongosCurOp) {
            testObj.currentOpFilter.planSummary = testObj.planSummary;
        }
        if (testObj.hasOwnProperty("command")) {
            testObj.currentOpFilter["command." + testObj.command] = {$exists: true};
        } else if (testObj.hasOwnProperty("operation")) {
            testObj.currentOpFilter.op = testObj.operation;
        }

        // Capture currentOp record for the query and confirm that the 'query' and 'planSummary'
        // fields contain the content expected. We are indirectly testing the 'ns' field as well
        // with the currentOp query argument.
        assert.soon(
            function() {
                var result = currentOp(testDB, testObj.currentOpFilter, truncatedOps, localOps);
                assert.commandWorked(result);

                if (result.inprog.length > 0) {
                    result.inprog.forEach((op) => {
                        assert.eq(op.appName, "MongoDB Shell", tojson(result));
                        assert.eq(
                            op.clientMetadata.application.name, "MongoDB Shell", tojson(result));
                    });
                    return true;
                }

                return false;
            },
            function() {
                return "Failed to find operation from " + tojson(testObj.currentOpFilter) +
                    " in currentOp() output: " +
                    tojson(currentOp(testDB, {}, truncatedOps, localOps)) +
                    (isLocalMongosCurOp ? ", with localOps=false: " +
                             tojson(currentOp(testDB, {}, truncatedOps, false))
                                        : "");
            });

        // Allow the query to complete.
        FixtureHelpers.runCommandOnEachPrimary({
            db: conn.getDB("admin"),
            cmdObj: {configureFailPoint: "setYieldAllLocksHang", mode: "off"}
        });

        awaitShell();
        delete TestData.currentOpCollName;
        delete TestData.currentOpTest;
    }

    /**
     * Runs a set of tests to verify that the currentOp output appears as expected. These tests
     * assume that the 'truncateOps' parameter is false, so no command objects in the currentOp
     * output will be truncated to string.
     */
    function runStandardTests() {
        //
        // Confirm currentOp content for commands defined in 'testList'.
        //
        var testList = [
            {
                test: function(db) {
                    assert.eq(db.currentop_query
                                  .aggregate([{$match: {a: 1, $comment: "currentop_query"}}], {
                                      collation: {locale: "fr"},
                                      hint: {_id: 1},
                                      comment: "currentop_query_2"
                                  })
                                  .itcount(),
                              1);
                },
                planSummary: "IXSCAN { _id: 1 }",
                currentOpFilter: commandOrOriginatingCommand({
                    "aggregate": {$exists: true},
                    "pipeline.0.$match.$comment": "currentop_query",
                    "comment": "currentop_query_2",
                    "collation.locale": "fr",
                    "hint": {_id: 1}
                },
                                                             isRemoteShardCurOp),
            },
            {
                test: function(db) {
                    assert.eq(db.currentop_query.find({a: 1, $comment: "currentop_query"})
                                  .collation({locale: "fr"})
                                  .count(),
                              1);
                },
                command: "count",
                planSummary: "COLLSCAN",
                currentOpFilter:
                    {"command.query.$comment": "currentop_query", "command.collation.locale": "fr"}
            },
            {
                test: function(db) {
                    assert.eq(
                        db.currentop_query.distinct(
                            "a", {a: 1, $comment: "currentop_query"}, {collation: {locale: "fr"}}),
                        [1]);
                },
                command: "distinct",
                planSummary: "COLLSCAN",
                currentOpFilter:
                    {"command.query.$comment": "currentop_query", "command.collation.locale": "fr"}
            },
            {
                test: function(db) {
                    assert.eq(db.currentop_query.find({a: 1}).comment("currentop_query").itcount(),
                              1);
                },
                command: "find",
                planSummary: "COLLSCAN",
                currentOpFilter: {"command.comment": "currentop_query"}
            },
            {
                test: function(db) {
                    assert.eq(db.currentop_query.find({a: 1}).comment("currentop_query").itcount(),
                              1);
                },
                command: "find",
                // Yields only take place on a mongod. Since this test depends on checking that the
                // currentOp's reported 'numYields' has advanced beyond zero, this test is not
                // expected to work when running against a mongos with localOps=true.
                skipMongosLocalOps: true,
                planSummary: "COLLSCAN",
                currentOpFilter: {"command.comment": "currentop_query", numYields: {$gt: 0}}
            },
            {
                test: function(db) {
                    assert.eq(db.currentop_query.findAndModify({
                        query: {_id: 1, a: 1, $comment: "currentop_query"},
                        update: {$inc: {b: 1}},
                        collation: {locale: "fr"}
                    }),
                              {"_id": 1, "a": 1});
                },
                command: "findandmodify",
                planSummary: "IXSCAN { _id: 1 }",
                currentOpFilter:
                    {"command.query.$comment": "currentop_query", "command.collation.locale": "fr"}
            },
            {
                test: function(db) {
                    assert.commandWorked(db.currentop_query.mapReduce(() => {}, (a, b) => {}, {
                        query: {$comment: "currentop_query_mr"},
                        out: {inline: 1},
                    }));
                },
                planSummary: "COLLSCAN",
                // A mapReduce which gets sent to the shards is internally translated to an
                // aggregation.
                currentOpFilter:
                    (isRemoteShardCurOp ? {
                        "cursor.originatingCommand.aggregate": "currentop_query",
                        "cursor.originatingCommand.pipeline.0.$match.$comment": "currentop_query_mr"
                    }
                                        : {
                                              "command.query.$comment": "currentop_query_mr",
                                              "ns": /^currentop_query.*currentop_query/
                                          }),
            },
            {
                test: function(db) {
                    assert.commandWorked(db.currentop_query.remove(
                        {a: 2, $comment: "currentop_query"}, {collation: {locale: "fr"}}));
                },
                operation: "remove",
                planSummary: "COLLSCAN",
                currentOpFilter: (isLocalMongosCurOp
                                      ? {"command.delete": coll.getName(), "command.ordered": true}
                                      : {
                                            "command.q.$comment": "currentop_query",
                                            "command.collation.locale": "fr"
                                        })
            },
            {
                test: function(db) {
                    assert.commandWorked(
                        db.currentop_query.update({a: 1, $comment: "currentop_query"},
                                                  {$inc: {b: 1}},
                                                  {collation: {locale: "fr"}, multi: true}));
                },
                operation: "update",
                planSummary: "COLLSCAN",
                currentOpFilter: (isLocalMongosCurOp
                                      ? {"command.update": coll.getName(), "command.ordered": true}
                                      : {
                                            "command.q.$comment": "currentop_query",
                                            "command.collation.locale": "fr"
                                        })
            }
        ];

        testList.forEach(confirmCurrentOpContents);

        //
        // Confirm currentOp contains collation for find command.
        //
        confirmCurrentOpContents({
            test: function(db) {
                assert.eq(db.currentop_query.find({a: 1})
                              .comment("currentop_query")
                              .collation({locale: "fr"})
                              .itcount(),
                          1);
            },
            command: "find",
            planSummary: "COLLSCAN",
            currentOpFilter:
                {"command.comment": "currentop_query", "command.collation.locale": "fr"}
        });

        //
        // Confirm currentOp content for the $geoNear aggregation stage.
        //
        dropAndRecreateTestCollection();
        for (let i = 0; i < 10; ++i) {
            assert.commandWorked(coll.insert({a: i, loc: {type: "Point", coordinates: [i, i]}}));
        }
        assert.commandWorked(coll.createIndex({loc: "2dsphere"}));
        confirmCurrentOpContents({
            test: function(db) {
                assert.commandWorked(db.runCommand({
                    aggregate: "currentop_query",
                    cursor: {},
                    pipeline: [{
                        $geoNear: {
                            near: {type: "Point", coordinates: [1, 1]},
                            distanceField: "dist",
                            spherical: true,
                            query: {$comment: "currentop_query"},
                        }
                    }],
                    collation: {locale: "fr"},
                    comment: "currentop_query",
                }));
            },
            planSummary: "GEO_NEAR_2DSPHERE { loc: \"2dsphere\" }",
            currentOpFilter: commandOrOriginatingCommand({
                "aggregate": {$exists: true},
                "pipeline.0.$geoNear.query.$comment": "currentop_query",
                "collation.locale": "fr",
                "comment": "currentop_query",
            },
                                                         isRemoteShardCurOp),
        });

        //
        // Confirm currentOp content for getMore. This case tests command and legacy getMore
        // with originating find and aggregate commands.
        //
        dropAndRecreateTestCollection();
        for (let i = 0; i < 10; ++i) {
            assert.commandWorked(coll.insert({a: i}));
        }

        const originatingCommands = {
            find: {find: "currentop_query", filter: {}, comment: "currentop_query", batchSize: 0},
            aggregate: {
                aggregate: "currentop_query",
                pipeline: [{$match: {}}],
                comment: "currentop_query",
                cursor: {batchSize: 0}
            }
        };

        for (let cmdName in originatingCommands) {
            const cmdObj = originatingCommands[cmdName];
            const cmdRes = testDB.runCommand(cmdObj);
            assert.commandWorked(cmdRes);

            TestData.commandResult = cmdRes;

            // If this is a non-localOps test running via mongoS, then the cursorID we obtained
            // above is the ID of the mongoS cursor, and will not match the IDs of any of the
            // individual shard cursors in the currentOp output. We therefore don't perform an
            // exact match on 'command.getMore', but only verify that the cursor ID is non-zero.
            const filter = {
                "command.getMore":
                    (isRemoteShardCurOp ? {$gt: 0} : TestData.commandResult.cursor.id),
                [`cursor.originatingCommand.${cmdName}`]: {$exists: true},
                "cursor.originatingCommand.comment": "currentop_query"
            };

            confirmCurrentOpContents({
                test: function(db) {
                    const cursor = new DBCommandCursor(db, TestData.commandResult, 5);
                    assert.eq(cursor.itcount(), 10);
                },
                command: "getMore",
                planSummary: "COLLSCAN",
                currentOpFilter: filter,
            });

            delete TestData.commandResult;
        }
    }

    /**
     * Runs a set of tests to verify that currentOp will serialize objects exceeding ~1000 bytes
     * to string when the 'truncateOps' parameter is set.
     */
    function runTruncationTests() {
        dropAndRecreateTestCollection();
        assert.commandWorked(coll.insert({a: 1}));

        // When the currentOp command serializes the query object as a string, individual string
        // values inside it are truncated at 150 characters. To test "total length" truncation
        // we need to pass multiple values, each smaller than 150 bytes.
        TestData.queryFilter = {
            "1": "1".repeat(149),
            "2": "2".repeat(149),
            "3": "3".repeat(149),
            "4": "4".repeat(149),
            "5": "5".repeat(149),
            "6": "6".repeat(149),
            "7": "7".repeat(149),
        };

        var truncatedQueryString = "^\\{ find: \"currentop_query\", filter: \\{ " +
            "1: \"1{149}\", 2: \"2{149}\", 3: \"3{149}\", 4: \"4{149}\", 5: \"5{149}\", " +
            "6: \"6{149}\", 7: \"7+\\.\\.\\.";

        let currentOpFilter;

        // Verify that the currentOp command removes the comment field from the command while
        // truncating the command. Command {find: <coll>, comment: <comment>, filter: {XYZ}} should
        // be represented as {$truncated: "{find: <coll>, filter: {XY...", comment: <comment>}.
        currentOpFilter = {
            "command.$truncated": {$regex: truncatedQueryString},
            "command.comment": "currentop_query"
        };

        confirmCurrentOpContents({
            test: function(db) {
                // We put the 'comment' field before the large 'filter' in the command object so
                // that, when the object is truncated in the currentOp output, we can confirm that
                // the 'comment' field has been removed from the object and promoted to a top-level
                // field.
                assert.commandWorked(db.runCommand({
                    find: "currentop_query",
                    comment: "currentop_query",
                    filter: TestData.queryFilter
                }));
            },
            planSummary: "COLLSCAN",
            currentOpFilter: currentOpFilter
        });

        // Verify that a command without the "comment" field appears as {$truncated: <string>} when
        // truncated by currentOp.
        currentOpFilter = {
            "command.$truncated": {$regex: truncatedQueryString},
            "command.comment": {$exists: false}
        };

        confirmCurrentOpContents({
            test: function(db) {
                assert.commandWorked(
                    db.runCommand({find: "currentop_query", filter: TestData.queryFilter}));
            },
            planSummary: "COLLSCAN",
            currentOpFilter: currentOpFilter
        });

        // Verify that an originatingCommand truncated by currentOp appears as { $truncated:
        // <string>, comment: <string> }.
        const cmdRes = testDB.runCommand({
            find: "currentop_query",
            comment: "currentop_query",
            filter: TestData.queryFilter,
            batchSize: 0
        });
        assert.commandWorked(cmdRes);

        TestData.commandResult = cmdRes;

        currentOpFilter = {
            "command.getMore": (isRemoteShardCurOp ? {$gt: 0} : TestData.commandResult.cursor.id),
            "cursor.originatingCommand.$truncated": {$regex: truncatedQueryString},
            "cursor.originatingCommand.comment": "currentop_query"
        };

        confirmCurrentOpContents({
            test: function(db) {
                var cursor = new DBCommandCursor(db, TestData.commandResult, 5);
                assert.eq(cursor.itcount(), 0);
            },
            planSummary: "COLLSCAN",
            currentOpFilter: currentOpFilter,
        });

        delete TestData.commandResult;

        // Verify that an aggregation truncated by currentOp appears as { $truncated: <string>,
        // comment: <string> } when a comment parameter is present.
        truncatedQueryString =
            "^\\{ aggregate: \"currentop_query\", pipeline: \\[ \\{ \\$match: \\{ " +
            "1: \"1{149}\", 2: \"2{149}\", 3: \"3{149}\", 4: \"4{149}\", 5: \"5{149}\", " +
            "6: \"6{149}\", 7: \"7+\\.\\.\\.";

        currentOpFilter = commandOrOriginatingCommand(
            {"$truncated": {$regex: truncatedQueryString}, "comment": "currentop_query"},
            isRemoteShardCurOp);

        confirmCurrentOpContents({
            test: function(db) {
                assert.commandWorked(db.runCommand({
                    aggregate: "currentop_query",
                    comment: "currentop_query",
                    pipeline: [{$match: TestData.queryFilter}],
                    cursor: {}
                }));
            },
            planSummary: "COLLSCAN",
            currentOpFilter: currentOpFilter,
        });

        delete TestData.queryFilter;
    }
}

function currentOpCommand(inputDB, filter, truncatedOps, localOps) {
    return inputDB.getSiblingDB("admin").runCommand(
        Object.assign({currentOp: true, $truncateOps: truncatedOps}, filter));
}

function currentOpAgg(inputDB, filter, truncatedOps, localOps) {
    return {
        inprog:
            inputDB.getSiblingDB("admin")
                .aggregate([
                    {
                        $currentOp:
                            {localOps: (localOps || false), truncateOps: (truncatedOps || false)}
                    },
                    {$match: filter}
                ])
                .toArray(),
        ok: 1
    };
}

for (let connType of [rsConn, mongosConn]) {
    for (let truncatedOps of [false, true]) {
        for (let localOps of [false, true]) {
            // Run all tests using the $currentOp aggregation stage.
            runTests({
                conn: connType,
                currentOp: currentOpAgg,
                localOps: localOps,
                truncatedOps: truncatedOps
            });
        }
        // Run tests using the currentOp command. The 'localOps' parameter is not supported.
        runTests({
            conn: connType,
            currentOp: currentOpCommand,
            localOps: false,
            truncatedOps: truncatedOps
        });
    }
}

st.stop();
})();

/**
 * Confirms slow currentOp logging does not conflict with applying an oplog batch.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const rst = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
            slowms: 30000,  // Don't log slow operations on secondary.
        },
    ]
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(coll.insert({_id: 'a'}));

const secondary = rst.getSecondary();
const secondaryDB = secondary.getDB(testDB.getName());
assert.commandWorked(secondaryDB.adminCommand({
    configureFailPoint: 'hangAfterCollectionInserts',
    mode: 'alwaysOn',
    data: {
        collectionNS: coll.getFullName(),
        first_id: 'b',
    },
}));

try {
    assert.commandWorked(coll.insert({_id: 'b'}));
    checkLog.containsJson(secondary, 20289);

    jsTestLog('Running currentOp() with slow operation logging.');
    // Lower slowms to make currentOp() log slow operation while the secondary is procesing the
    // commitIndexBuild oplog entry during oplog application.
    // Use admin db on secondary to avoid lock conflict with inserts in test db.
    const secondaryAdminDB = secondaryDB.getSiblingDB('admin');
    const profileResult = assert.commandWorked(secondaryAdminDB.setProfilingLevel(0, {slowms: -1}));
    jsTestLog('Configured profiling to always log slow ops: ' + tojson(profileResult));
    const currentOpResult = assert.commandWorked(secondaryAdminDB.currentOp());
    jsTestLog('currentOp() with slow operation logging: ' + tojson(currentOpResult));
    assert.commandWorked(
        secondaryAdminDB.setProfilingLevel(profileResult.was, {slowms: profileResult.slowms}));
    jsTestLog('Completed currentOp() with slow operation logging.');
} finally {
    assert.commandWorked(
        secondaryDB.adminCommand({configureFailPoint: 'hangAfterCollectionInserts', mode: 'off'}));
}

rst.stopSet();
})();

/**
 * Tests that the time-tracking metrics in the 'transaction' object in currentOp() are being tracked
 * correctly.
 * @tags: [uses_transactions, uses_prepare_transaction]
 */

(function() {
'use strict';
load("jstests/core/txns/libs/prepare_helpers.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const collName = 'currentop_transaction_metrics';
const testDB = rst.getPrimary().getDB('test');
const adminDB = rst.getPrimary().getDB('admin');
testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
assert.commandWorked(testDB[collName].insert({x: 1}, {writeConcern: {w: "majority"}}));

const session = adminDB.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase('test');

session.startTransaction();
// Run a few operations so that the transaction goes through several active/inactive periods.
assert.commandWorked(sessionDB[collName].update({}, {a: 1}));
assert.commandWorked(sessionDB[collName].insert({_id: "insert-1"}));
assert.commandWorked(sessionDB[collName].insert({_id: "insert-2"}));
assert.commandWorked(sessionDB[collName].insert({_id: "insert-3"}));

const transactionFilter = {
    active: false,
    'lsid': {$exists: true},
    'transaction.parameters.txnNumber': {$eq: 0},
    'transaction.parameters.autocommit': {$eq: false},
    'transaction.timePreparedMicros': {$exists: false}
};

let currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: transactionFilter}]).toArray();
assert.eq(currentOp.length, 1);

// Check that the currentOp's transaction subdocument's fields align with our expectations.
let transactionDocument = currentOp[0].transaction;
assert.gte(transactionDocument.timeOpenMicros,
           transactionDocument.timeActiveMicros + transactionDocument.timeInactiveMicros);

// Check that preparing the transaction enables the 'timePreparedMicros' field in currentOp.
const prepareTimestamp = PrepareHelpers.prepareTransaction(session);

const prepareTransactionFilter = {
    active: false,
    'lsid': {$exists: true},
    'transaction.parameters.txnNumber': {$eq: 0},
    'transaction.parameters.autocommit': {$eq: false},
    'transaction.timePreparedMicros': {$exists: true}
};

currentOp = adminDB.aggregate([{$currentOp: {}}, {$match: prepareTransactionFilter}]).toArray();
assert.eq(currentOp.length, 1);

// Check that the currentOp's transaction subdocument's fields align with our expectations.
const prepareTransactionDocument = currentOp[0].transaction;
assert.gte(
    prepareTransactionDocument.timeOpenMicros,
    prepareTransactionDocument.timeActiveMicros + prepareTransactionDocument.timeInactiveMicros);
assert.gte(prepareTransactionDocument.timePreparedMicros, 0);

PrepareHelpers.commitTransaction(session, prepareTimestamp);
session.endSession();

rst.stopSet();
})();

/**
 * Tests that the transaction items in the 'twoPhaseCommitCoordinator' object in currentOp() are
 * being tracked correctly.
 * @tags: [
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */

(function() {
'use strict';
load('jstests/libs/fail_point_util.js');
load('jstests/sharding/libs/sharded_transactions_helpers.js');  // for waitForFailpoint

function curOpAfterFailpoint(failPoint, filter, timesEntered, curOpParams) {
    jsTest.log(`waiting for failpoint '${failPoint.failPointName}' to be entered ${
        timesEntered} time(s).`);
    if (timesEntered > 1) {
        const expectedLog = "Hit " + failPoint.failPointName + " failpoint";
        waitForFailpoint(expectedLog, timesEntered);
    } else {
        failPoint.wait();
    }

    jsTest.log(`Running curOp operation after '${failPoint.failPointName}' failpoint.`);
    let result = adminDB.aggregate([{$currentOp: {}}, {$match: filter}]).toArray();

    jsTest.log(`${result.length} matching curOp entries after '${failPoint.failPointName}':\n${
        tojson(result)}`);

    failPoint.off();

    return result;
}

function enableFailPoints(shard, failPointNames) {
    let failPoints = {};

    failPointNames.forEach(function(failPointName) {
        failPoints[failPointName] = configureFailPoint(shard, failPointName);
    });

    return failPoints;
}

function startTransaction(session, collectionName, insertValue) {
    const dbName = session.getDatabase('test');
    jsTest.log(`Starting a new transaction on ${dbName}.${collectionName}`);
    session.startTransaction();
    // insert into both shards
    assert.commandWorked(dbName[collectionName].insert({_id: -1 * insertValue}));
    assert.commandWorked(dbName[collectionName].insert({_id: insertValue}));

    return [session.getTxnNumber_forTesting(), session.getSessionId()];
}

function commitTxn(st, lsid, txnNumber) {
    let cmd = "db.adminCommand({" +
        "commitTransaction: 1," +
        "lsid: " + tojson(lsid) + "," +
        "txnNumber: NumberLong(" + txnNumber + ")," +
        "stmtId: NumberInt(0)," +
        "autocommit: false," +
        "})";
    cmd = "assert.commandWorked(" + cmd + ");";
    return startParallelShell(cmd, st.s.port);
}

function coordinatorCuropFilter(session, txnNumber) {
    return {
        'twoPhaseCommitCoordinator.lsid.id': session.getSessionId().id,
        'twoPhaseCommitCoordinator.txnNumber': txnNumber,
        'twoPhaseCommitCoordinator.state': {$exists: true},
    };
}

function undefinedToZero(num) {
    return typeof (num) === 'undefined' ? 0 : num;
}

function assertStepDuration(
    expectedStepDurations, currentDuration, lowerBoundExclusive, stepDurationsDoc) {
    let actualValue = stepDurationsDoc[currentDuration];
    if (expectedStepDurations.includes(currentDuration)) {
        assert.gt(
            actualValue,
            lowerBoundExclusive,
            `expected ${currentDuration} to be > ${lowerBoundExclusive}, got '${actualValue}'`);
    } else {
        assert.eq(typeof (actualValue),
                  'undefined',
                  `expected ${currentDuration} to be undefined, got '${actualValue}'`);
    }
}

function assertCuropFields(coordinator,
                           commitStartCutoff,
                           expectedState,
                           expectedStepDurations,
                           expectedCommitDecision,
                           expectedNumParticipants,
                           result) {
    // mongos broadcasts currentOp to all the shards and puts each shard’s
    // response in a subobject under the shard’s name
    let expectedShardName = coordinator.name.substr(0, coordinator.name.indexOf("/"));
    assert.eq(result.shard, expectedShardName);
    assert.eq("transaction coordinator", result.desc);

    let twoPhaseCommitCoordinatorDoc = result.twoPhaseCommitCoordinator;
    assert.eq(expectedState, twoPhaseCommitCoordinatorDoc.state);
    assert.eq(false, twoPhaseCommitCoordinatorDoc.hasRecoveredFromFailover);
    if (expectedNumParticipants) {
        assert.eq(expectedNumParticipants, twoPhaseCommitCoordinatorDoc.numParticipants);
    }
    if (expectedCommitDecision) {
        assert.eq(twoPhaseCommitCoordinatorDoc.commitDecision.decision, expectedCommitDecision);
    }
    assert.gte(twoPhaseCommitCoordinatorDoc.commitStartTime, commitStartCutoff);
    assert.gt(Date.parse(twoPhaseCommitCoordinatorDoc.deadline), commitStartCutoff);

    let stepDurationsDoc = twoPhaseCommitCoordinatorDoc.stepDurations;
    assertStepDuration(expectedStepDurations, 'writingParticipantListMicros', 0, stepDurationsDoc);
    assertStepDuration(expectedStepDurations, 'waitingForVotesMicros', 0, stepDurationsDoc);
    assertStepDuration(expectedStepDurations, 'writingDecisionMicros', 0, stepDurationsDoc);

    let durationSum = undefinedToZero(stepDurationsDoc.writingParticipantListMicros) +
        undefinedToZero(stepDurationsDoc.waitingForVotesMicros) +
        undefinedToZero(stepDurationsDoc.writingDecisionMicros);

    // make sure totalCommitDuration is at least as big as all the other durations.
    assertStepDuration(
        expectedStepDurations, 'totalCommitDurationMicros', durationSum - 1, stepDurationsDoc);

    let expectedClientFields = ['host', 'client_s', 'connectionId', 'appName', 'clientMetadata'];
    assert.hasFields(result, expectedClientFields);
}

const numShards = 2;
const dbName = "test";
const collectionName = 'currentop_two_phase';
const ns = dbName + "." + collectionName;
const authUser = {
    user: "user",
    pwd: "password",
    roles: jsTest.adminUserRoles
};

function setupCluster(withAuth) {
    let defaultOpts = {rs: {nodes: 1}, shards: numShards, config: 1};
    let authOpts = {other: {keyFile: 'jstests/libs/key1'}};

    let opts = defaultOpts;
    if (withAuth) {
        opts = Object.merge(opts, authOpts);
    }

    const st = new ShardingTest(opts);
    const adminDB = st.s.getDB('admin');
    const coordinator = st.shard0;
    const participant = st.shard1;

    if (withAuth) {
        adminDB.createUser(authUser);
        assert(adminDB.auth(authUser.user, authUser.pwd));
    }

    assert.commandWorked(adminDB.adminCommand({enableSharding: dbName}));
    assert.commandWorked(adminDB.adminCommand({movePrimary: dbName, to: coordinator.shardName}));
    assert.commandWorked(adminDB.adminCommand({shardCollection: ns, key: {_id: 1}}));
    assert.commandWorked(adminDB.adminCommand({split: ns, middle: {_id: 0}}));
    assert.commandWorked(
        adminDB.adminCommand({moveChunk: ns, find: {_id: 0}, to: participant.shardName}));
    // this find is to ensure all the shards' filtering metadata are up to date
    assert.commandWorked(st.s.getDB(dbName).runCommand({find: collectionName}));
    return [st, adminDB, coordinator, participant];
}

let [st, adminDB, coordinator, participant] = setupCluster(false);

(function() {
jsTest.log("Check curop coordinator state when idle");
let session = adminDB.getMongo().startSession();
const commitStartCutoff = Date.now();
let [txnNumber, lsid] = startTransaction(session, collectionName, 1);
let expectedState = "inactive";
let filter = coordinatorCuropFilter(session, txnNumber);

let results =
    adminDB.aggregate([{$currentOp: {"idleSessions": false}}, {$match: filter}]).toArray();
jsTest.log(`Curop result(s): ${tojson(results)}`);
assert.eq(0, results.length);

results = adminDB.aggregate([{$currentOp: {"idleSessions": true}}, {$match: filter}]).toArray();
jsTest.log(`Curop result(s): ${tojson(results)}`);
assert.eq(1, results.length);
assertCuropFields(coordinator, commitStartCutoff, expectedState, [], null, 0, results[0]);
})();

(function() {
jsTest.log("Check curop coordinator state while transaction is executing.");
let session = adminDB.getMongo().startSession();
const commitStartCutoff = Date.now();
let [txnNumber, lsid] = startTransaction(session, collectionName, 2);

let failPointStates = {
    'hangBeforeWritingParticipantList': {
        'expectNumFailPoints': 1,
        'expectedState': 'writingParticipantList',
        'expectedStepDurations': ['writingParticipantListMicros', 'totalCommitDurationMicros'],
        'expectedCommitDecision': null,
        'expectedNumParticipants': numShards,
    },
    'hangBeforeSendingPrepare': {
        'expectNumFailPoints': 2,
        'expectedState': 'waitingForVotes',
        'expectedStepDurations':
            ['writingParticipantListMicros', 'waitingForVotesMicros', 'totalCommitDurationMicros'],
        'expectedCommitDecision': null,
        'expectedNumParticipants': numShards,
    },
    'hangBeforeWaitingForDecisionWriteConcern': {
        'expectNumFailPoints': 1,
        'expectedState': 'writingDecision',
        'expectedStepDurations': [
            'writingParticipantListMicros',
            'waitingForVotesMicros',
            'writingDecisionMicros',
            'totalCommitDurationMicros'
        ],
        'expectedCommitDecision': 'commit',
        'expectedNumParticipants': numShards,
    },
    'hangBeforeSendingCommit': {
        'expectNumFailPoints': 2,
        'expectedState': 'waitingForDecisionAck',
        'expectedStepDurations': [
            'writingParticipantListMicros',
            'waitingForVotesMicros',
            'writingDecisionMicros',
            'waitingForDecisionAcksMicros',
            'totalCommitDurationMicros'
        ],
        'expectedCommitDecision': 'commit',
        'expectedNumParticipants': numShards,
    },
    'hangBeforeDeletingCoordinatorDoc': {
        'expectNumFailPoints': 1,
        'expectedState': 'deletingCoordinatorDoc',
        'expectedStepDurations': [
            'writingParticipantListMicros',
            'waitingForVotesMicros',
            'writingDecisionMicros',
            'waitingForDecisionAcksMicros',
            'deletingCoordinatorDocMicros',
            'totalCommitDurationMicros'
        ],
        'expectedCommitDecision': 'commit',
        'expectedNumParticipants': numShards,
    }
};

// Not using 'Object.keys(failPointStates)' since lexical order is not guaranteed
let failPointNames = [
    'hangBeforeWritingParticipantList',
    'hangBeforeSendingPrepare',
    'hangBeforeWaitingForDecisionWriteConcern',
    'hangBeforeSendingCommit',
    'hangBeforeDeletingCoordinatorDoc'
];
let failPoints = enableFailPoints(coordinator, failPointNames);

let commitJoin = commitTxn(st, lsid, txnNumber);

failPointNames.forEach(function(failPointName) {
    let expectNumFailPoints = failPointStates[failPointName].expectNumFailPoints;
    let expectedState = failPointStates[failPointName].expectedState;
    let expectedStepDurations = failPointStates[failPointName].expectedStepDurations;
    let expectedCommitDecision = failPointStates[failPointName].commitDecision;
    let expectedNumParticipants = failPointStates[failPointName].expectedNumParticipants;

    let filter = coordinatorCuropFilter(session, txnNumber, expectedState);
    let results = curOpAfterFailpoint(
        failPoints[failPointName], filter, expectNumFailPoints, {idleSessions: true});

    assert.eq(1, results.length);
    assertCuropFields(coordinator,
                      commitStartCutoff,
                      expectedState,
                      expectedStepDurations,
                      expectedCommitDecision,
                      expectedNumParticipants,
                      results[0]);
});

commitJoin();
})();
st.stop();

(function() {
[st, adminDB, coordinator, participant] = setupCluster(true);
jsTest.log("Check curop allUsers flag with auth enabled");
let session = adminDB.getMongo().startSession();
const commitStartCutoff = Date.now();
let [txnNumber, _] = startTransaction(session, collectionName, 1);
let filter = coordinatorCuropFilter(session, txnNumber);

let results = adminDB.aggregate([{$currentOp: {'allUsers': false}}, {$match: filter}]).toArray();
jsTest.log(`Curop result(s): ${tojson(results)}`);
assert.eq(0, results.length);

results = adminDB.aggregate([{$currentOp: {'allUsers': true}}, {$match: filter}]).toArray();
jsTest.log(`Curop result(s): ${tojson(results)}`);
assert.eq(1, results.length);
assertCuropFields(coordinator, commitStartCutoff, 'inactive', [], null, null, results[0]);

adminDB.logout();
})();

st.stop();
})();

/**
 * Test the cursor server status "moreThanOneBatch" and "totalOpened" metric on mongoS.
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

const st = new ShardingTest({shards: 2});
st.stopBalancer();

const db = st.s.getDB("test");
const coll = db.getCollection(jsTestName());

function getNumberOfCursorsOpened() {
    return db.adminCommand({serverStatus: 1}).metrics.mongos.cursor.totalOpened;
}

function getNumberOfCursorsMoreThanOneBatch() {
    return db.adminCommand({serverStatus: 1}).metrics.mongos.cursor.moreThanOneBatch;
}

coll.drop();
assert.commandWorked(db.adminCommand({enableSharding: db.getName()}));
st.ensurePrimaryShard(db.getName(), st.shard0.shardName);
db.adminCommand({shardCollection: coll.getFullName(), key: {_id: 1}});
assert.commandWorked(db.adminCommand({split: coll.getFullName(), middle: {_id: 0}}));

jsTestLog("Inserting documents into the collection: " + jsTestName());
for (let i = -4; i < 4; i++) {
    assert.commandWorked(coll.insert({_id: i, a: 4 * i, b: "hello"}));
}

const initialNumCursorsOpened = getNumberOfCursorsOpened();
const initialNumCursorsMoreThanOneBatch = getNumberOfCursorsMoreThanOneBatch();
jsTestLog("Cursors opened initially: " + initialNumCursorsOpened);
jsTestLog("Cursors with more than one batch initially: " + initialNumCursorsMoreThanOneBatch);

jsTestLog("Running find.");
let cmdRes = assert.commandWorked(db.runCommand({find: coll.getName(), batchSize: 2}));
let cursorId = cmdRes.cursor.id;
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 1, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 0, cmdRes);

jsTestLog("Killing cursor with cursorId: " + cursorId);
assert.commandWorked(db.runCommand({killCursors: coll.getName(), cursors: [cursorId]}));
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 1, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 0, cmdRes);

jsTestLog("Running second find, this time will run getMore.");
cmdRes = assert.commandWorked(db.runCommand({find: coll.getName(), batchSize: 2}));
cursorId = cmdRes.cursor.id;
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 2, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 0, cmdRes);

jsTestLog("Running getMore for cursorId: " + cursorId);
cmdRes = assert.commandWorked(
    db.runCommand({getMore: cursorId, collection: coll.getName(), batchSize: 2}));
// Expect the number of cursors with more than one batch to not have changed.
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 2, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 0, cmdRes);

jsTestLog("Killing cursor with cursorId: " + cursorId);
assert.commandWorked(db.runCommand({killCursors: coll.getName(), cursors: [cursorId]}));
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 2, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 1, cmdRes);

jsTestLog("Running aggregate command.");
cmdRes = assert.commandWorked(
    db.runCommand({aggregate: coll.getName(), pipeline: [], cursor: {batchSize: 2}}));
cursorId = cmdRes.cursor.id;
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 3, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 1, cmdRes);

jsTestLog("Running getMore on aggregate cursor: " + cursorId);
cmdRes = assert.commandWorked(
    db.runCommand({getMore: cursorId, collection: coll.getName(), batchSize: 2}));
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 3, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 1, cmdRes);

// Use a batchSize that's greater than the number of documents and therefore exhaust the cursor.
jsTestLog("Exhausting cursor with cursorId: " + cursorId);
cmdRes = assert.commandWorked(
    db.runCommand({getMore: cursorId, collection: coll.getName(), batchSize: 20}));
assert.eq(getNumberOfCursorsOpened() - initialNumCursorsOpened, 3, cmdRes);
assert.eq(getNumberOfCursorsMoreThanOneBatch() - initialNumCursorsMoreThanOneBatch, 2, cmdRes);

st.stop();
})();

/**
 * Tests for the Graph#findCycle() method.
 */
(function() {
'use strict';

load('jstests/libs/cycle_detection.js');  // for Graph

(function testLinearChainHasNoCycle() {
    const graph = new Graph();
    graph.addEdge('A', 'B');
    graph.addEdge('B', 'C');
    graph.addEdge('C', 'D');

    assert.eq([], graph.findCycle());
})();

(function testGraphWithoutCycleButCommonAncestor() {
    const graph = new Graph();
    graph.addEdge('A', 'B');
    graph.addEdge('A', 'C');
    graph.addEdge('B', 'D');
    graph.addEdge('C', 'D');

    assert.eq([], graph.findCycle());
})();

(function testEmptyGraphHasNoCycle() {
    const graph = new Graph();
    assert.eq([], graph.findCycle());
})();

(function testGraphWithAllNodesInCycle() {
    const graph = new Graph();
    graph.addEdge(1, 2);
    graph.addEdge(2, 3);
    graph.addEdge(3, 4);
    graph.addEdge(4, 5);
    graph.addEdge(5, 1);

    assert.eq([1, 2, 3, 4, 5, 1], graph.findCycle());
})();

(function testGraphWithSomeNodesNotInCycle() {
    const graph = new Graph();
    graph.addEdge(1, 2);
    graph.addEdge(2, 3);
    graph.addEdge(3, 4);
    graph.addEdge(4, 5);
    graph.addEdge(5, 3);

    assert.eq([3, 4, 5, 3], graph.findCycle());
})();

(function testGraphWithSelfLoopConsideredCycle() {
    const graph = new Graph();
    graph.addEdge(0, 0);
    assert.eq([0, 0], graph.findCycle());
})();

(function testGraphUsesNonReferentialEquality() {
    const w = {a: new NumberInt(1)};
    const x = {a: new NumberInt(1)};
    const y = {a: new NumberLong(1)};
    const z = {a: 1};

    let graph = new Graph();
    graph.addEdge(w, x);
    assert.eq([w, x], graph.findCycle());

    graph = new Graph();
    graph.addEdge(w, y);
    assert.eq([], graph.findCycle());

    graph = new Graph();
    graph.addEdge(w, z);
    assert.eq([w, z], graph.findCycle());
})();

(function testGraphMinimizesCycleUsingNonReferentialEquality() {
    const graph = new Graph();
    graph.addEdge({a: 1}, {a: 2});
    graph.addEdge({a: 2}, {a: 3});
    graph.addEdge({a: 3}, {a: 4});
    graph.addEdge({a: 4}, {a: 5});
    graph.addEdge({a: 5}, {a: 3});

    assert.eq([{a: 3}, {a: 4}, {a: 5}, {a: 3}], graph.findCycle());
})();
})();

/**
 * Verifies that the data consistency checks work against the variety of cluster types we use in our
 * testing.
 *
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */
load("jstests/libs/logv2_helpers.js");

// The global 'db' variable is used by the data consistency hooks.
var db;

(function() {
"use strict";

// We skip doing the data consistency checks while terminating the cluster because they conflict
// with the counts of the number of times the "dbhash" and "validate" commands are run.
TestData.skipCollectionAndIndexValidation = true;
TestData.skipCheckDBHashes = true;

function makePatternForDBHash(dbName) {
    if (isJsonLogNoConn()) {
        return new RegExp(
            `Slow query.*"ns":"${dbName}\\.\\$cmd","appName":"MongoDB Shell","command":{"db[Hh]ash`,
            "g");
    }
    return new RegExp(
        "COMMAND.*command " + dbName + "\\.\\$cmd appName: \"MongoDB Shell\" command: db[Hh]ash",
        "g");
}

function makePatternForValidate(dbName, collName) {
    if (isJsonLogNoConn()) {
        return new RegExp(
            `Slow query.*"ns":"${
                dbName}\\.\\$cmd","appName":"MongoDB Shell","command":{"validate":"${collName}"`,
            "g");
    }
    return new RegExp("COMMAND.*command " + dbName +
                          "\\.\\$cmd appName: \"MongoDB Shell\" command: validate { validate: \"" +
                          collName + "\"",
                      "g");
}

function countMatches(pattern, output) {
    assert(pattern.global, "the 'g' flag must be used to find all matches");

    let numMatches = 0;
    while (pattern.exec(output) !== null) {
        ++numMatches;
    }
    return numMatches;
}

function runDataConsistencyChecks(testCase) {
    db = testCase.conn.getDB("test");
    try {
        clearRawMongoProgramOutput();

        load("jstests/hooks/run_check_repl_dbhash.js");
        load("jstests/hooks/run_validate_collections.js");

        // We terminate the processes to ensure that the next call to rawMongoProgramOutput()
        // will return all of their output.
        testCase.teardown();
        return rawMongoProgramOutput();
    } finally {
        db = undefined;
    }
}

(function testReplicaSetWithVotingSecondaries() {
    const numNodes = 2;
    const rst = new ReplSetTest({
        nodes: numNodes,
        nodeOptions: {
            setParameter: {logComponentVerbosity: tojson({command: 1})},
        }
    });
    rst.startSet();
    rst.initiateWithNodeZeroAsPrimary();

    // Insert a document so the "dbhash" and "validate" commands have some actual work to do.
    assert.commandWorked(rst.nodes[0].getDB("test").mycoll.insert({}));
    const output = runDataConsistencyChecks({conn: rst.nodes[0], teardown: () => rst.stopSet()});

    let pattern = makePatternForDBHash("test");
    assert.eq(numNodes,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from each node in the log output");

    pattern = makePatternForValidate("test", "mycoll");
    assert.eq(numNodes,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from each node in the log output");
})();

(function testReplicaSetWithNonVotingSecondaries() {
    const numNodes = 2;
    const rst = new ReplSetTest({
        nodes: numNodes,
        nodeOptions: {
            setParameter: {logComponentVerbosity: tojson({command: 1})},
        }
    });
    rst.startSet();

    const replSetConfig = rst.getReplSetConfig();
    for (let i = 1; i < numNodes; ++i) {
        replSetConfig.members[i].priority = 0;
        replSetConfig.members[i].votes = 0;
    }
    rst.initiate(replSetConfig);

    // Insert a document so the "dbhash" and "validate" commands have some actual work to do.
    assert.commandWorked(rst.nodes[0].getDB("test").mycoll.insert({}));
    const output = runDataConsistencyChecks({conn: rst.nodes[0], teardown: () => rst.stopSet()});

    let pattern = makePatternForDBHash("test");
    assert.eq(numNodes,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from each node in the log output");

    pattern = makePatternForValidate("test", "mycoll");
    assert.eq(numNodes,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from each node in the log output");
})();

(function testShardedClusterWithOneNodeCSRS() {
    const st = new ShardingTest({
        mongos: 1,
        config: 1,
        configOptions: {
            setParameter: {logComponentVerbosity: tojson({command: 1})},
        },
        shards: 1
    });

    // We shard a collection in order to guarantee that at least one collection on the "config"
    // database exists for when we go to run the data consistency checks against the CSRS.
    st.shardColl(st.s.getDB("test").mycoll, {_id: 1}, false);

    const output = runDataConsistencyChecks({conn: st.s, teardown: () => st.stop()});

    let pattern = makePatternForDBHash("config");
    assert.eq(0,
              countMatches(pattern, output),
              "expected not to find " + tojson(pattern) + " in the log output for 1-node CSRS");

    // The choice of using the "config.collections" collection here is mostly arbitrary as the
    // "config.databases" and "config.chunks" collections are also implicitly created as part of
    // sharding a collection.
    pattern = makePatternForValidate("config", "collections");
    assert.eq(1,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " in the log output for 1-node CSRS");
})();

(function testShardedCluster() {
    const st = new ShardingTest({
        mongos: 1,
        config: 3,
        configOptions: {
            setParameter: {logComponentVerbosity: tojson({command: 1})},
        },
        shards: 1,
        rs: {nodes: 2},
        rsOptions: {
            setParameter: {logComponentVerbosity: tojson({command: 1})},
        }
    });

    // We shard a collection in order to guarantee that at least one collection on the "config"
    // database exists for when we go to run the data consistency checks against the CSRS.
    st.shardColl(st.s.getDB("test").mycoll, {_id: 1}, false);

    // Insert a document so the "dbhash" and "validate" commands have some actual work to do on
    // the replica set shard.
    assert.commandWorked(st.s.getDB("test").mycoll.insert({_id: 0}));
    const output = runDataConsistencyChecks({conn: st.s, teardown: () => st.stop()});

    // The "config" database exists on both the CSRS and the replica set shards due to the
    // "config.transactions" collection.
    let pattern = makePatternForDBHash("config");
    assert.eq(5,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) +
                  " from each CSRS node and each replica set shard node in the log output");

    // The choice of using the "config.collections" collection here is mostly arbitrary as the
    // "config.databases" and "config.chunks" collections are also implicitly created as part of
    // sharding a collection.
    pattern = makePatternForValidate("config", "collections");
    assert.eq(3,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from each CSRS node in the log output");

    pattern = makePatternForDBHash("test");
    assert.eq(2,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) +
                  " from each replica set shard node in the log output");

    pattern = makePatternForValidate("test", "mycoll");
    assert.eq(2,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) +
                  " from each replica set shard node in the log output");
})();
})();

/**
 * Tests that the dbHash command separately lists the names of capped collections on the database.
 *
 * @tags: [requires_replication, requires_capped]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const db = rst.getPrimary().getDB("test");

// We create a capped collection as well as a non-capped collection and verify that the "capped"
// field in the dbHash command response only lists the capped one.
assert.commandWorked(db.runCommand({create: "noncapped"}));
assert.commandWorked(db.runCommand({create: "capped", capped: true, size: 4096}));

let res = assert.commandWorked(db.runCommand({dbHash: 1}));
assert.eq(["capped", "noncapped"], Object.keys(res.collections).sort());
assert.eq(["capped"], res.capped);

// If the capped collection is excluded from the list of collections to md5sum, then it won't
// appear in the "capped" field either.
res = assert.commandWorked(db.runCommand({dbHash: 1, collections: ["noncapped"]}));
assert.eq([], res.capped);

rst.stopSet();
})();

// The map reduce command is deprecated in 5.0.
//
// In this test, we run the map reduce command multiple times.
// We want to make sure that the deprecation warning message is only logged once despite
// the multiple invocations in an effort to not clutter the dev's console.
// More specifically, we expect to only log 1/127 of mapReduce() events.

(function() {
"use strict";
load("jstests/libs/log.js");  // For findMatchingLogLine, findMatchingLogLines

jsTest.log('Test standalone');
const caseInsensitive = {
    collation: {locale: "simple", strength: 2}
};
const standalone = MongoRunner.runMongod({});
const dbName = 'test';
const collName = "test_map_reduce_command_deprecation_messaging";
const db = standalone.getDB(dbName);
const coll = db.getCollection(collName);
const fieldMatcher = {
    msg:
        "The map reduce command is deprecated. For more information, see https://docs.mongodb.com/manual/core/map-reduce/"
};

function mapFunc() {
    emit(this.cust_id, this.amount);
}
function reduceFunc(key, values) {
    return Array.sum(values);
}

coll.drop();
assert.commandWorked(coll.insert({cust_id: "A", amount: 100, status: "B"}));
assert.commandWorked(coll.insert({cust_id: "A", amount: 200, status: "B"}));
assert.commandWorked(coll.insert({cust_id: "B", amount: 50, status: "B"}));

// Assert that deprecation msg is not logged before map reduce command is even run.
var globalLogs = db.adminCommand({getLog: 'global'});
var matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 0, matchingLogLines);

assert.commandWorked(db.runCommand(
    {mapReduce: collName, map: mapFunc, reduce: reduceFunc, query: {b: 2}, out: "order_totals"}));

assert.commandWorked(coll.insert({cust_id: "B", amount: 50, status: "B"}));

assert.commandWorked(db.runCommand(
    {mapReduce: collName, map: mapFunc, reduce: reduceFunc, query: {b: 2}, out: "order_totals"}));

assert.commandWorked(coll.insert({cust_id: "A", amount: 200, status: "B"}));

assert.commandWorked(db.runCommand(
    {mapReduce: collName, map: mapFunc, reduce: reduceFunc, query: {"B": 2}, out: "order_totals"}));

// Now that we have ran map reduce command, make sure the deprecation message is logged once.
globalLogs = db.adminCommand({getLog: 'global'});
matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 1, matchingLogLines);
MongoRunner.stopMongod(standalone);

// The tests below this comment connect through mongos and shard a collection (creating replica
// sets). This if stanza assures that we skip the portion below if testing a build variant with
// --nojournal and WiredTiger, as that variant will always fail when using replica sets.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

jsTest.log('Test cluster');

const st = new ShardingTest({shards: 2, mongos: 1});

const session = st.s.getDB("test").getMongo().startSession();
const mongosDB = session.getDatabase("test");
const mongosColl = mongosDB.testing;

mongosColl.drop();
assert.commandWorked(mongosDB.createCollection(mongosColl.getName(), caseInsensitive));

assert.commandWorked(mongosColl.insert({cust_id: "A", amount: 100, status: "B"}));
assert.commandWorked(mongosColl.insert({cust_id: "A", amount: 200, status: "B"}));
assert.commandWorked(mongosColl.insert({cust_id: "B", amount: 50, status: "B"}));
assert.commandWorked(mongosColl.insert({cust_id: "A", amount: 10, status: "B"}));
assert.commandWorked(mongosColl.insert({cust_id: "A", amount: 20, status: "B"}));
assert.commandWorked(mongosColl.insert({cust_id: "B", amount: 5, status: "B"}));

assert.commandWorked(st.s0.adminCommand({enableSharding: mongosDB.getName()}));
st.ensurePrimaryShard(db.getName(), st.shard0.shardName);
assert.commandWorked(
    st.s0.adminCommand({shardCollection: mongosColl.getFullName(), key: {_id: 1}}));

// Assert that deprecation msg is not logged before map reduce command is even run.
globalLogs = mongosDB.adminCommand({getLog: 'global'});
matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 0, matchingLogLines);

// Check the logs of the primary shard of the mongos.
globalLogs = st.shard0.getDB("test").adminCommand({getLog: 'global'});
matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 0, matchingLogLines);

assert.commandWorked(mongosDB.runCommand({
    mapReduce: mongosColl.getName(),
    map: mapFunc,
    reduce: reduceFunc,
    query: {b: 2},
    out: "order_totals"
}));

assert.commandWorked(mongosColl.insert({cust_id: "B", amount: 50, status: "B"}));

assert.commandWorked(mongosDB.runCommand({
    mapReduce: mongosColl.getName(),
    map: mapFunc,
    reduce: reduceFunc,
    query: {b: 2},
    out: "order_totals"
}));

assert.commandWorked(mongosColl.insert({cust_id: "A", amount: 200, status: "B"}));

assert.commandWorked(mongosDB.runCommand({
    mapReduce: mongosColl.getName(),
    map: mapFunc,
    reduce: reduceFunc,
    query: {"B": 2},
    out: "order_totals"
}));

// Now that we have ran map reduce command, make sure the deprecation message is logged once.
globalLogs = mongosDB.adminCommand({getLog: 'global'});
matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 1, matchingLogLines);

// Check the logs of the primary shard of the mongos.
globalLogs = st.shard0.getDB("test").adminCommand({getLog: 'global'});
matchingLogLines = [...findMatchingLogLines(globalLogs.log, fieldMatcher)];
assert.eq(matchingLogLines.length, 0, matchingLogLines);

st.stop();
})();

/**
 * Test that $setWindowFields behaves deterministically if
 * internalQueryAppendIdToSetWindowFieldsSort is enabled.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();

const dbName = jsTestName();
const db = conn.getDB(dbName);
assert.commandWorked(db.dropDatabase());
const collA = db.getCollection('a');
collA.createIndex({val: 1, a: 1});
let i = 0;
for (i = 0; i < 3; i++) {
    collA.insert({_id: i, val: 1, a: i});
}
const collB = db.getCollection('b');
collB.createIndex({val: 1, b: 1});
for (i = 0; i < 3; i++) {
    collB.insert({_id: i, val: 1, b: (3 - i)});
}

// This ensures that if the query knob is turned off (which it is by default) we get the results in
// a and b order respectively.
let resultA = collA
                  .aggregate([
                      {$setWindowFields: {sortBy: {val: 1}, output: {ids: {$push: "$_id"}}}},
                      {$limit: 1},
                      {$project: {_id: 0, ids: 1}}
                  ])
                  .toArray()[0];
let resultB = collB
                  .aggregate([
                      {$setWindowFields: {sortBy: {val: 1}, output: {ids: {$push: "$_id"}}}},
                      {$limit: 1},
                      {$project: {_id: 0, ids: 1}}
                  ])
                  .toArray()[0];

assert.eq(resultA["ids"], [0, 1, 2]);
assert.eq(resultB["ids"], [2, 1, 0]);

assert.commandWorked(
    db.adminCommand({setParameter: 1, internalQueryAppendIdToSetWindowFieldsSort: true}));

// Because of the index resultA's ids array should be in a order and resultB's ids should be in b
// order unless the query knob is working properly.
resultA = collA
              .aggregate([
                  {$setWindowFields: {sortBy: {val: 1}, output: {ids: {$push: "$_id"}}}},
                  {$limit: 1},
                  {$project: {_id: 0, ids: 1}}
              ])
              .toArray()[0];
resultB = collB
              .aggregate([
                  {$setWindowFields: {sortBy: {val: 1}, output: {ids: {$push: "$_id"}}}},
                  {$limit: 1},
                  {$project: {_id: 0, ids: 1}}
              ])
              .toArray()[0];

// This assertion ensures that the results are the same.
assert.eq(resultA["ids"], resultB["ids"]);

MongoRunner.stopMongod(conn);
})();

(function() {
const emrcFalseConn =
    MongoRunner.runMongod({storageEngine: "devnull", enableMajorityReadConcern: false});
assert(!emrcFalseConn);
var logContents = rawMongoProgramOutput();
assert(logContents.indexOf("enableMajorityReadConcern:false is no longer supported") > 0);

// Even though enableMajorityReadConcern: true is the default, the server internally changes
// this value to false when running with the devnull storage engine.
const emrcDefaultConn = MongoRunner.runMongod({storageEngine: "devnull"});
db = emrcDefaultConn.getDB("test");

res = db.foo.insert({x: 1});
assert.eq(1, res.nInserted, tojson(res));

// Skip collection validation during stopMongod if invalid storage engine.
TestData.skipCollectionAndIndexValidation = true;

MongoRunner.stopMongod(emrcDefaultConn);
}());

/**
 * Tests the DataConsistencyChecker.getDiff() function can be used to compare the contents between
 * different collections.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const dbName = "diff_different_collections_test";
const collName1 = "coll_one";
const collName2 = "coll_two";

const primaryDB = rst.getPrimary().getDB(dbName);

const matchingDocs = Array.from({length: 100}, (_, i) => ({_id: i, num: i * 2}));
assert.commandWorked(primaryDB[collName1].insert(matchingDocs));
assert.commandWorked(primaryDB[collName2].insert(matchingDocs));

let diff = DataConsistencyChecker.getDiff(primaryDB[collName1].find().sort({_id: 1}),
                                          primaryDB[collName2].find().sort({_id: 1}));

assert.eq(diff, {docsWithDifferentContents: [], docsMissingOnFirst: [], docsMissingOnSecond: []});

const expectedMissingOnSecond = [{_id: 30.2, num: -1}, {_id: 70.4, num: -2}];
const expectedMissingOnFirst = [{_id: 10, num: 20}, {_id: 50, num: 100}];

assert.commandWorked(primaryDB[collName1].insert(expectedMissingOnSecond));
assert.commandWorked(primaryDB[collName1].remove(
    {_id: {$in: expectedMissingOnFirst.map(doc => doc._id)}}, {justOne: false}));
assert.commandWorked(
    primaryDB[collName1].update({_id: {$in: [40, 90]}}, {$set: {extra: "yes"}}, {multi: true}));

// Type fidelity is expected to be preserved by replication so intentionally test comparisons of
// distinct but equivalent BSON types.
assert.commandWorked(primaryDB[collName1].update({_id: 2}, {$set: {num: NumberLong(4)}}));

diff = DataConsistencyChecker.getDiff(primaryDB[collName1].find().sort({_id: 1}),
                                      primaryDB[collName2].find().sort({_id: 1}));

assert.eq(diff,
          {
              docsWithDifferentContents: [
                  {first: {_id: 2, num: NumberLong(4)}, second: {_id: 2, num: 4}},
                  {first: {_id: 40, num: 80, extra: "yes"}, second: {_id: 40, num: 80}},
                  {first: {_id: 90, num: 180, extra: "yes"}, second: {_id: 90, num: 180}},
              ],
              docsMissingOnFirst: expectedMissingOnFirst,
              docsMissingOnSecond: expectedMissingOnSecond
          },
          "actual mismatch between collections differed");

// It is also possible to compare the contents of different collections across different servers.
rst.awaitReplication();
const secondaryDB = rst.getSecondary().getDB(dbName);

diff = DataConsistencyChecker.getDiff(primaryDB[collName1].find().sort({_id: 1}),
                                      secondaryDB[collName2].find().sort({_id: 1}));

assert.eq(diff,
          {
              docsWithDifferentContents: [
                  {first: {_id: 2, num: NumberLong(4)}, second: {_id: 2, num: 4}},
                  {first: {_id: 40, num: 80, extra: "yes"}, second: {_id: 40, num: 80}},
                  {first: {_id: 90, num: 180, extra: "yes"}, second: {_id: 90, num: 180}},
              ],
              docsMissingOnFirst: expectedMissingOnFirst,
              docsMissingOnSecond: expectedMissingOnSecond
          },
          "actual mismatch between servers differed");

rst.stopSet();
})();

/**
 * Tests the DataConsistencyChecker#getCollectionDiffUsingSessions() method for comparing the
 * contents between a primary and secondary server.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const dbName = "diff_using_session_test";
const collName = "mycoll";

const primaryDB = rst.getPrimary().startSession().getDatabase(dbName);
const secondaryDB = rst.getSecondary().startSession().getDatabase(dbName);

// The default WC is majority and rsSyncApplyStop failpoint will prevent satisfying any majority
// writes.
assert.commandWorked(rst.getPrimary().adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

assert.commandWorked(primaryDB[collName].insert(
    Array.from({length: 100}, (_, i) => ({_id: i, num: i * 2})), {writeConcern: {w: 2}}));

// There should be no missing or mismatched documents after having waited for replication.
let diff = DataConsistencyChecker.getCollectionDiffUsingSessions(
    primaryDB.getSession(), secondaryDB.getSession(), dbName, collName);

assert.eq(diff, {docsWithDifferentContents: [], docsMissingOnSource: [], docsMissingOnSyncing: []});

// We pause replication on the secondary to intentionally cause the contents between the primary and
// the secondary to differ.
assert.commandWorked(
    secondaryDB.adminCommand({configureFailPoint: "rsSyncApplyStop", mode: "alwaysOn"}));

const expectedMissingOnSecondary = [{_id: 30.2, num: -1}, {_id: 70.4, num: -2}];
const expectedMissingOnPrimary = [{_id: 10, num: 20}, {_id: 50, num: 100}];

assert.commandWorked(primaryDB[collName].insert(expectedMissingOnSecondary));
assert.commandWorked(primaryDB[collName].remove(
    {_id: {$in: expectedMissingOnPrimary.map(doc => doc._id)}}, {justOne: false}));
assert.commandWorked(
    primaryDB[collName].update({_id: {$in: [40, 90]}}, {$set: {extra: "yes"}}, {multi: true}));

// Type fidelity is expected to be preserved by replication so intentionally test comparisons of
// distinct but equivalent BSON types.
assert.commandWorked(primaryDB[collName].update({_id: 2}, {$set: {num: NumberLong(4)}}));

diff = DataConsistencyChecker.getCollectionDiffUsingSessions(
    primaryDB.getSession(), secondaryDB.getSession(), dbName, collName);

assert.eq(diff, {
    docsWithDifferentContents: [
        {sourceNode: {_id: 2, num: NumberLong(4)}, syncingNode: {_id: 2, num: 4}},
        {sourceNode: {_id: 40, num: 80, extra: "yes"}, syncingNode: {_id: 40, num: 80}},
        {sourceNode: {_id: 90, num: 180, extra: "yes"}, syncingNode: {_id: 90, num: 180}},
    ],
    docsMissingOnSource: expectedMissingOnPrimary,
    docsMissingOnSyncing: expectedMissingOnSecondary
});

assert.commandWorked(
    secondaryDB.adminCommand({configureFailPoint: "rsSyncApplyStop", mode: "off"}));

rst.stopSet();
})();

if (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger") {
    var baseDir = "jstests_per_db_and_split_c_and_i";
    var dbpath = MongoRunner.dataPath + baseDir + "/";

    var m = MongoRunner.runMongod(
        {dbpath: dbpath, wiredTigerDirectoryForIndexes: '', directoryperdb: ''});
    db = m.getDB("foo");
    db.bar.insert({x: 1});
    assert.eq(1, db.bar.count());

    db.adminCommand({fsync: 1});

    assert(listFiles(dbpath + "/foo/index").length > 0);
    assert(listFiles(dbpath + "/foo/collection").length > 0);

    MongoRunner.stopMongod(m);

    // Subsequent attempts to start server using same dbpath but different
    // wiredTigerDirectoryForIndexes and directoryperdb options should fail.
    assert.throws(() => MongoRunner.runMongod({dbpath: dbpath, port: m.port, restart: true}));
    assert.throws(() => MongoRunner.runMongod(
                      {dbpath: dbpath, port: m.port, restart: true, directoryperdb: ''}));
    assert.throws(
        () => MongoRunner.runMongod(
            {dbpath: dbpath, port: m.port, restart: true, wiredTigerDirectoryForIndexes: ''}));
}

/**
 * Tests that a mongod started with --directoryperdb will write data for database x into a directory
 * named x inside the dbpath.
 *
 * This test does not make sense for in-memory storage engines, since they will not produce any data
 * files.
 * @tags: [requires_persistence]
 */

(function() {
'use strict';

const baseDir = "jstests_directoryperdb";
const dbpath = MongoRunner.dataPath + baseDir + "/";
const dbname = "foo";

const isDirectoryPerDBSupported =
    jsTest.options().storageEngine == "wiredTiger" || !jsTest.options().storageEngine;

const m = MongoRunner.runMongod({dbpath: dbpath, directoryperdb: ''});

if (!isDirectoryPerDBSupported) {
    assert.isnull(m, 'storage engine without directoryperdb support should fail to start up');
    return;
} else {
    assert(m, 'storage engine with directoryperdb support failed to start up');
}

const getDir = function(dbName, dbDirPath) {
    return listFiles(dbDirPath).filter(function(path) {
        return path.name.endsWith(dbName);
    });
};

const checkDirExists = function(dbName, dbDirPath) {
    const files = getDir(dbName, dbDirPath);
    assert.eq(1,
              files.length,
              "dbpath did not contain '" + dbName +
                  "' directory when it should have: " + tojson(listFiles(dbDirPath)));
    assert.gt(listFiles(files[0].name).length, 0);
};

const checkDirRemoved = function(dbName, dbDirPath) {
    checkLog.containsJson(db.getMongo(), 4888200, {db: dbName});
    assert.soon(
        function() {
            const files = getDir(dbName, dbDirPath);
            if (files.length == 0) {
                return true;
            } else {
                return false;
            }
        },
        "dbpath contained '" + dbName +
            "' directory when it should have been removed:" + tojson(listFiles(dbDirPath)),
        10 * 1000);  // The periodic task to run data table cleanup runs once a second.
};

const db = m.getDB(dbname);
assert.commandWorked(db.bar.insert({x: 1}));
checkDirExists(dbname, dbpath);

// Test that dropping the last collection in the database causes the database directory to be
// removed.
assert(db.bar.drop());
checkDirRemoved(dbname, dbpath);

// Test that dropping the entire database causes the database directory to be removed.
assert.commandWorked(db.bar.insert({x: 1}));
checkDirExists(dbname, dbpath);
assert.commandWorked(db.dropDatabase());
checkDirRemoved(dbname, dbpath);

MongoRunner.stopMongod(m);

// Subsequent attempt to start server using same dbpath without directoryperdb should fail.
assert.throws(() => MongoRunner.runMongod({dbpath: dbpath, restart: true}));
}());

// Test that test-only set parameters are disabled.

(function() {
'use strict';

function assertFails(opts) {
    assert.throws(() => MongoRunner.runMongod(opts), [], "Mongod startup up");
}

function assertStarts(opts) {
    const mongod = MongoRunner.runMongod(opts);
    assert(mongod, "Mongod startup up");
    MongoRunner.stopMongod(mongod);
}

TestData.enableTestCommands = false;

// enableTestCommands not specified.
assertFails({
    'setParameter': {
        AlwaysRecordTraffic: 'false',
    },
});

// enableTestCommands specified as truthy.
['1', 'true'].forEach(v => {
    assertStarts({
        'setParameter': {
            enableTestCommands: v,
            takeUnstableCheckpointOnShutdown: 'false',
        },
    });
});

// enableTestCommands specified as falsy.
['0', 'false'].forEach(v => {
    assertFails({
        'setParameter': {
            enableTestCommands: v,
            AlwaysRecordTraffic: 'false',
        },
    });
});
}());
