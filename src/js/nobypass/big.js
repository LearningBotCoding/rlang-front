/**
 * Confirms that mongod will return an error when the result generated for a distinct command
 * exceeds MaxBSONSize.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();
const db = conn.getDB('test');
const coll = db.test;

const largeString = new Array(1000 * 1000).join('x');

let bulk = coll.initializeUnorderedBulkOp();
for (let x = 0; x < 17; ++x) {
    bulk.insert({_id: (largeString + x.toString())});
}
assert.commandWorked(bulk.execute());

assert.commandFailedWithCode(db.runCommand({distinct: "test", key: "_id", query: {}}), 17217);

MongoRunner.stopMongod(conn);
})();

// Confirms that there's no attempt to drop a temp collection after $out is performed.
(function() {
"use strict";

// Prevent the mongo shell from gossiping its cluster time, since this will increase the amount
// of data logged for each op.
TestData.skipGossipingClusterTime = true;

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");
const testDB = conn.getDB("test");
const coll = testDB.do_not_drop_coll_after_succesful_out;

assert.commandWorked(coll.insert({a: 1}));

assert.commandWorked(testDB.setLogLevel(2, "command"));
assert.commandWorked(testDB.adminCommand({clearLog: "global"}));

coll.aggregate([{$out: coll.getName() + "_out"}]);
const log = assert.commandWorked(testDB.adminCommand({getLog: "global"})).log;

for (let i = 0; i < log.length; ++i) {
    const line = log[i];
    assert.eq(line.indexOf("drop test.tmp.agg_out"), -1, line);
}

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that starting a mongod with --repair after an unclean shutdown does not attempt to rebuild
 * indexes before repairing the instance. Replication is used to get the database into a state where
 * an index has been dropped on disk, but still exists in the catalog.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const dbName = "indexRebuild";
const collName = "coll";

const rst = new ReplSetTest({
    name: "doNotRebuildIndexesBeforeRepair",
    nodes: 2,
    nodeOptions: {setParameter: {logComponentVerbosity: tojsononeline({storage: {recovery: 2}})}}
});
const nodes = rst.startSet();
rst.initiate();

if (!rst.getPrimary().adminCommand("serverStatus").storageEngine.supportsSnapshotReadConcern) {
    // Only snapshotting storage engines can pause advancing the stable timestamp allowing us
    // to get into a state where indexes exist, but the underlying tables were dropped.
    rst.stopSet();
    return;
}

let primary = rst.getPrimary();
let testDB = primary.getDB(dbName);
let coll = testDB.getCollection(collName);
// The default WC is majority and disableSnapshotting failpoint will prevent satisfying any majority
// writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

assert.commandWorked(testDB.runCommand({
    createIndexes: collName,
    indexes: [
        {key: {a: 1}, name: 'a_1'},
        {key: {b: 1}, name: 'b_1'},
    ],
    writeConcern: {w: "majority"},
}));
assert.eq(3, coll.getIndexes().length);
rst.awaitReplication(undefined, ReplSetTest.OpTimeType.LAST_DURABLE);

// Lock the index entries into a stable checkpoint by shutting down.
rst.stopSet(undefined, true);
rst.startSet(undefined, true);

// Disable snapshotting on all members of the replica set so that further operations do not
// enter the majority snapshot.
nodes.forEach(node => assert.commandWorked(node.adminCommand(
                  {configureFailPoint: "disableSnapshotting", mode: "alwaysOn"})));

// Dropping the index would normally modify the collection metadata and drop the
// table. Because we're not advancing the stable timestamp and we're going to crash the
// server, the catalog change won't take effect, but ident being dropped will.
primary = rst.getPrimary();
testDB = primary.getDB(dbName);
coll = testDB.getCollection(collName);
assert.commandWorked(coll.dropIndexes());
rst.awaitReplication();

let primaryDbpath = rst.getPrimary().dbpath;
let primaryPort = rst.getPrimary().port;
rst.stop(0, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL}, {forRestart: true});
rst.stopSet(undefined, true);

// This should succeed in rebuilding the indexes, but only after the databases have been
// repaired.
assert.eq(0,
          runMongoProgram("mongod", "--repair", "--port", primaryPort, "--dbpath", primaryDbpath));

// Restarting the replica set would roll back the index drop. Instead we want to start a
// standalone and verify that repair rebuilt the indexes.
let mongod = MongoRunner.runMongod({dbpath: primaryDbpath, noCleanData: true});
assert.eq(3, mongod.getDB(dbName)[collName].getIndexes().length);

MongoRunner.stopMongod(mongod);
})();

/**
 * @tags: [
 *   requires_fastcount,
 * ]
 * Tests the countDocuments and estimatedDocumentCount commands.
 */
(function() {
"use strict";

const standalone = MongoRunner.runMongod();
const dbName = "test";
const db = standalone.getDB(dbName);
const collName = "document_count_functions";
const coll = db.getCollection(collName);

coll.drop();
assert.eq(0, coll.countDocuments({i: 1}));
assert.commandWorked(db.createCollection(collName));
assert.eq(0, coll.countDocuments({i: 1}));

assert.commandWorked(coll.insert({i: 1, j: 1}));
assert.commandWorked(coll.insert({i: 2, j: 1}));
assert.commandWorked(coll.insert({i: 2, j: 2}));

// Base case: Pass a valid query into countDocuments without any extra options.
assert.eq(1, coll.countDocuments({i: 1}));
assert.eq(2, coll.countDocuments({i: 2}));

// Base case: Call estimatedDocumentCount without any extra options.
assert.eq(3, coll.estimatedDocumentCount());

assert.commandWorked(coll.insert({i: 1, j: 2}));
assert.commandWorked(coll.insert({i: 1, j: 3}));
assert.commandWorked(coll.insert({i: 1, j: 4}));

// Limit case: Limit the number of documents to count. There are 4 {i: 1} documents,
// but we will set the limit to 3.
assert.eq(3, coll.countDocuments({i: 1}, {limit: 3}));

// Skip case: Skip a certain number of documents for the count. We will skip 2, meaning
// that we will have 2 left.
assert.eq(2, coll.countDocuments({i: 1}, {skip: 2}));

assert.commandWorked(coll.createIndex({i: 1}));

// Aggregate stage case: Add an option that gets added as an aggregation argument.
assert.eq(4, coll.countDocuments({i: 1}, {hint: {i: 1}}));

// Set fail point to make sure estimatedDocumentCount times out.
assert.commandWorked(
    db.adminCommand({configureFailPoint: 'maxTimeAlwaysTimeOut', mode: 'alwaysOn'}));

// maxTimeMS case: Expect an error if an operation times out.
assert.commandFailedWithCode(assert.throws(function() {
                                              coll.estimatedDocumentCount({maxTimeMS: 100});
                                          }),
                                          ErrorCodes.MaxTimeMSExpired);

// Disable fail point.
assert.commandWorked(db.adminCommand({configureFailPoint: 'maxTimeAlwaysTimeOut', mode: 'off'}));

MongoRunner.stopMongod(standalone);
})();

/**
 * Tests that the "drop" command can abort in-progress index builds.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_collection_aborts_in_progress_index_builds";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

assert.commandWorked(coll.insert({a: 1}));
assert.commandWorked(coll.insert({b: 1}));

assert.commandWorked(coll.createIndex({a: 1}));

jsTest.log("Starting two index builds and freezing them.");
IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitFirstIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {a: 1, b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1_b_1");

const awaitSecondIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "b_1");

jsTest.log("Dropping collection " + dbName + "." + collName + " with in-progress index builds");
const awaitDrop = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.runCommand({drop: TestData.collName}));
}, conn.port);

try {
    checkLog.containsJson(testDB.getMongo(), 23879);  // "About to abort all index builders"
} finally {
    IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
}

awaitFirstIndexBuild();
awaitSecondIndexBuild();
awaitDrop();

MongoRunner.stopMongod(conn);
}());

/**
 * verify dropConnections command works for replica sets
 * @tags: [
 *   requires_replication,
 * ]
 */

(function() {
"use strict";

const rst = new ReplSetTest({nodes: 3});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
rst.awaitSecondaryNodes();

function getConnPoolHosts() {
    const ret = primary.adminCommand({connPoolStats: 1});
    assert.commandWorked(ret);
    jsTestLog("Connection pool stats by host: " + tojson(ret.hosts));
    return ret.hosts;
}

// To test the dropConnections command, first remove the secondary. This should have no effect
// on the existing connection pool, but it'll prevent the primary from reconnecting to it after
// dropConnections. Then, execute dropConnections and check that the primary has 0 connections
// to the secondary.
const cfg = primary.getDB('local').system.replset.findOne();
const memberHost = cfg.members[2].host;
assert.eq(memberHost in getConnPoolHosts(), true);

const removedMember = cfg.members.splice(2, 1);
assert.eq(removedMember[0].host, memberHost);
cfg.version++;

jsTestLog("Reconfiguring to omit " + memberHost);
assert.commandWorked(primary.adminCommand({replSetReconfig: cfg}));

// Reconfig did not affect the connection pool
assert.eq(memberHost in getConnPoolHosts(), true);

// Test dropConnections
jsTestLog("Dropping connections to " + memberHost);
assert.commandWorked(primary.adminCommand({dropConnections: 1, hostAndPort: [memberHost]}));
assert.soon(() => {
    return !(memberHost in getConnPoolHosts());
});

// Need to re-add removed node, or the test complains about the replset config
cfg.members.push(removedMember[0]);
cfg.version++;
assert.commandWorked(primary.adminCommand({replSetReconfig: cfg}));

rst.stopSet();
})();

/**
 * verify dropConnections command works for sharded clusters
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */

(function() {
"use strict";

const st = new ShardingTest({
    config: {nodes: 1},
    shards: 1,
    rs0: {nodes: 3},
    mongos: 1,
});
const mongos = st.s0;
const rst = st.rs0;
const primary = rst.getPrimary();

mongos.adminCommand({multicast: {ping: 0}});

function getConnPoolHosts() {
    const ret = mongos.adminCommand({connPoolStats: 1});
    assert.commandWorked(ret);
    jsTestLog("Connection pool stats by host: " + tojson(ret.hosts));
    return ret.hosts;
}

const cfg = primary.getDB('local').system.replset.findOne();
const memberHost = cfg.members[2].host;
assert.eq(memberHost in getConnPoolHosts(), true);

const removedMember = cfg.members.splice(2, 1);
assert.eq(removedMember[0].host, memberHost);
cfg.version++;

jsTestLog("Reconfiguring to omit " + memberHost);
assert.commandWorked(primary.adminCommand({replSetReconfig: cfg}));
assert.eq(memberHost in getConnPoolHosts(), true);

jsTestLog("Dropping connections to " + memberHost);
assert.commandWorked(mongos.adminCommand({dropConnections: 1, hostAndPort: [memberHost]}));
assert.soon(() => {
    return !(memberHost in getConnPoolHosts());
});

// need to re-add removed node or test complain about the replset config
cfg.members.push(removedMember[0]);
cfg.version++;
assert.commandWorked(primary.adminCommand({replSetReconfig: cfg}));

st.stop();
})();

/**
 * Tests that the _mdb_catalog does not reuse RecordIds after a catalog restart.
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

TestData.rollbackShutdowns = true;
TestData.logComponentVerbosity = {
    storage: {recovery: 2}
};
load('jstests/replsets/libs/rollback_test.js');

const rollbackTest = new RollbackTest();
let primary = rollbackTest.getPrimary();
// Do a majority write to guarantee the stable timestamp contains this create. Otherwise startup
// replication recovery will recreate the collection, initializing the _mdb_catalog RecordId
// generator.
assert.commandWorked(
    primary.getDB("foo").runCommand({create: "timestamped", writeConcern: {w: "majority"}}));

// This restart forces the _mdb_catalog to refresh, uninitializing the auto-incrementing RecordId
// generator.
jsTestLog({msg: "Restarting primary.", primary: primary, nodeId: primary.nodeId});
const SIGTERM = 15;  // clean shutdown
rollbackTest.restartNode(primary.nodeId, SIGTERM);

let rollbackNode = rollbackTest.transitionToRollbackOperations();
jsTestLog({
    msg: "The restarted primary must be the node that goes into rollback.",
    primary: primary,
    rollback: rollbackNode
});
assert.eq(primary, rollbackNode);
// The `timestamped` collection is positioned as the last record in the _mdb_catalog. An unpatched
// MongoDB dropping this collection will result in its RecordId being reused.
assert.commandWorked(rollbackNode.getDB("foo").runCommand({drop: "timestamped"}));
// Reusing the RecordId with an untimestamped write (due to being in the `local` database) results
// in an illegal update chain. A successful rollback should see the timestamped collection. But an
// unpatched MongoDB would have the untimestamped update on the same update chain, "locking in" the
// drop.
assert.commandWorked(rollbackNode.getDB("local").createCollection("untimestamped"));

rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

assert.contains("timestamped", rollbackNode.getDB("foo").getCollectionNames());
assert.contains("untimestamped", rollbackNode.getDB("local").getCollectionNames());
rollbackTest.stop();
})();

/**
 * Tests that the "dropDatabase" command can abort in-progress index builds on all the collections
 * it is dropping.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_database_aborts_in_progress_index_builds";
const firstCollName = "first";
const secondCollName = "second";

TestData.dbName = dbName;

const testDB = conn.getDB(dbName);
testDB.getCollection(firstCollName).drop();
testDB.getCollection(secondCollName).drop();

assert.commandWorked(testDB.createCollection(firstCollName));
assert.commandWorked(testDB.createCollection(secondCollName));
const firstColl = testDB.getCollection(firstCollName);
const secondColl = testDB.getCollection(secondCollName);

assert.commandWorked(firstColl.insert({a: 1}));
assert.commandWorked(firstColl.insert({b: 1}));

assert.commandWorked(secondColl.insert({a: 1}));
assert.commandWorked(secondColl.insert({b: 1}));

assert.commandWorked(firstColl.createIndex({a: 1}));
assert.commandWorked(secondColl.createIndex({a: 1}));

jsTest.log("Starting an index build on each collection and freezing them.");
IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitFirstIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), firstColl.getFullName(), {b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, firstCollName, "b_1");

const awaitSecondIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), secondColl.getFullName(), {b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, secondCollName, "b_1");

jsTest.log("Dropping database " + dbName + " with in-progress index builds on its collections.");

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'dropDatabaseHangAfterWaitingForIndexBuilds', mode: 'alwaysOn'}));

const awaitDropDatabase = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.dropDatabase());
}, conn.port);
try {
    checkLog.contains(
        testDB.getMongo(),
        "About to abort all index builders running for collections in the given database");
    IndexBuildTest.resumeIndexBuilds(testDB.getMongo());

    checkLog.contains(
        testDB.getMongo(),
        "dropDatabase - fail point dropDatabaseHangAfterWaitingForIndexBuilds enabled");

    // Cannot create a collection on the database while it is drop pending.
    assert.commandFailedWithCode(testDB.createCollection("third"), ErrorCodes.DatabaseDropPending);

    // Performing a bulk write should throw DatabaseDropPending.
    let bulk = testDB["third"].initializeUnorderedBulkOp();
    bulk.insert({});

    try {
        bulk.execute();
    } catch (ex) {
        assert.eq(true, ex instanceof BulkWriteError);
        assert.writeErrorWithCode(ex, ErrorCodes.DatabaseDropPending);
    }
} finally {
    IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
    testDB.adminCommand(
        {configureFailPoint: 'dropDatabaseHangAfterWaitingForIndexBuilds', mode: 'off'});
}

awaitFirstIndexBuild();
awaitSecondIndexBuild();
awaitDropDatabase();

MongoRunner.stopMongod(conn);
}());

/**
 * Verifies that a dropDatabase operation interrupted due to stepping down resets the drop pending
 * flag. Additionally, after the node steps down, we ensure it can drop the database as instructed
 * by the new primary.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
    nodes: [
        {},
        {},
    ]
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'dropDatabaseHangAfterWaitingForIndexBuilds', mode: 'alwaysOn'}));

assert.commandWorked(coll.insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

let awaitIndexBuild = startParallelShell(() => {
    const coll = db.getSiblingDB('test').getCollection('test');
    assert.commandFailedWithCode(coll.createIndex({a: 1}), ErrorCodes.IndexBuildAborted);
}, primary.port);

IndexBuildTest.waitForIndexBuildToStart(testDB, coll.getName(), "a_1");

let awaitDropDatabase = startParallelShell(() => {
    assert.commandFailedWithCode(db.getSiblingDB('test').dropDatabase(),
                                 ErrorCodes.InterruptedDueToReplStateChange);
}, primary.port);

checkLog.containsJson(primary, 4612300);

assert.commandWorked(testDB.adminCommand({clearLog: "global"}));
let awaitStepDown = startParallelShell(() => {
    assert.commandWorked(db.adminCommand({replSetStepDown: 30}));
}, primary.port);

IndexBuildTest.resumeIndexBuilds(primary);

checkLog.containsJson(primary, 21344);
assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'dropDatabaseHangAfterWaitingForIndexBuilds', mode: 'off'}));

awaitIndexBuild();
awaitDropDatabase();
awaitStepDown();

rst.awaitReplication();

// Have the new primary try to drop the database. The stepped down node must successfully replicate
// this dropDatabase command.
assert.commandWorked(rst.getPrimary().getDB('test').dropDatabase());
rst.stopSet();
})();

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 *
 * In this file, we test calling "dropIndexes" with a complex index name whose index build is
 * in-progress.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_indexes_aborts_in_progress_index_builds_complex_name";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

jsTest.log("Aborting index builder with one index build and complex index spec");
assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({b: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {a: 1, b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1_b_1");

const awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.runCommand({dropIndexes: TestData.collName, index: "a_1_b_1"}));
}, conn.port);

checkLog.contains(testDB.getMongo(), "About to abort index builder");
IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
awaitIndexBuild();
awaitDropIndex();

assert.eq(1, testDB.getCollection(collName).getIndexes().length);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 *
 * In this file, we test calling "dropIndexes" with a key pattern whose index build is in-progress.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_indexes_aborts_in_progress_index_builds_key_pattern";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

jsTest.log("Aborting index builder by key pattern");
assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({b: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {a: 1, b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1_b_1");

const awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.runCommand({dropIndexes: TestData.collName, index: {a: 1, b: 1}}));
}, conn.port);

checkLog.contains(testDB.getMongo(), "About to abort index builder");
IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
awaitIndexBuild();
awaitDropIndex();

assert.eq(1, testDB.getCollection(collName).getIndexes().length);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 *
 * In this file, we test calling "dropIndexes" with a list of index names, which will be used to
 * abort a single index builder only, which was building all the given indexes.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_indexes_aborts_in_progress_index_builds_multiple";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

jsTest.log("Aborting index builder with multiple index builds");
assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({b: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitIndexBuild = IndexBuildTest.startIndexBuild(testDB.getMongo(),
                                                       coll.getFullName(),
                                                       [{a: 1}, {b: 1}, {a: 1, b: 1}],
                                                       {},
                                                       [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1");
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "b_1");
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1_b_1");

const awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(
        testDB.runCommand({dropIndexes: TestData.collName, index: ["b_1", "a_1_b_1", "a_1"]}));
}, conn.port);

checkLog.contains(testDB.getMongo(), "About to abort index builder");
IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
awaitIndexBuild();
awaitDropIndex();

assert.eq(1, testDB.getCollection(collName).getIndexes().length);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 *
 * This test also confirms that secondary reads are supported while index builds are in progress.
 *
 * In this file, we test calling "dropIndexes" with a simple index name whose index build is
 * in-progress.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");
load('jstests/replsets/libs/secondary_reads_test.js');

const dbName = "drop_indexes_aborts_in_progress_index_builds_simple_name";

const secondaryReadsTest = new SecondaryReadsTest(dbName);

let primaryDB = secondaryReadsTest.getPrimaryDB();
const conn = primaryDB.getMongo();

const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

jsTest.log("Aborting index builder with one index build and simple index spec");
assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());
IndexBuildTest.pauseIndexBuilds(secondaryReadsTest.getSecondaryDB().getMongo());

const awaitIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {a: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1");
IndexBuildTest.waitForIndexBuildToStart(secondaryReadsTest.getSecondaryDB(), collName, "a_1");

// Test secondary reads during oplog application.
// Prevent a batch from completing on the secondary.
const pauseAwait = secondaryReadsTest.pauseSecondaryBatchApplication();

for (let i = 100; i < 200; i++) {
    assert.commandWorked(testDB.getCollection(collName).insert({a: i}));
}

// Wait for the batch application to pause.
pauseAwait();

// Do a bunch of reads on the 'collName' collection on the secondary.
// No errors should be encountered on the secondary.
let readFn = function() {
    for (let x = 0; x < TestData.nOps; x++) {
        assert.commandWorked(db.runCommand({
            find: TestData.collName,
            filter: {a: x},
        }));
        // Sleep a bit to make these reader threads less CPU intensive.
        sleep(60);
    }
};
TestData.nOps = 10;
const nReaders = 3;
secondaryReadsTest.startSecondaryReaders(nReaders, readFn);

// Disable the failpoint and let the batch complete.
secondaryReadsTest.resumeSecondaryBatchApplication();
secondaryReadsTest.stopReaders();

const awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.runCommand({dropIndexes: TestData.collName, index: "a_1"}));
}, conn.port);

checkLog.contains(testDB.getMongo(), "About to abort index builder");
IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
IndexBuildTest.resumeIndexBuilds(secondaryReadsTest.getSecondaryDB().getMongo());
awaitIndexBuild();
awaitDropIndex();

assert.eq(1, testDB.getCollection(collName).getIndexes().length);

secondaryReadsTest.stop();
}());

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 *
 * In this file, we test calling "dropIndexes" with the "*" wildcard which will abort all
 * in-progress index builds and remove ready indexes.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_indexes_aborts_in_progress_index_builds_wildcard";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({b: 1}));

assert.commandWorked(testDB.getCollection(collName).createIndex({a: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitFirstIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {a: 1, b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "a_1_b_1");

const awaitSecondIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "b_1");

jsTest.log("Aborting all index builders and ready indexes with '*' wildcard");
assert.commandWorked(testDB.runCommand({dropIndexes: collName, index: "*"}));
checkLog.containsJson(testDB.getMongo(), 23879);  // "About to abort all index builders"

IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
awaitFirstIndexBuild();
awaitSecondIndexBuild();

assert.eq(1, testDB.getCollection(collName).getIndexes().length);  // _id index

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that the "dropIndexes" command can abort in-progress index builds. The "dropIndexes"
 * command will only abort in-progress index builds if the user specifies all of the indexes that a
 * single builder is building together, as we can only abort at the index builder granularity level.
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

const dbName = "drop_indexes_aborts_in_progress_index_builds_wildcard";
const collName = "test";

TestData.dbName = dbName;
TestData.collName = collName;

const testDB = conn.getDB(dbName);
testDB.getCollection(collName).drop();

assert.commandWorked(testDB.createCollection(collName));
const coll = testDB.getCollection(collName);

assert.commandWorked(testDB.getCollection(collName).insert({a: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({b: 1}));
assert.commandWorked(testDB.getCollection(collName).insert({c: 1}));

assert.commandWorked(testDB.getCollection(collName).createIndex({a: 1}));

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());

const awaitFirstIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {b: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "b_1");

const awaitSecondIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {c: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "c_1");

let awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandFailedWithCode(
        testDB.runCommand({dropIndexes: TestData.collName, index: ["a_1", "b_1"]}),
        [ErrorCodes.BackgroundOperationInProgressForNamespace]);
}, conn.port);
awaitDropIndex();

awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandFailedWithCode(
        testDB.runCommand({dropIndexes: TestData.collName, index: ["a_1", "c_1"]}),
        [ErrorCodes.BackgroundOperationInProgressForNamespace]);
}, conn.port);
awaitDropIndex();

awaitDropIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandFailedWithCode(
        testDB.runCommand({dropIndexes: TestData.collName, index: ["b_1", "c_1"]}),
        [ErrorCodes.BackgroundOperationInProgressForNamespace]);
}, conn.port);
awaitDropIndex();

IndexBuildTest.resumeIndexBuilds(testDB.getMongo());
awaitFirstIndexBuild();
awaitSecondIndexBuild();

assert.eq(4, testDB.getCollection(collName).getIndexes().length);

MongoRunner.stopMongod(conn);
}());

/**
 * Verifies that the dropIndexes command does not invariant if it sees a similar index build
 * completed that it successfully aborted.
 */
(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = "dropIndexesOnRecreatedIndex";

const conn = MongoRunner.runMongod({});
assert(conn);

const db = conn.getDB(dbName);
assert.commandWorked(db.createCollection(collName));

const coll = db.getCollection(collName);
assert.commandWorked(coll.insert({a: 1}));

const indexSpec = {
    a: 1
};

IndexBuildTest.pauseIndexBuilds(conn);

// Start an index build on {a: 1}.
let awaitIndexBuild = IndexBuildTest.startIndexBuild(
    conn, coll.getFullName(), indexSpec, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToStart(db, collName, "a_1");

const failPoint = "hangAfterAbortingIndexes";
let res = assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "alwaysOn"}));
let timesEntered = res.count;

TestData.dbName = dbName;
TestData.collName = collName;
TestData.indexSpec = indexSpec;

// Abort {a: 1} while it is being built.
let awaitDropIndexes = startParallelShell(() => {
    assert.commandWorked(db.getSiblingDB(TestData.dbName)
                             .getCollection(TestData.collName)
                             .dropIndexes(TestData.indexSpec));
}, conn.port);

// Wait until {a: 1} is aborted, but before the dropIndexes command finishes.
awaitIndexBuild();
assert.commandWorked(conn.adminCommand({
    waitForFailPoint: failPoint,
    timesEntered: timesEntered + 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

// Recreate {a: 1} and let it finish.
IndexBuildTest.resumeIndexBuilds(conn);
assert.commandWorked(coll.createIndex(indexSpec));

// Allow dropIndexes to finish. The dropIndexes should drop the newly recreated {a: 1} index.
assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "off"}));
awaitDropIndexes();

// The collection should only have the _id index.
let indexes = coll.getIndexes();
assert.eq(1, indexes.length);

MongoRunner.stopMongod(conn);
}());

/**
 * The dropIndexes command has to have the same assertions on the primary and secondary nodes.
 *
 * dropIndexes for applyOps will return 'BackgroundOperationInProgressForNamespace' if there are any
 * in-progress index builds. For initial sync, this causes all of the in-progress index builds to be
 * aborted. However, during steady state replication, the dropIndexes for applyOps would hang until
 * there are no more in-progress index builds. But because the abortIndexBuild/commitIndexBuild
 * oplog entries come after the dropIndexes oplog entry, replication will stall indefinitely waiting
 * for this condition.
 *
 * This happens because on the primary, the dropIndexes command would abort in-progress index builds
 * and drop any ready indexes even if there are index builds in-progress. To solve this problem, the
 * dropIndexes command cannot drop any ready indexes while there are any in-progress index builds.
 *
 * @tags: [requires_replication]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const replSet = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
            },
        },
    ]
});

replSet.startSet();
replSet.initiateWithHighElectionTimeout();

const dbName = "test";
const collName = "drop_indexes_prevents_dropping_ready_indexes_after_aborting";

const primary = replSet.getPrimary();
const secondary = replSet.getSecondary();
const db = primary.getDB(dbName);
const coll = db.getCollection(collName);

for (let i = 0; i < 5; i++) {
    assert.commandWorked(coll.insert({a: i, b: i}));
}

jsTestLog("Starting an index build on {a: 1} and hanging on the primary");
IndexBuildTest.pauseIndexBuilds(primary);
let awaitIndexBuild = IndexBuildTest.startIndexBuild(
    primary, coll.getFullName(), {a: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToStart(primary.getDB(dbName), coll.getName(), "a_1");

const failPoint = "hangAfterAbortingIndexes";
let res = assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "alwaysOn"}));
let timesEntered = res.count;

TestData.dbName = dbName;
TestData.collName = collName;

jsTestLog(
    "Aborting index build on {a: 1} and hanging dropIndexes while yielding the collection lock");
let awaitDropIndexes = startParallelShell(() => {
    assert.commandFailedWithCode(db.getSiblingDB(TestData.dbName)
                                     .runCommand({dropIndexes: TestData.collName, index: ["a_1"]}),
                                 ErrorCodes.BackgroundOperationInProgressForNamespace);
}, primary.port);

awaitIndexBuild();
assert.commandWorked(primary.adminCommand({
    waitForFailPoint: failPoint,
    timesEntered: timesEntered + 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Creating the index {a: 1} to completion");
IndexBuildTest.resumeIndexBuilds(primary);
assert.commandWorked(coll.createIndex({a: 1}));

jsTestLog("Starting an index build on {b: 1} and hanging on the secondary");
IndexBuildTest.pauseIndexBuilds(secondary);
awaitIndexBuild = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {b: 1}, {}, [], 2);
IndexBuildTest.waitForIndexBuildToStart(secondary.getDB(dbName), coll.getName(), "b_1");

jsTestLog("Resuming the dropIndexes command");
assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "off"}));
awaitDropIndexes();

jsTestLog("Waiting for dropIndexes to replicate");
replSet.awaitReplication();

jsTestLog("Resuming the index build {b: 1} on the secondary");
IndexBuildTest.resumeIndexBuilds(secondary);
awaitIndexBuild();

replSet.stopSet();
}());

/* Tests that dropping the oplog is forbidden on standalone nodes with storage engines
 * that support the command "replSetResizeOplog". The support for this command is
 * provided only by the WiredTiger storage engine.
 * Therefore, attempts to drop the oplog when using these storage engines should fail.
 * Also, nodes running in a replica set will forbid dropping the oplog, but
 * for a different reason.
 * Note: We detect whether a storage engine supports the replSetResizeOplog command
 * by checking whether it supportsRecoveryTimestamp().
 *
 * @tags: [
 *  requires_persistence,
 *  # Multiversion testing is not supported for tests running ReplSetTest as standalones.
 *  multiversion_incompatible,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/storage_engine_utils.js");

// Start a standalone node.
let primary = MongoRunner.runMongod();
let localDB = primary.getDB('local');

// Standalone nodes don't start with an oplog; create one. The size of the oplog doesn't
// matter. We are capping the oplog because some storage engines do not allow the creation
// of uncapped oplog collections.
assert.commandWorked(localDB.runCommand({create: 'oplog.rs', capped: true, size: 1000}));

if (storageEngineIsWiredTiger()) {
    const ret = assert.commandFailed(localDB.runCommand({drop: 'oplog.rs'}));
    assert.eq("can't drop oplog on storage engines that support replSetResizeOplog command",
              ret.errmsg);
} else {
    assert.commandWorked(localDB.runCommand({drop: 'oplog.rs'}));
}

MongoRunner.stopMongod(primary);
}());
/**
 * Test that drop view only takes database IX lock.
 *
 * @tags: [requires_db_locking]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");

const conn = MongoRunner.runMongod({});
const db = conn.getDB("test");

assert.commandWorked(db.runCommand({insert: "a", documents: [{x: 1}]}));
assert.commandWorked(db.createView("view", "a", []));

const failPoint = configureFailPoint(db, "hangDuringDropCollection");

// This only holds a database IX lock.
const awaitDrop =
    startParallelShell(() => assert(db.getSiblingDB("test")["view"].drop()), conn.port);
failPoint.wait();

// This takes a database IX lock and should not be blocked.
assert.commandWorked(db.runCommand({insert: "a", documents: [{y: 1}]}));

failPoint.off();

awaitDrop();
MongoRunner.stopMongod(conn);
})();

/*
 * SERVER-35172: Test that dropCollection does not return a message containing multiple "ns" fields
 * @tags: [requires_wiredtiger]
 */

(function() {
"use strict";
var conn = MongoRunner.runMongod();
var db = conn.getDB('test');

let coll = db.dropcollection_duplicate_fields;
// Repeat 100 times for the sake of probabilities
for (let i = 0; i < 100; i++) {
    coll.drop();
    coll.insert({x: 1});

    assert.commandWorked(db.adminCommand(
        {configureFailPoint: 'WTWriteConflictException', mode: {activationProbability: 0.1}}));

    // will blow up if res is not valid
    let res = db.runCommand({drop: 'dropcollection_duplicate_fields'});

    assert.commandWorked(
        db.adminCommand({configureFailPoint: 'WTWriteConflictException', mode: "off"}));
}

MongoRunner.stopMongod(conn);
})();

/*
 * Tests that dropDatabase respects maxTimeMS.
 * @tags: [requires_replication, uses_transactions]
 */
(function() {
load("jstests/libs/wait_for_command.js");
const rst = ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dropDB = rst.getPrimary().getDB("drop");

(function assertCollectionDropCanBeInterrupted() {
    assert.commandWorked(dropDB.bar.insert({_id: 0}, {writeConcern: {w: 'majority'}}));
    const session = dropDB.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase("drop");
    session.startTransaction();
    assert.commandWorked(sessionDB.bar.insert({_id: 1}));
    assert.commandFailedWithCode(dropDB.runCommand({dropDatabase: 1, maxTimeMS: 100}),
                                 ErrorCodes.MaxTimeMSExpired);

    assert.commandWorked(session.commitTransaction_forTesting());
    session.endSession();
})();

(function assertDatabaseDropCanBeInterrupted() {
    assert.commandWorked(dropDB.bar.insert({}));

    assert.commandWorked(rst.getPrimary().adminCommand(
        {configureFailPoint: "dropDatabaseHangAfterAllCollectionsDrop", mode: "alwaysOn"}));

    // This will get blocked by the failpoint when collection drop phase finishes.
    let dropDatabaseShell = startParallelShell(() => {
        assert.commandFailedWithCode(
            db.getSiblingDB("drop").runCommand({dropDatabase: 1, maxTimeMS: 5000}),
            ErrorCodes.MaxTimeMSExpired);
    }, rst.getPrimary().port);

    checkLog.contains(
        dropDB.getMongo(),
        "dropDatabase - fail point dropDatabaseHangAfterAllCollectionsDrop enabled. Blocking until fail point is disabled");

    let sleepCommand = startParallelShell(() => {
        // Make dropDatabase timeout.
        assert.commandFailedWithCode(
            db.getSiblingDB("drop").adminCommand(
                {sleep: 1, secs: 500, lockTarget: "drop", lock: "ir", $comment: "Lock sleep"}),
            ErrorCodes.Interrupted);
    }, rst.getPrimary().port);

    checkLog.contains(dropDB.getMongo(), "Test-only command 'sleep' invoked");

    // dropDatabase now gets unblocked by the failpoint but will immediately
    // get blocked by acquiring the database lock for dropping the database.
    assert.commandWorked(rst.getPrimary().adminCommand(
        {configureFailPoint: "dropDatabaseHangAfterAllCollectionsDrop", mode: "off"}));

    dropDatabaseShell();

    // Interrupt the sleep command.
    const sleepID = waitForCommand(
        "sleepCmd",
        op => (op["ns"] == "admin.$cmd" && op["command"]["$comment"] == "Lock sleep"),
        dropDB.getSiblingDB("admin"));
    assert.commandWorked(dropDB.getSiblingDB("admin").killOp(sleepID));

    sleepCommand();
})();

rst.stopSet();
})();

/**
 * Test that hidden index status can be replicated by secondary nodes and will be persisted
 * into the index catalog, that is hidden index remains hidden after restart.
 *
 * @tags: [
 *   requires_journaling,
 *   requires_replication,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/get_index_helpers.js");  // For GetIndexHelpers.findByName.

const dbName = "test";

function isIndexHidden(indexes, indexName) {
    const idx = GetIndexHelpers.findByName(indexes, indexName);
    return idx && idx.hidden;
}

//
// Test that hidden index status can be replicated by secondary nodes and will be persisted into the
// index catalog in a replica set.
//
const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();
const primaryDB = rst.getPrimary().getDB(dbName);
primaryDB.coll.drop();

// Create a hidden index.
assert.commandWorked(primaryDB.coll.createIndex({a: 1}, {hidden: true}));
assert(isIndexHidden(primaryDB.coll.getIndexes(), "a_1"));

// Explicitly create an unhidden index.
assert.commandWorked(primaryDB.coll.createIndex({b: 1}, {hidden: false}));
assert(!isIndexHidden(primaryDB.coll.getIndexes(), "b_1"));

// Wait for the replication finishes before stopping the replica set.
rst.awaitReplication();

// Restart the replica set.
rst.stopSet(/* signal */ undefined, /* forRestart */ true);
rst.startSet(/* signal */ undefined, /* forRestart */ true);
const secondaryDB = rst.getSecondary().getDB(dbName);

// Test that after restart the index is still hidden.
assert(isIndexHidden(secondaryDB.coll.getIndexes(), "a_1"));

// Test that 'hidden: false' shouldn't be written to the index catalog.
let idxSpec = GetIndexHelpers.findByName(secondaryDB.coll.getIndexes(), "b_1");
assert.eq(idxSpec.hidden, undefined);

rst.stopSet();

//
// Test that hidden index status will be persisted into the index catalog in a standalone mongod,
// whereas, an unhidden index will not write 'hidden: false' to the index catalog even when
// createIndexes specifies 'hidden: false' explicitly.
//
// Start a mongod.
let conn = MongoRunner.runMongod();
assert.neq(null, conn, 'mongod was unable to start up');
let db = conn.getDB(dbName);
db.coll.drop();

// Create a hidden index.
assert.commandWorked(db.coll.createIndex({a: 1}, {hidden: true}));
assert(isIndexHidden(db.coll.getIndexes(), "a_1"));

// Explicitly create an unhidden index.
assert.commandWorked(db.coll.createIndex({b: 1}, {hidden: false}));
assert(!isIndexHidden(db.coll.getIndexes(), "b_1"));

// Restart the mongod.
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: true, cleanData: false, dbpath: conn.dbpath});
db = conn.getDB(dbName);

// Test that after restart the index is still hidden.
assert(isIndexHidden(db.coll.getIndexes(), "a_1"));

// Test that 'hidden: false' shouldn't be written to the index catalog.
idxSpec = GetIndexHelpers.findByName(db.coll.getIndexes(), "b_1");
assert.eq(idxSpec.hidden, undefined);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that view creation and modification is correctly persisted.
 *
 * This test requires persistence to ensure data survives a restart.
 * @tags: [requires_persistence]
 */
(function() {
'use strict';

// The following test verifies that writeConcern: {j: true} ensures that the view catalog is
// durable.
let dbpath = MongoRunner.dataPath + '_durable_view_catalog';
resetDbpath(dbpath);

let mongodArgs = {dbpath: dbpath, noCleanData: true, journal: ''};

// Start a mongod.
let conn = MongoRunner.runMongod(mongodArgs);
assert.neq(null, conn, 'mongod was unable to start up');

// Now connect to the mongod, create, remove and modify views and then abruptly stop the server.
let viewsDB = conn.getDB('test');
let pipe = [{$match: {}}];
assert.commandWorked(viewsDB.runCommand({create: "view1", viewOn: "collection", pipeline: pipe}));
assert.commandWorked(viewsDB.runCommand({create: "view2", viewOn: "collection", pipeline: pipe}));
assert.commandWorked(viewsDB.runCommand({create: "view3", viewOn: "collection", pipeline: pipe}));
assert.commandWorked(viewsDB.runCommand({collMod: "view3", viewOn: "view2"}));
// On the final modification, require a sync to ensure durability.
assert.commandWorked(viewsDB.runCommand({drop: "view1", writeConcern: {j: 1}}));

// Hard kill the mongod to ensure the data was indeed synced to durable storage.
MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});

// Restart the mongod.
conn = MongoRunner.runMongod(mongodArgs);
assert.neq(null, conn, 'mongod was unable to restart after receiving a SIGKILL');

// Check that our journaled write still is present.
viewsDB = conn.getDB('test');
let actualViews = viewsDB.system.views.find().toArray();
let expectedViews = [
    {"_id": "test.view2", "viewOn": "collection", "pipeline": pipe},
    {"_id": "test.view3", "viewOn": "view2", "pipeline": pipe}
];
assert.eq(actualViews, expectedViews, "view definitions not correctly persisted");
let listedViews =
    viewsDB.runCommand({listCollections: 1, filter: {type: "view"}})
        .cursor.firstBatch.map((function(x) {
            return {_id: "test." + x.name, viewOn: x.options.viewOn, pipeline: x.options.pipeline};
        }));
assert.sameMembers(listedViews, expectedViews, "persisted view definitions not correctly loaded");

// Insert an invalid view definition directly into system.views to bypass normal validation.
assert.commandWorked(viewsDB.adminCommand({
    applyOps: [
        {op: "i", ns: viewsDB.getName() + ".system.views", o: {_id: "badView", pipeline: "badType"}}
    ]
}));

// Skip collection validation during stopMongod if invalid views exists.
TestData.skipValidationOnInvalidViewDefinitions = true;

// Restarting the mongod should succeed despite the presence of invalid view definitions.
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod(mongodArgs);
assert.neq(
    null,
    conn,
    "after inserting bad views, failed to restart mongod with options: " + tojson(mongodArgs));

// Now that the database's view catalog has been marked as invalid, all view operations in that
// database should fail.
viewsDB = conn.getDB("test");
assert.commandFailedWithCode(viewsDB.runCommand({find: "view2"}), ErrorCodes.InvalidViewDefinition);
assert.commandFailedWithCode(viewsDB.runCommand({create: "view4", viewOn: "collection"}),
                             ErrorCodes.InvalidViewDefinition);
assert.commandFailedWithCode(viewsDB.runCommand({collMod: "view2", viewOn: "view4"}),
                             ErrorCodes.InvalidViewDefinition);

// Checks that dropping a nonexistent view or collection is not affected by an invalid view existing
// in the view catalog.
assert.commandFailedWithCode(viewsDB.runCommand({drop: "view4"}), ErrorCodes.NamespaceNotFound);
assert.commandFailedWithCode(viewsDB.runCommand({listCollections: 1}),
                             ErrorCodes.InvalidViewDefinition);

// Manually remove the invalid view definition from system.views, and then verify that view
// operations work successfully without requiring a server restart.
assert.commandWorked(viewsDB.adminCommand(
    {applyOps: [{op: "d", ns: viewsDB.getName() + ".system.views", o: {_id: "badView"}}]}));
assert.commandWorked(viewsDB.runCommand({find: "view2"}));
assert.commandWorked(viewsDB.runCommand({create: "view4", viewOn: "collection"}));
assert.commandWorked(viewsDB.runCommand({collMod: "view2", viewOn: "view4"}));
assert.commandWorked(viewsDB.runCommand({drop: "view4"}));
assert.commandWorked(viewsDB.runCommand({listCollections: 1}));
MongoRunner.stopMongod(conn);
})();

(function() {
"use script";

// This test makes assertions about the number of sessions, which are not compatible with
// implicit sessions.
TestData.disableImplicitSessions = true;

var res;
var refresh = {refreshLogicalSessionCacheNow: 1};
var startSession = {startSession: 1};

// Start up a standalone server.
var conn = MongoRunner.runMongod();
var admin = conn.getDB("admin");
var config = conn.getDB("config");

// Trigger an initial refresh, as a sanity check.
res = admin.runCommand(refresh);
assert.commandWorked(res, "failed to refresh");

var sessions = [];
for (var i = 0; i < 20; i++) {
    res = admin.runCommand(startSession);
    assert.commandWorked(res, "unable to start session");
    sessions.push(res);
}

res = admin.runCommand(refresh);
assert.commandWorked(res, "failed to refresh");

assert.eq(config.system.sessions.count(), 20, "refresh should have written 20 session records");

var endSessionsIds = [];
for (var i = 0; i < 10; i++) {
    endSessionsIds.push(sessions[i].id);
}
res = admin.runCommand({endSessions: endSessionsIds});
assert.commandWorked(res, "failed to end sessions");

res = admin.runCommand(refresh);
assert.commandWorked(res, "failed to refresh");

assert.eq(config.system.sessions.count(),
          10,
          "endSessions and refresh should result in 10 remaining sessions");

// double delete the remaining 10
endSessionsIds = [];
for (var i = 10; i < 20; i++) {
    endSessionsIds.push(sessions[i].id);
    endSessionsIds.push(sessions[i].id);
}

res = admin.runCommand({endSessions: endSessionsIds});
assert.commandWorked(res, "failed to end sessions");

res = admin.runCommand(refresh);
assert.commandWorked(res, "failed to refresh");

assert.eq(config.system.sessions.count(),
          0,
          "endSessions and refresh should result in 0 remaining sessions");

// delete some sessions that were never created
res = admin.runCommand({
    endSessions: [
        {"id": UUID("bacb219c-214c-47f9-a94a-6c7f434b3bae")},
        {"id": UUID("bacb219c-214c-47f9-a94a-6c7f434b3baf")}
    ]
});

res = admin.runCommand(refresh);
assert.commandWorked(res, "failed to refresh");

// verify that end on the session handle actually ends sessions
{
    var session = conn.startSession();

    assert.commandWorked(session.getDatabase("admin").runCommand({usersInfo: 1}),
                         "do something to tickle the session");
    assert.commandWorked(session.getDatabase("admin").runCommand(refresh), "failed to refresh");
    assert.eq(config.system.sessions.count(), 1, "usersInfo should have written 1 session record");

    session.endSession();
    assert.commandWorked(admin.runCommand(refresh), "failed to refresh");
    assert.eq(config.system.sessions.count(),
              0,
              "endSessions and refresh should result in 0 remaining sessions");
}

MongoRunner.stopMongod(conn);
}());

/**
 * Be sure that an exchange won't deadlock when one of the consumer's buffers is full. Iterates two
 * consumers on an Exchange with a very small buffer. This test was designed to reproduce
 * SERVER-37499.
 * @tags: [
 *   requires_sharding,
 *   uses_transactions,
 * ]
 */
(function() {
// This test manually simulates a session, which is not compatible with implicit sessions.
TestData.disableImplicitSessions = true;

// Start a sharded cluster. For this test, we'll just need to talk to the shard directly.
const st = new ShardingTest({shards: 1, mongos: 1});

const adminDB = st.shard0.getDB("admin");
const session = st.shard0.getDB("test").getMongo().startSession();
const shardDB = session.getDatabase("test");
const coll = shardDB.exchange_in_session;

let bigString = '';
for (let i = 0; i < 20; i++) {
    bigString += 's';
}

// Insert some documents.
const nDocs = 50;
for (let i = 0; i < nDocs; i++) {
    assert.commandWorked(coll.insert({_id: i, bigString: bigString}));
}

session.startTransaction();

// Set up an Exchange with two cursors.
let res = assert.commandWorked(shardDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [],
    exchange: {
        policy: 'keyRange',
        consumers: NumberInt(2),
        key: {_id: 1},
        boundaries: [{a: MinKey}, {a: nDocs / 2}, {a: MaxKey}],
        consumerIds: [NumberInt(0), NumberInt(1)],
        bufferSize: NumberInt(128)
    },
    cursor: {batchSize: 0},
}));

function spawnShellToIterateCursor(cursorId) {
    let code = `const cursor = ${tojson(cursorId)};`;
    code += `const sessionId = ${tojson(session.getSessionId())};`;
    code += `const collName = "${coll.getName()}";`;
    function iterateCursorWithNoDocs() {
        const getMoreCmd = {
            getMore: cursor.id,
            collection: collName,
            batchSize: 4,
            lsid: sessionId,
            txnNumber: NumberLong(0),
            autocommit: false
        };

        let resp = null;
        while (!resp || resp.cursor.id != 0) {
            resp = assert.commandWorked(db.runCommand(getMoreCmd));
        }
    }
    code += `(${iterateCursorWithNoDocs.toString()})();`;
    return startParallelShell(code, st.rs0.getPrimary().port);
}

let parallelShells = [];
for (let curs of res.cursors) {
    parallelShells.push(spawnShellToIterateCursor(curs.cursor));
}

assert.soon(function() {
    for (let waitFn of parallelShells) {
        waitFn();
    }
    return true;
});

assert.commandWorked(session.abortTransaction_forTesting());

st.stop();
})();

(function() {
'use strict';

var runTest = function(compressor) {
    var mongo = MongoRunner.runMongod({networkMessageCompressors: compressor});

    let shell = startParallelShell(function() {
        var collName = 'exhaustCollection';
        var fp = 'beforeCompressingExhaustResponse';
        db[collName].drop();

        const kDocumentCount = 10;
        for (var i = 0; i < kDocumentCount; i++) {
            assert.commandWorked(db.runCommand({insert: collName, documents: [{a: i}]}));
        }

        const kBatchSize = 2;
        const preRes =
            assert.commandWorked(db.adminCommand({configureFailPoint: fp, mode: "alwaysOn"}));

        db.exhaustCollection.find({})
            .batchSize(kBatchSize)
            .addOption(DBQuery.Option.exhaust)
            .toArray();

        const postRes =
            assert.commandWorked(db.adminCommand({configureFailPoint: fp, mode: "off"}));

        // The initial response for find command has kBatchSize docs and the remaining docs comes
        // in batches of kBatchSize in response to the getMore command with the exhaustAllowed bit
        // set.
        const kExpectedDelta = Math.floor((kDocumentCount - kBatchSize) / kBatchSize);
        assert.eq(
            postRes.count - preRes.count, kExpectedDelta, "Exhaust messages are not compressed");
    }, mongo.port, false, "--networkMessageCompressors", compressor);

    shell();

    MongoRunner.stopMongod(mongo);
};

runTest("snappy");
}());

/**
 * Verifies mongos and mongod's behavior for exhaust queries.
 *
 * 'ShardingTest' requires replication.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

const docs = [{a: 1}, {a: 2}, {a: 3}, {a: 4}, {a: 5}, {a: 6}, {a: 7}, {a: 8}];
const kBatchSize = 3;
const kNoOfDocs = docs.length;

[{
    "setUp": () => {
        const conn = MongoRunner.runMongod();
        const db = conn.getDB(jsTestName());
        return {"env": conn, "db": db};
    },
    "tearDown": (env) => MongoRunner.stopMongod(env),
    "verifyThis": (cursor, docIdx, doc) => {
        // Because the first batch is returned from a find command without exhaustAllowed bit,
        // moreToCome bit is not set in reply message.
        const isFirstBatch = docIdx < kBatchSize;

        // The last batch which does not contain the full batch size is returned without moreToCome
        // bit set.
        const isLastBatch = docIdx >= kNoOfDocs - (kNoOfDocs % kBatchSize);

        if (isFirstBatch || isLastBatch) {
            assert(!cursor._hasMoreToCome(), `${docIdx} doc: ${doc}`);
        } else {
            assert(cursor._hasMoreToCome(), `${docIdx} doc: ${doc}`);
        }
    }
},
 {
     "setUp": () => {
         const st = new ShardingTest({shards: 1, config: 1});
         const db = st.s0.getDB(jsTestName());
         return {"env": st, "db": db};
     },
     "tearDown": (env) => env.stop(),
     "verifyThis": (cursor, docIdx, doc) => {
         // Mongos does not support exhaust queries, not by returning an error but by sending reply
         // without moreToCome bit set. So, _hasMoreToCome() is always false.
         assert(!cursor._hasMoreToCome(), `${docIdx} doc: ${doc}`);
     }
 }].forEach(({setUp, tearDown, verifyThis}) => {
    const {env, db} = setUp();

    db.coll.drop();

    assert.commandWorked(db.coll.insert(docs));

    let cursor = db.coll.find().batchSize(kBatchSize).addOption(DBQuery.Option.exhaust);
    let docIdx = 0;
    cursor.forEach(doc => {
        verifyThis(cursor, docIdx, doc);
        ++docIdx;
    });

    tearDown(env);
});
}());

/**
 * Tests that various forms of normal and abnormal shutdown write to the log files as expected.
 * @tags: [
 *   live_record_incompatible,
 *   requires_sharding,
 * ]
 */

(function() {

function makeShutdownByCrashFn(crashHow) {
    return function(conn) {
        var admin = conn.getDB("admin");
        assert.commandWorked(admin.runCommand(
            {configureFailPoint: "crashOnShutdown", mode: "alwaysOn", data: {how: crashHow}}));
        admin.shutdownServer();
    };
}

function makeRegExMatchFn(pattern) {
    return function(text) {
        return pattern.test(text);
    };
}

function testShutdownLogging(launcher, crashFn, matchFn, expectedExitCode) {
    clearRawMongoProgramOutput();
    var conn = launcher.start({});

    function checkOutput() {
        var logContents = rawMongoProgramOutput();
        function printLog() {
            // We can't just return a string because it will be well over the max
            // line length.
            // So we just print manually.
            print("================ BEGIN LOG CONTENTS ==================");
            logContents.split(/\n/).forEach((line) => {
                print(line);
            });
            print("================ END LOG CONTENTS =====================");
            return "";
        }

        assert(matchFn(logContents), printLog);
    }

    crashFn(conn);
    launcher.stop(conn, undefined, {allowedExitCode: expectedExitCode});
    checkOutput();
}

function runAllTests(launcher) {
    const SIGSEGV = 11;
    const SIGABRT = 6;
    testShutdownLogging(launcher, function(conn) {
        conn.getDB('admin').shutdownServer();
    }, makeRegExMatchFn(/Terminating via shutdown command/), MongoRunner.EXIT_CLEAN);

    testShutdownLogging(launcher,
                        makeShutdownByCrashFn('fault'),
                        makeRegExMatchFn(/Invalid access at address[\s\S]*printStackTrace/),
                        -SIGSEGV);

    testShutdownLogging(launcher,
                        makeShutdownByCrashFn('abort'),
                        makeRegExMatchFn(/Got signal[\s\S]*printStackTrace/),
                        -SIGABRT);
}

if (_isWindows()) {
    print("SKIPPING TEST ON WINDOWS");
    return;
}

if (_isAddressSanitizerActive()) {
    print("SKIPPING TEST ON ADDRESS SANITIZER BUILD");
    return;
}

(function testMongod() {
    print("********************\nTesting exit logging in mongod\n********************");

    runAllTests({
        start: function(opts) {
            var actualOpts = {nojournal: ""};
            Object.extend(actualOpts, opts);
            return MongoRunner.runMongod(actualOpts);
        },

        stop: MongoRunner.stopMongod
    });
}());

(function testMongos() {
    print("********************\nTesting exit logging in mongos\n********************");

    var st = new ShardingTest({shards: 1});
    var mongosLauncher = {
        start: function(opts) {
            var actualOpts = {configdb: st._configDB};
            Object.extend(actualOpts, opts);
            return MongoRunner.runMongos(actualOpts);
        },

        stop: MongoRunner.stopMongos
    };

    runAllTests(mongosLauncher);
    st.stop();
}());
}());

/**
 * Tests that $group stage reports memory footprint per accumulator when explain is run with
 * verbosities "executionStats" and "allPlansExecution".
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");  // For getAggPlanStage().

const conn = MongoRunner.runMongod();
const testDB = conn.getDB('test');
const coll = testDB.explain_group_stage_exec_stats;
coll.drop();
const bigStr = Array(1025).toString();  // 1KB of ','
const maxMemoryLimitForGroupStage = 1024 * 300;
const debugBuild = testDB.adminCommand('buildInfo').debug;
const nDocs = 1000;
const nGroups = 50;

const bulk = coll.initializeUnorderedBulkOp();
for (let i = 1; i <= nDocs; i++) {
    bulk.insert({_id: i, a: i, b: i % nGroups, bigStr: bigStr});
}
assert.commandWorked(bulk.execute());

const pipeline = [
    {$match: {a: {$gt: 0}}},
    {$sort: {b: 1}},
    {$group: {_id: "$b", count: {$sum: 1}, push: {$push: "$bigStr"}, set: {$addToSet: "$bigStr"}}},
];

const expectedAccumMemUsages = {
    count: nGroups * 60,
    push: nDocs * 1024,
    set: nGroups * 1024,
};

const expectedTotalMemoryUsage =
    Object.values(expectedAccumMemUsages).reduce((acc, val) => acc + val, 0);
const expectedSpillCount = Math.ceil(expectedTotalMemoryUsage / maxMemoryLimitForGroupStage);

/**
 * Checks that the execution stats in the explain output for a $group stage are as expected.
 * - 'stage' is an explain output of $group stage.
 * - 'expectedAccumMemUsages' is used to check the memory footprint stats for each accumulator.
 * - 'isExecExplain' indicates that the explain output is run with verbosity "executionStats" or
 * "allPlansExecution".
 * - 'expectedSpills' indicates how many times the data was spilled to disk when executing $group
 * stage.
 */
function checkGroupStages(stage, expectedAccumMemUsages, isExecExplain, expectedSpills) {
    // Tracks the memory usage per accumulator in total as 'stages' passed in could be the explain
    // output across a cluster.
    let totalAccumMemoryUsageBytes = 0;
    assert(stage.hasOwnProperty("$group"), stage);

    if (isExecExplain) {
        assert(stage.hasOwnProperty("maxAccumulatorMemoryUsageBytes"), stage);
        const maxAccmMemUsages = stage["maxAccumulatorMemoryUsageBytes"];
        for (const field of Object.keys(maxAccmMemUsages)) {
            totalAccumMemoryUsageBytes += maxAccmMemUsages[field];

            // Ensures that the expected accumulators are all included and the corresponding
            // memory usage is in a reasonable range. Note that in debug mode, data will be
            // spilled to disk every time we add a new value to a pre-existing group.
            if (!debugBuild && expectedAccumMemUsages.hasOwnProperty(field)) {
                assert.gt(maxAccmMemUsages[field], expectedAccumMemUsages[field]);
                assert.lt(maxAccmMemUsages[field], 5 * expectedAccumMemUsages[field]);
            }
        }

        // Don't verify spill count for debug builds, since for debug builds a spill occurs on every
        // duplicate id in a group.
        if (!debugBuild) {
            assert.eq(stage.usedDisk, expectedSpills > 0, stage);
            assert.gte(stage.spills, expectedSpills, stage);
            assert.lte(stage.spills, 2 * expectedSpills, stage);
        }
    } else {
        assert(!stage.hasOwnProperty("usedDisk"), stage);
        assert(!stage.hasOwnProperty("spills"), stage);
        assert(!stage.hasOwnProperty("maxAccumulatorMemoryUsageBytes"), stage);
    }

    // Add some wiggle room to the total memory used compared to the limit parameter since the check
    // for spilling to disk happens after each document is processed.
    if (expectedSpills > 0)
        assert.gt(maxMemoryLimitForGroupStage + 4 * 1024, totalAccumMemoryUsageBytes, stage);
}

let groupStages = getAggPlanStage(coll.explain("executionStats").aggregate(pipeline), "$group");
checkGroupStages(groupStages, expectedAccumMemUsages, true, 0);

groupStages = getAggPlanStage(coll.explain("allPlansExecution").aggregate(pipeline), "$group");
checkGroupStages(groupStages, expectedAccumMemUsages, true, 0);

groupStages = getAggPlanStage(coll.explain("queryPlanner").aggregate(pipeline), "$group");
checkGroupStages(groupStages, {}, false, 0);

// Set MaxMemory low to force spill to disk.
assert.commandWorked(testDB.adminCommand(
    {setParameter: 1, ["internalDocumentSourceGroupMaxMemoryBytes"]: maxMemoryLimitForGroupStage}));

groupStages = getAggPlanStage(
    coll.explain("executionStats").aggregate(pipeline, {"allowDiskUse": true}), "$group");
checkGroupStages(groupStages, {}, true, expectedSpillCount);

groupStages = getAggPlanStage(
    coll.explain("allPlansExecution").aggregate(pipeline, {"allowDiskUse": true}), "$group");
checkGroupStages(groupStages, {}, true, expectedSpillCount);

groupStages = getAggPlanStage(
    coll.explain("queryPlanner").aggregate(pipeline, {"allowDiskUse": true}), "$group");
checkGroupStages(groupStages, {}, false, 0);

MongoRunner.stopMongod(conn);
}());

// Test that explain correctly outputs whether the planner hit or, and, or scan limits.

(function() {
"use strict";
load("jstests/libs/fixture_helpers.js");

const conn = MongoRunner.runMongod({});
const testDB = conn.getDB(jsTestName());
const coll = testDB.planner_index_limit;
coll.drop();

// Test scanLimit.
coll.createIndex({e: 1, s: 1});
let inList = [];
for (let i = 0; i < 250; i++) {
    inList.push(i);
}
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryMaxScansToExplode": 0,
    }
});
const scanResult = coll.find({e: {$in: inList}}).sort({s: 1}).explain();
assert(scanResult.queryPlanner.maxScansToExplodeReached, tojson(scanResult));
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryMaxScansToExplode": 200,
    }
});
coll.drop();

// Test orLimit.
coll.createIndex({common: 1});
coll.createIndex({one: 1});
coll.createIndex({two: 1});
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxOrSolutions": 1,
    }
});
const orResult = coll.find({common: 1, $or: [{one: 0, two: 0}, {one: 1, two: 1}]}).explain();
assert(orResult.queryPlanner.maxIndexedOrSolutionsReached, tojson(orResult));
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxOrSolutions": 10,
    }
});

// Test andLimit.
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxIntersectPerAnd": 1,
    }
});
const andResult = coll.find({common: 1, two: 0, one: 1}).explain();
assert(andResult.queryPlanner.maxIndexedAndSolutionsReached, tojson(andResult));

// Test that andLimit and orLimit will both show in one query.
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxOrSolutions": 1,
    }
});
const comboResult = coll.find({common: 1, one: 10, $or: [{one: 1}, {two: 2}]}).explain();
assert(comboResult.queryPlanner.maxIndexedAndSolutionsReached, tojson(comboResult));
assert(comboResult.queryPlanner.maxIndexedOrSolutionsReached, tojson(comboResult));

// Reset values to defaults.
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxOrSolutions": 10,
    }
});
FixtureHelpers.runCommandOnEachPrimary({
    db: testDB.getSiblingDB("admin"),
    cmdObj: {
        setParameter: 1,
        "internalQueryEnumerationMaxIntersectPerAnd": 3,
    }
});
MongoRunner.stopMongod(conn);
})();

/**
 * Tests the explain command with the maxTimeMS option.
 */
(function() {
"use strict";

const standalone = MongoRunner.runMongod();
assert.neq(null, standalone, "mongod was unable to start up");

const dbName = "test";
const db = standalone.getDB(dbName);
const collName = "explain_max_time_ms";
const coll = db.getCollection(collName);

const destCollName = "explain_max_time_ms_dest";
const mapFn = function() {
    emit(this.i, this.j);
};
const reduceFn = function(key, values) {
    return Array.sum(values);
};

coll.drop();
assert.commandWorked(db.createCollection(collName));

assert.commandWorked(coll.insert({i: 1, j: 1}));
assert.commandWorked(coll.insert({i: 2, j: 1}));
assert.commandWorked(coll.insert({i: 2, j: 2}));

// Set fail point to make sure operations with "maxTimeMS" set will time out.
assert.commandWorked(
    db.adminCommand({configureFailPoint: "maxTimeAlwaysTimeOut", mode: "alwaysOn"}));

for (const verbosity of ["executionStats", "allPlansExecution"]) {
    // Expect explain to time out if "maxTimeMS" is set on the aggregate command.
    assert.commandFailedWithCode(assert.throws(function() {
                                                  coll.explain(verbosity).aggregate(
                                                      [{$match: {i: 1}}], {maxTimeMS: 1});
                                              }),
                                              ErrorCodes.MaxTimeMSExpired);
    // Expect explain to time out if "maxTimeMS" is set on the count command.
    assert.commandFailedWithCode(assert.throws(function() {
                                                  coll.explain(verbosity).count({i: 1},
                                                                                {maxTimeMS: 1});
                                              }),
                                              ErrorCodes.MaxTimeMSExpired);
    // Expect explain to time out if "maxTimeMS" is set on the distinct command.
    assert.commandFailedWithCode(assert.throws(function() {
                                                  coll.explain(verbosity).distinct(
                                                      "i", {}, {maxTimeMS: 1});
                                              }),
                                              ErrorCodes.MaxTimeMSExpired);
    // Expect explain to time out if "maxTimeMS" is set on the find command.
    assert.commandFailedWithCode(assert.throws(function() {
                                                  coll.find().maxTimeMS(1).explain(verbosity);
                                              }),
                                              ErrorCodes.MaxTimeMSExpired);
    assert.commandFailedWithCode(
        assert.throws(function() {
                         coll.explain(verbosity).find().maxTimeMS(1).finish();
                     }),
                     ErrorCodes.MaxTimeMSExpired);
    // Expect explain to time out if "maxTimeMS" is set on the findAndModify command.
    assert.commandFailedWithCode(assert.throws(function() {
                                                  coll.explain(verbosity).findAndModify(
                                                      {update: {$inc: {j: 1}}, maxTimeMS: 1});
                                              }),
                                              ErrorCodes.MaxTimeMSExpired);
    // Expect explain to time out if "maxTimeMS" is set on the mapReduce command.
    assert.commandFailedWithCode(
        assert.throws(function() {
                         coll.explain(verbosity).mapReduce(
                             mapFn, reduceFn, {out: destCollName, maxTimeMS: 1});
                     }),
                     ErrorCodes.MaxTimeMSExpired);
}

// Disable fail point.
assert.commandWorked(db.adminCommand({configureFailPoint: "maxTimeAlwaysTimeOut", mode: "off"}));

MongoRunner.stopMongod(standalone);
})();

/**
 * Test that explain output is correctly truncated when it grows too large.
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");

const dbName = "test";
const collName = jsTestName();
const explainSizeParam = "internalQueryExplainSizeThresholdBytes";

const conn = MongoRunner.runMongod({});
assert.neq(conn, null, "mongod failed to start up");

const testDb = conn.getDB(dbName);
const coll = testDb[collName];
coll.drop();

assert.commandWorked(coll.createIndex({a: 1}));

// Explain output should show a simple IXSCAN => FETCH => SORT plan with no truncation.
let explain = coll.find({a: 1, b: 1}).sort({c: 1}).explain();
let winningPlan = getWinningPlan(explain.queryPlanner);
let sortStage = getPlanStage(winningPlan, "SORT");
assert.neq(sortStage, null, explain);
let fetchStage = getPlanStage(sortStage, "FETCH");
assert.neq(fetchStage, null, explain);
let ixscanStage = getPlanStage(sortStage, "IXSCAN");
assert.neq(ixscanStage, null, explain);

// Calculate the size of explain output's winning plan without the index scan. If the explain size
// threshold is set near this amount, then the IXSCAN stage will need to be truncated.
assert.neq(ixscanStage, null, explain);
const newExplainSize = Object.bsonsize(winningPlan) - Object.bsonsize(ixscanStage) - 10;
assert.gt(newExplainSize, 0);

// Reduce the size at which we start truncating explain output. If we explain the same query again,
// then the FETCH stage should be present, but the IXSCAN stage should be truncated.
assert.commandWorked(testDb.adminCommand({setParameter: 1, [explainSizeParam]: newExplainSize}));

explain = coll.find({a: 1, b: 1}).sort({c: 1}).explain();
assert(planHasStage(testDb, explain, "SORT"), explain);
fetchStage = getPlanStage(getWinningPlan(explain.queryPlanner), "FETCH");
assert.neq(fetchStage, null, explain);
assert(fetchStage.hasOwnProperty("inputStage"), explain);
assert(fetchStage.inputStage.hasOwnProperty("warning"), explain);
assert.eq(
    fetchStage.inputStage.warning, "stats tree exceeded BSON size limit for explain", explain);
assert(!planHasStage(testDb, explain, "IXSCAN"), explain);

MongoRunner.stopMongod(conn);
}());

// This test checks that .explain() reports the correct trial period statistics for a winning plan
// in the "allPlansExecution" section.
(function() {
"use strict";

const dbName = "test";
const collName = jsTestName();

const conn = MongoRunner.runMongod({});
assert.neq(conn, null, "mongod failed to start up");

const db = conn.getDB(dbName);
const coll = db[collName];
coll.drop();

assert.commandWorked(coll.createIndex({a: 1}));
assert.commandWorked(coll.createIndex({b: 1}));

// Configure the server such that the trial period should end after doing 10 reads from storage.
assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryPlanEvaluationWorks: 10}));

assert.commandWorked(coll.insert(Array.from({length: 20}, (v, i) => {
    return {a: 1, b: 1, c: i};
})));

const explain = coll.find({a: 1, b: 1}).sort({c: 1}).explain("allPlansExecution");

// Since there are 20 documents, we expect that the full execution plan reports at least 20 keys
// examined and 20 docs examined.
//
// It is possible that more than 20 keys/docs are examined, because we expect the plan to be closed
// and re-opened during the trial period when running with SBE (which does not clear the execution
// stats from the first open).
assert.gte(explain.executionStats.totalKeysExamined, 20);
assert.gte(explain.executionStats.totalDocsExamined, 20);

// Extract the first plan in the "allPlansExecution" array. The winning plan is always reported
// first.
const winningPlanTrialPeriodStats = explain.executionStats.allPlansExecution[0];

// The number of keys examined and docs examined should both be less than 10, since we configured
// the trial period for each candidate plan to end after 10 storage reads.
assert.lte(winningPlanTrialPeriodStats.totalKeysExamined, 10);
assert.lte(winningPlanTrialPeriodStats.totalDocsExamined, 10);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests where/function can be interrupted through maxTimeMS and query knob.
 */
(function() {
"use strict";

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

let db = conn.getDB("test_where_function_interrupt");
let coll = db.getCollection("foo");

let expensiveFunction = function() {
    sleep(1000);
    return true;
};
assert.commandWorked(coll.insert(Array.from({length: 1000}, _ => ({}))));

let checkInterrupt = function(cursor) {
    let err = assert.throws(function() {
        cursor.itcount();
    }, [], "expected interrupt error due to maxTimeMS being exceeded");
    assert.commandFailedWithCode(
        err, [ErrorCodes.MaxTimeMSExpired, ErrorCodes.Interrupted, ErrorCodes.InternalError]);
};

let tests = [
    {
        // Test that $where can be interrupted with a maxTimeMS of 100 ms.
        timeout: 100,
        query: {$where: expensiveFunction},
        err: checkInterrupt,
    },
    {
        // Test that $function can be interrupted with a maxTimeMS of 100 ms.
        timeout: 100,
        query: {
            $expr: {
                $function: {
                    body: expensiveFunction,
                    args: [],
                    lang: 'js',
                }
            }
        },
        err: checkInterrupt
    },
    {

        // Test that $function can be interrupted by a query knob of 100 ms.
        pre: function() {
            assert.commandWorked(
                db.adminCommand({setParameter: 1, internalQueryJavaScriptFnTimeoutMillis: 100}));
        },
        query: {
            $expr: {
                $function: {
                    body: expensiveFunction,
                    args: [],
                    lang: 'js',
                }
            }
        },
        err: checkInterrupt
    },
];

tests.forEach(function(testCase) {
    if (testCase.pre) {
        testCase.pre();
    }

    let cursor = coll.find(testCase.query);

    if (testCase.timeout) {
        cursor.maxTimeMS(testCase.timeout);
    }
    testCase.err(cursor);
});

MongoRunner.stopMongod(conn);
})();

/**
 * Test that the find command can spill to disk while executing a blocking sort, if the client
 * explicitly allows disk usage.
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");
load("jstests/libs/sbe_util.js");  // For checkSBEEnabled.

// Only allow blocking sort execution to use 100 kB of memory.
const kMaxMemoryUsageBytes = 100 * 1024;

const kNumDocsWithinMemLimit = 70;
const kNumDocsExceedingMemLimit = 100;

const options = {
    setParameter: "internalQueryMaxBlockingSortMemoryUsageBytes=" + kMaxMemoryUsageBytes
};
const conn = MongoRunner.runMongod(options);
assert.neq(null, conn, "mongod was unable to start up with options: " + tojson(options));

const testDb = conn.getDB("test");
const collection = testDb.external_sort_find;
const isSBEEnabled = checkSBEEnabled(testDb);

// Construct a document that is just over 1 kB.
const charToRepeat = "-";
const templateDoc = {
    padding: charToRepeat.repeat(1024)
};

// Insert data into the collection without exceeding the memory threshold.
for (let i = 0; i < kNumDocsWithinMemLimit; ++i) {
    templateDoc.sequenceNumber = i;
    assert.commandWorked(collection.insert(templateDoc));
}

// We should be able to successfully sort the collection with or without disk use allowed.
assert.eq(kNumDocsWithinMemLimit, collection.find().sort({sequenceNumber: -1}).itcount());
assert.eq(kNumDocsWithinMemLimit,
          collection.find().sort({sequenceNumber: -1}).allowDiskUse().itcount());

function getFindSortStats(allowDiskUse) {
    let cursor = collection.find().sort({sequenceNumber: -1});
    if (allowDiskUse) {
        cursor = cursor.allowDiskUse();
    }
    const stageName = isSBEEnabled ? "sort" : "SORT";
    const explain = cursor.explain("executionStats");
    return getPlanStage(explain.executionStats.executionStages, stageName);
}

function getAggregationSortStatsPipelineOptimizedAway() {
    const cursor = collection.explain("executionStats").aggregate([{$sort: {sequenceNumber: -1}}], {
        allowDiskUse: true
    });
    const stageName = isSBEEnabled ? "sort" : "SORT";

    // Use getPlanStage() instead of getAggPlanStage(), because the pipeline is optimized away for
    // this query.
    return getPlanStage(cursor.executionStats.executionStages, stageName);
}

function getAggregationSortStatsForPipeline() {
    const cursor =
        collection.explain("executionStats")
            .aggregate([{$_internalInhibitOptimization: {}}, {$sort: {sequenceNumber: -1}}],
                       {allowDiskUse: true});
    return getAggPlanStage(cursor, "$sort");
}

// Explain should report that less than 100 kB of memory was used, and we did not spill to disk.
// Test that this result is the same whether or not 'allowDiskUse' is set.
let sortStats = getFindSortStats(false);
assert.eq(sortStats.memLimit, kMaxMemoryUsageBytes);
assert.lt(sortStats.totalDataSizeSorted, kMaxMemoryUsageBytes);
assert.eq(sortStats.usedDisk, false);
sortStats = getFindSortStats(true);
assert.eq(sortStats.memLimit, kMaxMemoryUsageBytes);
assert.lt(sortStats.totalDataSizeSorted, kMaxMemoryUsageBytes);
assert.eq(sortStats.usedDisk, false);

// Add enough data to exceed the memory threshold.
for (let i = kNumDocsWithinMemLimit; i < kNumDocsExceedingMemLimit; ++i) {
    templateDoc.sequenceNumber = i;
    assert.commandWorked(collection.insert(templateDoc));
}

// The sort should fail if disk use is not allowed, but succeed if disk use is allowed.
assert.commandFailedWithCode(
    testDb.runCommand({find: collection.getName(), sort: {sequenceNumber: -1}}),
    ErrorCodes.QueryExceededMemoryLimitNoDiskUseAllowed);
assert.eq(kNumDocsExceedingMemLimit,
          collection.find().sort({sequenceNumber: -1}).allowDiskUse().itcount());

// Explain should report that the SORT stage failed if disk use is not allowed.
sortStats = getFindSortStats(false);

// SBE will not report the 'failed' field within sort stats.
if (isSBEEnabled) {
    assert(!sortStats.hasOwnProperty("failed"), sortStats);
} else {
    assert.eq(sortStats.failed, true, sortStats);
}
assert.eq(sortStats.usedDisk, false);
assert.lt(sortStats.totalDataSizeSorted, kMaxMemoryUsageBytes);
assert(!sortStats.inputStage.hasOwnProperty("failed"));

// Explain should report that >=100 kB of memory was used, and that we spilled to disk.
sortStats = getFindSortStats(true);
assert.eq(sortStats.memLimit, kMaxMemoryUsageBytes);
assert.gte(sortStats.totalDataSizeSorted, kMaxMemoryUsageBytes);
assert.eq(sortStats.usedDisk, true);

// If disk use is not allowed but there is a limit, we should be able to avoid exceeding the memory
// limit.
assert.eq(kNumDocsWithinMemLimit,
          collection.find().sort({sequenceNumber: -1}).limit(kNumDocsWithinMemLimit).itcount());

// Create a view on top of the collection. When a find command is run against the view without disk
// use allowed, the command should fail with the expected error code. When the find command allows
// disk use, however, the command should succeed.
assert.commandWorked(testDb.createView("identityView", collection.getName(), []));
const identityView = testDb.identityView;
assert.commandFailedWithCode(
    testDb.runCommand({find: identityView.getName(), sort: {sequenceNumber: -1}}),
    ErrorCodes.QueryExceededMemoryLimitNoDiskUseAllowed);
assert.eq(kNumDocsExceedingMemLimit,
          identityView.find().sort({sequenceNumber: -1}).allowDiskUse().itcount());

// Computing the expected number of spills based on the approximate document size. At this moment
// the number of documents in the collection is 'kNumDocsExceedingMemLimit'
const approximateDocumentSize = Object.bsonsize(templateDoc) + 20;
const expectedNumberOfSpills =
    Math.ceil(approximateDocumentSize * kNumDocsExceedingMemLimit / kMaxMemoryUsageBytes);

// Verify that performing sorting on the collection using find that exceeds the memory limit results
// in 'expectedNumberOfSpills' when allowDiskUse is set to true.
const findExternalSortStats = getFindSortStats(true);
assert.eq(findExternalSortStats.usedDisk, true, findExternalSortStats);
assert.eq(findExternalSortStats.spills, expectedNumberOfSpills, findExternalSortStats);

// Verify that performing sorting on the collection using aggregate that exceeds the memory limit
// and can be optimized away results in 'expectedNumberOfSpills' when allowDiskUse is set to true.
const aggregationExternalSortStatsForNonPipeline = getAggregationSortStatsPipelineOptimizedAway();
assert.eq(aggregationExternalSortStatsForNonPipeline.usedDisk,
          true,
          aggregationExternalSortStatsForNonPipeline);
assert.eq(aggregationExternalSortStatsForNonPipeline.spills,
          expectedNumberOfSpills,
          aggregationExternalSortStatsForNonPipeline);

// Verify that performing sorting on the collection using aggregate pipeline that exceeds the memory
// limit and can not be optimized away results in 'expectedNumberOfSpills' when allowDiskUse is set
// to true.
const aggregationExternalSortStatsForPipeline = getAggregationSortStatsForPipeline();
assert.eq(aggregationExternalSortStatsForPipeline.usedDisk,
          true,
          aggregationExternalSortStatsForPipeline);
assert.eq(aggregationExternalSortStatsForPipeline.spills,
          expectedNumberOfSpills,
          aggregationExternalSortStatsForPipeline);

MongoRunner.stopMongod(conn);
}());

/**
 * Test that 'failGetMoreAfterCursorCheckout' works.
 * @tags: [requires_replication, requires_journaling]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const testDB = rst.getPrimary().getDB(jsTestName());
const coll = testDB.test;

// Insert a set of test documents into the collection.
for (let i = 0; i < 10; ++i) {
    assert.commandWorked(coll.insert({_id: i}));
}

// Perform the test for both 'find' and 'aggregate' cursors.
for (let testCursor of [coll.find({}).sort({_id: 1}).batchSize(2),
                        coll.aggregate([{$sort: {_id: 1}}], {cursor: {batchSize: 2}})]) {
    // Activate the failpoint and set the exception that it will throw.
    assert.commandWorked(testDB.adminCommand({
        configureFailPoint: "failGetMoreAfterCursorCheckout",
        mode: "alwaysOn",
        data: {"errorCode": ErrorCodes.ShutdownInProgress}
    }));

    // Consume the documents from the first batch, leaving the cursor open.
    assert.docEq(testCursor.next(), {_id: 0});
    assert.docEq(testCursor.next(), {_id: 1});
    assert.eq(testCursor.objsLeftInBatch(), 0);

    // Issue a getMore and confirm that the failpoint throws the expected exception.
    const getMoreRes = assert.throws(() => testCursor.hasNext() && testCursor.next());
    assert.commandFailedWithCode(getMoreRes, ErrorCodes.ShutdownInProgress);

    // Disable the failpoint.
    assert.commandWorked(
        testDB.adminCommand({configureFailPoint: "failGetMoreAfterCursorCheckout", mode: "off"}));
}

rst.stopSet();
}());

(function() {
'use strict';

var conn = MongoRunner.runMongod();
var admin = conn.getDB('admin');
var db = conn.getDB('test');

var fpCmd = {
    configureFailPoint: 'failCommand',
    mode: {times: 1},
    data: {
        failCommands: ['insert'],
        blockConnection: true,
        blockTimeMS: 1000,
    },
};

assert.commandWorked(admin.runCommand(fpCmd));

var insertCmd = {
    insert: 'coll',
    documents: [{x: 1}],
    maxTimeMS: 100,
};

assert.commandFailedWithCode(db.runCommand(insertCmd), ErrorCodes.MaxTimeMSExpired);

MongoRunner.stopMongod(conn);
})();

(function() {
"use strict";

load("jstests/libs/parallelTester.js");

const conn = MongoRunner.runMongod();
assert.neq(null, conn);
const kDbName = "test_failcommand_noparallel";
const db = conn.getDB(kDbName);

// Test times when closing connection.
// Use distinct because it is rarely used by internal operations, making it less likely unrelated
// activity triggers the failpoint.
assert.commandWorked(db.adminCommand({
    configureFailPoint: "failCommand",
    mode: {times: 2},
    data: {
        closeConnection: true,
        failCommands: ["distinct"],
    }
}));
assert.throws(() => db.runCommand({distinct: "c", key: "_id"}));
assert.throws(() => db.runCommand({distinct: "c", key: "_id"}));
assert.commandWorked(db.runCommand({distinct: "c", key: "_id"}));
assert.commandWorked(db.adminCommand({configureFailPoint: "failCommand", mode: "off"}));

// Test the blockConnection patterns.
jsTest.log("Test validation of blockConnection fields");
{
    // 'blockTimeMS' is required when 'blockConnection' is true.
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            blockConnection: true,
            failCommands: ["hello"],
        }
    }));
    assert.commandFailedWithCode(db.runCommand({hello: 1}), ErrorCodes.InvalidOptions);

    // 'blockTimeMS' must be non-negative.
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            blockConnection: true,
            blockTimeMS: -100,
            failCommands: ["hello"],
        }
    }));
    assert.commandFailedWithCode(db.runCommand({hello: 1}), ErrorCodes.InvalidOptions);

    assert.commandWorked(db.adminCommand({configureFailPoint: "failCommand", mode: "off"}));
}

// Insert a test document.
assert.commandWorked(db.runCommand({
    insert: "c",
    documents: [{_id: 'block_test', run_id: 0}],
}));

/**
 * Returns the "run_id" of the test document.
 */
function checkRunId() {
    const ret = db.runCommand({find: "c", filter: {_id: 'block_test'}});
    assert.commandWorked(ret);

    const doc = ret["cursor"]["firstBatch"][0];
    return doc["run_id"];
}

/**
 * Runs update to increment the "run_id" of the test document by one.
 */
function incrementRunId() {
    assert.commandWorked(db.runCommand({
        update: "c",
        updates: [{q: {_id: 'block_test'}, u: {$inc: {run_id: 1}}}],
    }));
}

/**
 * Starts and returns a thread for updating the test document by incrementing the
 * "run_id" by one.
 */
function startIncrementRunIdThread() {
    const latch = new CountDownLatch(1);
    let thread = new Thread(function(connStr, dbName, latch) {
        jsTest.log("Sending update");

        const client = new Mongo(connStr);
        const db = client.getDB(dbName);
        latch.countDown();
        assert.commandWorked(db.runCommand({
            update: "c",
            updates: [{q: {_id: 'block_test'}, u: {$inc: {run_id: 1}}}],
        }));

        jsTest.log("Successfully applied update");
    }, conn.name, kDbName, latch);
    thread.start();
    latch.await();
    return thread;
}

assert.eq(checkRunId(), 0);
const kLargeBlockTimeMS = 60 * 1000;

jsTest.log("Test that only commands listed in failCommands block");
{
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            blockConnection: true,
            blockTimeMS: kLargeBlockTimeMS,
            failCommands: ["update"],
        }
    }));
    let thread = startIncrementRunIdThread();

    // Check that other commands get through.
    assert.commandWorked(db.runCommand({hello: 1}));
    assert.eq(checkRunId(), 0);

    // Wait for the blocked update to get through.
    thread.join();
    assert.soon(() => {
        return checkRunId() == 1;
    });

    assert.commandWorked(db.adminCommand({configureFailPoint: "failCommand", mode: "off"}));
}

jsTest.log("Test command changes");
{
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            blockConnection: true,
            blockTimeMS: kLargeBlockTimeMS,
            failCommands: ["update", "insert"],
        }
    }));
    assert.eq(checkRunId(), 1);

    // Drop update from the command list and verify that the update gets through.
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            blockConnection: true,
            blockTimeMS: kLargeBlockTimeMS,
            failCommands: ["insert"],
        }
    }));
    incrementRunId();
    assert.eq(checkRunId(), 2);

    assert.commandWorked(db.adminCommand({configureFailPoint: "failCommand", mode: "off"}));
}

MongoRunner.stopMongod(conn);
}());

// Tests that manipulating the featureCompatibilityVersion document in admin.system.version changes
// the value of the featureCompatibilityVersion server parameter.

(function() {
"use strict";

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");

let adminDB = conn.getDB("admin");

// Initially the featureCompatibilityVersion is latestFCV.
checkFCV(adminDB, latestFCV);

// Updating the featureCompatibilityVersion document changes the featureCompatibilityVersion
// server parameter.
for (let oldVersion of [lastLTSFCV, lastContinuousFCV]) {
    // Fully downgraded to oldVersion.
    assert.commandWorked(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                       {$set: {version: oldVersion}}));
    checkFCV(adminDB, oldVersion);

    // Upgrading to latest.
    assert.commandWorked(
        adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                      {$set: {version: oldVersion, targetVersion: latestFCV}}));
    checkFCV(adminDB, oldVersion, latestFCV);

    // Downgrading to oldVersion.
    assert.commandWorked(adminDB.system.version.update(
        {_id: "featureCompatibilityVersion"},
        {$set: {version: oldVersion, targetVersion: oldVersion, previousVersion: latestFCV}}));
    checkFCV(adminDB, oldVersion, oldVersion);

    // When present, "previousVersion" will always be the latestFCV.
    assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                            {$set: {previousVersion: oldVersion}}),
                              4926901);
    checkFCV(adminDB, oldVersion, oldVersion);

    // Downgrading FCV must have a 'previousVersion' field.
    assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                            {$unset: {previousVersion: true}}),
                              4926902);
    checkFCV(adminDB, oldVersion, oldVersion);

    // Reset to latestFCV.
    assert.commandWorked(adminDB.system.version.update(
        {_id: "featureCompatibilityVersion"},
        {$set: {version: latestFCV}, $unset: {targetVersion: true, previousVersion: true}}));
    checkFCV(adminDB, latestFCV);
}

if (lastLTSFCV !== lastContinuousFCV) {
    // Test that we can update from last-lts to last-continuous when the two versions are not equal.
    // This upgrade path is exposed to users through the setFeatureCompatibilityVersion command with
    // fromConfigServer: true.
    assert.commandWorked(adminDB.system.version.update(
        {_id: "featureCompatibilityVersion"},
        {$set: {version: lastLTSFCV, targetVersion: lastContinuousFCV}}));
    checkFCV(adminDB, lastLTSFCV, lastContinuousFCV);

    // Reset to latestFCV.
    assert.commandWorked(adminDB.system.version.update(
        {_id: "featureCompatibilityVersion"},
        {$set: {version: latestFCV}, $unset: {targetVersion: true, previousVersion: true}}));
    checkFCV(adminDB, latestFCV);
}

// Updating the featureCompatibilityVersion document with an invalid version fails.
assert.writeErrorWithCode(
    adminDB.system.version.update({_id: "featureCompatibilityVersion"}, {$set: {version: "3.2"}}),
    4926900);
checkFCV(adminDB, latestFCV);

// Updating the featureCompatibilityVersion document with an invalid targetVersion fails.
assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                        {$set: {targetVersion: lastLTSFCV}}),
                          4926904);
checkFCV(adminDB, latestFCV);

assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                        {$set: {targetVersion: lastContinuousFCV}}),
                          4926904);
checkFCV(adminDB, latestFCV);

assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                        {$set: {targetVersion: latestFCV}}),
                          4926904);
checkFCV(adminDB, latestFCV);

// Setting an unknown field.
assert.writeErrorWithCode(adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                                        {$set: {unknownField: "unknown"}}),
                          40415);
checkFCV(adminDB, latestFCV);

MongoRunner.stopMongod(conn);
}());

// Tests that interrupting a filemd5 command while the PlanExecutor is yielded will correctly clean
// up the PlanExecutor without crashing the server. This test was designed to reproduce
// SERVER-35361.
(function() {
"use strict";

load("jstests/libs/curop_helpers.js");  // For waitForCurOpByFailPoint().

const conn = MongoRunner.runMongod();
assert.neq(null, conn);
const db = conn.getDB("test");
db.fs.chunks.drop();
assert.commandWorked(db.fs.chunks.insert({files_id: 1, n: 0, data: new BinData(0, "64string")}));
assert.commandWorked(db.fs.chunks.insert({files_id: 1, n: 1, data: new BinData(0, "test")}));
db.fs.chunks.createIndex({files_id: 1, n: 1});

const kFailPointName = "waitInFilemd5DuringManualYield";
assert.commandWorked(db.adminCommand({configureFailPoint: kFailPointName, mode: "alwaysOn"}));

const failingMD5Shell =
    startParallelShell(() => assert.commandFailedWithCode(db.runCommand({filemd5: 1, root: "fs"}),
                                                          ErrorCodes.Interrupted),
                       conn.port);

// Wait for filemd5 to manually yield and hang.
const curOps =
    waitForCurOpByFailPoint(db, "test.fs.chunks", kFailPointName, {"command.filemd5": 1});

const opId = curOps[0].opid;

// Kill the operation, then disable the failpoint so the command recognizes it's been killed.
assert.commandWorked(db.killOp(opId));
assert.commandWorked(db.adminCommand({configureFailPoint: kFailPointName, mode: "off"}));

failingMD5Shell();
MongoRunner.stopMongod(conn);
}());

//
// Run 'find' by UUID while renaming a collection concurrently. See SERVER-34615.
//

(function() {
"use strict";
const dbName = "do_concurrent_rename";
const collName = "collA";
const otherName = "collB";
const repeatFind = 100;
load("jstests/noPassthrough/libs/concurrent_rename.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");
jsTestLog("Create collection.");
let findRenameDB = conn.getDB(dbName);
findRenameDB.dropDatabase();
assert.commandWorked(findRenameDB.runCommand({"create": collName}));
assert.commandWorked(findRenameDB.runCommand({insert: collName, documents: [{fooField: 'FOO'}]}));

let infos = findRenameDB.getCollectionInfos();
let uuid = infos[0].info.uuid;
const findCmd = {
    "find": uuid
};

// Assert 'find' command by UUID works.
assert.commandWorked(findRenameDB.runCommand(findCmd));

jsTestLog("Start parallel shell for renames.");
let renameShell =
    startParallelShell(funWithArgs(doRenames, dbName, collName, otherName), conn.port);

// Wait until we receive confirmation that the parallel shell has started.
assert.soon(() => conn.getDB("test").await_data.findOne({_id: "signal parent shell"}) !== null,
            "Expected parallel shell to insert a document.");

jsTestLog("Start 'find' commands.");
while (conn.getDB("test").await_data.findOne({_id: "rename has ended"}) == null) {
    for (let i = 0; i < repeatFind; i++) {
        let res = findRenameDB.runCommand(findCmd);

        // This is an acceptable transient error until SERVER-31695 has been completed.
        if (res.code === ErrorCodes.QueryPlanKilled) {
            print("Ignoring transient QueryPlanKilled error: " + res.errmsg);
            continue;
        }
        assert.commandWorked(res, "could not run " + tojson(findCmd));
        let cursor = new DBCommandCursor(findRenameDB, res);
        let errMsg =
            "expected more data from command " + tojson(findCmd) + ", with result " + tojson(res);
        assert(cursor.hasNext(), errMsg);
        let doc = cursor.next();
        assert.eq(doc.fooField, "FOO");
        assert(!cursor.hasNext(), "expected to have exhausted cursor for results " + tojson(res));
    }
}
renameShell();
MongoRunner.stopMongod(conn);
}());

/**
 * Tests that flow control outputs a log when it is maximally engaged on some cadence.
 *
 * @tags: [
 *   requires_flow_control,
 *   requires_majority_read_concern,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const replSet = new ReplSetTest({name: "flow_control_logging", nodes: 3});
replSet.startSet({
    setParameter: {
        flowControlSamplePeriod: 1,  // Increase resolution to detect lag in a light write workload.
        flowControlWarnThresholdSeconds: 1,
        // Configure flow control to engage after one second of lag.
        flowControlTargetLagSeconds: 1,
        flowControlThresholdLagPercentage: 1,
        // Use a speedy no-op writer to avoid needing a robust background writer.
        writePeriodicNoops: true,
        periodicNoopIntervalSecs:
            2  // replSet.initiate() can hang with a one second interval for reasons.
    }
});
replSet.initiate();

// Stop replication which will pin the commit point.
for (let sec of replSet.getSecondaries()) {
    assert.commandWorked(sec.adminCommand(
        {configureFailPoint: "pauseBatchApplicationAfterWritingOplogEntries", mode: "alwaysOn"}));
}

const timeoutMilliseconds = 30 * 1000;
// The test has stopped replication and the primary's no-op writer is configured to create an
// oplog entry every other second. Once the primary notices the sustainer rate is not moving, it
// should start logging a warning once per second. This check waits for two log messages to make
// sure the appropriate state variables are being reset.
checkLog.containsWithAtLeastCount(replSet.getPrimary(),
                                  "Flow control is engaged and the sustainer point is not moving.",
                                  2,
                                  timeoutMilliseconds);

// Restart replication so the replica set will shut down.
for (let sec of replSet.getSecondaries()) {
    assert.commandWorked(sec.adminCommand(
        {configureFailPoint: "pauseBatchApplicationAfterWritingOplogEntries", mode: "off"}));
}

replSet.stopSet();
})();

/**
 * The FTDC connection pool stats from mongos are a different structure than the connPoolStats
 * command, verify its contents.
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */
load('jstests/libs/ftdc.js');

(function() {
'use strict';
const testPath = MongoRunner.toRealPath('ftdc_dir');
const st = new ShardingTest({
    shards: 2,
    mongos: {
        s0: {setParameter: {diagnosticDataCollectionDirectoryPath: testPath}},
    }
});

const admin = st.s0.getDB('admin');
const stats = verifyGetDiagnosticData(admin).connPoolStats;
jsTestLog(`Diagnostic connection pool stats: ${tojson(stats)}`);

assert(stats.hasOwnProperty('totalInUse'));
assert(stats.hasOwnProperty('totalAvailable'));
assert(stats.hasOwnProperty('totalCreated'));
assert(stats.hasOwnProperty('totalRefreshing'));
assert("hello" in stats["replicaSetMonitor"]);
const helloStats = stats["replicaSetMonitor"]["hello"];
assert(helloStats.hasOwnProperty('currentlyActive'));
assert("getHostAndRefresh" in stats["replicaSetMonitor"]);
const getHostStats = stats["replicaSetMonitor"]["getHostAndRefresh"];
assert(getHostStats.hasOwnProperty('currentlyActive'));

// The connPoolStats command reply has "hosts", but FTDC's stats do not.
assert(!stats.hasOwnProperty('hosts'));

// Check a few properties, without attempting to be thorough.
assert(stats.connectionsInUsePerPool.hasOwnProperty('NetworkInterfaceTL-ShardRegistry'));
assert(stats.replicaSetPingTimesMillis.hasOwnProperty(st.configRS.name));

st.stop();
})();

/**
 * Verify the FTDC metrics for mirrored reads.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
load('jstests/libs/ftdc.js');

(function() {
'use strict';

const kDbName = "mirrored_reads_ftdc_test";
const kCollName = "test";
const kOperations = 100;

function getMirroredReadsStats(rst) {
    return rst.getPrimary().getDB(kDbName).serverStatus({mirroredReads: 1}).mirroredReads;
}

function getDiagnosticData(rst) {
    let db = rst.getPrimary().getDB('admin');
    const stats = verifyGetDiagnosticData(db).serverStatus;
    assert(stats.hasOwnProperty('mirroredReads'));
    return stats.mirroredReads;
}

function sendAndCheckReads(rst) {
    let seenBeforeReads = getMirroredReadsStats(rst).seen;

    jsTestLog(`Sending ${kOperations} reads to primary`);
    for (var i = 0; i < kOperations; ++i) {
        rst.getPrimary().getDB(kDbName).runCommand({find: kCollName, filter: {}});
    }

    jsTestLog("Verifying reads were seen by the maestro");
    let seenAfterReads = getMirroredReadsStats(rst).seen;
    assert.lte(seenBeforeReads + kOperations, seenAfterReads);
}

function activateFailPoint(rst) {
    const db = rst.getPrimary().getDB(kDbName);
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "mirrorMaestroExpectsResponse",
        mode: "alwaysOn",
    }));
}

const rst = new ReplSetTest({nodes: 3});
rst.startSet();
rst.initiateWithHighElectionTimeout();

// Mirror every mirror-able command.
assert.commandWorked(
    rst.getPrimary().adminCommand({setParameter: 1, mirrorReads: {samplingRate: 1.0}}));

jsTestLog("Verifying diagnostic collection for mirrored reads");
{
    let statsBeforeReads = getDiagnosticData(rst);
    // The following metrics are not included by default.
    assert(!statsBeforeReads.hasOwnProperty('resolved'));
    assert(!statsBeforeReads.hasOwnProperty('resolvedBreakdown'));

    let seenBeforeReads = statsBeforeReads.seen;
    sendAndCheckReads(rst);
    assert.soon(() => {
        let seenAfterReads = getDiagnosticData(rst).seen;
        jsTestLog(`Seen ${seenAfterReads} mirrored reads so far`);
        return seenBeforeReads + kOperations <= seenAfterReads;
    }, "Failed to update FTDC metrics within time limit", 30000);
}

jsTestLog("Verifying diagnostic collection when mirrorMaestroExpectsResponse");
{
    activateFailPoint(rst);
    assert.soon(() => {
        return getDiagnosticData(rst).hasOwnProperty('resolved');
    }, "Failed to find 'resolved' in mirrored reads FTDC metrics within time limit", 30000);
    let resolvedBeforeReads = getDiagnosticData(rst).resolved;
    sendAndCheckReads(rst);
    assert.soon(() => {
        let resolvedAfterReads = getDiagnosticData(rst).resolved;
        jsTestLog(`Mirrored ${resolvedAfterReads} reads so far`);
        // There are two secondaries, so `kOperations * 2` reads must be resolved.
        return resolvedBeforeReads + kOperations * 2 <= resolvedAfterReads;
    }, "Failed to update extended FTDC metrics within time limit", 10000);
}

rst.stopSet();
})();

/**
 * Test that verifies FTDC works in mongos.
 * @tags: [
 *   requires_sharding,
 * ]
 */
load('jstests/libs/ftdc.js');

(function() {
'use strict';
let testPath1 = MongoRunner.toRealPath('ftdc_setdir1');
let testPath2 = MongoRunner.toRealPath('ftdc_setdir2');
let testPath3 = MongoRunner.toRealPath('ftdc_setdir3');
// SERVER-30394: Use a directory relative to the current working directory.
let testPath4 = 'ftdc_setdir4/';
let testLog3 = testPath3 + "mongos_ftdc.log";
let testLog4 = testPath4 + "mongos_ftdc.log";

// Make the log file directory for mongos.
mkdir(testPath3);
mkdir(testPath4);

// Startup 3 mongos:
// 1. Normal MongoS with no log file to verify FTDC can be startup at runtime with a path.
// 2. MongoS with explict diagnosticDataCollectionDirectoryPath setParameter at startup.
// 3. MongoS with log file to verify automatic FTDC path computation works.
let st = new ShardingTest({
    shards: 1,
    mongos: {
        s0: {verbose: 0},
        s1: {setParameter: {diagnosticDataCollectionDirectoryPath: testPath2}},
        s2: {logpath: testLog3},
        s3: {logpath: testLog4}
    }
});

let admin1 = st.s0.getDB('admin');
let admin2 = st.s1.getDB('admin');
let admin3 = st.s2.getDB('admin');
let admin4 = st.s3.getDB('admin');

function setParam(admin, obj) {
    var ret = admin.runCommand(Object.extend({setParameter: 1}, obj));
    return ret;
}

function getParam(admin, field) {
    var q = {getParameter: 1};
    q[field] = 1;

    var ret = admin.runCommand(q);
    assert.commandWorked(ret);
    return ret[field];
}

// Verify FTDC can be started at runtime.
function verifyFTDCDisabledOnStartup() {
    jsTestLog("Running verifyFTDCDisabledOnStartup");
    verifyCommonFTDCParameters(admin1, false);

    // 1. Try to enable and fail
    assert.commandFailed(setParam(admin1, {"diagnosticDataCollectionEnabled": 1}));

    // 2. Set path and succeed
    assert.commandWorked(setParam(admin1, {"diagnosticDataCollectionDirectoryPath": testPath1}));

    // 3. Set path again and fail
    assert.commandFailed(setParam(admin1, {"diagnosticDataCollectionDirectoryPath": testPath1}));

    // 4. Enable successfully
    assert.commandWorked(setParam(admin1, {"diagnosticDataCollectionEnabled": 1}));

    // 5. Validate getDiagnosticData returns FTDC data now
    jsTestLog("Verifying FTDC getDiagnosticData");
    verifyGetDiagnosticData(admin1);
}

// Verify FTDC is already running if there was a path set at startup.
function verifyFTDCStartsWithPath() {
    jsTestLog("Running verifyFTDCStartsWithPath");
    verifyCommonFTDCParameters(admin2, true);

    // 1. Set path fail
    assert.commandFailed(setParam(admin2, {"diagnosticDataCollectionDirectoryPath": testPath2}));

    // 2. Enable successfully
    assert.commandWorked(setParam(admin2, {"diagnosticDataCollectionEnabled": 1}));

    // 3. Validate getDiagnosticData returns FTDC data now
    jsTestLog("Verifying FTDC getDiagnosticData");
    verifyGetDiagnosticData(admin2);
}

function normpath(path) {
    // On Windows, strip the drive path because MongoRunner.toRealPath() returns a Unix Path
    // while FTDC returns a Windows path.
    return path.replace(/\\/g, "/").replace(/\w:/, "");
}

// Verify FTDC is already running if there was a path set at startup.
function verifyFTDCStartsWithLogFile() {
    jsTestLog("Running verifyFTDCStartsWithLogFile");
    verifyCommonFTDCParameters(admin3, true);

    // 1. Verify that path is computed correctly.
    let computedPath = getParam(admin3, "diagnosticDataCollectionDirectoryPath");
    assert.eq(normpath(computedPath), normpath(testPath3 + "mongos_ftdc.diagnostic.data"));

    // 2. Set path fail
    assert.commandFailed(setParam(admin3, {"diagnosticDataCollectionDirectoryPath": testPath3}));

    // 3. Enable successfully
    assert.commandWorked(setParam(admin3, {"diagnosticDataCollectionEnabled": 1}));

    // 4. Validate getDiagnosticData returns FTDC data now
    jsTestLog("Verifying FTDC getDiagnosticData");
    verifyGetDiagnosticData(admin3);
}

// Verify FTDC is already running if there is a relative log file path.
function verifyFTDCStartsWithRelativeLogFile() {
    jsTestLog("Running verifyFTDCStartsWithRelativeLogFile");
    verifyCommonFTDCParameters(admin4, true);

    // Skip verification of diagnosticDataCollectionDirectoryPath because it relies on comparing
    // cwd vs dbPath.

    // 1. Enable successfully
    assert.commandWorked(setParam(admin4, {"diagnosticDataCollectionEnabled": 1}));

    // 2. Validate getDiagnosticData returns FTDC data now
    jsTestLog("Verifying FTDC getDiagnosticData");
    verifyGetDiagnosticData(admin4);
}

verifyFTDCDisabledOnStartup();
verifyFTDCStartsWithPath();
verifyFTDCStartsWithLogFile();
verifyFTDCStartsWithRelativeLogFile();

st.stop();
})();

// validate command line ftdc parameter parsing

(function() {
'use strict';
var m = MongoRunner.runMongod({setParameter: "diagnosticDataCollectionPeriodMillis=101"});

// Check the defaults are correct
//
function getparam(field) {
    var q = {getParameter: 1};
    q[field] = 1;

    var ret = m.getDB("admin").runCommand(q);
    return ret[field];
}

assert.eq(getparam("diagnosticDataCollectionPeriodMillis"), 101);
MongoRunner.stopMongod(m);
})();

//
// Integration test of the geo code
//
// Basically, this tests adds a random number of docs with a random number of points,
// given a 2d environment of random precision which is either randomly earth-like or of
// random bounds, and indexes these points after a random amount of points have been added
// with a random number of additional fields which correspond to whether the documents are
// in randomly generated circular, spherical, box, and box-polygon shapes (and exact),
// queried randomly from a set of query types.  Each point is randomly either and object
// or array, and all points and document data fields are nested randomly in arrays (or not).
//
// We approximate the user here as a random function :-)
//
// These random point fields can then be tested against all types of geo queries using these random
// shapes.
//
// Tests can be easily reproduced by getting the test number from the output directly before a
// test fails, and hard-wiring that as the test number.
//

(function() {
"use strict";

load("jstests/libs/geo_math.js");

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start.");
const db = conn.getDB("test");

var randEnvironment = function() {
    // Normal earth environment
    if (Random.rand() < 0.5) {
        return {max: 180, min: -180, bits: Math.floor(Random.rand() * 32) + 1, earth: true};
    }

    var scales = [0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000];
    var scale = scales[Math.floor(Random.rand() * scales.length)];
    var offset = Random.rand() * scale;

    var max = Random.rand() * scale + offset;
    var min = -Random.rand() * scale + offset;
    var bits = Math.floor(Random.rand() * 32) + 1;

    return {max: max, min: min, bits: bits, earth: false};
};

var randPoint = function(env, query) {
    if (query && Random.rand() > 0.5)
        return query.exact;

    if (env.earth)
        return [Random.rand() * 360 - 180, Random.rand() * 180 - 90];

    var range = env.max - env.min;
    return [Random.rand() * range + env.min, Random.rand() * range + env.min];
};

var randLocType = function(loc, wrapIn) {
    return randLocTypes([loc], wrapIn)[0];
};

var randLocTypes = function(locs, wrapIn) {
    var rLocs = [];

    for (var i = 0; i < locs.length; i++) {
        rLocs.push(locs[i]);
    }

    if (wrapIn) {
        var wrappedLocs = [];
        for (var i = 0; i < rLocs.length; i++) {
            var wrapper = {};
            wrapper[wrapIn] = rLocs[i];
            wrappedLocs.push(wrapper);
        }

        return wrappedLocs;
    }

    return rLocs;
};

var randDataType = function() {
    var scales = [1, 10, 100, 1000, 10000];
    var docScale = scales[Math.floor(Random.rand() * scales.length)];
    var locScale = scales[Math.floor(Random.rand() * scales.length)];

    var numDocs = 40000;
    var maxLocs = 40000;
    // Make sure we don't blow past our test resources
    while (numDocs * maxLocs > 40000) {
        numDocs = Math.floor(Random.rand() * docScale) + 1;
        maxLocs = Math.floor(Random.rand() * locScale) + 1;
    }

    return {numDocs: numDocs, maxLocs: maxLocs};
};

function computexscandist(latDegrees, maxDistDegrees) {
    // See s2cap.cc
    //
    // Compute the range of longitudes covered by the cap.  We use the law
    // of sines for spherical triangles.  Consider the triangle ABC where
    // A is the north pole, B is the center of the cap, and C is the point
    // of tangency between the cap boundary and a line of longitude.  Then
    // C is a right angle, and letting a,b,c denote the sides opposite A,B,C,
    // we have sin(a)/sin(A) = sin(c)/sin(C), or sin(A) = sin(a)/sin(c).
    // Here "a" is the cap angle, and "c" is the colatitude (90 degrees
    // minus the latitude).  This formula also works for negative latitudes.
    //
    // Angle A is the difference of longitudes of B and C.
    var sin_c = Math.cos(deg2rad(latDegrees));
    var sin_a = Math.sin(deg2rad(maxDistDegrees));
    if (sin_a > sin_c) {
        // Double floating number error, return invalid distance
        return 180;
    }
    var angleA = Math.asin(sin_a / sin_c);
    return rad2deg(angleA);
}

function errorMarginForPoint(env) {
    if (!env.bits) {
        return 0.01;
    }
    var scalingFactor = Math.pow(2, env.bits);
    return ((env.max - env.min) / scalingFactor) * Math.sqrt(2);
}

function pointIsOK(startPoint, radius, env) {
    var error = errorMarginForPoint(env);
    var distDegrees = rad2deg(radius) + error;
    // TODO SERVER-24440: Points close to the north and south poles may fail to be returned by
    // $nearSphere queries answered using a "2d" index. We have empirically found that points
    // with latitudes between 89 and 90 degrees are potentially affected by this issue, so we
    // additionally reject any coordinates with a latitude that falls within that range.
    if ((startPoint[1] + distDegrees > 89) || (startPoint[1] - distDegrees < -89)) {
        return false;
    }
    var xscandist = computexscandist(startPoint[1], distDegrees);
    return (startPoint[0] + xscandist < 180) && (startPoint[0] - xscandist > -180);
}

var randQuery = function(env) {
    var center = randPoint(env);

    var sphereRadius = -1;
    var sphereCenter = null;
    if (env.earth) {
        // Get a start point that doesn't require wrapping
        // TODO: Are we a bit too aggressive with wrapping issues?
        var i;
        for (i = 0; i < 5; i++) {
            sphereRadius = Random.rand() * 45 * Math.PI / 180;
            sphereCenter = randPoint(env);
            if (pointIsOK(sphereCenter, sphereRadius, env)) {
                break;
            }
        }
        if (i == 5)
            sphereRadius = -1;
    }

    var box = [randPoint(env), randPoint(env)];

    var boxPoly = [
        [box[0][0], box[0][1]],
        [box[0][0], box[1][1]],
        [box[1][0], box[1][1]],
        [box[1][0], box[0][1]]
    ];

    if (box[0][0] > box[1][0]) {
        var swap = box[0][0];
        box[0][0] = box[1][0];
        box[1][0] = swap;
    }

    if (box[0][1] > box[1][1]) {
        var swap = box[0][1];
        box[0][1] = box[1][1];
        box[1][1] = swap;
    }

    return {
        center: center,
        radius: box[1][0] - box[0][0],
        exact: randPoint(env),
        sphereCenter: sphereCenter,
        sphereRadius: sphereRadius,
        box: box,
        boxPoly: boxPoly
    };
};

var resultTypes = {
    "exact": function(loc) {
        return query.exact[0] == loc[0] && query.exact[1] == loc[1];
    },
    "center": function(loc) {
        return Geo.distance(query.center, loc) <= query.radius;
    },
    "box": function(loc) {
        return loc[0] >= query.box[0][0] && loc[0] <= query.box[1][0] &&
            loc[1] >= query.box[0][1] && loc[1] <= query.box[1][1];
    },
    "sphere": function(loc) {
        return (query.sphereRadius >= 0
                    ? (Geo.sphereDistance(query.sphereCenter, loc) <= query.sphereRadius)
                    : false);
    },
    "poly": function(loc) {
        return loc[0] >= query.box[0][0] && loc[0] <= query.box[1][0] &&
            loc[1] >= query.box[0][1] && loc[1] <= query.box[1][1];
    }
};

var queryResults = function(locs, query, results) {
    if (!results["center"]) {
        for (var type in resultTypes) {
            results[type] = {docsIn: 0, docsOut: 0, locsIn: 0, locsOut: 0};
        }
    }

    var indResults = {};
    for (var type in resultTypes) {
        indResults[type] = {docIn: false, locsIn: 0, locsOut: 0};
    }

    for (var type in resultTypes) {
        var docIn = false;
        for (var i = 0; i < locs.length; i++) {
            if (resultTypes[type](locs[i])) {
                results[type].locsIn++;
                indResults[type].locsIn++;
                indResults[type].docIn = true;
            } else {
                results[type].locsOut++;
                indResults[type].locsOut++;
            }
        }
        if (indResults[type].docIn)
            results[type].docsIn++;
        else
            results[type].docsOut++;
    }

    return indResults;
};

var randQueryAdditions = function(doc, indResults) {
    for (var type in resultTypes) {
        var choice = Random.rand();
        if (Random.rand() < 0.25)
            doc[type] = (indResults[type].docIn ? {docIn: "yes"} : {docIn: "no"});
        else if (Random.rand() < 0.5)
            doc[type] = (indResults[type].docIn ? {docIn: ["yes"]} : {docIn: ["no"]});
        else if (Random.rand() < 0.75)
            doc[type] = (indResults[type].docIn ? [{docIn: "yes"}] : [{docIn: "no"}]);
        else
            doc[type] = (indResults[type].docIn ? [{docIn: ["yes"]}] : [{docIn: ["no"]}]);
    }
};

var randIndexAdditions = function(indexDoc) {
    for (var type in resultTypes) {
        if (Random.rand() < 0.5)
            continue;

        var choice = Random.rand();
        if (Random.rand() < 0.5)
            indexDoc[type] = 1;
        else
            indexDoc[type + ".docIn"] = 1;
    }
};

var randYesQuery = function() {
    var choice = Math.floor(Random.rand() * 7);
    if (choice == 0)
        return {$ne: "no"};
    else if (choice == 1)
        return "yes";
    else if (choice == 2)
        return /^yes/;
    else if (choice == 3)
        return {$in: ["good", "yes", "ok"]};
    else if (choice == 4)
        return {$exists: true};
    else if (choice == 5)
        return {$nin: ["bad", "no", "not ok"]};
    else if (choice == 6)
        return {$not: /^no/};
};

var locArray = function(loc) {
    if (loc.x)
        return [loc.x, loc.y];
    if (!loc.length)
        return [loc[0], loc[1]];
    return loc;
};

var locsArray = function(locs) {
    if (locs.loc) {
        const arr = [];
        for (var i = 0; i < locs.loc.length; i++)
            arr.push(locArray(locs.loc[i]));
        return arr;
    } else {
        const arr = [];
        for (var i = 0; i < locs.length; i++)
            arr.push(locArray(locs[i].loc));
        return arr;
    }
};

var minBoxSize = function(env, box) {
    return env.bucketSize * Math.pow(2, minBucketScale(env, box));
};

var minBucketScale = function(env, box) {
    if (box.length && box[0].length)
        box = [box[0][0] - box[1][0], box[0][1] - box[1][1]];

    if (box.length)
        box = Math.max(box[0], box[1]);

    print(box);
    print(env.bucketSize);

    return Math.ceil(Math.log(box / env.bucketSize) / Math.log(2));
};

// TODO:  Add spherical $uniqueDocs tests
var numTests = 100;

// Our seed will change every time this is run, but
// each individual test will be reproducible given
// that seed and test number
var seed = new Date().getTime();
// seed = 175 + 288 + 12

for (var test = 0; test < numTests; test++) {
    Random.srand(seed + test);
    // Random.srand( 42240 )
    // Random.srand( 7344 )
    var t = db.testAllGeo;
    t.drop();

    print("Generating test environment #" + test);
    var env = randEnvironment();
    // env.bits = 11
    var query = randQuery(env);
    var data = randDataType();
    // data.numDocs = 5; data.maxLocs = 1;
    var paddingSize = Math.floor(Random.rand() * 10 + 1);
    var results = {};
    var totalPoints = 0;
    print("Calculating target results for " + data.numDocs + " docs with max " + data.maxLocs +
          " locs ");

    var bulk = t.initializeUnorderedBulkOp();
    for (var i = 0; i < data.numDocs; i++) {
        var numLocs = Math.floor(Random.rand() * data.maxLocs + 1);
        totalPoints += numLocs;

        var multiPoint = [];
        for (var p = 0; p < numLocs; p++) {
            var point = randPoint(env, query);
            multiPoint.push(point);
        }

        var indResults = queryResults(multiPoint, query, results);

        var doc;
        // Nest the keys differently
        if (Random.rand() < 0.5)
            doc = {locs: {loc: randLocTypes(multiPoint)}};
        else
            doc = {locs: randLocTypes(multiPoint, "loc")};

        randQueryAdditions(doc, indResults);

        doc._id = i;
        bulk.insert(doc);
    }
    assert.commandWorked(bulk.execute());

    var indexDoc = {"locs.loc": "2d"};
    randIndexAdditions(indexDoc);

    // "earth" is used to drive test setup and not a valid createIndex option or required at
    // this point. It must be removed before calling createIndex().
    delete env.earth;

    assert.commandWorked(t.createIndex(indexDoc, env));

    var padding = "x";
    for (var i = 0; i < paddingSize; i++)
        padding = padding + padding;

    print(padding);

    printjson({
        seed: seed,
        test: test,
        env: env,
        query: query,
        data: data,
        results: results,
        paddingSize: paddingSize
    });

    // exact
    print("Exact query...");
    assert.eq(
        results.exact.docsIn,
        t.find({"locs.loc": randLocType(query.exact), "exact.docIn": randYesQuery()}).count());

    // $center
    print("Center query...");
    print("Min box : " + minBoxSize(env, query.radius));
    assert.eq(results.center.docsIn,
              t.find({
                   "locs.loc": {$within: {$center: [query.center, query.radius], $uniqueDocs: 1}},
                   "center.docIn": randYesQuery()
               }).count());

    print("Center query update...");
    var res = t.update({
        "locs.loc": {$within: {$center: [query.center, query.radius], $uniqueDocs: true}},
        "center.docIn": randYesQuery()
    },
                       {$set: {centerPaddingA: padding}},
                       false,
                       true);
    assert.eq(results.center.docsIn, res.nModified);

    if (query.sphereRadius >= 0) {
        print("Center sphere query...");
        // $centerSphere
        assert.eq(
            results.sphere.docsIn,
            t.find({
                 "locs.loc": {$within: {$centerSphere: [query.sphereCenter, query.sphereRadius]}},
                 "sphere.docIn": randYesQuery()
             }).count());

        print("Center sphere query update...");
        res = t.update({
            "locs.loc": {
                $within:
                    {$centerSphere: [query.sphereCenter, query.sphereRadius], $uniqueDocs: true}
            },
            "sphere.docIn": randYesQuery()
        },
                       {$set: {spherePaddingA: padding}},
                       false,
                       true);
        assert.eq(results.sphere.docsIn, res.nModified);
    }

    // $box
    print("Box query...");
    assert.eq(results.box.docsIn, t.find({
                                       "locs.loc": {$within: {$box: query.box, $uniqueDocs: true}},
                                       "box.docIn": randYesQuery()
                                   }).count());

    // $polygon
    print("Polygon query...");
    assert.eq(results.poly.docsIn, t.find({
                                        "locs.loc": {$within: {$polygon: query.boxPoly}},
                                        "poly.docIn": randYesQuery()
                                    }).count());

    // $near
    print("Near query...");
    assert.eq(results.center.docsIn,
              t.find({"locs.loc": {$near: query.center, $maxDistance: query.radius}}).count(true),
              "Near query: center: " + query.center + "; radius: " + query.radius +
                  "; docs: " + results.center.docsIn + "; locs: " + results.center.locsIn);

    if (query.sphereRadius >= 0) {
        print("Near sphere query...");
        // $centerSphere
        assert.eq(
            results.sphere.docsIn,
            t.find({
                 "locs.loc": {$nearSphere: query.sphereCenter, $maxDistance: query.sphereRadius}
             }).count(true),
            "Near sphere query: sphere center: " + query.sphereCenter +
                "; radius: " + query.sphereRadius + "; docs: " + results.sphere.docsIn +
                "; locs: " + results.sphere.locsIn);
    }

    // $geoNear aggregation stage.
    const aggregationLimit = 2 * results.center.docsIn;
    if (aggregationLimit > 0) {
        var output = t.aggregate([
                          {
                              $geoNear: {
                                  near: query.center,
                                  maxDistance: query.radius,
                                  includeLocs: "pt",
                                  distanceField: "dis",
                              }
                          },
                          {$limit: aggregationLimit}
                      ]).toArray();

        const errmsg = {
            limit: aggregationLimit,
            center: query.center,
            radius: query.radius,
            docs: results.center.docsIn,
            locs: results.center.locsIn,
            actualResult: output
        };
        assert.eq(results.center.docsIn, output.length, tojson(errmsg));

        let lastDistance = 0;
        for (var i = 0; i < output.length; i++) {
            var retDistance = output[i].dis;
            assert.close(retDistance, Geo.distance(locArray(query.center), output[i].pt));
            assert.lte(retDistance, query.radius);
            assert.gte(retDistance, lastDistance);
            lastDistance = retDistance;
        }
    }

    // $polygon
    print("Polygon remove...");
    res =
        t.remove({"locs.loc": {$within: {$polygon: query.boxPoly}}, "poly.docIn": randYesQuery()});
    assert.eq(results.poly.docsIn, res.nRemoved);
}

MongoRunner.stopMongod(conn);
})();

// Test sanity of geo queries with a lot of points

(function() {
"use strict";
const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start.");
const db = conn.getDB("test");

var maxFields = 3;

for (var fields = 1; fields < maxFields; fields++) {
    var coll = db.testMnyPts;
    coll.drop();

    var totalPts = 500 * 1000;

    var bulk = coll.initializeUnorderedBulkOp();
    // Add points in a 100x100 grid
    for (var i = 0; i < totalPts; i++) {
        var ii = i % 10000;

        var doc = {loc: [ii % 100, Math.floor(ii / 100)]};

        // Add fields with different kinds of data
        for (var j = 0; j < fields; j++) {
            var field = null;

            if (j % 3 == 0) {
                // Make half the points not searchable
                field = "abcdefg" + (i % 2 == 0 ? "h" : "");
            } else if (j % 3 == 1) {
                field = new Date();
            } else {
                field = true;
            }

            doc["field" + j] = field;
        }

        bulk.insert(doc);
    }
    assert.commandWorked(bulk.execute());

    // Create the query for the additional fields
    const queryFields = {};
    for (var j = 0; j < fields; j++) {
        var field = null;

        if (j % 3 == 0) {
            field = "abcdefg";
        } else if (j % 3 == 1) {
            field = {$lte: new Date()};
        } else {
            field = true;
        }

        queryFields["field" + j] = field;
    }

    coll.createIndex({loc: "2d"});

    // Check that quarter of points in each quadrant
    for (var i = 0; i < 4; i++) {
        var x = i % 2;
        var y = Math.floor(i / 2);

        var box = [[0, 0], [49, 49]];
        box[0][0] += (x == 1 ? 50 : 0);
        box[1][0] += (x == 1 ? 50 : 0);
        box[0][1] += (y == 1 ? 50 : 0);
        box[1][1] += (y == 1 ? 50 : 0);

        // Now only half of each result comes back
        assert.eq(totalPts / (4 * 2),
                  coll.find(Object.extend({loc: {$within: {$box: box}}}, queryFields)).count());
        assert.eq(totalPts / (4 * 2),
                  coll.find(Object.extend({loc: {$within: {$box: box}}}, queryFields)).itcount());
    }

    // Check that half of points in each half
    for (var i = 0; i < 2; i++) {
        var box = [[0, 0], [49, 99]];
        box[0][0] += (i == 1 ? 50 : 0);
        box[1][0] += (i == 1 ? 50 : 0);

        assert.eq(totalPts / (2 * 2),
                  coll.find(Object.extend({loc: {$within: {$box: box}}}, queryFields)).count());
        assert.eq(totalPts / (2 * 2),
                  coll.find(Object.extend({loc: {$within: {$box: box}}}, queryFields)).itcount());
    }

    // Check that all but corner set of points in radius
    var circle = [[0, 0], (100 - 1) * Math.sqrt(2) - 0.25];

    // All [99,x] pts are field0 : "abcdefg"
    assert.eq(totalPts / 2 - totalPts / (100 * 100),
              coll.find(Object.extend({loc: {$within: {$center: circle}}}, queryFields)).count());
    assert.eq(totalPts / 2 - totalPts / (100 * 100),
              coll.find(Object.extend({loc: {$within: {$center: circle}}}, queryFields)).itcount());
}

MongoRunner.stopMongod(conn);
})();

// this tests all points using $near
var db;
(function() {
"use strict";
load("jstests/libs/geo_near_random.js");

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start.");
db = conn.getDB("test");

var test = new GeoNearRandomTest("weekly.geo_near_random1");

test.insertPts(1000);

test.testPt([0, 0]);
test.testPt(test.mkPt());
test.testPt(test.mkPt());
test.testPt(test.mkPt());
test.testPt(test.mkPt());

MongoRunner.stopMongod(conn);
})();

// this tests 1% of all points using $near and $nearSphere
var db;
(function() {
"use strict";
load("jstests/libs/geo_near_random.js");

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start.");
db = conn.getDB("test");

var test = new GeoNearRandomTest("weekly.geo_near_random2");

test.insertPts(50000);

const opts = {
    sphere: 0,
    nToTest: test.nPts * 0.01
};
test.testPt([0, 0], opts);
test.testPt(test.mkPt(), opts);
test.testPt(test.mkPt(), opts);
test.testPt(test.mkPt(), opts);
test.testPt(test.mkPt(), opts);

opts.sphere = 1;
test.testPt([0, 0], opts);
test.testPt(test.mkPt(0.8), opts);
test.testPt(test.mkPt(0.8), opts);
test.testPt(test.mkPt(0.8), opts);
test.testPt(test.mkPt(0.8), opts);

MongoRunner.stopMongod(conn);
})();

// Checks that global histogram counters for collections are updated as we expect.
// @tags: [
//   requires_replication,
// ]

(function() {
"use strict";
var name = "operationalLatencyHistogramTest";

var mongo = MongoRunner.runMongod();
var testDB = mongo.getDB("test");
var testColl = testDB[name + "coll"];

testColl.drop();

function getHistogramStats() {
    return testDB.serverStatus({opLatencies: {histograms: 1}}).opLatencies;
}

var lastHistogram = getHistogramStats();

// Checks that the difference in the histogram is what we expect, and also
// accounts for the serverStatus command itself.
function checkHistogramDiff(reads, writes, commands) {
    var thisHistogram = getHistogramStats();
    assert.eq(thisHistogram.reads.ops - lastHistogram.reads.ops, reads);
    assert.eq(thisHistogram.writes.ops - lastHistogram.writes.ops, writes);
    // Running the server status itself will increment command stats by one.
    assert.eq(thisHistogram.commands.ops - lastHistogram.commands.ops, commands + 1);
    return thisHistogram;
}

// Insert
var numRecords = 100;
for (var i = 0; i < numRecords; i++) {
    assert.commandWorked(testColl.insert({_id: i}));
}
lastHistogram = checkHistogramDiff(0, numRecords, 0);

// Update
for (var i = 0; i < numRecords; i++) {
    assert.commandWorked(testColl.update({_id: i}, {x: i}));
}
lastHistogram = checkHistogramDiff(0, numRecords, 0);

// Find
var cursors = [];
for (var i = 0; i < numRecords; i++) {
    cursors[i] = testColl.find({x: {$gte: i}}).batchSize(2);
    assert.eq(cursors[i].next()._id, i);
}
lastHistogram = checkHistogramDiff(numRecords, 0, 0);

// GetMore
for (var i = 0; i < numRecords / 2; i++) {
    // Trigger two getmore commands.
    assert.eq(cursors[i].next()._id, i + 1);
    assert.eq(cursors[i].next()._id, i + 2);
    assert.eq(cursors[i].next()._id, i + 3);
    assert.eq(cursors[i].next()._id, i + 4);
}
lastHistogram = checkHistogramDiff(numRecords, 0, 0);

// KillCursors
// The last cursor has no additional results, hence does not need to be closed.
for (var i = 0; i < numRecords - 1; i++) {
    cursors[i].close();
}
lastHistogram = checkHistogramDiff(0, 0, numRecords - 1);

// Remove
for (var i = 0; i < numRecords; i++) {
    assert.commandWorked(testColl.remove({_id: i}));
}
lastHistogram = checkHistogramDiff(0, numRecords, 0);

// Upsert
for (var i = 0; i < numRecords; i++) {
    assert.commandWorked(testColl.update({_id: i}, {x: i}, {upsert: 1}));
}
lastHistogram = checkHistogramDiff(0, numRecords, 0);

// Aggregate
for (var i = 0; i < numRecords; i++) {
    testColl.aggregate([{$match: {x: i}}, {$group: {_id: "$x"}}]);
}
lastHistogram = checkHistogramDiff(numRecords, 0, 0);

// Count
for (var i = 0; i < numRecords; i++) {
    testColl.count({x: i});
}
lastHistogram = checkHistogramDiff(numRecords, 0, 0);

// FindAndModify
testColl.findAndModify({query: {}, update: {pt: {type: "Point", coordinates: [0, 0]}}});
lastHistogram = checkHistogramDiff(0, 1, 0);

// CreateIndex
assert.commandWorked(testColl.createIndex({pt: "2dsphere"}));
lastHistogram = checkHistogramDiff(0, 0, 1);

// $geoNear aggregation stage
assert.commandWorked(testDB.runCommand({
    aggregate: testColl.getName(),
    pipeline: [{
        $geoNear: {
            near: {type: "Point", coordinates: [0, 0]},
            spherical: true,
            distanceField: "dist",
        }
    }],
    cursor: {},
}));
lastHistogram = checkHistogramDiff(1, 0, 0);

// GetIndexes
testColl.getIndexes();
lastHistogram = checkHistogramDiff(0, 0, 1);

// Reindex
assert.commandWorked(testColl.reIndex());
lastHistogram = checkHistogramDiff(0, 0, 1);

// DropIndex
assert.commandWorked(testColl.dropIndex({pt: "2dsphere"}));
lastHistogram = checkHistogramDiff(0, 0, 1);

// Explain
testColl.explain().find().next();
lastHistogram = checkHistogramDiff(0, 0, 1);

// CollStats
assert.commandWorked(testDB.runCommand({collStats: testColl.getName()}));
lastHistogram = checkHistogramDiff(0, 0, 1);

// CollMod
assert.commandWorked(testDB.runCommand({collStats: testColl.getName(), validationLevel: "off"}));
lastHistogram = checkHistogramDiff(0, 0, 1);

// Compact
var commandResult = testDB.runCommand({compact: testColl.getName()});
// If storage engine supports compact, it should count as a command.
if (!commandResult.ok) {
    assert.commandFailedWithCode(commandResult, ErrorCodes.CommandNotSupported);
}
lastHistogram = checkHistogramDiff(0, 0, 1);

// DataSize
testColl.dataSize();
lastHistogram = checkHistogramDiff(0, 0, 1);

// PlanCache
testColl.getPlanCache().list();
lastHistogram = checkHistogramDiff(1, 0, 0);

// ServerStatus
assert.commandWorked(testDB.serverStatus());
lastHistogram = checkHistogramDiff(0, 0, 1);

// WhatsMyURI
assert.commandWorked(testColl.runCommand("whatsmyuri"));
lastHistogram = checkHistogramDiff(0, 0, 1);

// Test non-command.
assert.commandFailed(testColl.runCommand("IHopeNobodyEverMakesThisACommand"));
lastHistogram = checkHistogramDiff(0, 0, 1);
MongoRunner.stopMongod(mongo);
}());

// Checks that the global histogram counter for transactions are updated as we expect.
// @tags: [requires_replication, uses_transactions]
(function() {
"use strict";

// Set up the replica set.
const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();

// Set up the test database.
const dbName = "test";
const collName = "global_transaction_latency_histogram";

const testDB = primary.getDB(dbName);
testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
assert.commandWorked(testDB.runCommand({create: collName, writeConcern: {w: "majority"}}));

// Start the session.
const sessionOptions = {
    causalConsistency: false
};
const session = testDB.getMongo().startSession(sessionOptions);
const sessionDb = session.getDatabase(dbName);
const sessionColl = sessionDb[collName];

function getHistogramStats() {
    return testDB.serverStatus({opLatencies: {histograms: 1}}).opLatencies;
}

// Checks that the actual value is within a minimum on the bound of the expected value. All
// arguments must be in the same units.
function assertLowerBound(expected, actual, bound) {
    assert.gte(actual, expected - bound);
}

// This function checks the diff between the last histogram and the current histogram, not the
// absolute values.
function checkHistogramDiff(lastHistogram, thisHistogram, fields) {
    for (let key in fields) {
        if (fields.hasOwnProperty(key)) {
            assert.eq(thisHistogram[key].ops - lastHistogram[key].ops, fields[key]);
        }
    }
    return thisHistogram;
}

// This function checks the diff between the last histogram's accumulated transactions latency
// and this histogram's accumulated transactions latency is within a reasonable bound of what
// we expect.
function checkHistogramLatencyDiff(lastHistogram, thisHistogram, sleepTime) {
    let latencyDiff = thisHistogram.transactions.latency - lastHistogram.transactions.latency;
    // Check the bound in microseconds, which is the unit the latency is in. We do not check
    // upper bound because of unknown extra server latency.
    assertLowerBound(sleepTime * 1000, latencyDiff, 50000);
    return thisHistogram;
}

let lastHistogram = getHistogramStats();

// Verify the base stats are correct.
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 0, "writes": 0, "commands": 1, "transactions": 0});

// Test histogram increments on a successful transaction. "commitTransaction" and "serverStatus"
// commands are counted towards the "commands" counter.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "insert-1"}));
assert.commandWorked(session.commitTransaction_forTesting());
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 0, "writes": 1, "commands": 2, "transactions": 1});

// Test histogram increments on aborted transaction due to error (duplicate insert).
session.startTransaction();
assert.commandFailedWithCode(sessionColl.insert({_id: "insert-1"}), ErrorCodes.DuplicateKey);
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 0, "writes": 1, "commands": 1, "transactions": 1});

// Ensure that the transaction was aborted on failure.
assert.commandFailedWithCode(session.commitTransaction_forTesting(), ErrorCodes.NoSuchTransaction);
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 0, "writes": 0, "commands": 2, "transactions": 0});

// Test histogram increments on an aborted transaction. "abortTransaction" command is counted
// towards the "commands" counter.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "insert-2"}));
assert.commandWorked(session.abortTransaction_forTesting());
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 0, "writes": 1, "commands": 2, "transactions": 1});

// Test histogram increments on a multi-statement committed transaction.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "insert-3"}));
assert.commandWorked(sessionColl.insert({_id: "insert-4"}));
assert.eq(sessionColl.find({_id: "insert-1"}).itcount(), 1);
assert.commandWorked(session.commitTransaction_forTesting());
lastHistogram = checkHistogramDiff(lastHistogram,
                                   getHistogramStats(),
                                   {"reads": 1, "writes": 2, "commands": 2, "transactions": 1});

// Test that the cumulative transaction latency counter is updated appropriately after a
// sequence of back-to-back 200 ms transactions.
const sleepTime = 200;
for (let i = 0; i < 3; i++) {
    session.startTransaction();
    assert.eq(sessionColl.find({_id: "insert-1"}).itcount(), 1);
    sleep(sleepTime);
    assert.commandWorked(session.commitTransaction_forTesting());
    lastHistogram = checkHistogramLatencyDiff(lastHistogram, getHistogramStats(), sleepTime);
}

session.endSession();
rst.stopSet();
}());

// Test that sharded $graphLookup can resolve sharded views correctly.
// @tags: [requires_sharding, requires_fcv_51]
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertArrayEq.
load("jstests/libs/log.js");                  // For findMatchingLogLines.

const sharded = new ShardingTest({
    mongos: 1,
    shards: [{verbose: 3}, {verbose: 3}, {verbose: 3}],
    config: 1,
});
assert(sharded.adminCommand({enableSharding: "test"}));

const isShardedLookupEnabled =
    sharded.s.adminCommand({getParameter: 1, featureFlagShardedLookup: 1})
        .featureFlagShardedLookup.value;
if (!isShardedLookupEnabled) {
    sharded.stop();
    return;
}

const testDBName = "test";
const testDB = sharded.getDB(testDBName);
sharded.ensurePrimaryShard(testDBName, sharded.shard0.shardName);

// Create 'docs' collection which will be backed by shard 1 from which we will run aggregations.
const docs = testDB.docs;
docs.drop();
const docsDocs = [
    {_id: 1, shard_key: "shard1", name: "Carter", subject: "Astrophysics"},
    {_id: 2, shard_key: "shard1", name: "Jackson", subject: "Archaeology"},
    {_id: 3, shard_key: "shard1", name: "Jones", subject: "Archaeology"},
    {_id: 4, shard_key: "shard1", name: "Mann", subject: "Theoretical Physics"},
    {_id: 5, shard_key: "shard1", name: "Mann", subject: "Xenobiology"},
];
assert.commandWorked(docs.createIndex({shard_key: 1}));
assert.commandWorked(docs.insertMany(docsDocs));
assert(sharded.s.adminCommand({shardCollection: docs.getFullName(), key: {shard_key: 1}}));

// Create first 'subjects' collection which will be backed by shard 2.
const subjects = testDB.subjects;
subjects.drop();
const subjectsDocs = [
    {_id: 1, shard_key: "shard2", name: "Science"},
    {_id: 2, shard_key: "shard2", name: "Biology", parent: "Science"},
    {_id: 3, shard_key: "shard2", name: "Physics", parent: "Science"},
    {_id: 4, shard_key: "shard2", name: "Anthropology", parent: "Humanities"},
    {_id: 5, shard_key: "shard2", name: "Astrophysics", parent: "Physics"},
    {_id: 6, shard_key: "shard2", name: "Archaeology", parent: "Anthropology"},
    {_id: 7, shard_key: "shard2", name: "Theoretical Physics", parent: "Physics"},
    {_id: 8, shard_key: "shard2", name: "Xenobiology", parent: "Biology"},
    {_id: 9, shard_key: "shard2", name: "Humanities"},
];
assert.commandWorked(subjects.createIndex({shard_key: 1}));
assert.commandWorked(subjects.insertMany(subjectsDocs));
assert(sharded.s.adminCommand({shardCollection: subjects.getFullName(), key: {shard_key: 1}}));

let testCount = 0;
function getMatchingLogsForTestRun(logs, fields) {
    let foundTest = false;

    // Filter out any logs that happened before the current aggregation.
    function getLogsForTestRun(log) {
        if (foundTest) {
            return true;
        }
        const m = findMatchingLogLine([log], {comment: "test " + testCount});
        if (m !== null) {
            foundTest = true;
        }
        return foundTest;
    }

    // Pick only those remaining logs which match the input 'fields'.
    return [...findMatchingLogLines(logs.filter(getLogsForTestRun), fields)];
}

function getShardedViewExceptions(shard) {
    const shardLog = assert.commandWorked(sharded[shard].adminCommand({getLog: "global"})).log;
    return ["test.docs", "test.subjects"].map(ns => {
        return {ns: ns, count: [...getMatchingLogsForTestRun(shardLog, {id: 5865400, ns})].length};
    });
}

function testGraphLookupView({collection, pipeline, expectedResults, expectedExceptions}) {
    assertArrayEq({
        actual: collection.aggregate(pipeline, {comment: "test " + testCount}).toArray(),
        expected: expectedResults
    });
    if (expectedExceptions) {
        // Count how many CommandOnShardedViewNotSupported exceptions we get and verify that they
        // match the number we were expecting.
        const exceptionCounts = getShardedViewExceptions(expectedExceptions.shard);
        for (const actualEx of exceptionCounts) {
            const ns = actualEx.ns;
            const actualCount = actualEx.count;
            const expectedCount = expectedExceptions[ns];
            assert(actualCount == expectedCount,
                   "expected: " + expectedCount + " exceptions for ns " + ns + ", actually got " +
                       actualCount + " exceptions.");
        }
    }
    testCount++;
}

function checkView(viewName, expected) {
    assertArrayEq({actual: testDB[viewName].find({}).toArray(), expected});
}

function moveChunksByShardKey(collection, shard) {
    assert.commandWorked(testDB.adminCommand({
        moveChunk: collection.getFullName(),
        find: {shard_key: shard},
        to: sharded[shard].shardName,
        _waitForDelete: true
    }));
}

// In order to trigger CommandOnShardedViewNotSupportedOnMongod exceptions where a shard cannot
// resolve a view definition, ensure that:
// - 'docs' is backed by shard 1.
// - 'subjects' is backed by shard 2.
moveChunksByShardKey(docs, "shard1");
moveChunksByShardKey(subjects, "shard2");

// Create a view with an empty pipeline on 'subjects'.
assert.commandWorked(testDB.createView("emptyViewOnSubjects", subjects.getName(), []));
checkView("emptyViewOnSubjects", subjectsDocs);

// Test a $graphLookup that triggers a CommandOnShardedViewNotSupportedOnMongod exception for a view
// with an empty pipeline.
testGraphLookupView({
    collection: docs,
    pipeline: [
        {$graphLookup: {
            from: "emptyViewOnSubjects",
            startWith: "$subject",
            connectFromField: "parent",
            connectToField: "name",
            as: "subjects",
        }},
        {$project: {
            name: 1,
            subjects: "$subjects.name"
        }}
    ],
    expectedResults: [
        {_id: 1, name: "Carter", subjects: ["Astrophysics", "Physics", "Science"]},
        {_id: 2, name: "Jackson", subjects: ["Anthropology", "Archaeology", "Humanities"]},
        {_id: 3, name: "Jones", subjects: ["Anthropology", "Archaeology", "Humanities"]},
        {_id: 4, name: "Mann", subjects: ["Physics", "Science", "Theoretical Physics"]},
        {_id: 5, name: "Mann", subjects: ["Biology", "Science", "Xenobiology"]},
    ],
    // Expect only one exception when trying to resolve the view 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard1", "test.docs": 0, "test.subjects": 1},
});

// Test a $graphLookup with a restrictSearchWithMatch that triggers a
// CommandOnShardedViewNotSupportedOnMongod exception for a view with an empty pipeline.
testGraphLookupView({
    collection: docs,
    pipeline: [
        {$graphLookup: {
            from: "emptyViewOnSubjects",
            startWith: "$subject",
            connectFromField: "parent",
            connectToField: "name",
            as: "subjects",
            restrictSearchWithMatch: { "name" : {$nin: ["Anthropology", "Archaeology", "Humanities"]} }
        }},
        {$project: {
            name: 1,
            science: {$gt: [{$size: "$subjects"}, 0]}
        }}
    ],
    expectedResults: [
        {_id: 1, name: "Carter", science: true},
        {_id: 2, name: "Jackson",  science: false},
        {_id: 3, name: "Jones",  science: false},
        {_id: 4, name: "Mann", science: true},
        {_id: 5, name: "Mann",  science: true},
    ],
    // Expect only one exception when trying to resolve the view 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard1", "test.docs": 0, "test.subjects": 1},
});

// Create a view with an empty pipeline on the existing empty view on 'subjects'.
assert.commandWorked(
    testDB.createView("emptyViewOnViewOnSubjects", testDB.emptyViewOnSubjects.getName(), []));
checkView("emptyViewOnViewOnSubjects", subjectsDocs);

// Test a $graphLookup that triggers a CommandOnShardedViewNotSupportedOnMongod exception for a view
// on another view.
testGraphLookupView({
    collection: docs,
    pipeline: [
        {$graphLookup: {
            from: "emptyViewOnViewOnSubjects",
            startWith: "$subject",
            connectFromField: "parent",
            connectToField: "name",
            as: "subjects",
        }},
        {$project: {
            name: 1,
            subjects: "$subjects.name"
        }}
    ],
    expectedResults: [
        {_id: 1, name: "Carter", subjects: ["Astrophysics", "Physics", "Science"]},
        {_id: 2, name: "Jackson", subjects: ["Anthropology", "Archaeology", "Humanities"]},
        {_id: 3, name: "Jones", subjects: ["Anthropology", "Archaeology", "Humanities"]},
        {_id: 4, name: "Mann", subjects: ["Physics", "Science", "Theoretical Physics"]},
        {_id: 5, name: "Mann", subjects: ["Biology", "Science", "Xenobiology"]},
    ],
    // Expect only one exception when trying to resolve the view 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard1", "test.docs": 0, "test.subjects": 1},
});

// Create a view with a pipeline on 'docs' that runs another $graphLookup.
assert.commandWorked(testDB.createView("physicists", docs.getName(), [
        {$graphLookup: {
            from: "emptyViewOnSubjects",
            startWith: "$subject",
            connectFromField: "parent",
            connectToField: "name",
            as: "subjects",
        }},
        {$match: {
            "subjects.name": "Physics"
        }},
        {$project: {
            name: 1,
            specialty: "$subject",
            subjects: "$subjects.name"
        }}
]));
checkView("physicists", [
    {
        _id: 1,
        name: "Carter",
        specialty: "Astrophysics",
        subjects: ["Astrophysics", "Physics", "Science"]
    },
    {
        _id: 4,
        name: "Mann",
        specialty: "Theoretical Physics",
        subjects: ["Physics", "Science", "Theoretical Physics"]
    },
]);

// Test a $graphLookup that triggers a CommandOnShardedViewNotSupportedOnMongod exception for a view
// with a pipeline that contains a $graphLookup.
testGraphLookupView({
    collection: subjects,
    pipeline: [
        {$graphLookup: {
            from: "physicists",
            startWith: "$name",
            connectFromField: "subjects",
            connectToField: "specialty",
            as: "practitioner",
        }},
        {$unwind: "$practitioner"},
    ],
    expectedResults: [
        {_id: 5, shard_key: "shard2", name: "Astrophysics", parent: "Physics",
         practitioner: {_id: 1, name: "Carter", specialty: "Astrophysics",
         subjects: ["Astrophysics", "Physics", "Science"]}},
        {_id: 7, shard_key: "shard2", name: "Theoretical Physics", parent: "Physics",
        practitioner: {_id: 4, name: "Mann", specialty: "Theoretical Physics",
        subjects: ["Physics", "Science", "Theoretical Physics"]}},
    ],
    // Expect one exception when trying to resolve the view 'physicists' on collection 'docs' and
    // another four on 'subjects' when trying to resolve 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard2", "test.docs": 1, "test.subjects": 4},
});

// Create a view with a pipeline on 'physicists' to test resolution of a view on another view.
assert.commandWorked(testDB.createView("physicist", testDB.physicists.getName(), [
    {$match: {"specialty": "Astrophysics"}},
]));

// Test a $graphLookup that triggers a CommandOnShardedViewNotSupportedOnMongod exception for a view
// on another view.
testGraphLookupView({
    collection: subjects,
    pipeline: [
        {$graphLookup: {
            from: "physicist",
            startWith: "$name",
            connectFromField: "subjects",
            connectToField: "specialty",
            as: "practitioner",
        }},
        {$unwind: "$practitioner"},
    ],
    expectedResults: [
        {_id: 5, shard_key: "shard2", name: "Astrophysics", parent: "Physics",
         practitioner: {_id: 1, name: "Carter", specialty: "Astrophysics",
         subjects: ["Astrophysics", "Physics", "Science"]}},
    ],
    // Expect one exception when trying to resolve the view 'physicists' on collection 'docs' and
    // one on 'subjects' when trying to resolve 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard2", "test.docs": 1, "test.subjects": 1},
});

// Test a $graphLookup with restrictSearchWithMatch that triggers a
// CommandOnShardedViewNotSupportedOnMongod exception for a view with a pipeline that contains a
// $graphLookup.
testGraphLookupView({
    collection: subjects,
    pipeline: [
        {$graphLookup: {
            from: "physicists",
            startWith: "$name",
            connectFromField: "subjects",
            connectToField: "specialty",
            as: "practitioner",
            restrictSearchWithMatch: { name: "Mann" }
        }},
        {$unwind: "$practitioner"},
    ],
    expectedResults: [
        {_id: 7, shard_key: "shard2", name: "Theoretical Physics", parent: "Physics",
         practitioner: {_id: 4, name: "Mann", specialty: "Theoretical Physics",
         subjects: ["Physics", "Science", "Theoretical Physics"]}},
    ],
    // Expect one exception when trying to resolve the view 'physicists' on collection 'docs' and
    // another two on 'subjects' when trying to resolve 'emptyViewOnSubjects'.
    expectedExceptions: {"shard": "shard2", "test.docs": 1, "test.subjects": 2},
});

sharded.stop();
}());

// Tests that when the featureCompatibilityVersion is not equal to the downgrade version, running
// hello/isMaster with internalClient returns a response with minWireVersion == maxWireVersion. This
// ensures that an older version mongod/mongos will fail to connect to the node when it is upgraded,
// upgrading, or downgrading.
//
(function() {
"use strict";

const conn = MongoRunner.runMongod();
const adminDB = conn.getDB("admin");

// This test manually runs hello/isMaster with the 'internalClient' field, which means that to the
// mongod, the connection appears to be from another server. This makes mongod to return a
// hello/isMaster response with a real 'minWireVersion' for internal clients instead of 0.
//
// The value of 'internalClient.maxWireVersion' in the hello/isMaster command does not matter for
// the purpose of this test and the hello/isMaster command will succeed regardless because this is
// run through the shell and the shell is always compatible talking to the server. In reality
// though, a real internal client with a lower binary version is expected to hang up immediately
// after receiving the response to the hello/isMaster command from a latest server with an upgraded
// FCV.
//
// And we need to use a side connection to do so in order to prevent the test connection from
// being closed on FCV changes.
function cmdAsInternalClient(cmd) {
    const command =
        {[cmd]: 1, internalClient: {minWireVersion: NumberInt(0), maxWireVersion: NumberInt(7)}};
    const connInternal = new Mongo(adminDB.getMongo().host);
    const res = assert.commandWorked(connInternal.adminCommand(command));
    connInternal.close();
    return res;
}

// When the featureCompatibilityVersion is equal to the upgrade version, running hello/isMaster with
// internalClient returns a response with minWireVersion == maxWireVersion.
checkFCV(adminDB, latestFCV);
let res = cmdAsInternalClient("hello");
assert.eq(res.minWireVersion, res.maxWireVersion, tojson(res));
res = cmdAsInternalClient("isMaster");
assert.eq(res.minWireVersion, res.maxWireVersion, tojson(res));

// Test wire version for upgrade/downgrade.
function runTest(downgradeFCV, downgradeWireVersion, maxWireVersion, cmd) {
    // When the featureCompatibilityVersion is upgrading, running hello/isMaster with internalClient
    // returns a response with minWireVersion == maxWireVersion.
    assert.commandWorked(
        adminDB.system.version.update({_id: "featureCompatibilityVersion"},
                                      {$set: {version: downgradeFCV, targetVersion: latestFCV}}));
    let res = cmdAsInternalClient(cmd);
    assert.eq(res.minWireVersion, res.maxWireVersion, tojson(res));
    assert.eq(maxWireVersion, res.maxWireVersion, tojson(res));

    // When the featureCompatibilityVersion is downgrading, running hello/isMaster with
    // internalClient returns a response with minWireVersion == maxWireVersion.
    assert.commandWorked(adminDB.system.version.update(
        {_id: "featureCompatibilityVersion"},
        {$set: {version: downgradeFCV, targetVersion: downgradeFCV, previousVersion: latestFCV}}));
    res = cmdAsInternalClient(cmd);
    assert.eq(res.minWireVersion, res.maxWireVersion, tojson(res));
    assert.eq(maxWireVersion, res.maxWireVersion, tojson(res));

    // When the featureCompatibilityVersion is equal to the downgrade version, running
    // hello/isMaster with internalClient returns a response with minWireVersion + 1 ==
    // maxWireVersion.
    assert.commandWorked(adminDB.runCommand({setFeatureCompatibilityVersion: downgradeFCV}));
    res = cmdAsInternalClient(cmd);
    assert.eq(downgradeWireVersion, res.minWireVersion, tojson(res));
    assert.eq(maxWireVersion, res.maxWireVersion, tojson(res));

    // When the internalClient field is missing from the hello/isMaster command, the response
    // returns the full wire version range from minWireVersion == 0 to maxWireVersion == latest
    // version, even if the featureCompatibilityVersion is equal to the upgrade version.
    assert.commandWorked(adminDB.runCommand({setFeatureCompatibilityVersion: latestFCV}));
    res = adminDB.runCommand({[cmd]: 1});
    assert.commandWorked(res);
    assert.eq(res.minWireVersion, 0, tojson(res));
    assert.lt(res.minWireVersion, res.maxWireVersion, tojson(res));
    assert.eq(maxWireVersion, res.maxWireVersion, tojson(res));
}

// Test upgrade/downgrade between 'latest' and 'last-continuous' if 'last-continuous' is not
// 'last-lts'.
if (lastContinuousFCV !== lastLTSFCV) {
    runTest(lastContinuousFCV, res.maxWireVersion - 1, res.maxWireVersion, "hello");
    runTest(lastContinuousFCV, res.maxWireVersion - 1, res.maxWireVersion, "isMaster");
    runTest(lastContinuousFCV, res.maxWireVersion - 1, res.maxWireVersion, "ismaster");
}

// Test upgrade/downgrade between 'latest' and 'last-lts'.
runTest(lastLTSFCV, res.maxWireVersion - numVersionsSinceLastLTS, res.maxWireVersion, "hello");
runTest(lastLTSFCV, res.maxWireVersion - numVersionsSinceLastLTS, res.maxWireVersion, "isMaster");
runTest(lastLTSFCV, res.maxWireVersion - numVersionsSinceLastLTS, res.maxWireVersion, "ismaster");

MongoRunner.stopMongod(conn);
})();

/**
 * Validate that the 'collMod' command with 'hidden' field will return expected result document for
 * the command and generate expected oplog entries in which hiding a hidden index or un-hiding a
 * visible index will be a no-op if TTL index option is not involved.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */

(function() {
"use strict";
load("jstests/libs/get_index_helpers.js");

const rst = new ReplSetTest({nodes: 2, nodeOpts: {binVersion: "latest"}});
rst.startSet();
rst.initiate();

const dbName = jsTestName();
const collName = "hidden_index";
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB[collName];
const oplogColl = primary.getDB("local")['oplog.rs'];

// Validate that the generated oplog entries filtered by given filter are what we expect.
function validateCollModOplogEntryCount(hiddenFilter, expectedCount) {
    let filter = {
        "ns": `${dbName}.$cmd`,
        "o.collMod": collName,
    };
    filter = Object.assign(filter, hiddenFilter);
    assert.eq(oplogColl.find(filter).count(), expectedCount);
}

// Validate that the index-related fields in the result document for the 'collMod' command are what
// we expect.
function validateResultForCollMod(result, expectedResult) {
    assert.eq(result.hidden_old, expectedResult.hidden_old, result);
    assert.eq(result.hidden_new, expectedResult.hidden_new, result);
    assert.eq(result.expireAfterSeconds_old, expectedResult.expireAfterSeconds_old, result);
    assert.eq(result.expireAfterSeconds_new, expectedResult.expireAfterSeconds_new, result);
}

primaryColl.drop();
assert.commandWorked(primaryColl.createIndex({a: 1}));
assert.commandWorked(primaryColl.createIndex({b: 1}, {expireAfterSeconds: 5}));

// Hiding a non-hidden index will generate the oplog entry with a 'hidden_old: false'.
let result = assert.commandWorked(primaryColl.hideIndex('a_1'));
validateResultForCollMod(result, {hidden_old: false, hidden_new: true});
validateCollModOplogEntryCount({"o.index.hidden": true, "o2.hidden_old": false}, 1);

// Hiding a hidden index won't generate both 'hidden' and 'hidden_old' field as it's a no-op. The
// result for no-op 'collMod' command shouldn't contain 'hidden' field.
result = assert.commandWorked(primaryColl.hideIndex('a_1'));
validateResultForCollMod(result, {});
validateCollModOplogEntryCount({"o.index.hidden": true, "o2.hidden_old": true}, 0);

// Un-hiding an hidden index will generate the oplog entry with a 'hidden_old: true'.
result = assert.commandWorked(primaryColl.unhideIndex('a_1'));
validateResultForCollMod(result, {hidden_old: true, hidden_new: false});
validateCollModOplogEntryCount({"o.index.hidden": false, "o2.hidden_old": true}, 1);

// Un-hiding a non-hidden index won't generate both 'hidden' and 'hidden_old' field as it's a no-op.
// The result for no-op 'collMod' command shouldn't contain 'hidden' field.
result = assert.commandWorked(primaryColl.unhideIndex('a_1'));
validateResultForCollMod(result, {});
validateCollModOplogEntryCount({"o.index.hidden": false, "o2.hidden_old": false}, 0);

// Validate that if both 'expireAfterSeconds' and 'hidden' options are specified but the 'hidden'
// option is a no-op, the operation as a whole will NOT be a no-op - instead, it will generate an
// oplog entry with only 'expireAfterSeconds'. Ditto for the command result returned to the user.
result = assert.commandWorked(primaryDB.runCommand({
    "collMod": primaryColl.getName(),
    "index": {"name": "b_1", "expireAfterSeconds": 10, "hidden": false},
}));
validateResultForCollMod(result, {expireAfterSeconds_old: 5, expireAfterSeconds_new: 10});
validateCollModOplogEntryCount({
    "o.index.expireAfterSeconds": 10,
    "o2.expireAfterSeconds_old": 5,
},
                               1);

// Test that the index was successfully modified.
const idxSpec = GetIndexHelpers.findByName(primaryColl.getIndexes(), "b_1");
assert.eq(idxSpec.hidden, undefined);
assert.eq(idxSpec.expireAfterSeconds, 10);

rst.stopSet();
})();

// Check that connecting via IPv4 keeps working when
// binding to localhost and enabling IPv6.

(function() {
'use strict';

const proc = MongoRunner.runMongod({bind_ip: "localhost", "ipv6": ""});
assert.neq(proc, null);

assert.soon(function() {
    try {
        const uri = 'mongodb://127.0.0.1:' + proc.port + '/test';
        const conn = new Mongo(uri);
        assert.commandWorked(conn.adminCommand({ping: 1}));
        return true;
    } catch (e) {
        jsTestLog('Ping failed: ' + tojson(e));
        return false;
    }
}, "Cannot connect to 127.0.0.1 when bound to localhost", 30 * 1000);
MongoRunner.stopMongod(proc);
})();

// Test keep-alive when using mongod's internal HttpClient.
// @tags: [requires_http_client]

(function() {
'use strict';

load('jstests/noPassthrough/libs/configExpand/lib.js');

function runTest(mongod, web) {
    assert(mongod);
    const admin = mongod.getDB('admin');

    // Only bother with this test when using curl >= 7.57.0.
    const http_status = admin.adminCommand({serverStatus: 1, http_client: 1});
    const http_client = assert.commandWorked(http_status).http_client;
    if (http_client.type !== 'curl') {
        print("*** Skipping test, not using curl");
        return;
    }

    printjson(http_client);
    if (http_client.running.version_num < 0x73900) {
        // 39 hex == 57 dec, so 0x73900 == 7.57.0
        print(
            "*** Skipping test, curl < 7.57.0 does not support connection pooling via share interface");
        return;
    }

    // Issue a series of requests to the mock server.
    for (let i = 0; i < 10; ++i) {
        const cmd = admin.runCommand({httpClientRequest: 1, uri: web.getStringReflectionURL(i)});
        const reflect = assert.commandWorked(cmd).body;
        assert.eq(reflect, i, "Mock server reflected something unexpected.");
    }

    // Check connect count.
    const countCmd = admin.runCommand({httpClientRequest: 1, uri: web.getURL() + '/connect_count'});
    const count = assert.commandWorked(countCmd).body;
    assert.eq(count, 1, "Connections were not kept alive.");

    // Force the open connection to close.
    const closeCmd =
        admin.runCommand({httpClientRequest: 1, uri: web.getURL() + '/connection_close'});
    const close = assert.commandWorked(closeCmd).body;
    assert.eq(close, 'closed');

    // Check count with new connection.
    const connectsCmd =
        admin.runCommand({httpClientRequest: 1, uri: web.getURL() + '/connect_count'});
    const connects = assert.commandWorked(connectsCmd).body;
    assert.eq(connects, 2, "Connection count incorrect.");

    // Check 404 returns failure.
    const failedCmd = assert.commandWorked(
        admin.runCommand({httpClientRequest: 1, uri: web.getURL() + '/no_such_path'}));
    assert.eq(failedCmd.code, 404);
}

const web = new ConfigExpandRestServer();
web.start();
const mongod = MongoRunner.runMongod({setParameter: 'enableTestCommands=1'});
runTest(mongod, web);
MongoRunner.stopMongod(mongod);
web.stop();
})();

/**
 * Tests that building geo indexes using the hybrid method handles the unindexing of invalid
 * geo documents.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.createCollection(coll.getName()));

// Insert an invalid geo document that will be removed before the indexer starts a collecton
// scan.
assert.commandWorked(coll.insert({
    _id: 0,
    b: {type: 'invalid_geo_json_type', coordinates: [100, 100]},
}));

// We are using this fail point to pause the index build before it starts the collection scan.
// This is important for this test because we are mutating the collection state before the index
// builder is able to observe the invalid geo document.
// By comparison, IndexBuildTest.pauseIndexBuilds() stalls the index build in the middle of the
// collection scan.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'alwaysOn'}));

const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {b: '2dsphere'});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'b_2dsphere');

// Insert a valid geo document to initialize the hybrid index builder's side table state.
assert.commandWorked(coll.insert({
    b: {type: 'Polygon', coordinates: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
}));

// Removing the invalid geo document should not cause any issues for the side table accounting.
assert.commandWorked(coll.remove({_id: 0}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'off'}));

// Wait for the index build to finish. Since the invalid geo document is removed before the
// index build scans the collection, the index should be built successfully.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'b_2dsphere']);

let res = assert.commandWorked(coll.validate({full: true}));
assert(res.valid, 'validation failed on primary: ' + tojson(res));

rst.stopSet();
})();

/**
 * Tests that building geo indexes using the hybrid method handles the unindexing of invalid
 * geo documents.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.createCollection(coll.getName()));

// Insert an invalid geo document that will be removed before the indexer starts a collecton
// scan.
assert.commandWorked(coll.insert({
    _id: 0,
    b: {type: 'invalid_geo_json_type', coordinates: [100, 100]},
}));

// We are using this fail point to pause the index build before it starts the collection scan.
// This is important for this test because we are mutating the collection state before the index
// builder is able to observe the invalid geo document.
// By comparison, IndexBuildTest.pauseIndexBuilds() stalls the index build in the middle of the
// collection scan.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'alwaysOn'}));

const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {b: '2dsphere'});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'b_2dsphere');

// Fixing the invalid geo document should not cause any issues for the side table accounting.
assert.commandWorked(coll.update(
    {_id: 0}, {b: {type: 'Polygon', coordinates: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterSettingUpIndexBuild', mode: 'off'}));

// Wait for the index build to finish. Since the invalid geo document is removed before the
// index build scans the collection, the index should be built successfully.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'b_2dsphere']);

let res = assert.commandWorked(coll.validate({full: true}));
assert(res.valid, 'validation failed on primary: ' + tojson(res));

rst.stopSet();
})();

/**
 * Tests that hybrid index builds on timeseries buckets collections behave correctly when they
 * receive concurrent writes.
 */
load("jstests/noPassthrough/libs/index_build.js");
load('jstests/core/timeseries/libs/timeseries.js');

(function() {
"use strict";

const conn = MongoRunner.runMongod();
if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const collName = 'ts';
const testDB = conn.getDB(dbName);
const tsColl = testDB[collName];
const bucketsColl = testDB.getCollection('system.buckets.' + collName);

const timeField = 'time';
const metaField = 'meta';

const runTest = (config) => {
    // Populate the collection.
    tsColl.drop();

    assert.commandWorked(testDB.createCollection(
        collName, {timeseries: {timeField: timeField, metaField: metaField}}));

    let nDocs = 10;
    for (let i = 0; i < nDocs; i++) {
        assert.commandWorked(tsColl.insert({
            _id: i,
            [timeField]: new Date(),
        }));
    }

    jsTestLog("Testing: " + tojson(config));

    // Prevent the index build from completing.
    IndexBuildTest.pauseIndexBuilds(conn);

    // Build an index on the buckets collection, not the time-series view, and wait for it to start.
    const indexName = "test_index";
    let indexOptions = config.indexOptions || {};
    indexOptions.name = indexName;
    const awaitIndex = IndexBuildTest.startIndexBuild(
        conn, bucketsColl.getFullName(), config.indexSpec, indexOptions);
    IndexBuildTest.waitForIndexBuildToStart(testDB, bucketsColl.getName(), indexName);

    // Perform writes while the index build is in progress.
    assert.commandWorked(tsColl.insert({
        _id: nDocs++,
        [timeField]: new Date(),
    }));

    let extraDocs = config.extraDocs || [];
    extraDocs.forEach((doc) => {
        const template = {_id: nDocs++, [timeField]: new Date()};
        const newObj = Object.assign({}, doc, template);
        assert.commandWorked(tsColl.insert(newObj));
    });

    // Allow the index build to complete.
    IndexBuildTest.resumeIndexBuilds(conn);
    awaitIndex();

    // Due to the nature of bucketing, we can't reliably make assertions about the contents of the
    // buckets collection, so we rely on validate to ensure the index is built correctly.
    const validateRes = assert.commandWorked(bucketsColl.validate());
    assert(validateRes.valid, validateRes);
};

const basicOps = [
    {[metaField]: -Math.pow(-2147483648, 34)},  // -Inf
    {[metaField]: 0},
    {[metaField]: 0},
    {[metaField]: {foo: 'bar'}},
    {[metaField]: {foo: 1}},
    {[metaField]: 'hello world'},
    {[metaField]: 1},
    {},
    {[metaField]: Math.pow(-2147483648, 34)},  // Inf
];

runTest({
    indexSpec: {[metaField]: 1},
    extraDocs: basicOps,
});

runTest({
    indexSpec: {[metaField]: -1},
    extraDocs: basicOps,
});

runTest({
    indexSpec: {[metaField]: 'hashed'},
    extraDocs: basicOps,

});

runTest({
    indexSpec: {[metaField + '.$**']: 1},
    extraDocs: basicOps,
});
runTest({
    indexSpec: {[metaField + '$**']: 1},
    extraDocs: basicOps,
});

runTest({
    indexSpec: {[metaField]: '2dsphere'},
    extraDocs: [
        {[metaField]: {type: 'Polygon', coordinates: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}},
        {[metaField]: {type: 'Point', coordinates: [0, 1]}},
    ]
});

runTest({
    indexSpec: {[metaField]: '2dsphere'},
    indexOptions: {sparse: true},
    extraDocs: [
        {[metaField]: {type: 'Polygon', coordinates: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}},
        {[metaField]: {type: 'Point', coordinates: [0, 1]}},
        {},
    ]
});

runTest({
    indexSpec: {'control.min.time': 1},
    extraDocs: basicOps,
});

runTest({
    indexSpec: {'control.max.time': -1},
    extraDocs: basicOps,
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that write operations are accepted and result in correct indexing behavior for each phase
 * of hybrid index builds.
 */
load("jstests/libs/logv2_helpers.js");

(function() {
"use strict";

let conn = MongoRunner.runMongod();
let testDB = conn.getDB('test');

let turnFailPointOn = function(failPointName, data) {
    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: failPointName, mode: "alwaysOn", data: data || {}}));
};

let turnFailPointOff = function(failPointName) {
    assert.commandWorked(testDB.adminCommand({configureFailPoint: failPointName, mode: "off"}));
};

let totalDocs = 0;
let crudOpsForPhase = function(coll, phase) {
    let bulk = coll.initializeUnorderedBulkOp();

    // Create 1000 documents in a specific range for this phase.
    for (let i = 0; i < 1000; i++) {
        bulk.insert({i: (phase * 1000) + i});
    }
    totalDocs += 1000;

    if (phase <= 0) {
        assert.commandWorked(bulk.execute());
        return;
    }

    // Update 50 documents.
    // For example, if phase is 2, documents [100, 150) will be updated to [-100, -150).
    let start = (phase - 1) * 100;
    for (let j = start; j < (100 * phase) - 50; j++) {
        bulk.find({i: j}).update({$set: {i: -j}});
    }
    // Delete 25 documents.
    // Similarly, if phase is 2, documents [150, 200) will be removed.
    for (let j = start + 50; j < 100 * phase; j++) {
        bulk.find({i: j}).remove();
    }
    totalDocs -= 50;

    assert.commandWorked(bulk.execute());
};

crudOpsForPhase(testDB.hybrid, 0);
assert.eq(totalDocs, testDB.hybrid.count());

// Hang the build after the first document.
turnFailPointOn("hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", {fieldsToMatch: {i: 1}});

// Start the background build.
let bgBuild = startParallelShell(function() {
    assert.commandWorked(db.hybrid.createIndex({i: 1}, {background: true}));
}, conn.port);

checkLog.containsJson(conn, 20386, {
    where: "before",
    doc: function(doc) {
        return doc.i === 1;
    }
});

// Phase 1: Collection scan and external sort
// Insert documents while doing the bulk build.
crudOpsForPhase(testDB.hybrid, 1);
assert.eq(totalDocs, testDB.hybrid.count());

// Enable pause after bulk dump into index.
turnFailPointOn("hangAfterIndexBuildDumpsInsertsFromBulk");

// Wait for the bulk insert to complete.
turnFailPointOff("hangIndexBuildDuringCollectionScanPhaseBeforeInsertion");
checkLog.contains(conn, "Hanging after dumping inserts from bulk builder");

// Phase 2: First drain
// Do some updates, inserts and deletes after the bulk builder has finished.

// Hang after yielding
turnFailPointOn("hangDuringIndexBuildDrainYield", {namespace: testDB.hybrid.getFullName()});

// Enable pause after first drain.
turnFailPointOn("hangAfterIndexBuildFirstDrain");

crudOpsForPhase(testDB.hybrid, 2);
assert.eq(totalDocs, testDB.hybrid.count());

// Allow first drain to start.
turnFailPointOff("hangAfterIndexBuildDumpsInsertsFromBulk");

// Ensure the operation yields during the drain, then attempt some operations.
checkLog.contains(conn, "Hanging index build during drain yield");
assert.commandWorked(testDB.hybrid.insert({i: "during yield"}));
assert.commandWorked(testDB.hybrid.remove({i: "during yield"}));
turnFailPointOff("hangDuringIndexBuildDrainYield");

// Wait for first drain to finish.
checkLog.contains(conn, "Hanging after index build first drain");

// Phase 3: Second drain
// Enable pause after second drain.
turnFailPointOn("hangAfterIndexBuildSecondDrain");

// Add inserts that must be consumed in the second drain.
crudOpsForPhase(testDB.hybrid, 3);
assert.eq(totalDocs, testDB.hybrid.count());

// Allow second drain to start.
turnFailPointOff("hangAfterIndexBuildFirstDrain");

// Wait for second drain to finish.
checkLog.contains(conn, "Hanging after index build second drain");

// Phase 4: Final drain and commit.
// Add inserts that must be consumed in the final drain.
crudOpsForPhase(testDB.hybrid, 4);
assert.eq(totalDocs, testDB.hybrid.count());

// Allow final drain to start.
turnFailPointOff("hangAfterIndexBuildSecondDrain");

// Wait for build to complete.
bgBuild();

assert.eq(totalDocs, testDB.hybrid.count());
assert.commandWorked(testDB.hybrid.validate({full: true}));

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that hybrid index builds result in a consistent state and correct multikey behavior across
 * various index types.
 */
load("jstests/noPassthrough/libs/index_build.js");
load("jstests/libs/analyze_plan.js");  // For getWinningPlan to analyze explain() output.

(function() {
"use strict";

const conn = MongoRunner.runMongod();
const dbName = 'test';
const collName = 'foo';
const testDB = conn.getDB(dbName);
const testColl = testDB[collName];

const runTest = (config) => {
    // Populate the collection.
    const nDocs = 10;
    testColl.drop();
    for (let i = 0; i < nDocs; i++) {
        assert.commandWorked(testColl.insert({x: i}));
    }

    jsTestLog("Testing: " + tojson(config));

    // Prevent the index build from completing.
    IndexBuildTest.pauseIndexBuilds(conn);

    // Start the index build and wait for it to start.
    const indexName = "test_index";
    let indexOptions = config.indexOptions || {};
    indexOptions.name = indexName;
    const awaitIndex = IndexBuildTest.startIndexBuild(
        conn, testColl.getFullName(), config.indexSpec, indexOptions);
    IndexBuildTest.waitForIndexBuildToStart(testDB, collName, indexName);

    // Perform writes while the index build is in progress.
    assert.commandWorked(testColl.insert({x: nDocs + 1}));
    assert.commandWorked(testColl.update({x: 0}, {x: -1}));
    assert.commandWorked(testColl.remove({x: -1}));

    let extraDocs = config.extraDocs || [];
    extraDocs.forEach((doc) => {
        assert.commandWorked(testColl.insert(doc));
    });

    // Allow the index build to complete.
    IndexBuildTest.resumeIndexBuilds(conn);
    awaitIndex();

    // Ensure the index is usable and has the expected multikey state.
    let explain = testColl.find({x: 1}).hint(indexName).explain();
    let plan = getWinningPlan(explain.queryPlanner);
    assert.eq("FETCH", plan.stage, explain);
    assert.eq("IXSCAN", plan.inputStage.stage, explain);
    assert.eq(
        config.expectMultikey,
        plan.inputStage.isMultiKey,
        `Index multikey state "${plan.inputStage.isMultiKey}" was not "${config.expectMultikey}"`);

    const validateRes = assert.commandWorked(testColl.validate());
    assert(validateRes.valid, validateRes);
};

// Hashed indexes should never be multikey.
runTest({
    indexSpec: {x: 'hashed'},
    expectMultikey: false,
});

// Wildcard indexes are not multikey unless they have multikey documents.
runTest({
    indexSpec: {'$**': 1},
    expectMultikey: false,
});
runTest({
    indexSpec: {'$**': 1},
    expectMultikey: true,
    extraDocs: [{x: [1, 2, 3]}],
});

// '2dsphere' indexes are not multikey unless they have multikey documents.
runTest({
    indexSpec: {x: 1, b: '2dsphere'},
    expectMultikey: false,
});

// '2dsphere' indexes are automatically sparse. If we insert a document where 'x' is multikey, even
// though 'b' is omitted, the index is still considered multikey. See SERVER-39705.
runTest({
    indexSpec: {x: 1, b: '2dsphere'},
    extraDocs: [
        {x: [1, 2]},
    ],
    expectMultikey: true,
});

// Test that a partial index is not multikey when a multikey document is not indexed.
runTest({
    indexSpec: {x: 1},
    indexOptions: {partialFilterExpression: {a: {$gt: 0}}},
    extraDocs: [
        {x: [0, 1, 2], a: 0},
    ],
    expectMultikey: false,
});

// Test that a partial index is multikey when a multikey document is indexed.
runTest({
    indexSpec: {x: 1},
    indexOptions: {partialFilterExpression: {a: {$gt: 0}}},
    extraDocs: [
        {x: [0, 1, 2], a: 1},
    ],
    expectMultikey: true,
});

// Text indexes are not multikey unless documents make them multikey.
runTest({
    indexSpec: {x: 'text'},
    extraDocs: [
        {x: 'hello'},
    ],
    expectMultikey: false,
});

// Text indexes can be multikey if a field has multiple words.
runTest({
    indexSpec: {x: 'text'},
    extraDocs: [
        {x: 'hello world'},
    ],
    expectMultikey: true,
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that building partial geo indexes using the hybrid method preserves multikey information.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.createCollection(coll.getName()));

// Insert document into collection to avoid optimization for index creation on an empty collection.
// This allows us to pause index builds on the collection using a fail point.
assert.commandWorked(coll.insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

// Create a 2dsphere partial index for documents where 'a', the field in the filter expression,
// is greater than 0.
const partialIndex = {
    b: '2dsphere'
};
const createIdx = IndexBuildTest.startIndexBuild(
    primary, coll.getFullName(), partialIndex, {partialFilterExpression: {a: {$gt: 0}}});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'b_2dsphere');

// This document has an invalid geoJSON format (duplicated points), but will not be indexed.
const unindexedDoc = {
    _id: 0,
    a: -1,
    b: {type: "Polygon", coordinates: [[[0, 0], [0, 1], [1, 1], [0, 1], [0, 0]]]},
};

// This document has valid geoJson, and will be indexed.
const indexedDoc = {
    _id: 1,
    a: 1,
    b: {type: "Polygon", coordinates: [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
};

assert.commandWorked(coll.insert(unindexedDoc));
assert.commandWorked(coll.insert(indexedDoc));

// Removing unindexed document should succeed without error.
assert.commandWorked(coll.remove({_id: 0}));

IndexBuildTest.resumeIndexBuilds(primary);

// Wait for the index build to finish.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'b_2dsphere']);

let res = assert.commandWorked(coll.validate({full: true}));
assert(res.valid, 'validation failed on primary: ' + tojson(res));

rst.stopSet();
})();

/**
 * Tests that building partial indexes using the hybrid method preserves multikey information.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.createCollection(coll.getName()));

// Insert document into collection to avoid optimization for index creation on an empty collection.
// This allows us to pause index builds on the collection using a fail point.
assert.commandWorked(coll.insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

// Create a partial index for documents where 'a', the field in the filter expression,
// is equal to 1.
const partialIndex = {
    a: 1
};
const createIdx = IndexBuildTest.startIndexBuild(
    primary, coll.getFullName(), partialIndex, {partialFilterExpression: {a: 1}});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'a_1');

assert.commandWorked(coll.insert({_id: 0, a: 1}));

// Update the document so that it no longer meets the partial index criteria.
assert.commandWorked(coll.update({_id: 0}, {$set: {a: 0}}));

IndexBuildTest.resumeIndexBuilds(primary);

// Wait for the index build to finish.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1']);

let res = assert.commandWorked(coll.validate({full: true}));
assert(res.valid, 'validation failed on primary: ' + tojson(res));

rst.stopSet();
})();

/**
 * Tests that building sparse compound geo indexes using the hybrid method preserves multikey
 * information.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(testDB.createCollection(coll.getName()));

// Insert document into collection to avoid optimization for index creation on an empty collection.
// This allows us to pause index builds on the collection using a fail point.
assert.commandWorked(coll.insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

const createIdx = IndexBuildTest.startIndexBuild(
    primary, coll.getFullName(), {a: 1, b: '2dsphere'}, {sparse: true});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'a_1_b_2dsphere');

assert.commandWorked(coll.insert({a: [1, 2]}));

IndexBuildTest.resumeIndexBuilds(primary);

// Wait for the index build to finish.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1_b_2dsphere']);

let res = assert.commandWorked(coll.validate({full: true}));
assert(res.valid, 'validation failed on primary: ' + tojson(res));

rst.stopSet();
})();

/**
 * Tests that write operations are accepted and result in correct indexing behavior for each phase
 * of hybrid unique index builds. This test inserts a duplicate document at different phases of an
 * index build to confirm that the resulting behavior is failure.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/logv2_helpers.js");

let replSetTest = new ReplSetTest({name: "hybrid_updates", nodes: 2});
replSetTest.startSet();
replSetTest.initiate();

let conn = replSetTest.getPrimary();
let testDB = conn.getDB('test');

// Enables a failpoint, runs 'hitFailpointFunc' to hit the failpoint, then runs
// 'duringFailpointFunc' while the failpoint is active.
let doDuringFailpoint = function(
    failPointName, structuredLogRegEx, hitFailpointFunc, duringFailpointFunc, stopKey) {
    clearRawMongoProgramOutput();
    assert.commandWorked(testDB.adminCommand({
        configureFailPoint: failPointName,
        mode: "alwaysOn",
        data: {fieldsToMatch: {i: stopKey}}
    }));

    hitFailpointFunc();

    assert.soon(() => structuredLogRegEx.test(rawMongoProgramOutput()));

    duringFailpointFunc();

    assert.commandWorked(testDB.adminCommand({configureFailPoint: failPointName, mode: "off"}));
};

const docsToInsert = 1000;
let setUp = function(coll) {
    coll.drop();

    let bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < docsToInsert; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());
};

let buildIndexInBackground = function(coll, expectDuplicateKeyError) {
    const createIndexFunction = function(collFullName) {
        const coll = db.getMongo().getCollection(collFullName);
        return coll.createIndex({i: 1}, {background: true, unique: true});
    };
    const assertFunction = expectDuplicateKeyError ? function(collFullName) {
        assert.commandFailedWithCode(createIndexFunction(collFullName), ErrorCodes.DuplicateKey);
    } : function(collFullName) {
        assert.commandWorked(createIndexFunction(collFullName));
    };
    return startParallelShell('const createIndexFunction = ' + createIndexFunction + ';\n' +
                                  'const assertFunction = ' + assertFunction + ';\n' +
                                  'assertFunction("' + coll.getFullName() + '")',
                              conn.port);
};

/**
 * Run a background index build on a unique index under different configurations. Introduce
 * duplicate keys on the index that may cause it to fail or succeed, depending on the following
 * optional parameters:
 * {
 *   // Which operation used to introduce a duplicate key.
 *   operation {string}: "insert", "update"
 *
 *   // Whether or not resolve the duplicate key before completing the build.
 *   resolve {bool}
 *
 *   // Which phase of the index build to introduce the duplicate key.
 *   phase {number}: 0-4
 * }
 */
let runTest = function(config) {
    jsTestLog("running test with config: " + tojson(config));

    const collName = Object.keys(config).length
        ? 'hybrid_' + config.operation[0] + '_r' + Number(config.resolve) + '_p' + config.phase
        : 'hybrid';
    const coll = testDB.getCollection(collName);
    setUp(coll);

    // Expect the build to fail with a duplicate key error if we insert a duplicate key and
    // don't resolve it.
    let expectDuplicate = config.resolve === false;

    let awaitBuild;
    let buildIndex = function() {
        awaitBuild = buildIndexInBackground(coll, expectDuplicate);
    };

    // Introduce a duplicate key, either from an insert or update. Optionally, follow-up with an
    // operation that will resolve the duplicate by removing it or updating it.
    const dup = {i: 0};
    let doOperation = function() {
        if ("insert" == config.operation) {
            assert.commandWorked(coll.insert(dup));
            if (config.resolve) {
                assert.commandWorked(coll.deleteOne(dup));
            }
        } else if ("update" == config.operation) {
            assert.commandWorked(coll.update(dup, {i: 1}));
            if (config.resolve) {
                assert.commandWorked(coll.update({i: 1}, dup));
            }
        }
    };

    const stopKey = 0;
    switch (config.phase) {
        // Just build the index without any failpoints.
        case undefined:
            buildIndex();
            break;
        // Hang before scanning the first document.
        case 0:
            doDuringFailpoint(
                "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion",
                new RegExp("\"id\":20386.*\"where\":\"before\",\"doc\":.*\"i\":" + stopKey),
                buildIndex,
                doOperation,
                stopKey);
            break;
        // Hang after scanning the first document.
        case 1:
            doDuringFailpoint(
                "hangIndexBuildDuringCollectionScanPhaseAfterInsertion",
                new RegExp("\"id\":20386.*\"where\":\"after\",\"doc\":.*\"i\":" + stopKey),
                buildIndex,
                doOperation,
                stopKey);
            break;
        // Hang before the first drain and after dumping the keys from the external sorter into
        // the index.
        case 2:
            doDuringFailpoint("hangAfterIndexBuildDumpsInsertsFromBulk",
                              new RegExp("\"id\":20665"),
                              buildIndex,
                              doOperation);
            break;
        // Hang before the second drain.
        case 3:
            doDuringFailpoint("hangAfterIndexBuildFirstDrain",
                              new RegExp("\"id\":20666"),
                              buildIndex,
                              doOperation);
            break;
        // Hang before the final drain and commit.
        case 4:
            doDuringFailpoint("hangAfterIndexBuildSecondDrain",
                              new RegExp("\"id\":20667"),
                              buildIndex,
                              doOperation);
            break;
        default:
            assert(false, "Invalid phase: " + config.phase);
    }

    awaitBuild();

    let expectedDocs = docsToInsert;
    expectedDocs += (config.operation == "insert" && config.resolve === false) ? 1 : 0;

    assert.eq(expectedDocs, coll.count());
    assert.eq(expectedDocs, coll.find().itcount());
    assert.commandWorked(coll.validate({full: true}));
};

runTest({});

for (let i = 0; i <= 4; i++) {
    runTest({operation: "insert", resolve: true, phase: i});
    runTest({operation: "insert", resolve: false, phase: i});
    runTest({operation: "update", resolve: true, phase: i});
    runTest({operation: "update", resolve: false, phase: i});
}

replSetTest.stopSet();
})();

/**
 * Test that hyphenated database name can work with dbStats when directoryperdb is turned on.
 *
 * @tags: [requires_persistence]
 */
(function() {
"use strict";
var isDirectoryPerDBSupported =
    jsTest.options().storageEngine == "wiredTiger" || !jsTest.options().storageEngine;
if (!isDirectoryPerDBSupported)
    return;

const dbName = "test-hyphen";
let conn = MongoRunner.runMongod({directoryperdb: ''});

conn.getDB(dbName).a.insert({x: 1});
let res = conn.getDB(dbName).runCommand({dbStats: 1, scale: 1});
jsTestLog("dbStats: " + tojson(res));
assert(res.db == "test-hyphen");
assert(res.fsUsedSize > 0);
assert(res.fsTotalSize > 0);

MongoRunner.stopMongod(conn);
})();

// Test that 'notablescan' parameter does not affect queries internal namespaces.
// @tags: [uses_transactions]
(function() {
"use strict";

const dbName = "test";
const collName = "coll";

function runTests(ServerType) {
    const s = new ServerType();

    const configDB = s.getConn().getDB("config");
    const session = s.getConn().getDB(dbName).getMongo().startSession();
    const primaryDB = session.getDatabase(dbName);

    // Implicitly create the collection outside of the transaction.
    assert.commandWorked(primaryDB.getCollection(collName).insert({x: 1}));

    // Run a transaction so the 'config.transactions' collection is implicitly created.
    session.startTransaction();
    assert.commandWorked(primaryDB.getCollection(collName).insert({x: 2}));
    assert.commandWorked(session.commitTransaction_forTesting());

    // Run a predicate query that would fail if we did not ignore the 'notablescan' flag.
    assert.eq(configDB.transactions.find({any_nonexistent_field: {$exists: true}}).itcount(), 0);

    // Run the same query against the user created collection honoring the 'notablescan' flag.
    // This will cause the query to fail as there is no viable query plan. Unfortunately,
    // the reported query error code is the cryptic 'BadValue'.
    assert.commandFailedWithCode(
        primaryDB.runCommand({find: collName, filter: {any_nonexistent_field: {$exists: true}}}),
        ErrorCodes.NoQueryExecutionPlans);

    s.stop();
}

function Sharding() {
    this.st = new ShardingTest({
        shards: 2,
        config: 1,
        other: {
            shardOptions: {setParameter: {notablescan: true}},
            configOptions: {setParameter: {notablescan: true}}
        }
    });
}

Sharding.prototype.stop = function() {
    this.st.stop();
};

Sharding.prototype.getConn = function() {
    return this.st.s0;
};

function ReplSet() {
    this.rst = new ReplSetTest({nodes: 1, nodeOptions: {setParameter: {notablescan: true}}});
    this.rst.startSet();
    this.rst.initiate();
}

ReplSet.prototype.stop = function() {
    this.rst.stopSet();
};

ReplSet.prototype.getConn = function() {
    return this.rst.getPrimary();
};

[ReplSet, Sharding].forEach(runTests);
}());

/**
 * Verifies behavior around implicit sessions in the mongo shell.
 */
(function() {
"use strict";

/**
 * Runs the given function, inspecting the outgoing command object and making assertions about
 * its logical session id.
 */
function inspectCommandForSessionId(func, {shouldIncludeId, expectedId, differentFromId}) {
    const mongoRunCommandOriginal = Mongo.prototype.runCommand;

    const sentinel = {};
    let cmdObjSeen = sentinel;

    Mongo.prototype.runCommand = function runCommandSpy(dbName, cmdObj, options) {
        cmdObjSeen = cmdObj;
        return mongoRunCommandOriginal.apply(this, arguments);
    };

    try {
        assert.doesNotThrow(func);
    } finally {
        Mongo.prototype.runCommand = mongoRunCommandOriginal;
    }

    if (cmdObjSeen === sentinel) {
        throw new Error("Mongo.prototype.runCommand() was never called: " + func.toString());
    }

    // If the command is in a wrapped form, then we look for the actual command object inside
    // the query/$query object.
    let cmdName = Object.keys(cmdObjSeen)[0];
    if (cmdName === "query" || cmdName === "$query") {
        cmdObjSeen = cmdObjSeen[cmdName];
        cmdName = Object.keys(cmdObjSeen)[0];
    }

    if (shouldIncludeId) {
        assert(cmdObjSeen.hasOwnProperty("lsid"),
               "Expected operation " + tojson(cmdObjSeen) + " to have a logical session id.");

        if (expectedId) {
            assert(bsonBinaryEqual(expectedId, cmdObjSeen.lsid),
                   "The sent session id did not match the expected, sent: " +
                       tojson(cmdObjSeen.lsid) + ", expected: " + tojson(expectedId));
        }

        if (differentFromId) {
            assert(!bsonBinaryEqual(differentFromId, cmdObjSeen.lsid),
                   "The sent session id was not different from the expected, sent: " +
                       tojson(cmdObjSeen.lsid) + ", expected: " + tojson(differentFromId));
        }

    } else {
        assert(!cmdObjSeen.hasOwnProperty("lsid"),
               "Expected operation " + tojson(cmdObjSeen) + " to not have a logical session id.");
    }

    return cmdObjSeen.lsid;
}

// Tests regular behavior of implicit sessions.
function runTest() {
    const conn = MongoRunner.runMongod();

    // Commands run on a database without an explicit session should use an implicit one.
    const testDB = conn.getDB("test");
    const coll = testDB.getCollection("foo");
    const implicitId = inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true});

    // Unacknowledged writes have no session id.
    inspectCommandForSessionId(function() {
        coll.insert({x: 1}, {writeConcern: {w: 0}});
    }, {shouldIncludeId: false});

    assert(bsonBinaryEqual(testDB.getSession().getSessionId(), implicitId),
           "Expected the id of the database's implicit session to match the one sent, sent: " +
               tojson(implicitId) +
               " db session id: " + tojson(testDB.getSession().getSessionId()));

    // Implicit sessions are not causally consistent.
    assert(!testDB.getSession().getOptions().isCausalConsistency(),
           "Expected the database's implicit session to not be causally consistent");

    // Further commands run on the same database should reuse the implicit session.
    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // New collections from the same database should inherit the implicit session.
    const collTwo = testDB.getCollection("bar");
    inspectCommandForSessionId(function() {
        assert.commandWorked(collTwo.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // Sibling databases should inherit the implicit session.
    let siblingColl = testDB.getSiblingDB("foo").getCollection("bar");
    inspectCommandForSessionId(function() {
        assert.commandWorked(siblingColl.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // A new database from the same connection should inherit the implicit session.
    const newCollSameConn = conn.getDB("testTwo").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(newCollSameConn.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // A new database from a new connection should use a different implicit session.
    const newCollNewConn = new Mongo(conn.host).getDB("test").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(newCollNewConn.insert({x: 1}));
    }, {shouldIncludeId: true, differentFromId: implicitId});

    // The original implicit session should still live on the first database.
    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // Databases created from an explicit session should override any implicit sessions.
    const session = conn.startSession();
    const sessionColl = session.getDatabase("test").getCollection("foo");
    const explicitId = inspectCommandForSessionId(function() {
        assert.commandWorked(sessionColl.insert({x: 1}));
    }, {shouldIncludeId: true, differentFromId: implicitId});

    assert(bsonBinaryEqual(session.getSessionId(), explicitId),
           "Expected the id of the explicit session to match the one sent, sent: " +
               tojson(explicitId) + " explicit session id: " + tojson(session.getSessionId()));
    assert(bsonBinaryEqual(sessionColl.getDB().getSession().getSessionId(), explicitId),
           "Expected id of the database's session to match the explicit session's id, sent: " +
               tojson(sessionColl.getDB().getSession().getSessionId()) +
               ", explicit session id: " + tojson(session.getSessionId()));

    // The original implicit session should still live on the first database.
    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    // New databases on the same connection as the explicit session should still inherit the
    // original implicit session.
    const newCollSameConnAfter = conn.getDB("testThree").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(newCollSameConnAfter.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    session.endSession();
    MongoRunner.stopMongod(conn);
}

// Tests behavior when the test flag to disable implicit sessions is changed.
function runTestTransitionToDisabled() {
    const conn = MongoRunner.runMongod();

    // Existing implicit sessions should be erased when the disable flag is set.
    const coll = conn.getDB("test").getCollection("foo");
    const implicitId = inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true});

    TestData.disableImplicitSessions = true;

    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: false});

    // After the flag is unset, databases using existing connections with implicit sessions will
    // use the original implicit sessions again and new connections will create and use new
    // implicit sessions.
    TestData.disableImplicitSessions = false;

    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    const newColl = conn.getDB("test").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(newColl.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: implicitId});

    const newCollNewConn = new Mongo(conn.host).getDB("test").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(newCollNewConn.insert({x: 1}));
    }, {shouldIncludeId: true, differentFromId: implicitId});

    // Explicit sessions should not be affected by the disable flag being set.
    const session = conn.startSession();
    const sessionColl = session.getDatabase("test").getCollection("foo");
    const explicitId = inspectCommandForSessionId(function() {
        assert.commandWorked(sessionColl.insert({x: 1}));
    }, {shouldIncludeId: true});

    TestData.disableImplicitSessions = true;

    inspectCommandForSessionId(function() {
        assert.commandWorked(sessionColl.insert({x: 1}));
    }, {shouldIncludeId: true, expectedId: explicitId});

    session.endSession();
    MongoRunner.stopMongod(conn);
}

// Tests behavior of implicit sessions when they are disabled via a test flag.
function runTestDisabled() {
    const conn = MongoRunner.runMongod();

    // Commands run without an explicit session should not use an implicit one.
    const coll = conn.getDB("test").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(coll.insert({x: 1}));
    }, {shouldIncludeId: false});

    // Explicit sessions should still include session ids.
    const session = conn.startSession();
    const sessionColl = session.getDatabase("test").getCollection("foo");
    inspectCommandForSessionId(function() {
        assert.commandWorked(sessionColl.insert({x: 1}));
    }, {shouldIncludeId: true});

    // Commands run in a parallel shell inherit the disable flag.
    TestData.inspectCommandForSessionId = inspectCommandForSessionId;
    const awaitShell = startParallelShell(function() {
        const parallelColl = db.getCollection("foo");
        TestData.inspectCommandForSessionId(function() {
            assert.commandWorked(parallelColl.insert({x: 1}));
        }, {shouldIncludeId: false});
    }, conn.port);
    awaitShell();

    session.endSession();
    MongoRunner.stopMongod(conn);
}

runTest();

runTestTransitionToDisabled();

assert(_shouldUseImplicitSessions());

TestData.disableImplicitSessions = true;
runTestDisabled();
})();

/*
 * Tests that an index build fails gracefully as it is interrupted just before it signals that it
 * is ready to commit by updating the corresponding document in config.system.indexBuilds.
 *
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/libs/fail_point_util.js');
load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('coll');
const indexBuildsColl = primary.getCollection('config.system.indexBuilds');

assert.commandWorked(coll.insert({_id: 1, a: 1}));

// Enable fail point which makes index build hang before it reads the index build
const failPoint = configureFailPoint(primary, 'hangBeforeGettingIndexBuildEntry');

const createIndex = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1}, {}, [
    ErrorCodes.IndexBuildAborted,
    ErrorCodes.NoMatchingDocument
]);

failPoint.wait();

// Index build should be present in the config.system.indexBuilds collection.
const indexMap =
    IndexBuildTest.assertIndexes(coll, 2, ['_id_'], ['a_1'], {includeBuildUUIDs: true});
const indexBuildUUID = indexMap['a_1'].buildUUID;
assert(indexBuildsColl.findOne({_id: indexBuildUUID}));

// Abort the index build. It will remove the document for the index build from the
// config.system.indexBuilds collection.
jsTestLog('Aborting the index build');
const abortIndexThread = startParallelShell('assert.commandWorked(db.getMongo().getCollection("' +
                                                coll.getFullName() + '").dropIndex("a_1"))',
                                            primary.port);
checkLog.containsJson(primary, 4656010);

// Unblock the index build and wait for the threads to join.
failPoint.off();

abortIndexThread();

// Index build should be removed from the config.system.indexBuilds collection.
assert.isnull(indexBuildsColl.findOne({_id: indexBuildUUID}));

createIndex();

jsTestLog('Waiting for index build to complete');
IndexBuildTest.waitForIndexBuildToStop(testDB, coll.getName(), 'a_1');

IndexBuildTest.assertIndexes(coll, 1, ['_id_']);

rst.stopSet();
})();

/**
 * Tests that we don't hit 3 way deadlock between a interrupted index build, prepared txn and step
 * down thread.
 *
 * This test creates a scenario where:
 * 1) Starts an index build.
 * 2) Transaction gets prepared and holds the collection lock in MODE_IX.
 * 3) A dropIndexes command attempts to abort the createIndexes command. The abort command holds the
 *    RSTL lock in MODE_IX and tries to acquire a MODE_X collection lock, but blocks on the prepared
 *    transaction.
 * 4) Step down enqueues RSTL in MODE_X and waits for aborting thread to release RSTL lock.
 * 5) The aborting thread gets interrupted by step down, step down completes, and the index build
 *    eventually completes on the new primary.
 *
 * @tags: [
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */
load('jstests/noPassthrough/libs/index_build.js');
load("jstests/replsets/rslib.js");
load("jstests/core/txns/libs/prepare_helpers.js");
load("jstests/libs/fail_point_util.js");

(function() {

"use strict";

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB[collName];

TestData.dbName = dbName;
TestData.collName = collName;

jsTestLog("Do a document write");
assert.commandWorked(primaryColl.insert({_id: 1, a: 1}));

// Enable fail point which makes index build hang in an interruptible state.
const failPoint = "hangAfterIndexBuildDumpsInsertsFromBulk";
let res =
    assert.commandWorked(primary.adminCommand({configureFailPoint: failPoint, mode: "alwaysOn"}));
let timesEntered = res.count;

const indexName = "a_1";
const createIndex = IndexBuildTest.startIndexBuild(primary,
                                                   primaryColl.getFullName(),
                                                   {a: 1},
                                                   {name: indexName},
                                                   ErrorCodes.InterruptedDueToReplStateChange);
jsTestLog("Waiting for index build to hit failpoint");
assert.commandWorked(primary.adminCommand({
    waitForFailPoint: failPoint,
    timesEntered: timesEntered + 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Start txn");
const session = primary.startSession();
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection(collName);
session.startTransaction();
assert.commandWorked(sessionColl.insert({x: 1}, {$set: {y: 1}}));

jsTestLog("Prepare txn");
PrepareHelpers.prepareTransaction(session);

// Attempt to abort the index build. It will hang holding the RSTL while waiting for the collection
// X lock.
const abortIndexThread = startParallelShell(() => {
    jsTestLog("Attempting to abort the index build");
    assert.commandFailedWithCode(db.getSiblingDB('test').coll.dropIndex("a_1"),
                                 ErrorCodes.InterruptedDueToReplStateChange);
}, primary.port);
checkLog.containsJson(primary, 4656010);

// Stepdown should interrupt the dropIndexes operation and cause it to drop its queued lock.
jsTestLog("Stepping down the primary");
assert.commandWorked(primaryDB.adminCommand({"replSetStepDown": 5 * 60, "force": true}));

// Unblock the index build and wait for threads to join. The stepdown should succeed in unblocking
// the abort. In the case of single-phase index builds, the abort will succeed after the stepdown.
assert.commandWorked(primary.adminCommand({configureFailPoint: failPoint, mode: "off"}));
abortIndexThread();
createIndex();

jsTestLog("Waiting for node to become secondary");
waitForState(primary, ReplSetTest.State.SECONDARY);
// Allow the primary to be re-elected, and wait for it.

assert.commandWorked(primary.adminCommand({replSetFreeze: 0}));
rst.getPrimary();

jsTestLog("Abort txn");
assert.commandWorked(session.abortTransaction_forTesting());

jsTestLog("Waiting for index build to complete");
IndexBuildTest.waitForIndexBuildToStop(primaryDB, primaryColl.getName(), indexName);

IndexBuildTest.assertIndexes(primaryColl, 2, ["_id_", indexName]);

rst.stopSet();
})();

/**
 * Capped cursors return CappedPositionLost when the document they were positioned on gets deleted.
 * When this occurs during the collection scan phase of an index build, it will get restarted.
 */
(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/noPassthrough/libs/index_build.js");

const conn = MongoRunner.runMongod({});

const dbName = "test";
const collName = "index_build_capped_position_lost";

const db = conn.getDB(dbName);
assert.commandWorked(
    db.createCollection(collName, {capped: true, size: 1024 * 1024 * 1024, max: 5}));

const coll = db.getCollection(collName);

for (let i = 0; i < 5; i++) {
    assert.commandWorked(coll.insert({a: i}));
}

// Hang the collection scan phase of the index build when it's halfway finished.
let fp = configureFailPoint(
    conn, "hangIndexBuildDuringCollectionScanPhaseAfterInsertion", {fieldsToMatch: {a: 3}});

const awaitCreateIndex = IndexBuildTest.startIndexBuild(conn, coll.getFullName(), {a: 1});
fp.wait();

// Rollover the capped collection.
for (let i = 5; i < 10; i++) {
    assert.commandWorked(coll.insert({a: i}));
}

fp.off();
checkLog.containsJson(conn, 5470300, {
    error: function(error) {
        return error.code === ErrorCodes.CappedPositionLost;
    }
});                                                     // Collection scan restarted.
checkLog.containsJson(conn, 20391, {totalRecords: 5});  // Collection scan complete.

awaitCreateIndex();

IndexBuildTest.assertIndexes(coll, 2, ["_id_", "a_1"]);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests that secondaries drain side writes while waiting for the primary to commit an index build.
 *
 * This test does not make very many correctness assertions because this exercises a performance
 * optimization. Instead we log the time difference between how long the primary and secondary took
 * to complete the index builds. The expectation is that these values are close to each other.
 *
 * @tags: [
 *   requires_replication,
 * ]
 *
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

replSet.startSet();
replSet.initiate();

const primary = replSet.getPrimary();

const dbName = 'test';
const primaryDB = primary.getDB(dbName);
const coll = primaryDB.test;

let insertDocs = function(numDocs) {
    const bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < numDocs; i++) {
        bulk.insert({a: i, b: i});
    }
    assert.commandWorked(bulk.execute());
};
insertDocs(10000);
replSet.awaitReplication();

// Start and pause the index build on the primary so that it does not start collection scanning.
IndexBuildTest.pauseIndexBuilds(primary);
const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1, b: 1});

const secondary = replSet.getSecondary();
const secondaryDB = secondary.getDB(dbName);

// Wait until the secondary reports that it is ready to commit.
// "Index build waiting for next action before completing final phase"
checkLog.containsJson(secondary, 3856203);

// Insert a high volume of documents. Since the secondary has reported that it is ready to commit,
// the expectation is that the secondary will intercept and drain these writes as they are
// replicated from primary.
insertDocs(50000);
// "index build: drained side writes"
checkLog.containsJson(secondary, 20689);

// Record how long it takes for the index build to complete from this point onward.
let start = new Date();
IndexBuildTest.resumeIndexBuilds(primary);

// Wait for index build to finish on primary.
createIdx();
let primaryEnd = new Date();

// Wait for the index build to complete on the secondary.
IndexBuildTest.waitForIndexBuildToStop(secondaryDB);
let secondaryEnd = new Date();

// We don't make any assertions about these times, just report them for informational purposes. The
// expectation is that they are as close to each other as possible, which would suggest that the
// secondary does not spend more time completing the index than the primary.
jsTestLog("these values should be similar:");
jsTestLog("elapsed on primary: " + (primaryEnd - start));
jsTestLog("elapsed on secondary: " + (secondaryEnd - start));

IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1_b_1']);
replSet.stopSet();
})();

/**
 * Tests resource consumption metrics for index builds.
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');  // For IndexBuildTest

var rst = new ReplSetTest({
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

/**
 * Build an index. Expect that metrics are reasonable and only reported on the primary node.
 */
(function buildIndex() {
    clearMetrics(primary);
    assert.commandWorked(primaryDB[collName].createIndex({a: 1}));

    assertMetrics(primary, (metrics) => {
        // Each document is 29 bytes. Assert that we read at least as many document bytes as there
        // are in the collection. Some additional data is read from the catalog, but it has
        // randomized fields, so we don't make any exact assertions.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 1 * nDocs);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);

        // Some bytes are written to the catalog and config.system.indexBuilds collection.
        assert.gt(metrics[dbName].docBytesWritten, 0);
        assert.gt(metrics[dbName].docUnitsWritten, 0);

        // The inserted keys will vary in size from 4 to 7 bytes depending on their value. Assert
        // that the number of bytes fall within that range.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 4 * nDocs);
        assert.lt(metrics[dbName].idxEntryBytesWritten, 7 * nDocs);

        // Some index entries are written to the config.system.indexBuilds collection, but we should
        // read at least as many units as there are documents in the collection.
        assert.gte(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // The secondary should not collect metrics for replicated index builds.
    rst.awaitReplication();
    assertMetrics(secondary, (metrics) => {
        assert.eq(undefined, metrics[dbName]);
    });
})();

assert.commandWorked(primaryDB[collName].dropIndex({a: 1}));

/**
 * Build an index. Expect that metrics are reasonable and only reported on the primary node.
 */
(function buildUniqueIndex() {
    clearMetrics(primary);
    assert.commandWorked(primaryDB[collName].createIndex({a: 1}, {unique: true}));

    assertMetrics(primary, (metrics) => {
        // Each document is 29 bytes. Assert that we read at least as many document bytes as there
        // are in the collection. Some additional data is read from the catalog, but it has
        // randomized fields, so we don't make any exact assertions.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 1 * nDocs);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);

        // Some bytes are written to the catalog and config.system.indexBuilds collection.
        assert.gt(metrics[dbName].docBytesWritten, 0);
        assert.gt(metrics[dbName].docUnitsWritten, 0);

        // The inserted keys will vary in size from 4 to 7 bytes depending on their value. Assert
        // that the number of bytes fall within that range.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 4 * nDocs);
        assert.lt(metrics[dbName].idxEntryBytesWritten, 7 * nDocs);

        // Some index entries are written to the config.system.indexBuilds collection, but we should
        // read at least as many units as there are documents in the collection.
        assert.gte(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // The secondary should not collect metrics for replicated index builds.
    rst.awaitReplication();
    assertMetrics(secondary, (metrics) => {
        assert.eq(undefined, metrics[dbName]);
    });
})();

assert.commandWorked(primaryDB[collName].dropIndex({a: 1}));

/**
 * Build a unique index that fails. Expect that metrics are reasonable and only reported on the
 * primary node.
 */
(function buildFailedUniqueIndex() {
    // Insert a document at the end that makes the index non-unique.
    assert.commandWorked(primaryDB[collName].insert({a: (nDocs - 1)}));

    clearMetrics(primary);
    assert.commandFailedWithCode(primaryDB[collName].createIndex({a: 1}, {unique: true}),
                                 ErrorCodes.DuplicateKey);

    assertMetrics(primary, (metrics) => {
        // Each document is 29 bytes. Assert that we read at least as many document bytes as there
        // are in the collection. Some additional data is read from the catalog, but it has
        // randomized fields, so we don't make any exact assertions.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 1 * nDocs);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);

        // Some bytes are written to the catalog and config.system.indexBuilds collection.
        assert.gt(metrics[dbName].docBytesWritten, 0);
        assert.gt(metrics[dbName].docUnitsWritten, 0);

        // The inserted keys will vary in size from 4 to 7 bytes depending on their value. Assert
        // that the number of bytes fall within that range.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 4 * nDocs);
        assert.lt(metrics[dbName].idxEntryBytesWritten, 7 * nDocs);

        // Some index entries are written to the config.system.indexBuilds collection, but we should
        // read at least as many units as there are documents in the collection.
        assert.gte(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // The secondary should not collect metrics for replicated index builds.
    rst.awaitReplication();
    assertMetrics(secondary, (metrics) => {
        assert.eq(undefined, metrics[dbName]);
    });
})();

/**
 * Abort an active index build. Expect that the primary node that aborts the index build collects
 * and reports read metrics.
 */
(function buildIndexInterrupt() {
    clearMetrics(primary);

    // Hang the index build after kicking off the build on the primary, but before scanning the
    // collection.
    const failPoint = configureFailPoint(primary, 'hangAfterStartingIndexBuildUnlocked');
    const awaitIndex = IndexBuildTest.startIndexBuild(
        primary, primaryDB[collName].getFullName(), {a: 1}, {}, [ErrorCodes.IndexBuildAborted]);

    // Waits until the collection scan is finished.
    failPoint.wait();

    // Abort the index build and wait for it to exit.
    const abortIndexThread =
        startParallelShell('assert.commandWorked(db.getMongo().getCollection("' +
                               primaryDB[collName].getFullName() + '").dropIndex({a: 1}))',
                           primary.port);
    checkLog.containsJson(primary, 4656010);

    failPoint.off();

    abortIndexThread();
    awaitIndex();

    // Wait for the abort to replicate.
    rst.awaitReplication();

    assertMetrics(primary, (metrics) => {
        printjson(metrics);
        // Each document is 29 bytes. Assert that we read at least as many document bytes as there
        // are in the collection since the index build is interrupted after this step. Some
        // additional data is read from the catalog, but it has randomized fields, so we don't make
        // any exact assertions.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 1 * nDocs);
        assert.eq(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.eq(metrics[dbName].secondaryMetrics.docUnitsRead, 0);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);
        assert.eq(metrics[dbName].secondaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].secondaryMetrics.sorterSpills, 0);

        // Some bytes are written to the catalog and config.system.indexBuilds collection.
        assert.gt(metrics[dbName].docBytesWritten, 0);
        assert.gt(metrics[dbName].docUnitsWritten, 0);

        // The index build will have been interrupted before inserting any keys into the index,
        // however it will have written documents into the config.system.indexBuilds collection when
        // created and interrupted by the drop.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 0);
        assert.lte(metrics[dbName].idxEntryBytesWritten, 40);
        assert.gt(metrics[dbName].idxEntryUnitsWritten, 0);
        assert.lte(metrics[dbName].idxEntryUnitsWritten, 4);

        assert.lt(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // No metrics should be collected on the secondary.
    assertMetrics(secondary, (metrics) => {
        assert(!metrics[dbName]);
    });

    // Ensure the index was actually built. Do this after checking metrics because the helper calls
    // listIndexes which contributes to metrics.
    IndexBuildTest.assertIndexes(primaryDB[collName], 1, ['_id_']);
    IndexBuildTest.assertIndexes(secondaryDB[collName], 1, ['_id_']);
})();

/**
 * Start an index build on one node and commit it on a different node. Expect that the primary node
 * that commits the index build collects and reports read metrics attributed to the primary state.
 * The the stepped-down node should not collect anything.
 */
(function buildIndexWithStepDown() {
    clearMetrics(primary);
    clearMetrics(secondary);

    // Hang the index build after kicking off the build on the secondary, but before scanning the
    // collection.
    IndexBuildTest.pauseIndexBuilds(primary);
    const awaitIndex = IndexBuildTest.startIndexBuild(primary,
                                                      primaryDB[collName].getFullName(),
                                                      {a: 1},
                                                      {},
                                                      [ErrorCodes.InterruptedDueToReplStateChange]);
    IndexBuildTest.waitForIndexBuildToStart(secondaryDB);

    // Step down the primary node. The command will return an error but the index build will
    // continue running.
    assert.commandWorked(primary.adminCommand({replSetStepDown: 60, force: true}));
    awaitIndex();

    // Allow the secondary to take over. Note that it needs a quorum (by default a majority) and
    // will wait for the old primary to complete.
    rst.stepUp(secondary);

    // Allow the index build to resume and wait for it to commit.
    IndexBuildTest.resumeIndexBuilds(primary);
    IndexBuildTest.waitForIndexBuildToStop(secondaryDB);
    rst.awaitReplication();

    // Get the metrics from what is now the new primary.
    assertMetrics(secondary, (metrics) => {
        // Each document is 29 bytes. Assert that we read at least as many document bytes as there
        // are in the collection. Some additional data is read from the catalog, but it has
        // randomized fields, so we don't make any exact assertions.
        assert.gt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.gt(metrics[dbName].primaryMetrics.docUnitsRead, 1 * nDocs);
        assert.eq(metrics[dbName].secondaryMetrics.docBytesRead, 0);
        assert.eq(metrics[dbName].secondaryMetrics.docUnitsRead, 0);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);
        assert.eq(metrics[dbName].secondaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].secondaryMetrics.sorterSpills, 0);

        // Some bytes are written to the catalog and config.system.indexBuilds collection.
        assert.gt(metrics[dbName].docBytesWritten, 0);
        assert.gt(metrics[dbName].docUnitsWritten, 0);

        // The inserted keys will vary in size from 4 to 7 bytes depending on their value. Assert
        // that the number of bytes fall within that range.
        assert.gt(metrics[dbName].idxEntryBytesWritten, 4 * nDocs);
        assert.lt(metrics[dbName].idxEntryBytesWritten, 7 * nDocs);

        // Some index entries are written to the config.system.indexBuilds collection, but we should
        // read at least as many units as there are documents in the collection.
        assert.gte(metrics[dbName].idxEntryUnitsWritten, 1 * nDocs);
    });

    // No significant metrics should be collected on the old primary.
    assertMetrics(primary, (metrics) => {
        // The old primary may have read document bytes on the catalog and config.system.indexBuilds
        // when setting up, but ensure that it does not read an entire collection's worth of data.
        // The reads are attributed to the secondary state because the node is no longer primary
        // when it aggregates its metrics after getting interrupted by the stepdown.
        assert.gte(metrics[dbName].primaryMetrics.docBytesRead, 0);
        assert.lt(metrics[dbName].primaryMetrics.docBytesRead, 29 * nDocs);
        assert.lt(metrics[dbName].secondaryMetrics.docBytesRead, 29 * nDocs);

        // We intentionally do not collect sorting metrics for index builds due to their already
        // high impact on the server.
        assert.eq(metrics[dbName].primaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].primaryMetrics.sorterSpills, 0);
        assert.eq(metrics[dbName].secondaryMetrics.keysSorted, 0);
        assert.eq(metrics[dbName].secondaryMetrics.sorterSpills, 0);

        assert.eq(metrics[dbName].docBytesWritten, 0);
        assert.eq(metrics[dbName].docUnitsWritten, 0);
        assert.eq(metrics[dbName].idxEntryBytesWritten, 0);
        assert.eq(metrics[dbName].idxEntryBytesWritten, 0);
        assert.eq(metrics[dbName].idxEntryUnitsWritten, 0);
    });

    // Ensure the index was actually built. Do this after checking metrics because the helper calls
    // listIndexes which contributes to metrics.
    IndexBuildTest.assertIndexes(primaryDB[collName], 2, ['_id_', 'a_1']);
    IndexBuildTest.assertIndexes(secondaryDB[collName], 2, ['_id_', 'a_1']);
})();
rst.stopSet();
}());
/**
 * Tests restarting a secondary once an index build starts. The index build should complete when the
 * node starts back up.
 *
 * @tags: [
 *   requires_journaling,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');

const replTest = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on the secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
            slowms: 30000,  // Don't log slow operations on the secondary. See SERVER-44821.
        },
    ]
});

replTest.startSet();
replTest.initiate();

const primary = replTest.getPrimary();
const secondary = replTest.getSecondary();
// The default WC is majority and this test can't satisfy majority writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

const primaryDB = primary.getDB('test');
const secondaryDB = secondary.getDB('test');

const collectionName = jsTestName();
const coll = primaryDB.getCollection(collectionName);

const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 100; ++i) {
    bulk.insert({i: i, x: i, y: i});
}
assert.commandWorked(bulk.execute({j: true}));

// Make sure the documents make it to the secondary.
replTest.awaitReplication();

// Pause the index build on the primary after replicating the startIndexBuild oplog entry.
IndexBuildTest.pauseIndexBuilds(primaryDB);
const indexi = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {i: 1});
const indexx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {x: 1});
const indexy = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {y: 1});

// Wait for build to start on the secondary.
jsTestLog("Waiting for all index builds to start on the secondary");
IndexBuildTest.waitForIndexBuildToStart(secondaryDB, coll.getName(), "i_1");
IndexBuildTest.waitForIndexBuildToStart(secondaryDB, coll.getName(), "x_1");
IndexBuildTest.waitForIndexBuildToStart(secondaryDB, coll.getName(), "y_1");

replTest.stop(secondary);
replTest.start(secondary,
               {
                   setParameter: {
                       "failpoint.hangAfterSettingUpIndexBuildUnlocked": tojson({mode: "alwaysOn"}),
                   }
               },
               true /* restart */);

// Verify that we do not wait for the index build to complete on startup.
assert.soonNoExcept(() => {
    IndexBuildTest.assertIndexes(secondaryDB.getCollection(collectionName),
                                 4,
                                 ["_id_"],
                                 ["i_1", "x_1", "y_1"],
                                 {includeBuildUUIDs: true});
    return true;
});

assert.commandWorked(secondary.adminCommand(
    {configureFailPoint: 'hangAfterSettingUpIndexBuildUnlocked', mode: 'off'}));

// Let index build complete on primary, which replicates a commitIndexBuild to the secondary.
IndexBuildTest.resumeIndexBuilds(primaryDB);

assert.soonNoExcept(function() {
    return 4 === secondaryDB.getCollection(collectionName).getIndexes().length;
}, "Index build did not complete after restart");
indexi();
indexx();
indexy();

replTest.stopSet();
}());

/**
 * Restarts replica set members in standalone mode after a shutdown during an in-progress two-phase
 * index build.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');
const rst = new ReplSetTest({
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

const nodes = rst.startSet();
rst.initiate();

const dbName = 'test';
const collName = 'testColl';

const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB.getCollection(collName);
const secondary = rst.getSecondary();
const secondaryDB = secondary.getDB(dbName);

assert.commandWorked(primaryColl.insert({a: 1}));

jsTest.log("Starting an index build on the primary and waiting for the secondary.");
IndexBuildTest.pauseIndexBuilds(primary);
const indexSpec = {
    a: 1
};
const indexName = "a_1";
const createIndexCmd =
    IndexBuildTest.startIndexBuild(primary, primaryColl.getFullName(), indexSpec);
IndexBuildTest.waitForIndexBuildToStart(secondaryDB, collName, indexName);

jsTest.log("Force checkpoints to move the durable timestamps forward.");
rst.awaitReplication();
assert.commandWorked(primary.adminCommand({fsync: 1}));
assert.commandWorked(secondary.adminCommand({fsync: 1}));
jsTest.log("Checkpoints taken. Stopping replica set to restart individual nodes in standalone.");

TestData.skipCheckDBHashes = true;
rst.stopSet(/*signal=*/null, /*forRestart=*/true);
TestData.skipCheckDBHashes = false;

function restartStandalone(node) {
    // Startup a mongod process on the nodes data files with recoverFromOplogAsStandalone=true. This
    // parameter ensures that when the standalone starts up, it applies all unapplied oplog entries
    // since the last shutdown.
    const recoveryMongod = MongoRunner.runMongod({
        dbpath: node.dbpath,
        noReplSet: true,
        noCleanData: true,
        setParameter: 'recoverFromOplogAsStandalone=true'
    });

    // We need to shutdown this instance of mongod as using the recoverFromOplogAsStandalone=true
    // parameter puts the server into read-only mode, but we need to be able to perform writes for
    // this test.
    MongoRunner.stopMongod(recoveryMongod);

    return MongoRunner.runMongod({dbpath: node.dbpath, noReplSet: true, noCleanData: true});
}

(function restartPrimaryAsStandaloneAndCreate() {
    jsTest.log("Restarting primary in standalone mode.");
    const mongod = restartStandalone(primary);
    const db = mongod.getDB(dbName);
    const coll = db.getCollection(collName);
    IndexBuildTest.assertIndexes(coll, 2, ["_id_"], [indexName], {includeBuildUUIDs: true});

    // As a standalone, this should fail because of the unfinished index.
    assert.commandFailedWithCode(coll.createIndex(indexSpec), ErrorCodes.CannotCreateIndex);

    // Drop the index, then recreate it successfully.
    assert.commandWorked(coll.dropIndex(indexSpec));
    assert.commandWorked(coll.createIndex(indexSpec));
    IndexBuildTest.assertIndexes(coll, 2, ["_id_", indexName]);
    MongoRunner.stopMongod(mongod);
})();

(function restartSecondaryAsStandaloneAndCreate() {
    jsTest.log("Restarting secondary in standalone mode.");
    const mongod = restartStandalone(secondary);
    const db = mongod.getDB(dbName);
    const coll = db.getCollection(collName);
    IndexBuildTest.assertIndexes(coll, 2, ["_id_"], [indexName], {includeBuildUUIDs: true});

    // As a standalone, this should fail because of the unfinished index.
    assert.commandFailedWithCode(coll.createIndex(indexSpec), ErrorCodes.CannotCreateIndex);

    // Drop the index, then recreate it successfully.
    assert.commandWorked(coll.dropIndex(indexSpec));
    assert.commandWorked(coll.createIndex(indexSpec));
    IndexBuildTest.assertIndexes(coll, 2, ["_id_", indexName]);
    MongoRunner.stopMongod(mongod);
})();

// TODO: SERVER-59688
createIndexCmd({checkExitSuccess: false});
})();

/*
 * This test ensures an index build can yield during bulk load phase.
 */
(function() {

"use strict";

load("jstests/noPassthrough/libs/index_build.js");
load("jstests/libs/fail_point_util.js");

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);

TestData.dbName = jsTestName();
TestData.collName = "coll";

const testDB = conn.getDB(TestData.dbName);
testDB.getCollection(TestData.collName).drop();

assert.commandWorked(testDB.createCollection(TestData.collName));
const coll = testDB.getCollection(TestData.collName);

for (let i = 0; i < 3; i++) {
    assert.commandWorked(coll.insert({_id: i, x: i}));
}

// Make the index build bulk load yield often.
assert.commandWorked(
    conn.adminCommand({setParameter: 1, internalIndexBuildBulkLoadYieldIterations: 1}));

jsTestLog("Enable hangDuringIndexBuildBulkLoadYield fail point");
let failpoint = configureFailPoint(
    testDB, "hangDuringIndexBuildBulkLoadYield", {namespace: coll.getFullName()});

jsTestLog("Create index");
const awaitIndex = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), coll.getFullName(), {x: 1}, {}, [ErrorCodes.IndexBuildAborted]);

// Wait until index build (bulk load phase) yields.
jsTestLog("Wait for the index build to yield and hang");
failpoint.wait();

jsTestLog("Drop the collection");
const awaitDrop = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(testDB.runCommand({drop: TestData.collName}));
}, conn.port);

// Wait until the index build starts aborting to make sure the drop happens before the index build
// finishes.
checkLog.containsJson(testDB, 465611);
failpoint.off();

// "Index build: joined after abort".
checkLog.containsJson(testDB, 20655);

awaitIndex();
awaitDrop();

MongoRunner.stopMongod(conn);
})();
/*
 * This test ensures it is not possible for the collection scan phase of an index build to encounter
 * a prepare conflict after yielding. See SERVER-44577.
 *
 * @tags: [
 *   requires_replication,
 *   uses_transactions,
 *   uses_prepare_transaction,
 * ]
 */
load("jstests/core/txns/libs/prepare_helpers.js");  // For PrepareHelpers.
load("jstests/noPassthrough/libs/index_build.js");  // For IndexBuildTest
load("jstests/libs/fail_point_util.js");

(function() {

"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = jsTestName();
const collName = "coll";

const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);
const primaryAdmin = primary.getDB('admin');
const primaryColl = primaryDB[collName];
const collNss = primaryColl.getFullName();

for (var i = 0; i < 3; i++) {
    assert.commandWorked(primaryColl.insert({_id: i, x: i}));
}
rst.awaitReplication();

// Make the index build collection scan yield often.
assert.commandWorked(primary.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 2}));

jsTestLog("Enable setYieldAllLocksHang fail point");
let res = assert.commandWorked(primaryAdmin.runCommand(
    {configureFailPoint: "setYieldAllLocksHang", data: {namespace: collNss}, mode: "alwaysOn"}));
let timesEntered = res.count;

jsTestLog("Create index");
const awaitIndex = IndexBuildTest.startIndexBuild(primary, primaryColl.getFullName(), {x: 1});

// Wait until index build (collection scan phase) yields.
jsTestLog("Wait for the index build to yield and hang");
assert.commandWorked(primaryAdmin.runCommand({
    waitForFailPoint: "setYieldAllLocksHang",
    timesEntered: timesEntered + 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Start a txn");
const session = primary.startSession();
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection(collName);
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: 20}));

jsTestLog("Prepare txn");
PrepareHelpers.prepareTransaction(session);

// This will make the hybrid build previously started resume.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "setYieldAllLocksHang", mode: "off"}));

jsTestLog("Wait for index build to complete collection scanning phase");
checkLog.containsJson(primary, 20391);

session.abortTransaction_forTesting();
awaitIndex();

rst.stopSet();
})();
/**
 * Tests that index builds don't block on operations that conflict because they are in a prepared
 * state.
 *
 * @tags: [
 *   requires_replication,
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */
(function() {
"use strict";

load("jstests/core/txns/libs/prepare_helpers.js");  // for PrepareHelpers
load("jstests/noPassthrough/libs/index_build.js");  // for IndexBuildTest

const replSetTest = new ReplSetTest({
    name: "index_builds_ignore_prepare_conflicts",
    nodes: [
        {},
        {rsConfig: {priority: 0}},
    ],
});
replSetTest.startSet();
replSetTest.initiate();

const primary = replSetTest.getPrimary();
const primaryDB = primary.getDB('test');

let numDocs = 10;
let setUp = function(coll) {
    coll.drop();
    let bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < numDocs; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());
};

/**
 * Run a background index build, and depending on the provided node, 'conn', ensure that a
 * prepared update does not introduce prepare conflicts on the index builder.
 */
let runTest = function(conn) {
    const testDB = conn.getDB('test');

    const collName = 'index_builds_ignore_prepare_conflicts';
    const coll = primaryDB.getCollection(collName);
    setUp(coll);

    // Start and pause an index build.
    IndexBuildTest.pauseIndexBuilds(conn);
    const awaitBuild = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {i: 1});
    const opId = IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, 'i_1');

    // This insert will block until the index build pauses and releases its exclusive lock.
    // This guarantees that the subsequent transaction can immediately acquire a lock and not
    // fail with a LockTimeout error.
    assert.commandWorked(coll.insert({i: numDocs++}));

    // Start a session and introduce a document that is in a prepared state, but should be
    // ignored by the index build, at least until the transaction commits.
    const session = primaryDB.getMongo().startSession();
    const sessionDB = session.getDatabase('test');
    const sessionColl = sessionDB.getCollection(collName);
    session.startTransaction();
    assert.commandWorked(sessionColl.update({i: 0}, {i: "prepared"}));
    // Use w:1 because the secondary will be unable to replicate the prepare while an index
    // build is running.
    const prepareTimestamp = PrepareHelpers.prepareTransaction(session, {w: 1});

    // Let the index build continue until just before it completes. Set the failpoint just
    // before the second drain, which would take lock that conflicts with the prepared
    // transaction and prevent the index build from completing entirely.
    const failPointName = "hangAfterIndexBuildFirstDrain";
    clearRawMongoProgramOutput();
    assert.commandWorked(conn.adminCommand({configureFailPoint: failPointName, mode: "alwaysOn"}));

    // Unpause the index build from the first failpoint so that it can resume and pause at the
    // next failpoint.
    IndexBuildTest.resumeIndexBuilds(conn);
    assert.soon(() =>
                    rawMongoProgramOutput().indexOf("Hanging after index build first drain") >= 0);

    // Right before the index build completes, ensure no prepare conflicts were hit.
    IndexBuildTest.assertIndexBuildCurrentOpContents(testDB, opId, (op) => {
        printjson(op);
        assert.eq(undefined, op.prepareReadConflicts);
    });

    // Because prepare uses w:1, ensure it is majority committed before committing the
    // transaction.
    PrepareHelpers.awaitMajorityCommitted(replSetTest, prepareTimestamp);

    // Commit the transaction before completing the index build, releasing locks which will
    // allow the index build to complete.
    assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestamp));

    // Allow the index build to complete.
    assert.commandWorked(conn.adminCommand({configureFailPoint: failPointName, mode: "off"}));

    awaitBuild();
    IndexBuildTest.waitForIndexBuildToStop(testDB, collName, "i_1");

    assert.eq(numDocs, coll.count());
    assert.eq(numDocs, coll.find().itcount());
};

runTest(replSetTest.getPrimary());

replSetTest.stopSet();
})();

/**
 * Confirms slow currentOp logging does not conflict with processing commitIndexBuild, which may
 * block replication.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

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

assert.commandWorked(coll.insert({a: 1}));

const secondary = rst.getSecondary();
IndexBuildTest.pauseIndexBuilds(secondary);

const createIdx =
    IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1}, {background: true});

// Wait for secondary to start processing commitIndexBuild oplog entry from the primary.
const secondaryDB = secondary.getDB(testDB.getName());
assert.soon(function() {
    const filter = {
        'command.commitIndexBuild': {$exists: true},
        'waitingForLatch.captureName': 'AnonymousLockable',
        '$all': true,
    };
    const result = assert.commandWorked(secondaryDB.currentOp(filter));
    assert.lte(
        result.inprog.length,
        1,
        'expected at most one commitIndexBuild entry in currentOp() output: ' + tojson(result));
    if (result.inprog.length == 0) {
        return false;
    }
    jsTestLog('Secondary started processing commitIndexBuild: ' + tojson(result));
    return true;
}, 'secondary did not receive commitIndexBuild oplog entry');

jsTestLog('Running currentOp() with slow operation logging.');
// Lower slowms to make currentOp() log slow operation while the secondary is procesing the
// commitIndexBuild oplog entry during oplog application.
const profileResult = assert.commandWorked(secondaryDB.setProfilingLevel(0, {slowms: -1}));
jsTestLog('Configured profiling to always log slow ops: ' + tojson(profileResult));
const currentOpResult = assert.commandWorked(secondaryDB.currentOp());
jsTestLog('currentOp() with slow operation logging: ' + tojson(currentOpResult));
assert.commandWorked(secondaryDB.setProfilingLevel(0, {slowms: 30000}));
jsTestLog('Completed currentOp() with slow operation logging.');

const opId = IndexBuildTest.waitForIndexBuildToStart(secondaryDB);

// Wait for the index build to stop.
IndexBuildTest.resumeIndexBuilds(secondary);
IndexBuildTest.waitForIndexBuildToStop(secondaryDB);

// Wait for parallel shell to stop.
createIdx();

rst.stopSet();
})();

/**
 * If a user attempts to downgrade the server while there is an index build in progress, the
 * downgrade should succeed without blocking.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
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
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(coll.insert({a: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1});
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, coll.getName(), 'a_1');

// Downgrade the primary using the setFeatureCompatibilityVersion command.
try {
    assert.commandWorked(primary.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}));
} finally {
    IndexBuildTest.resumeIndexBuilds(primary);
}

IndexBuildTest.waitForIndexBuildToStop(testDB);

createIdx();

IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1']);

// This confirms that the downgrade command will complete successfully after the index build has
// completed.
assert.commandWorked(primary.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}));

rst.stopSet();
})();

/**
 * Confirms that creating index creation on an empty collection does not require the thread pool
 * that we use for hybrid index builds. The fail points that we typically use to suspend hybrid
 * should not affect the progress of index builds on empty collections.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
            slowms: 30000,  // Don't log slow operations on secondary. See SERVER-44821.
        },
    ]
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');

// This test uses a non-empty collection as a control by suspending index builds on the thread pool.
// The optimization for an empty collection should not go through the code path for an index build
// that requires a collection scan and the hybrid index build machinery for managing side writes.
IndexBuildTest.pauseIndexBuilds(primary);
const coll = testDB.getCollection('test');
assert.commandWorked(coll.insert({a: 1}));
const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1});

try {
    // When the index build starts, find its op id.
    const secondaryDB = primary.getDB(testDB.getName());
    const opId = IndexBuildTest.waitForIndexBuildToStart(testDB);

    // Creating an index on an empty collection should not be affected by the fail point we used
    // to suspend index builds.
    const emptyColl = testDB.getCollection('emptyColl');
    assert.commandWorked(testDB.createCollection(emptyColl.getName()));

    // Build index with a writeConcern that ensures the index is finished on all the nodes.
    assert.commandWorked(testDB.runCommand({
        createIndexes: emptyColl.getName(),
        indexes: [{key: {b: 1}, name: 'b_1'}],
        writeConcern: {
            w: nodes.length,
            wtimeout: ReplSetTest.kDefaultTimeoutMS,
        },
    }));
    IndexBuildTest.assertIndexes(emptyColl, 2, ['_id_', 'b_1']);

    const secondary = rst.getSecondary();
    const secondaryEmptyColl = secondary.getCollection(emptyColl.getFullName());
    IndexBuildTest.assertIndexes(secondaryEmptyColl, 2, ['_id_', 'b_1']);

    // Index build optimatization for empty collection is replicated via old-style createIndexes
    // oplog entry.
    const cmdNs = testDB.getCollection('$cmd').getFullName();
    const ops = rst.dumpOplog(
        primary, {op: 'c', ns: cmdNs, 'o.createIndexes': emptyColl.getName(), 'o.name': 'b_1'});
    assert.eq(1,
              ops.length,
              'createIndexes oplog entry not generated for empty collection: ' + tojson(ops));
} finally {
    // Wait for the index build to stop.
    IndexBuildTest.resumeIndexBuilds(primary);
    IndexBuildTest.waitForIndexBuildToStop(testDB);
}

// Expect successful createIndex command invocation in parallel shell. A new index should be
// present on the primary.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1']);

rst.stopSet();
})();

/**
 * Confirms that index creation on a secondary does not use the optimization for empty collections
 * that a primary would apply to two phase index builds.
 *
 * This test starts an index build on a non-empty collection but clears the collection
 * before the index build is added to the catalog. This causes the secondary to see an empty
 * collection.
 *
 * This test should also work when the primary builds the index single-phased. The secondary should
 * be able to optimize for the empty collection case and build the index inlined.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');
load("jstests/libs/fail_point_util.js");

// Use a 3-node replica set config to ensure that the primary waits for the secondaries when the
// commit quorum is in effect.
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

assert.commandWorked(coll.insert({a: 1}));

const res = assert.commandWorked(primary.adminCommand(
    {configureFailPoint: 'hangBeforeInitializingIndexBuild', mode: 'alwaysOn'}));
const failpointTimesEntered = res.count;

const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1});

try {
    assert.commandWorked(primary.adminCommand({
        waitForFailPoint: "hangBeforeInitializingIndexBuild",
        timesEntered: failpointTimesEntered + 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    }));

    // Remove the document from the collection so that the secondary sees an empty collection.
    assert.commandWorked(coll.remove({a: 1}));
} finally {
    assert.commandWorked(primary.adminCommand(
        {configureFailPoint: 'hangBeforeInitializingIndexBuild', mode: 'off'}));
}

// Expect successful createIndex command invocation in parallel shell. A new index should be
// present on the primary.
createIdx();
IndexBuildTest.assertIndexes(coll, 2, ['_id_', 'a_1']);

rst.awaitReplication();
const secondary = rst.getSecondary();
const secondaryDB = primary.getDB(testDB.getName());
const secondaryColl = secondary.getCollection(coll.getFullName());
IndexBuildTest.assertIndexes(secondaryColl, 2, ['_id_', 'a_1']);

rst.stopSet();
})();

/**
 * Confirms that an index build is aborted after step-up by a new primary when there are key
 * generation errors. This test orchestrates a scenario such that a secondary detects (and
 * ignores) an indexing error. After step-up, the node retries indexing the skipped record before
 * completing. The expected result is that the node, now primary, aborts the index build for the
 * entire replica set.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
    nodes: [
        {},
        {},
    ],
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

// Insert a document that cannot be indexed because it causes a CannotIndexParallelArrays error
// code.
const badDoc = {
    _id: 0,
    a: [0, 1],
    b: [2, 3]
};
assert.commandWorked(coll.insert(badDoc));

// Start an index build on primary and secondary, but prevent the primary from scanning the
// collection. Do not stop the secondary; intentionally let it scan the invalid document, which we
// will resolve later.

// We are using this fail point to pause the index build before it starts the collection scan.
// This is important for this test because we are mutating the collection state before the index
// builder is able to observe the invalid document.
// By comparison, IndexBuildTest.pauseIndexBuilds() stalls the index build in the middle of the
// collection scan.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterInitializingIndexBuild', mode: 'alwaysOn'}));
const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1, b: 1});

// Wait for the index build to start on the secondary.
const secondary = rst.getSecondary();
const secondaryDB = secondary.getDB(testDB.getName());
const secondaryColl = secondaryDB.getCollection(coll.getName());
IndexBuildTest.waitForIndexBuildToStart(secondaryDB);
rst.awaitReplication();
IndexBuildTest.assertIndexes(secondaryColl, 2, ["_id_"], ["a_1_b_1"], {includeBuildUUIDs: true});

// Step down the primary.
const stepDown = startParallelShell(() => {
    assert.commandWorked(db.adminCommand({"replSetStepDown": 60, "force": true}));
}, primary.port);

// Expect a failed createIndex command invocation in the parallel shell due to stepdown even though
// the index build will continue in the background.
const exitCode = createIdx({checkExitSuccess: false});
assert.neq(0, exitCode, 'expected shell to exit abnormally due to index build being terminated');
checkLog.containsJson(primary, 20441);

// Wait for stepdown to complete.
stepDown();

// Unblock the index build on the old primary.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'hangAfterInitializingIndexBuild', mode: 'off'}));

const newPrimary = rst.getPrimary();
const newPrimaryDB = newPrimary.getDB('test');
const newPrimaryColl = newPrimaryDB.getCollection('test');

// Ensure the old primary doesn't take over again.
assert.neq(primary.port, newPrimary.port);

// The index should not be present on the old primary after processing the abortIndexBuild oplog
// entry from the new primary.
jsTestLog("waiting for index build to stop on old primary");
IndexBuildTest.waitForIndexBuildToStop(testDB);
rst.awaitReplication();
IndexBuildTest.assertIndexes(coll, 1, ['_id_']);

// Check that index was not built on the new primary.
jsTestLog("waiting for index build to stop on new primary");
IndexBuildTest.waitForIndexBuildToStop(newPrimaryDB);
IndexBuildTest.assertIndexes(newPrimaryColl, 1, ['_id_']);

rst.stopSet();
})();
