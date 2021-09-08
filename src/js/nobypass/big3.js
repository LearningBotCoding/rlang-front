/**
 * Tests that different values for the same configuration string key have the following order of
 * preference:
 *   1. index-specific options specified to createIndex().
 *   2. collection-wide options specified as "indexOptionDefaults" to createCollection().
 *   3. system-wide options specified by --wiredTigerIndexConfigString or by
 *     inMemoryIndexConfigString.
 */
(function() {
'use strict';

var engine = 'wiredTiger';
if (jsTest.options().storageEngine) {
    engine = jsTest.options().storageEngine;
}

// Skip this test if not running with the right storage engine.
if (engine !== 'wiredTiger' && engine !== 'inMemory') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger" or "inMemory"');
    return;
}

// Skip this test when 'xxxIndexConfigString' is already set in TestData.
// TODO: This test can be enabled when MongoRunner supports combining WT config strings with
// commas.
if (jsTest.options()[engine + 'IndexConfigString']) {
    jsTest.log('Skipping test because system-wide defaults for index options are already set');
    return;
}

// Use different values for the same configuration string key to test that index-specific
// options override collection-wide options, and that collection-wide options override
// system-wide options.
var systemWideConfigString = 'split_pct=70,';
var collectionWideConfigString = 'split_pct=75,';
var indexSpecificConfigString = 'split_pct=80,';

// Start up a mongod with system-wide defaults for index options and create a collection without
// any additional options. Tests than an index without any additional options should take on the
// system-wide defaults, whereas an index with additional options should override the
// system-wide defaults.
runTest({});

// Start up a mongod with system-wide defaults for index options and create a collection with
// additional options. Tests than an index without any additional options should take on the
// collection-wide defaults, whereas an index with additional options should override the
// collection-wide defaults.
runTest({indexOptionDefaults: collectionWideConfigString});

function runTest(collOptions) {
    var hasIndexOptionDefaults = collOptions.hasOwnProperty('indexOptionDefaults');

    var dbpath = MongoRunner.dataPath + 'wt_index_option_defaults';
    resetDbpath(dbpath);

    // Start a mongod with system-wide defaults for engine-specific index options.
    var conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
        [engine + 'IndexConfigString']: systemWideConfigString,
    });
    assert.neq(null, conn, 'mongod was unable to start up');

    var testDB = conn.getDB('test');
    var cmdObj = {create: 'coll'};

    // Apply collection-wide defaults for engine-specific index options if any were
    // specified.
    if (hasIndexOptionDefaults) {
        cmdObj.indexOptionDefaults = {
            storageEngine: {[engine]: {configString: collOptions.indexOptionDefaults}}
        };
    }
    assert.commandWorked(testDB.runCommand(cmdObj));

    // Create an index that does not specify any engine-specific options.
    assert.commandWorked(testDB.coll.createIndex({a: 1}, {name: 'without_options'}));

    // Create an index that specifies engine-specific index options.
    assert.commandWorked(testDB.coll.createIndex({b: 1}, {
        name: 'with_options',
        storageEngine: {[engine]: {configString: indexSpecificConfigString}}
    }));

    var collStats = testDB.runCommand({collStats: 'coll'});
    assert.commandWorked(collStats);

    checkIndexWithoutOptions(collStats.indexDetails);
    checkIndexWithOptions(collStats.indexDetails);

    MongoRunner.stopMongod(conn);

    function checkIndexWithoutOptions(indexDetails) {
        var indexSpec = getIndexSpecByName(testDB.coll, 'without_options');
        assert(!indexSpec.hasOwnProperty('storageEngine'),
               'no storage engine options should have been set in the index spec: ' +
                   tojson(indexSpec));

        var creationString = indexDetails.without_options.creationString;
        if (hasIndexOptionDefaults) {
            assert.eq(-1,
                      creationString.indexOf(systemWideConfigString),
                      'system-wide index option present in the creation string even though a ' +
                          'collection-wide option was specified: ' + creationString);
            assert.lte(0,
                       creationString.indexOf(collectionWideConfigString),
                       'collection-wide index option not present in the creation string: ' +
                           creationString);
        } else {
            assert.lte(
                0,
                creationString.indexOf(systemWideConfigString),
                'system-wide index option not present in the creation string: ' + creationString);
            assert.eq(-1,
                      creationString.indexOf(collectionWideConfigString),
                      'collection-wide index option present in creation string even though ' +
                          'it was not specified: ' + creationString);
        }

        assert.eq(-1,
                  creationString.indexOf(indexSpecificConfigString),
                  'index-specific option present in creation string even though it was not' +
                      ' specified: ' + creationString);
    }

    function checkIndexWithOptions(indexDetails) {
        var indexSpec = getIndexSpecByName(testDB.coll, 'with_options');
        assert(
            indexSpec.hasOwnProperty('storageEngine'),
            'storage engine options should have been set in the index spec: ' + tojson(indexSpec));
        assert.docEq({[engine]: {configString: indexSpecificConfigString}},
                     indexSpec.storageEngine,
                     engine + ' index options not present in the index spec');

        var creationString = indexDetails.with_options.creationString;
        assert.eq(-1,
                  creationString.indexOf(systemWideConfigString),
                  'system-wide index option present in the creation string even though an ' +
                      'index-specific option was specified: ' + creationString);
        assert.eq(-1,
                  creationString.indexOf(collectionWideConfigString),
                  'system-wide index option present in the creation string even though an ' +
                      'index-specific option was specified: ' + creationString);
        assert.lte(0,
                   creationString.indexOf(indexSpecificConfigString),
                   'index-specific option not present in the creation string: ' + creationString);
    }
}

function getIndexSpecByName(coll, indexName) {
    var indexes = coll.getIndexes().filter(function(spec) {
        return spec.name === indexName;
    });
    assert.eq(1, indexes.length, 'index "' + indexName + '" not found');
    return indexes[0];
}
})();

/**
 * Tests that a null embedded malformed string is rejected gracefully.
 */
(function() {
'use strict';

var engine = 'wiredTiger';
if (jsTest.options().storageEngine) {
    engine = jsTest.options().storageEngine;
}

// Skip this test if not running with the right storage engine.
if (engine !== 'wiredTiger' && engine !== 'inMemory') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger" or "inMemory"');
    return;
}

// Build an array of malformed strings to test
var malformedStrings = ["\u0000000", "\0,", "bl\0ah", "split_pct=30,\0split_pct=35,"];

// Start up a mongod.
// Test that collection and index creation with malformed creation strings fail gracefully.
runTest();

function runTest() {
    var dbpath = MongoRunner.dataPath + 'wt_malformed_creation_string';
    resetDbpath(dbpath);

    // Start a mongod
    var conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
    });
    assert.neq(null, conn, 'mongod was unable to start up');

    var testDB = conn.getDB('test');

    // Collection creation with malformed string should fail
    for (var i = 0; i < malformedStrings.length; i++) {
        assert.commandFailedWithCode(
            testDB.createCollection(
                'coll', {storageEngine: {[engine]: {configString: malformedStrings[i]}}}),
            ErrorCodes.FailedToParse);
    }

    // Create collection to test index creation on
    assert.commandWorked(testDB.createCollection('coll'));

    // Index creation with malformed string should fail
    for (var i = 0; i < malformedStrings.length; i++) {
        assert.commandFailedWithCode(testDB.coll.createIndex({a: 1}, {
            name: 'with_malformed_str',
            storageEngine: {[engine]: {configString: malformedStrings[i]}}
        }),
                                     ErrorCodes.FailedToParse);
    }

    MongoRunner.stopMongod(conn);
}
})();

/**
 * Tests that having journaled write operations since the last checkpoint triggers an error when
 * --wiredTigerEngineConfigString log=(recover=error) is specified in combination with --nojournal.
 * Also verifies that deleting the journal/ directory allows those operations to safely be ignored.
 */
(function() {
'use strict';

// Skip this test if not running with the "wiredTiger" storage engine.
if (jsTest.options().storageEngine && jsTest.options().storageEngine !== 'wiredTiger') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger"');
    return;
}

// Skip this test until we figure out why journaled writes are replayed after last checkpoint.
TestData.skipCollectionAndIndexValidation = true;

var dbpath = MongoRunner.dataPath + 'wt_nojournal_skip_recovery';
resetDbpath(dbpath);

// Start a mongod with journaling enabled.
var conn = MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    journal: '',
    // Wait an hour between checkpoints to ensure one isn't created after the fsync command is
    // executed and before the mongod is terminated. This is necessary to ensure that exactly 90
    // documents with the 'journaled' field exist in the collection.
    wiredTigerEngineConfigString: 'checkpoint=(wait=3600)'
});
assert.neq(null, conn, 'mongod was unable to start up');

// Execute unjournaled inserts, but periodically do a journaled insert. Triggers a checkpoint
// prior to the mongod being terminated.
var awaitShell = startParallelShell(function() {
    for (let loopNum = 1; true; ++loopNum) {
        var bulk = db.nojournal.initializeUnorderedBulkOp();
        for (var i = 0; i < 100; ++i) {
            bulk.insert({unjournaled: i});
        }
        assert.commandWorked(bulk.execute({j: false}));
        assert.commandWorked(db.nojournal.insert({journaled: loopNum}, {writeConcern: {j: true}}));

        // Create a checkpoint slightly before the mongod is terminated.
        if (loopNum === 90) {
            assert.commandWorked(db.adminCommand({fsync: 1}));
        }
    }
}, conn.port);

// After some journaled write operations have been performed against the mongod, send a SIGKILL
// to the process to trigger an unclean shutdown.
assert.soon(
    function() {
        var count = conn.getDB('test').nojournal.count({journaled: {$exists: true}});
        if (count >= 100) {
            MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});
            return true;
        }
        return false;
    },
    'the parallel shell did not perform at least 100 journaled inserts',
    5 * 60 * 1000 /*timeout ms*/);

var exitCode = awaitShell({checkExitSuccess: false});
assert.neq(0, exitCode, 'expected shell to exit abnormally due to mongod being terminated');

// Restart the mongod with journaling disabled, but configure it to error if the database needs
// recovery.
assert.throws(() => MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    nojournal: '',
    wiredTigerEngineConfigString: 'log=(recover=error)',
}),
              [],
              'mongod should not have started up because it requires recovery');

// Remove the journal files.
assert(removeFile(dbpath + '/journal'), 'failed to remove the journal directory');

// Restart the mongod with journaling disabled again.
conn = MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    nojournal: '',
    wiredTigerEngineConfigString: 'log=(recover=error)',
});
assert.neq(null, conn, 'mongod was unable to start up after removing the journal directory');

var count = conn.getDB('test').nojournal.count({journaled: {$exists: true}});
assert.lte(90, count, 'missing documents that were present in the last checkpoint');
assert.gte(90,
           count,
           'journaled write operations since the last checkpoint should not have been' +
               ' replayed');

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that journaled write operations that have occurred since the last checkpoint are replayed
 * when the mongod is killed and restarted with --nojournal.
 */
(function() {
'use strict';

// Skip this test if not running with the "wiredTiger" storage engine.
if (jsTest.options().storageEngine && jsTest.options().storageEngine !== 'wiredTiger') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger"');
    return;
}

// Returns a function that primarily executes unjournaled inserts, but periodically does a
// journaled insert. If 'checkpoint' is true, then the fsync command is run to create a
// checkpoint prior to the mongod being terminated.
function insertFunctionFactory(checkpoint) {
    var insertFunction = function() {
        for (var iter = 0; iter < 1000; ++iter) {
            var bulk = db.nojournal.initializeUnorderedBulkOp();
            for (var i = 0; i < 100; ++i) {
                bulk.insert({unjournaled: i});
            }
            assert.commandWorked(bulk.execute({j: false}));
            assert.commandWorked(db.nojournal.insert({journaled: iter}, {writeConcern: {j: true}}));
            if (__checkpoint_template_placeholder__ && iter === 50) {
                assert.commandWorked(db.adminCommand({fsync: 1}));
            }
        }
    };

    return '(' +
        insertFunction.toString().replace('__checkpoint_template_placeholder__',
                                          checkpoint.toString()) +
        ')();';
}

function runTest(options) {
    var dbpath = MongoRunner.dataPath + 'wt_nojournal_toggle';
    resetDbpath(dbpath);

    // Start a mongod with journaling enabled.
    var conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
        journal: '',
    });
    assert.neq(null, conn, 'mongod was unable to start up');

    // Run a mixture of journaled and unjournaled write operations against the mongod.
    var awaitShell = startParallelShell(insertFunctionFactory(options.checkpoint), conn.port);

    // After some journaled write operations have been performed against the mongod, send a
    // SIGKILL to the process to trigger an unclean shutdown.
    assert.soon(function() {
        var testDB = conn.getDB('test');
        var count = testDB.nojournal.count({journaled: {$exists: true}});
        if (count >= 100) {
            // We saw 100 journaled inserts, but visibility does not guarantee durability, so
            // do an extra journaled write to make all visible commits durable, before killing
            // the mongod.
            assert.commandWorked(testDB.nojournal.insert({final: true}, {writeConcern: {j: true}}));
            MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});
            return true;
        }
        return false;
    }, 'the parallel shell did not perform at least 100 journaled inserts');

    var exitCode = awaitShell({checkExitSuccess: false});
    assert.neq(0, exitCode, 'expected shell to exit abnormally due to mongod being terminated');

    // Restart the mongod with journaling disabled.
    conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
        nojournal: '',
    });
    assert.neq(null, conn, 'mongod was unable to restart after receiving a SIGKILL');

    var testDB = conn.getDB('test');
    assert.eq(1, testDB.nojournal.count({final: true}), 'final journaled write was not found');
    assert.lte(100,
               testDB.nojournal.count({journaled: {$exists: true}}),
               'journaled write operations since the last checkpoint were not replayed');

    var initialNumLogWrites = testDB.serverStatus().wiredTiger.log['log write operations'];
    assert.commandWorked(testDB.nojournal.insert({a: 1}, {writeConcern: {fsync: true}}));
    assert.eq(initialNumLogWrites,
              testDB.serverStatus().wiredTiger.log['log write operations'],
              'journaling is still enabled even though --nojournal was specified');

    MongoRunner.stopMongod(conn);

    // Restart the mongod with journaling enabled.
    conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
        journal: '',
    });
    assert.neq(null, conn, 'mongod was unable to start up after re-enabling journaling');

    // Change the database object to connect to the restarted mongod.
    testDB = conn.getDB('test');
    initialNumLogWrites = testDB.serverStatus().wiredTiger.log['log write operations'];

    assert.commandWorked(testDB.nojournal.insert({a: 1}, {writeConcern: {fsync: true}}));
    assert.lt(initialNumLogWrites,
              testDB.serverStatus().wiredTiger.log['log write operations'],
              'journaling is still disabled even though --journal was specified');

    MongoRunner.stopMongod(conn);
}

// Operations from the journal should be replayed even when the mongod is terminated before
// anything is written to disk.
jsTest.log('Running the test without ever creating a checkpoint');
runTest({checkpoint: false});

// Repeat the test again, but ensure that some data is written to disk before the mongod is
// terminated.
jsTest.log('Creating a checkpoint part-way through running the test');
runTest({checkpoint: true});
})();

// This test inserts multiple records into a collection creating a btree spanning multiple pages,
// restarts the server and scans the collection. This would trigger application thread to read from
// the disk as the startup would only partially load the collection data into the cache. In doing so
// check that the WiredTiger storage statistics are present in the slowop log message and in the
// system.profile collection for the profiled read operation.
//
// @tags: [requires_profiling]

(function() {
'use strict';

load("jstests/libs/profiler.js");  // For getLatestProfilerEntry.
load("jstests/libs/logv2_helpers.js");

let readStatRegx = /storage:{ data: { bytesRead: ([0-9]+)/;
if (isJsonLogNoConn()) {
    readStatRegx = /"storage":{"data":{"bytesRead":([0-9]+)/;
}

let checkLogStats = function() {
    // Check if the log output contains the expected statistics.
    let mongodLogs = rawMongoProgramOutput();
    let lines = mongodLogs.split('\n');
    let match;
    let logLineCount = 0;
    for (let line of lines) {
        if ((match = readStatRegx.exec(line)) !== null) {
            jsTestLog(line);
            logLineCount++;
        }
    }
    assert.gte(logLineCount, 1);
};

let checkSystemProfileStats = function(profileObj, statName) {
    // Check that the profiled operation contains the expected statistics.
    assert(profileObj.hasOwnProperty("storage"), tojson(profileObj));
    assert(profileObj.storage.hasOwnProperty("data"), tojson(profileObj));
    assert(profileObj.storage.data.hasOwnProperty(statName), tojson(profileObj));
};

// This test can only be run if the storageEngine is wiredTiger
if (jsTest.options().storageEngine && (jsTest.options().storageEngine !== "wiredTiger")) {
    jsTestLog("Skipping test because storageEngine is not wiredTiger");
} else {
    let name = "wt_op_stat";

    jsTestLog("run mongod");
    let conn = MongoRunner.runMongod();
    assert.neq(null, conn, "mongod was unable to start up");
    let testDB = conn.getDB(name);

    // Insert 200 documents of size 1K each, spanning multiple pages in the btree.
    let value = 'a'.repeat(1024);

    jsTestLog("insert data");
    for (let i = 0; i < 200; i++) {
        assert.commandWorked(testDB.foo.insert({x: value}));
    }

    let connport = conn.port;
    MongoRunner.stopMongod(conn);

    // Restart the server
    conn = MongoRunner.runMongod({
        restart: true,
        port: connport,
        slowms: "0",
    });

    clearRawMongoProgramOutput();

    // Scan the collection and check the bytes read statistic in the slowop log and
    // system.profile.
    testDB = conn.getDB(name);
    testDB.setProfilingLevel(2);
    jsTestLog("read data");
    let cur = testDB.foo.find();
    while (cur.hasNext()) {
        cur.next();
    }

    // Look for the storage statistics in the profiled output of the find command.
    let profileObj = getLatestProfilerEntry(testDB, {op: "query", ns: "wt_op_stat.foo"});
    checkSystemProfileStats(profileObj, "bytesRead");

    // Stopping the mongod waits until all of its logs have been read by the mongo shell.
    MongoRunner.stopMongod(conn);
    checkLogStats();

    jsTestLog("Success!");
}
})();

/**
 * Tests that WT_PREPARE_CONFLICT errors are retried correctly on read operations, but does not test
 * any actual prepare transaction functionality.
 * @tag: [requires_wiredtiger]
 */
(function() {
"strict";

let conn = MongoRunner.runMongod();
let testDB = conn.getDB("test");

let t = testDB.prepare_conflict;
t.drop();

// Test different types of operations: removals, updates, and index operations.
assert.commandWorked(t.createIndex({x: 1}));
assert.commandWorked(
    t.createIndex({y: 1}, {partialFilterExpression: {_id: {$gte: 500}}, unique: true}));
let rand = {"#RAND_INT": [0, 1000]};
let ops = [
    {op: "remove", ns: t.getFullName(), query: {_id: rand}, writeCmd: true},
    {op: "findOne", ns: t.getFullName(), query: {_id: rand}, readCmd: true},
    {
        op: "update",
        ns: t.getFullName(),
        query: {_id: rand},
        update: {$inc: {x: 1}},
        upsert: true,
        writeCmd: true
    },
    {op: "findOne", ns: t.getFullName(), query: {x: rand}, readCmd: true},
    {
        op: "update",
        ns: t.getFullName(),
        query: {_id: rand},
        update: {$inc: {y: 1}},
        upsert: true,
        writeCmd: true
    },
    {op: "findOne", ns: t.getFullName(), query: {y: rand}, readCmd: true},
    {op: "findOne", ns: t.getFullName(), query: {_id: rand}, readCmd: true},
];

let seconds = 5;
let parallel = 5;
let host = testDB.getMongo().host;

let benchArgs = {ops, seconds, parallel, host};

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'WTAlwaysNotifyPrepareConflictWaiters', mode: 'alwaysOn'}));

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'WTPrepareConflictForReads', mode: {activationProbability: 0.05}}));

res = benchRun(benchArgs);

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'WTPrepareConflictForReads', mode: "off"}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'WTAlwaysNotifyPrepareConflictWaiters', mode: 'off'}));
res = t.validate();
assert(res.valid, tojson(res));
MongoRunner.stopMongod(conn);
})();

/**
 * Tests the WT prepare conflict behavior of running operations outside of a multi-statement
 * transaction when another operation is being performed concurrently inside of the multi-statement
 * transaction with the "WTSkipPrepareConflictRetries" failpoint is enabled.
 *
 * @tags: [
 *   uses_prepare_transaction,
 *   uses_transactions,
 * ]
 */
(function() {
"use strict";

load("jstests/core/txns/libs/prepare_helpers.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB("test");
const testColl = testDB.getCollection("wt_skip_prepare_conflict_retries_failpoint");

const session = primary.startSession({causalConsistency: false});
const sessionDB = session.getDatabase(testDB.getName());
const sessionColl = sessionDB.getCollection(testColl.getName());

assert.commandWorked(testDB.runCommand({profile: 2}));

assert.commandWorked(testColl.insert({_id: 1, note: "from before transaction"}, {w: "majority"}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "WTSkipPrepareConflictRetries", mode: "alwaysOn"}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "skipWriteConflictRetries", mode: "alwaysOn"}));

// A non-transactional operation conflicting with a write operation performed inside a
// multistatement transaction can encounter a WT_PREPARE_CONFLICT in the wiredtiger
// layer under several circumstances, such as performing an insert, update, or find
// on a document that is in a prepare statement. The non-transactional operation
// would then be retried after the prepared transaction commits or aborts. However, with the
// "WTSkipPrepareConflictRetries"failpoint enabled, the non-transactional operation would
// instead return with a WT_ROLLBACK error. This would then get bubbled up as a
// WriteConflictException. Enabling the "skipWriteConflictRetries" failpoint then prevents
// the higher layers from retrying the entire operation.
session.startTransaction();

assert.commandWorked(sessionColl.update({_id: 1}, {$set: {note: "from prepared transaction"}}));

const prepareTimestamp = PrepareHelpers.prepareTransaction(session);

assert.commandFailedWithCode(
    testColl.update({_id: 1}, {$set: {note: "outside prepared transaction"}}),
    ErrorCodes.WriteConflict);

assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestamp));

const profileEntry =
    testDB.system.profile.findOne({"command.u.$set.note": "outside prepared transaction"});
assert.gte(profileEntry.prepareReadConflicts, 1);

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "WTSkipPrepareConflictRetries", mode: "off"}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "skipWriteConflictRetries", mode: "off"}));

session.endSession();
rst.stopSet();
})();

/**
 * This test is only for the WiredTiger storage engine. Test to reproduce recovery bugs in WT from
 * WT-2696 and WT-2706.  Have several threads inserting unique data.  Kill -9 mongod.  After
 * restart and recovery verify that all expected records inserted are there and no records in the
 * middle of the data set are lost.
 *
 * @tags: [requires_wiredtiger, requires_journaling]
 */

load('jstests/libs/parallelTester.js');  // For Thread

(function() {
'use strict';

// Skip this test if not running with the "wiredTiger" storage engine.
if (jsTest.options().storageEngine && jsTest.options().storageEngine !== 'wiredTiger') {
    jsTest.log('Skipping test because storageEngine is not "wiredTiger"');
    return;
}

var dbpath = MongoRunner.dataPath + 'wt_unclean_shutdown';
resetDbpath(dbpath);

var conn = MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    // Modify some WT settings:
    // - Disable checkpoints based on log size so that we know no checkpoint gets written.
    // - Explicitly set checkpoints to 60 seconds in case the default ever changes.
    // - Turn off archiving and compression for easier debugging if there is a failure.
    // - Make the maximum file size small to encourage lots of file changes.  WT-2706 was
    // related to log file switches.
    wiredTigerEngineConfigString:
        'checkpoint=(wait=60,log_size=0),log=(archive=false,compressor=none,file_max=10M)'
});
assert.neq(null, conn, 'mongod was unable to start up');

var insertWorkload = function(host, start, end) {
    var conn = new Mongo(host);
    var testDB = conn.getDB('test');

    // Create a record larger than 128K which is the threshold to doing an unbuffered log
    // write in WiredTiger.
    var largeString = 'a'.repeat(1024 * 128);

    for (var i = start; i < end; i++) {
        var doc = {_id: i, x: 0};
        // One of the bugs, WT-2696, was related to large records that used the unbuffered
        // log code.  Periodically insert the large record to stress that code path.
        if (i % 30 === 0) {
            doc.x = largeString;
        }

        try {
            testDB.coll.insert(doc);
        } catch (e) {
            // Terminate the loop when mongod is killed.
            break;
        }
    }
    // Return i, the last record we were trying to insert.  It is possible that mongod gets
    // killed in the middle but not finding a record at the end is okay.  We're only
    // interested in records missing in the middle.
    return {start: start, end: i};
};

// Start the insert workload threads with partitioned input spaces.
// We don't run long enough for threads to overlap.  Adjust the per thread value if needed.
var max_per_thread = 1000000;
var num_threads = 8;
var threads = [];
for (var i = 0; i < num_threads; i++) {
    var t = new Thread(
        insertWorkload, conn.host, i * max_per_thread, max_per_thread + (i * max_per_thread));
    threads.push(t);
    t.start();
}

// Sleep for sometime less than a minute so that mongod has not yet written a checkpoint.
// That will force WT to run recovery all the way from the beginning and we can detect missing
// records.  Sleep for 40 seconds to generate plenty of workload.
sleep(40000);

// Mongod needs an unclean shutdown so that WT recovery is forced on restart and we can detect
// any missing records.
MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});

// Retrieve the start and end data from each thread.
var retData = [];
threads.forEach(function(t) {
    t.join();
    retData.push(t.returnData());
});

// Restart the mongod.  This forces WT to run recovery.
conn = MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    wiredTigerEngineConfigString: 'log=(archive=false,compressor=none,file_max=10M)'
});
assert.neq(null, conn, 'mongod should have restarted');

// Verify that every item between start and end for every thread exists in the collection now
// that recovery has completed.
var coll = conn.getDB('test').coll;
for (var i = 0; i < retData.length; i++) {
    // For each start and end, verify every data item exists.
    var thread_data = retData[i];
    var absent = null;
    var missing = null;
    for (var j = thread_data.start; j <= thread_data.end; j++) {
        var idExists = coll.find({_id: j}).count() > 0;
        // The verification is a bit complex.  We only want to fail if records in the middle
        // of the range are missing.  Records at the end may be missing due to when mongod
        // was killed and records in memory are lost.  It is only a bug if a record is missing
        // and a subsequent record exists.
        if (!idExists) {
            absent = j;
        } else if (absent !== null) {
            missing = absent;
            break;
        }
    }
    assert.eq(null,
              missing,
              'Thread ' + i + ' missing id ' + missing +
                  ' start and end for all threads: ' + tojson(retData));
}

MongoRunner.stopMongod(conn);
})();

// Ensure that multi-update and multi-remove operations yield regularly.
// @tags: [requires_profiling]
(function() {
'use strict';

function countOpYields(coll, op) {
    const profileEntry = coll.getDB()
                             .system.profile.find({ns: coll.getFullName()})
                             .sort({$natural: -1})
                             .limit(1)
                             .next();
    assert.eq(profileEntry.op, op);
    return profileEntry.numYield;
}

const nDocsToInsert = 300;
const worksPerYield = 50;

// Start a mongod that will yield every 50 work cycles.
const mongod = MongoRunner.runMongod({
    setParameter: `internalQueryExecYieldIterations=${worksPerYield}`,
    profile: 2,
});
assert.neq(null, mongod, 'mongod was unable to start up');

const coll = mongod.getDB('test').yield_during_writes;
coll.drop();

for (let i = 0; i < nDocsToInsert; i++) {
    assert.commandWorked(coll.insert({_id: i}));
}

// A multi-update doing a collection scan should yield about nDocsToInsert / worksPerYield
// times.
assert.commandWorked(coll.update({}, {$inc: {counter: 1}}, {multi: true}));
assert.gt(countOpYields(coll, 'update'), (nDocsToInsert / worksPerYield) - 2);

// Likewise, a multi-remove should also yield approximately every worksPerYield documents.
assert.commandWorked(coll.remove({}, {multi: true}));
assert.gt(countOpYields(coll, 'remove'), (nDocsToInsert / worksPerYield) - 2);

MongoRunner.stopMongod(mongod);
})();

/**
 * Tests that resizing the oplog works as expected and validates input arguments.
 *
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

let replSet = new ReplSetTest({nodes: 2, oplogSize: 50});
replSet.startSet();
replSet.initiate();

let primary = replSet.getPrimary();

const MB = 1024 * 1024;
const GB = 1024 * MB;
const PB = 1024 * GB;
const EB = 1024 * PB;

assert.eq(primary.getDB('local').oplog.rs.stats().maxSize, 50 * MB);

// Too small: 990MB
assert.commandFailedWithCode(primary.getDB('admin').runCommand({replSetResizeOplog: 1, size: 900}),
                             51024,  // IDL Error code for invalid fields
                             "Expected replSetResizeOplog to fail because the size was too small");

// Way too small: -1GB
assert.commandFailedWithCode(
    primary.getDB('admin').runCommand({replSetResizeOplog: 1, size: -1 * GB / MB}),
    51024,  // IDL Error code for invalid fields
    "Expected replSetResizeOplog to fail because the size was too small");

// Too big: 8EB
assert.commandFailedWithCode(
    primary.getDB('admin').runCommand({replSetResizeOplog: 1, size: 8 * EB / MB}),
    51024,  // IDL Error code for invalid fields
    "Expected replSetResizeOplog to fail because the size was too big");

// Min Retention Hours not valid: -1hr
assert.commandFailedWithCode(
    primary.getDB('admin').runCommand({replSetResizeOplog: 1, size: 990, minRetentionHours: -1}),
    51024,  // IDL Error code for invalid fields
    "Expected replSetResizeOplog to fail because the minimum retention hours was too low");

// The maximum: 1PB
assert.commandWorked(primary.getDB('admin').runCommand({replSetResizeOplog: 1, size: 1 * PB / MB}));

// Valid size and minRetentionHours
assert.commandWorked(primary.getDB('admin').runCommand(
    {replSetResizeOplog: 1, size: 1 * PB / MB, minRetentionHours: 5}));

// Valid minRetentionHours with no size parameter.
assert.commandWorked(
    primary.getDB('admin').runCommand({replSetResizeOplog: 1, minRetentionHours: 1}));

assert.eq(primary.getDB('local').oplog.rs.stats().maxSize, 1 * PB);

replSet.stopSet();
})();

// This test ensures that the replWriterThreadCount server parameter:
//       1) cannot be less than 1
//       2) cannot be greater than 256
//       3) is actually set to the passed in value
//       4) cannot be altered at run time

(function() {
"use strict";

// too low a count
clearRawMongoProgramOutput();
assert.throws(() => MongoRunner.runMongod({setParameter: 'replWriterThreadCount=0'}));
assert(
    rawMongoProgramOutput().match(
        "Invalid value for parameter replWriterThreadCount: 0 is not greater than or equal to 1"),
    "mongod started with too low a value for replWriterThreadCount");

// too high a count
clearRawMongoProgramOutput();
assert.throws(() => MongoRunner.runMongod({setParameter: 'replWriterThreadCount=257'}));
assert(
    rawMongoProgramOutput().match(
        "Invalid value for parameter replWriterThreadCount: 257 is not less than or equal to 256"),
    "mongod started with too high a value for replWriterThreadCount");

// proper count
clearRawMongoProgramOutput();
let mongo = MongoRunner.runMongod({setParameter: 'replWriterThreadCount=24'});
assert.neq(null, mongo, "mongod failed to start with a suitable replWriterThreadCount value");
assert(!rawMongoProgramOutput().match("Invalid value for parameter replWriterThreadCount"),
       "despite accepting the replWriterThreadCount value, mongod logged an error");

// getParameter to confirm the value was set
var result = mongo.getDB("admin").runCommand({getParameter: 1, replWriterThreadCount: 1});
assert.eq(24, result.replWriterThreadCount, "replWriterThreadCount was not set internally");

// setParameter to ensure it is not possible
assert.commandFailed(mongo.getDB("admin").runCommand({setParameter: 1, replWriterThreadCount: 1}));
MongoRunner.stopMongod(mongo);
}());

/**
 * Tests that DBClientRS performs re-targeting when it sees an ErrorCodes.NotWritablePrimary error
 * response from a command even if "not master" doesn't appear in the message.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

// Set the refresh period to 10 min to rule out races
_setShellFailPoint({
    configureFailPoint: "modifyReplicaSetMonitorDefaultRefreshPeriod",
    mode: "alwaysOn",
    data: {
        period: 10 * 60,
    },
});

const rst = new ReplSetTest({
    nodes: 3,
    nodeOptions: {
        setParameter:
            {"failpoint.respondWithNotPrimaryInCommandDispatch": tojson({mode: "alwaysOn"})}
    }
});
rst.startSet();
rst.initiate();

const directConn = rst.getPrimary();
const rsConn = new Mongo(rst.getURL());
assert(rsConn.isReplicaSetConnection(),
       "expected " + rsConn.host + " to be a replica set connection string");

function stepDownPrimary(rst) {
    const awaitShell = startParallelShell(
        () => assert.commandWorked(db.adminCommand({replSetStepDown: 60, force: true})),
        directConn.port);

    // We wait for the primary to transition to the SECONDARY state to ensure we're waiting
    // until after the parallel shell has started the replSetStepDown command.
    const reconnectNode = false;
    rst.waitForState(directConn, ReplSetTest.State.SECONDARY, null, reconnectNode);

    return awaitShell;
}

const awaitShell = stepDownPrimary(rst);

// Wait for a new primary to be elected and agreed upon by nodes.
rst.getPrimary();
rst.awaitNodesAgreeOnPrimary();

// DBClientRS should discover the current primary eventually and get NotWritablePrimary errors in
// the meantime.
assert.soon(() => {
    const res = rsConn.getDB("test").runCommand({create: "mycoll"});
    if (!res.ok) {
        assert(res.code == ErrorCodes.NotWritablePrimary);
    }
    return res.ok;
});

awaitShell();

rst.stopSet();
})();

/**
 * Tests the behavior of how getMore operations are routed by the mongo shell when using a replica
 * set connection and cursors are established on a secondary.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";
var rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "getmore";

// We create our own replica set connection because 'rst.nodes' is an array of direct
// connections to each individual node.
var conn = new Mongo(rst.getURL());

var coll = conn.getDB(dbName)[collName];
coll.drop();

// Insert several document so that we can use a cursor to fetch them in multiple batches.
var res = coll.insert([{}, {}, {}, {}, {}]);
assert.commandWorked(res);
assert.eq(5, res.nInserted);

// Wait for the secondary to catch up because we're going to try and do reads from it.
rst.awaitReplication();

// Establish a cursor on the secondary and verify that the getMore operations are routed to it.
var cursor = coll.find().readPref("secondary").batchSize(2);
assert.eq(5, cursor.itcount(), "failed to read the documents from the secondary");

rst.stopSet();
})();

/**
 * Tests that DBClientRS doesn't do any re-targeting on replica set member state changes until it
 * sees a "not master" error response from a command.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const directConn = rst.getPrimary();
const rsConn = new Mongo(rst.getURL());
assert(rsConn.isReplicaSetConnection(),
       "expected " + rsConn.host + " to be a replica set connection string");

function stepDownPrimary(rst) {
    const awaitShell = startParallelShell(
        () => assert.commandWorked(db.adminCommand({replSetStepDown: 60, force: true})),
        directConn.port);

    // We wait for the primary to transition to the SECONDARY state to ensure we're waiting
    // until after the parallel shell has started the replSetStepDown command and the server is
    // paused at the failpoint. Do not attempt to reconnect to the node, since the node will be
    // holding the global X lock at the failpoint.
    const reconnectNode = false;
    rst.waitForState(directConn, ReplSetTest.State.SECONDARY, null, reconnectNode);

    return awaitShell;
}

const failpoint = "stepdownHangBeforePerformingPostMemberStateUpdateActions";
assert.commandWorked(directConn.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));

const awaitShell = stepDownPrimary(rst);

const error = assert.throws(function() {
    // DBClientRS will continue to send command requests to the node it believed to be primary
    // even after it stepped down so long as it hasn't closed its connection. But this may also
    // throw if the ReplicaSetMonitor's backgroud refresh has already noticed that this node is
    // no longer primary.
    assert.commandFailedWithCode(rsConn.getDB("test").runCommand({find: "mycoll"}),
                                 ErrorCodes.NotPrimaryNoSecondaryOk);

    // However, once the server responds back with a "not master" error, DBClientRS will cause
    // the ReplicaSetMonitor to attempt to discover the current primary, which will cause this
    // to definitely throw.
    rsConn.getDB("test").runCommand({find: "mycoll"});
});
assert(/Could not find host/.test(error.toString()),
       "find command failed for a reason other than being unable to discover a new primary: " +
           tojson(error));

try {
    assert.commandWorked(directConn.adminCommand({configureFailPoint: failpoint, mode: "off"}));
} catch (e) {
    if (!isNetworkError(e)) {
        throw e;
    }

    // We ignore network errors because it's possible that depending on how quickly the server
    // closes connections that the connection would get closed before the server has a chance to
    // respond to the configureFailPoint command with ok=1.
}

awaitShell();
rst.stopSet();
})();

/**
 * Tests mongoD-specific semantics of postBatchResumeToken for $changeStream aggregations.
 * @tags: [
 *   requires_majority_read_concern,
 *   uses_transactions,
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
const collName = "report_post_batch_resume_token";
const testCollection = assertDropAndRecreateCollection(db, collName);
const otherCollection = assertDropAndRecreateCollection(db, "unrelated_" + collName);
const oplogColl = db.getSiblingDB("local").oplog.rs;

let docId = 0;  // Tracks _id of documents inserted to ensure that we do not duplicate.
const batchSize = 2;

// Helper function to perform generic comparisons and dump the oplog on failure.
function assertCompare(cmpFn, left, right, cmpOp, cmpVal) {
    assert[cmpOp](cmpFn(left, right),
                  cmpVal,
                  {left: left, right: right, oplogEntries: oplogColl.find().toArray()});
}

// Start watching the test collection in order to capture a resume token.
let csCursor = testCollection.watch();

// Write some documents to the test collection and get the resume token from the first doc.
for (let i = 0; i < 5; ++i) {
    assert.commandWorked(testCollection.insert({_id: docId++}));
}
const resumeTokenFromDoc = csCursor.next()._id;
csCursor.close();

// Test that postBatchResumeToken is present on a non-empty initial aggregate batch.
assert.soon(() => {
    csCursor = testCollection.watch([], {resumeAfter: resumeTokenFromDoc});
    csCursor.close();  // We don't need any results after the initial batch.
    return csCursor.objsLeftInBatch();
});
while (csCursor.objsLeftInBatch()) {
    csCursor.next();
}
let initialAggPBRT = csCursor.getResumeToken();
assert.neq(undefined, initialAggPBRT);

// Test that the PBRT is correctly updated when reading events from within a transaction.
const session = db.getMongo().startSession();
const sessionDB = session.getDatabase(db.getName());

const sessionColl = sessionDB[testCollection.getName()];
const sessionOtherColl = sessionDB[otherCollection.getName()];
session.startTransaction();

// Open a stream of batchSize:2 and grab the PBRT of the initial batch.
csCursor = testCollection.watch([], {cursor: {batchSize: batchSize}});
initialAggPBRT = csCursor.getResumeToken();
assert.eq(csCursor.objsLeftInBatch(), 0);

// Write 3 documents to testCollection and 1 to the unrelated collection within the transaction.
for (let i = 0; i < 3; ++i) {
    assert.commandWorked(sessionColl.insert({_id: docId++}));
}
assert.commandWorked(sessionOtherColl.insert({}));
assert.commandWorked(session.commitTransaction_forTesting());
session.endSession();

// Grab the next 2 events, which should be the first 2 events in the transaction.
assert(csCursor.hasNext());  // Causes a getMore to be dispatched.
assert.eq(csCursor.objsLeftInBatch(), 2);

// The clusterTime should be the same on each, but the resume token keeps advancing.
const txnEvent1 = csCursor.next(), txnEvent2 = csCursor.next();
const txnClusterTime = txnEvent1.clusterTime;
assertCompare(timestampCmp, txnEvent2.clusterTime, txnClusterTime, "eq", 0);
assertCompare(bsonWoCompare, txnEvent1._id, initialAggPBRT, "gt", 0);
assertCompare(bsonWoCompare, txnEvent2._id, txnEvent1._id, "gt", 0);

// The PBRT of the first transaction batch is equal to the last document's resumeToken.
let getMorePBRT = csCursor.getResumeToken();
assertCompare(bsonWoCompare, getMorePBRT, txnEvent2._id, "eq", 0);

// Save this PBRT so that we can test resuming from it later on.
const resumePBRT = getMorePBRT;

// Now get the next batch. This contains the third of the four transaction operations.
let previousGetMorePBRT = getMorePBRT;
assert(csCursor.hasNext());  // Causes a getMore to be dispatched.
assert.eq(csCursor.objsLeftInBatch(), 1);

// The clusterTime of this event is the same as the two events from the previous batch, but its
// resume token is greater than the previous PBRT.
const txnEvent3 = csCursor.next();
assertCompare(timestampCmp, txnEvent3.clusterTime, txnClusterTime, "eq", 0);
assertCompare(bsonWoCompare, txnEvent3._id, previousGetMorePBRT, "gt", 0);

// Because we wrote to the unrelated collection, the final event in the transaction does not
// appear in the batch. But in this case it also does not allow our PBRT to advance beyond the
// last event in the batch, because the unrelated event is within the same transaction and
// therefore has the same clusterTime.
getMorePBRT = csCursor.getResumeToken();
assertCompare(bsonWoCompare, getMorePBRT, txnEvent3._id, "eq", 0);

// Confirm that resuming from the PBRT of the first batch gives us the third transaction write.
csCursor = testCollection.watch([], {resumeAfter: resumePBRT});
assert.docEq(csCursor.next(), txnEvent3);
assert(!csCursor.hasNext());

rst.stopSet();
})();

/**
 * Tests the "requireApiVersion" mongod/mongos parameter.
 *
 * This test is incompatible with parallel and passthrough suites; concurrent jobs fail while
 * requireApiVersion is true.
 *
 * @tags: [
 *   requires_journaling,
 *   requires_replication,
 *   requires_transactions,
 * ]
 */

(function() {
"use strict";

function runTest(db, supportsTransctions, writeConcern = {}, secondaries = []) {
    assert.commandWorked(db.runCommand({setParameter: 1, requireApiVersion: true}));
    for (const secondary of secondaries) {
        assert.commandWorked(secondary.adminCommand({setParameter: 1, requireApiVersion: true}));
    }

    assert.commandFailedWithCode(db.runCommand({ping: 1}), 498870, "command without apiVersion");
    assert.commandWorked(db.runCommand({ping: 1, apiVersion: "1"}));
    assert.commandFailed(db.runCommand({ping: 1, apiVersion: "not a real API version"}));

    // Create a collection and do some writes with writeConcern majority.
    const collName = "testColl";
    assert.commandWorked(db.runCommand({create: collName, apiVersion: "1", writeConcern}));
    assert.commandWorked(db.runCommand(
        {insert: collName, documents: [{a: 1, b: 2}], apiVersion: "1", writeConcern}));

    // User management commands loop back into the system so make sure they set apiVersion
    // internally
    assert.commandWorked(db.adminCommand(
        {createRole: 'testRole', apiVersion: "1", writeConcern, privileges: [], roles: []}));
    assert.commandWorked(db.adminCommand({dropRole: 'testRole', apiVersion: "1", writeConcern}));

    /*
     * "getMore" accepts apiVersion.
     */
    assert.commandWorked(db.runCommand(
        {insert: "collection", documents: [{}, {}, {}], apiVersion: "1", writeConcern}));
    let reply = db.runCommand({find: "collection", batchSize: 1, apiVersion: "1"});
    assert.commandWorked(reply);
    assert.commandFailedWithCode(
        db.runCommand({getMore: reply.cursor.id, collection: "collection"}), 498870);
    assert.commandWorked(
        db.runCommand({getMore: reply.cursor.id, collection: "collection", apiVersion: "1"}));

    if (supportsTransctions) {
        /*
         * Commands in transactions require API version.
         */
        const session = db.getMongo().startSession({causalConsistency: false});
        const sessionDb = session.getDatabase(db.getName());
        assert.commandFailedWithCode(sessionDb.runCommand({
            find: "collection",
            batchSize: 1,
            txnNumber: NumberLong(0),
            stmtId: NumberInt(2),
            startTransaction: true,
            autocommit: false
        }),
                                     498870);
        reply = sessionDb.runCommand({
            find: "collection",
            batchSize: 1,
            apiVersion: "1",
            txnNumber: NumberLong(1),
            stmtId: NumberInt(0),
            startTransaction: true,
            autocommit: false
        });
        assert.commandWorked(reply);
        assert.commandFailedWithCode(sessionDb.runCommand({
            getMore: reply.cursor.id,
            collection: "collection",
            txnNumber: NumberLong(1),
            stmtId: NumberInt(1),
            autocommit: false
        }),
                                     498870);
        assert.commandWorked(sessionDb.runCommand({
            getMore: reply.cursor.id,
            collection: "collection",
            txnNumber: NumberLong(1),
            stmtId: NumberInt(1),
            autocommit: false,
            apiVersion: "1"
        }));

        assert.commandFailedWithCode(
            sessionDb.runCommand(
                {commitTransaction: 1, txnNumber: NumberLong(1), autocommit: false}),
            498870);

        assert.commandWorked(sessionDb.runCommand(
            {commitTransaction: 1, apiVersion: "1", txnNumber: NumberLong(1), autocommit: false}));

        // Start a new txn so we can test abortTransaction.
        reply = sessionDb.runCommand({
            find: "collection",
            apiVersion: "1",
            txnNumber: NumberLong(2),
            stmtId: NumberInt(0),
            startTransaction: true,
            autocommit: false
        });
        assert.commandWorked(reply);
        assert.commandFailedWithCode(
            sessionDb.runCommand(
                {abortTransaction: 1, txnNumber: NumberLong(2), autocommit: false}),
            498870);
        assert.commandWorked(sessionDb.runCommand(
            {abortTransaction: 1, apiVersion: "1", txnNumber: NumberLong(2), autocommit: false}));
    }

    assert.commandWorked(
        db.runCommand({setParameter: 1, requireApiVersion: false, apiVersion: "1"}));
    for (const secondary of secondaries) {
        assert.commandWorked(
            secondary.adminCommand({setParameter: 1, requireApiVersion: false, apiVersion: "1"}));
    }
    assert.commandWorked(db.runCommand({ping: 1}));
}

function requireApiVersionOnShardOrConfigServerTest() {
    assert.throws(
        () => MongoRunner.runMongod(
            {shardsvr: "", replSet: "dummy", setParameter: {"requireApiVersion": true}}),
        [],
        "mongod should not be able to start up with --shardsvr and requireApiVersion=true");

    assert.throws(
        () => MongoRunner.runMongod(
            {configsvr: "", replSet: "dummy", setParameter: {"requireApiVersion": 1}}),
        [],
        "mongod should not be able to start up with --configsvr and requireApiVersion=true");

    const rs = new ReplSetTest({nodes: 1});
    rs.startSet({shardsvr: ""});
    rs.initiate();
    const singleNodeShard = rs.getPrimary();
    assert.neq(null, singleNodeShard, "mongod was not able to start up");
    assert.commandFailed(
        singleNodeShard.adminCommand({setParameter: 1, requireApiVersion: true}),
        "should not be able to set requireApiVersion=true on mongod that was started with --shardsvr");
    rs.stopSet();

    const configsvrRS = new ReplSetTest({nodes: 1});
    configsvrRS.startSet({configsvr: ""});
    configsvrRS.initiate();
    const configsvrConn = configsvrRS.getPrimary();
    assert.neq(null, configsvrConn, "mongod was not able to start up");
    assert.commandFailed(
        configsvrConn.adminCommand({setParameter: 1, requireApiVersion: 1}),
        "should not be able to set requireApiVersion=true on mongod that was started with --configsvr");
    configsvrRS.stopSet();
}

requireApiVersionOnShardOrConfigServerTest();

const mongod = MongoRunner.runMongod();
runTest(mongod.getDB("admin"), false /* supportsTransactions */);
MongoRunner.stopMongod(mongod);

const rst = new ReplSetTest({nodes: 3});
rst.startSet();
rst.initiateWithHighElectionTimeout();

runTest(rst.getPrimary().getDB("admin"),
        true /* supportsTransactions */,
        {w: "majority"} /* writeConcern */,
        rst.getSecondaries());
rst.stopSet();

const st = new ShardingTest({});
runTest(st.s0.getDB("admin"), true /* supportsTransactions */);
st.stop();
}());

/**
 * Tests restarting the server and then shutting down uncleanly, both times recovering from a
 * timestamp before the commit timestamp of an index build.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/libs/fail_point_util.js');
load('jstests/noPassthrough/libs/index_build.js');

const replTest = new ReplSetTest({nodes: 1});
replTest.startSet();
replTest.initiate();

const primary = function() {
    return replTest.getPrimary();
};
const testDB = function() {
    return primary().getDB('test');
};
const coll = function() {
    return testDB()[jsTestName()];
};

assert.commandWorked(coll().insert({a: 0}));

const fp = configureFailPoint(primary(), 'hangIndexBuildBeforeCommit');
const awaitCreateIndex = IndexBuildTest.startIndexBuild(primary(), coll().getFullName(), {a: 1});
fp.wait();

// Get a timestamp before the commit timestamp of the index build.
const ts =
    assert.commandWorked(testDB().runCommand({insert: coll().getName(), documents: [{a: 1}]}))
        .operationTime;

configureFailPoint(primary(), 'holdStableTimestampAtSpecificTimestamp', {timestamp: ts});
fp.off();
awaitCreateIndex();

const ident = assert.commandWorked(testDB().runCommand({collStats: coll().getName()}))
                  .indexDetails.a_1.uri.substring('statistics:table:'.length);

replTest.restart(primary(), {
    setParameter: {
        // Set minSnapshotHistoryWindowInSeconds to 0 so that the the oldest timestamp can move
        // forward, despite the stable timestamp being held steady.
        minSnapshotHistoryWindowInSeconds: 0,
        'failpoint.holdStableTimestampAtSpecificTimestamp':
            tojson({mode: 'alwaysOn', data: {timestamp: ts}})
    }
});

const checkLogs = function() {
    // On startup, the node will recover from before the index commit timestamp.
    checkLog.containsJson(primary(), 23987, {
        recoveryTimestamp: (recoveryTs) => {
            return timestampCmp(
                       Timestamp(recoveryTs['$timestamp']['t'], recoveryTs['$timestamp']['i']),
                       ts) <= 0;
        }
    });

    // Since the index build was not yet completed at the recovery timestamp, its ident will be
    // dropped.
    checkLog.containsJson(primary(), 22206, {
        index: 'a_1',
        namespace: coll().getFullName(),
        ident: ident,
        commitTimestamp: {$timestamp: {t: 0, i: 0}},
    });

    // The oldest timestamp moving forward will cause the ident reaper to drop the ident.
    checkLog.containsJson(primary(), 22237, {
        ident: ident,
        dropTimestamp: {$timestamp: {t: 0, i: 0}},
    });
};

checkLogs();

// Shut down uncleanly so that a checkpoint is not taken. This will cause the index catalog entry
// referencing the now-dropped ident to still be present.
replTest.stop(0, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL}, {forRestart: true});
replTest.start(0, undefined, true /* restart */);

checkLogs();

IndexBuildTest.assertIndexes(coll(), 2, ['_id_', 'a_1']);

replTest.stopSet();
})();

/**
 * Tests that if an index build fails to resume during setup (before the index builds thread is
 * created), the index build will successfully restart from the beginning.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = jsTestName();

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let coll = primary.getDB(dbName).getCollection(collName);

assert.commandWorked(coll.insert({a: 1}));

ResumableIndexBuildTest.runFailToResume(rst,
                                        dbName,
                                        collName,
                                        {a: 1},
                                        {failPointAfterStartup: "failToParseResumeIndexInfo"},
                                        [{a: 2}, {a: 3}],
                                        [{a: 4}, {a: 5}],
                                        true /* failWhileParsing */);

ResumableIndexBuildTest.runFailToResume(rst,
                                        dbName,
                                        collName,
                                        {a: 1},
                                        {failPointAfterStartup: "failSetUpResumeIndexBuild"},
                                        [{a: 6}, {a: 7}],
                                        [{a: 8}, {a: 9}]);

ResumableIndexBuildTest.runFailToResume(rst,
                                        dbName,
                                        collName,
                                        {a: 1},
                                        {removeTempFilesBeforeStartup: true},
                                        [{a: 10}, {a: 11}],
                                        [{a: 12}, {a: 13}]);

rst.stopSet();
})();
/**
 * Tests that an index build is resumable only once across restarts. If the resumed index build
 * fails to run to completion before shutdown, it will restart from the beginning on the next server
 * startup.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = jsTestName();

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();

ResumableIndexBuildTest.runResumeInterruptedByShutdown(
    rst,
    dbName,
    collName + "_collscan_drain",
    {a: 1},                    // index key pattern
    "resumable_index_build1",  // index name
    {name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386},
    "collection scan",
    {a: 1},  // initial doc
    [{a: 2}, {a: 3}],
    [{a: 4}, {a: 5}]);

ResumableIndexBuildTest.runResumeInterruptedByShutdown(
    rst,
    dbName,
    collName + "_bulkload_drain_multikey",
    {a: 1},                    // index key pattern
    "resumable_index_build2",  // index name
    {name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400},
    "bulk load",
    {a: [11, 22, 33]},  // initial doc
    [{a: 77}, {a: 88}],
    [{a: 99}, {a: 100}]);

rst.stopSet();
})();

/**
 * Tests that a node can be successfully restarted when the bridge is enabled. Also verifies the
 * bridge configuration is left intact even after the node is restarted.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");  // for reconnect

const rst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0, votes: 0}}],
    useBridge: true,
});

rst.startSet();
rst.initiate();
rst.awaitNodesAgreeOnPrimary();

const primary = rst.getPrimary();
const secondary = rst.getSecondary();

const primaryDB = primary.getDB("test");
const primaryColl = primaryDB.getCollection("restart_node_with_bridge");

function assertWriteReplicates() {
    assert.commandWorked(
        primaryColl.update({_id: 0}, {$inc: {counter: 1}}, {upsert: true, writeConcern: {w: 2}}));
}

function assertWriteFailsToReplicate() {
    assert.commandFailedWithCode(
        primaryColl.update({_id: 0}, {$inc: {counter: 1}}, {writeConcern: {w: 2, wtimeout: 1000}}),
        ErrorCodes.WriteConcernFailed);
}

// By default, the primary should be connected to the secondary. Replicating a write should
// therefore succeed.
assertWriteReplicates();

// We disconnect the primary from the secondary and verify that replicating a write fails.
primary.disconnect(secondary);
assertWriteFailsToReplicate();

// We restart the secondary and verify that replicating a write still fails.
rst.restart(secondary);
assertWriteFailsToReplicate();

// We restart the primary and verify that replicating a write still fails.
rst.restart(primary);
rst.getPrimary();
// Note that we specify 'primaryDB' to avoid having reconnect() send a message directly to the
// mongod process rather than going through the mongobridge process as well.
reconnect(primaryDB);
assertWriteFailsToReplicate();

// We reconnect the primary to the secondary and verify that replicating a write succeeds.
primary.reconnect(secondary);
assertWriteReplicates();

rst.stopSet();
}());

/**
 * Tests that resumable index builds in the bulk load phase write their state to disk upon clean
 * shutdown and are resumed from the same phase to completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const runTests = function(docs, indexSpecsFlat, collNameSuffix) {
    const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName() + collNameSuffix);
    assert.commandWorked(coll.insert(docs));

    const runTest = function(indexSpecs, iteration) {
        ResumableIndexBuildTest.run(
            rst,
            dbName,
            coll.getName(),
            indexSpecs,
            [{name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400}],
            iteration,
            ["bulk load"],
            [{skippedPhaseLogID: 20391}]);
    };

    runTest([[indexSpecsFlat[0]]], 0);
    runTest([[indexSpecsFlat[0]]], 1);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 0);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 1);
    runTest([indexSpecsFlat], 0);
    runTest([indexSpecsFlat], 1);
};

runTests([{a: 1, b: 1}, {a: 2, b: 2}], [{a: 1}, {b: 1}], "");
runTests([{a: [1, 2], b: [1, 2]}, {a: 2, b: 2}], [{a: 1}, {b: 1}], "_multikey_first");
runTests([{a: 1, b: 1}, {a: [1, 2], b: [1, 2]}], [{a: 1}, {b: 1}], "_multikey_last");
runTests([{a: [1, 2], b: 1}, {a: 2, b: [1, 2]}], [{a: 1}, {b: 1}], "_multikey_mixed");
runTests([{a: [1, 2], b: {c: [3, 4]}, d: ""}, {e: "", f: [[]], g: null, h: 8}],
         [{"$**": 1}, {h: 1}],
         "_wildcard");

rst.stopSet();
})();
/**
 * Tests that a resumable index build large enough to spill to disk during the collection scan
 * phase writes its state to disk upon clean shutdown during the bulk load phase and is resumed
 * from the same phase to completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest(
    {nodes: 1, nodeOptions: {setParameter: {maxIndexBuildMemoryUsageMegabytes: 50}}});
rst.startSet();
rst.initiate();

// Insert enough data so that the collection scan spills to disk.
const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName());
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < 100; i++) {
    // Each document is at least 1 MB.
    bulk.insert({a: i.toString().repeat(1024 * 1024)});
}
assert.commandWorked(bulk.execute());

ResumableIndexBuildTest.run(
    rst,
    dbName,
    coll.getName(),
    [[{a: 1}]],
    [{name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400}],
    50,
    ["bulk load"],
    [{skippedPhaseLogID: 20391}]);

rst.stopSet();
})();
/**
 * Tests that a resumable index build clears "_tmp" directory except for files for the build on
 * startup recovery.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const numDocuments = 100;
const maxIndexBuildMemoryUsageMB = 50;

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {setParameter: {maxIndexBuildMemoryUsageMegabytes: maxIndexBuildMemoryUsageMB}}
});
rst.startSet();
rst.initiate();

// Insert enough data so that the collection scan spills to disk.
const primary = rst.getPrimary();
const coll = primary.getDB(dbName).getCollection(jsTestName());
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < numDocuments; i++) {
    // Each document is at least 1 MB.
    bulk.insert({a: i.toString().repeat(1024 * 1024)});
}
assert.commandWorked(bulk.execute());

// Manually writes a garbage file to "_tmp" directory.
const tmpDir = primary.dbpath + "/_tmp/";
mkdir(tmpDir);
writeFile(tmpDir + "garbage", "");

// Runs a resumable index build till completed to make sure the spilled files in "_tmp" directory
// are not deleted with the garbage file.
ResumableIndexBuildTest.run(
    rst,
    dbName,
    coll.getName(),
    [[{a: 1}]],
    [{name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386}],
    // Each document is at least 1 MB, so the index build must have spilled to disk by this point.
    maxIndexBuildMemoryUsageMB,
    ["collection scan"],
    [{numScannedAfterResume: numDocuments - maxIndexBuildMemoryUsageMB}]);

// Asserts the garbage file is deleted.
const files = listFiles(tmpDir);
assert.eq(files.length, 0, files);

rst.stopSet();
})();
/**
 * Tests that resumable index builds in the collection scan phase write their state to disk upon
 * clean shutdown and are resumed from the same phase to completion when the node is started back
 * up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const runTests = function(docs, indexSpecsFlat, collNameSuffix) {
    const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName() + collNameSuffix);
    assert.commandWorked(coll.insert(docs));

    const runTest = function(indexSpecs, iteration) {
        ResumableIndexBuildTest.run(
            rst,
            dbName,
            coll.getName(),
            indexSpecs,
            [{
                name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion",
                logIdWithBuildUUID: 20386
            }],
            iteration,
            ["collection scan"],
            [{numScannedAfterResume: 2 - iteration}]);
    };

    runTest([[indexSpecsFlat[0]]], 0);
    runTest([[indexSpecsFlat[0]]], 1);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 0);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 1);
    runTest([indexSpecsFlat], 0);
    runTest([indexSpecsFlat], 1);
};

runTests([{a: 1, b: 1}, {a: 2, b: 2}], [{a: 1}, {b: 1}], "");
runTests([{a: [1, 2], b: [1, 2]}, {a: 2, b: 2}], [{a: 1}, {b: 1}], "_multikey_first");
runTests([{a: 1, b: 1}, {a: [1, 2], b: [1, 2]}], [{a: 1}, {b: 1}], "_multikey_last");
runTests([{a: [1, 2], b: 1}, {a: 2, b: [1, 2]}], [{a: 1}, {b: 1}], "_multikey_mixed");
runTests([{a: [1, 2], b: {c: [3, 4]}, d: ""}, {e: "", f: [[]], g: null, h: 8}],
         [{"$**": 1}, {h: 1}],
         "_wildcard");

rst.stopSet();
})();
/**
 * Tests that a resumable index build large enough to spill to disk during the collection scan
 * phase writes its state to disk upon clean shutdown during the collection scan phase and is
 * resumed from the same phase to completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const numDocuments = 100;
const maxIndexBuildMemoryUsageMB = 50;

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {setParameter: {maxIndexBuildMemoryUsageMegabytes: maxIndexBuildMemoryUsageMB}}
});
rst.startSet();
rst.initiate();

// Insert enough data so that the collection scan spills to disk.
const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName());
const bulk = coll.initializeUnorderedBulkOp();
for (let i = 0; i < numDocuments; i++) {
    // Each document is at least 1 MB.
    bulk.insert({a: i.toString().repeat(1024 * 1024)});
}
assert.commandWorked(bulk.execute());

ResumableIndexBuildTest.run(
    rst,
    dbName,
    coll.getName(),
    [[{a: 1}]],
    [{name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386}],
    // Each document is at least 1 MB, so the index build must have spilled to disk by this point.
    maxIndexBuildMemoryUsageMB,
    ["collection scan"],
    [{numScannedAfterResume: numDocuments - maxIndexBuildMemoryUsageMB}]);

rst.stopSet();
})();
/**
 * Tests that resumable index builds in the drain writes phase write their state to disk upon clean
 * shutdown and are resumed from the same phase to completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const runTests = function(docs, indexSpecsFlat, sideWrites, collNameSuffix) {
    const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName() + collNameSuffix);
    assert.commandWorked(coll.insert(docs));

    const runTest = function(indexSpecs, iteration) {
        ResumableIndexBuildTest.run(
            rst,
            dbName,
            coll.getName(),
            indexSpecs,
            [{name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}],
            iteration,
            ["drain writes"],
            [{skippedPhaseLogID: 20392}],
            sideWrites,
            [{a: 4}, {a: 5}]);
    };

    runTest([[indexSpecsFlat[0]]], 0);
    runTest([[indexSpecsFlat[0]]], 1);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 0);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]], 1);
    runTest([indexSpecsFlat], 0);
    runTest([indexSpecsFlat], 1);
};

runTests({a: 1}, [{a: 1}, {b: 1}], [{a: 2, b: 2}, {a: 3, b: 3}], "");
runTests({a: 1}, [{a: 1}, {b: 1}], [{a: [2, 3], b: [2, 3]}, {a: 3, b: 3}], "_multikey_first");
runTests({a: 1}, [{a: 1}, {b: 1}], [{a: 2, b: 2}, {a: [3, 4], b: [3, 4]}], "_multikey_last");
runTests({a: 1}, [{a: 1}, {b: 1}], [{a: [2, 3], b: 2}, {a: 3, b: [3, 4]}], "_multikey_mixed");
runTests({a: 1},
         [{"$**": 1}, {h: 1}],
         [{a: [1, 2], b: {c: [3, 4]}, d: ""}, {e: "", f: [[]], g: null, h: 8}],
         "_wildcard");

rst.stopSet();
})();
/**
 * Tests that resumable index build state is written to disk upon clean shutdown when an index
 * build is in the drain writes phase on a primary, and that the index build is subsequently
 * completed when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = jsTestName();

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const coll = primary.getDB(dbName).getCollection(collName);

assert.commandWorked(coll.insert({a: 1}));

jsTestLog("Testing when primary shuts down in the middle of the first drain");

ResumableIndexBuildTest.run(
    rst,
    dbName,
    collName,
    [[{a: 1}]],
    [{name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}],
    0,
    ["drain writes"],
    [{skippedPhaseLogID: 20392}],
    [{a: 2}, {a: 3}],
    [{a: 4}, {a: 5}]);
ResumableIndexBuildTest.run(
    rst,
    dbName,
    collName,
    [[{a: 1}]],
    [{name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}],
    1,
    ["drain writes"],
    [{skippedPhaseLogID: 20392}],
    [{a: 6}, {a: 7}],
    [{a: 8}, {a: 9}]);

jsTestLog("Testing when primary shuts down after voting, but before commit quorum satisfied");

ResumableIndexBuildTest.runOnPrimaryToTestCommitQuorum(
    rst,
    dbName,
    collName,
    {a: 1},
    "hangIndexBuildAfterSignalPrimaryForCommitReadiness",
    "hangAfterIndexBuildFirstDrain",
    [{a: 10}, {a: 11}],
    [{a: 12}, {a: 13}]);

jsTestLog(
    "Testing when primary shuts down after commit quorum satisfied, but before commitIndexBuild oplog entry written");

ResumableIndexBuildTest.runOnPrimaryToTestCommitQuorum(
    rst,
    dbName,
    collName,
    {a: 1},
    "hangIndexBuildAfterSignalPrimaryForCommitReadiness",
    "hangIndexBuildAfterSignalPrimaryForCommitReadiness",
    [{a: 14}, {a: 15}],
    [{a: 16}, {a: 17}]);

rst.stopSet();
})();
/**
 * Tests that resumable index build state is written to disk upon clean shutdown when an index
 * build is in the drain writes phase on a secondary, and that the index build is subsequently
 * completed when the node is started back up.
 *
 * For this test, the secondary is ineligible to become primary, but still has a vote so it is
 * required for the commit quorum to be satisfied.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = jsTestName();

let rst = new ReplSetTest({
    nodes: [
        {},
        {rsConfig: {priority: 0}},
    ]
});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let coll = primary.getDB(dbName).getCollection(collName);

assert.commandWorked(coll.insert({a: 1}));

jsTestLog("Testing when secondary shuts down in the middle of the first drain");

ResumableIndexBuildTest.runOnSecondary(rst,
                                       dbName,
                                       collName,
                                       {a: 1},
                                       "hangIndexBuildDuringDrainWritesPhase",
                                       0,
                                       undefined, /* primaryFailPointName */
                                       [{a: 2}, {a: 3}],
                                       [{a: 4}, {a: 5}]);
ResumableIndexBuildTest.runOnSecondary(rst,
                                       dbName,
                                       collName,
                                       {a: 1},
                                       "hangIndexBuildDuringDrainWritesPhase",
                                       1,
                                       undefined, /* primaryFailPointName */
                                       [{a: 6}, {a: 7}],
                                       [{a: 8}, {a: 9}]);

jsTestLog("Testing when secondary shuts down before voting");

ResumableIndexBuildTest.runOnSecondary(rst,
                                       dbName,
                                       collName,
                                       {a: 1},
                                       "hangAfterIndexBuildFirstDrain",
                                       {},
                                       undefined, /* primaryFailPointName */
                                       [{a: 10}, {a: 11}],
                                       [{a: 12}, {a: 13}]);

jsTestLog(
    "Testing when secondary shuts down after commit quorum satisfied, but before replicating commitIndexBuild oplog entry");

ResumableIndexBuildTest.runOnSecondary(rst,
                                       dbName,
                                       collName,
                                       {a: 1},
                                       "hangIndexBuildAfterSignalPrimaryForCommitReadiness",
                                       {},
                                       "hangIndexBuildBeforeCommit",
                                       [{a: 14}, {a: 15}],
                                       [{a: 16}, {a: 17}]);

rst.stopSet();
})();
/**
 * Tests that resumable index builds that have been initialized, but not yet begun the collection
 * scan, write their state to disk upon clean shutdown and are resumed from the same phase to
 * completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const runTests = function(docs, indexSpecsFlat, collNameSuffix) {
    const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName() + collNameSuffix);
    assert.commandWorked(coll.insert(docs));

    const runTest = function(indexSpecs) {
        ResumableIndexBuildTest.run(
            rst,
            dbName,
            coll.getName(),
            indexSpecs,
            [{name: "hangIndexBuildBeforeWaitingUntilMajorityOpTime", logIdWithBuildUUID: 4940901}],
            {},
            ["initialized"],
            [{numScannedAfterResume: 1}]);
    };

    runTest([[indexSpecsFlat[0]]]);
    runTest([[indexSpecsFlat[0]], [indexSpecsFlat[1]]]);
    runTest([indexSpecsFlat]);
};

runTests({a: 1, b: 1}, [{a: 1}, {b: 1}], "");
runTests({a: [1, 2], b: [1, 2]}, [{a: 1}, {b: 1}], "_multikey");
runTests({a: [1, 2], b: {c: [3, 4]}, d: ""}, [{"$**": 1}, {d: 1}], "_wildcard");

rst.stopSet();
})();
/**
 * Tests that resumable index builds in different phases write their state to disk upon clean
 * shutdown and are resumed from the same phase to completion when the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const runTest = function(docs, indexSpecs, failPoints, resumePhases, resumeChecks, collNameSuffix) {
    const coll = rst.getPrimary().getDB(dbName).getCollection(
        jsTestName() + "_" + resumePhases[0].replace(" ", "_") + "_" +
        resumePhases[1].replace(" ", "_") + collNameSuffix);
    assert.commandWorked(coll.insert(docs));

    ResumableIndexBuildTest.run(rst,
                                dbName,
                                coll.getName(),
                                indexSpecs,
                                failPoints,
                                1,
                                resumePhases,
                                resumeChecks,
                                [{a: 4, b: 4}, {a: 5, b: 5}, {a: 6, b: 6}],
                                [{a: 7, b: 7}, {a: 8, b: 8}, {a: 9, b: 9}]);
};

const runTests = function(failPoints, resumePhases, resumeChecks) {
    runTest([{a: 1, b: 1}, {a: 2, b: 2}, {a: 3, b: 3}],
            [[{a: 1}], [{b: 1}]],
            failPoints,
            resumePhases,
            resumeChecks,
            "");
    runTest([{a: [1, 2]}, {a: 2, b: 2}, {a: 3, b: [3, 4]}],
            [[{a: 1}], [{b: 1}]],
            failPoints,
            resumePhases,
            resumeChecks,
            "_multikey");
    runTest([{a: [1, 2], b: {c: [3, 4]}}, {d: "", e: "", f: [[]]}, {g: null, h: 8}],
            [[{"$**": 1}], [{h: 1}]],
            failPoints,
            resumePhases,
            resumeChecks,
            "_wildcard");
};

runTests(
    [
        {name: "hangIndexBuildBeforeWaitingUntilMajorityOpTime", logIdWithBuildUUID: 4940901},
        {name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386}
    ],
    ["initialized", "collection scan"],
    [{numScannedAfterResume: 6}, {numScannedAfterResume: 5}]);
runTests(
    [
        {name: "hangIndexBuildBeforeWaitingUntilMajorityOpTime", logIdWithBuildUUID: 4940901},
        {name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400}
    ],
    ["initialized", "bulk load"],
    [{numScannedAfterResume: 6}, {skippedPhaseLogID: 20391}]);
runTests(
    [
        {name: "hangIndexBuildBeforeWaitingUntilMajorityOpTime", logIdWithBuildUUID: 4940901},
        {name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}
    ],
    ["initialized", "drain writes"],
    [{numScannedAfterResume: 6}, {skippedPhaseLogID: 20392}]);
runTests(
    [
        {name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386},
        {name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400}
    ],
    ["collection scan", "bulk load"],
    [{numScannedAfterResume: 5}, {skippedPhaseLogID: 20391}]);
runTests(
    [
        {name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion", logIdWithBuildUUID: 20386},
        {name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}
    ],
    ["collection scan", "drain writes"],
    [{numScannedAfterResume: 5}, {skippedPhaseLogID: 20392}]);
runTests(
    [
        {name: "hangIndexBuildDuringBulkLoadPhase", logIdWithIndexName: 4924400},
        {name: "hangIndexBuildDuringDrainWritesPhase", logIdWithIndexName: 4841800}
    ],
    ["bulk load", "drain writes"],
    [{skippedPhaseLogID: 20391}, {skippedPhaseLogID: 20392}]);

rst.stopSet();
})();
/**
 * Tests that resumable index builds on time-series collections in the collection scan phase write
 * their state to disk upon clean shutdown and are resumed from the same phase to completion when
 * the node is started back up.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");
load("jstests/core/timeseries/libs/timeseries.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const db = rst.getPrimary().getDB(dbName);

if (!TimeseriesTest.timeseriesCollectionsEnabled(db.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    rst.stopSet();
    return;
}

const coll = db.timeseries_resumable_index_build;
coll.drop();

const timeFieldName = "time";
const metaFieldName = 'meta';
assert.commandWorked(db.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

// Use different metadata fields to guarantee creating three individual buckets in the buckets
// collection.
for (let i = 0; i < 3; i++) {
    assert.commandWorked(coll.insert({
        _id: i,
        measurement: "measurement",
        time: ISODate(),
        meta: i,
    }));
}

const bucketColl = db.getCollection("system.buckets." + coll.getName());
ResumableIndexBuildTest.run(
    rst,
    dbName,
    bucketColl.getName(),
    [[{"control.min.time": 1}, {"control.max.time": 1}]],
    [{name: "hangIndexBuildDuringCollectionScanPhaseAfterInsertion", logIdWithBuildUUID: 20386}],
    /*iteration=*/0,
    ["collection scan"],
    [{numScannedAfterResume: 2}]);

rst.stopSet();
})();
/**
 * Test that we can use the $_resumeAfter and $_requestResumeToken options to resume a query
 * even after the node has been restarted.
 *
 * @tags: [
 *   requires_persistence,
 * ]
 */

(function() {
"use strict";
const testName = TestData.testName;
let conn = MongoRunner.runMongod();
let db = conn.getDB(testName);

jsTestLog("Setting up the data.");
const testData = [{_id: 0, a: 1}, {_id: 1, b: 2}, {_id: 2, c: 3}, {_id: 3, d: 4}];
assert.commandWorked(db.test.insert(testData));

jsTestLog("Running the initial query.");
let res = assert.commandWorked(
    db.runCommand({find: "test", hint: {$natural: 1}, batchSize: 2, $_requestResumeToken: true}));
assert.eq(2, res.cursor.firstBatch.length);
assert.contains(res.cursor.firstBatch[0], testData);
let queryData = res.cursor.firstBatch;
assert.hasFields(res.cursor, ["postBatchResumeToken"]);
let resumeToken = res.cursor.postBatchResumeToken;

jsTestLog("Restarting the node.");
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB(testName);

jsTestLog("Running the second query after restarting the node.");
res = assert.commandWorked(db.runCommand({
    find: "test",
    hint: {$natural: 1},
    batchSize: 10,
    $_requestResumeToken: true,
    $_resumeAfter: resumeToken
}));
// This should have exhausted the collection.
assert.eq(0, res.cursor.id);
assert.eq(2, res.cursor.firstBatch.length);
queryData = queryData.concat(res.cursor.firstBatch);
assert.sameMembers(testData, queryData);
MongoRunner.stopMongod(conn);
})();

/**
 * Unit test to verify that 'retryOnNetworkError' works correctly for common network connection
 * issues.
 */

(function() {
"use strict";
let node = MongoRunner.runMongod();
let hostname = node.host;

jsTestLog("Test connecting to a healthy node.");
let numRetries = 5;
let sleepMs = 50;
let attempts = 0;
retryOnNetworkError(function() {
    attempts++;
    new Mongo(hostname);
}, numRetries, sleepMs);
assert.eq(attempts, 1);

jsTestLog("Test connecting to a node that is down.");
MongoRunner.stopMongod(node);
attempts = 0;
try {
    retryOnNetworkError(function() {
        attempts++;
        new Mongo(hostname);
    }, numRetries, sleepMs);
} catch (e) {
    jsTestLog("Caught exception after exhausting retries: " + e);
}
assert.eq(attempts, numRetries + 1);

jsTestLog("Test connecting to a node with an invalid hostname.");
let invalidHostname = "very-invalid-host-name";
attempts = 0;
try {
    retryOnNetworkError(function() {
        attempts++;
        new Mongo(invalidHostname);
    }, numRetries, sleepMs);
} catch (e) {
    jsTestLog("Caught exception after exhausting retries: " + e);
}
assert.eq(attempts, numRetries + 1);
}());
/*
 * Verify behavior of retryable write commands on a standalone mongod.
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

const standalone = MongoRunner.runMongod();
const testDB = standalone.getDB("test");

// Commands sent to standalone nodes are not allowed to have transaction numbers.
assert.commandFailedWithCode(
    testDB.runCommand(
        {insert: "foo", documents: [{x: 1}], txnNumber: NumberLong(1), lsid: {id: UUID()}}),
    ErrorCodes.IllegalOperation,
    "expected command with transaction number to fail on standalone mongod");

MongoRunner.stopMongod(standalone);
}());

/**
 * Tests that a collection drop can be rolled back.
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

load('jstests/replsets/libs/rollback_test.js');

// Returns list of collections in database, including pending drops.
// Assumes all collections fit in first batch of results.
function listCollections(database) {
    return assert
        .commandWorked(database.runCommand({listCollections: 1, includePendingDrops: true}))
        .cursor.firstBatch;
}

// Operations that will be present on both nodes, before the common point.
const collName = 'test.t';
const renameTargetCollName = 'test.x';
const noOpsToRollbackCollName = 'test.k';
let CommonOps = (node) => {
    const coll = node.getCollection(collName);
    const mydb = coll.getDB();
    assert.commandWorked(mydb.createCollection(coll.getName()));
    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.insert({_id: 0, a: 0}));

    // Replicate a drop.
    const replicatedDropCollName = 'w';
    const collToDrop = mydb.getCollection(replicatedDropCollName);
    assert.commandWorked(mydb.createCollection(collToDrop.getName()));
    assert(collToDrop.drop());
    TwoPhaseDropCollectionTest.waitForDropToComplete(mydb, replicatedDropCollName);

    // This collection will be dropped during a rename.
    const renameTargetColl = node.getCollection(renameTargetCollName);
    assert.commandWorked(mydb.createCollection(renameTargetColl.getName()));
    assert.commandWorked(renameTargetColl.createIndex({b: 1}));
    assert.commandWorked(renameTargetColl.insert({_id: 8, b: 8}));
    assert.commandWorked(renameTargetColl.insert({_id: 9, b: 9}));

    // This collection will be dropped without any CRUD ops to rollback.
    const noOpsToRollbackColl = node.getCollection(noOpsToRollbackCollName);
    assert.commandWorked(mydb.createCollection(noOpsToRollbackColl.getName()));
    assert.commandWorked(noOpsToRollbackColl.createIndex({c: 1}));
    assert.commandWorked(noOpsToRollbackColl.insert({_id: 20, c: 20}));
    assert.commandWorked(noOpsToRollbackColl.insert({_id: 21, c: 21}));
};

// Operations that will be performed on the rollback node past the common point.
let RollbackOps = (node) => {
    const coll = node.getCollection(collName);

    // Rollback algorithm may refer to dropped collection if it has to undo an insert.
    assert.commandWorked(coll.insert({_id: 1, a: 1}));

    const mydb = coll.getDB();
    const collectionsBeforeDrop = listCollections(mydb);
    assert(coll.drop());
    const collectionsAfterDrop = listCollections(mydb);
    const supportsPendingDrops = mydb.serverStatus().storageEngine.supportsPendingDrops;
    jsTestLog('supportsPendingDrops = ' + supportsPendingDrops);
    if (!supportsPendingDrops) {
        assert.eq(collectionsAfterDrop.length,
                  collectionsBeforeDrop.length,
                  'listCollections did not report the same number of collections in database ' +
                      mydb.getName() + ' after dropping collection ' + coll.getFullName() +
                      '. Before: ' + tojson(collectionsBeforeDrop) +
                      '. After: ' + tojson(collectionsAfterDrop));
    } else {
        assert.lt(collectionsAfterDrop.length,
                  collectionsBeforeDrop.length,
                  'listCollections did not report fewer collections in database ' + mydb.getName() +
                      ' after dropping collection ' + coll.getFullName() + '. Before: ' +
                      tojson(collectionsBeforeDrop) + '. After: ' + tojson(collectionsAfterDrop));
        assert.gt(mydb.serverStatus().storageEngine.dropPendingIdents,
                  0,
                  'There is no drop pending ident in the storage engine.');
    }

    const renameTargetColl = node.getCollection(renameTargetCollName);
    assert.commandWorked(renameTargetColl.insert({_id: 10, b: 10}));
    assert.commandWorked(renameTargetColl.insert({_id: 11, b: 11}));
    const renameSourceColl = mydb.getCollection('z');
    assert.commandWorked(mydb.createCollection(renameSourceColl.getName()));
    assert.commandWorked(renameSourceColl.renameCollection(renameTargetColl.getName(), true));

    const noOpsToRollbackColl = node.getCollection(noOpsToRollbackCollName);
    assert(noOpsToRollbackColl.drop());

    // This collection will not exist after rollback.
    const tempColl = node.getCollection('test.a');
    assert.commandWorked(mydb.createCollection(tempColl.getName()));
    assert.commandWorked(tempColl.insert({_id: 100, y: 100}));
    assert(tempColl.drop());
};

// Set up Rollback Test.
const rollbackTest = new RollbackTest();
CommonOps(rollbackTest.getPrimary());

const rollbackNode = rollbackTest.transitionToRollbackOperations();
RollbackOps(rollbackNode);

{
    // Check collection drop oplog entry.
    const replTest = rollbackTest.getTestFixture();
    const ops = replTest.dumpOplog(rollbackNode, {ns: 'test.$cmd', 'o.drop': 't'});
    assert.eq(1, ops.length);
    const op = ops[0];
    assert(op.hasOwnProperty('o2'), 'expected o2 field in drop oplog entry: ' + tojson(op));
    assert(op.o2.hasOwnProperty('numRecords'), 'expected count in drop oplog entry: ' + tojson(op));
    assert.eq(2, op.o2.numRecords, 'incorrect count in drop oplog entry: ' + tojson(op));
}

// Check collection rename oplog entry.
{
    const replTest = rollbackTest.getTestFixture();
    const ops = replTest.dumpOplog(
        rollbackNode, {ns: 'test.$cmd', 'o.renameCollection': 'test.z', 'o.to': 'test.x'});
    assert.eq(1, ops.length);
    const op = ops[0];
    assert(op.hasOwnProperty('o2'), 'expected o2 field in rename oplog entry: ' + tojson(op));
    assert(op.o2.hasOwnProperty('numRecords'),
           'expected count in rename oplog entry: ' + tojson(op));
    assert.eq(4, op.o2.numRecords, 'incorrect count in rename oplog entry: ' + tojson(op));
}

// Wait for rollback to finish.
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

// Check collection count.
const primary = rollbackTest.getPrimary();
const coll = primary.getCollection(collName);
assert.eq(1, coll.find().itcount());
assert.eq(1, coll.count());
const renameTargetColl = primary.getCollection(renameTargetCollName);
assert.eq(2, renameTargetColl.find().itcount());
assert.eq(2, renameTargetColl.count());
const noOpsToRollbackColl = primary.getCollection(noOpsToRollbackCollName);
assert.eq(2, noOpsToRollbackColl.find().itcount());
assert.eq(2, noOpsToRollbackColl.count());

rollbackTest.stop();
})();

/**
 * Builds an index in a 3-node replica set in a rolling fashion. This test implements the procedure
 * documented in:
 *     https://docs.mongodb.com/manual/tutorial/build-indexes-on-replica-sets/
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

(function() {
'use strict';

// Set up replica set
const replTest = new ReplSetTest({nodes: 3});

const nodes = replTest.startSet();
replTest.initiate();

const dbName = 'test';
const collName = 't';

// Populate collection to avoid empty collection optimization.
function insertDocs(coll, startId, numDocs) {
    const bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < numDocs; ++i) {
        const v = startId + i;
        bulk.insert({_id: v, a: v, b: v});
    }
    assert.commandWorked(bulk.execute());
}

let primary = replTest.getPrimary();
let primaryDB = primary.getDB(dbName);
let coll = primaryDB.getCollection(collName);

const numDocs = 100;
insertDocs(coll, 0, numDocs);
assert.eq(numDocs, coll.count(), 'unexpected number of documents after bulk insert.');

// Make sure the documents make it to the secondaries.
replTest.awaitReplication();

// Ensure we can create an index through replication.
assert.commandWorked(coll.createIndex({a: 1}, {name: 'replicated_index_a_1'}));

const secondaries = replTest.getSecondaries();
assert.eq(nodes.length - 1,
          secondaries.length,
          'unexpected number of secondaries: ' + tojson(secondaries));

const standalonePort = allocatePort();
jsTestLog('Standalone server will listen on port: ' + standalonePort);

function buildIndexOnNodeAsStandalone(node) {
    jsTestLog('A. Restarting as standalone: ' + node.host);
    replTest.stop(node, /*signal=*/null, /*opts=*/null, {forRestart: true, waitpid: true});
    const standalone = MongoRunner.runMongod({
        restart: true,
        dbpath: node.dbpath,
        port: standalonePort,
        setParameter: {
            disableLogicalSessionCacheRefresh: true,
            ttlMonitorEnabled: false,
        },
    });
    if (jsTestOptions().keyFile) {
        assert(jsTest.authenticate(standalone),
               'Failed authentication during restart: ' + standalone.host);
    }

    jsTestLog('B. Building index on standalone: ' + standalone.host);
    const standaloneDB = standalone.getDB(dbName);
    const standaloneColl = standaloneDB.getCollection(collName);
    assert.commandWorked(standaloneColl.createIndex({b: 1}, {name: 'rolling_index_b_1'}));

    jsTestLog('C. Restarting as replica set node: ' + node.host);
    MongoRunner.stopMongod(standalone);
    replTest.restart(node);
    replTest.awaitReplication();
}

buildIndexOnNodeAsStandalone(secondaries[0]);

jsTestLog('D. Repeat the procedure for the remaining secondary: ' + secondaries[1].host);
buildIndexOnNodeAsStandalone(secondaries[1]);

replTest.awaitNodesAgreeOnPrimary(
    replTest.kDefaultTimeoutMS, replTest.nodes, replTest.getNodeId(primary));

jsTestLog('E. Build index on the primary: ' + primary.host);
assert.commandWorked(primaryDB.adminCommand({replSetStepDown: 60}));
const newPrimary = replTest.getPrimary();
jsTestLog('Stepped down primary for index build: ' + primary.host +
          '. New primary elected: ' + newPrimary.host);
buildIndexOnNodeAsStandalone(primary);

// Ensure we can create an index after doing a rolling index build.
let newPrimaryDB = newPrimary.getDB(dbName);
let newColl = newPrimaryDB.getCollection(collName);
assert.commandWorked(newColl.createIndex({a: 1, b: 1}, {name: 'post_rolling_index_a_1_b_1'}));

insertDocs(newColl, numDocs, numDocs);
assert.eq(numDocs * 2, newColl.count(), 'unexpected number of documents after bulk insert.');

replTest.stopSet();
}());

// Verifies currentOp returns the expected fields for idle and active transactions in basic cases.
// More cases are covered in unit tests.
// @tags: [uses_transactions, uses_atclustertime]
(function() {
"use strict";

load("jstests/libs/curop_helpers.js");   // For waitForCurOpByFailPoint().
load("jstests/libs/parallelTester.js");  // for Thread.

function verifyCurrentOpFields(res, isActive) {
    // Verify top level fields relevant to transactions. Note this does not include every field, so
    // the number of fields in the response shouldn't be asserted on.

    const expectedFields = [
        "type",
        "host",
        "desc",
        "connectionId",
        "client",
        "appName",
        "clientMetadata",
        "active",
        "lsid",
        "transaction",
    ];

    assert.hasFields(res, expectedFields, tojson(res));

    if (isActive) {
        assert.eq(res.type, "op", tojson(res));
    } else {
        assert.eq(res.type, "idleSession", tojson(res));
        assert.eq(res.desc, "inactive transaction", tojson(res));
    }

    // Verify the transactions sub object.

    const transaction = res.transaction;
    const expectedTransactionsFields = [
        "parameters",
        "startWallClockTime",
        "timeOpenMicros",
        "timeActiveMicros",
        "timeInactiveMicros",
        "globalReadTimestamp",
        "numParticipants",
        "participants",
        "numNonReadOnlyParticipants",
        "numReadOnlyParticipants",
        // Commit hasn't started so don't expect 'commitStartWallClockTime' or 'commitType'.
    ];

    assert.hasFields(transaction, expectedTransactionsFields, tojson(transaction));
    assert.eq(
        expectedTransactionsFields.length, Object.keys(transaction).length, tojson(transaction));

    // Verify transaction parameters sub object.

    const parameters = transaction.parameters;
    const expectedParametersFields = [
        "txnNumber",
        "autocommit",
        "readConcern",
    ];

    assert.hasFields(parameters, expectedParametersFields, tojson(parameters));
    assert.eq(expectedParametersFields.length, Object.keys(parameters).length, tojson(parameters));

    // Verify participants sub array.

    const participants = transaction.participants;
    const expectedParticipantFields = [
        "name",
        "coordinator",
        // 'readOnly' will not be set until a response has been received from that participant, so
        // it will not be present for the active transaction because of the failpoint and is handled
        // specially.
    ];

    participants.forEach((participant) => {
        assert.hasFields(participant, expectedParticipantFields, tojson(participant));
        if (isActive) {
            // 'readOnly' should not be set.
            assert.eq(expectedParticipantFields.length,
                      Object.keys(participant).length,
                      tojson(participant));
        } else {
            // 'readOnly' should always be set for the inactive transaction.
            assert.hasFields(participant, ["readOnly"], tojson(participant));
            assert.eq(expectedParticipantFields.length + 1,  // +1 for readOnly.
                      Object.keys(participant).length,
                      tojson(participant));
        }
    });
}

function getCurrentOpForFilter(st, matchFilter) {
    const res = st.s.getDB("admin")
                    .aggregate([{$currentOp: {localOps: true}}, {$match: matchFilter}])
                    .toArray();
    assert.eq(1, res.length, res);
    return res[0];
}

const dbName = "test";
const collName = "foo";
const st = new ShardingTest({shards: 1, config: 1});

const session = st.s.startSession();
const sessionDB = session.getDatabase(dbName);

// Insert a document to set up a collection.
assert.commandWorked(sessionDB[collName].insert({x: 1}));

jsTest.log("Inactive transaction.");
(() => {
    session.startTransaction({readConcern: {level: "snapshot"}});
    assert.eq(1, sessionDB[collName].find({x: 1}).itcount());

    const res = getCurrentOpForFilter(st, {"lsid.id": session.getSessionId().id});
    verifyCurrentOpFields(res, false /* isActive */);

    assert.commandWorked(session.abortTransaction_forTesting());
})();

jsTest.log("Active transaction.");
(() => {
    assert.commandWorked(st.rs0.getPrimary().adminCommand(
        {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "alwaysOn"}));

    const txnThread = new Thread(function(host, dbName, collName) {
        const mongosConn = new Mongo(host);
        const threadSession = mongosConn.startSession();

        threadSession.startTransaction({readConcern: {level: "snapshot"}});
        assert.commandWorked(threadSession.getDatabase(dbName).runCommand(
            {find: collName, filter: {}, comment: "active_txn_find"}));

        assert.commandWorked(threadSession.abortTransaction_forTesting());
        threadSession.endSession();
    }, st.s.host, dbName, collName);
    txnThread.start();

    // Wait until we know the failpoint has been reached.
    waitForCurOpByFailPointNoNS(st.rs0.getPrimary().getDB("admin"), "waitInFindBeforeMakingBatch");

    // We don't know the id of the session started by the parallel thread, so use the find's comment
    // to get its currentOp output.
    const res = getCurrentOpForFilter(st, {"command.comment": "active_txn_find"});
    verifyCurrentOpFields(res, true /* isActive */);

    assert.commandWorked(st.rs0.getPrimary().adminCommand(
        {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "off"}));
    txnThread.join();
})();

session.endSession();
st.stop();
}());

// Tests multi-statement transactions metrics in the serverStatus output from mongos in various
// basic cases.
// @tags: [
//   uses_multi_shard_transaction,
//   uses_transactions,
// ]
(function() {
"use strict";

load("jstests/libs/curop_helpers.js");   // For waitForCurOpByFailPoint().
load("jstests/libs/parallelTester.js");  // for Thread.
load("jstests/sharding/libs/sharded_transactions_helpers.js");

// Verifies the transaction server status response has the fields that we expect.
function verifyServerStatusFields(res) {
    const expectedFields = [
        "currentOpen",
        "currentActive",
        "currentInactive",
        "totalStarted",
        "totalAborted",
        "abortCause",
        "totalCommitted",
        "totalContactedParticipants",
        "totalParticipantsAtCommit",
        "totalRequestsTargeted",
        "commitTypes",
    ];

    assert(res.hasOwnProperty("transactions"),
           "Expected serverStatus response to have a 'transactions' field, res: " + tojson(res));

    assert.hasFields(res.transactions,
                     expectedFields,
                     "The 'transactions' field did not have all of the expected fields, res: " +
                         tojson(res.transactions));

    assert.eq(expectedFields.length,
              Object.keys(res.transactions).length,
              "the 'transactions' field had an unexpected number of fields, res: " +
                  tojson(res.transactions));

    // Verify the "commitTypes" sub-object has the expected fields.
    const commitTypes = [
        "noShards",
        "singleShard",
        "singleWriteShard",
        "readOnly",
        "twoPhaseCommit",
        "recoverWithToken",
    ];
    const commitTypeFields = ["initiated", "successful", "successfulDurationMicros"];

    assert.hasFields(res.transactions.commitTypes,
                     commitTypes,
                     "The 'transactions' field did not have each expected commit type, res: " +
                         tojson(res.transactions));

    assert.eq(commitTypes.length,
              Object.keys(res.transactions.commitTypes).length,
              "the 'transactions' field had an unexpected number of commit types, res: " +
                  tojson(res.transactions));

    commitTypes.forEach((type) => {
        assert.hasFields(res.transactions.commitTypes[type],
                         commitTypeFields,
                         "commit type " + type +
                             " did not have all the expected fields, commit types: " +
                             tojson(res.transactions.commitTypes));

        assert.eq(commitTypeFields.length,
                  Object.keys(res.transactions.commitTypes[type]).length,
                  "commit type " + type + " had an unexpected number of fields, commit types: " +
                      tojson(res.transactions.commitTypes));
    });
}

class ExpectedCommitType {
    constructor() {
        this.initiated = 0;
        this.successful = 0;
        this.successfulDurationMicros = 0;
    }
}

class ExpectedAbortCause {
    constructor() {
    }
}

class ExpectedTransactionServerStatus {
    constructor() {
        this.currentOpen = 0;
        this.currentActive = 0;
        this.currentInactive = 0;
        this.totalStarted = 0;
        this.totalAborted = 0;
        this.abortCause = new ExpectedAbortCause();
        this.totalCommitted = 0;
        this.totalContactedParticipants = 0;
        this.totalParticipantsAtCommit = 0;
        this.totalRequestsTargeted = 0;
        this.commitTypes = {
            noShards: new ExpectedCommitType(),
            singleShard: new ExpectedCommitType(),
            singleWriteShard: new ExpectedCommitType(),
            readOnly: new ExpectedCommitType(),
            twoPhaseCommit: new ExpectedCommitType(),
            recoverWithToken: new ExpectedCommitType(),
        };
    }
}

// Verifies the transaction values in the server status response match the provided values.
function verifyServerStatusValues(st, expectedStats) {
    const res = assert.commandWorked(st.s.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(res);

    const stats = res.transactions;
    assert.eq(expectedStats.currentOpen,
              stats.currentOpen,
              "unexpected currentOpen, res: " + tojson(stats));
    assert.eq(expectedStats.currentActive,
              stats.currentActive,
              "unexpected currentActive, res: " + tojson(stats));
    assert.eq(expectedStats.currentInactive,
              stats.currentInactive,
              "unexpected currentInactive, res: " + tojson(stats));
    assert.eq(expectedStats.totalStarted,
              stats.totalStarted,
              "unexpected totalStarted, res: " + tojson(stats));
    assert.eq(expectedStats.totalAborted,
              stats.totalAborted,
              "unexpected totalAborted, res: " + tojson(stats));
    assert.eq(expectedStats.totalCommitted,
              stats.totalCommitted,
              "unexpected totalCommitted, res: " + tojson(stats));
    assert.eq(expectedStats.totalContactedParticipants,
              stats.totalContactedParticipants,
              "unexpected totalContactedParticipants, res: " + tojson(stats));
    assert.eq(expectedStats.totalParticipantsAtCommit,
              stats.totalParticipantsAtCommit,
              "unexpected totalParticipantsAtCommit, res: " + tojson(stats));
    assert.eq(expectedStats.totalRequestsTargeted,
              stats.totalRequestsTargeted,
              "unexpected totalRequestsTargeted, res: " + tojson(stats));

    const commitTypes = res.transactions.commitTypes;
    Object.keys(commitTypes).forEach((commitType) => {
        assert.eq(
            expectedStats.commitTypes[commitType].initiated,
            commitTypes[commitType].initiated,
            "unexpected initiated for " + commitType + ", commit types: " + tojson(commitTypes));
        assert.eq(
            expectedStats.commitTypes[commitType].successful,
            commitTypes[commitType].successful,
            "unexpected successful for " + commitType + ", commit types: " + tojson(commitTypes));

        assert.lte(expectedStats.commitTypes[commitType].successfulDurationMicros,
                   commitTypes[commitType].successfulDurationMicros,
                   "unexpected successfulDurationMicros for " + commitType +
                       ", commit types: " + tojson(commitTypes));
        expectedStats.commitTypes[commitType].successfulDurationMicros =
            commitTypes[commitType].successfulDurationMicros;

        if (commitTypes[commitType].successful != 0) {
            assert.gt(commitTypes[commitType].successfulDurationMicros,
                      0,
                      "unexpected successfulDurationMicros for " + commitType +
                          ", commit types: " + tojson(commitTypes));
        }
    });

    const abortCause = res.transactions.abortCause;
    Object.keys(abortCause).forEach((cause) => {
        assert.eq(expectedStats.abortCause[cause],
                  abortCause[cause],
                  "unexpected abortCause for " + cause + ", res: " + tojson(stats));
    });

    assert.eq(
        Object.keys(abortCause).length,
        Object.keys(expectedStats.abortCause).length,
        "the 'transactions' field had an unexpected number of abort causes, res: " + tojson(stats));
}

function abortFromUnderneath(st, session) {
    st._rs.forEach((rs) => {
        assert.commandWorkedOrFailedWithCode(rs.test.getPrimary().adminCommand({
            abortTransaction: 1,
            lsid: session.getSessionId(),
            txnNumber: session.getTxnNumber_forTesting(),
            autocommit: false
        }),
                                             ErrorCodes.NoSuchTransaction);
    });
}

const dbName = "test";
const collName = "foo";
const ns = dbName + '.' + collName;

const st = new ShardingTest({shards: 2, mongos: 2, config: 1});

const session = st.s.startSession();
const sessionDB = session.getDatabase(dbName);

const otherRouterSession = st.s1.startSession();
const otherRouterSessionDB = otherRouterSession.getDatabase(dbName);

// Set up two chunks: [-inf, 0), [0, inf) one on each shard, with one document in each.

assert.commandWorked(sessionDB[collName].createIndex({skey: 1}));
assert.commandWorked(sessionDB[collName].insert({skey: -1}));
assert.commandWorked(sessionDB[collName].insert({skey: 1}));

assert.commandWorked(st.s.adminCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.shardName);
assert.commandWorked(st.s.adminCommand({shardCollection: ns, key: {skey: 1}}));
assert.commandWorked(st.s.adminCommand({split: ns, middle: {skey: 0}}));
assert.commandWorked(st.s.adminCommand({moveChunk: ns, find: {skey: 1}, to: st.shard1.shardName}));
flushRoutersAndRefreshShardMetadata(st, {ns});

let expectedStats = new ExpectedTransactionServerStatus();

let nextSkey = 0;
const uniqueSkey = function uniqueSkey() {
    return nextSkey++;
};

//
// Helpers for setting up transactions that will trigger the various commit paths.
//

function startNoShardsTransaction() {
    session.startTransaction();
    assert.commandWorked(session.getDatabase("doesntExist").runCommand({find: collName}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    verifyServerStatusValues(st, expectedStats);
}

function startSingleShardTransaction() {
    session.startTransaction();
    assert.commandWorked(sessionDB[collName].insert({skey: uniqueSkey(), x: 1}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
}

function startSingleWriteShardTransaction() {
    session.startTransaction();
    assert.commandWorked(sessionDB[collName].insert({skey: uniqueSkey(), x: 1}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);

    assert.commandWorked(sessionDB.runCommand({find: collName}));

    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);
}

function startReadOnlyTransaction() {
    session.startTransaction();
    assert.commandWorked(sessionDB.runCommand({find: collName}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 2;
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);
}

function startTwoPhaseCommitTransaction() {
    session.startTransaction();
    assert.commandWorked(sessionDB[collName].insert({skey: -5}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);

    assert.commandWorked(sessionDB[collName].insert({skey: 5}));

    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
}

function setUpTransactionToRecoverCommit({shouldCommit}) {
    otherRouterSession.startTransaction();
    let resWithRecoveryToken = assert.commandWorked(otherRouterSessionDB.runCommand(
        {insert: collName, documents: [{skey: uniqueSkey(), x: 5}]}));
    if (shouldCommit) {
        assert.commandWorked(otherRouterSession.commitTransaction_forTesting());
    } else {
        assert.commandWorked(otherRouterSession.abortTransaction_forTesting());
    }

    // The stats on the main mongos shouldn't have changed.
    verifyServerStatusValues(st, expectedStats);

    return resWithRecoveryToken.recoveryToken;
}

//
// Test cases for serverStatus output.
//

jsTest.log("Default values.");
(() => {
    verifyServerStatusValues(st, expectedStats);
})();

// Note committing a no shards transaction can only succeed.
jsTest.log("Committed no shards transaction.");
(() => {
    startNoShardsTransaction();

    assert.commandWorked(session.commitTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.noShards.initiated += 1;
    expectedStats.commitTypes.noShards.successful += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Successful single shard transaction.");
(() => {
    startSingleShardTransaction();

    assert.commandWorked(session.commitTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.singleShard.initiated += 1;
    expectedStats.commitTypes.singleShard.successful += 1;
    expectedStats.totalParticipantsAtCommit += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Failed single shard transaction.");
(() => {
    startSingleShardTransaction();

    abortFromUnderneath(st, session);
    assert.commandFailedWithCode(session.commitTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["NoSuchTransaction"] = 1;
    expectedStats.commitTypes.singleShard.initiated += 1;
    expectedStats.totalParticipantsAtCommit += 1;
    // The one shard is targeted for the commit then the implicit abort.
    expectedStats.totalRequestsTargeted += 1 + 1;
    verifyServerStatusValues(st, expectedStats);
})();

// TODO (SERVER-48340): Re-enable the single-write-shard transaction commit optimization.
jsTest.log("Successful single write shard transaction.");
(() => {
    startSingleWriteShardTransaction();

    assert.commandWorked(session.commitTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.commitTypes.twoPhaseCommit.successful += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

// TODO (SERVER-48340): Re-enable the single-write-shard transaction commit optimization.
jsTest.log("Failed single write shard transaction.");
(() => {
    startSingleWriteShardTransaction();

    abortFromUnderneath(st, session);
    assert.commandFailedWithCode(session.commitTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["NoSuchTransaction"] += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    // There are no implicit aborts after two phase commit, so the coordinator is targeted once.
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Successful read only transaction.");
(() => {
    startReadOnlyTransaction();

    assert.commandWorked(session.commitTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.readOnly.initiated += 1;
    expectedStats.commitTypes.readOnly.successful += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Failed read only transaction.");
(() => {
    startReadOnlyTransaction();

    abortFromUnderneath(st, session);
    assert.commandFailedWithCode(session.commitTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["NoSuchTransaction"] += 1;
    expectedStats.commitTypes.readOnly.initiated += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    // Both shards are targeted for the commit then the implicit abort.
    expectedStats.totalRequestsTargeted += 2 + 2;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Successful two phase commit transaction.");
(() => {
    startTwoPhaseCommitTransaction();

    assert.commandWorked(session.commitTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.commitTypes.twoPhaseCommit.successful += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);

    // Remove the inserted documents.
    assert.commandWorked(sessionDB[collName].remove({skey: {$in: [-5, 5]}}));
})();

jsTest.log("Failed two phase commit transaction.");
(() => {
    startTwoPhaseCommitTransaction();

    abortFromUnderneath(st, session);
    assert.commandFailedWithCode(session.commitTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["NoSuchTransaction"] += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.totalParticipantsAtCommit += 2;
    // There are no implicit aborts after two phase commit, so the coordinator is targeted once.
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Recover successful commit result.");
(() => {
    const recoveryToken = setUpTransactionToRecoverCommit({shouldCommit: true});

    assert.commandWorked(st.s.adminCommand({
        commitTransaction: 1,
        lsid: otherRouterSession.getSessionId(),
        txnNumber: otherRouterSession.getTxnNumber_forTesting(),
        autocommit: false,
        recoveryToken
    }));

    // Note that current open and inactive aren't incremented by setUpTransactionToRecoverCommit.
    expectedStats.totalStarted += 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.recoverWithToken.initiated += 1;
    expectedStats.commitTypes.recoverWithToken.successful += 1;
    // The participant stats shouldn't increase if we're recovering commit.
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Recover failed commit result.");
(() => {
    const recoveryToken = setUpTransactionToRecoverCommit({shouldCommit: false});

    assert.commandFailedWithCode(st.s.adminCommand({
        commitTransaction: 1,
        lsid: otherRouterSession.getSessionId(),
        txnNumber: otherRouterSession.getTxnNumber_forTesting(),
        autocommit: false,
        recoveryToken
    }),
                                 ErrorCodes.NoSuchTransaction);

    // Note that current open and inactive aren't incremented by setUpTransactionToRecoverCommit.
    expectedStats.totalStarted += 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["NoSuchTransaction"] += 1;
    expectedStats.commitTypes.recoverWithToken.initiated += 1;
    // The participant stats shouldn't increase if we're recovering commit.
    // There are no implicit aborts during commit recovery, so the recovery shard is targeted
    // once.
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Empty recovery token.");
(() => {
    otherRouterSession.startTransaction();
    let resWithEmptyRecoveryToken =
        assert.commandWorked(otherRouterSessionDB.runCommand({find: collName}));
    assert.commandWorked(otherRouterSession.commitTransaction_forTesting());

    // The stats on the main mongos shouldn't have changed.
    verifyServerStatusValues(st, expectedStats);

    assert.commandFailedWithCode(st.s.adminCommand({
        commitTransaction: 1,
        lsid: otherRouterSession.getSessionId(),
        txnNumber: otherRouterSession.getTxnNumber_forTesting(),
        autocommit: false,
        recoveryToken: resWithEmptyRecoveryToken.recoveryToken
    }),
                                 ErrorCodes.NoSuchTransaction);

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.commitTypes.recoverWithToken.initiated += 1;
    // No requests are targeted and the decision isn't learned, so total committed/aborted and
    // total requests sent shouldn't change.
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Explicitly aborted transaction.");
(() => {
    session.startTransaction();
    assert.commandWorked(sessionDB[collName].insert({skey: uniqueSkey(), x: 2}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);

    assert.commandWorked(session.abortTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["abort"] = 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Implicitly aborted transaction.");
(() => {
    assert.commandWorked(sessionDB[collName].insert({_id: 1, skey: 1}));

    session.startTransaction();
    assert.commandFailedWithCode(sessionDB[collName].insert({_id: 1, skey: 1}),
                                 ErrorCodes.DuplicateKey);

    expectedStats.totalStarted += 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["DuplicateKey"] = 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 2;  // Plus one for the implicit abort.
    verifyServerStatusValues(st, expectedStats);

    assert.commandFailedWithCode(session.abortTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);

    // A failed abortTransaction leads to an implicit abort, so two requests are targeted.
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Abandoned transaction.");
(() => {
    session.startTransaction();
    assert.commandWorked(sessionDB[collName].insert({skey: -15}));

    expectedStats.currentOpen += 1;
    expectedStats.currentInactive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);

    session.startTransaction_forTesting({}, {ignoreActiveTxn: true});
    assert.commandWorked(sessionDB[collName].insert({skey: -15}));

    // Note that overwriting a transaction will end the previous transaction from a diagnostics
    // perspective.
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 1;
    expectedStats.totalRequestsTargeted += 1;
    // The router never learned if the previous transaction committed or aborted, so the aborted
    // counter shouldn't be incremented.
    verifyServerStatusValues(st, expectedStats);

    // Abort to clear the shell's session state.
    assert.commandWorked(session.abortTransaction_forTesting());

    expectedStats.currentOpen -= 1;
    expectedStats.currentInactive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["abort"] += 1;
    expectedStats.totalRequestsTargeted += 1;
    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Active transaction.");
(() => {
    assert.commandWorked(st.rs0.getPrimary().adminCommand(
        {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "alwaysOn", data: {nss: ns}}));

    const txnThread = new Thread(function(host, dbName, collName) {
        const mongosConn = new Mongo(host);
        const threadSession = mongosConn.startSession();

        threadSession.startTransaction();
        assert.commandWorked(
            threadSession.getDatabase(dbName).runCommand({find: collName, filter: {}}));
        assert.commandWorked(threadSession.abortTransaction_forTesting());
        threadSession.endSession();
    }, st.s.host, dbName, collName);
    txnThread.start();

    // Wait until we know the failpoint has been reached.
    waitForCurOpByFailPointNoNS(st.rs0.getPrimary().getDB("admin"), "waitInFindBeforeMakingBatch");

    expectedStats.currentOpen += 1;
    expectedStats.currentActive += 1;
    expectedStats.totalStarted += 1;
    expectedStats.totalContactedParticipants += 2;
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);

    assert.commandWorked(st.rs0.getPrimary().adminCommand(
        {configureFailPoint: "waitInFindBeforeMakingBatch", mode: "off"}));
    txnThread.join();

    expectedStats.currentOpen -= 1;
    expectedStats.currentActive -= 1;
    expectedStats.totalAborted += 1;
    expectedStats.abortCause["abort"] += 1;
    expectedStats.totalRequestsTargeted += 2;
    verifyServerStatusValues(st, expectedStats);
})();

const retrySession = st.s.startSession({retryWrites: true});
const retrySessionDB = retrySession.getDatabase(dbName);

jsTest.log("Change shard key with retryable write - findAndModify.");
(() => {
    // Insert document to be updated.
    assert.commandWorked(retrySessionDB[collName].insert({skey: -10}));

    // Retryable write findAndModify that would change the shard key. Uses a txn internally. Throws
    // on error.
    retrySessionDB[collName].findAndModify({query: {skey: -10}, update: {$set: {skey: 10}}});

    expectedStats.totalStarted += 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.commitTypes.twoPhaseCommit.successful += 1;
    expectedStats.totalContactedParticipants += 2;
    expectedStats.totalParticipantsAtCommit += 2;
    expectedStats.totalRequestsTargeted += 4;

    verifyServerStatusValues(st, expectedStats);
})();

jsTest.log("Change shard key with retryable write - batch write command.");
(() => {
    // Insert document to be updated.
    assert.commandWorked(retrySessionDB[collName].insert({skey: -15}));

    // Retryable write update that would change the shard key. Uses a txn internally.
    assert.commandWorked(retrySessionDB[collName].update({skey: -15}, {$set: {skey: 15}}));

    expectedStats.totalStarted += 1;
    expectedStats.totalCommitted += 1;
    expectedStats.commitTypes.twoPhaseCommit.initiated += 1;
    expectedStats.commitTypes.twoPhaseCommit.successful += 1;
    expectedStats.totalContactedParticipants += 2;
    expectedStats.totalParticipantsAtCommit += 2;
    expectedStats.totalRequestsTargeted += 4;

    verifyServerStatusValues(st, expectedStats);
})();

session.endSession();
st.stop();
}());

/*
 * Tests that split horizon reconfig results in unknown ServerDescription in
 * StreamableReplicaSetMonitor.
 */
(function() {
'use strict';

const st = new ShardingTest(
    {mongos: [{setParameter: {replicaSetMonitorProtocol: "streamable"}}], config: 1, shards: 0});
const configRSPrimary = st.configRS.getPrimary();

const unknownTopologyChangeRegex = new RegExp(
    `Topology Change.*${st.configRS.name}.*topologyType:.*ReplicaSetNoPrimary.*type:.*Unknown`);
const knownTopologyChangeRegex = new RegExp(
    `Topology Change.*${st.configRS.name}.*topologyType:.*ReplicaSetWithPrimary.*type:.*RSPrimary`);
const expeditedMonitoringAfterNetworkErrorRegex =
    new RegExp(`RSM monitoring host in expedited mode until we detect a primary`);
const unknownServerDescriptionRegex =
    new RegExp("(" + unknownTopologyChangeRegex.source + ")|(" +
               expeditedMonitoringAfterNetworkErrorRegex.source + ")");

jsTest.log("Wait until the RSM on the mongos finds out about the config server primary");
checkLog.contains(st.s, knownTopologyChangeRegex);

jsTest.log("Run split horizon reconfig and verify that it results in unknown server description");
const rsConfig = configRSPrimary.getDB("local").system.replset.findOne();
for (let i = 0; i < rsConfig.members.length; i++) {
    rsConfig.members[i].horizons = {specialHorizon: 'horizon.com:100' + i};
}
rsConfig.version++;

assert.commandWorked(st.s.adminCommand({clearLog: 'global'}));
assert.commandWorked(configRSPrimary.adminCommand({replSetReconfig: rsConfig}));

checkLog.contains(st.s, unknownServerDescriptionRegex);

jsTest.log("Verify that the RSM eventually has the right topology description again");
checkLog.contains(st.s, knownTopologyChangeRegex);
st.stop();
})();

// Tests the _runningMongoChildProcessIds shell built-in.
// @tags: [live_record_incompatible]

(function() {
'use strict';

/**
 * @param {NumberLong[]} expected pids
 */
function assertRunningMongoChildProcessIds(expected) {
    const expectedSorted = expected.sort();
    const children = MongoRunner.runningChildPids().sort();
    assert.eq(expectedSorted, children);
}

// Empty before we start anything.
assertRunningMongoChildProcessIds([]);

(function() {
// Start 1 mongod.
const mongod = MongoRunner.runMongod({});
try {
    // Call 3 times just for good-measure.
    assertRunningMongoChildProcessIds([mongod.pid]);
    assertRunningMongoChildProcessIds([mongod.pid]);
    assertRunningMongoChildProcessIds([mongod.pid]);
} finally {
    MongoRunner.stopMongod(mongod);
}
})();

(function() {
// Start 2 mongods.
const mongod1 = MongoRunner.runMongod({});
const mongod2 = MongoRunner.runMongod({});
try {
    assertRunningMongoChildProcessIds([mongod1.pid, mongod2.pid]);

    // Stop mongod1 and only mongod2 should remain.
    MongoRunner.stopMongod(mongod1);
    assertRunningMongoChildProcessIds([mongod2.pid]);
} finally {
    // It is safe to stop multiple times.
    MongoRunner.stopMongos(mongod2);
    MongoRunner.stopMongos(mongod1);
}
})();

// empty at the end
assertRunningMongoChildProcessIds([]);
})();

/**
 * Tests that an incomplete rolling index build can be salvaged when building the same index across
 * a replica set when one or more secondaries already have the index built.
 *
 * By default, the commit quorum is "votingMembers", which is all data-bearing replica set members.
 * The issue arises when starting an index build on the primary which the secondaries have already
 * built to completion. The secondaries would treat the "startIndexBuild" oplog entry as a no-op and
 * return immediately. This causes the secondaries to skip voting for the index build to be
 * committed or aborted, which prevents the primary from satisfying the commit quorum. The
 * "setIndexCommitQuorum" command can be used to modify the commit quorum of in-progress index
 * builds to get out of this situation.
 *
 * Note: this is an incorrect way to build indexes, but demonstrates that "setIndexCommitQuorum" can
 * get a user out of this situation if they end up in it.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

// Set up the replica set. We need to set "oplogApplicationEnforcesSteadyStateConstraints=false" as
// we'll be violating the index build process by having the index already built on the secondary
// nodes. This is false by default outside of our testing.
const replTest = new ReplSetTest({
    nodes: 3,
    nodeOptions: {setParameter: {oplogApplicationEnforcesSteadyStateConstraints: false}}
});

const nodes = replTest.startSet();
replTest.initiate();

const dbName = "test";
const collName = "t";

// Populate collection to avoid empty collection optimization.
function insertDocs(coll, startId, numDocs) {
    const bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < numDocs; ++i) {
        const v = startId + i;
        bulk.insert({_id: v, a: v, b: v});
    }
    assert.commandWorked(bulk.execute());
}

let primary = replTest.getPrimary();
let primaryDB = primary.getDB(dbName);
let coll = primaryDB.getCollection(collName);

const numDocs = 100;
insertDocs(coll, 0, numDocs);
assert.eq(numDocs, coll.count(), "unexpected number of documents after bulk insert.");

// Make sure the documents make it to the secondaries.
replTest.awaitReplication();

const secondaries = replTest.getSecondaries();
assert.eq(nodes.length - 1,
          secondaries.length,
          "unexpected number of secondaries: " + tojson(secondaries));

const standalonePort = allocatePort();
jsTestLog("Standalone server will listen on port: " + standalonePort);

function buildIndexOnNodeAsStandalone(node) {
    jsTestLog("A. Restarting as standalone: " + node.host);
    replTest.stop(node, /*signal=*/null, /*opts=*/null, {forRestart: true, waitpid: true});
    const standalone = MongoRunner.runMongod({
        restart: true,
        dbpath: node.dbpath,
        port: standalonePort,
        setParameter: {
            disableLogicalSessionCacheRefresh: true,
            ttlMonitorEnabled: false,
        },
    });
    if (jsTestOptions().keyFile) {
        assert(jsTest.authenticate(standalone),
               "Failed authentication during restart: " + standalone.host);
    }

    jsTestLog("B. Building index on standalone: " + standalone.host);
    const standaloneDB = standalone.getDB(dbName);
    const standaloneColl = standaloneDB.getCollection(collName);
    assert.commandWorked(standaloneColl.createIndex({b: 1}, {name: "rolling_index_b_1"}));

    jsTestLog("C. Restarting as replica set node: " + node.host);
    MongoRunner.stopMongod(standalone);
    replTest.restart(node);
    replTest.awaitReplication();
}

buildIndexOnNodeAsStandalone(secondaries[0]);

jsTestLog("D. Repeat the procedure for the remaining secondary: " + secondaries[1].host);
buildIndexOnNodeAsStandalone(secondaries[1]);

replTest.awaitNodesAgreeOnPrimary(
    replTest.kDefaultTimeoutMS, replTest.nodes, replTest.getNodeId(primary));

// The primary does not perform the rolling index build procedure. Instead, the createIndex command
// is issued against the replica set, where both the secondaries have already built the index.
jsTestLog("E. Build index on the primary as part of the replica set: " + primary.host);
let awaitIndexBuild = IndexBuildTest.startIndexBuild(
    primary, coll.getFullName(), {b: 1}, {name: "rolling_index_b_1"});
IndexBuildTest.waitForIndexBuildToStart(primaryDB, coll.getName(), "rolling_index_b_1");

checkLog.containsJson(primary, 3856203);  // Waiting for the commit quorum to be satisfied.

// The drain phase periodically runs while waiting for the commit quorum to be satisfied.
insertDocs(coll, numDocs, numDocs * 2);
checkLog.containsJson(primary, 20689, {index: "rolling_index_b_1"});  // Side writes drained.

// As the secondaries won't vote, we change the commit quorum to 1. This will allow the primary to
// proceed with committing the index build.
assert.commandWorked(primaryDB.runCommand(
    {setIndexCommitQuorum: collName, indexNames: ["rolling_index_b_1"], commitQuorum: 1}));
awaitIndexBuild();

replTest.stopSet();
}());

/**
 * Verify that $sample push down works properly in a transaction. This test was designed to
 * reproduce SERVER-57642.
 *
 * Requires WiredTiger for random cursor support.
 * @tags: [requires_wiredtiger, requires_replication]
 */
(function() {
'use strict';

load('jstests/libs/analyze_plan.js');  // For planHasStage.

// Set up.
const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const collName = 'sample_pushdown';
const dbName = 'test';
const testDB = rst.getPrimary().getDB(dbName);
const coll = testDB[collName];

// In order to construct a plan that uses a storage engine random cursor, we not only need more
// than 100 records in our collection, we also need the sample size to be less than 5% of the
// number of documents in our collection.
const numDocs = 1000;
const sampleSize = numDocs * .03;
let docs = [];
for (let i = 0; i < numDocs; ++i) {
    docs.push({a: i});
}
assert.commandWorked(coll.insert(docs));
const pipeline = [{$sample: {size: sampleSize}}, {$match: {a: {$gte: 0}}}];

// Verify that our pipeline uses $sample push down.
const explain = coll.explain().aggregate(pipeline);
assert(aggPlanHasStage(explain, "$sampleFromRandomCursor"), tojson(explain));

// Start the transaction.
const session = testDB.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
session.startTransaction();

// Run the pipeline.
const randDocs = sessionDB[collName].aggregate(pipeline).toArray();

// Verify that we have at least one result.
assert.gt(randDocs.length, 0, tojson(randDocs));

// Clean up.
assert.commandWorked(session.abortTransaction_forTesting());
rst.stopSet();
})();

// Tests readConcern level metrics in the serverStatus output.
// @tags: [
//   requires_majority_read_concern,
//   requires_persistence,
//   uses_transactions,
// ]
(function() {
"use strict";

// Verifies that the server status response has the fields that we expect.
function verifyServerStatusFields(serverStatusResponse) {
    assert(serverStatusResponse.hasOwnProperty("readConcernCounters"),
           "Missing 'readConcernCounters' from serverStatus\n" + tojson(serverStatusResponse));

    assert(serverStatusResponse.readConcernCounters.hasOwnProperty("nonTransactionOps"),
           "Missing 'nonTransactionOps' from 'readConcernCounters'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("none"),
           "Missing 'none' from 'readConcernCounters.nonTransactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("local"),
           "Missing 'local' from 'readConcernCounters.nonTransactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("available"),
           "Missing 'available' from 'readConcernCounters.nonTransactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("majority"),
           "Missing 'majority' from 'readConcernCounters.nonTransactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("snapshot"),
           "Missing 'snapshot' from 'readConcernCounters.nonTransactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.snapshot.hasOwnProperty(
               "withClusterTime"),
           "Missing 'withClusterTime' from 'readConcernCounters.nonTransactionOps.snapshot'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.nonTransactionOps.snapshot.hasOwnProperty(
               "withoutClusterTime"),
           "Missing 'withoutClusterTime' from 'readConcernCounters.nonTransactionOps.snapshot'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(
        serverStatusResponse.readConcernCounters.nonTransactionOps.hasOwnProperty("linearizable"),
        "Missing 'linearizable' from 'readConcernCounters.nonTransactionOps'\n" +
            tojson(serverStatusResponse.readConcernCounters));

    assert(serverStatusResponse.readConcernCounters.hasOwnProperty("transactionOps"),
           "Missing 'transactionOps' from 'readConcernCounters'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.hasOwnProperty("none"),
           "Missing 'none' from 'readConcernCounters.transactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.hasOwnProperty("local"),
           "Missing 'local' from 'readConcernCounters.transactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.hasOwnProperty("majority"),
           "Missing 'majority' from 'readConcernCounters.transactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.hasOwnProperty("snapshot"),
           "Missing 'snapshot' from 'readConcernCounters.transactionOps'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.snapshot.hasOwnProperty(
               "withClusterTime"),
           "Missing 'withClusterTime' from 'readConcernCounters.transactionOps.snapshot'\n" +
               tojson(serverStatusResponse.readConcernCounters));
    assert(serverStatusResponse.readConcernCounters.transactionOps.snapshot.hasOwnProperty(
               "withoutClusterTime"),
           "Missing 'withoutClusterTime' from 'readConcernCounters.transactionOps.snapshot'\n" +
               tojson(serverStatusResponse.readConcernCounters));
}

// Verifies that the given value of the server status response is incremented in the way
// we expect.
function verifyServerStatusChange(initialStats,
                                  newStats,
                                  fields,
                                  expectedIncrement,
                                  {isTransaction = false, atClusterTime = false} = {}) {
    verifyServerStatusFields(newStats);
    initialStats = initialStats.readConcernCounters;
    newStats = newStats.readConcernCounters;
    if (!Array.isArray(fields)) {
        fields = [fields];
    }

    if (fields.length === 1 && fields[0] === "none") {
        // Implicit default RC was used.
        fields.push("noneInfo.implicitDefault.local");
    }

    fields.forEach(field => {
        let fieldPath;
        if (isTransaction) {
            fieldPath = "transactionOps";
        } else {
            fieldPath = "nonTransactionOps";
        }
        if (field === "snapshot") {
            fieldPath = fieldPath + ".snapshot";
            if (atClusterTime) {
                field = "withClusterTime";
            } else {
                field = "withoutClusterTime";
            }
        }

        fieldPath = fieldPath + "." + field;
        let pathComponents = fieldPath.split(".");
        let initialParent = initialStats;
        let newParent = newStats;
        for (let i = 0; i < pathComponents.length - 1; i++) {
            assert(initialParent.hasOwnProperty(pathComponents[i]),
                   "initialStats did not contain component " + i + " of path " + fieldPath +
                       ", initialStats: " + tojson(initialStats));
            initialParent = initialParent[pathComponents[i]];

            assert(newParent.hasOwnProperty(pathComponents[i]),
                   "newStats did not contain component " + i + " of path " + fieldPath +
                       ", newStats: " + tojson(newStats));
            newParent = newParent[pathComponents[i]];
        }

        // Test the expected increment of the changed element. The element may not exist in the
        // initial stats, in which case it is treated as 0.
        let lastPathComponent = pathComponents[pathComponents.length - 1];
        let initialValue = 0;
        if (initialParent.hasOwnProperty(lastPathComponent)) {
            initialValue = initialParent[lastPathComponent];
        }
        assert(newParent.hasOwnProperty(lastPathComponent),
               "newStats did not contain last component of path " + fieldPath +
                   ", newStats: " + tojson(newStats));
        assert.eq(initialValue + expectedIncrement,
                  newParent[lastPathComponent],
                  "expected " + fieldPath + " to increase by " + expectedIncrement +
                      ", initialStats: " + tojson(initialStats) +
                      ", newStats: " + tojson(newStats));

        // Update the value of the field to the new value so we can compare the rest of the fields
        // using assert.docEq.
        initialParent[lastPathComponent] = newParent[lastPathComponent];
    });
    assert.docEq(initialStats,
                 newStats,
                 "Expected docEq after updating " + tojson(fields) +
                     ", initialStats: " + tojson(initialStats) + ", newStats: " + tojson(newStats));
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const dbName = "test";
const collName = "server_read_concern_metrics";
const testDB = primary.getDB(dbName);
const testColl = testDB[collName];
testDB.runCommand({drop: collName});
assert.commandWorked(testDB.createCollection(collName, {writeConcern: {w: "majority"}}));
assert.commandWorked(testColl.insert({_id: 0}, {writeConcern: {w: 'majority'}}));

const sessionOptions = {
    causalConsistency: false
};
const session = testDB.getMongo().startSession(sessionOptions);
const sessionDb = session.getDatabase(dbName);
const sessionColl = sessionDb[collName];

// Run an initial transaction to get config.transactions state into memory.
session.startTransaction();
assert.eq(sessionColl.find().itcount(), 1);
assert.commandWorked(session.abortTransaction_forTesting());

function getServerStatus(conn) {
    // Don't return defaultRWConcern because it may trigger a refresh of the read write concern
    // defaults, which unexpectedly increases the opReadConcernCounters.
    return assert.commandWorked(conn.adminCommand({serverStatus: 1, defaultRWConcern: false}));
}

// Get initial serverStatus.
let serverStatus = getServerStatus(testDB);
verifyServerStatusFields(serverStatus);

// Run a command without a readConcern.
assert.eq(testColl.find().itcount(), 1);
let newStatus = getServerStatus(testDB);
verifyServerStatusChange(serverStatus, newStatus, "none", 1);
serverStatus = newStatus;

// Non-transaction reads.
for (let level of ["none", "local", "available", "snapshot", "majority", "linearizable"]) {
    jsTestLog("Testing non-transaction reads with readConcern " + level);
    let readConcern = {};
    if (level !== "none") {
        readConcern = {level: level};
    }

    assert.commandWorked(testDB.runCommand({find: collName, readConcern: readConcern}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 1);
    serverStatus = newStatus;

    assert.commandWorked(testDB.runCommand(
        {aggregate: collName, pipeline: [], cursor: {}, readConcern: readConcern}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 1);
    serverStatus = newStatus;

    assert.commandWorked(
        testDB.runCommand({distinct: collName, key: "_id", readConcern: readConcern}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 1);
    serverStatus = newStatus;

    if (level !== "snapshot") {
        assert.commandWorked(testDB.runCommand({count: collName, readConcern: readConcern}));
        newStatus = getServerStatus(testDB);
        verifyServerStatusChange(serverStatus, newStatus, level, 1);
        serverStatus = newStatus;
    }

    if (level in ["none", "local", "available"]) {
        // mapReduce only support local and available.
        assert.commandWorked(testDB.runCommand({
            mapReduce: collName,
            map: function() {
                emit(this.a, this.a);
            },
            reduce: function(key, vals) {
                return 1;
            },
            out: {inline: 1},
            readConcern: readConcern
        }));
        newStatus = getServerStatus(testDB);
        verifyServerStatusChange(serverStatus, newStatus, level, 1);
        serverStatus = newStatus;
    }

    // getMore does not count toward readConcern metrics. getMore inherits the readConcern of the
    // originating command.
    let res = assert.commandWorked(
        testDB.runCommand({find: collName, batchSize: 0, readConcern: readConcern}));
    serverStatus = getServerStatus(testDB);
    assert.commandWorked(testDB.runCommand({getMore: res.cursor.id, collection: collName}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 0);
}

// Test non-transaction snapshot with atClusterTime.
let insertTimestamp =
    assert.commandWorked(testDB.runCommand({insert: "atClusterTime", documents: [{_id: 0}]}))
        .operationTime;
jsTestLog("Testing non-transaction reads with atClusterTime");
serverStatus = getServerStatus(testDB);
assert.commandWorked(testDB.runCommand(
    {find: "atClusterTime", readConcern: {level: "snapshot", atClusterTime: insertTimestamp}}));
newStatus = getServerStatus(testDB);
verifyServerStatusChange(serverStatus, newStatus, "snapshot", 1, {atClusterTime: true});
serverStatus = newStatus;

// Transaction reads.
for (let level of ["none", "local", "snapshot", "majority"]) {
    jsTestLog("Testing transaction reads with readConcern " + level);
    if (level === "none") {
        session.startTransaction();
    } else {
        session.startTransaction({readConcern: {level: level}});
    }
    assert.eq(sessionColl.find().itcount(), 1);
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 1, {isTransaction: true});
    serverStatus = newStatus;

    // Run a second find in the same transaction. It will inherit the readConcern from the
    // transaction.
    assert.eq(sessionColl.find().itcount(), 1);
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 1, {isTransaction: true});
    serverStatus = newStatus;

    // Run an insert in the same transaction. This should not count toward the readConcern metrics.
    assert.commandWorked(
        sessionDb.runCommand({insert: "transaction", documents: [{level: level}]}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, level, 0, {isTransaction: true});
    assert.commandWorked(session.abortTransaction_forTesting());
    serverStatus = newStatus;
}

// Test transaction snapshot with atClusterTime.
insertTimestamp =
    assert.commandWorked(testDB.runCommand({insert: "atClusterTime", documents: [{_id: 1}]}))
        .operationTime;
jsTestLog("Testing transaction reads with atClusterTime");
session.startTransaction({readConcern: {level: "snapshot", atClusterTime: insertTimestamp}});
serverStatus = getServerStatus(testDB);

assert.eq(sessionColl.find().itcount(), 1);
newStatus = getServerStatus(testDB);
verifyServerStatusChange(
    serverStatus, newStatus, "snapshot", 1, {isTransaction: true, atClusterTime: true});
serverStatus = newStatus;

// Perform another read in the same transaction.
assert.eq(sessionColl.find().itcount(), 1);
newStatus = getServerStatus(testDB);
verifyServerStatusChange(
    serverStatus, newStatus, "snapshot", 1, {isTransaction: true, atClusterTime: true});
assert.commandWorked(session.abortTransaction_forTesting());

// Non-transaction CWRC reads.
for (let level of ["local", "available", "majority"]) {
    jsTestLog("Testing non-transaction reads with CWRC readConcern " + level);
    assert.commandWorked(
        primary.adminCommand({setDefaultRWConcern: 1, defaultReadConcern: {level: level}}));

    assert.commandWorked(testDB.runCommand({find: collName}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1);
    serverStatus = newStatus;

    assert.commandWorked(testDB.runCommand({aggregate: collName, pipeline: [], cursor: {}}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1);
    serverStatus = newStatus;

    assert.commandWorked(testDB.runCommand({distinct: collName, key: "_id"}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1);
    serverStatus = newStatus;

    assert.commandWorked(testDB.runCommand({count: collName}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1);
    serverStatus = newStatus;

    if (level in ["local", "available"]) {
        // mapReduce only support local and available.
        assert.commandWorked(testDB.runCommand({
            mapReduce: collName,
            map: function() {
                emit(this.a, this.a);
            },
            reduce: function(key, vals) {
                return 1;
            },
            out: {inline: 1}
        }));
        newStatus = getServerStatus(testDB);
        verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1);
        serverStatus = newStatus;
    }

    // getMore does not count toward readConcern metrics. getMore inherits the readConcern of the
    // originating command.
    let res = assert.commandWorked(testDB.runCommand({find: collName, batchSize: 0}));
    serverStatus = getServerStatus(testDB);
    assert.commandWorked(testDB.runCommand({getMore: res.cursor.id, collection: collName}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 0);
}

// Transaction CWRC reads.
for (let level of ["local", "majority"]) {
    jsTestLog("Testing transaction reads with CWRC readConcern " + level);
    assert.commandWorked(
        primary.adminCommand({setDefaultRWConcern: 1, defaultReadConcern: {level: level}}));

    session.startTransaction({readConcern: {}});

    assert.eq(sessionColl.find().itcount(), 1);
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(
        serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1, {isTransaction: true});
    serverStatus = newStatus;

    // Run a second find in the same transaction. It will inherit the readConcern from the
    // transaction.
    assert.eq(sessionColl.find().itcount(), 1);
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(
        serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 1, {isTransaction: true});
    serverStatus = newStatus;

    // Run an insert in the same transaction. This should not count toward the readConcern metrics.
    assert.commandWorked(
        sessionDb.runCommand({insert: "transaction", documents: [{level: level}]}));
    newStatus = getServerStatus(testDB);
    verifyServerStatusChange(
        serverStatus, newStatus, ["none", "noneInfo.CWRC." + level], 0, {isTransaction: true});
    assert.commandWorked(session.abortTransaction_forTesting());
    serverStatus = newStatus;
}

session.endSession();
rst.stopSet();
}());

/**
 * Tests for serverStatus metrics.stage stats.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

// In memory map of stage names to their counters. Used to verify that serverStatus is
// incrementing the appropriate stages correctly across multiple pipelines.
let countersWeExpectToIncreaseMap = {};

function checkCounters(command, countersWeExpectToIncrease, countersWeExpectNotToIncrease = []) {
    // Capture the pre-aggregation counts of the stages which we expect not to increase.
    let metrics = db.serverStatus().metrics.aggStageCounters;
    let noIncreaseCounterMap = {};
    for (const stage of countersWeExpectNotToIncrease) {
        if (!countersWeExpectToIncreaseMap[stage]) {
            countersWeExpectToIncreaseMap[stage] = 0;
        }
        noIncreaseCounterMap[stage] = metrics[stage];
    }

    // Update in memory map to reflect what each counter's count should be after running 'command'.
    for (const stage of countersWeExpectToIncrease) {
        if (!countersWeExpectToIncreaseMap[stage]) {
            countersWeExpectToIncreaseMap[stage] = 0;
        }
        countersWeExpectToIncreaseMap[stage]++;
    }

    // Run the command and update metrics to reflect the post-command serverStatus state.
    command();
    metrics = db.serverStatus().metrics.aggStageCounters;

    // Verify that serverStatus reflects expected counters.
    for (const stage of countersWeExpectToIncrease) {
        assert.eq(metrics[stage], countersWeExpectToIncreaseMap[stage]);
    }

    // Verify that the counters which we expect not to increase did not do so.
    for (const stage of countersWeExpectNotToIncrease) {
        assert.eq(metrics[stage], noIncreaseCounterMap[stage]);
    }
}

function runTests(db, coll) {
    // Reset our counter map before running any aggregations.
    countersWeExpectToIncreaseMap = {};

    // Setup for agg stages which have nested pipelines.
    assert.commandWorked(coll.insert([
        {"_id": 1, "item": "almonds", "price": 12, "quantity": 2},
        {"_id": 2, "item": "pecans", "price": 20, "quantity": 1},
        {"_id": 3}
    ]));

    assert.commandWorked(db.inventory.insert([
        {"_id": 1, "sku": "almonds", description: "product 1", "instock": 120},
        {"_id": 2, "sku": "bread", description: "product 2", "instock": 80},
        {"_id": 3, "sku": "cashews", description: "product 3", "instock": 60},
        {"_id": 4, "sku": "pecans", description: "product 4", "instock": 70},
        {"_id": 5, "sku": null, description: "Incomplete"},
        {"_id": 6}
    ]));

    // $skip
    checkCounters(() => coll.aggregate([{$skip: 5}]).toArray(), ['$skip']);
    // $project is an alias for $unset.
    checkCounters(() => coll.aggregate([{$project: {title: 1, author: 1}}]).toArray(),
                  ['$project'],
                  ['$unset']);
    // $count is an alias for $project and $group.
    checkCounters(
        () => coll.aggregate([{$count: "test"}]).toArray(), ['$count'], ['$project', '$group']);

    // $lookup
    checkCounters(
        () => coll.aggregate([{$lookup: {from: "inventory", pipeline: [{$match: {inStock: 70}}], as: "inventory_docs"}}]).toArray(),
        ['$lookup', "$match"]);

    // $merge
    checkCounters(
        () => coll.aggregate([{
                      $merge:
                          {into: coll.getName(), whenMatched: [{$set: {a: {$multiply: ["$a", 2]}}}]}
                  }])
                  .toArray(),
        ['$merge', "$set"]);

    // $facet
    checkCounters(
        () =>
            coll.aggregate([{
                    $facet: {"a": [{$match: {price: {$exists: 1}}}], "b": [{$project: {title: 1}}]}
                }])
                .toArray(),
        ['$facet', '$match', "$project"]);

    // Verify that explain ticks counters.
    checkCounters(() => coll.explain().aggregate([{$match: {a: 5}}]), ["$match"]);

    // Verify that a pipeline in an update ticks counters.
    checkCounters(() => coll.update(
                      {price: {$gte: 0}}, [{$addFields: {a: {$add: ['$a', 1]}}}], {multi: true}),
                  ["$addFields"],
                  ["$set"]);

    // Verify that a stage which appears multiple times in a pipeline has an accurate count.
    checkCounters(
        () =>
            coll.aggregate([
                    {
                        $facet:
                            {"a": [{$match: {price: {$exists: 1}}}], "b": [{$project: {title: 1}}]}
                    },
                    {
                        $facet: {
                            "c": [{$match: {instock: {$exists: 1}}}],
                            "d": [{$project: {title: 0}}]
                        }
                    }
                ])
                .toArray(),
        ["$facet", "$match", "$project", "$facet", "$match", "$project"]);

    // Verify that a pipeline used in a view ticks counters.
    const viewName = "counterView";
    assert.commandWorked(db.createView(viewName, coll.getName(), [{"$project": {_id: 0}}]));
    // Note that $project's counter will also be ticked since the $project used to generate the view
    // will be stitched together with the pipeline specified to the aggregate command.
    checkCounters(() => db[viewName].aggregate([{$match: {a: 5}}]).toArray(),
                  ["$match", "$project"]);
}

// Standalone
const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");
let db = conn.getDB(jsTest.name());
const collName = jsTest.name();
let coll = db[collName];
runTests(db, coll);

MongoRunner.stopMongod(conn);

// Sharded cluster
const st = new ShardingTest({shards: 2});
db = st.s.getDB(jsTest.name());
coll = db[collName];
st.shardColl(coll, {_id: 1}, {_id: "hashed"});

runTests(db, coll);

st.stop();
}());

/**
 * Tests that serverStatus contains a catalogStats section.
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const replSet = new ReplSetTest({nodes: 2});
replSet.startSet();
replSet.initiate();

let primary = replSet.getPrimary();
let db1 = primary.getDB('db1');
let db2 = primary.getDB('db2');

const assertCatalogStats = (db, assertFn) => {
    assertFn(db.serverStatus().catalogStats);
};

let internalCollectionsAtStart;
let internalViewsAtStart;
assertCatalogStats(db1, (stats) => {
    assert.eq(0, stats.capped);
    assert.eq(0, stats.collections);
    assert.eq(0, stats.timeseries);
    assert.eq(0, stats.views);
    internalCollectionsAtStart = stats.internalCollections;
    internalViewsAtStart = stats.internalViews;
});

assert.commandWorked(db1.coll.insert({a: 1}));
assert.commandWorked(db1.createCollection('capped', {capped: true, size: 1024}));
assert.commandWorked(db1.createCollection('view', {viewOn: 'coll', pipeline: []}));
assert.commandWorked(db1.createCollection('ts', {timeseries: {timeField: 't'}}));

assertCatalogStats(db1, (stats) => {
    assert.eq(1, stats.capped);
    assert.eq(2, stats.collections);
    assert.eq(1, stats.timeseries);
    assert.eq(1, stats.views);
    // An system.views and system.buckets collection should have been created.
    assert.eq(internalCollectionsAtStart + 2, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

// Ensure the stats stay accurate in the view catalog with a collMod.
assert.commandWorked(db1.runCommand({collMod: 'view', pipeline: [{$match: {a: 1}}]}));
assertCatalogStats(db1, (stats) => {
    assert.eq(1, stats.capped);
    assert.eq(2, stats.collections);
    assert.eq(1, stats.timeseries);
    assert.eq(1, stats.views);
    // An system.views and system.buckets collection should have been created.
    assert.eq(internalCollectionsAtStart + 2, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

assert.commandWorked(db2.coll.insert({a: 1}));
assert.commandWorked(db2.createCollection('capped', {capped: true, size: 1024}));
assert.commandWorked(db2.createCollection('view', {viewOn: 'coll', pipeline: []}));
assert.commandWorked(db2.createCollection('ts', {timeseries: {timeField: 't'}}));

assertCatalogStats(db1, (stats) => {
    assert.eq(2, stats.capped);
    assert.eq(4, stats.collections);
    assert.eq(2, stats.timeseries);
    assert.eq(2, stats.views);
    // An system.views and system.buckets collection should have been created.
    assert.eq(internalCollectionsAtStart + 4, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

replSet.stopSet(undefined, /* restart */ true);
replSet.startSet({}, /* restart */ true);
primary = replSet.getPrimary();
db1 = primary.getDB('db1');
db2 = primary.getDB('db2');

// Ensure stats are the same after restart.
assertCatalogStats(db1, (stats) => {
    assert.eq(2, stats.capped);
    assert.eq(4, stats.collections);
    assert.eq(2, stats.timeseries);
    assert.eq(2, stats.views);
    assert.eq(internalCollectionsAtStart + 4, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

assert(db1.coll.drop());
assert(db1.capped.drop());
assert(db1.view.drop());
assert(db1.ts.drop());

assertCatalogStats(db1, (stats) => {
    assert.eq(1, stats.capped);
    assert.eq(2, stats.collections);
    assert.eq(1, stats.timeseries);
    assert.eq(1, stats.views);
    // The system.views collection will stick around
    assert.eq(internalCollectionsAtStart + 3, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

db1.dropDatabase();

assertCatalogStats(db1, (stats) => {
    assert.eq(1, stats.capped);
    assert.eq(2, stats.collections);
    assert.eq(1, stats.timeseries);
    assert.eq(1, stats.views);
    // The system.views collection should be dropped
    assert.eq(internalCollectionsAtStart + 2, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

db2.dropDatabase();

assertCatalogStats(db1, (stats) => {
    assert.eq(0, stats.capped);
    assert.eq(0, stats.collections);
    assert.eq(0, stats.timeseries);
    assert.eq(0, stats.views);
    // The system.views collection should be dropped
    assert.eq(internalCollectionsAtStart, stats.internalCollections);
    assert.eq(internalViewsAtStart, stats.internalViews);
});

replSet.stopSet();
})();

/**
 * Tests for serverStatus metrics.dotsAndDollarsFields stats.
 * @tags: [
 * ]
 */
(function() {
"use strict";

const mongod = MongoRunner.runMongod();
const dbName = "dots_and_dollars_fields";
const db = mongod.getDB(dbName);
const collName = "server_status_metrics_dots_and_dollars_fields";
const coll = db[collName];
let serverStatusMetrics = db.serverStatus().metrics.dotsAndDollarsFields;

//
// Test that "metrics.dotsAndDollarsField.inserts" is being updated correctly.
//

let insertCount = 0;
function runCommandAndCheckInsertCount(cmdToRun, add) {
    assert.commandWorked(cmdToRun());

    insertCount += add;
    const dotsAndDollarsMetrics = db.serverStatus().metrics.dotsAndDollarsFields;
    assert.eq(dotsAndDollarsMetrics.inserts, insertCount, dotsAndDollarsMetrics);
}

runCommandAndCheckInsertCount(() => coll.insert({"$a.b": 1}), 1);
runCommandAndCheckInsertCount(() => coll.insert({a: {a: 1}, "$.": 1, "$second": 1, "$3rd": 1}), 1);
// Only account for top-level $-prefixed fields.
runCommandAndCheckInsertCount(() => coll.insert({a: {"$a.b": 1}}), 0);
runCommandAndCheckInsertCount(() => coll.insert({obj: {obj: {arr: [1, 2, {"$a$": 1}]}}}), 0);
// This update command is actually an upsert which inserts an object.
runCommandAndCheckInsertCount(
    () => coll.update(
        {"field-not-found": 1},
        [{$replaceWith: {$setField: {field: {$literal: "$a.b"}, input: "$$ROOT", value: 1}}}],
        {upsert: true}),
    1);
runCommandAndCheckInsertCount(
    () => coll.insertMany([{"$dollar-field": 1}, {"$dollar-field": 1}, {"$$-field": 1}]), 3);
// An upsert findAndModify command because no match document was found.
runCommandAndCheckInsertCount(() => db.runCommand({
    findAndModify: collName,
    query: {"not-found": 1},
    update: [{$replaceWith: {$literal: {_id: 2, "$a$": 3}}}],
    upsert: true,
}),
                              1);

//
// Test that "metrics.dotsAndDollarsField.updates" is being updated correctly.
//

assert(coll.drop());
assert.commandWorked(coll.insert({_id: 1}));

let updateCount = 0;

function runCommandAndCheckUpdateCount(cmdToRun, add) {
    assert.commandWorked(cmdToRun());

    updateCount += add;
    const dotsAndDollarsMetrics = db.serverStatus().metrics.dotsAndDollarsFields;
    assert.eq(dotsAndDollarsMetrics.updates, updateCount, dotsAndDollarsMetrics);
}

//
// Test pipeline-style updates.
//
runCommandAndCheckUpdateCount(() => db.runCommand({
    update: collName,
    updates: [{q: {}, u: [{$replaceWith: {$literal: {$db: 1}}}], upsert: true}]
}),
                              1);

runCommandAndCheckUpdateCount(() => db.runCommand({
    update: collName,
    updates: [
        {q: {}, u: [{$replaceWith: {$literal: {"$dollar-field": 1}}}], upsert: true},
        {
            q: {_id: 1},
            u: [{
                $replaceWith: {
                    $setField:
                        {field: {$literal: "$ab"}, input: {$literal: {"a.b": "b"}}, value: 12345}
                }
            }]
        }
    ]
}),
                              2);

// No-op because no match document found and 'upsert' is set to false, so do not tick.
runCommandAndCheckUpdateCount(() => db.runCommand({
    update: collName,
    updates: [{
        q: {"not-found": "null"},
        u: [{$replaceWith: {$literal: {"$dollar-field": 1}}}],
        upsert: false
    }]
}),
                              0);

//
// Test findAndModify command.
//
runCommandAndCheckUpdateCount(() => db.runCommand({
    findAndModify: collName,
    query: {_id: 1},
    update: {_id: 1, out: {$in: 1, "x": 2}},
    upsert: true,
}),
                              1);
runCommandAndCheckUpdateCount(() => db.runCommand({
    findAndModify: collName,
    query: {_id: 1},
    update: [{$replaceWith: {$literal: {_id: 1, "$dollar-field": 3}}}],
}),
                              1);

MongoRunner.stopMongod(mongod);
})();

/**
 * Tests that serverStatus metrics correctly print commands.hello if the user sends hello
 * and commands.isMaster if the user sends isMaster or ismaster
 */
(function() {
"use strict";
const mongod = MongoRunner.runMongod();
const dbName = "server_status_metrics_hello_command";
const db = mongod.getDB(dbName);
let serverStatusMetrics = db.serverStatus().metrics.commands;
const initialIsMasterTotal = serverStatusMetrics.isMaster.total;
const initialHelloTotal = 0;

// Running hello command.
jsTestLog("Running hello command");
assert.commandWorked(db.runCommand({hello: 1}));
serverStatusMetrics = db.serverStatus().metrics.commands;
assert.eq(
    serverStatusMetrics.hello.total, initialHelloTotal + 1, "commands.hello should increment");
assert.eq(serverStatusMetrics.isMaster.total,
          initialIsMasterTotal,
          "commands.isMaster should not increment");

// Running isMaster command.
jsTestLog("Running isMaster command");
assert.commandWorked(db.runCommand({isMaster: 1}));
serverStatusMetrics = db.serverStatus().metrics.commands;
assert.eq(
    serverStatusMetrics.hello.total, initialHelloTotal + 1, "commands.hello should not increment");
assert.eq(serverStatusMetrics.isMaster.total,
          initialIsMasterTotal + 1,
          "commands.isMaster should increment");

// Running ismaster command.
jsTestLog("Running ismaster command");
assert.commandWorked(db.runCommand({ismaster: 1}));
serverStatusMetrics = db.serverStatus().metrics.commands;
assert.eq(
    serverStatusMetrics.hello.total, initialHelloTotal + 1, "commands.hello should not increment");
assert.eq(serverStatusMetrics.isMaster.total,
          initialIsMasterTotal + 2,
          "commands.isMaster should increment");
MongoRunner.stopMongod(mongod);
})();

/**
 * Tests for serverStatus metrics.queryExecutor stats.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");
const db = conn.getDB(jsTest.name());
const coll = db[jsTest.name()];

let getCollectionScans = () => {
    return db.serverStatus().metrics.queryExecutor.collectionScans.total;
};
let getCollectionScansNonTailable = () => {
    return db.serverStatus().metrics.queryExecutor.collectionScans.nonTailable;
};

// Create and populate a capped collection so that we can run tailable queries.
const nDocs = 32;
coll.drop();
assert.commandWorked(db.createCollection(jsTest.name(), {capped: true, size: nDocs * 100}));

for (let i = 0; i < nDocs; i++) {
    assert.commandWorked(coll.insert({a: i}));
}

// Test nontailable collection scans update collectionScans counters appropriately.
for (let i = 0; i < nDocs; i++) {
    assert.eq(coll.find({a: i}).itcount(), 1);
    assert.eq(i + 1, getCollectionScans());
    assert.eq(i + 1, getCollectionScansNonTailable());
}

// Test tailable collection scans update collectionScans counters appropriately.
for (let i = 0; i < nDocs; i++) {
    assert.eq(coll.find({a: i}).tailable().itcount(), 1);
    assert.eq(nDocs + i + 1, getCollectionScans());
    assert.eq(nDocs, getCollectionScansNonTailable());
}

// Run a query which will require the client to fetch multiple batches from the server. Ensure
// that the getMore commands don't increment the counter of collection scans.
assert.eq(coll.find({}).batchSize(2).itcount(), nDocs);
assert.eq((nDocs * 2) + 1, getCollectionScans());
assert.eq(nDocs + 1, getCollectionScansNonTailable());

// Create index to test that index scans don't up the collection scan counter.
assert.commandWorked(coll.createIndex({a: 1}));
// Run a bunch of index scans.
for (let i = 0; i < nDocs; i++) {
    assert.eq(coll.find({a: i}).itcount(), 1);
}
// Assert that the number of collection scans hasn't increased.
assert.eq((nDocs * 2) + 1, getCollectionScans());
assert.eq(nDocs + 1, getCollectionScansNonTailable());

MongoRunner.stopMongod(conn);
}());

// Tests multi-document transactions metrics in the serverStatus output.
// @tags: [uses_transactions]
(function() {
"use strict";
load("jstests/libs/fail_point_util.js");  // For configureFailPoint

// Verifies that the server status response has the fields that we expect.
function verifyServerStatusFields(serverStatusResponse) {
    assert(serverStatusResponse.hasOwnProperty("transactions"),
           "Expected the serverStatus response to have a 'transactions' field\n" +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.transactions.hasOwnProperty("currentActive"),
           "The 'transactions' field in serverStatus did not have the 'currentActive' field\n" +
               tojson(serverStatusResponse.transactions));
    assert(serverStatusResponse.transactions.hasOwnProperty("currentInactive"),
           "The 'transactions' field in serverStatus did not have the 'currentInactive' field\n" +
               tojson(serverStatusResponse.transactions));
    assert(serverStatusResponse.transactions.hasOwnProperty("currentOpen"),
           "The 'transactions' field in serverStatus did not have the 'currentOpen' field\n" +
               tojson(serverStatusResponse.transactions));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalAborted"),
           "The 'transactions' field in serverStatus did not have the 'totalAborted' field\n" +
               tojson(serverStatusResponse.transactions));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalCommitted"),
           "The 'transactions' field in serverStatus did not have the 'totalCommitted' field\n" +
               tojson(serverStatusResponse.transactions));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalStarted"),
           "The 'transactions' field in serverStatus did not have the 'totalStarted' field\n" +
               tojson(serverStatusResponse.transactions));
}

// Verifies that the given value of the server status response is incremented in the way
// we expect.
function verifyServerStatusChange(initialStats, newStats, valueName, expectedIncrement) {
    assert.eq(initialStats[valueName] + expectedIncrement,
              newStats[valueName],
              "expected " + valueName + " to increase by " + expectedIncrement);
}

// Set up the replica set.
const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();

// Set up the test database.
const dbName = "test";
const collName = "server_transactions_metrics";
const testDB = primary.getDB(dbName);
const adminDB = rst.getPrimary().getDB('admin');
testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
assert.commandWorked(testDB.runCommand({create: collName, writeConcern: {w: "majority"}}));

// Start the session.
const sessionOptions = {
    causalConsistency: false
};
const session = testDB.getMongo().startSession(sessionOptions);
const sessionDb = session.getDatabase(dbName);
const sessionColl = sessionDb[collName];

// Get state of server status before the transaction.
let initialStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(initialStatus);

// This transaction will commit.
jsTest.log("Start a transaction and then commit it.");

// Compare server status after starting a transaction with the server status before.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "insert-1"}));

let newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
// Verify that the open transaction counter is incremented while inside the transaction.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 1);
// Verify that when not running an operation, the transaction is inactive.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 1);

// Compare server status after the transaction commit with the server status before.
assert.commandWorked(session.commitTransaction_forTesting());
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalStarted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalCommitted", 1);
// Verify that current open counter is decremented on commit.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 0);
// Verify that both active and inactive are 0 on commit.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 0);

// This transaction will abort.
jsTest.log("Start a transaction and then abort it.");

// Compare server status after starting a transaction with the server status before.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "insert-2"}));

newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
// Verify that the open transaction counter is incremented while inside the transaction.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 1);
// Verify that when not running an operation, the transaction is inactive.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 1);

// Compare server status after the transaction abort with the server status before.
assert.commandWorked(session.abortTransaction_forTesting());
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalStarted", 2);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalCommitted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalAborted", 1);
// Verify that current open counter is decremented on abort.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 0);
// Verify that both active and inactive are 0 on abort.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 0);

// This transaction will abort due to a duplicate key insert.
jsTest.log("Start a transaction that will abort on a duplicated key error.");

// Compare server status after starting a transaction with the server status before.
session.startTransaction();
// Inserting a new document will work fine, and the transaction starts.
assert.commandWorked(sessionColl.insert({_id: "insert-3"}));

newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
// Verify that the open transaction counter is incremented while inside the transaction.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 1);
// Verify that when not running an operation, the transaction is inactive.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 1);

// Compare server status after the transaction abort with the server status before.
// The duplicated insert will fail, causing the transaction to abort.
assert.commandFailedWithCode(sessionColl.insert({_id: "insert-3"}), ErrorCodes.DuplicateKey);
// Ensure that the transaction was aborted on failure.
assert.commandFailedWithCode(session.commitTransaction_forTesting(), ErrorCodes.NoSuchTransaction);
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalStarted", 3);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalCommitted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalAborted", 2);
// Verify that current open counter is decremented on abort caused by an error.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 0);
// Verify that both active and inactive are 0 on abort.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 0);

// Hang the transaction on a failpoint in the middle of an operation to check active and
// inactive counters while operation is running inside a transaction.
jsTest.log("Start a transaction that will hang in the middle of an operation due to a fail point.");
const fpHangDuringBatchUpdate = configureFailPoint(primary, 'hangDuringBatchUpdate');

const transactionFn = function() {
    const collName = 'server_transactions_metrics';
    const session = db.getMongo().startSession({causalConsistency: false});
    const sessionDb = session.getDatabase('test');
    const sessionColl = sessionDb[collName];

    session.startTransaction({readConcern: {level: 'snapshot'}});
    assert.commandWorked(sessionColl.update({}, {"update-1": 2}));
    assert.commandWorked(session.commitTransaction_forTesting());
};
const transactionProcess = startParallelShell(transactionFn, primary.port);

// Keep running currentOp() until we see the transaction subdocument.
assert.soon(function() {
    const transactionFilter = {
        active: true,
        'lsid': {$exists: true},
        'transaction.parameters.txnNumber': {$eq: 0}
    };
    return 1 === adminDB.aggregate([{$currentOp: {}}, {$match: transactionFilter}]).itcount();
});

jsTestLog("Wait until the operation is in the middle of executing.");
fpHangDuringBatchUpdate.wait();

newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
// Verify that the open transaction counter is incremented while inside the transaction.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 1);
// Verify that the metrics show that the transaction is active while inside the operation.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 0);

// Now the transaction can proceed.
fpHangDuringBatchUpdate.off();
transactionProcess();
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
// Verify that current open counter is decremented on commit.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentOpen", 0);
// Verify that both active and inactive are 0 after the transaction finishes.
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentActive", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentInactive", 0);

// End the session and stop the replica set.
session.endSession();
rst.stopSet();
}());

/**
 * Tests prepared transactions metrics in the serverStatus output.
 * @tags: [uses_transactions, uses_prepare_transaction]
 */
(function() {
"use strict";
load("jstests/core/txns/libs/prepare_helpers.js");
load("jstests/replsets/rslib.js");

/**
 * Verifies that the serverStatus response has the fields that we expect.
 */
function verifyServerStatusFields(serverStatusResponse) {
    assert(serverStatusResponse.hasOwnProperty("transactions"),
           "Expected the serverStatus response to have a 'transactions' field: " +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalPrepared"),
           "Expected the serverStatus response to have a 'totalPrepared' field: " +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalPreparedThenCommitted"),
           "Expected the serverStatus response to have a 'totalPreparedThenCommitted' field: " +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.transactions.hasOwnProperty("totalPreparedThenAborted"),
           "Expected the serverStatus response to have a 'totalPreparedThenAborted' field: " +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.transactions.hasOwnProperty("currentPrepared"),
           "Expected the serverStatus response to have a 'currentPrepared' field: " +
               tojson(serverStatusResponse));
}

/**
 * Verifies that the given value of the server status response is incremented in the way
 * we expect.
 */
function verifyServerStatusChange(initialStats, newStats, valueName, expectedIncrement) {
    assert.eq(initialStats[valueName] + expectedIncrement,
              newStats[valueName],
              "expected " + valueName + " to increase by " + expectedIncrement);
}

/**
 * Verifies that the timestamp of the oldest active transaction in the transactions table
 * is greater than the lower bound and less than or equal to the upper bound.
 */
function verifyOldestActiveTransactionTimestamp(testDB, lowerBound, upperBound) {
    let res = assert.commandWorked(
        testDB.getSiblingDB("config").getCollection("transactions").runCommand("find", {
            "filter": {"state": {"$in": ["prepared", "inProgress"]}},
            "sort": {"startOpTime": 1},
            "readConcern": {"level": "local"},
            "limit": 1
        }));

    let entry = res.cursor.firstBatch[0];
    assert.neq(undefined, entry);

    assert.lt(lowerBound,
              entry.startOpTime.ts,
              "oldest active transaction timestamp should be greater than the lower bound");
    assert.lte(
        entry.startOpTime.ts,
        upperBound,
        "oldest active transaction timestamp should be less than or equal to the upper bound");
}

// Set up the replica set.
const rst = new ReplSetTest({nodes: 1});

rst.startSet();
rst.initiate();
const primary = rst.getPrimary();

// Set up the test database.
const dbName = "test";
const collName = "server_transactions_metrics_for_prepared_transactions";
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

// Get state of server status before the transaction.
const initialStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(initialStatus);

// Test server metrics for a prepared transaction that is committed.
jsTest.log("Prepare a transaction and then commit it");

const doc1 = {
    _id: 1,
    x: 1
};

// Start transaction and prepare transaction.
session.startTransaction();
assert.commandWorked(sessionColl.insert(doc1));

const opTimeBeforePrepareForCommit = getLastOpTime(primary);
const prepareTimestampForCommit = PrepareHelpers.prepareTransaction(session);

// Verify the total and current prepared transaction counter is updated and the oldest active
// oplog entry timestamp is shown.
let newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalPrepared", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentPrepared", 1);

// Verify that the prepare entry has the oldest timestamp of any active transaction
// in the transactions table.
verifyOldestActiveTransactionTimestamp(
    testDB, opTimeBeforePrepareForCommit.ts, prepareTimestampForCommit);

// Verify the total prepared and committed transaction counters are updated after a commit
// and that the current prepared counter is decremented.
PrepareHelpers.commitTransaction(session, prepareTimestampForCommit);
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(
    initialStatus.transactions, newStatus.transactions, "totalPreparedThenCommitted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentPrepared", 0);

// Verify that other prepared transaction metrics have not changed.
verifyServerStatusChange(
    initialStatus.transactions, newStatus.transactions, "totalPreparedThenAborted", 0);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalPrepared", 1);

// Test server metrics for a prepared transaction that is aborted.
jsTest.log("Prepare a transaction and then abort it");

const doc2 = {
    _id: 2,
    x: 2
};

// Start transaction and prepare transaction.
session.startTransaction();
assert.commandWorked(sessionColl.insert(doc2));

const opTimeBeforePrepareForAbort = getLastOpTime(primary);
const prepareTimestampForAbort = PrepareHelpers.prepareTransaction(session);

// Verify that the total and current prepared counter is updated and the oldest active oplog
// entry timestamp is shown.
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalPrepared", 2);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentPrepared", 1);

// Verify that the prepare entry has the oldest timestamp of any active transaction
// in the transactions table.
verifyOldestActiveTransactionTimestamp(
    testDB, opTimeBeforePrepareForAbort.ts, prepareTimestampForAbort);

// Verify the total prepared and aborted transaction counters are updated after an abort and the
// current prepared counter is decremented.
assert.commandWorked(session.abortTransaction_forTesting());
newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
verifyServerStatusFields(newStatus);
verifyServerStatusChange(
    initialStatus.transactions, newStatus.transactions, "totalPreparedThenAborted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "currentPrepared", 0);

// Verify that other prepared transaction metrics have not changed.
verifyServerStatusChange(
    initialStatus.transactions, newStatus.transactions, "totalPreparedThenCommitted", 1);
verifyServerStatusChange(initialStatus.transactions, newStatus.transactions, "totalPrepared", 2);

// End the session and stop the replica set.
session.endSession();
rst.stopSet();
}());

// Tests multi-document transactions metrics are still correct after 'killSessions'.
// @tags: [uses_transactions]
(function() {
"use strict";

// Verifies that the given value of the transaction metrics is incremented in the way we expect.
function verifyMetricsChange(initialStats, newStats, valueName, expectedIncrement) {
    assert.eq(initialStats[valueName] + expectedIncrement,
              newStats[valueName],
              "expected " + valueName + " to increase by " + expectedIncrement +
                  ".\nInitial stats: " + tojson(initialStats) + "; New stats: " + tojson(newStats));
}

// Set up the replica set and enable majority read concern for atClusterTime snapshot reads.
const rst = new ReplSetTest({nodes: 1, nodeOptions: {enableMajorityReadConcern: "true"}});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "server_transactions_metrics_kill_sessions";
const testDB = rst.getPrimary().getDB(dbName);
assert.commandWorked(testDB.runCommand({create: collName, writeConcern: {w: "majority"}}));

const sessionOptions = {
    causalConsistency: false
};
let session = testDB.getMongo().startSession(sessionOptions);
let sessionDb = session.getDatabase(dbName);

let initialMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;

jsTest.log("Start a transaction.");
session.startTransaction();
assert.commandWorked(sessionDb.runCommand({find: collName}));

let newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 1);
verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 1);

jsTest.log("Kill session " + tojson(session.getSessionId()) + ".");
assert.commandWorked(testDB.runCommand({killSessions: [session.getSessionId()]}));

newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 0);
verifyMetricsChange(initialMetrics, newMetrics, "totalCommitted", 0);
verifyMetricsChange(initialMetrics, newMetrics, "totalAborted", 1);
verifyMetricsChange(initialMetrics, newMetrics, "totalStarted", 1);

session.endSession();

session = testDB.getMongo().startSession(sessionOptions);
sessionDb = session.getDatabase(dbName);

jsTest.log("Start a snapshot transaction at a time that is too old.");
session.startTransaction({readConcern: {level: "snapshot", atClusterTime: Timestamp(1, 1)}});
// Operation runs unstashTransactionResources() and throws prior to onUnstash(). As a result,
// the transaction will be implicitly aborted.
assert.commandFailedWithCode(sessionDb.runCommand({find: collName}), ErrorCodes.SnapshotTooOld);

newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 0);

// Kill the session that threw exception before.
jsTest.log("Kill session " + tojson(session.getSessionId()) + ".");
assert.commandWorked(testDB.runCommand({killSessions: [session.getSessionId()]}));

newMetrics = assert.commandWorked(testDB.adminCommand({serverStatus: 1})).transactions;
verifyMetricsChange(initialMetrics, newMetrics, "currentActive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentInactive", 0);
verifyMetricsChange(initialMetrics, newMetrics, "currentOpen", 0);
verifyMetricsChange(initialMetrics, newMetrics, "totalCommitted", 0);
verifyMetricsChange(initialMetrics, newMetrics, "totalAborted", 2);
verifyMetricsChange(initialMetrics, newMetrics, "totalStarted", 2);

session.endSession();

rst.stopSet();
}());

// Tests the 'lastCommittedTransaction' serverStatus section.
// @tags: [uses_transactions, uses_prepare_transaction]
(function() {
"use strict";
load("jstests/core/txns/libs/prepare_helpers.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
assert.commandWorked(primary.adminCommand({setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}}));
let newDefaultWC = {"w": 1, "wtimeout": 0, "provenance": "customDefault"};

const dbName = "test";
const collName = "coll";

const session = primary.getDB(dbName).getMongo().startSession();
const sessionDb = session.getDatabase(dbName);
const sessionColl = sessionDb[collName];
assert.commandWorked(sessionDb.runCommand({create: collName}));

function checkLastCommittedTransaction(operationCount, writeConcern) {
    let res = assert.commandWorked(primary.adminCommand({serverStatus: 1}));
    assert(res.hasOwnProperty("transactions"), () => tojson(res));
    assert(res.transactions.hasOwnProperty("lastCommittedTransaction"),
           () => tojson(res.transactions));
    assert.eq(operationCount,
              res.transactions.lastCommittedTransaction.operationCount,
              () => tojson(res.transactions));
    if (operationCount === 0) {
        assert.eq(0,
                  res.transactions.lastCommittedTransaction.oplogOperationBytes,
                  () => tojson(res.transactions));
    } else {
        assert.lt(0,
                  res.transactions.lastCommittedTransaction.oplogOperationBytes,
                  () => tojson(res.transactions));
    }
    assert.docEq(writeConcern,
                 res.transactions.lastCommittedTransaction.writeConcern,
                 () => tojson(res.transactions));
}

// Initially the 'lastCommittedTransaction' section is not present.
let res = assert.commandWorked(primary.adminCommand({serverStatus: 1}));
assert(res.hasOwnProperty("transactions"), () => tojson(res));
assert(!res.transactions.hasOwnProperty("lastCommittedTransaction"), () => tojson(res));

// Start a transaction. The 'lastCommittedTransaction' section is not yet updated.
session.startTransaction();
assert.commandWorked(sessionColl.insert({}));
res = assert.commandWorked(primary.adminCommand({serverStatus: 1}));
assert(res.hasOwnProperty("transactions"), () => tojson(res));
assert(!res.transactions.hasOwnProperty("lastCommittedTransaction"), () => tojson(res));

// Prepare the transaction. The 'lastCommittedTransaction' section is not yet updated.
let prepareTimestampForCommit = PrepareHelpers.prepareTransaction(session);
res = assert.commandWorked(primary.adminCommand({serverStatus: 1}));
assert(res.hasOwnProperty("transactions"), () => tojson(res));
assert(!res.transactions.hasOwnProperty("lastCommittedTransaction"), () => tojson(res));

// Commit the transaction. The 'lastCommittedTransaction' section should be updated.
assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestampForCommit));
checkLastCommittedTransaction(1, newDefaultWC);

// Check that we are able to exclude 'lastCommittedTransaction'. FTDC uses this to filter out
// the section as it frequently triggers scheme changes.
let filteredRes = assert.commandWorked(
    primary.adminCommand({serverStatus: 1, transactions: {includeLastCommitted: false}}));
assert(!filteredRes.transactions.hasOwnProperty("lastCommittedTransaction"),
       () => tojson(filteredRes));

function runTests(prepare) {
    jsTestLog("Testing server transaction metrics with prepare=" + prepare);

    function commitTransaction() {
        if (prepare) {
            prepareTimestampForCommit = PrepareHelpers.prepareTransaction(session);
            assert.commandWorked(
                PrepareHelpers.commitTransaction(session, prepareTimestampForCommit));
        } else {
            assert.commandWorked(session.commitTransaction_forTesting());
        }
    }

    // Run a transaction with multiple write operations.
    session.startTransaction();
    assert.commandWorked(sessionColl.insert({}));
    assert.commandWorked(sessionColl.insert({}));
    commitTransaction();
    checkLastCommittedTransaction(2, newDefaultWC);

    // Run a read-only transaction.
    session.startTransaction();
    sessionColl.findOne();
    commitTransaction();
    checkLastCommittedTransaction(0, newDefaultWC);

    // Run a transaction with non-default writeConcern.
    session.startTransaction({writeConcern: {w: 1}});
    assert.commandWorked(sessionColl.insert({}));
    commitTransaction();
    checkLastCommittedTransaction(1, {w: 1, wtimeout: 0, provenance: "clientSupplied"});

    // Run a read-only transaction with non-default writeConcern.
    session.startTransaction({writeConcern: {w: "majority"}});
    sessionColl.findOne();
    commitTransaction();
    checkLastCommittedTransaction(0, {w: "majority", wtimeout: 0, provenance: "clientSupplied"});
}

runTests(true /*prepare*/);
runTests(false /*prepare*/);

session.endSession();
rst.stopSet();
}());

// Test that transactions run on secondaries do not change the serverStatus transaction metrics.
// @tags: [
//   uses_transactions,
// ]
(function() {
"use strict";

TestData.enableTestCommands = false;
TestData.authenticationDatabase = "local";

const dbName = "test";
const collName = "server_transaction_metrics_secondary";

// Start up the replica set. We want a stable topology, so make the secondary unelectable.
const replTest = new ReplSetTest({name: collName, nodes: 2});
replTest.startSet();
let config = replTest.getReplSetConfig();
config.members[1].priority = 0;
replTest.initiate(config);

const primary = replTest.getPrimary();
const secondary = replTest.getSecondary();

// Set secondaryOk=true so that normal read commands would be allowed on the secondary.
secondary.setSecondaryOk();

// Create a test collection that we can run commands against.
assert.commandWorked(primary.getDB(dbName)[collName].insert({_id: 0}));
replTest.awaitLastOpCommitted();

// Initiate a session on the secondary.
const sessionOptions = {
    causalConsistency: false
};
const secondarySession = secondary.getDB(dbName).getMongo().startSession(sessionOptions);
let secDb = secondarySession.getDatabase(dbName);
let metrics;

jsTestLog("Trying to start transaction on secondary.");
secondarySession.startTransaction();

// Initially there are no transactions in the system.
metrics = assert.commandWorked(secondary.adminCommand({serverStatus: 1, repl: 0, metrics: 0}))
              .transactions;
assert.eq(0, metrics.currentActive);
assert.eq(0, metrics.currentInactive);
assert.eq(0, metrics.currentOpen);
assert.eq(0, metrics.totalAborted);
assert.eq(0, metrics.totalCommitted);
assert.eq(0, metrics.totalStarted);

jsTestLog("Run transaction statement.");
assert.eq(assert.throws(() => secDb[collName].findOne({_id: 0})).code,
                       ErrorCodes.NotWritablePrimary);

// The metrics are not affected.
metrics = assert.commandWorked(secondary.adminCommand({serverStatus: 1, repl: 0, metrics: 0}))
              .transactions;
assert.eq(0, metrics.currentActive);
assert.eq(0, metrics.currentInactive);
assert.eq(0, metrics.currentOpen);
assert.eq(0, metrics.totalAborted);
assert.eq(0, metrics.totalCommitted);
assert.eq(0, metrics.totalStarted);

jsTestLog("Abort the transaction.");
assert.commandFailedWithCode(secondarySession.abortTransaction_forTesting(),
                             ErrorCodes.NotWritablePrimary);

// The metrics are not affected.
metrics = assert.commandWorked(secondary.adminCommand({serverStatus: 1, repl: 0, metrics: 0}))
              .transactions;
assert.eq(0, metrics.currentActive);
assert.eq(0, metrics.currentInactive);
assert.eq(0, metrics.currentOpen);
assert.eq(0, metrics.totalAborted);
assert.eq(0, metrics.totalCommitted);
assert.eq(0, metrics.totalStarted);

jsTestLog("Done trying transaction on secondary.");
secondarySession.endSession();

replTest.stopSet();
}());

// Tests writeConcern metrics in the serverStatus output.
// @tags: [
//   requires_journaling,
//   requires_persistence,
//   requires_replication,
// ]
(function() {
"use strict";

// Verifies that the server status response has the fields that we expect.
function verifyServerStatusFields(serverStatusResponse) {
    assert(serverStatusResponse.hasOwnProperty("opWriteConcernCounters"),
           "Expected the serverStatus response to have a 'opWriteConcernCounters' field\n" +
               tojson(serverStatusResponse));
    assert(serverStatusResponse.opWriteConcernCounters.hasOwnProperty("insert"),
           "The 'opWriteConcernCounters' field in serverStatus did not have the 'insert' field\n" +
               tojson(serverStatusResponse.opWriteConcernCounters));
    assert(serverStatusResponse.opWriteConcernCounters.hasOwnProperty("update"),
           "The 'opWriteConcernCounters' field in serverStatus did not have the 'update' field\n" +
               tojson(serverStatusResponse.opWriteConcernCounters));
    assert(serverStatusResponse.opWriteConcernCounters.hasOwnProperty("delete"),
           "The 'opWriteConcernCounters' field in serverStatus did not have the 'delete' field\n" +
               tojson(serverStatusResponse.opWriteConcernCounters));
}

// Verifies that the given path of the server status response is incremented in the way we
// expect, and no other changes occurred. This function modifies its inputs.
function verifyServerStatusChange(initialStats, newStats, paths, expectedIncrement) {
    paths.forEach(path => {
        // Traverse to the parent of the changed element.
        let pathComponents = path.split(".");
        let initialParent = initialStats;
        let newParent = newStats;
        for (let i = 0; i < pathComponents.length - 1; i++) {
            assert(initialParent.hasOwnProperty(pathComponents[i]),
                   "initialStats did not contain component " + i + " of path " + path +
                       ", initialStats: " + tojson(initialStats));
            initialParent = initialParent[pathComponents[i]];

            assert(newParent.hasOwnProperty(pathComponents[i]),
                   "newStats did not contain component " + i + " of path " + path +
                       ", newStats: " + tojson(newStats));
            newParent = newParent[pathComponents[i]];
        }

        // Test the expected increment of the changed element. The element may not exist in the
        // initial stats, in which case it is treated as 0.
        let lastPathComponent = pathComponents[pathComponents.length - 1];
        let initialValue = 0;
        if (initialParent.hasOwnProperty(lastPathComponent)) {
            initialValue = initialParent[lastPathComponent];
        }
        assert(newParent.hasOwnProperty(lastPathComponent),
               "newStats did not contain last component of path " + path +
                   ", newStats: " + tojson(newStats));
        assert.eq(initialValue + expectedIncrement,
                  newParent[lastPathComponent],
                  "expected " + path + " to increase by " + expectedIncrement + ", initialStats: " +
                      tojson(initialStats) + ", newStats: " + tojson(newStats));

        // Delete the changed element.
        delete initialParent[lastPathComponent];
        delete newParent[lastPathComponent];
    });

    // The stats objects should be equal without the changed element.
    assert.eq(0,
              bsonWoCompare(initialStats, newStats),
              "expected initialStats and newStats to be equal after removing " + tojson(paths) +
                  ", initialStats: " + tojson(initialStats) + ", newStats: " + tojson(newStats));
}

let rst;
let primary;
let secondary;
const dbName = "test";
const collName = "server_write_concern_metrics";
let testDB;
let testColl;

function initializeReplicaSet(isPSASet) {
    let replSetNodes = [{}, {}];
    if (isPSASet) {
        replSetNodes.push({arbiter: true});
    }
    rst = new ReplSetTest({
        nodes: replSetNodes,
        nodeOptions: {setParameter: 'reportOpWriteConcernCountersInServerStatus=true'}
    });
    rst.startSet();
    let config = rst.getReplSetConfig();
    config.members[1].priority = 0;
    config.members[0].tags = {dc_va: "rack1"};
    config.settings = {getLastErrorModes: {myTag: {dc_va: 1}}};
    rst.initiate(config);
    primary = rst.getPrimary();
    secondary = rst.getSecondary();
    testDB = primary.getDB(dbName);
    testColl = testDB[collName];
}

function resetCollection(setupCommand) {
    testColl.drop();
    assert.commandWorked(testDB.createCollection(collName));
    if (setupCommand) {
        assert.commandWorked(testDB.runCommand(setupCommand));
    }
}

function testWriteConcernMetrics(cmd, opName, inc, isPSASet, setupCommand) {
    initializeReplicaSet(isPSASet);

    // Run command with no writeConcern and no CWWC set.
    resetCollection(setupCommand);
    let serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    let newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [
                                 opName +
                                     (isPSASet ? ".noneInfo.implicitDefault.wnum.1"
                                               : ".noneInfo.implicitDefault.wmajority"),
                                 opName + ".none"
                             ],
                             inc);

    // Run command with no writeConcern with CWWC set to majority.
    resetCollection(setupCommand);
    assert.commandWorked(primary.adminCommand({
        setDefaultRWConcern: 1,
        defaultWriteConcern: {w: "majority"},
        writeConcern: {w: "majority"}
    }));
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".noneInfo.CWWC.wmajority", opName + ".none"],
                             inc);

    // Run command with no writeConcern with CWWC set to w:1.
    resetCollection(setupCommand);
    assert.commandWorked(primary.adminCommand(
        {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".noneInfo.CWWC.wnum.1", opName + ".none"],
                             inc);

    // Run command with no writeConcern and with CWWC set to j:true.
    resetCollection(setupCommand);
    assert.commandWorked(primary.adminCommand(
        {setDefaultRWConcern: 1, defaultWriteConcern: {j: true}, writeConcern: {w: "majority"}}));
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".noneInfo.CWWC.wnum.1", opName + ".none"],
                             inc);

    // Run command with no writeConcern and with CWWC set with (w: "myTag").
    resetCollection(setupCommand);
    assert.commandWorked(primary.adminCommand({
        setDefaultRWConcern: 1,
        defaultWriteConcern: {w: "myTag"},
        writeConcern: {w: "majority"}
    }));
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".noneInfo.CWWC.wtag.myTag", opName + ".none"],
                             inc);

    // Run command with writeConcern {j: true}. This should be counted as having no 'w' value.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {j: true}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".noneInfo.implicitDefault.wnum.1", opName + ".none"],
                             inc);

    // Run command with writeConcern {w: "majority"}.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {w: "majority"}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".wmajority"],
                             inc);

    // Run command with writeConcern {w: 0}.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {w: 0}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".wnum.0"],
                             inc);

    // Run command with writeConcern {w: 1}.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {w: 1}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".wnum.1"],
                             inc);

    // Run command with writeConcern {w: 2}.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {w: 2}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".wnum.2"],
                             inc);

    // Run command with writeConcern {w: "myTag"}.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(
        testDB.runCommand(Object.assign(Object.assign({}, cmd), {writeConcern: {w: "myTag"}})));
    newStatus = assert.commandWorked(testDB.adminCommand({serverStatus: 1}));
    verifyServerStatusChange(serverStatus.opWriteConcernCounters,
                             newStatus.opWriteConcernCounters,
                             [opName + ".wtag.myTag"],
                             inc);

    // writeConcern metrics are not tracked on the secondary.
    resetCollection(setupCommand);
    serverStatus = assert.commandWorked(secondary.adminCommand({serverStatus: 1}));
    verifyServerStatusFields(serverStatus);
    assert.commandWorked(testDB.runCommand(cmd));
    newStatus = assert.commandWorked(secondary.adminCommand({serverStatus: 1}));
    assert.eq(0,
              bsonWoCompare(serverStatus.opWriteConcernCounters, newStatus.opWriteConcernCounters),
              "expected no change in secondary writeConcern metrics, before: " +
                  tojson(serverStatus) + ", after: " + tojson(newStatus));

    rst.stopSet();
}

for (const isPSASet of [true, false]) {
    // Test single insert/update/delete.
    testWriteConcernMetrics({insert: collName, documents: [{}]}, "insert", 1, isPSASet);
    testWriteConcernMetrics(
        {update: collName, updates: [{q: {}, u: {$set: {a: 1}}}]}, "update", 1, isPSASet);
    testWriteConcernMetrics(
        {delete: collName, deletes: [{q: {}, limit: 1}]}, "delete", 1, isPSASet);

    // Test batch writes.
    testWriteConcernMetrics({insert: collName, documents: [{}, {}]}, "insert", 2, isPSASet);
    testWriteConcernMetrics(
        {update: collName, updates: [{q: {}, u: {$set: {a: 1}}}, {q: {}, u: {$set: {a: 1}}}]},
        "update",
        2,
        isPSASet);
    testWriteConcernMetrics(
        {delete: collName, deletes: [{q: {}, limit: 1}, {q: {}, limit: 1}]}, "delete", 2, isPSASet);

    // Test applyOps.  All sequences of setup + command must be idempotent in steady-state oplog
    // application, as testWriteConcernMetrics will run them multiple times.
    testWriteConcernMetrics(
        {applyOps: [{op: "i", ns: testColl.getFullName(), o: {_id: 0}}]}, "insert", 1, isPSASet);
    testWriteConcernMetrics(
        {applyOps: [{op: "u", ns: testColl.getFullName(), o2: {_id: 0}, o: {$set: {a: 1}}}]},
        "update",
        1,
        isPSASet);
    testWriteConcernMetrics({applyOps: [{op: "d", ns: testColl.getFullName(), o: {_id: 0}}]},
                            "delete",
                            1,
                            isPSASet,
                            {insert: collName, documents: [{_id: 0}]});
}
}());

/**
 * Assert that arbiters periodically clear out their logical session cache. Inability to do so
 * prohibits new clients from connecting.
 *
 * @tags: [requires_replication]
 */
(function() {
const name = "server54064";
const replSet = new ReplSetTest({
    name: name,
    nodes: 2,
    // `disableLogicalSessionCacheRefresh` is true by default, but is disabled for testing purposes
    // in servers spawned from the shell.
    nodeOptions: {
        setParameter: {
            // Each refresh on an arbiter will empty the cache.
            logicalSessionRefreshMillis: 10000,
            // The number of connections/sessions before the cache is full, prohibiting new clients
            // from connecting.
            maxSessions: 3,
            disableLogicalSessionCacheRefresh: false
        }
    }
});
const nodes = replSet.nodeList();

replSet.startSet();
replSet.initiate({
    _id: name,
    members: [{_id: 0, host: nodes[0]}, {_id: 1, host: nodes[1], arbiterOnly: true}],
});

let arbConn = replSet.getArbiter();
assert.soon(() => {
    // Connect to mongo in a tight loop to exhaust the number of available logical sessions in the
    // cache.
    const result = new Mongo(arbConn.host).adminCommand({hello: 1});
    if (result['ok'] === 0) {
        assert.commandFailedWithCode(result, ErrorCodes.TooManyLogicalSessions);
        return true;
    }
    return false;
});

assert.soon(() => {
    // Once we observe that the number of sessions is maxed out, loop again to confirm that the
    // cache eventually does get cleared.
    const result = new Mongo(arbConn.host).adminCommand({hello: 1});
    if (result['ok'] === 0) {
        assert.commandFailedWithCode(result, ErrorCodes.TooManyLogicalSessions);
        return false;
    }
    return true;
});

replSet.stopSet();
})();

/**
 * Explicit shell session should prohibit w: 0 writes.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();
const session = conn.startSession();
const sessionColl = session.getDatabase("test").getCollection("foo");
const err = assert.throws(() => {
    sessionColl.insert({x: 1}, {writeConcern: {w: 0}});
});

assert.includes(
    err.toString(), "Unacknowledged writes are prohibited with sessions", "wrong error message");

session.endSession();
MongoRunner.stopMongod(conn);
})();

load('jstests/libs/sessions_collection.js');

(function() {
"use strict";

// This test makes assertions about the number of sessions, which are not compatible with
// implicit sessions.
TestData.disableImplicitSessions = true;

let timeoutMinutes = 5;

var startSession = {startSession: 1};
var conn =
    MongoRunner.runMongod({setParameter: "localLogicalSessionTimeoutMinutes=" + timeoutMinutes});

var admin = conn.getDB("admin");
var config = conn.getDB("config");

// Test that we can use sessions before the sessions collection exists.
{
    validateSessionsCollection(conn, false, false, timeoutMinutes);
    assert.commandWorked(admin.runCommand({startSession: 1}));
    validateSessionsCollection(conn, false, false, timeoutMinutes);
}

// Test that a refresh will create the sessions collection.
{
    assert.commandWorked(admin.runCommand({refreshLogicalSessionCacheNow: 1}));
    validateSessionsCollection(conn, true, true, timeoutMinutes);
}

// Test that a refresh will (re)create the TTL index on the sessions collection.
{
    assert.commandWorked(config.system.sessions.dropIndex({lastUse: 1}));
    validateSessionsCollection(conn, true, false, timeoutMinutes);
    assert.commandWorked(admin.runCommand({refreshLogicalSessionCacheNow: 1}));
    validateSessionsCollection(conn, true, true, timeoutMinutes);
}

MongoRunner.stopMongod(conn);

timeoutMinutes = 4;
conn = MongoRunner.runMongod({
    restart: conn,
    cleanData: false,
    setParameter: "localLogicalSessionTimeoutMinutes=" + timeoutMinutes
});
admin = conn.getDB("admin");
config = conn.getDB("config");

// Test that a change to the TTL index expiration on restart will generate a collMod to change
// the expiration time.
{
    assert.commandWorked(admin.runCommand({refreshLogicalSessionCacheNow: 1}));
    validateSessionsCollection(conn, true, true, timeoutMinutes);
}

MongoRunner.stopMongod(conn);
})();

load("jstests/libs/parallelTester.js");

/**
 * @tags: [requires_replication, requires_sharding, sets_replica_set_matching_strategy]
 */

(function() {
"use strict";

const kDbName = 'test';

const minConns = 4;
var stepParams = {
    ShardingTaskExecutorPoolMinSize: minConns,
    ShardingTaskExecutorPoolMaxSize: 10,
    ShardingTaskExecutorPoolMaxConnecting: 5,
    ShardingTaskExecutorPoolHostTimeoutMS: 300000,
    ShardingTaskExecutorPoolRefreshRequirementMS: 60000,
    ShardingTaskExecutorPoolRefreshTimeoutMS: 20000,
    ShardingTaskExecutorPoolReplicaSetMatching: "disabled",
};

const st = new ShardingTest({
    config: {nodes: 1},
    shards: 1,
    rs0: {nodes: 1},
    mongos: [{setParameter: stepParams}],
});
const mongos = st.s0;
const rst = st.rs0;
const primary = rst.getPrimary();

const cfg = primary.getDB('local').system.replset.findOne();
const allHosts = cfg.members.map(x => x.host);
const mongosDB = mongos.getDB(kDbName);
const primaryOnly = [primary.name];

function configureReplSetFailpoint(name, modeValue) {
    st.rs0.nodes.forEach(function(node) {
        assert.commandWorked(node.getDB("admin").runCommand({
            configureFailPoint: name,
            mode: modeValue,
            data: {
                shouldCheckForInterrupt: true,
                nss: kDbName + ".test",
            },
        }));
    });
}

var threads = [];
function launchFinds({times, readPref, shouldFail}) {
    jsTestLog("Starting " + times + " connections");
    for (var i = 0; i < times; i++) {
        var thread = new Thread(function(connStr, readPref, dbName, shouldFail) {
            var client = new Mongo(connStr);
            const ret = client.getDB(dbName).runCommand(
                {find: "test", limit: 1, "$readPreference": {mode: readPref}});

            if (shouldFail) {
                assert.commandFailed(ret);
            } else {
                assert.commandWorked(ret);
            }
        }, st.s.host, readPref, kDbName, shouldFail);
        thread.start();
        threads.push(thread);
    }
}

var currentCheckNum = 0;
function hasConnPoolStats(args) {
    const checkNum = currentCheckNum++;
    jsTestLog("Check #" + checkNum + ": " + tojson(args));
    var {ready, pending, active, hosts, isAbsent, checkStatsFunc} = args;

    ready = ready ? ready : 0;
    pending = pending ? pending : 0;
    active = active ? active : 0;
    hosts = hosts ? hosts : allHosts;
    checkStatsFunc = checkStatsFunc ? checkStatsFunc : function(stats) {
        return stats.available == ready && stats.refreshing == pending && stats.inUse == active;
    };

    function checkStats(res, host) {
        var stats = res.hosts[host];
        if (!stats) {
            jsTestLog("Connection stats for " + host + " are absent");
            return isAbsent;
        }

        jsTestLog("Connection stats for " + host + ": " + tojson(stats));
        return checkStatsFunc(stats);
    }

    function checkAllStats() {
        var res = mongos.adminCommand({connPoolStats: 1});
        return hosts.map(host => checkStats(res, host)).every(x => x);
    }

    assert.soon(checkAllStats, "Check #" + checkNum + " failed", 10000);

    jsTestLog("Check #" + checkNum + " successful");
}

function updateSetParameters(params) {
    var cmd = Object.assign({"setParameter": 1}, params);
    assert.commandWorked(mongos.adminCommand(cmd));
}

function dropConnections() {
    assert.commandWorked(mongos.adminCommand({dropConnections: 1, hostAndPort: allHosts}));
}

function resetPools() {
    dropConnections();
    mongos.adminCommand({multicast: {ping: 0}});
    hasConnPoolStats({ready: 4});
}

function runSubTest(name, fun) {
    jsTestLog("Running test for " + name);

    resetPools();

    fun();

    updateSetParameters(stepParams);
}

assert.commandWorked(mongosDB.test.insert({x: 1}));
assert.commandWorked(mongosDB.test.insert({x: 2}));
assert.commandWorked(mongosDB.test.insert({x: 3}));
st.rs0.awaitReplication();

runSubTest("MinSize", function() {
    dropConnections();

    // Launch an initial find to trigger to min
    launchFinds({times: 1, readPref: "primary"});
    hasConnPoolStats({ready: minConns});

    // Increase by one
    updateSetParameters({ShardingTaskExecutorPoolMinSize: 5});
    hasConnPoolStats({ready: 5});

    // Increase to MaxSize
    updateSetParameters({ShardingTaskExecutorPoolMinSize: 10});
    hasConnPoolStats({ready: 10});

    // Decrease to zero
    updateSetParameters({ShardingTaskExecutorPoolMinSize: 0});
});

runSubTest("MaxSize", function() {
    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "alwaysOn");
    dropConnections();

    // Launch 10 blocked finds
    launchFinds({times: 10, readPref: "primary"});
    hasConnPoolStats({active: 10, hosts: primaryOnly});

    // Increase by 5 and Launch another 4 blocked finds
    updateSetParameters({ShardingTaskExecutorPoolMaxSize: 15});
    launchFinds({times: 4, readPref: "primary"});
    hasConnPoolStats({active: 14, hosts: primaryOnly});

    // Launch yet another 2, these should add only 1 connection
    launchFinds({times: 2, readPref: "primary"});
    hasConnPoolStats({active: 15, hosts: primaryOnly});

    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "off");
    hasConnPoolStats({ready: 15, pending: 0, hosts: primaryOnly});
});

// Test maxConnecting
runSubTest("MaxConnecting", function() {
    const maxPending1 = 2;
    const maxPending2 = 4;
    const conns = 6;

    updateSetParameters({
        ShardingTaskExecutorPoolMaxSize: 100,
        ShardingTaskExecutorPoolMaxConnecting: maxPending1,
    });

    configureReplSetFailpoint("waitInHello", "alwaysOn");
    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "alwaysOn");
    dropConnections();

    // Go to the limit of maxConnecting, so we're stuck here
    launchFinds({times: maxPending1, readPref: "primary"});
    hasConnPoolStats({pending: maxPending1});

    // More won't run right now
    launchFinds({times: conns - maxPending1, readPref: "primary"});
    hasConnPoolStats({pending: maxPending1});

    // If we increase our limit, it should fill in some of the connections
    updateSetParameters({ShardingTaskExecutorPoolMaxConnecting: maxPending2});
    hasConnPoolStats({pending: maxPending2});

    // Dropping the limit doesn't cause us to drop pending
    updateSetParameters({ShardingTaskExecutorPoolMaxConnecting: maxPending1});
    hasConnPoolStats({pending: maxPending2});

    // Release our pending and walk away
    configureReplSetFailpoint("waitInHello", "off");
    hasConnPoolStats({
        // Expects the number of pending connections to be zero.
        checkStatsFunc: function(stats) {
            return stats.refreshing == 0;
        }
    });
    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "off");
});

runSubTest("Timeouts", function() {
    const conns = minConns;
    const pendingTimeoutMS = 5000;
    const toRefreshTimeoutMS = 1000;
    const idleTimeoutMS1 = 20000;
    const idleTimeoutMS2 = 15500;

    // Updating separately since the validation depends on existing params
    updateSetParameters({
        ShardingTaskExecutorPoolRefreshTimeoutMS: pendingTimeoutMS,
    });
    updateSetParameters({
        ShardingTaskExecutorPoolRefreshRequirementMS: toRefreshTimeoutMS,
    });
    updateSetParameters({
        ShardingTaskExecutorPoolHostTimeoutMS: idleTimeoutMS1,
    });

    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "alwaysOn");
    dropConnections();

    // Make ready connections
    launchFinds({times: conns, readPref: "primary"});
    configureReplSetFailpoint("waitInFindBeforeMakingBatch", "off");
    hasConnPoolStats({ready: conns});

    // Block refreshes and wait for the toRefresh timeout
    configureReplSetFailpoint("waitInHello", "alwaysOn");
    sleep(toRefreshTimeoutMS);

    // Confirm that we're in pending for all of our conns
    hasConnPoolStats({pending: conns});

    // Set our min conns to 0 to make sure we don't refresh after pending timeout
    updateSetParameters({
        ShardingTaskExecutorPoolMinSize: 0,
    });

    // Wait for our pending timeout
    sleep(pendingTimeoutMS);
    hasConnPoolStats({});

    configureReplSetFailpoint("waitInHello", "off");

    // Reset the min conns to make sure normal refresh doesn't extend the timeout
    updateSetParameters({
        ShardingTaskExecutorPoolMinSize: minConns,
    });

    // Wait for our host timeout and confirm the pool drops
    sleep(idleTimeoutMS1);
    hasConnPoolStats({isAbsent: true});

    // Reset the pool
    resetPools();

    // Sleep for a shorter timeout and then update so we're already expired
    sleep(idleTimeoutMS2);
    updateSetParameters({ShardingTaskExecutorPoolHostTimeoutMS: idleTimeoutMS2});
    hasConnPoolStats({isAbsent: true});
});

threads.forEach(function(thread) {
    thread.join();
});

st.stop();
})();

// Test --setShellParameter CLI switch.

(function() {
'use strict';

function test(ssp, succeed) {
    const result = runMongoProgram('mongo', '--setShellParameter', ssp, '--nodb', '--eval', ';');
    assert.eq(
        0 == result, succeed, '--setShellParameter ' + ssp + 'worked/didn\'t-work unexpectedly');
}

// Allowlisted
test('disabledSecureAllocatorDomains=foo', true);

// Not allowlisted
test('enableTestCommands=1', false);

// Unknown
test('theAnswerToTheQuestionOfLifeTheUniverseAndEverything=42', false);
})();

/**
 * This test is intended to exercise shard filtering logic. This test works by sharding a
 * collection, and then inserting orphaned documents directly into one of the shards. It then runs a
 * find() and makes sure that orphaned documents are filtered out.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");

// Deliberately inserts orphans outside of migration.
TestData.skipCheckOrphans = true;
const st = new ShardingTest({shards: 2});
const collName = "test.shardfilter";
const mongosDb = st.s.getDB("test");
const mongosColl = st.s.getCollection(collName);

assert.commandWorked(st.s.adminCommand({enableSharding: "test"}));
st.ensurePrimaryShard("test", st.shard1.name);
assert.commandWorked(
    st.s.adminCommand({shardCollection: collName, key: {a: 1, "b.c": 1, "d.e.f": 1}}));

// Put a chunk with no data onto shard0 in order to make sure that both shards get targeted.
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 20, "b.c": 0, "d.e.f": 0}}));
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 30, "b.c": 0, "d.e.f": 0}}));
assert.commandWorked(st.s.adminCommand(
    {moveChunk: collName, find: {a: 25, "b.c": 0, "d.e.f": 0}, to: st.shard0.shardName}));

// Shard the collection and insert some docs.
const docs = [
    {_id: 0, a: 1, b: {c: 1}, d: {e: {f: 1}}, g: 100},
    {_id: 1, a: 1, b: {c: 2}, d: {e: {f: 2}}, g: 100.9},
    {_id: 2, a: 1, b: {c: 3}, d: {e: {f: 3}}, g: "a"},
    {_id: 3, a: 1, b: {c: 3}, d: {e: {f: 3}}, g: [1, 2, 3]},
    {_id: 4, a: "a", b: {c: "b"}, d: {e: {f: "c"}}, g: null},
    {_id: 5, a: 1.0, b: {c: "b"}, d: {e: {f: Infinity}}, g: NaN}
];
assert.commandWorked(mongosColl.insert(docs));
assert.eq(mongosColl.find().itcount(), 6);

// Insert some documents with valid partial shard keys to both shards. The versions of these
// documents on shard0 are orphans, since all of the data is owned by shard1.
const docsWithMissingAndNullKeys = [
    {_id: 6, a: "missingParts"},
    {_id: 7, a: null, b: {c: 1}, d: {e: {f: 1}}},
    {_id: 8, a: "null", b: {c: null}, d: {e: {f: 1}}},
    {_id: 9, a: "deepNull", b: {c: 1}, d: {e: {f: null}}},
];
assert.commandWorked(st.shard0.getCollection(collName).insert(docsWithMissingAndNullKeys));
assert.commandWorked(st.shard1.getCollection(collName).insert(docsWithMissingAndNullKeys));

// Insert orphan docs without missing or null shard keys onto shard0 and test that they get filtered
// out.
const orphanDocs = [
    {_id: 10, a: 100, b: {c: 10}, d: {e: {f: 999}}, g: "a"},
    {_id: 11, a: 101, b: {c: 11}, d: {e: {f: 1000}}, g: "b"}
];
assert.commandWorked(st.shard0.getCollection(collName).insert(orphanDocs));
assert.eq(mongosColl.find().itcount(), 10);

// Insert docs directly into shard0 to test that regular (non-null, non-missing) shard keys get
// filtered out.
assert.commandWorked(st.shard0.getCollection(collName).insert(docs));
assert.eq(mongosColl.find().itcount(), 10);

// Ensure that shard filtering works correctly for a query that can use the index supporting the
// shard key. In this case, shard filtering can occur before the FETCH stage, but the plan is not
// covered.
let explain = mongosColl.find({a: {$gte: 0}}).explain();
assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE", explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "SHARDING_FILTER"), explain);
assert(isIxscan(mongosDb, explain.queryPlanner.winningPlan), explain);
assert(!isIndexOnly(mongosDb, explain.queryPlanner.winningPlan), explain);
assert.sameMembers(mongosColl.find({a: {$gte: 0}}).toArray(), [
    {_id: 0, a: 1, b: {c: 1}, d: {e: {f: 1}}, g: 100},
    {_id: 1, a: 1, b: {c: 2}, d: {e: {f: 2}}, g: 100.9},
    {_id: 2, a: 1, b: {c: 3}, d: {e: {f: 3}}, g: "a"},
    {_id: 3, a: 1, b: {c: 3}, d: {e: {f: 3}}, g: [1, 2, 3]},
    {_id: 5, a: 1, b: {c: "b"}, d: {e: {f: Infinity}}, g: NaN}
]);

// In this case, shard filtering is done as part of a covered plan.
explain = mongosColl.find({a: {$gte: 0}}, {_id: 0, a: 1}).explain();
assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE", explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "SHARDING_FILTER"), explain);
assert(isIxscan(mongosDb, explain.queryPlanner.winningPlan), explain);
assert(isIndexOnly(mongosDb, explain.queryPlanner.winningPlan), explain);
assert.sameMembers(mongosColl.find({a: {$gte: 0}}, {_id: 0, a: 1}).toArray(), [
    {a: 1},
    {a: 1},
    {a: 1},
    {a: 1},
    {a: 1},
]);

// Drop the collection and shard it by a new key that has no dotted fields. Again, make sure that
// shard0 has an empty chunk.
assert(mongosColl.drop());
assert.commandWorked(st.s.adminCommand({shardCollection: collName, key: {a: 1, b: 1, c: 1, d: 1}}));
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 20, b: 0, c: 0, d: 0}}));
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 30, b: 0, c: 0, d: 0}}));
assert.commandWorked(st.s.adminCommand(
    {moveChunk: collName, find: {a: 25, b: 0, c: 0, d: 0}, to: st.shard0.shardName}));

// Insert some data via mongos, and also insert some documents directly to shard0 to produce an
// orphans.
assert.commandWorked(mongosColl.insert([
    {_id: 0, a: 0, b: 0, c: 0, d: 0},
    {_id: 1, a: 1, b: 1, c: 1, d: 1},
    {_id: 2, a: -1, b: -1, c: -1, d: -1},
]));
assert.commandWorked(st.shard0.getCollection(collName).insert({_id: 3, a: 0, b: 0, c: 0, d: 0}));
assert.commandWorked(st.shard0.getCollection(collName).insert({_id: 4, a: 0, b: 99, c: 0, d: 99}));
assert.commandWorked(st.shard0.getCollection(collName).insert({_id: 5, a: 0, b: 0, c: 99, d: 99}));
assert.commandWorked(st.shard0.getCollection(collName).insert({_id: 6, a: 0, b: 99, c: 99, d: 99}));

// Run a query that can use covered shard filtering where the projection involves more than one
// field of the shard key.
explain = mongosColl.find({a: {$gte: 0}}, {_id: 0, a: 1, b: 1, d: 1}).explain();
assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE", explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "SHARDING_FILTER"), explain);
assert(isIxscan(mongosDb, explain.queryPlanner.winningPlan), explain);
assert(isIndexOnly(mongosDb, explain.queryPlanner.winningPlan), explain);
assert.sameMembers(mongosColl.find({a: {$gte: 0}}, {_id: 0, a: 1, b: 1, d: 1}).toArray(),
                   [{a: 0, b: 0, d: 0}, {a: 1, b: 1, d: 1}]);

// Run a query that will use a covered OR plan.
explain = mongosColl.find({$or: [{a: 0, c: 0}, {a: 25, c: 0}]}, {_id: 0, a: 1, c: 1}).explain();
assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE", explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "SHARDING_FILTER"), explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "OR"), explain);
assert(isIndexOnly(mongosDb, explain.queryPlanner.winningPlan), explain);
assert.sameMembers(
    mongosColl.find({$or: [{a: 0, c: 0}, {a: 25, c: 0}]}, {_id: 0, a: 1, c: 1}).toArray(),
    [{a: 0, c: 0}]);

// Similar case to above, but here the index scans involve a single interval of the index.
explain = mongosColl.find({$or: [{a: 0, b: 0}, {a: 25, b: 0}]}, {_id: 0, a: 1, b: 1}).explain();
assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE", explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "SHARDING_FILTER"), explain);
assert(planHasStage(mongosDb, explain.queryPlanner.winningPlan, "OR"), explain);
assert(isIndexOnly(mongosDb, explain.queryPlanner.winningPlan), explain);
assert.sameMembers(
    mongosColl.find({$or: [{a: 0, b: 0}, {a: 25, b: 0}]}, {_id: 0, a: 1, b: 1}).toArray(),
    [{a: 0, b: 0}]);

st.stop();
})();

// A test to ensure that shard_fixture.js is consistent with shardingtest.js
// @tags: [
//   requires_sharding,
// ]

(function() {
'use strict';

load('jstests/concurrency/fsm_libs/shard_fixture.js');

const rsTestOriginal = new ShardingTest({shards: 2, mongos: 2, config: 2});

const rsTestWrapper =
    new FSMShardingTest(`mongodb://${rsTestOriginal.s0.host},${rsTestOriginal.s1.host}`);

assert.eq(rsTestWrapper.s(0).host, rsTestOriginal.s0.host);
assert.eq(rsTestWrapper.s(1).host, rsTestOriginal.s1.host);
assert.eq(rsTestWrapper.s(2), rsTestOriginal.s2);  // Both should be undefined.

assert.eq(rsTestWrapper.shard(0).host, rsTestOriginal.shard0.host);
assert.eq(rsTestWrapper.shard(1).host, rsTestOriginal.shard1.host);
assert.eq(rsTestWrapper.shard(2), rsTestOriginal.shard2);  // Both should be undefined.

assert.eq(rsTestWrapper.rs(0).getURL(), rsTestOriginal.rs0.getURL());
assert.eq(rsTestWrapper.rs(1).getURL(), rsTestOriginal.rs1.getURL());
assert.eq(rsTestWrapper.rs(2), rsTestOriginal.rs2);  // Both should be undefined.

assert.eq(rsTestWrapper.d(0), rsTestOriginal.d0);  // Both should be undefined.

assert.eq(rsTestWrapper.c(0).host, rsTestOriginal.c0.host);
assert.eq(rsTestWrapper.c(1).host, rsTestOriginal.c1.host);
assert.eq(rsTestWrapper.c(2), rsTestOriginal.c2);  // Both should be undefined.

rsTestOriginal.stop();
})();

/**
 * Verifies that the $collStats aggregation stage includes the shard and hostname for each output
 * document when run via mongoS, and that the former is absent when run on a non-shard mongoD.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();
let testDB = conn.getDB(jsTestName());
let testColl = testDB.test;

// getHostName() doesn't include port, db.getMongo().host is 127.0.0.1:<port>
const hostName = (getHostName() + ":" + testDB.getMongo().host.split(":")[1]);

// Test that the shard field is absent and the host field is present when run on mongoD.
assert.eq(testColl
              .aggregate([
                  {$collStats: {latencyStats: {histograms: true}}},
                  {$group: {_id: {shard: "$shard", host: "$host"}}}
              ])
              .toArray(),
          [{_id: {host: hostName}}]);

MongoRunner.stopMongod(conn);

// Test that both shard and hostname are present for $collStats results on a sharded cluster.
const st = new ShardingTest({name: jsTestName(), shards: 2});

testDB = st.s.getDB(jsTestName());
testColl = testDB.test;

assert.commandWorked(testDB.dropDatabase());

// Enable sharding on the test database.
assert.commandWorked(testDB.adminCommand({enableSharding: testDB.getName()}));

// Shard 'testColl' on {_id: 'hashed'}. This will automatically presplit the collection and
// place chunks on each shard.
assert.commandWorked(
    testDB.adminCommand({shardCollection: testColl.getFullName(), key: {_id: "hashed"}}));

// Group $collStats result by $shard and $host to confirm that both fields are present.
assert.eq(testColl
              .aggregate([
                  {$collStats: {latencyStats: {histograms: true}}},
                  {$group: {_id: {shard: "$shard", host: "$host"}}},
                  {$sort: {_id: 1}}
              ])
              .toArray(),
          [
              {_id: {shard: st.shard0.shardName, host: st.rs0.getPrimary().host}},
              {_id: {shard: st.shard1.shardName, host: st.rs1.getPrimary().host}},
          ]);

st.stop();
})();

/**
 * Tests index consistency metrics in the serverStatus output.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/sharded_index_consistency_metrics_helpers.js");

// This test creates inconsistent indexes.
TestData.skipCheckingIndexesConsistentAcrossCluster = true;

/*
 * Asserts that the serverStatus output does not contain the index consistency metrics
 * both by default and when 'shardedIndexConsistency' is explicitly included.
 */
function assertServerStatusNotContainIndexMetrics(conn) {
    let res = assert.commandWorked(conn.adminCommand({serverStatus: 1}));
    assert.eq(undefined, res.shardedIndexConsistency, tojson(res.shardedIndexConsistency));

    res = assert.commandWorked(conn.adminCommand({serverStatus: 1, shardedIndexConsistency: 1}));
    assert.eq(undefined, res.shardedIndexConsistency, tojson(res.shardedIndexConsistency));
}

/*
 * Asserts the serverStatus output for 'configPrimaryConn' has the expected number of collections
 * with inconsistent indexes and the output for 'configSecondaryConn' always reports 0. For each
 * mongod in 'connsWithoutIndexConsistencyMetrics', asserts that its serverStatus output does not
 * contain the index consistency metrics.
 */
function checkServerStatus(configPrimaryConn,
                           configSecondaryConn,
                           connsWithoutIndexConsistencyMetrics,
                           expectedNumCollsWithInconsistentIndexes) {
    // Sleep to let the periodic check run. Note this won't guarantee the check has run, but should
    // make it likely enough to catch bugs in most test runs.
    sleep(intervalMS * 2);

    checkServerStatusNumCollsWithInconsistentIndexes(configPrimaryConn,
                                                     expectedNumCollsWithInconsistentIndexes);

    // A config secondary should always report zero because only primaries run the aggregation to
    // find inconsistent indexes.
    checkServerStatusNumCollsWithInconsistentIndexes(configSecondaryConn, 0);

    for (const conn of connsWithoutIndexConsistencyMetrics) {
        assertServerStatusNotContainIndexMetrics(conn);
    }
}

const intervalMS = 500;
const st = new ShardingTest({
    shards: 2,
    config: 3,
    configOptions: {setParameter: {"shardedIndexConsistencyCheckIntervalMS": intervalMS}}
});
const dbName = "testDb";
const ns1 = dbName + ".testColl1";
const ns2 = dbName + ".testColl2";
const ns3 = dbName + ".testColl3";
const ns4 = dbName + ".testColl4";
const expiration = 1000000;
const filterExpr = {
    x: {$gt: 50}
};

assert.commandWorked(st.s.adminCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.shardName);
assert.commandWorked(st.s.adminCommand({shardCollection: ns1, key: {_id: "hashed"}}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns2, key: {_id: "hashed"}}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns3, key: {_id: "hashed"}}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns4, key: {_id: "hashed"}}));

// Disable the check on one config secondary to verify this means metrics won't be shown in
// serverStatus.
assert.commandWorked(st.config2.getDB("admin").runCommand(
    {setParameter: 1, enableShardedIndexConsistencyCheck: false}));

let configPrimaryConn = st.config0;
let configSecondaryConn = st.config1;
const connsWithoutIndexConsistencyMetrics = [st.config2, st.shard0, st.shard1, st.s];

checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 0);

// Create an inconsistent index for ns1.
assert.commandWorked(st.shard0.getCollection(ns1).createIndex({x: 1}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 1);

// Create another inconsistent index for ns1.
assert.commandWorked(st.shard1.getCollection(ns1).createIndexes([{y: 1}]));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 1);

// Create an inconsistent index for ns2.
assert.commandWorked(st.shard0.getCollection(ns2).createIndex({x: 1}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 2);

// Resolve the index inconsistency for ns2.
assert.commandWorked(st.shard1.getCollection(ns2).createIndex({x: 1}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 1);

// Create indexes with different keys for the same name and verify this is considered
// inconsistent.
assert.commandWorked(st.shard0.getCollection(ns2).createIndex({y: 1}, {name: "diffKey"}));
assert.commandWorked(st.shard1.getCollection(ns2).createIndex({z: 1}, {name: "diffKey"}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 2);

// Create indexes for n3 with the same options but in different orders on each shard, and verify
// that it is not considered as inconsistent.
assert.commandWorked(st.shard0.getCollection(ns3).createIndex({x: 1}, {
    name: "indexWithOptionsOrderedDifferently",
    partialFilterExpression: filterExpr,
    expireAfterSeconds: expiration
}));
assert.commandWorked(st.shard1.getCollection(ns3).createIndex({x: 1}, {
    name: "indexWithOptionsOrderedDifferently",
    expireAfterSeconds: expiration,
    partialFilterExpression: filterExpr
}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 2);

// Create indexes for n3 with the same key but different options on each shard, and verify that
// it is considered as inconsistent.
assert.commandWorked(st.shard0.getCollection(ns3).createIndex(
    {y: 1}, {name: "indexWithDifferentOptions", expireAfterSeconds: expiration}));
assert.commandWorked(
    st.shard1.getCollection(ns3).createIndex({y: 1}, {name: "indexWithDifferentOptions"}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 3);

// Create indexes where one is missing a property and verify this is considered inconsistent.
assert.commandWorked(st.shard0.getCollection(ns4).createIndex({y: 1}, {expireAfterSeconds: 100}));
assert.commandWorked(st.shard1.getCollection(ns4).createIndex({y: 1}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 4);

// Resolve the inconsistency on ns4.
assert.commandWorked(st.shard1.getCollection(ns4).dropIndex({y: 1}));
assert.commandWorked(st.shard1.getCollection(ns4).createIndex({y: 1}, {expireAfterSeconds: 100}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 3);

// Verify fields other than expireAfterSeconds and key are not ignored.
assert.commandWorked(st.shard0.getCollection(ns4).createIndex(
    {z: 1}, {expireAfterSeconds: 5, partialFilterExpression: {z: {$gt: 50}}}));
assert.commandWorked(st.shard1.getCollection(ns4).createIndex(
    {z: 1}, {expireAfterSeconds: 5, partialFilterExpression: {z: {$lt: 100}}}));
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 4);

//
// Verify the counter is only tracked by primaries and cleared on stepdown.
//

// Force the secondary that tracks inconsistent indexes to step up to primary and update the
// appropriate test variables.
st.configRS.stepUp(configSecondaryConn);
st.configRS.waitForState(configSecondaryConn, ReplSetTest.State.PRIMARY);
st.configRS.waitForState(configPrimaryConn, ReplSetTest.State.SECONDARY);
st.configRS.awaitNodesAgreeOnPrimary();

configSecondaryConn = configPrimaryConn;
configPrimaryConn = st.configRS.getPrimary();

// The new primary should start reporting the correct count and the old primary should start
// reporting 0.
checkServerStatus(configPrimaryConn, configSecondaryConn, connsWithoutIndexConsistencyMetrics, 4);

st.stop();

// Verify that the serverStatus output for standalones and non-sharded repilca set servers does
// not contain the index consistency metrics.
const standaloneMongod = MongoRunner.runMongod();
assertServerStatusNotContainIndexMetrics(standaloneMongod);
MongoRunner.stopMongod(standaloneMongod);

const rst = ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
assertServerStatusNotContainIndexMetrics(rst.getPrimary());
rst.stopSet();
}());

/**
 * Tests the aggregation that collects index consistency metrics for serverStatus retries on stale
 * version errors.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/sharded_index_consistency_metrics_helpers.js");
load("jstests/sharding/libs/shard_versioning_util.js");
load("jstests/libs/fail_point_util.js");

// This test creates inconsistent indexes.
TestData.skipCheckingIndexesConsistentAcrossCluster = true;

const intervalMS = 10000;
const st = new ShardingTest({
    shards: 2,
    config: 1,
    configOptions: {setParameter: {"shardedIndexConsistencyCheckIntervalMS": intervalMS}}
});

const dbName = "testDb";
const ns0 = dbName + ".testColl0";
const ns1 = dbName + ".testColl1";
const ns2 = dbName + ".testColl2";

// Create 3 sharded collections, two hashed and another with 3 chunks, 1 on shard1 and 2 on shard0.
assert.commandWorked(st.s.adminCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.shardName);

assert.commandWorked(st.s.adminCommand({shardCollection: ns0, key: {_id: "hashed"}}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns1, key: {_id: "hashed"}}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns2, key: {_id: 1}}));
assert.commandWorked(st.s.adminCommand({split: ns2, middle: {_id: 0}}));
assert.commandWorked(st.s.adminCommand({split: ns2, middle: {_id: 10}}));
assert.commandWorked(st.s.adminCommand({moveChunk: ns2, find: {_id: 10}, to: st.shard1.shardName}));

// Wait for the periodic index check to run by creating an inconsistency and waiting for the new
// counter value to be reported.
assert.commandWorked(st.shard0.getCollection(ns0).createIndex({x: 1}));
checkServerStatusNumCollsWithInconsistentIndexes(st.configRS.getPrimary(), 1);

// Clear the config server's log before the check below so an earlier failure to load inconsistent
// indexes won't trigger a failure when the logs are checked below.
assert.commandWorked(st.configRS.getPrimary().adminCommand({clearLog: "global"}));

// Create an index inconsistency on ns1 and then begin repeatedly moving a chunk between both shards
// for ns2 without refreshing the recipient so at least one shard should typically be stale when the
// periodic index check runs. The check should retry on stale config errors and be able to
// eventually return the correct counter.
assert.commandWorked(st.shard0.getCollection(ns1).createIndex({x: 1}));
assert.soon(
    () => {
        ShardVersioningUtil.moveChunkNotRefreshRecipient(st.s, ns2, st.shard0, st.shard1, {_id: 0});
        sleep(2000);
        ShardVersioningUtil.moveChunkNotRefreshRecipient(st.s, ns2, st.shard1, st.shard0, {_id: 0});
        sleep(2000);

        const latestCount =
            getServerStatusNumCollsWithInconsistentIndexes(st.configRS.getPrimary());
        jsTestLog(
            "Waiting for periodic index check to discover inconsistent indexes. Latest count: " +
            latestCount);
        return latestCount == 2;
    },
    "periodic index check couldn't discover inconsistent indexes with stale shards",
    undefined,
    undefined,
    {runHangAnalyzer: false});

// As an extra sanity check, verify the index check didn't log a failure.
checkLog.containsWithCount(st.configRS.getPrimary(), "Failed to check index consistency", 0);

st.stop();
}());

// @tags: [requires_profiling]
(function() {
"use strict";

const conn = MongoRunner.runMongod();
const uri = "mongodb://" + conn.host + "/test";
const tests = [];

// Asserts that system.profile contains only entries
// with application.name = appname (or undefined)
function assertProfileOnlyContainsAppName(db, appname) {
    const res = db.system.profile.distinct("appName");
    assert(res.length > 0, "system.profile does not contain any docs");
    if (res.length > 1 || res.indexOf(appname) === -1) {
        // Dump collection.
        print("dumping db.system.profile");
        db.system.profile.find().forEach((doc) => printjsononeline(doc));
        doassert(`system.profile expected to only have appName=${appname}` +
                 ` but found ${tojson(res)}`);
    }
}

tests.push(function testDefaultAppName() {
    const db = new Mongo(uri).getDB("test");
    assert.commandWorked(db.coll.insert({}));
    assertProfileOnlyContainsAppName(db, "MongoDB Shell");
});

tests.push(function testAppName() {
    const db = new Mongo(uri + "?appName=TestAppName").getDB("test");
    assert.commandWorked(db.coll.insert({}));
    assertProfileOnlyContainsAppName(db, "TestAppName");
});

tests.push(function testMultiWordAppName() {
    const db = new Mongo(uri + "?appName=Test%20App%20Name").getDB("test");
    assert.commandWorked(db.coll.insert({}));
    assertProfileOnlyContainsAppName(db, "Test App Name");
});

tests.push(function testLongAppName() {
    // From MongoDB Handshake specification:
    // The client.application.name cannot exceed 128 bytes. MongoDB will return an error if
    // these limits are not adhered to, which will result in handshake failure. Drivers MUST
    // validate these values and truncate driver provided values if necessary.
    const longAppName = "a".repeat(129);
    assert.throws(() => new Mongo(uri + "?appName=" + longAppName));

    // But a 128 character appname should connect without issue.
    const notTooLongAppName = "a".repeat(128);
    const db = new Mongo(uri + "?appName=" + notTooLongAppName).getDB("test");
    assert.commandWorked(db.coll.insert({}));
    assertProfileOnlyContainsAppName(db, notTooLongAppName);
});

tests.push(function testLongAppNameWithMultiByteUTF8() {
    // Each epsilon character is two bytes in UTF-8.
    const longAppName = "\u0190".repeat(65);
    assert.throws(() => new Mongo(uri + "?appName=" + longAppName));

    // But a 128 character appname should connect without issue.
    const notTooLongAppName = "\u0190".repeat(64);
    const db = new Mongo(uri + "?appName=" + notTooLongAppName).getDB("test");
    assert.commandWorked(db.coll.insert({}));
    assertProfileOnlyContainsAppName(db, notTooLongAppName);
});

tests.forEach((test) => {
    const db = conn.getDB("test");
    db.dropDatabase();
    // Entries in db.system.profile have application name.
    db.setProfilingLevel(2);
    test();
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests for the assertion functions in mongo/shell/assert.js.
 */
(() => {
    "use strict";

    const tests = [];

    const kDefaultTimeoutMS = 10 * 1000;
    const kSmallTimeoutMS = 200;
    const kSmallRetryIntervalMS = 1;
    const kDefaultRetryAttempts = 5;

    /* doassert tests */

    tests.push(function callingDoAssertWithStringThrowsException() {
        const expectedError = 'hello world';
        const actualError = assert.throws(() => {
            doassert(expectedError);
        });

        assert.eq('Error: ' + expectedError,
                  actualError,
                  'doAssert should throw passed msg as exception');
    });

    tests.push(function callingDoAssertWithObjectThrowsException() {
        const expectedError = {err: 'hello world'};
        const actualError = assert.throws(() => {
            doassert(expectedError);
        });

        assert.eq('Error: ' + tojson(expectedError),
                  actualError,
                  'doAssert should throw passed object as exception');
    });

    tests.push(function callingDoAssertWithStringPassedAsFunctionThrowsException() {
        const expectedError = 'hello world';
        const actualError = assert.throws(() => {
            doassert(() => {
                return expectedError;
            });
        });

        assert.eq('Error: ' + expectedError,
                  actualError,
                  'doAssert should throw passed msg as exception');
    });

    tests.push(function callingDoAssertWithObjectAsFunctionThrowsException() {
        const expectedError = {err: 'hello world'};
        const actualError = assert.throws(() => {
            doassert(() => {
                return expectedError;
            });
        });

        assert.eq('Error: ' + tojson(expectedError),
                  actualError,
                  'doAssert should throw passed object as exception');
    });

    /* assert tests */

    tests.push(function assertShouldFailForMoreThan2Args() {
        const err = assert.throws(() => {
            assert(1, 2, 3);
        });
        assert.neq(-1,
                   err.message.indexOf('Too many parameters'),
                   'Too many params message should be displayed');
    });

    tests.push(function assertShouldNotThrowExceptionForTrue() {
        assert.doesNotThrow(() => {
            assert(true, 'message');
        });
    });

    tests.push(function assertShouldThrowExceptionForFalse() {
        const expectedMessage = 'message';
        const err = assert.throws(() => {
            assert(false, expectedMessage);
        });

        assert.neq(
            -1, err.message.indexOf(expectedMessage), 'assert message should be thrown on error');
    });

    tests.push(function assertShouldThrowExceptionForFalseWithDefaultMessage() {
        const defaultMessage = 'assert failed';
        const err = assert.throws(() => {
            assert(false);
        });

        assert.eq(defaultMessage, err.message, 'assert message should be thrown on error');
    });

    tests.push(function assertShouldThrowExceptionForFalseWithDefaultMessagePrefix() {
        const prefix = 'assert failed';
        const message = 'the assertion failed';
        const err = assert.throws(() => {
            assert(false, message);
        });

        assert.neq(-1, err.message.indexOf(prefix), 'assert message should should contain prefix');
        assert.neq(-1,
                   err.message.indexOf(message),
                   'assert message should should contain original message');
    });

    tests.push(function assertShouldNotCallMsgFunctionsOnSuccess() {
        var called = false;

        assert(true, () => {
            called = true;
        });

        assert.eq(false, called, 'called should not have been udpated');
    });

    tests.push(function assertShouldCallMsgFunctionsOnFailure() {
        var called = false;

        assert.throws(() => {
            assert(false, () => {
                called = true;
                return 'error message';
            });
        });

        assert.eq(true, called, 'called should not have been udpated');
    });

    tests.push(function assertShouldAcceptObjectAsMsg() {
        const objMsg = {someMessage: 1};
        const err = assert.throws(() => {
            assert(false, objMsg);
        });

        assert.neq(-1,
                   err.message.indexOf(tojson(objMsg)),
                   'Error message should have included ' + tojson(objMsg));
    });

    tests.push(function assertShouldNotAcceptNonObjStringFunctionAsMsg() {
        const err = assert.throws(() => {
            assert(true, 1234);
        });

        assert.neq(-1, err.message.indexOf("msg parameter must be a "));
    });

    /* assert.automsg tests */

    tests.push(function automsgShouldPassToAssert() {
        const defaultMessage = '1 === 2';
        const err = assert.throws(() => {
            assert.automsg(defaultMessage);
        });

        assert.neq(-1, err.message.indexOf(defaultMessage), 'default message should be returned');
    });

    /* assert.eq tests */

    tests.push(function eqShouldPassOnEquality() {
        assert.doesNotThrow(() => {
            assert.eq(3, 3);
        });
    });

    tests.push(function eqShouldFailWhenNotEqual() {
        assert.throws(() => {
            assert.eq(2, 3);
        });
    });

    tests.push(function eqShouldNotCallMsgFunctionOnSuccess() {
        var called = false;

        assert.doesNotThrow(() => {
            assert.eq(3, 3, () => {
                called = true;
            });
        });

        assert.eq(false, called, 'msg function should not have been called');
    });

    tests.push(function eqShouldCallMsgFunctionOnFailure() {
        var called = false;

        assert.throws(() => {
            assert.eq(1, 3, () => {
                called = true;
            });
        });

        assert.eq(true, called, 'msg function should have been called');
    });

    tests.push(function eqShouldPassOnObjectsWithSameContent() {
        const a = {'foo': true};
        const b = {'foo': true};

        assert.doesNotThrow(() => {
            assert.eq(a, b);
        }, [], 'eq should not throw exception on two objects with the same content');
    });

    /* assert.eq.automsg tests */

    tests.push(function eqAutomsgShouldCreateMessage() {
        const defaultMessage = '[1] != [2]';
        const err = assert.throws(() => {
            assert.eq.automsg(1, 2);
        });

        assert.neq(-1, err.message.indexOf(defaultMessage), 'default message should be returned');
    });

    /* assert.neq tests */

    tests.push(function neqShouldFailOnEquality() {
        assert.throws(() => {
            assert.neq(3, 3);
        });
    });

    tests.push(function neqShouldPassWhenNotEqual() {
        assert.doesNotThrow(() => {
            assert.neq(2, 3);
        });
    });

    tests.push(function neqShouldFailOnObjectsWithSameContent() {
        const a = {'foo': true};
        const b = {'foo': true};

        assert.throws(() => {
            assert.neq(a, b);
        }, [], 'neq should throw exception on two objects with the same content');
    });

    /* assert.hasFields tests */

    tests.push(function hasFieldsRequiresAnArrayOfFields() {
        const object = {field1: 1, field2: 1, field3: 1};

        assert.throws(() => {
            assert.hasFields(object, 'field1');
        });
    });

    tests.push(function hasFieldsShouldPassWhenObjectHasField() {
        const object = {field1: 1, field2: 1, field3: 1};

        assert.doesNotThrow(() => {
            assert.hasFields(object, ['field1']);
        });
    });

    tests.push(function hasFieldsShouldFailWhenObjectDoesNotHaveField() {
        const object = {field1: 1, field2: 1, field3: 1};

        assert.throws(() => {
            assert.hasFields(object, ['fieldDoesNotExist']);
        });
    });

    /* assert.contains tests */

    tests.push(function containsShouldOnlyWorkOnArrays() {
        assert.throws(() => {
            assert.contains(42, 5);
        });
    });

    tests.push(function containsShouldPassIfArrayContainsValue() {
        const array = [1, 2, 3];

        assert.doesNotThrow(() => {
            assert.contains(2, array);
        });
    });

    tests.push(function containsShouldFailIfArrayDoesNotContainValue() {
        const array = [1, 2, 3];

        assert.throws(() => {
            assert.contains(42, array);
        });
    });

    /* assert.soon tests */

    tests.push(function soonPassesWhenFunctionPasses() {
        assert.doesNotThrow(() => {
            assert.soon(() => {
                return true;
            });
        });
    });

    tests.push(function soonFailsIfMethodNeverPasses() {
        assert.throws(() => {
            assert.soon(() => {
                return false;
            }, 'assert message', kSmallTimeoutMS, kSmallRetryIntervalMS, {runHangAnalyzer: false});
        });
    });

    tests.push(function soonPassesIfMethodEventuallyPasses() {
        var count = 0;
        assert.doesNotThrow(() => {
            assert.soon(() => {
                count += 1;
                return count === 3;
            }, 'assert message', kDefaultTimeoutMS, kSmallRetryIntervalMS);
        });
    });

    /* assert.soonNoExcept tests */

    tests.push(function soonNoExceptEventuallyPassesEvenWithExceptions() {
        var count = 0;
        assert.doesNotThrow(() => {
            assert.soonNoExcept(() => {
                count += 1;
                if (count < 3) {
                    throw new Error('failed');
                }
                return true;
            }, 'assert message', kDefaultTimeoutMS, kSmallRetryIntervalMS);
        });
    });

    tests.push(function soonNoExceptFailsIfExceptionAlwaysThrown() {
        var count = 0;
        assert.throws(() => {
            assert.soonNoExcept(() => {
                throw new Error('failed');
            }, 'assert message', kSmallTimeoutMS, kSmallRetryIntervalMS, {runHangAnalyzer: false});
        });
    });

    /* assert.retry tests */

    tests.push(function retryPassesAfterAFewAttempts() {
        var count = 0;

        assert.doesNotThrow(() => {
            assert.retry(() => {
                count += 1;
                return count === 3;
            }, 'assert message', kDefaultRetryAttempts, kSmallRetryIntervalMS);
        });
    });

    tests.push(function retryFailsAfterMaxAttempts() {
        assert.throws(() => {
            assert.retry(() => {
                return false;
            }, 'assert message', kDefaultRetryAttempts, kSmallRetryIntervalMS, {
                runHangAnalyzer: false
            });
        });
    });

    /* assert.retryNoExcept tests */

    tests.push(function retryNoExceptPassesAfterAFewAttempts() {
        var count = 0;

        assert.doesNotThrow(() => {
            assert.retryNoExcept(() => {
                count += 1;
                if (count < 3) {
                    throw new Error('failed');
                }
                return count === 3;
            }, 'assert message', kDefaultRetryAttempts, kSmallRetryIntervalMS);
        });
    });

    tests.push(function retryNoExceptFailsAfterMaxAttempts() {
        assert.throws(() => {
            assert.retryNoExcept(() => {
                throw new Error('failed');
            }, 'assert message', kDefaultRetryAttempts, kSmallRetryIntervalMS, {
                runHangAnalyzer: false
            });
        });
    });

    /* assert.time tests */

    tests.push(function timeIsSuccessfulIfFuncExecutesInTime() {
        assert.doesNotThrow(() => {
            assert.time(() => {
                return true;
            }, 'assert message', kDefaultTimeoutMS);
        });
    });

    tests.push(function timeFailsIfFuncDoesNotFinishInTime() {
        assert.throws(() => {
            assert.time(() => {
                return true;
            }, 'assert message', -5 * 60 * 1000, {runHangAnalyzer: false});
        });
    });

    /* assert.isnull tests */

    tests.push(function isnullPassesOnNull() {
        assert.doesNotThrow(() => {
            assert.isnull(null);
        });
    });

    tests.push(function isnullPassesOnUndefined() {
        assert.doesNotThrow(() => {
            assert.isnull(undefined);
        });
    });

    tests.push(function isnullFailsOnNotNull() {
        assert.throws(() => {
            assert.isnull('hello world');
        });
    });

    /* assert.lt tests */

    tests.push(function ltPassesWhenLessThan() {
        assert.doesNotThrow(() => {
            assert.lt(3, 5);
        });
    });

    tests.push(function ltFailsWhenNotLessThan() {
        assert.throws(() => {
            assert.lt(5, 3);
        });
    });

    tests.push(function ltFailsWhenEqual() {
        assert.throws(() => {
            assert.lt(5, 5);
        });
    });

    tests.push(function ltPassesWhenLessThanWithTimestamps() {
        assert.doesNotThrow(() => {
            assert.lt(Timestamp(3, 0), Timestamp(10, 0));
        });
    });

    tests.push(function ltFailsWhenNotLessThanWithTimestamps() {
        assert.throws(() => {
            assert.lt(Timestamp(0, 10), Timestamp(0, 3));
        });
    });

    tests.push(function ltFailsWhenEqualWithTimestamps() {
        assert.throws(() => {
            assert.lt(Timestamp(5, 0), Timestamp(5, 0));
        });
    });

    /* assert.gt tests */

    tests.push(function gtPassesWhenGreaterThan() {
        assert.doesNotThrow(() => {
            assert.gt(5, 3);
        });
    });

    tests.push(function gtFailsWhenNotGreaterThan() {
        assert.throws(() => {
            assert.gt(3, 5);
        });
    });

    tests.push(function gtFailsWhenEqual() {
        assert.throws(() => {
            assert.gt(5, 5);
        });
    });

    /* assert.lte tests */

    tests.push(function ltePassesWhenLessThan() {
        assert.doesNotThrow(() => {
            assert.lte(3, 5);
        });
    });

    tests.push(function lteFailsWhenNotLessThan() {
        assert.throws(() => {
            assert.lte(5, 3);
        });
    });

    tests.push(function ltePassesWhenEqual() {
        assert.doesNotThrow(() => {
            assert.lte(5, 5);
        });
    });

    /* assert.gte tests */

    tests.push(function gtePassesWhenGreaterThan() {
        assert.doesNotThrow(() => {
            assert.gte(5, 3);
        });
    });

    tests.push(function gteFailsWhenNotGreaterThan() {
        assert.throws(() => {
            assert.gte(3, 5);
        });
    });

    tests.push(function gtePassesWhenEqual() {
        assert.doesNotThrow(() => {
            assert.gte(5, 5);
        });
    });

    tests.push(function gtePassesWhenGreaterThanWithTimestamps() {
        assert.doesNotThrow(() => {
            assert.gte(Timestamp(0, 10), Timestamp(0, 3));
        });
    });

    tests.push(function gteFailsWhenNotGreaterThanWithTimestamps() {
        assert.throws(() => {
            assert.gte(Timestamp(0, 3), Timestamp(0, 10));
        });
    });

    tests.push(function gtePassesWhenEqualWIthTimestamps() {
        assert.doesNotThrow(() => {
            assert.gte(Timestamp(5, 0), Timestamp(5, 0));
        });
    });

    /* assert.betweenIn tests */

    tests.push(function betweenInPassWhenNumberIsBetween() {
        assert.doesNotThrow(() => {
            assert.betweenIn(3, 4, 5);
        });
    });

    tests.push(function betweenInFailsWhenNumberIsNotBetween() {
        assert.throws(() => {
            assert.betweenIn(3, 5, 4);
        });
    });

    tests.push(function betweenInPassWhenNumbersEqual() {
        assert.doesNotThrow(() => {
            assert.betweenIn(3, 3, 5);
        });
        assert.doesNotThrow(() => {
            assert.betweenIn(3, 5, 5);
        });
    });

    /* assert.betweenEx tests */

    tests.push(function betweenExPassWhenNumberIsBetween() {
        assert.doesNotThrow(() => {
            assert.betweenEx(3, 4, 5);
        });
    });

    tests.push(function betweenExFailsWhenNumberIsNotBetween() {
        assert.throws(() => {
            assert.betweenEx(3, 5, 4);
        });
    });

    tests.push(function betweenExFailsWhenNumbersEqual() {
        assert.throws(() => {
            assert.betweenEx(3, 3, 5);
        });
        assert.throws(() => {
            assert.betweenEx(3, 5, 5);
        });
    });

    /* assert.sameMembers tests */

    tests.push(function sameMembersFailsWithInvalidArguments() {
        assert.throws(() => assert.sameMembers());
        assert.throws(() => assert.sameMembers([]));
        assert.throws(() => assert.sameMembers({}, {}));
        assert.throws(() => assert.sameMembers(1, 1));
    });

    tests.push(function sameMembersFailsWhenLengthsDifferent() {
        assert.throws(() => assert.sameMembers([], [1]));
        assert.throws(() => assert.sameMembers([], [1]));
        assert.throws(() => assert.sameMembers([1, 2], [1]));
        assert.throws(() => assert.sameMembers([1], [1, 2]));
    });

    tests.push(function sameMembersFailsWhenCountsOfDuplicatesDifferent() {
        assert.throws(() => assert.sameMembers([1, 1], [1, 2]));
        assert.throws(() => assert.sameMembers([1, 2], [1, 1]));
    });

    tests.push(function sameMembersFailsWithDifferentObjects() {
        assert.throws(() => assert.sameMembers([{_id: 0, a: 0}], [{_id: 0, a: 1}]));
        assert.throws(() => assert.sameMembers([{_id: 1, a: 0}], [{_id: 0, a: 0}]));
        assert.throws(() => {
            assert.sameMembers([{a: [{b: 0, c: 0}], _id: 0}], [{_id: 0, a: [{c: 0, b: 1}]}]);
        });
    });

    tests.push(function sameMembersFailsWithDifferentBSONTypes() {
        assert.throws(() => {
            assert.sameMembers([new BinData(0, "JANgqwetkqwklEWRbWERKKJREtbq")],
                               [new BinData(0, "xxxgqwetkqwklEWRbWERKKJREtbq")]);
        });
        assert.throws(() => assert.sameMembers([new Timestamp(0, 1)], [new Timestamp(0, 2)]));
    });

    tests.push(function sameMembersFailsWithCustomCompareFn() {
        const compareBinaryEqual = (a, b) => bsonBinaryEqual(a, b);
        assert.throws(() => {
            assert.sameMembers([NumberLong(1)], [1], undefined /*msg*/, compareBinaryEqual);
        });
        assert.throws(() => {
            assert.sameMembers([NumberLong(1), NumberInt(2)],
                               [2, NumberLong(1)],
                               undefined /*msg*/,
                               compareBinaryEqual);
        });
    });

    tests.push(function sameMembersDoesNotSortNestedArrays() {
        assert.throws(() => assert.sameMembers([[1, 2]], [[2, 1]]));
        assert.throws(() => {
            assert.sameMembers([{a: [{b: 0}, {b: 1, c: 0}], _id: 0}],
                               [{_id: 0, a: [{c: 0, b: 1}, {b: 0}]}]);
        });
    });

    tests.push(function sameMembersPassesWithEmptyArrays() {
        assert.sameMembers([], []);
    });

    tests.push(function sameMembersPassesSingleElement() {
        assert.sameMembers([1], [1]);
    });

    tests.push(function sameMembersPassesWithSameOrder() {
        assert.sameMembers([1, 2], [1, 2]);
        assert.sameMembers([1, 2, 3], [1, 2, 3]);
    });

    tests.push(function sameMembersPassesWithDifferentOrder() {
        assert.sameMembers([2, 1], [1, 2]);
        assert.sameMembers([1, 2, 3], [3, 1, 2]);
    });

    tests.push(function sameMembersPassesWithDuplicates() {
        assert.sameMembers([1, 1, 2], [1, 1, 2]);
        assert.sameMembers([1, 1, 2], [1, 2, 1]);
        assert.sameMembers([2, 1, 1], [1, 1, 2]);
    });

    tests.push(function sameMembersPassesWithSortedNestedArrays() {
        assert.sameMembers([[1, 2]], [[1, 2]]);
        assert.sameMembers([{a: [{b: 0}, {b: 1, c: 0}], _id: 0}],
                           [{_id: 0, a: [{b: 0}, {c: 0, b: 1}]}]);
    });

    tests.push(function sameMembersPassesWithObjects() {
        assert.sameMembers([{_id: 0, a: 0}], [{_id: 0, a: 0}]);
        assert.sameMembers([{_id: 0, a: 0}, {_id: 1}], [{_id: 0, a: 0}, {_id: 1}]);
        assert.sameMembers([{_id: 0, a: 0}, {_id: 1}], [{_id: 1}, {_id: 0, a: 0}]);
    });

    tests.push(function sameMembersPassesWithUnsortedObjects() {
        assert.sameMembers([{a: 0, _id: 1}], [{_id: 1, a: 0}]);
        assert.sameMembers([{a: [{b: 1, c: 0}], _id: 0}], [{_id: 0, a: [{c: 0, b: 1}]}]);
    });

    tests.push(function sameMembersPassesWithBSONTypes() {
        assert.sameMembers([new BinData(0, "JANgqwetkqwklEWRbWERKKJREtbq")],
                           [new BinData(0, "JANgqwetkqwklEWRbWERKKJREtbq")]);
        assert.sameMembers([new Timestamp(0, 1)], [new Timestamp(0, 1)]);
    });

    tests.push(function sameMembersPassesWithOtherTypes() {
        assert.sameMembers([null], [null]);
        assert.sameMembers([undefined], [undefined]);
        assert.sameMembers(["a"], ["a"]);
        assert.sameMembers([null, undefined, "a"], [undefined, "a", null]);
    });

    tests.push(function sameMembersDefaultCompareIsFriendly() {
        assert.sameMembers([NumberLong(1), NumberInt(2)], [2, 1]);
    });

    tests.push(function sameMembersPassesWithCustomCompareFn() {
        const compareBinaryEqual = (a, b) => bsonBinaryEqual(a, b);
        assert.sameMembers([[1, 2]], [[1, 2]], undefined /*msg*/, compareBinaryEqual);
        assert.sameMembers([NumberLong(1), NumberInt(2)],
                           [NumberInt(2), NumberLong(1)],
                           undefined /*msg*/,
                           compareBinaryEqual);
    });

    tests.push(function assertCallsHangAnalyzer() {
        function runAssertTest(f) {
            const oldMongoRunner = MongoRunner;
            let runs = 0;
            try {
                MongoRunner.runHangAnalyzer = function() {
                    ++runs;
                };
                f();
                assert(false);
            } catch (e) {
                assert.eq(runs, 1);
            } finally {
                MongoRunner = oldMongoRunner;
            }
        }
        runAssertTest(() => assert.soon(
                          () => false, 'assert message', kSmallTimeoutMS, kSmallRetryIntervalMS));
        runAssertTest(
            () => assert.retry(
                () => false, 'assert message', kDefaultRetryAttempts, kSmallRetryIntervalMS));
        runAssertTest(() => assert.time(() => sleep(5),
                                        'assert message',
                                        1 /* we certainly take less than this */));
    });

    /* main */

    tests.forEach((test) => {
        jsTest.log(`Starting tests '${test.name}'`);
        test();
    });
})();

/**
 * Tests that bsonObjToArray converts BSON objects to JS arrays.
 */

(function() {
'use strict';
const conn = MongoRunner.runMongod();
const db = conn.getDB('test');
const tests = [];

tests.push(function objToArrayOk() {
    assert.eq([1, 2], bsonObjToArray({"a": 1, "b": 2}));
});

tests.push(function sortKeyToArrayOk() {
    assert.commandWorked(db.test.insert({_id: 1, a: 2, b: 2, c: 3}));
    assert.commandWorked(db.test.insert({_id: 2, a: 2, b: 3, c: 4}));
    const findCommand = {
        find: 'test',
        projection: {sortKey: {$meta: 'sortKey'}, _id: 0, a: 0, b: 0, c: 0},
        sort: {a: 1, b: 1},
    };
    const res1 = new DBCommandCursor(db, db.runCommand(findCommand)).toArray();
    assert.eq([2, 2], bsonObjToArray(res1[0]["sortKey"]));
    assert.eq([2, 3], bsonObjToArray(res1[1]["sortKey"]));
});
tests.forEach((test) => {
    jsTest.log(`Starting test '${test.name}'`);
    test();
});

MongoRunner.stopMongod(conn);
})();
/**
 * Tests that write operations executed through the mongo shell's CRUD API are assigned a
 * transaction number so that they can be retried.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const db = primary.startSession({retryWrites: true}).getDatabase("test");
const coll = db.shell_can_retry_writes;

function testCommandCanBeRetried(func, expected = true) {
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

    let cmdName = Object.keys(cmdObjSeen)[0];

    // If the command is in a wrapped form, then we look for the actual command object inside
    // the query/$query object.
    if (cmdName === "query" || cmdName === "$query") {
        cmdObjSeen = cmdObjSeen[cmdName];
        cmdName = Object.keys(cmdObjSeen)[0];
    }

    assert(cmdObjSeen.hasOwnProperty("lsid"),
           "Expected operation " + tojson(cmdObjSeen) +
               " to have a logical session id: " + func.toString());

    if (expected) {
        assert(
            cmdObjSeen.hasOwnProperty("txnNumber"),
            "Expected operation " + tojson(cmdObjSeen) +
                " to be assigned a transaction number since it can be retried: " + func.toString());
    } else {
        assert(!cmdObjSeen.hasOwnProperty("txnNumber"),
               "Expected operation " + tojson(cmdObjSeen) +
                   " to not be assigned a transaction number since it cannot be retried: " +
                   func.toString());
    }
}

testCommandCanBeRetried(function() {
    coll.insertOne({_id: 0});
});

testCommandCanBeRetried(function() {
    coll.updateOne({_id: 0}, {$set: {a: 1}});
});

testCommandCanBeRetried(function() {
    coll.updateOne({_id: 1}, {$set: {a: 2}}, {upsert: true});
});

testCommandCanBeRetried(function() {
    coll.deleteOne({_id: 1});
});

testCommandCanBeRetried(function() {
    coll.insertMany([{_id: 2, b: 3}, {_id: 3, b: 4}], {ordered: true});
});

testCommandCanBeRetried(function() {
    coll.insertMany([{_id: 4}, {_id: 5}], {ordered: false});
});

testCommandCanBeRetried(function() {
    coll.updateMany({a: {$gt: 0}}, {$set: {c: 7}});
}, false);

testCommandCanBeRetried(function() {
    coll.deleteMany({b: {$lt: 5}});
}, false);

//
// Tests for writeConcern.
//

testCommandCanBeRetried(function() {
    coll.insertOne({_id: 1}, {w: 1});
});

testCommandCanBeRetried(function() {
    coll.insertOne({_id: "majority"}, {w: "majority"});
});

//
// Tests for bulkWrite().
//

testCommandCanBeRetried(function() {
    coll.bulkWrite([{insertOne: {document: {_id: 10}}}]);
});

testCommandCanBeRetried(function() {
    coll.bulkWrite([{updateOne: {filter: {_id: 10}, update: {$set: {a: 1}}}}]);
});

testCommandCanBeRetried(function() {
    coll.bulkWrite([{updateOne: {filter: {_id: 10}, update: {$set: {a: 2}}, upsert: true}}]);
});

testCommandCanBeRetried(function() {
    coll.bulkWrite([{deleteOne: {filter: {_id: 10}}}]);
});

testCommandCanBeRetried(function() {
    coll.bulkWrite(
        [{insertOne: {document: {_id: 20, b: 3}}}, {insertOne: {document: {_id: 30, b: 4}}}],
        {ordered: true});
});

testCommandCanBeRetried(function() {
    coll.bulkWrite([{insertOne: {document: {_id: 40}}}, {insertOne: {document: {_id: 50}}}],
                   {ordered: false});
});

testCommandCanBeRetried(function() {
    coll.bulkWrite([{updateMany: {filter: {a: {$gt: 0}}, update: {$set: {c: 7}}}}]);
}, false);

testCommandCanBeRetried(function() {
    coll.bulkWrite([{deleteMany: {filter: {b: {$lt: 5}}}}]);
}, false);

//
// Tests for wrappers around "findAndModify" command.
//

testCommandCanBeRetried(function() {
    coll.findOneAndUpdate({_id: 100}, {$set: {d: 9}}, {upsert: true});
});

testCommandCanBeRetried(function() {
    coll.findOneAndReplace({_id: 100}, {e: 11});
});

testCommandCanBeRetried(function() {
    coll.findOneAndDelete({e: {$exists: true}});
});

db.getSession().endSession();
rst.stopSet();
})();

/**
 * Tests that read operations executed through the mongo shell's API are specify afterClusterTime
 * when causal consistency is enabled.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

// This test makes assertions on commands run without logical session ids.
TestData.disableImplicitSessions = true;

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

function runTests({withSession}) {
    let db;

    if (withSession) {
        primary.setCausalConsistency(false);
        db = primary.startSession({causalConsistency: true}).getDatabase("test");
    } else {
        primary.setCausalConsistency(true);
        db = primary.getDB("test");
    }

    const coll = db.shell_can_use_read_concern;
    coll.drop();

    function testCommandCanBeCausallyConsistent(func, {
        expectedSession: expectedSession = withSession,
        expectedAfterClusterTime: expectedAfterClusterTime = true
    } = {}) {
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

        let cmdName = Object.keys(cmdObjSeen)[0];

        // If the command is in a wrapped form, then we look for the actual command object
        // inside
        // the query/$query object.
        if (cmdName === "query" || cmdName === "$query") {
            cmdObjSeen = cmdObjSeen[cmdName];
            cmdName = Object.keys(cmdObjSeen)[0];
        }

        if (expectedSession) {
            assert(cmdObjSeen.hasOwnProperty("lsid"),
                   "Expected operation " + tojson(cmdObjSeen) +
                       " to have a logical session id: " + func.toString());
        } else {
            assert(!cmdObjSeen.hasOwnProperty("lsid"),
                   "Expected operation " + tojson(cmdObjSeen) +
                       " to not have a logical session id: " + func.toString());
        }

        // Explain read concerns are on the inner command.
        if (cmdName === "explain") {
            cmdObjSeen = cmdObjSeen[cmdName];
        }

        if (expectedAfterClusterTime) {
            assert(cmdObjSeen.hasOwnProperty("readConcern"),
                   "Expected operation " + tojson(cmdObjSeen) +
                       " to have a readConcern object since it can be causally consistent: " +
                       func.toString());

            const readConcern = cmdObjSeen.readConcern;
            assert(readConcern.hasOwnProperty("afterClusterTime"),
                   "Expected operation " + tojson(cmdObjSeen) +
                       " to specify afterClusterTime since it can be causally consistent: " +
                       func.toString());
        } else {
            assert(!cmdObjSeen.hasOwnProperty("readConcern"),
                   "Expected operation " + tojson(cmdObjSeen) + " to not have a readConcern" +
                       " object since it cannot be causally consistent: " + func.toString());
        }
    }

    //
    // Tests for the "find" and "getMore" commands.
    //

    {
        testCommandCanBeCausallyConsistent(function() {
            assert.commandWorked(coll.insert([{}, {}, {}, {}, {}]));
        }, {expectedSession: withSession, expectedAfterClusterTime: false});

        testCommandCanBeCausallyConsistent(function() {
            assert.commandWorked(
                db.runCommand({find: coll.getName(), batchSize: 5, singleBatch: true}));
        });

        const cursor = coll.find().batchSize(2);

        testCommandCanBeCausallyConsistent(function() {
            cursor.next();
            cursor.next();
        });

        testCommandCanBeCausallyConsistent(function() {
            cursor.next();
            cursor.next();
            cursor.next();
            assert(!cursor.hasNext());
        }, {
            expectedSession: withSession,
            expectedAfterClusterTime: false,
        });
    }

    //
    // Tests for the "count" command.
    //

    testCommandCanBeCausallyConsistent(function() {
        assert.commandWorked(db.runCommand({count: coll.getName()}));
    });

    testCommandCanBeCausallyConsistent(function() {
        assert.commandWorked(db.runCommand({query: {count: coll.getName()}}));
    });

    testCommandCanBeCausallyConsistent(function() {
        assert.commandWorked(db.runCommand({$query: {count: coll.getName()}}));
    });

    testCommandCanBeCausallyConsistent(function() {
        assert.eq(5, coll.count());
    });

    //
    // Tests for the "distinct" command.
    //

    testCommandCanBeCausallyConsistent(function() {
        assert.commandWorked(db.runCommand({distinct: coll.getName(), key: "_id"}));
    });

    testCommandCanBeCausallyConsistent(function() {
        const values = coll.distinct("_id");
        assert.eq(5, values.length, tojson(values));
    });

    //
    // Tests for the "aggregate" command.
    //

    {
        testCommandCanBeCausallyConsistent(function() {
            assert.commandWorked(
                db.runCommand({aggregate: coll.getName(), pipeline: [], cursor: {batchSize: 5}}));
        });

        testCommandCanBeCausallyConsistent(function() {
            assert.commandWorked(db.runCommand(
                {aggregate: coll.getName(), pipeline: [], cursor: {batchSize: 5}, explain: true}));
        });

        let cursor;

        testCommandCanBeCausallyConsistent(function() {
            cursor = coll.aggregate([], {cursor: {batchSize: 2}});
            cursor.next();
            cursor.next();
        });

        testCommandCanBeCausallyConsistent(function() {
            cursor.next();
            cursor.next();
            cursor.next();
            assert(!cursor.hasNext());
        }, {
            expectedSession: withSession,
            expectedAfterClusterTime: false,
        });
    }

    //
    // Tests for the "explain" command.
    //

    testCommandCanBeCausallyConsistent(function() {
        assert.commandWorked(db.runCommand({explain: {find: coll.getName()}}));
    });

    testCommandCanBeCausallyConsistent(function() {
        coll.find().explain();
    });

    testCommandCanBeCausallyConsistent(function() {
        coll.explain().find().finish();
    });

    db.getSession().endSession();
}

runTests({withSession: false});
runTests({withSession: true});

rst.stopSet();
})();

/**
 * Tests that the shell correctly checks for program extensions in Windows environments.
 */

(function() {
'use strict';

if (_isWindows()) {
    const filename = 'jstests/noPassthrough/libs/testWindowsExtension.bat';

    clearRawMongoProgramOutput();
    const result = runMongoProgram(filename);
    assert.eq(result, 42);
} else {
    jsTestLog("This test is only relevant for Windows environments.");
}
})();

/**
 * Tests for the command assertion functions in mongo/shell/assert.js.
 */

(function() {
"use strict";

const conn = MongoRunner.runMongod();
const db = conn.getDB("commandAssertions");
const kFakeErrCode = 1234567890;
const tests = [];

const sampleWriteConcernError = {
    n: 1,
    ok: 1,
    writeConcernError: {
        code: ErrorCodes.WriteConcernFailed,
        codeName: "WriteConcernFailed",
        errmsg: "waiting for replication timed out",
        errInfo: {
            wtimeout: true,
        },
    },
};

function setup() {
    db.coll.drop();
    assert.commandWorked(db.coll.insert({_id: 1}));
}

// Raw command responses.
tests.push(function rawCommandOk() {
    const res = db.runCommand({"ping": 1});
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
});

function _assertMsgFunctionExecution(
    assertFunc, assertParameter, {expectException: expectException = false} = {}) {
    var msgFunctionCalled = false;
    var expectedAssert = assert.doesNotThrow;

    if (expectException) {
        expectedAssert = assert.throws;
    }

    expectedAssert(() => {
        assertFunc(assertParameter, () => {
            msgFunctionCalled = true;
        });
    });

    assert.eq(expectException, msgFunctionCalled, "msg function execution should match assertion");
}

tests.push(function msgFunctionOnlyCalledOnFailure() {
    const res = db.runCommand({"ping": 1});

    _assertMsgFunctionExecution(assert.commandWorked, res, {expectException: false});
    _assertMsgFunctionExecution(
        assert.commandWorkedIgnoringWriteErrors, res, {expectException: false});
    _assertMsgFunctionExecution(assert.commandFailed, res, {expectException: true});

    var msgFunctionCalled = false;
    assert.throws(() => assert.commandFailedWithCode(res, 0, () => {
        msgFunctionCalled = true;
    }));
    assert.eq(true, msgFunctionCalled, "msg function execution should match assertion");
});

tests.push(function rawCommandErr() {
    const res = db.runCommand({"IHopeNobodyEverMakesThisACommand": 1});
    assert.throws(() => assert.commandWorked(res));
    assert.throws(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.CommandNotFound));
    // commandFailedWithCode should succeed if any of the passed error codes are matched.
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.CommandNotFound, kFakeErrCode]));
    assert.doesNotThrow(() => assert.commandWorkedOrFailedWithCode(
                            res,
                            [ErrorCodes.CommandNotFound, kFakeErrCode],
                            "threw even though failed with correct error codes"));
    assert.throws(
        () => assert.commandWorkedOrFailedWithCode(
            res, [kFakeErrCode], "didn't throw even though failed with incorrect error code"));
});

tests.push(function rawCommandWriteOk() {
    const res = db.runCommand({insert: "coll", documents: [{_id: 2}]});
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
    assert.doesNotThrow(
        () => assert.commandWorkedOrFailedWithCode(res, 0, "threw even though succeeded"));
});

tests.push(function rawCommandWriteErr() {
    const res = db.runCommand({insert: "coll", documents: [{_id: 1}]});
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
    assert.throws(
        () => assert.commandWorkedOrFailedWithCode(
            res, [ErrorCodes.DuplicateKey, kFakeErrCode], "expected to throw on write error"));
    assert.throws(() => assert.commandWorkedOrFailedWithCode(
                      res, [kFakeErrCode], "expected to throw on write error"));
});

tests.push(function collInsertWriteOk() {
    const res = db.coll.insert({_id: 2});
    assert(res instanceof WriteResult);
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
});

tests.push(function collInsertWriteErr() {
    const res = db.coll.insert({_id: 1});
    assert(res instanceof WriteResult);
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

tests.push(function collMultiInsertWriteOk() {
    const res = db.coll.insert([{_id: 3}, {_id: 2}]);
    assert(res instanceof BulkWriteResult);
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
    assert.throws(() =>
                      assert.commandWorkedOrFailedWithCode(res, 0, "threw even though succeeded"));
});

tests.push(function collMultiInsertWriteErr() {
    const res = db.coll.insert([{_id: 1}, {_id: 2}]);
    assert(res instanceof BulkWriteResult);
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

// Test when the insert command fails with ok:0 (i.e. not failing due to write err)
tests.push(function collInsertCmdErr() {
    const res = db.coll.insert({x: 1}, {writeConcern: {"bad": 1}});
    assert(res instanceof WriteCommandError);
    assert.throws(() => assert.commandWorked(res));
    assert.throws(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
});

tests.push(function collMultiInsertCmdErr() {
    const res = db.coll.insert([{x: 1}, {x: 2}], {writeConcern: {"bad": 1}});
    assert(res instanceof WriteCommandError);
    assert.throws(() => assert.commandWorked(res));
    assert.throws(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
});

tests.push(function mapReduceOk() {
    const res = db.coll.mapReduce(
        function() {
            emit(this._id, 0);
        },
        function(k, v) {
            return v[0];
        },
        {out: "coll_out"});
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
});

tests.push(function crudInsertOneOk() {
    const res = db.coll.insertOne({_id: 2});
    assert(res.hasOwnProperty("acknowledged"));
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
});

tests.push(function crudInsertOneErr() {
    let threw = false;
    let res = null;
    try {
        db.coll.insertOne({_id: 1});
    } catch (e) {
        threw = true;
        res = e;
    }
    assert(threw);
    assert(res instanceof WriteError);
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

tests.push(function crudInsertManyOk() {
    const res = db.coll.insertMany([{_id: 2}, {_id: 3}]);
    assert(res.hasOwnProperty("acknowledged"));
    assert.doesNotThrow(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.throws(() => assert.commandFailed(res));
    assert.throws(() => assert.commandFailedWithCode(res, 0));
});

tests.push(function crudInsertManyErr() {
    let threw = false;
    let res = null;
    try {
        db.coll.insertMany([{_id: 1}, {_id: 2}]);
    } catch (e) {
        threw = true;
        res = e;
    }
    assert(threw);
    assert(res instanceof BulkWriteError);
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

tests.push(function rawMultiWriteErr() {
    // Do an unordered bulk insert with duplicate keys to produce multiple write errors.
    const res = db.runCommand({"insert": "coll", documents: [{_id: 1}, {_id: 1}], ordered: false});
    assert(res.writeErrors.length == 2, "did not get multiple write errors");
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

tests.push(function bulkMultiWriteErr() {
    // Do an unordered bulk insert with duplicate keys to produce multiple write errors.
    const res = db.coll.insert([{_id: 1}, {_id: 1}], {ordered: false});
    assert.throws(() => assert.commandWorked(res));
    assert.doesNotThrow(() => assert.commandWorkedIgnoringWriteErrors(res));
    assert.doesNotThrow(() => assert.commandFailed(res));
    assert.doesNotThrow(() => assert.commandFailedWithCode(res, ErrorCodes.DuplicateKey));
    assert.doesNotThrow(
        () => assert.commandFailedWithCode(res, [ErrorCodes.DuplicateKey, kFakeErrCode]));
});

tests.push(function writeConcernErrorCausesCommandWorkedToAssert() {
    const result = sampleWriteConcernError;

    assert.throws(() => {
        assert.commandWorked(result);
    });
});

tests.push(function writeConcernErrorCausesCommandFailedToPass() {
    const result = sampleWriteConcernError;

    assert.doesNotThrow(() => {
        assert.commandFailed(result);
        assert.commandFailedWithCode(result, ErrorCodes.WriteConcernFailed);
    });
});

tests.push(function writeConcernErrorCanBeIgnored() {
    const result = sampleWriteConcernError;

    assert.doesNotThrow(() => {
        assert.commandWorkedIgnoringWriteConcernErrors(result);
    });
});

tests.push(function invalidResponsesAttemptToProvideInformationToCommandWorks() {
    const invalidResponses = [undefined, 'not a valid response', 42];

    invalidResponses.forEach((invalidRes) => {
        const error = assert.throws(() => {
            assert.commandWorked(invalidRes);
        });

        assert.gte(error.message.indexOf(invalidRes), 0);
        assert.gte(error.message.indexOf(typeof invalidRes), 0);
    });
});

tests.push(function invalidResponsesAttemptToProvideInformationCommandFailed() {
    const invalidResponses = [undefined, 'not a valid response', 42];

    invalidResponses.forEach((invalidRes) => {
        const error = assert.throws(() => {
            assert.commandFailed(invalidRes);
        });

        assert.gte(error.message.indexOf(invalidRes), 0);
        assert.gte(error.message.indexOf(typeof invalidRes), 0);
    });
});

tests.push(function assertCallsHangAnalyzer() {
    function runAssertTest(f, expectCall) {
        const oldMongoRunner = MongoRunner;
        let runs = 0;
        try {
            MongoRunner.runHangAnalyzer = function() {
                ++runs;
            };
            f();
            assert(false);
        } catch (e) {
            if (expectCall) {
                assert.eq(runs, 1);
            } else {
                assert.eq(runs, 0);
            }
        } finally {
            MongoRunner = oldMongoRunner;
        }
    }
    const nonTimeOutWriteConcernError = {
        n: 1,
        ok: 1,
        writeConcernError: {
            code: ErrorCodes.WriteConcernFailed,
            codeName: "WriteConcernFailed",
            errmsg: "foo",
        },
    };

    const lockTimeoutError = {
        ok: 0,
        errmsg: "Unable to acquire lock",
        code: ErrorCodes.LockTimeout,
        codeName: "LockTimeout",
    };

    const lockTimeoutTransientTransactionError = {
        errorLabels: ["TransientTransactionError"],
        ok: 0,
        errmsg: "Unable to acquire lock",
        code: ErrorCodes.LockTimeout,
        codeName: "LockTimeout",
    };

    runAssertTest(() => assert.commandWorked(sampleWriteConcernError), true);
    runAssertTest(() => assert.commandWorked(nonTimeOutWriteConcernError), false);

    runAssertTest(() => assert.commandFailed(sampleWriteConcernError), false);

    runAssertTest(
        () => assert.commandFailedWithCode(sampleWriteConcernError, ErrorCodes.DuplicateKey), true);
    runAssertTest(
        () => assert.commandFailedWithCode(nonTimeOutWriteConcernError, ErrorCodes.DuplicateKey),
        false);
    runAssertTest(
        () => assert.commandFailedWithCode(sampleWriteConcernError, ErrorCodes.WriteConcernFailed),
        false);

    runAssertTest(() => assert.commandWorkedIgnoringWriteConcernErrors(sampleWriteConcernError),
                  false);

    runAssertTest(() => assert.commandWorked(lockTimeoutError), true);
    runAssertTest(() => assert.commandFailed(lockTimeoutError), false);
    runAssertTest(() => assert.commandFailedWithCode(lockTimeoutError, ErrorCodes.DuplicateKey),
                  true);
    runAssertTest(() => assert.commandFailedWithCode(lockTimeoutError, ErrorCodes.LockTimeout),
                  false);

    runAssertTest(() => assert.commandWorked(lockTimeoutTransientTransactionError), false);
    runAssertTest(() => assert.commandFailed(lockTimeoutTransientTransactionError), false);
    runAssertTest(() => assert.commandFailedWithCode(lockTimeoutTransientTransactionError,
                                                     ErrorCodes.DuplicateKey),
                  false);
    runAssertTest(() => assert.commandFailedWithCode(lockTimeoutTransientTransactionError,
                                                     ErrorCodes.LockTimeout),
                  false);
});

tests.forEach((test) => {
    jsTest.log(`Starting test '${test.name}'`);
    setup();
    const oldMongoRunner = MongoRunner;
    try {
        // We shouldn't actually run the hang-analyzer for these tests.
        MongoRunner.runHangAnalyzer = Function.prototype;
        test();
    } finally {
        MongoRunner = oldMongoRunner;
    }
});

/* cleanup */
MongoRunner.stopMongod(conn);
})();

/**
 * Tests readConcern level snapshot outside of transactions.
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

const collName = "coll";
const primaryDB = replSet.getPrimary().getDB('test');
const collection = primaryDB[collName];

const docs = [...Array(10).keys()].map((i) => ({"_id": i}));
const insertTimestamp =
    assert.commandWorked(primaryDB.runCommand({insert: collName, documents: docs})).operationTime;
jsTestLog("Inserted 10 documents at: " + tojson(insertTimestamp));

// Test find with atClusterTime.
let cursor = collection.find().readConcern("snapshot", insertTimestamp);
assert.eq(cursor.getClusterTime(), insertTimestamp);

cursor = collection.find().readConcern("snapshot", insertTimestamp).batchSize(2);
cursor.next();
cursor.next();
assert.eq(cursor.objsLeftInBatch(), 0);
// This triggers a getMore.
cursor.next();
// Test that the read timestamp remains the same after the getMore.
assert.eq(cursor.getClusterTime(), insertTimestamp);

// Test find with snapshot readConcern.
cursor = collection.find().readConcern("snapshot");
// During primary stepup, we will rebuild PrimaryOnlyService instances, which requires creating
// indexes on certain collections. If a createCollection occurred after the insert of 10 documents,
// it is possible that the committed snapshot advanced past 'insertTimestamp'. Therefore, this read
// could be reading at a newer snapshot since we did not specify a specific 'atClusterTime'.
assert.gte(cursor.getClusterTime(), insertTimestamp);

// Test find with non-snapshot readConcern.
cursor = collection.find();
assert.eq(cursor.getClusterTime(), undefined);

// Test aggregate with atClusterTime.
cursor = collection.aggregate([{$sort: {_id: 1}}],
                              {readConcern: {level: "snapshot", atClusterTime: insertTimestamp}});
assert.eq(cursor.getClusterTime(), insertTimestamp);

// Test aggregate with snapshot readConcern. Similarly to the find with snapshot readConcern and no
// 'atClusterTime', it's possible that this aggregate can read at a newer snapshot than
// 'insertTimestamp'.
cursor = collection.aggregate([{$sort: {_id: 1}}], {readConcern: {level: "snapshot"}});
assert.gte(cursor.getClusterTime(), insertTimestamp);

// Test aggregate with non-snapshot readConcern.
cursor = collection.aggregate([{$sort: {_id: 1}}]);
assert.eq(cursor.getClusterTime(), undefined);

replSet.stopSet();
})();

/**
 * Tests for the ErrorCodes objects in error_codes.js generated file.
 */
(() => {
    "use strict";

    const tests = [];

    const nonExistingErrorCode = 999999999;

    tests.push(function errorCodesShouldThrowExceptionForNonExistingError() {
        assert.throws(() => {
            return ErrorCodes.thisIsAnErrorCodeThatDoesNotExist;
        });
    });

    tests.push(function errorCodesShouldNotThrowExceptionForExistingError() {
        assert.doesNotThrow(() => {
            return ErrorCodes.BadValue;
        });
    });

    tests.push(function errorCodesShouldNotThrowExceptionForInheritedAttributes() {
        assert.doesNotThrow(() => {
            return ErrorCodes.prototype;
        });
    });

    tests.push(function errorCodesShouldNotThrowExceptionForSymbols() {
        assert.doesNotThrow(() => {
            return print(+ErrorCodes);
        });
    });

    tests.push(function errorCodesShouldNotThrowExceptionForConstructor() {
        assert.doesNotThrow(() => {
            return ErrorCodes.constructor;
        });
    });

    tests.push(function errorCodeStringsShouldThrowExceptionForNonExistingError() {
        assert.throws(() => {
            return ErrorCodeStrings[nonExistingErrorCode];
        });
    });

    tests.push(function errorCodeStringsShouldNotThrowExceptionForExistingError() {
        assert.doesNotThrow(() => {
            return ErrorCodeStrings[2];
        });
    });

    tests.push(function errorCodesShouldHaveCategoriesDefined() {
        assert.eq(true, ErrorCodes.isNetworkError(ErrorCodes.HostNotFound));
    });

    tests.push(function errorCodesCategoriesShouldReturnFalseOnNonExistingErrorCodes() {
        assert.eq(false, ErrorCodes.isNetworkError(nonExistingErrorCode));
    });

    /* main */
    tests.forEach((test) => {
        jsTest.log(`Starting tests '${test.name}'`);
        test();
    });
})();

/**
 * Tests that the mongo shell gossips the greater of the client's clusterTime and the session's
 * clusterTime.
 * @tags: [requires_replication]
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const session1 = primary.startSession();
const session2 = primary.startSession();

const db = primary.getDB("test");
const coll = db.shell_gossip_cluster_time;

function testCommandGossipedWithClusterTime(func, expectedClusterTime) {
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

    let cmdName = Object.keys(cmdObjSeen)[0];

    // If the command is in a wrapped form, then we look for the actual command object inside
    // the query/$query object.
    if (cmdName === "query" || cmdName === "$query") {
        cmdObjSeen = cmdObjSeen[cmdName];
        cmdName = Object.keys(cmdObjSeen)[0];
    }

    if (expectedClusterTime === undefined) {
        assert(!cmdObjSeen.hasOwnProperty("$clusterTime"),
               "Expected operation " + tojson(cmdObjSeen) +
                   " to not have a $clusterTime object: " + func.toString());
    } else {
        assert(cmdObjSeen.hasOwnProperty("$clusterTime"),
               "Expected operation " + tojson(cmdObjSeen) +
                   " to have a $clusterTime object: " + func.toString());

        assert(bsonBinaryEqual(expectedClusterTime, cmdObjSeen.$clusterTime));
    }
}

assert(session1.getClusterTime() === undefined,
       "session1 has yet to be used, but has clusterTime: " + tojson(session1.getClusterTime()));
assert(session2.getClusterTime() === undefined,
       "session2 has yet to be used, but has clusterTime: " + tojson(session2.getClusterTime()));

// Advance the clusterTime outside of either of the sessions.
testCommandGossipedWithClusterTime(function() {
    assert.commandWorked(coll.insert({}));
}, primary.getClusterTime());

assert(session1.getClusterTime() === undefined,
       "session1 has yet to be used, but has clusterTime: " + tojson(session1.getClusterTime()));
assert(session2.getClusterTime() === undefined,
       "session2 has yet to be used, but has clusterTime: " + tojson(session2.getClusterTime()));

// Performing an operation with session1 should use the highest clusterTime seen by the client
// since session1 hasn't been used yet.
testCommandGossipedWithClusterTime(function() {
    const coll = session1.getDatabase("test").mycoll;
    assert.commandWorked(coll.insert({}));
}, primary.getClusterTime());

assert.eq(session1.getClusterTime(), primary.getClusterTime());

testCommandGossipedWithClusterTime(function() {
    const coll = session1.getDatabase("test").mycoll;
    assert.commandWorked(coll.insert({}));
}, session1.getClusterTime());

assert(session2.getClusterTime() === undefined,
       "session2 has yet to be used, but has clusterTime: " + tojson(session2.getClusterTime()));

primary.resetClusterTime_forTesting();
assert(primary.getClusterTime() === undefined,
       "client's cluster time should have been reset, but has clusterTime: " +
           tojson(primary.getClusterTime()));

// Performing an operation with session2 should use the highest clusterTime seen by session2
// since the client's clusterTime has been reset.
session2.advanceClusterTime(session1.getClusterTime());
testCommandGossipedWithClusterTime(function() {
    const coll = session2.getDatabase("test").mycoll;
    assert.commandWorked(coll.insert({}));
}, session2.getClusterTime());

assert.eq(session2.getClusterTime(), primary.getClusterTime());

primary.resetClusterTime_forTesting();
assert(primary.getClusterTime() === undefined,
       "client's cluster time should have been reset, but has clusterTime: " +
           tojson(primary.getClusterTime()));

// Performing an operation with session2 should use the highest clusterTime seen by session2
// since the highest clusterTime seen by session1 is behind that of session2's.
primary.advanceClusterTime(session1.getClusterTime());
testCommandGossipedWithClusterTime(function() {
    const coll = session2.getDatabase("test").mycoll;
    assert.commandWorked(coll.insert({}));
}, session2.getClusterTime());

rst.stopSet();
})();

/**
 * Tests that shellHelper.use() updates the global 'db' object.
 */

// We explicitly declare the global 'db' object since the rest of the test runs with strict-mode
// enabled.
var db;

(function() {
"use strict";

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, "mongod was unable to start up");

db = conn.getDB("db1");
assert.eq("db1", db.getName());

// Tests that shellHelper.use() updates the global 'db' object to refer to a DB object with the
// database name specified.
shellHelper.use("db2");
assert.eq("db2", db.getName());

// Replace the global 'db' object with a DB object from a new session and verify that
// shellHelper.use() still works.
db = conn.startSession().getDatabase("db1");
assert.eq("db1", db.getName());

const session = db.getSession();

// Tests that shellHelper.use() updates the global 'db' object to refer to a DB object with the
// database name specified. The DB objects should have the same underlying DriverSession object.
shellHelper.use("db2");
assert.eq("db2", db.getName());

assert(session === db.getSession(), "session wasn't inherited as part of switching databases");

session.endSession();
MongoRunner.stopMongod(conn);
})();

// Test that when running the shell for the first time creates the ~/.dbshell file, and it has
// appropriate permissions (where relevant).

(function() {
"use strict";

// Use dataPath because it includes the trailing "/" or "\".
var tmpHome = MongoRunner.dataPath;
// Ensure it exists and is a dir (eg. if running without resmoke.py and /data/db doesn't exist).
mkdir(tmpHome);
removeFile(tmpHome + ".dbshell");

var args = [];
var cmdline = "mongo --nodb";
var redirection = "";
var env = {};
if (_isWindows()) {
    args.push("cmd.exe");
    args.push("/c");
    cmdline = cmdline.replace("mongo", "mongo.exe");

    // Input is set to NUL.  The output must also be redirected to NUL, otherwise running the
    // jstest manually has strange terminal IO behaviour.
    redirection = "< NUL > NUL";

    // USERPROFILE set to the tmp homedir.
    // Since NUL is a character device, isatty() will return true, which means that .mongorc.js
    // will be created in the HOMEDRIVE + HOMEPATH location, so we must set them also.
    if (tmpHome.match("^[a-zA-Z]:")) {
        var tmpHomeDrive = tmpHome.substr(0, 2);
        var tmpHomePath = tmpHome.substr(2);
    } else {
        var _pwd = pwd();
        assert(_pwd.match("^[a-zA-Z]:"), "pwd must include drive");
        var tmpHomeDrive = _pwd.substr(0, 2);
        var tmpHomePath = tmpHome;
    }
    env = {USERPROFILE: tmpHome, HOMEDRIVE: tmpHomeDrive, HOMEPATH: tmpHomePath};

} else {
    args.push("sh");
    args.push("-c");

    // Use the mongo shell from the $PATH, Resmoke sets $PATH to
    // include all the mongo binaries first.
    cmdline = cmdline;

    // Set umask to 0 prior to running the shell.
    cmdline = "umask 0 ; " + cmdline;

    // stdin is /dev/null.
    redirection = "< /dev/null";

    // HOME set to the tmp homedir.
    if (!tmpHome.startsWith("/")) {
        tmpHome = pwd() + "/" + tmpHome;
    }
    env = {HOME: tmpHome};
}

// Add redirection to cmdline, and add cmdline to args.
cmdline += " " + redirection;
args.push(cmdline);
jsTestLog("Running args:\n    " + tojson(args) + "\nwith env:\n    " + tojson(env));
var pid = _startMongoProgram({args, env});
var rc = waitProgram(pid);

assert.eq(rc, 0);

var files = listFiles(tmpHome);
jsTestLog(tojson(files));

var findFile = function(baseName) {
    for (var i = 0; i < files.length; i++) {
        if (files[i].baseName === baseName) {
            return files[i];
        }
    }
    return undefined;
};

var targetFile = ".dbshell";
var file = findFile(targetFile);

assert.neq(typeof (file), "undefined", targetFile + " should exist, but it doesn't");
assert.eq(file.isDirectory, false, targetFile + " should not be a directory, but it is");
assert.eq(file.size, 0, targetFile + " should be empty, but it isn't");

if (!_isWindows()) {
    // On Unix, check that the file has the correct mode (permissions).
    // The shell has no way to stat a file.
    // There is no stat utility in POSIX.
    // `ls -l` is POSIX, so this is the best that we have.
    // Check for exactly "-rw-------".
    clearRawMongoProgramOutput();
    var rc = runProgram("ls", "-l", file.name);
    assert.eq(rc, 0);

    var output = rawMongoProgramOutput();
    var fields = output.split(" ");
    // First field is the prefix, second field is the `ls -l` permissions.
    assert.eq(fields[1].substr(0, 10), "-rw-------", targetFile + " has bad permissions");
}
})();

// Test that isInteractive() returns false when running script or --eval
// and true when running in interactive mode

(function() {
"use strict";

if (!_isWindows()) {
    clearRawMongoProgramOutput();
    var rc = runProgram("mongo", "--nodb", "--quiet", "--eval", "print(isInteractive())");
    assert.eq(rc, 0);
    var output = rawMongoProgramOutput();
    var response = (output.split('\n').slice(-2)[0]).split(' ')[1];
    assert.eq(response, "false", "Expected 'false' in script mode");
    // now try interactive
    clearRawMongoProgramOutput();
    rc = runProgram(
        "mongo", "--nodb", "--quiet", "--shell", "--eval", "print(isInteractive()); quit()");
    assert.eq(rc, 0);
    output = rawMongoProgramOutput();
    response = (output.split('\n').slice(-2)[0]).split(' ')[1];
    assert.eq(response, "true", "Expected 'true' in interactive mode");
}
})();

/**
 * Tests the exception handling behavior of the load() function across nested calls.
 */
(function() {
"use strict";

let isMain = true;

if (TestData.hasOwnProperty("loadDepth")) {
    isMain = false;
    ++TestData.loadDepth;
} else {
    TestData.loadDepth = 0;
    TestData.loadErrors = [];
}

if (TestData.loadDepth >= 3) {
    throw new Error("Intentionally thrown");
}

try {
    load("jstests/noPassthrough/shell_load_file.js");
} catch (e) {
    TestData.loadErrors.push(e);

    if (!isMain) {
        throw e;
    }
}

assert(isMain, "only the root caller of load() needs to check the generated JavaScript exceptions");

for (let i = 0; i < TestData.loadErrors.length; ++i) {
    const error = TestData.loadErrors[i];
    assert.eq("error loading js file: jstests/noPassthrough/shell_load_file.js", error.message);
    assert(
        /@jstests\/noPassthrough\/shell_load_file.js:/.test(error.stack) ||
            /@jstests\\noPassthrough\\shell_load_file.js:/.test(error.stack),
        () => "JavaScript stacktrace from load() didn't include file paths (AKA stack frames): " +
            error.stack);
}
})();

/**
 * Tests that the ports assigned to the mongobridge and mongod/mongos processes make it easy to
 * reason about which mongobridge process corresponds to a particular mongod/mongos process in the
 * logs.
 *
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

function checkBridgeOffset(node, processType) {
    const bridgePort = node.port;
    const serverPort = assert.commandWorked(node.adminCommand({getCmdLineOpts: 1})).parsed.net.port;
    assert.neq(bridgePort,
               serverPort,
               node + " is a connection to " + processType + " rather than to mongobridge");
    assert.eq(bridgePort + MongoBridge.kBridgeOffset,
              serverPort,
              "corresponding mongobridge and " + processType +
                  " ports should be staggered by a multiple of 10");
}

// We use >5 nodes to ensure that allocating twice as many ports doesn't interfere with having
// the corresponding mongobridge and mongod ports staggered by a multiple of 10.
const rst = new ReplSetTest({nodes: 7, useBridge: true});
rst.startSet();

// Rig the election so that the primary remains stable throughout this test despite the replica
// set having a larger number of members.
const replSetConfig = rst.getReplSetConfig();
for (let i = 1; i < rst.nodes.length; ++i) {
    replSetConfig.members[i].priority = 0;
    replSetConfig.members[i].votes = 0;
}
rst.initiate(replSetConfig);

for (let node of rst.nodes) {
    checkBridgeOffset(node, "mongod");
}

rst.stopSet();

// We run ShardingTest under mongobridge with 1-node replica set shards
resetAllocatedPorts();

const numMongos = 5;
const numShards = 5;
const st = new ShardingTest({
    mongos: numMongos,
    shards: numShards,
    config: {nodes: 1},
    rs: {nodes: 1},
    useBridge: true,
});

for (let i = 0; i < numMongos; ++i) {
    checkBridgeOffset(st["s" + i], "mongos");
}

for (let configServer of st.configRS.nodes) {
    checkBridgeOffset(configServer, "config server");
}

for (let i = 0; i < numShards; ++i) {
    for (let node of st["rs" + i].nodes) {
        checkBridgeOffset(node, "shard");
    }
}

st.stop();
})();

// @tags: [
//   uses_parallel_shell,
// ]

// SERVER-48068: check that runningChildPids doesn't unregister a pid multiple
// times from the registry
// Verify that an invariant failure doesn't occur in the program registry
try {
    var cleanup = startParallelShell("MongoRunner.runningChildPids();", undefined, true);
    var cleanup2 = startParallelShell("MongoRunner.runningChildPids();", undefined, true);
    sleep(5000);

    try {
        MongoRunner.runningChildPids();
        throw new Error('Simulating assert.soon() failure');
    } finally {
        cleanup();
        cleanup2();
    }

} catch (e) {
    assert.eq(e instanceof Error, true);
    assert.eq(e.message, 'Simulating assert.soon() failure');
}

print("shell_parallel_wait_for_pid.js SUCCESS");

(function() {
'use strict';
var checkShell = function(retCode) {
    var args = [
        "mongo",
        "--nodb",
        "--eval",
        "quit(" + retCode + ");",
    ];

    var actualRetCode = _runMongoProgram.apply(null, args);
    assert.eq(retCode, actualRetCode);
};

checkShell(0);
checkShell(5);
})();

/**
 * Tests that the mongo shell retries exactly once on retryable errors.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");
load("jstests/libs/write_concern_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = jsTest.name();

const rsConn = new Mongo(rst.getURL());
const db = rsConn.startSession({retryWrites: true}).getDatabase(dbName);

// We configure the mongo shell to log its retry attempts so there are more diagnostics
// available in case this test ever fails.
TestData.logRetryAttempts = true;

/**
 * The testCommandIsRetried() function serves as the fixture for writing test cases which run
 * commands against the server and assert that the mongo shell retries them correctly.
 *
 * The 'testFn' parameter is a function that performs an arbitrary number of operations against
 * the database. The command requests that the mongo shell attempts to send to the server
 * (including any command requests which are retried) are then specified as the sole argument to
 * the 'assertFn' parameter.
 *
 * The testFn(enableCapture, disableCapture) function can also selectively turn on and off the
 * capturing of command requests by calling the functions it receives for its first and second
 * parameters, respectively.
 */
function testCommandIsRetried(testFn, assertFn) {
    const mongoRunCommandOriginal = Mongo.prototype.runCommand;
    const cmdObjsSeen = [];

    let shouldCaptureCmdObjs = true;

    Mongo.prototype.runCommand = function runCommandSpy(dbName, cmdObj, options) {
        if (shouldCaptureCmdObjs) {
            cmdObjsSeen.push(cmdObj);
        }

        return mongoRunCommandOriginal.apply(this, arguments);
    };

    try {
        assert.doesNotThrow(() => testFn(
                                () => {
                                    shouldCaptureCmdObjs = true;
                                },
                                () => {
                                    shouldCaptureCmdObjs = false;
                                }));
    } finally {
        Mongo.prototype.runCommand = mongoRunCommandOriginal;
    }

    if (cmdObjsSeen.length === 0) {
        throw new Error("Mongo.prototype.runCommand() was never called: " + testFn.toString());
    }

    assertFn(cmdObjsSeen);
}

testCommandIsRetried(
    function testInsertRetriedOnWriteConcernError(enableCapture, disableCapture) {
        disableCapture();
        const secondary = rst.getSecondary();
        stopServerReplication(secondary);

        try {
            enableCapture();
            const res = db[collName].insert({}, {writeConcern: {w: 2, wtimeout: 1000}});
            assert.commandFailedWithCode(res, ErrorCodes.WriteConcernFailed);
            disableCapture();
        } finally {
            // We disable the failpoint in a finally block to prevent issues arising from shutting
            // down the secondary with the failpoint enabled.
            restartServerReplication(secondary);
        }
    },
    function assertInsertRetriedExactlyOnce(cmdObjsSeen) {
        assert.eq(2, cmdObjsSeen.length, () => tojson(cmdObjsSeen));
        assert(cmdObjsSeen.every(cmdObj => Object.keys(cmdObj)[0] === "insert"),
               () => "expected both attempts to be insert requests: " + tojson(cmdObjsSeen));
        assert.eq(cmdObjsSeen[0], cmdObjsSeen[1], "command request changed between retry attempts");
    });

testCommandIsRetried(
    function testUpdateRetriedOnRetryableCommandError(enableCapture, disableCapture) {
        disableCapture();

        const primary = rst.getPrimary();
        primary.adminCommand({
            configureFailPoint: "onPrimaryTransactionalWrite",
            data: {
                closeConnection: false,
                failBeforeCommitExceptionCode: ErrorCodes.InterruptedDueToReplStateChange
            },
            mode: {times: 1}
        });

        enableCapture();
        const res = db[collName].update({}, {$set: {a: 1}});
        assert.commandWorked(res);
        disableCapture();

        primary.adminCommand({configureFailPoint: "onPrimaryTransactionalWrite", mode: "off"});
    },
    function assertUpdateRetriedExactlyOnce(cmdObjsSeen) {
        assert.eq(2, cmdObjsSeen.length, () => tojson(cmdObjsSeen));
        assert(cmdObjsSeen.every(cmdObj => Object.keys(cmdObj)[0] === "update"),
               () => "expected both attempts to be update requests: " + tojson(cmdObjsSeen));
        assert.eq(cmdObjsSeen[0], cmdObjsSeen[1], "command request changed between retry attempts");
    });

rst.stopSet();
})();

// @tags: [requires_replication]
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let mongoUri = "mongodb://" + rst.nodes.map((node) => node.host).join(",") + "/test";
let conn = rst.nodes[0];

// There are three ways to enable retryable writes in the mongo shell.
// 1. (cmdline flag) start mongo shell with --retryWrites
// 2. (uri param) connect to a uri like mongodb://.../test?retryWrites=true
// 3. (session option) in mongo shell create a new session with {retryWrite: true}

function runShellScript(uri, cmdArgs, insertShouldHaveTxnNumber, shellFn) {
    // This function is stringified and called immediately in the mongo --eval.
    function testWrapper(insertShouldHaveTxnNumber, shellFn) {
        const mongoRunCommandOriginal = Mongo.prototype.runCommand;
        let insertFound = false;
        Mongo.prototype.runCommand = function runCommandSpy(dbName, cmdObj, options) {
            let cmdObjSeen = cmdObj;
            let cmdName = Object.keys(cmdObjSeen)[0];

            if (cmdName === "query" || cmdName === "$query") {
                cmdObjSeen = cmdObjSeen[cmdName];
                cmdName = Object.keys(cmdObj)[0];
            }

            if (cmdName === "insert") {
                insertFound = true;
                if (insertShouldHaveTxnNumber) {
                    assert(cmdObjSeen.hasOwnProperty("txnNumber"),
                           "insert sent without expected txnNumber");
                } else {
                    assert(!cmdObjSeen.hasOwnProperty("txnNumber"),
                           "insert sent with txnNumber unexpectedly");
                }
            }
            return mongoRunCommandOriginal.apply(this, arguments);
        };

        shellFn();
        assert(insertFound, "test did not run insert command");
    }

    // Construct the string to be passed to eval.
    let script = "(" + testWrapper.toString() + ")(";
    script += insertShouldHaveTxnNumber + ",";
    script += shellFn.toString();
    script += ")";

    let args = ["mongo", uri, "--eval", script].concat(cmdArgs);
    let exitCode = runMongoProgram(...args);
    assert.eq(exitCode, 0, `shell script "${shellFn.name}" exited with ${exitCode}`);
}

// Tests --retryWrites command line parameter.
runShellScript(mongoUri, ["--retryWrites"], true, function flagWorks() {
    assert(db.getSession().getOptions().shouldRetryWrites(), "retryWrites should be true");
    assert.commandWorked(db.coll.insert({}), "cannot insert");
});

// The uri param should override --retryWrites.
runShellScript(
    mongoUri + "?retryWrites=false", ["--retryWrites"], false, function flagOverridenByUri() {
        assert(!db.getSession().getOptions().shouldRetryWrites(), "retryWrites should be false");
        assert.commandWorked(db.coll.insert({}), "cannot insert");
    });

// Even if initial connection has retryWrites=false in uri, new connections should not be
// overriden.
runShellScript(
    mongoUri + "?retryWrites=false", ["--retryWrites"], true, function flagNotOverridenByNewConn() {
        let connUri = db.getMongo().host;  // does not have ?retryWrites=false.
        let sess = new Mongo(connUri).startSession();
        assert(sess.getOptions().shouldRetryWrites(), "retryWrites should be true");
        assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
    });

// Unless that uri also specifies retryWrites.
runShellScript(
    mongoUri + "?retryWrites=false", ["--retryWrites"], false, function flagOverridenInNewConn() {
        let connUri = "mongodb://" + db.getMongo().host + "/test?retryWrites=false";
        let sess = new Mongo(connUri).startSession();
        assert(!sess.getOptions().shouldRetryWrites(), "retryWrites should be false");
        assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
    });

// Session options should override --retryWrites as well.
runShellScript(mongoUri, ["--retryWrites"], false, function flagOverridenByOpts() {
    let connUri = "mongodb://" + db.getMongo().host + "/test";
    let sess = new Mongo(connUri).startSession({retryWrites: false});
    assert(!sess.getOptions().shouldRetryWrites(), "retryWrites should be false");
    assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
});

// Test uri retryWrites parameter.
runShellScript(mongoUri + "?retryWrites=true", [], true, function uriTrueWorks() {
    assert(db.getSession().getOptions().shouldRetryWrites(), "retryWrites should be true");
    assert.commandWorked(db.coll.insert({}), "cannot insert");
});

// Test that uri retryWrites=false works.
runShellScript(mongoUri + "?retryWrites=false", [], false, function uriFalseWorks() {
    assert(!db.getSession().getOptions().shouldRetryWrites(), "retryWrites should be false");
    assert.commandWorked(db.coll.insert({}), "cannot insert");
});

// Test SessionOptions retryWrites option.
runShellScript(mongoUri, [], true, function sessOptTrueWorks() {
    let connUri = "mongodb://" + db.getMongo().host + "/test";
    let sess = new Mongo(connUri).startSession({retryWrites: true});
    assert(sess.getOptions().shouldRetryWrites(), "retryWrites should be true");
    assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
});

// Test that SessionOptions retryWrites:false works.
runShellScript(mongoUri, [], false, function sessOptFalseWorks() {
    let connUri = "mongodb://" + db.getMongo().host + "/test";
    let sess = new Mongo(connUri).startSession({retryWrites: false});
    assert(!sess.getOptions().shouldRetryWrites(), "retryWrites should be false");
    assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
});

// Test that session option overrides uri option.
runShellScript(mongoUri + "?retryWrites=true", [], false, function sessOptOverridesUri() {
    let sess = db.getMongo().startSession({retryWrites: false});
    assert(!sess.getOptions().shouldRetryWrites(), "retryWrites should be false");
    assert.commandWorked(sess.getDatabase("test").coll.insert({}), "cannot insert");
});

rst.stopSet();
}());

/**
 * Tests Thread from jstests/libs/parallelTester.js.
 */

load('jstests/libs/parallelTester.js');  // for Thread

(() => {
    "use strict";

    const tests = [];

    tests.push(function checkTestData() {
        let testData = TestData;
        let worker = new Thread((testData) => {
            assert.eq(TestData, testData);
        }, testData);
        worker.start();
        worker.join();
        assert(!worker.hasFailed());
    });

    tests.push(function checkTestDataWithOtherArgs() {
        let testData = TestData;
        let arg1 = 1;
        let arg2 = {a: 1};
        let worker = new Thread((testData, arg1, arg2) => {
            assert.eq(TestData, testData);
            assert.eq(arg1, 1);
            assert.eq(arg2, {a: 1});
        }, testData, arg1, arg2);
        worker.start();
        worker.join();
        assert(!worker.hasFailed());
    });

    tests.push(function checkTestDataWithFunc() {
        let oldTestData = TestData;
        if (!TestData) {
            TestData = {};
        }
        TestData.func = function myfunc(x) {
            return x;
        };
        let testData = TestData;
        try {
            let worker = new Thread((testData) => {
                // We cannot directly compare testData & TestData because the func object
                // has extra whitespace and line control.
                assert.eq(Object.keys(TestData), Object.keys(testData));
                for (var property in TestData) {
                    if (TestData.hasOwnProperty(property) && !TestData.property instanceof Code) {
                        assert.eq(TestData.property, testData.property);
                    }
                }
                assert.eq(testData.func(7), 7);
                assert.eq(TestData.func(7), 7);
            }, testData);
            worker.start();
            worker.join();
            assert(!worker.hasFailed());
        } finally {
            TestData = oldTestData;
        }
    });

    tests.push(function nullTestData() {
        let oldTestData = TestData;
        TestData = null;
        try {
            let worker = new Thread(() => {
                assert.eq(TestData, null);
            });
            worker.start();
            worker.join();
            assert(!worker.hasFailed());
        } finally {
            TestData = oldTestData;
        }
    });

    tests.push(function undefinedTestData() {
        let oldTestData = TestData;
        TestData = undefined;
        try {
            let worker = new Thread(() => {
                assert.eq(TestData, undefined);
            });
            worker.start();
            worker.join();
            assert(!worker.hasFailed());
        } finally {
            TestData = oldTestData;
        }
    });

    function testUncaughtException(joinFn) {
        const thread = new Thread(function myFunction() {
            throw new Error("Intentionally thrown inside Thread");
        });
        thread.start();

        let error = assert.throws(joinFn, [thread]);
        assert(/Intentionally thrown inside Thread/.test(error.message),
               () => "Exception didn't include the message from the exception thrown in Thread: " +
                   tojson(error.message));
        assert(/myFunction@/.test(error.stack),
               () => "Exception doesn't contain stack frames from within the Thread: " +
                   tojson(error.stack));
        assert(/testUncaughtException@/.test(error.stack),
               () => "Exception doesn't contain stack frames from caller of the Thread: " +
                   tojson(error.stack));

        error = assert.throws(() => thread.join());
        assert.eq("Thread not running",
                  error.message,
                  "join() is expected to be called only once for the thread");

        assert.eq(true,
                  thread.hasFailed(),
                  "Uncaught exception didn't cause thread to be marked as having failed");
        assert.doesNotThrow(() => thread.returnData(),
                            [],
                            "returnData() threw an exception after join() had been called");
        assert.eq(undefined,
                  thread.returnData(),
                  "returnData() shouldn't have anything to return if the thread failed");
    }

    tests.push(function testUncaughtExceptionAndWaitUsingJoin() {
        testUncaughtException(thread => thread.join());
    });

    // The returnData() method internally calls the join() method and should also throw an exception
    // if the Thread had an uncaught exception.
    tests.push(function testUncaughtExceptionAndWaitUsingReturnData() {
        testUncaughtException(thread => thread.returnData());
    });

    tests.push(function testUncaughtExceptionInNativeCode() {
        const thread = new Thread(function myFunction() {
            new Timestamp(-1);
        });
        thread.start();

        const error = assert.throws(() => thread.join());
        assert(/Timestamp/.test(error.message),
               () => "Exception didn't include the message from the exception thrown in Thread: " +
                   tojson(error.message));
        assert(/myFunction@/.test(error.stack),
               () => "Exception doesn't contain stack frames from within the Thread: " +
                   tojson(error.stack));
    });

    tests.push(function testUncaughtExceptionFromNestedThreads() {
        const thread = new Thread(function myFunction1() {
            load("jstests/libs/parallelTester.js");

            const thread = new Thread(function myFunction2() {
                load("jstests/libs/parallelTester.js");

                const thread = new Thread(function myFunction3() {
                    throw new Error("Intentionally thrown inside Thread");
                });

                thread.start();
                thread.join();
            });

            thread.start();
            thread.join();
        });
        thread.start();

        const error = assert.throws(() => thread.join());
        assert(/Intentionally thrown inside Thread/.test(error.message),
               () => "Exception didn't include the message from the exception thrown in Thread: " +
                   tojson(error.message));
        assert(/myFunction3@/.test(error.stack),
               () => "Exception doesn't contain stack frames from within the innermost Thread: " +
                   tojson(error.stack));
        assert(/myFunction2@/.test(error.stack),
               () => "Exception doesn't contain stack frames from within an inner Thread: " +
                   tojson(error.stack));
        assert(/myFunction1@/.test(error.stack),
               () => "Exception doesn't contain stack frames from within the outermost Thread: " +
                   tojson(error.stack));
    });

    /* main */

    tests.forEach((test) => {
        jsTest.log(`Starting tests '${test.name}'`);
        test();
    });
})();

/**
 * Tests the default values for causal consistency and retryable writes as part of SessionOptions.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();

let session = conn.startSession();
assert(session.getOptions().isCausalConsistency(),
       "Causal consistency should be implicitly enabled for an explicit session");
assert(!session.getOptions().shouldRetryWrites(),
       "Retryable writes should not be implicitly enabled for an explicit session");
session.endSession();

session = conn.startSession({causalConsistency: true});
assert(session.getOptions().isCausalConsistency(),
       "Causal consistency should be able to be explicitly enabled");
assert(!session.getOptions().shouldRetryWrites(),
       "Retryable writes should not be implicitly enabled for an explicit session");
session.endSession();

session = conn.startSession({causalConsistency: false});
assert(!session.getOptions().isCausalConsistency(),
       "Causal consistency should be able to be explicitly disabled");
assert(!session.getOptions().shouldRetryWrites(),
       "Retryable writes should not be implicitly enabled for an explicit session");
session.endSession();

session = conn.startSession({retryWrites: false});
assert(session.getOptions().isCausalConsistency(),
       "Causal consistency should be implicitly enabled for an explicit session");
assert(!session.getOptions().shouldRetryWrites(),
       "Retryable writes should be able to be explicitly disabled");
session.endSession();

session = conn.startSession({retryWrites: true});
assert(session.getOptions().isCausalConsistency(),
       "Causal consistency should be implicitly enabled for an explicit session");
assert(session.getOptions().shouldRetryWrites(),
       "Retryable writes should be able to be explicitly enabled");
session.endSession();

function runMongoShellWithRetryWritesEnabled(func) {
    const args = [MongoRunner.mongoShellPath];
    args.push("--port", conn.port);
    args.push("--retryWrites");

    const jsCode = "(" + func.toString() + ")()";
    args.push("--eval", jsCode);

    const exitCode = runMongoProgram.apply(null, args);
    assert.eq(0, exitCode, "Encountered an error in the other mongo shell");
}

runMongoShellWithRetryWritesEnabled(function() {
    let session = db.getSession();
    assert(session.getOptions().isCausalConsistency(),
           "Causal consistency should be implicitly enabled for an explicit session");
    assert(session.getOptions().shouldRetryWrites(),
           "Retryable writes should be implicitly enabled on default session when using" +
               " --retryWrites");

    session = db.getMongo().startSession({retryWrites: false});
    assert(session.getOptions().isCausalConsistency(),
           "Causal consistency should be implicitly enabled for an explicit session");
    assert(!session.getOptions().shouldRetryWrites(),
           "Retryable writes should be able to be explicitly disabled");
    session.endSession();

    session = db.getMongo().startSession();
    assert(session.getOptions().isCausalConsistency(),
           "Causal consistency should be implicitly enabled for an explicit session");
    assert(session.getOptions().shouldRetryWrites(),
           "Retryable writes should be implicitly enabled on new sessions when using" +
               " --retryWrites");
    session.endSession();
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that reads through the shell uses the correct read concern.
 * @tags: [requires_replication, uses_transactions, requires_majority_read_concern]
 */
(function() {
"use strict";
const rst = new ReplSetTest({nodes: 2, nodeOptions: {enableMajorityReadConcern: ""}});
rst.startSet();
rst.initiate();

const collName = "shell_uses_transaction_read_concern";
const primary = rst.getPrimary();
const db = primary.getDB("test");
var coll = db.getCollection(collName);
const testDoc = {
    "test": "doc",
    "_id": 0
};
assert.commandWorked(coll.insertOne(testDoc));
rst.awaitReplication();

const getMajorityRCCount = () =>
    db.runCommand({serverStatus: 1}).readConcernCounters.nonTransactionOps.majority;
const getSnapshotRCCount = () =>
    db.runCommand({serverStatus: 1}).readConcernCounters.transactionOps.snapshot.withoutClusterTime;

// Command-level
assert.eq(coll.runCommand({"find": coll.getName(), readConcern: {level: "majority"}})
              .cursor.firstBatch.length,
          1);
assert.eq(getMajorityRCCount(), 1);

const session = primary.startSession({readConcern: {level: "majority"}});
coll = session.getDatabase("test").getCollection(collName);

// Session-level
assert.eq(coll.find({"_id": 0}).itcount(), 1);
assert.eq(coll.runCommand({"find": coll.getName()}).cursor.firstBatch.length, 1);
assert.eq(getMajorityRCCount(), 3);

// Check that the session read concern doesn't break explain.
assert.commandWorked(coll.runCommand(
    {explain: {count: collName, query: {"_id": 0}, readConcern: {level: "available"}}}));
assert.commandWorked(coll.runCommand({explain: {count: collName, query: {"_id": 0}}}));

// Transaction-level
session.startTransaction({readConcern: {level: "snapshot"}});
assert.eq(coll.runCommand({"find": coll.getName()}).cursor.firstBatch.length, 1);
assert.eq(coll.runCommand({"find": coll.getName()}).cursor.firstBatch.length, 1);
assert.eq(coll.find({"_id": 0}).itcount(), 1);
assert.docEq(coll.findOne({"_id": 0}), testDoc);

assert.commandWorked(session.commitTransaction_forTesting());
assert.eq(getSnapshotRCCount(), 4);

rst.stopSet();
})();

/**
 * Tests for the write assertion functions in mongo/shell/assert.js.
 *
 * @tags: [requires_replication]
 */

load("jstests/libs/write_concern_util.js");

(() => {
    "use strict";

    const kReallyShortTimeoutMS = 500;

    const replTest = new ReplSetTest({nodes: 1});
    replTest.startSet();
    replTest.initiate();

    const conn = replTest.getPrimary();
    const db = conn.getDB("writeAssertions");
    assert.neq(null, conn, "mongodb was unable to start up");
    const tests = [];

    function setup() {
        db.coll.drop();
    }

    function _doFailedWrite(collection) {
        const duplicateId = 42;

        const res = collection.insert({_id: duplicateId});
        assert.writeOK(res, "write to collection should have been successful");
        const failedRes = collection.insert({_id: duplicateId});
        assert.writeError(failedRes, "duplicate key write should have failed");
        return failedRes;
    }

    /* writeOK tests */
    tests.push(function writeOKSuccessfulWriteDoesNotCallMsgFunction() {
        var msgFunctionCalled = false;

        const result = db.coll.insert({data: "hello world"});
        assert.doesNotThrow(() => {
            assert.writeOK(result, () => {
                msgFunctionCalled = true;
            });
        });

        assert.eq(false, msgFunctionCalled, "message function should not have been called");
    });

    tests.push(function writeOKUnsuccessfulWriteDoesCallMsgFunction() {
        var msgFunctionCalled = false;

        const failedResult = _doFailedWrite(db.coll);
        assert.throws(() => {
            assert.writeOK(failedResult, () => {
                msgFunctionCalled = true;
            });
        });

        assert.eq(true, msgFunctionCalled, "message function should have been called");
    });

    /* writeError tests */
    tests.push(function writeErrorSuccessfulWriteDoesCallMsgFunction() {
        var msgFunctionCalled = false;

        const result = db.coll.insert({data: "hello world"});
        assert.throws(() => {
            assert.writeError(result, () => {
                msgFunctionCalled = true;
            });
        });

        assert.eq(true, msgFunctionCalled, "message function should have been called");
    });

    tests.push(function writeErrorUnsuccessfulWriteDoesNotCallMsgFunction() {
        var msgFunctionCalled = false;

        const failedResult = _doFailedWrite(db.coll);
        assert.doesNotThrow(() => {
            assert.writeError(failedResult, () => {
                msgFunctionCalled = true;
            });
        });

        assert.eq(false, msgFunctionCalled, "message function should not have been called");
    });

    tests.push(function writeConcernErrorIsCaughtFromInsert() {
        const result = db.coll.insert(
            {data: "hello world"}, {writeConcern: {w: 'invalid', wtimeout: kReallyShortTimeoutMS}});

        assert.throws(() => {
            assert.writeOK(result);
        });
    });

    tests.push(function writeConcernErrorCanBeIgnored() {
        const result = db.coll.insert(
            {data: "hello world"}, {writeConcern: {w: 'invalid', wtimeout: kReallyShortTimeoutMS}});

        assert.doesNotThrow(() => {
            assert.writeOK(
                result, 'write can ignore writeConcern', {ignoreWriteConcernErrors: true});
        });
    });

    /* main */

    tests.forEach((test) => {
        jsTest.log(`Starting tests '${test.name}'`);
        setup();
        test();
    });

    /* cleanup */
    replTest.stopSet();
})();

/**
 * Ensure that we allow mongod to shutdown cleanly while being fsync locked.
 */
(function() {
"use strict";

let conn = MongoRunner.runMongod();
let db = conn.getDB("test");

for (let i = 0; i < 10; i++) {
    assert.commandWorked(db.adminCommand({fsync: 1, lock: 1}));
}

MongoRunner.stopMongod(conn, MongoRunner.EXIT_CLEAN, {skipValidation: true});
}());

/**
 *  Starts standalone RS with skipShardingConfigurationChecks.
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_sharding,
 * ]
 */
(function() {
'use strict';

function expectState(rst, state) {
    assert.soon(function() {
        var status = rst.status();
        if (status.myState != state) {
            print("Waiting for state " + state + " in replSetGetStatus output: " + tojson(status));
        }
        return status.myState == state;
    });
}

assert.throws(() => MongoRunner.runMongod(
                  {configsvr: "", setParameter: 'skipShardingConfigurationChecks=true'}));

assert.throws(() => MongoRunner.runMongod(
                  {shardsvr: "", setParameter: 'skipShardingConfigurationChecks=true'}));

var st = new ShardingTest({name: "skipConfig", shards: {rs0: {nodes: 1}}});
var configRS = st.configRS;
var shardRS = st.rs0;

shardRS.stopSet(15, true);
configRS.stopSet(undefined, true);

jsTestLog("Restarting configRS as a standalone ReplicaSet");

for (let i = 0; i < configRS.nodes.length; i++) {
    delete configRS.nodes[i].fullOptions.configsvr;
    configRS.nodes[i].fullOptions.setParameter = 'skipShardingConfigurationChecks=true';
}
configRS.startSet({}, true);
expectState(configRS, ReplSetTest.State.PRIMARY);
configRS.stopSet();

jsTestLog("Restarting shardRS as a standalone ReplicaSet");
for (let i = 0; i < shardRS.nodes.length; i++) {
    delete shardRS.nodes[i].fullOptions.shardsvr;
    shardRS.nodes[i].fullOptions.setParameter = 'skipShardingConfigurationChecks=true';
}
shardRS.startSet({}, true);
expectState(shardRS, ReplSetTest.State.PRIMARY);
shardRS.stopSet();
MongoRunner.stopMongos(st.s);
})();

/**
 * Tests the write conflict behavior while the "skipWriteConflictRetries" failpoint is enabled
 * between operations performed outside of a multi-statement transaction when another operation is
 * being performed concurrently inside of a multi-statement transaction.
 *
 * Note that jstests/core/txns/write_conflicts_with_non_txns.js tests the write conflict behavior
 * while the "skipWriteConflictRetries" failpoint isn't enabled.
 *
 * @tags: [uses_transactions, uses_prepare_transaction]
 */
(function() {
"use strict";

load("jstests/core/txns/libs/prepare_helpers.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB("test");
const testColl = testDB.getCollection("skip_write_conflict_retries_failpoint");

const session = primary.startSession({causalConsistency: false});
const sessionDB = session.getDatabase(testDB.getName());
const sessionColl = sessionDB.getCollection(testColl.getName());

assert.commandWorked(testColl.runCommand(
    "createIndexes",
    {indexes: [{key: {a: 1}, name: "a_1", unique: true}], writeConcern: {w: "majority"}}));

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "skipWriteConflictRetries", mode: "alwaysOn"}));

// A non-transactional insert would ordinarily keep retrying if it conflicts with a write
// operation performed inside a multi-statement transaction. However, with the
// "skipWriteConflictRetries" failpoint enabled, the non-transactional insert should immediately
// fail with a WriteConflict error response.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "from transaction", a: 0}));

assert.commandFailedWithCode(testColl.insert({_id: "from outside transaction", a: 0}),
                             ErrorCodes.WriteConflict);

assert.commandWorked(session.commitTransaction_forTesting());
assert.eq(testColl.findOne({a: 0}), {_id: "from transaction", a: 0});

// A non-transactional update would ordinarily keep retrying if it conflicts with a write
// operation performed inside a multi-statement transaction. However, with the
// "skipWriteConflictRetries" failpoint enabled, the non-transactional insert should immediately
// fail with a WriteConflict error response.
session.startTransaction();
assert.commandWorked(sessionColl.insert({_id: "from prepared transaction", a: 1}));
const prepareTimestamp = PrepareHelpers.prepareTransaction(session);

assert.commandFailedWithCode(testColl.update({_id: "from transaction"}, {$set: {a: 1}}),
                             ErrorCodes.WriteConflict);

assert.commandWorked(PrepareHelpers.commitTransaction(session, prepareTimestamp));
assert.eq(testColl.findOne({a: 1}), {_id: "from prepared transaction", a: 1});

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "skipWriteConflictRetries", mode: "off"}));

session.endSession();
rst.stopSet();
})();

// Tests that a cursor is iterated in a transaction/session iff it was created in that
// transaction/session. Specifically tests this in the context of snapshot cursors.
// @tags: [uses_transactions]
(function() {
"use strict";

// This test makes assertions on commands run without logical session ids.
TestData.disableImplicitSessions = true;

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primaryDB = rst.getPrimary().getDB(dbName);

const session1 = primaryDB.getMongo().startSession();
const sessionDB1 = session1.getDatabase(dbName);

const session2 = primaryDB.getMongo().startSession();
const sessionDB2 = session2.getDatabase(dbName);

const bulk = primaryDB.coll.initializeUnorderedBulkOp();
for (let i = 0; i < 10; ++i) {
    bulk.insert({_id: i});
}
assert.commandWorked(bulk.execute({w: "majority"}));

// Establish a snapshot cursor in session1.
let res = assert.commandWorked(sessionDB1.runCommand({
    find: collName,
    readConcern: {level: "snapshot"},
    txnNumber: NumberLong(0),
    autocommit: false,
    startTransaction: true,
    batchSize: 2
}));
assert(res.hasOwnProperty("cursor"));
assert(res.cursor.hasOwnProperty("id"));
let cursorID = res.cursor.id;

// The cursor may not be iterated outside of any session.
assert.commandFailedWithCode(
    primaryDB.runCommand({getMore: cursorID, collection: collName, batchSize: 2}), 50737);

// The cursor can still be iterated in session1.
assert.commandWorked(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(0),
    batchSize: 2
}));

// The cursor may not be iterated in a different session.
assert.commandFailedWithCode(
    sessionDB2.runCommand({getMore: cursorID, collection: collName, batchSize: 2}), 50738);

// The cursor can still be iterated in session1.
assert.commandWorked(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(0),
    batchSize: 2
}));

// The cursor may not be iterated outside of any transaction.
assert.commandFailedWithCode(
    sessionDB1.runCommand({getMore: cursorID, collection: collName, batchSize: 2}), 50740);

// The cursor can still be iterated in its transaction in session1.
assert.commandWorked(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(0),
    batchSize: 2
}));

// The cursor may not be iterated in a different transaction on session1.
assert.commandWorked(sessionDB1.runCommand({
    find: collName,
    txnNumber: NumberLong(1),
    autocommit: false,
    readConcern: {level: "snapshot"},
    startTransaction: true
}));
assert.commandFailedWithCode(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(1),
    batchSize: 2
}),
                             50741);

// The cursor can no longer be iterated because its transaction has ended.
assert.commandFailedWithCode(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(0),
    batchSize: 2
}),
                             ErrorCodes.TransactionTooOld);

// Kill the cursor.
assert.commandWorked(
    sessionDB1.runCommand({killCursors: sessionDB1.coll.getName(), cursors: [cursorID]}));

// Establish a cursor outside of any transaction in session1.
res = assert.commandWorked(sessionDB1.runCommand({find: collName, batchSize: 2}));
assert(res.hasOwnProperty("cursor"));
assert(res.cursor.hasOwnProperty("id"));
cursorID = res.cursor.id;

// The cursor may not be iterated inside a transaction.
assert.commandWorked(sessionDB1.runCommand({
    find: collName,
    txnNumber: NumberLong(2),
    autocommit: false,
    readConcern: {level: "snapshot"},
    startTransaction: true
}));
assert.commandFailedWithCode(sessionDB1.runCommand({
    getMore: cursorID,
    collection: collName,
    autocommit: false,
    txnNumber: NumberLong(2),
    batchSize: 2
}),
                             50739);

// The cursor can still be iterated outside of any transaction. Exhaust the cursor.
assert.commandWorked(sessionDB1.runCommand({getMore: cursorID, collection: collName}));

// Establish a cursor outside of any session.
res = assert.commandWorked(primaryDB.runCommand({find: collName, batchSize: 2}));
assert(res.hasOwnProperty("cursor"));
assert(res.cursor.hasOwnProperty("id"));
cursorID = res.cursor.id;

// The cursor may not be iterated inside a session.
assert.commandFailedWithCode(
    sessionDB1.runCommand({getMore: cursorID, collection: collName, batchSize: 2}), 50736);

// The cursor can still be iterated outside of any session. Exhaust the cursor.
assert.commandWorked(primaryDB.runCommand({getMore: cursorID, collection: collName}));

session1.endSession();
session2.endSession();
rst.stopSet();
})();

// Tests that stashed transaction resources are destroyed at shutdown and stepdown.
// @tags: [
//   uses_transactions,
// ]
(function() {
"use strict";

const dbName = "test";
const collName = "coll";

//
// Test that stashed transaction resources are destroyed at shutdown.
//

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primaryDB = rst.getPrimary().getDB(dbName);

let session = primaryDB.getMongo().startSession();
let sessionDB = session.getDatabase(dbName);

for (let i = 0; i < 4; i++) {
    assert.commandWorked(sessionDB.coll.insert({_id: i}, {writeConcern: {w: "majority"}}));
}

// Create a snapshot read cursor.
assert.commandWorked(sessionDB.runCommand({
    find: collName,
    batchSize: 2,
    readConcern: {level: "snapshot"},
    startTransaction: true,
    autocommit: false,
    txnNumber: NumberLong(0)
}));

// It should be possible to shut down the server without hanging. We must skip collection
// validation, since this will hang.
const signal = true;  // Use default kill signal.
const forRestart = false;
rst.stopSet(signal, forRestart, {skipValidation: true});

function testStepdown(stepdownFunc) {
    rst = new ReplSetTest({nodes: 2});
    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const primaryDB = primary.getDB(dbName);

    const session = primaryDB.getMongo().startSession();
    const sessionDB = session.getDatabase(dbName);

    for (let i = 0; i < 4; i++) {
        assert.commandWorked(sessionDB.coll.insert({_id: i}, {writeConcern: {w: "majority"}}));
    }

    // Create a snapshot read cursor.
    const res = assert.commandWorked(sessionDB.runCommand({
        find: collName,
        batchSize: 2,
        readConcern: {level: "snapshot"},
        txnNumber: NumberLong(0),
        startTransaction: true,
        autocommit: false
    }));
    assert(res.hasOwnProperty("cursor"), tojson(res));
    assert(res.cursor.hasOwnProperty("id"), tojson(res));
    const cursorId = res.cursor.id;

    // It should be possible to step down the primary without hanging.
    stepdownFunc(rst);
    rst.waitForState(primary, ReplSetTest.State.SECONDARY);

    // Kill the cursor.
    assert.commandWorked(sessionDB.runCommand({killCursors: collName, cursors: [cursorId]}));
    rst.stopSet();
}

//
// Test that stashed transaction resources are destroyed at stepdown triggered by
// replSetStepDown.
//
function replSetStepDown(replSetTest) {
    assert.commandWorked(replSetTest.getPrimary().adminCommand({replSetStepDown: 60, force: true}));
}
testStepdown(replSetStepDown);

//
// Test that stashed transaction resources are destroyed at stepdown triggered by loss of
// quorum.
//
function stepDownOnLossOfQuorum(replSetTest) {
    const secondary = rst.getSecondary();
    const secondaryId = rst.getNodeId(secondary);
    rst.stop(secondaryId);
}
testStepdown(stepDownOnLossOfQuorum);
})();

/**
 * Test setting minSnapshotHistoryWindowInSeconds at runtime and that server keeps history for up to
 * minSnapshotHistoryWindowInSeconds.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication
 * ]
 */
(function() {
"use strict";

const replSet = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        // Increase log verbosity for storage so we can see how the oldest_timestamp is set.
        setParameter: {logComponentVerbosity: tojson({storage: 2})}
    }
});

replSet.startSet();
replSet.initiate();

const collName = "coll";
const primary = replSet.getPrimary();
const primaryDB = primary.getDB('test');

const historyWindowSecs = 10;
assert.commandWorked(primaryDB.adminCommand(
    {setParameter: 1, minSnapshotHistoryWindowInSeconds: historyWindowSecs}));

const insertTimestamp =
    assert.commandWorked(primaryDB.runCommand({insert: collName, documents: [{_id: 0}]}))
        .operationTime;
const startTime = Date.now();
jsTestLog(`Inserted one document at ${tojson(insertTimestamp)}`);
let nextId = 1;

// Test snapshot window with 1s margin.
const testMarginMS = 1000;

// Test that reading from a snapshot at insertTimestamp is valid for up to historyWindowSecs minus
// the testMarginMS (as a buffer) to avoid races between the client's snapshot read and the update
// of the oldest timestamp in the server.
const testWindowMS = historyWindowSecs * 1000 - testMarginMS;
while (Date.now() - startTime < testWindowMS) {
    // Test that reading from a snapshot at insertTimestamp is still valid.
    assert.commandWorked(primaryDB.runCommand(
        {find: collName, readConcern: {level: "snapshot", atClusterTime: insertTimestamp}}));

    // Perform writes to advance stable timestamp and oldest timestamp. We use majority writeConcern
    // so that we can make sure the stable timestamp and the oldest timestamp are updated after each
    // insert.
    assert.commandWorked(primaryDB.runCommand(
        {insert: collName, documents: [{_id: nextId}], writeConcern: {w: "majority"}}));
    nextId++;

    sleep(50);
}

// Sleep enough to make sure the insertTimestamp falls off the snapshot window.
const historyExpirationTime = startTime + historyWindowSecs * 1000;
sleep(historyExpirationTime + testMarginMS - Date.now());
// Perform another majority write to advance the stable timestamp and the oldest timestamp again.
assert.commandWorked(primaryDB.runCommand(
    {insert: collName, documents: [{_id: nextId}], writeConcern: {w: "majority"}}));

// Test that reading from a snapshot at insertTimestamp returns SnapshotTooOld.
assert.commandFailedWithCode(
    primaryDB.runCommand(
        {find: collName, readConcern: {level: "snapshot", atClusterTime: insertTimestamp}}),
    ErrorCodes.SnapshotTooOld);

// Test that the SnapshotTooOld is recorded in serverStatus.
const serverStatusWT = assert.commandWorked(primaryDB.adminCommand({serverStatus: 1})).wiredTiger;
assert.eq(1,
          serverStatusWT["snapshot-window-settings"]["total number of SnapshotTooOld errors"],
          tojson(serverStatusWT));

replSet.stopSet();
})();

// Tests snapshot isolation on readConcern level snapshot read.
// @tags: [
//   requires_majority_read_concern,
//   uses_transactions,
// ]
(function() {
"use strict";

const dbName = "test";
const collName = "coll";

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
let conf = rst.getReplSetConfig();
conf.members[1].votes = 0;
conf.members[1].priority = 0;
rst.initiate(conf);

const primaryDB = rst.getPrimary().getDB(dbName);

function parseCursor(cmdResult) {
    if (cmdResult.hasOwnProperty("cursor")) {
        assert(cmdResult.cursor.hasOwnProperty("id"));
        return cmdResult.cursor;
    } else if (cmdResult.hasOwnProperty("cursors") && cmdResult.cursors.length === 1 &&
               cmdResult.cursors[0].hasOwnProperty("cursor")) {
        assert(cmdResult.cursors[0].cursor.hasOwnProperty("id"));
        return cmdResult.cursors[0].cursor;
    }

    throw Error("parseCursor failed to find cursor object. Command Result: " + tojson(cmdResult));
}

function runTest({useCausalConsistency, establishCursorCmd, readConcern}) {
    let cmdName = Object.getOwnPropertyNames(establishCursorCmd)[0];

    jsTestLog(`Test establishCursorCmd: ${cmdName},
     useCausalConsistency: ${useCausalConsistency},
     readConcern: ${tojson(readConcern)}`);

    primaryDB.runCommand({drop: collName, writeConcern: {w: "majority"}});

    const session = primaryDB.getMongo().startSession({causalConsistency: useCausalConsistency});
    const sessionDb = session.getDatabase(dbName);

    const bulk = primaryDB.coll.initializeUnorderedBulkOp();
    for (let x = 0; x < 10; ++x) {
        bulk.insert({_id: x});
    }
    assert.commandWorked(bulk.execute({w: "majority"}));

    session.startTransaction({readConcern: readConcern});

    // Establish a snapshot batchSize:0 cursor.
    let res = assert.commandWorked(sessionDb.runCommand(establishCursorCmd));
    let cursor = parseCursor(res);

    assert(cursor.hasOwnProperty("firstBatch"), tojson(res));
    assert.eq(0, cursor.firstBatch.length, tojson(res));
    assert.neq(cursor.id, 0);

    // Insert an 11th document which should not be visible to the snapshot cursor. This write is
    // performed outside of the session.
    assert.commandWorked(primaryDB.coll.insert({_id: 10}, {writeConcern: {w: "majority"}}));

    // Fetch the first 5 documents.
    res = assert.commandWorked(
        sessionDb.runCommand({getMore: cursor.id, collection: collName, batchSize: 5}));
    cursor = parseCursor(res);
    assert.neq(0, cursor.id, tojson(res));
    assert(cursor.hasOwnProperty("nextBatch"), tojson(res));
    assert.eq(5, cursor.nextBatch.length, tojson(res));

    // Exhaust the cursor, retrieving the remainder of the result set. Performing a second
    // getMore tests snapshot isolation across multiple getMore invocations.
    res = assert.commandWorked(
        sessionDb.runCommand({getMore: cursor.id, collection: collName, batchSize: 20}));
    assert.commandWorked(session.commitTransaction_forTesting());

    // The cursor has been exhausted.
    cursor = parseCursor(res);
    assert.eq(0, cursor.id, tojson(res));

    // Only the remaining 5 of the initial 10 documents are returned. The 11th document is not
    // part of the result set.
    assert(cursor.hasOwnProperty("nextBatch"), tojson(res));
    assert.eq(5, cursor.nextBatch.length, tojson(res));

    // Perform a second snapshot read under a new transaction.
    session.startTransaction({readConcern: readConcern});
    res =
        assert.commandWorked(sessionDb.runCommand({find: collName, sort: {_id: 1}, batchSize: 20}));
    assert.commandWorked(session.commitTransaction_forTesting());

    // The cursor has been exhausted.
    cursor = parseCursor(res);
    assert.eq(0, cursor.id, tojson(res));

    // All 11 documents are returned.
    assert(cursor.hasOwnProperty("firstBatch"), tojson(res));
    assert.eq(11, cursor.firstBatch.length, tojson(res));

    session.endSession();
}

// Test transaction reads using find or aggregate. Inserts outside
// transaction aren't visible, even after they are majority-committed.
// (This is a requirement for readConcern snapshot, but it is merely an
// implementation detail for majority or for the default, local. At some
// point, it would be desirable to have a transaction with readConcern
// local or majority see writes from other sessions. However, our current
// implementation of ensuring any data we read does not get rolled back
// relies on the fact that we read from a single WT snapshot, since we
// choose the timestamp to wait on in the first command of the
// transaction.)
let findCmd = {find: collName, sort: {_id: 1}, batchSize: 0};
let aggCmd = {aggregate: collName, pipeline: [{$sort: {_id: 1}}], cursor: {batchSize: 0}};

for (let establishCursorCmd of [findCmd, aggCmd]) {
    for (let useCausalConsistency of [false, true]) {
        for (let readConcern of [{level: "snapshot"}, {level: "majority"}, null]) {
            runTest({
                establishCursorCmd: establishCursorCmd,
                useCausalConsistency: useCausalConsistency,
                readConcern: readConcern
            });
        }
    }
}

rst.stopSet();
})();

/* Test that both transaction and non-transaction snapshot reads on capped collections are not
 * allowed.
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

const collName = "coll";
const primary = replSet.getPrimary();
const primaryDB = primary.getDB('test');

assert.commandWorked(primaryDB.createCollection(collName, {capped: true, size: 32, max: 1}));

// Non-transaction snapshot reads on capped collections are not allowed.
assert.commandFailedWithCode(
    primaryDB.runCommand({find: collName, readConcern: {level: "snapshot"}}),
    ErrorCodes.SnapshotUnavailable);
assert.commandFailedWithCode(
    primaryDB.runCommand(
        {aggregate: collName, pipeline: [], cursor: {}, readConcern: {level: "snapshot"}}),
    ErrorCodes.SnapshotUnavailable);
assert.commandFailedWithCode(
    primaryDB.runCommand({distinct: collName, key: "_id", readConcern: {level: "snapshot"}}),
    ErrorCodes.SnapshotUnavailable);

// After starting a transaction with read concern snapshot, the following find command should fail.
// This is because transaction snapshot reads are banned on capped collections.
const session = primary.startSession({causalConsistency: false});
const sessionDB = session.getDatabase('test');
session.startTransaction({readConcern: {level: 'snapshot'}});
assert.commandFailedWithCode(sessionDB.runCommand({find: collName}),
                             ErrorCodes.SnapshotUnavailable);

replSet.stopSet();
})();

// This test verifies that particular code paths exit early (I.e. are killed) or not by:
//
// 1. Set a fail point that will hang the code path
// 2. Open a new client with sockettimeoutms set (to force the disconnect) and a special appname
//    (to allow easy checking for the specific connection)
// 3. Run the tested command on the special connection and wait for it to timeout
// 4. Use an existing client to check current op for that special appname.  Return true if it's
//    still there at the end of a timeout
// 5. Disable the fail point
//
// @tags: [requires_sharding]

(function() {
"use strict";

const testName = "socket_disconnect_kills";

// Used to generate unique appnames
let id = 0;

// client - A client connection for curop (and that holds the hostname)
// pre - A callback to run with the timing out socket
// post - A callback to run after everything else has resolved (cleanup)
//
// Returns false if the op was gone from current op
function check(client, pre, post) {
    const interval = 200;
    const timeout = 10000;
    const socketTimeout = 5000;

    const host = client.host;

    // Make a socket which will timeout
    id++;
    let conn =
        new Mongo(`mongodb://${host}/?socketTimeoutMS=${socketTimeout}&appName=${testName}${id}`);

    // Make sure it works at all
    assert.commandWorked(conn.adminCommand({ping: 1}));

    try {
        // Make sure that whatever operation we ran had a network error
        assert.throws(function() {
            try {
                pre(conn);
            } catch (e) {
                throw e;
            }
        }, [], "error doing query: failed: network error while attempting");

        // Spin until the op leaves currentop, or timeout passes
        const start = new Date();

        while (1) {
            if (!client.getDB("admin")
                     .aggregate([
                         {$currentOp: {localOps: true}},
                         {$match: {appName: testName + id}},
                     ])
                     .itcount()) {
                return false;
            }

            if (((new Date()).getTime() - start.getTime()) > timeout) {
                return true;
            }

            sleep(interval);
        }
    } finally {
        post();
    }
}

function runWithCuropFailPointEnabled(client, failPointName) {
    return function(entry) {
        entry[0](client,
                 function(client) {
                     assert.commandWorked(client.adminCommand({
                         configureFailPoint: failPointName,
                         mode: "alwaysOn",
                         data: {shouldCheckForInterrupt: true},
                     }));

                     entry[1](client);
                 },
                 function() {
                     assert.commandWorked(
                         client.adminCommand({configureFailPoint: failPointName, mode: "off"}));
                 });
    };
}

function runWithCmdFailPointEnabled(client) {
    return function(entry) {
        const failPointName = "waitInCommandMarkKillOnClientDisconnect";

        entry[0](client,
                 function(client) {
                     assert.commandWorked(client.adminCommand({
                         configureFailPoint: failPointName,
                         mode: "alwaysOn",
                         data: {appName: testName + id},
                     }));

                     entry[1](client);
                 },
                 function() {
                     assert.commandWorked(
                         client.adminCommand({configureFailPoint: failPointName, mode: "off"}));
                 });
    };
}

function checkClosedEarly(client, pre, post) {
    assert(!check(client, pre, post), "operation killed on socket disconnect");
}

function checkNotClosedEarly(client, pre, post) {
    assert(check(client, pre, post), "operation not killed on socket disconnect");
}

function runCommand(cmd) {
    return function(client) {
        assert.commandWorked(client.getDB(testName).runCommand(cmd));
    };
}

function runTests(client) {
    let admin = client.getDB("admin");

    // set timeout for js function execution to 100 ms to speed up tests that run inf loop.
    assert.commandWorked(client.getDB(testName).adminCommand(
        {setParameter: 1, internalQueryJavaScriptFnTimeoutMillis: 100}));
    assert.commandWorked(client.getDB(testName).test.insert({x: 1}));
    assert.commandWorked(client.getDB(testName).test.insert({x: 2}));
    assert.commandWorked(client.getDB(testName).test.insert({x: 3}));

    [[checkClosedEarly, runCommand({find: "test", filter: {}})],
     [
         checkClosedEarly,
         runCommand({
             find: "test",
             filter: {
                 $where: function() {
                     sleep(100000);
                 }
             }
         })
     ],
     [
         checkClosedEarly,
         runCommand({
             find: "test",
             filter: {
                 $where: function() {
                     while (true) {
                     }
                 }
             }
         })
     ],
    ].forEach(runWithCuropFailPointEnabled(client, "waitInFindBeforeMakingBatch"));

    // After SERVER-39475, re-enable these tests and add negative testing for $out cursors.
    const serverSupportsEarlyDisconnectOnGetMore = false;
    if (serverSupportsEarlyDisconnectOnGetMore) {
        [[
            checkClosedEarly,
            function(client) {
                let result = assert.commandWorked(
                    client.getDB(testName).runCommand({find: "test", filter: {}, batchSize: 0}));
                assert.commandWorked(client.getDB(testName).runCommand(
                    {getMore: result.cursor.id, collection: "test"}));
            }
        ]].forEach(runWithCuropFailPointEnabled(client,
                                                "waitAfterPinningCursorBeforeGetMoreBatch"));
    }

    [[checkClosedEarly, runCommand({aggregate: "test", pipeline: [], cursor: {}})],
     [checkNotClosedEarly, runCommand({aggregate: "test", pipeline: [{$out: "out"}], cursor: {}})],
    ].forEach(runWithCmdFailPointEnabled(client));

    [[checkClosedEarly, runCommand({count: "test"})],
     [checkClosedEarly, runCommand({distinct: "test", key: "x"})],
     [checkClosedEarly, runCommand({authenticate: "test", user: "x", pass: "y"})],
     [checkClosedEarly, runCommand({getnonce: 1})],
     [checkClosedEarly, runCommand({saslStart: 1})],
     [checkClosedEarly, runCommand({saslContinue: 1})],
     [checkClosedEarly, runCommand({hello: 1})],
     [checkClosedEarly, runCommand({listCollections: 1})],
     [checkClosedEarly, runCommand({listDatabases: 1})],
     [checkClosedEarly, runCommand({listIndexes: "test"})],
    ].forEach(runWithCmdFailPointEnabled(client));
}

{
    let proc = MongoRunner.runMongod();
    assert.neq(proc, null);
    runTests(proc);
    MongoRunner.stopMongod(proc);
}

{
    let st = ShardingTest({mongo: 1, config: 1, shards: 1});
    runTests(st.s0);
    st.stop();
}
})();

/**
 * Test that estimate of the total data size sorted when spilling to disk is reasonable.
 *
 * This test was originally designed to reproduce SERVER-53760.
 */
(function() {
"use strict";
load('jstests/libs/analyze_plan.js');  // For 'getAggPlanStage()'.

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");
const db = conn.getDB("test");

const collName = jsTestName();
const coll = db[collName];
coll.drop();
const numDocs = 5;
const bigStrLen = numDocs * 40;
const arrLen = numDocs * 40;

// To reproduce SERVER-53760, we need to create a collection with N documents, where each document
// is sizable and consists of an array field, called `data`. Then, if we pass the collection through
// a pipeline consisting of a `$unwind` (on `data`) followed by `$sort`, the documents in the output
// of `$unwind` all share the same backing BSON in the original collection. Next, if the sorter does
// not calculate the appropriate size of the document (by discarding the parts of backing BSON not
// used by each document in the output of `$unwind`), the size approximation can be way bigger than
// the actual amount, which can result in (unnecessarily) opening too many files (and even running
// out of the number of allowed open files for some operating systems). In this example, it'd be a
// factor of 100x.
const docs = [];
let totalSize = 0;
const str = "a".repeat(bigStrLen);
for (let i = 0; i < numDocs; ++i) {
    let doc = {_id: i, foo: i * 2};
    let arr = [];
    for (let j = 0; j < arrLen; ++j) {
        arr.push({bigString: str, uniqueValue: j});
    }

    doc["data"] = arr;
    docs.push(doc);
    totalSize += Object.bsonsize(doc);
}

assert.commandWorked(
    db.adminCommand({setParameter: 1, internalQueryMaxBlockingSortMemoryUsageBytes: 5000}));
assert.commandWorked(coll.insert(docs));

function createPipeline(collection) {
    return collection.aggregate(
        [
            {$unwind: "$data"},
            {$sort: {'_id': -1, 'data.uniqueValue': -1}},
            {$limit: 900},
            {$group: {_id: 0, sumTop900UniqueValues: {$sum: '$data.uniqueValue'}}}
        ],
        {allowDiskUse: true});
}

const explain = createPipeline(coll.explain("executionStats"));
const sortStages = getAggPlanStages(explain, "$sort");

assert.eq(sortStages.length, 1, explain);
const sort = sortStages[0];
const dataBytesSorted = sort["totalDataSizeSortedBytesEstimate"];

// The total data size sorted is no greater than 3x the total size of all documents sorted.
assert.lt(dataBytesSorted, 3 * totalSize, explain);

assert.eq(createPipeline(coll).toArray(), [{_id: 0, sumTop900UniqueValues: 94550}], explain);

MongoRunner.stopMongod(conn);
})();

if (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger") {
    var baseDir = "jstests_split_c_and_i";

    var dbpath = MongoRunner.dataPath + baseDir + "/";

    var m = MongoRunner.runMongod({dbpath: dbpath, wiredTigerDirectoryForIndexes: ''});
    db = m.getDB("foo");
    db.bar.insert({x: 1});
    assert.eq(1, db.bar.count());

    db.adminCommand({fsync: 1});

    assert(listFiles(dbpath + "/index").length > 0);
    assert(listFiles(dbpath + "/collection").length > 0);

    MongoRunner.stopMongod(m);

    // Subsequent attempts to start server using same dbpath but different
    // wiredTigerDirectoryForIndexes and directoryperdb options should fail.
    assert.throws(() => MongoRunner.runMongod({dbpath: dbpath, restart: true}));
    assert.throws(() => MongoRunner.runMongod({dbpath: dbpath, restart: true, directoryperdb: ''}));
    assert.throws(() => MongoRunner.runMongod({
        dbpath: dbpath,
        restart: true,
        wiredTigerDirectoryForIndexes: '',
        directoryperdb: ''
    }));
}

// validate default for opensslCipherConfig

(function() {
'use strict';

function getparam(mongod, field) {
    var q = {getParameter: 1};
    q[field] = 1;

    var ret = mongod.getDB("admin").runCommand(q);
    return ret[field];
}

function assertCorrectConfig(mongodArgs, expectedConfig) {
    let m = MongoRunner.runMongod(mongodArgs);
    assert.eq(getparam(m, "opensslCipherConfig"), expectedConfig);
    MongoRunner.stopMongod(m);
}

const defaultConfig = "HIGH:!EXPORT:!aNULL@STRENGTH";

// if sslMode is disabled, cipher config should be set to default
assertCorrectConfig({sslMode: 'disabled'}, defaultConfig);

// if sslMode is enabled, cipher config should have default
assertCorrectConfig({
    sslMode: 'allowSSL',
    sslPEMKeyFile: "jstests/libs/server.pem",
    sslCAFile: "jstests/libs/ca.pem"
},
                    defaultConfig);

// setting through setParameter or tlsCipherConfig should override default
assertCorrectConfig({
    sslMode: 'allowSSL',
    sslPEMKeyFile: "jstests/libs/server.pem",
    sslCAFile: "jstests/libs/ca.pem",
    setParameter: "opensslCipherConfig=HIGH"
},
                    "HIGH");

assertCorrectConfig({
    sslMode: 'allowSSL',
    sslPEMKeyFile: "jstests/libs/server.pem",
    sslCAFile: "jstests/libs/ca.pem",
    tlsCipherConfig: "HIGH"
},
                    "HIGH");
})();
/**
 * Tests that a standalone succeeds when passed the 'recoverFromOplogAsStandalone' parameter.
 *
 * This test only makes sense for storage engines that support recover to stable timestamp.
 * @tags: [
 *   requires_journaling,
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";
load("jstests/replsets/rslib.js");
load("jstests/libs/write_concern_util.js");

const name = 'standalone_replication_recovery';
const dbName = name;
const collName = 'srr_coll';
const logLevel = tojson({storage: {recovery: 2}});

const rst = new ReplSetTest({
    nodes: 2,
});

function getColl(conn) {
    return conn.getDB(dbName)[collName];
}

function assertDocsInColl(node, nums) {
    let results = getColl(node).find().sort({_id: 1}).toArray();
    let expected = nums.map((i) => ({_id: i}));
    if (!friendlyEqual(results, expected)) {
        rst.dumpOplog(node, {}, 100);
    }
    assert.eq(results, expected, "actual (left) != expected (right)");
}

jsTestLog("Test that an empty standalone fails trying to recover.");
assert.throws(
    () => rst.start(0, {noReplSet: true, setParameter: {recoverFromOplogAsStandalone: true}}));

jsTestLog("Initiating as a replica set.");
// Restart as a replica set node without the flag so we can add operations to the oplog.
let nodes = rst.startSet({setParameter: {logComponentVerbosity: logLevel}});
let node = nodes[0];
let secondary = nodes[1];
rst.initiate(
    {_id: name, members: [{_id: 0, host: node.host}, {_id: 2, host: secondary.host, priority: 0}]});

// The default WC is majority and stopServerReplication will prevent satisfying any majority writes.
assert.commandWorked(rst.getPrimary().adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

// Create the collection with w:majority and then perform a clean restart to ensure that
// the collection is in a stable checkpoint.
assert.commandWorked(node.getDB(dbName).runCommand(
    {create: collName, writeConcern: {w: "majority", wtimeout: ReplSetTest.kDefaultTimeoutMS}}));
assertDocsInColl(node, []);
node = rst.restart(node, {"noReplSet": false});
reconnect(node);
assert.eq(rst.getPrimary(), node);

// Keep node 0 the primary, but prevent it from committing any writes.
stopServerReplication(secondary);

assert.commandWorked(getColl(node).insert({_id: 3}, {writeConcern: {w: 1, j: 1}}));
assert.commandWorked(getColl(node).insert({_id: 4}, {writeConcern: {w: 1, j: 1}}));
assert.commandWorked(getColl(node).insert({_id: 5}, {writeConcern: {w: 1, j: 1}}));
assertDocsInColl(node, [3, 4, 5]);

jsTestLog("Test that if we kill the node, recovery still plays.");
rst.stop(node, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});
node = rst.restart(node, {"noReplSet": false});
reconnect(node);
assert.eq(rst.getPrimary(), node);
assertDocsInColl(node, [3, 4, 5]);

jsTestLog("Test that a replica set node cannot start up with the parameter set.");
assert.throws(
    () => rst.restart(
        0, {setParameter: {recoverFromOplogAsStandalone: true, logComponentVerbosity: logLevel}}));

jsTestLog("Test that on restart as a standalone we only see committed writes by default.");
node = rst.start(node, {noReplSet: true, setParameter: {logComponentVerbosity: logLevel}}, true);
reconnect(node);
assertDocsInColl(node, []);

// Test that we can run the validate command on a standalone.
assert.commandWorked(node.getDB(dbName).runCommand({"validate": collName}));

jsTestLog("Test that on restart with the flag set we play recovery.");
node = rst.restart(node, {
    noReplSet: true,
    setParameter: {recoverFromOplogAsStandalone: true, logComponentVerbosity: logLevel}
});
reconnect(node);
assertDocsInColl(node, [3, 4, 5]);

// Test that we can run the validate command on a standalone that recovered.
assert.commandWorked(node.getDB(dbName).runCommand({"validate": collName}));

jsTestLog("Test that we go into read-only mode.");
assert.commandFailedWithCode(getColl(node).insert({_id: 1}), ErrorCodes.IllegalOperation);

jsTestLog("Test that we cannot set the parameter during standalone runtime.");
assert.commandFailed(node.adminCommand({setParameter: 1, recoverFromOplogAsStandalone: true}));
assert.commandFailed(node.adminCommand({setParameter: 1, recoverFromOplogAsStandalone: false}));

jsTestLog("Test that on restart after standalone recovery we do not see replicated writes.");
node = rst.restart(node, {
    noReplSet: true,
    setParameter: {recoverFromOplogAsStandalone: false, logComponentVerbosity: logLevel}
});
reconnect(node);
assertDocsInColl(node, []);
assert.commandWorked(getColl(node).insert({_id: 6}));
assertDocsInColl(node, [6]);
node = rst.restart(node, {
    noReplSet: true,
    setParameter: {recoverFromOplogAsStandalone: true, logComponentVerbosity: logLevel}
});
reconnect(node);
assertDocsInColl(node, [3, 4, 5, 6]);

jsTestLog("Test that we can restart again as a replica set node.");
node = rst.restart(node, {
    noReplSet: false,
    setParameter: {recoverFromOplogAsStandalone: false, logComponentVerbosity: logLevel}
});
reconnect(node);
assert.eq(rst.getPrimary(), node);
assertDocsInColl(node, [3, 4, 5, 6]);

jsTestLog("Test that we cannot set the parameter during replica set runtime.");
assert.commandFailed(node.adminCommand({setParameter: 1, recoverFromOplogAsStandalone: true}));
assert.commandFailed(node.adminCommand({setParameter: 1, recoverFromOplogAsStandalone: false}));

jsTestLog("Test that we can still recover as a standalone.");
assert.commandWorked(getColl(node).insert({_id: 7}));
assertDocsInColl(node, [3, 4, 5, 6, 7]);
node = rst.restart(node, {
    noReplSet: true,
    setParameter: {recoverFromOplogAsStandalone: false, logComponentVerbosity: logLevel}
});
reconnect(node);
assertDocsInColl(node, [6]);
node = rst.restart(node, {
    noReplSet: true,
    setParameter: {recoverFromOplogAsStandalone: true, logComponentVerbosity: logLevel}
});
reconnect(node);
assertDocsInColl(node, [3, 4, 5, 6, 7]);

jsTestLog("Restart as a replica set node so that the test can complete successfully.");
node = rst.restart(node, {
    noReplSet: false,
    setParameter: {recoverFromOplogAsStandalone: false, logComponentVerbosity: logLevel}
});
reconnect(node);
assert.eq(rst.getPrimary(), node);
assertDocsInColl(node, [3, 4, 5, 6, 7]);

restartServerReplication(secondary);

// Skip checking db hashes since we do a write as a standalone.
TestData.skipCheckDBHashes = true;
rst.stopSet();
})();
(function() {
'use strict';

// This test makes assertions about the number of sessions, which are not compatible with
// implicit sessions.
TestData.disableImplicitSessions = true;

var conn;
var admin;
var foo;
var result;
const request = {
    startSession: 1
};

conn = MongoRunner.runMongod({setParameter: {maxSessions: 2}});
admin = conn.getDB("admin");

// ensure that the cache is empty
var serverStatus = assert.commandWorked(admin.adminCommand({serverStatus: 1}));
assert.eq(0, serverStatus.logicalSessionRecordCache.activeSessionsCount);

// test that we can run startSession unauthenticated when the server is running without --auth

result = admin.runCommand(request);
assert.commandWorked(
    result,
    "failed test that we can run startSession unauthenticated when the server is running without --auth");
assert(result.id, "failed test that our session response has an id");
assert.eq(result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

// test that startSession added to the cache
serverStatus = assert.commandWorked(admin.adminCommand({serverStatus: 1}));
assert.eq(1, serverStatus.logicalSessionRecordCache.activeSessionsCount);

// test that we can run startSession authenticated when the server is running without --auth

admin.createUser({user: 'user0', pwd: 'password', roles: []});
admin.auth("user0", "password");

result = admin.runCommand(request);
assert.commandWorked(
    result,
    "failed test that we can run startSession authenticated when the server is running without --auth");
assert(result.id, "failed test that our session response has an id");
assert.eq(result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

assert.commandFailed(admin.runCommand(request),
                     "failed test that we can't run startSession when the cache is full");
MongoRunner.stopMongod(conn);

//

conn = MongoRunner.runMongod({auth: "", nojournal: ""});
admin = conn.getDB("admin");
foo = conn.getDB("foo");

// test that we can't run startSession unauthenticated when the server is running with --auth

assert.commandFailed(
    admin.runCommand(request),
    "failed test that we can't run startSession unauthenticated when the server is running with --auth");

//

admin.createUser({user: 'admin', pwd: 'admin', roles: jsTest.adminUserRoles});
admin.auth("admin", "admin");
admin.createUser({user: 'user0', pwd: 'password', roles: jsTest.basicUserRoles});
foo.createUser({user: 'user1', pwd: 'password', roles: jsTest.basicUserRoles});
admin.createUser({user: 'user2', pwd: 'password', roles: []});
admin.logout();

// test that we can run startSession authenticated as one user with proper permissions

admin.auth("user0", "password");
result = admin.runCommand(request);
assert.commandWorked(
    result,
    "failed test that we can run startSession authenticated as one user with proper permissions");
assert(result.id, "failed test that our session response has an id");
assert.eq(result.timeoutMinutes, 30, "failed test that our session record has the correct timeout");

// test that we cant run startSession authenticated as two users with proper permissions

foo.auth("user1", "password");
assert.commandFailed(
    admin.runCommand(request),
    "failed test that we cant run startSession authenticated as two users with proper permissions");

// test that we cant run startSession authenticated as one user without proper permissions

admin.logout();
admin.auth("user2", "password");
assert.commandFailed(
    admin.runCommand(request),
    "failed test that we cant run startSession authenticated as one user without proper permissions");

//

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that normal startup writes to the log files as expected.
 */

(function() {

'use strict';

function makeRegExMatchFn(pattern) {
    return function(text) {
        return pattern.test(text);
    };
}

function testStartupLogging(launcher, matchFn, expectedExitCode) {
    assert(matchFn(rawMongoProgramOutput()));
}

function validateWaitingMessage(launcher) {
    clearRawMongoProgramOutput();
    var conn = launcher.start({});
    launcher.stop(conn, undefined, {});
    testStartupLogging(
        launcher,
        makeRegExMatchFn(
            /"id":23016,\s*"ctx":"listener","msg":"Waiting for connections","attr":{"port":/));
}

print("********************\nTesting startup logging in mongod\n********************");

validateWaitingMessage({
    start: function(opts) {
        return MongoRunner.runMongod(opts);
    },
    stop: MongoRunner.stopMongod
});
}());

/**
 * Test that a confirmed write against a primary with oplog holes behind it when a crash occurs will
 * be truncated on startup recovery.
 *
 * There must be more than 1 voting node, otherwise the write concern behavior changes to waiting
 * for no holes for writes with {j: true} write concern, and no confirmed writes will be truncated.
 *
 * @tags: [
 *   # Replica sets using WT require journaling (startup error otherwise).
 *   requires_journaling,
 *   # The primary is restarted and must retain its data.
 *   requires_persistence,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");

const rst = new ReplSetTest({name: jsTest.name(), nodes: 2});
rst.startSet();
// Make sure there are no election timeouts. This should prevent primary stepdown. Normally we would
// set the secondary node votes to 0, but that would affect the feature that is being tested.
rst.initiateWithHighElectionTimeout();

const primary = rst.getPrimary();
const dbName = "testDB";
const collName = jsTest.name();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB[collName];

assert.commandWorked(primaryDB.createCollection(collName, {writeConcern: {w: "majority"}}));

const failPoint = configureFailPoint(primaryDB,
                                     "hangAfterCollectionInserts",
                                     {collectionNS: primaryColl.getFullName(), first_id: "b"});
let ps = undefined;
try {
    // Hold back the durable timestamp by leaving an uncommitted transaction hanging.

    TestData.dbName = dbName;
    TestData.collName = collName;

    ps = startParallelShell(() => {
        jsTestLog("Insert a document that will hang before the insert completes.");
        // Crashing the server while this command is running may cause the parallel shell code to
        // error and stop executing. We will therefore ignore the result of this command and
        // parallel shell. Test correctness is guaranteed by waiting for the failpoint this command
        // hits.
        db.getSiblingDB(TestData.dbName)[TestData.collName].insert({_id: "b"});
    }, primary.port);

    jsTest.log("Wait for async insert to hit the failpoint.");
    failPoint.wait();

    // Execute an insert with confirmation that it made it to disk ({j: true});
    //
    // The primary's durable timestamp should be pinned by the prior hanging uncommitted write. So
    // this second write will have an oplog hole behind it and will be truncated after a crash.
    assert.commandWorked(
        primaryColl.insert({_id: "writeAfterHole"}, {writeConcern: {w: 1, j: true}}));

    const findResult = primaryColl.findOne({_id: "writeAfterHole"});
    assert.eq(findResult, {"_id": "writeAfterHole"});

    jsTest.log("Force a checkpoint so the primary has data on startup recovery after a crash");
    assert.commandWorked(primary.adminCommand({fsync: 1}));

    // Crash and restart the primary, which should truncate the second successful write, because
    // the first write never committed and left a hole in the oplog.
    rst.stop(primary, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});
} catch (error) {
    // Turn off the failpoint before allowing the test to end, so nothing hangs while the server
    // shuts down or in post-test hooks.
    failPoint.off();
    throw error;
} finally {
    if (ps) {
        ps({checkExitSuccess: false});
    }
}

rst.start(primary);

// Wait for the restarted node to complete startup recovery and start accepting user requests.
// Note: no new primary will be elected because of the high election timeout set on the replica set.
assert.soonNoExcept(function() {
    const nodeState = assert.commandWorked(primary.adminCommand("replSetGetStatus")).myState;
    return nodeState == ReplSetTest.State.SECONDARY;
});

// Confirm that the write with the oplog hold behind it is now gone (truncated) as expected.
primary.setSecondaryOk();
const find = primary.getDB(dbName).getCollection(collName).findOne({_id: "writeAfterHole"});
assert.eq(find, null);

rst.stopSet();
})();

/**
 * Tests that performing a stepdown on the primary during a dropDatabase command doesn't have any
 * negative effects when the new primary runs the same dropDatabase command while the old primary
 * is still in the midst of dropping the database.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const dbName = "test";
const collName = "coll";

const replSet = new ReplSetTest({nodes: 2});
replSet.startSet();
replSet.initiate();

let primary = replSet.getPrimary();
let testDB = primary.getDB(dbName);

const size = 5;
jsTest.log("Creating " + size + " test documents.");
var bulk = testDB.getCollection(collName).initializeUnorderedBulkOp();
for (var i = 0; i < size; ++i) {
    bulk.insert({i: i});
}
assert.commandWorked(bulk.execute());
replSet.awaitReplication();

const failpoint = "dropDatabaseHangAfterAllCollectionsDrop";
assert.commandWorked(primary.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));

// Run the dropDatabase command and stepdown the primary while it is running.
const awaitShell = startParallelShell(() => {
    db.dropDatabase();
}, testDB.getMongo().port);

// Ensure the dropDatabase command has begun before stepping down.
checkLog.contains(primary,
                  "dropDatabase - fail point dropDatabaseHangAfterAllCollectionsDrop " +
                      "enabled. Blocking until fail point is disabled");

assert.commandWorked(testDB.adminCommand({replSetStepDown: 60, force: true}));
replSet.waitForState(primary, ReplSetTest.State.SECONDARY);

assert.commandWorked(primary.adminCommand({configureFailPoint: failpoint, mode: "off"}));
awaitShell();

primary = replSet.getPrimary();
testDB = primary.getDB(dbName);

// Run dropDatabase on the new primary. The secondary (formerly the primary) should be able to
// drop the database too.
testDB.dropDatabase();
replSet.awaitReplication();

replSet.stopSet();
})();

/**
 * Tests that performing a stepdown on the primary during a dropDatabase command doesn't result in
 * any crashes when setting the drop-pending flag back to false.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/noPassthrough/libs/index_build.js");

const dbName = "test";
const collName = "coll";

const replSet = new ReplSetTest({nodes: 2});
replSet.startSet();
replSet.initiate();

const primary = replSet.getPrimary();

let testDB = primary.getDB(dbName);
const testColl = testDB.getCollection(collName);

var bulk = testColl.initializeUnorderedBulkOp();
for (var i = 0; i < 5; ++i) {
    bulk.insert({x: i});
}
assert.commandWorked(bulk.execute());
replSet.awaitReplication();

IndexBuildTest.pauseIndexBuilds(testDB.getMongo());
const awaitIndexBuild = IndexBuildTest.startIndexBuild(
    testDB.getMongo(), testColl.getFullName(), {x: 1}, {}, [ErrorCodes.IndexBuildAborted]);
IndexBuildTest.waitForIndexBuildToScanCollection(testDB, collName, "x_1");

const failpoint = "dropDatabaseHangAfterWaitingForIndexBuilds";
assert.commandWorked(primary.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn"}));

// Run the dropDatabase command and stepdown the primary while it is running.
const awaitDropDatabase = startParallelShell(() => {
    assert.commandFailedWithCode(db.dropDatabase(), ErrorCodes.InterruptedDueToReplStateChange);
}, testDB.getMongo().port);

checkLog.containsJson(primary, 4612302);
IndexBuildTest.resumeIndexBuilds(testDB.getMongo());

awaitIndexBuild();

// Ensure the dropDatabase command has begun before stepping down.
checkLog.containsJson(primary, 4612300);

assert.commandWorked(testDB.adminCommand({replSetStepDown: 60, force: true}));
replSet.waitForState(primary, ReplSetTest.State.SECONDARY);

assert.commandWorked(primary.adminCommand({configureFailPoint: failpoint, mode: "off"}));

awaitDropDatabase();

const newPrimary = replSet.getPrimary();
assert(primary.port != newPrimary.port);

// The {x: 1} index was aborted and should not be present even though the dropDatabase command was
// interrupted. Only the _id index will exist.
let indexesRes = assert.commandWorked(newPrimary.getDB(dbName).runCommand({listIndexes: collName}));
assert.eq(1, indexesRes.cursor.firstBatch.length);

indexesRes =
    assert.commandWorked(replSet.getSecondary().getDB(dbName).runCommand({listIndexes: collName}));
assert.eq(1, indexesRes.cursor.firstBatch.length);

// Run dropDatabase on the new primary. The secondary (formerly the primary) should be able to
// drop the database too.
newPrimary.getDB(dbName).dropDatabase();
replSet.awaitReplication();

replSet.stopSet();
})();

/**
 * Tests that a query with default read preference ("primary") will succeed even if the node being
 * queried steps down before the final result batch has been delivered.
 * @tags: [
 *   requires_replication,
 *   requires_sharding,
 * ]
 */

// Checking UUID consistency involves talking to a shard node, which in this test is shutdown
TestData.skipCheckingUUIDsConsistentAcrossCluster = true;
TestData.skipCheckOrphans = true;

(function() {
'use strict';

// Set the refresh period to 10 min to rule out races
_setShellFailPoint({
    configureFailPoint: "modifyReplicaSetMonitorDefaultRefreshPeriod",
    mode: "alwaysOn",
    data: {
        period: 10 * 60,
    },
});

var dbName = "test";
var collName = jsTest.name();

function runTest(host, rst, waitForPrimary) {
    // We create a new connection to 'host' here instead of passing in the original connection.
    // This to work around the fact that connections created by ReplSetTest already have secondaryOk
    // set on them, but we need a connection with secondaryOk not set for this test.
    var conn = new Mongo(host);
    var coll = conn.getDB(dbName).getCollection(collName);
    assert(!coll.exists());
    assert.commandWorked(coll.insert([{}, {}, {}, {}, {}]));
    var cursor = coll.find().batchSize(2);
    // Retrieve the first batch of results.
    cursor.next();
    cursor.next();
    assert.eq(0, cursor.objsLeftInBatch());
    var primary = rst.getPrimary();
    var secondary = rst.getSecondary();
    assert.commandWorked(primary.getDB("admin").runCommand({replSetStepDown: 60, force: true}));
    rst.waitForState(primary, ReplSetTest.State.SECONDARY);
    if (waitForPrimary) {
        rst.waitForState(secondary, ReplSetTest.State.PRIMARY);
    }
    // When the primary steps down, it closes all client connections. Since 'conn' may be a
    // direct connection to the primary and the shell doesn't automatically retry operations on
    // network errors, we run a dummy operation here to force the shell to reconnect.
    try {
        conn.getDB("admin").runCommand("ping");
    } catch (e) {
    }

    // Even though our connection doesn't have secondaryOk set, we should still be able to iterate
    // our cursor and kill our cursor.
    assert(cursor.hasNext());
    assert.doesNotThrow(function() {
        cursor.close();
    });
}

// Test querying a replica set primary directly.
var rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
runTest(rst.getPrimary().host, rst, false);
rst.stopSet();

rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiate();
runTest(rst.getURL(), rst, true);
rst.stopSet();

// Test querying a replica set primary through mongos.
var st = new ShardingTest({shards: 1, rs: {nodes: 2}, config: 2});
rst = st.rs0;
runTest(st.s0.host, rst, true);
st.stop();
})();

/**
 * Test that retryable findAndModify commands will store pre- and post- images in the appropriate
 * collections according to the parameter value of `storeFindAndModifyImagesInSideCollection`.
 *
 * @tags: [requires_replication]
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

const numNodes = 2;

function checkOplogEntry(entry, lsid, txnNum, stmtId, prevTs, retryImageArgs) {
    assert.neq(entry, null);
    assert.neq(entry.lsid, null);
    assert.eq(lsid, entry.lsid.id, entry);
    assert.eq(txnNum, entry.txnNumber, entry);
    assert.eq(stmtId, entry.stmtId, entry);

    const oplogPrevTs = entry.prevOpTime.ts;
    assert.eq(prevTs.getTime(), oplogPrevTs.getTime(), entry);

    if (retryImageArgs.needsRetryImage) {
        assert.eq(retryImageArgs.imageKind, entry.needsRetryImage, entry);
        assert(!entry.hasOwnProperty("preImageOpTime"));
        assert(!entry.hasOwnProperty("postImageOpTime"));
    } else {
        assert(!entry.hasOwnProperty("needsRetryImage"));
    }
}

function checkSessionCatalog(conn, sessionId, txnNum, expectedTs) {
    const coll = conn.getDB('config').transactions;
    const sessionDoc = coll.findOne({'_id.id': sessionId});

    assert.eq(txnNum, sessionDoc.txnNum);
    const writeTs = sessionDoc.lastWriteOpTime.ts;
    assert.eq(expectedTs.getTime(), writeTs.getTime());
}

function checkImageCollection(conn, sessionInfo, expectedTs, expectedImage, expectedImageKind) {
    const coll = conn.getDB('config').image_collection;
    const imageDoc = coll.findOne({'_id.id': sessionInfo.sessionId});

    assert.eq(sessionInfo.txnNum, imageDoc.txnNum, imageDoc);
    assert.eq(expectedImage, imageDoc.image, imageDoc);
    assert.eq(expectedImageKind, imageDoc.imageKind, imageDoc);
    assert.eq(expectedTs.getTime(), imageDoc.ts.getTime(), imageDoc);
}

function assertRetryCommand(cmdResponse, retryResponse) {
    // The retry response can contain a different 'clusterTime' from the initial response.
    delete cmdResponse.$clusterTime;
    delete retryResponse.$clusterTime;

    assert.eq(cmdResponse, retryResponse);
}

function runTests(lsid, mainConn, primary, secondary, storeImagesInSideCollection, docId) {
    const setParam = {
        setParameter: 1,
        storeFindAndModifyImagesInSideCollection: storeImagesInSideCollection
    };
    primary.adminCommand(setParam);

    let txnNumber = NumberLong(docId);
    let incrementTxnNumber = function() {
        txnNumber = NumberLong(txnNumber + 1);
    };

    const oplog = primary.getDB('local').oplog.rs;

    // ////////////////////////////////////////////////////////////////////////
    // // Test findAndModify command (upsert)

    let cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        update: {$set: {x: 1}},
        new: true,
        upsert: true,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    assert.commandWorked(mainConn.getDB('test').runCommand(cmd));

    ////////////////////////////////////////////////////////////////////////
    // Test findAndModify command (in-place update, return pre-image)

    incrementTxnNumber();
    cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        update: {$inc: {x: 1}},
        new: false,
        upsert: false,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    let expectedPreImage = mainConn.getDB('test').user.findOne({_id: docId});
    let res = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assert.eq(res.value, expectedPreImage);
    // Get update entry.
    let updateOp = oplog.findOne({ns: 'test.user', op: 'u', txnNumber: txnNumber});
    // Check that the findAndModify oplog entry and sessions record has the appropriate fields
    // and values.
    const expectedWriteTs = Timestamp(0, 0);
    const expectedStmtId = 0;
    let retryArgs = {needsRetryImage: storeImagesInSideCollection, imageKind: "preImage"};
    checkOplogEntry(updateOp, lsid, txnNumber, expectedStmtId, expectedWriteTs, retryArgs);
    checkSessionCatalog(primary, lsid, txnNumber, updateOp.ts);
    checkSessionCatalog(secondary, lsid, txnNumber, updateOp.ts);
    if (storeImagesInSideCollection) {
        const sessionInfo = {sessionId: lsid, txnNum: txnNumber};
        checkImageCollection(primary, sessionInfo, updateOp.ts, expectedPreImage, "preImage");
        checkImageCollection(secondary, sessionInfo, updateOp.ts, expectedPreImage, "preImage");
    } else {
        // The preImage should be stored in the oplog.
        const preImage = oplog.findOne({ns: 'test.user', op: 'n', ts: updateOp.preImageOpTime.ts});
        assert.eq(expectedPreImage, preImage.o);
    }
    // Assert that retrying the command will produce the same response.
    let retryRes = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assertRetryCommand(res, retryRes);

    ////////////////////////////////////////////////////////////////////////
    // Test findAndModify command (in-place update, return post-image)

    incrementTxnNumber();
    cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        update: {$inc: {x: 1}},
        new: true,
        upsert: false,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    res = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    let expectedPostImage = mainConn.getDB('test').user.findOne({_id: docId});
    // Get update entry.
    updateOp = oplog.findOne({ns: 'test.user', op: 'u', txnNumber: txnNumber});
    // Check that the findAndModify oplog entry and sessions record has the appropriate fields
    // and values.
    retryArgs = {needsRetryImage: storeImagesInSideCollection, imageKind: "postImage"};
    checkOplogEntry(updateOp, lsid, txnNumber, expectedStmtId, expectedWriteTs, retryArgs);
    checkSessionCatalog(primary, lsid, txnNumber, updateOp.ts);
    checkSessionCatalog(secondary, lsid, txnNumber, updateOp.ts);
    if (storeImagesInSideCollection) {
        const sessionInfo = {sessionId: lsid, txnNum: txnNumber};
        checkImageCollection(primary, sessionInfo, updateOp.ts, expectedPostImage, "postImage");
        checkImageCollection(secondary, sessionInfo, updateOp.ts, expectedPostImage, "postImage");
    } else {
        // The postImage should be stored in the oplog.
        const postImage =
            oplog.findOne({ns: 'test.user', op: 'n', ts: updateOp.postImageOpTime.ts});
        assert.eq(expectedPostImage, postImage.o);
    }
    // Assert that retrying the command will produce the same response.
    retryRes = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assertRetryCommand(res, retryRes);

    ////////////////////////////////////////////////////////////////////////
    // Test findAndModify command (replacement update, return pre-image)
    incrementTxnNumber();
    cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        update: {y: 1},
        new: false,
        upsert: false,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    expectedPreImage = mainConn.getDB('test').user.findOne({_id: docId});
    res = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    // Get update entry.
    updateOp = oplog.findOne({ns: 'test.user', op: 'u', txnNumber: txnNumber});
    retryArgs = {needsRetryImage: storeImagesInSideCollection, imageKind: "preImage"};
    // Check that the findAndModify oplog entry and sessions record has the appropriate fields
    // and values.
    checkOplogEntry(updateOp, lsid, txnNumber, expectedStmtId, expectedWriteTs, retryArgs);
    checkSessionCatalog(primary, lsid, txnNumber, updateOp.ts);
    checkSessionCatalog(secondary, lsid, txnNumber, updateOp.ts);
    if (storeImagesInSideCollection) {
        const sessionInfo = {sessionId: lsid, txnNum: txnNumber};
        checkImageCollection(primary, sessionInfo, updateOp.ts, expectedPreImage, "preImage");
        checkImageCollection(secondary, sessionInfo, updateOp.ts, expectedPreImage, "preImage");
    } else {
        // The preImage should be stored in the oplog.
        const preImage = oplog.findOne({ns: 'test.user', op: 'n', ts: updateOp.preImageOpTime.ts});
        assert.eq(expectedPreImage, preImage.o);
    }

    // Assert that retrying the command will produce the same response.
    retryRes = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assertRetryCommand(res, retryRes);

    ////////////////////////////////////////////////////////////////////////
    // Test findAndModify command (replacement update, return post-image)

    incrementTxnNumber();
    cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        update: {z: 1},
        new: true,
        upsert: false,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    res = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    expectedPostImage = mainConn.getDB('test').user.findOne({_id: docId});

    // Get update entry.
    updateOp = oplog.findOne({ns: 'test.user', op: 'u', txnNumber: txnNumber});
    retryArgs = {needsRetryImage: storeImagesInSideCollection, imageKind: "postImage"};
    // Check that the findAndModify oplog entry and sessions record has the appropriate fields
    // and values.
    checkOplogEntry(updateOp, lsid, txnNumber, expectedStmtId, expectedWriteTs, retryArgs);
    checkSessionCatalog(primary, lsid, txnNumber, updateOp.ts);
    checkSessionCatalog(secondary, lsid, txnNumber, updateOp.ts);
    if (storeImagesInSideCollection) {
        const sessionInfo = {sessionId: lsid, txnNum: txnNumber};
        checkImageCollection(primary, sessionInfo, updateOp.ts, expectedPostImage, "postImage");
        checkImageCollection(secondary, sessionInfo, updateOp.ts, expectedPostImage, "postImage");
    } else {
        // The postImage should be stored in the oplog.
        const postImage =
            oplog.findOne({ns: 'test.user', op: 'n', ts: updateOp.postImageOpTime.ts});
        assert.eq(expectedPostImage, postImage.o);
    }
    // Assert that retrying the command will produce the same response.
    retryRes = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assertRetryCommand(res, retryRes);

    ////////////////////////////////////////////////////////////////////////
    // Test findAndModify command (remove, return pre-image)
    incrementTxnNumber();
    cmd = {
        findAndModify: 'user',
        query: {_id: docId},
        remove: true,
        new: false,
        lsid: {id: lsid},
        txnNumber: txnNumber,
        writeConcern: {w: numNodes},
    };

    expectedPreImage = mainConn.getDB('test').user.findOne({_id: docId});
    res = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));

    // Get delete entry from top of oplog.
    const deleteOp = oplog.findOne({ns: 'test.user', op: 'd', txnNumber: txnNumber});
    retryArgs = {needsRetryImage: storeImagesInSideCollection, imageKind: "preImage"};
    checkOplogEntry(deleteOp, lsid, txnNumber, expectedStmtId, expectedWriteTs, retryArgs);
    checkSessionCatalog(primary, lsid, txnNumber, deleteOp.ts);
    checkSessionCatalog(secondary, lsid, txnNumber, deleteOp.ts);
    if (storeImagesInSideCollection) {
        const sessionInfo = {sessionId: lsid, txnNum: txnNumber};
        checkImageCollection(primary, sessionInfo, deleteOp.ts, expectedPreImage, "preImage");
        checkImageCollection(secondary, sessionInfo, deleteOp.ts, expectedPreImage, "preImage");
    } else {
        // The preImage should be stored in the oplog.
        const preImage = oplog.findOne({ns: 'test.user', op: 'n', ts: deleteOp.preImageOpTime.ts});
        assert.eq(expectedPreImage, preImage.o);
    }
    // Assert that retrying the command will produce the same response.
    retryRes = assert.commandWorked(mainConn.getDB('test').runCommand(cmd));
    assertRetryCommand(res, retryRes);
}

const lsid = UUID();
const rst = new ReplSetTest({nodes: numNodes});
rst.startSet();
rst.initiate();
runTests(lsid, rst.getPrimary(), rst.getPrimary(), rst.getSecondary(), true, 40);
runTests(lsid, rst.getPrimary(), rst.getPrimary(), rst.getSecondary(), false, 50);
rst.stopSet();
// Test that retryable findAndModifys will store pre- and post- images in the
// 'config.image_collection' table.
const st = new ShardingTest({shards: {rs0: {nodes: numNodes}}});
runTests(lsid, st.s, st.rs0.getPrimary(), st.rs0.getSecondary(), true, 60);
runTests(lsid, st.s, st.rs0.getPrimary(), st.rs0.getSecondary(), true, 70);
st.stop();
})();

/**
 * Tests that mongod fails to start if enableMajorityReadConcern is set to false on non test only
 * storage engines, which are only expected to support read concern majority.
 *
 * Also verifies that the server automatically uses enableMajorityReadConcern=false if we're using a
 * test only storage engine.
 */
(function() {
"use strict";

const storageEngine = jsTest.options().storageEngine;
if (storageEngine === "wiredTiger" || storageEngine === "inMemory") {
    const conn = MongoRunner.runMongod({enableMajorityReadConcern: false});
    assert(!conn);
    var logContents = rawMongoProgramOutput();
    assert(logContents.indexOf("enableMajorityReadConcern:false is no longer supported") > 0);
    return;
}

if (storageEngine === "ephemeralForTest") {
    const conn = MongoRunner.runMongod();
    assert(conn);
    var logContents = rawMongoProgramOutput();
    assert(
        logContents.indexOf(
            "Test storage engine does not support enableMajorityReadConcern=true, forcibly setting to false") >
        0);
    MongoRunner.stopMongod(conn);
    return;
}
})();
/**
 * SERVER-20617: Tests that journaled write operations survive a kill -9 of the mongod.
 *
 * This test requires persistence to ensure data survives a restart.
 * @tags: [requires_persistence]
 */
(function() {
'use strict';

//  The following test verifies that writeConcern: {j: true} ensures that data is durable.
var dbpath = MongoRunner.dataPath + 'sync_write';
resetDbpath(dbpath);

var mongodArgs = {dbpath: dbpath, noCleanData: true, journal: ''};

// Start a mongod.
var conn = MongoRunner.runMongod(mongodArgs);
assert.neq(null, conn, 'mongod was unable to start up');

// Now connect to the mongod, do a journaled write and abruptly stop the server.
var testDB = conn.getDB('test');
assert.commandWorked(testDB.synced.insert({synced: true}, {writeConcern: {j: true}}));
MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});

// Restart the mongod.
conn = MongoRunner.runMongod(mongodArgs);
assert.neq(null, conn, 'mongod was unable to restart after receiving a SIGKILL');

// Check that our journaled write still is present.
testDB = conn.getDB('test');
assert.eq(1, testDB.synced.count({synced: true}), 'synced write was not found');
MongoRunner.stopMongod(conn);
})();

/**
 * Ensure that authorization system collections' indexes are correctly generated.
 *
 * This test requires users to persist across a restart.
 * @tags: [requires_persistence]
 */

(function() {
let conn = MongoRunner.runMongod();
let config = conn.getDB("config");
let db = conn.getDB("admin");

// TEST: User and role collections start off with no indexes
assert.eq(0, db.system.users.getIndexes().length);
assert.eq(0, db.system.roles.getIndexes().length);

// TEST: User and role creation generates indexes
db.createUser({user: "user", pwd: "pwd", roles: []});
assert.eq(2, db.system.users.getIndexes().length);

db.createRole({role: "role", privileges: [], roles: []});
assert.eq(2, db.system.roles.getIndexes().length);

// TEST: Destroying admin.system.users index and restarting will recreate it
assert.commandWorked(db.system.users.dropIndexes());
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB("admin");
assert.eq(2, db.system.users.getIndexes().length);
assert.eq(2, db.system.roles.getIndexes().length);

// TEST: Destroying admin.system.roles index and restarting will recreate it
assert.commandWorked(db.system.roles.dropIndexes());
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB("admin");
assert.eq(2, db.system.users.getIndexes().length);
assert.eq(2, db.system.roles.getIndexes().length);

// TEST: Destroying both authorization indexes and restarting will recreate them
assert.commandWorked(db.system.users.dropIndexes());
assert.commandWorked(db.system.roles.dropIndexes());
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB("admin");
assert.eq(2, db.system.users.getIndexes().length);
assert.eq(2, db.system.roles.getIndexes().length);

// TEST: Destroying the admin.system.users index and restarting will recreate it, even if
// admin.system.roles does not exist
// Use _mergeAuthzCollections to clear admin.system.users and admin.system.roles.
assert.commandWorked(db.adminCommand({
    _mergeAuthzCollections: 1,
    tempUsersCollection: 'admin.tempusers',
    tempRolesCollection: 'admin.temproles',
    db: "",
    drop: true
}));
db.createUser({user: "user", pwd: "pwd", roles: []});
assert.commandWorked(db.system.users.dropIndexes());
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB("admin");
assert.eq(2, db.system.users.getIndexes().length);

// TEST: Destroying the admin.system.roles index and restarting will recreate it, even if
// admin.system.users does not exist
// Use _mergeAuthzCollections to clear admin.system.users and admin.system.roles.
assert.commandWorked(db.adminCommand({
    _mergeAuthzCollections: 1,
    tempUsersCollection: 'admin.tempusers',
    tempRolesCollection: 'admin.temproles',
    db: "",
    drop: true
}));
db.createRole({role: "role", privileges: [], roles: []});
assert.commandWorked(db.system.roles.dropIndexes());
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, cleanData: false});
db = conn.getDB("admin");
assert.eq(2, db.system.roles.getIndexes().length);
MongoRunner.stopMongod(conn);
})();

// Tests that specifying a maxTimeMS on a getMore request to mongos is not interpreted as a deadline
// for the operationfor a tailable + awaitData cursor.
// This test was designed to reproduce SERVER-33942 against a mongos.
// @tags: [
//   requires_capped,
//   requires_sharding,
// ]
(function() {
"use strict";

const st = new ShardingTest({shards: 2});

const db = st.s.getDB("test");
const coll = db.capped;
assert.commandWorked(db.runCommand({create: "capped", capped: true, size: 1024}));
assert.commandWorked(coll.insert({}));
const findResult = assert.commandWorked(
    db.runCommand({find: "capped", filter: {}, tailable: true, awaitData: true}));

const cursorId = findResult.cursor.id;
assert.neq(cursorId, 0);

// Test that the getMores on this tailable cursor are immune to interrupt.
assert.commandWorked(
    db.adminCommand({configureFailPoint: "maxTimeAlwaysTimeOut", mode: "alwaysOn"}));
assert.commandWorked(db.runCommand({getMore: cursorId, collection: "capped", maxTimeMS: 30}));
assert.commandWorked(db.runCommand({getMore: cursorId, collection: "capped"}));
assert.commandWorked(db.adminCommand({configureFailPoint: "maxTimeAlwaysTimeOut", mode: "off"}));

st.stop();
}());

/**
 * Configures the failCommand failpoint to test the firing of a tripwire assertion (tassert).
 */
(function() {
'use strict';

let conn, testDB, adminDB;

const mongoRunnerSetupHelper = () => {
    conn = MongoRunner.runMongod({});
    testDB = conn.getDB("tassert_failpoint");
    adminDB = conn.getDB("admin");
};

/**
 * Helper for verifying the server exits with `exitCode`.
 */
const mongoRunnerExitHelper = (exitCode) => {
    assert.commandWorked(adminDB.runCommand({configureFailPoint: "failCommand", mode: "off"}));
    assert.eq(exitCode, MongoRunner.stopMongod(conn, null, {allowedExitCode: exitCode}));
};

// test with a tassert: true and closeConnection:true configuration. This should fire a tassert.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        failCommands: ["ping"],
        closeConnection: true,
        tassert: true,
    }
}));

assert.throws(() => testDB.runCommand({ping: 1}));
mongoRunnerExitHelper(MongoRunner.EXIT_ABRUPT);

// test with a tassert:true and extraInfo:true configuration. This should fire a tassert.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        errorCode: ErrorCodes.CannotImplicitlyCreateCollection,
        failCommands: ["create"],
        tassert: true,
        errorExtraInfo: {
            "ns": "namespace error",
        }
    }
}));

{
    let result = testDB.runCommand({create: "collection"});
    assert(result.ok == 0);
    assert(result.code == ErrorCodes.CannotImplicitlyCreateCollection);
    assert(result.ns == "namespace error");
}

mongoRunnerExitHelper(MongoRunner.EXIT_ABRUPT);

// test with a tassert:true and errorCode-only configuration. This should fire a tassert.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        errorCode: ErrorCodes.InvalidNamespace,
        failCommands: ["ping"],
        tassert: true,
    }
}));

{
    let result = testDB.runCommand({ping: 1});
    assert(result.code == ErrorCodes.InvalidNamespace);
}

mongoRunnerExitHelper(MongoRunner.EXIT_ABRUPT);

// test with a tassert: false and closeConnection:true configuration. This should NOT fire a
// tassert.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        failCommands: ["ping"],
        closeConnection: true,
        tassert: false,
    }
}));

assert.throws(() => testDB.runCommand({ping: 1}));
mongoRunnerExitHelper(MongoRunner.EXIT_CLEAN);

// test with a tassert:false and extraErrorInfo + errorCode configuration.
// This should NOT fire a tassert and should instead produce a uassert.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        errorCode: ErrorCodes.CannotImplicitlyCreateCollection,
        failCommands: ["create"],
        tassert: false,
        errorExtraInfo: {
            "ns": "namespace error",
        }
    }
}));

{
    let result = testDB.runCommand({create: "collection"});
    assert(result.ok == 0);
    assert(result.code == ErrorCodes.CannotImplicitlyCreateCollection);
    assert(result.ns == "namespace error");
}
mongoRunnerExitHelper(MongoRunner.EXIT_CLEAN);

// test with a tassert:false and errorCode-only configuration.
// This should NOT fire a tassert and should instead produce a uassert..
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        errorCode: ErrorCodes.InvalidNamespace,
        failCommands: ["ping"],
        tassert: false,
    }
}));

{
    let result = testDB.runCommand({ping: 1});
    assert(result.code == ErrorCodes.InvalidNamespace);
}

mongoRunnerExitHelper(MongoRunner.EXIT_CLEAN);

// test with a tassert:true only configuration.
// This should NOT fire a tassert and should NOT produce an error.
// tassert should only be fired with one of the other settings.
mongoRunnerSetupHelper();

assert.commandWorked(adminDB.runCommand({
    configureFailPoint: "failCommand",
    mode: "alwaysOn",
    data: {
        failCommands: ["ping"],
        tassert: true,
    }
}));

assert.commandWorked(testDB.runCommand({ping: 1}));

mongoRunnerExitHelper(MongoRunner.EXIT_CLEAN);
})();

/**
 * This test makes makes sure Thread works with --enableJavaScriptProtection
 */
(function() {
'use strict';
load('jstests/libs/parallelTester.js');

function testThread(threadType) {
    function threadFn(args) {
        // Ensure objects are passed through properly
        assert(args instanceof Object);
        // Ensure functions inside objects are still functions
        assert(args.func1 instanceof Function);
        assert(args.func1());
        // Ensure Code objects are converted to functions
        assert(args.func2 instanceof Function);
        assert(args.func2());
        // Ensure arrays are passed through properly
        assert(args.funcArray instanceof Array);
        // Ensure functions inside arrays are still functions.
        assert(args.funcArray[0] instanceof Function);
        assert(args.funcArray[0]());
        return true;
    }

    function returnTrue() {
        return true;
    }

    var args = {
        func1: returnTrue,
        // Pass some Code objects to simulate what happens with --enableJavaScriptProtection
        func2: new Code(returnTrue.toString()),
        funcArray: [new Code(returnTrue.toString())]
    };

    var thread = new threadType(threadFn, args);
    thread.start();
    thread.join();
    assert(thread.returnData());
}

// Test the Thread class
testThread(Thread);
}());

/**
 * Tests the collection block compressor during table creation for the following scenarios:
 * 1. The default collection block compressor for regular collections is snappy and can be
 *    configured globally.
 * 2. The default collection block compressor for time-series collections is zstd and ignores the
 *    configured global.
 * 3. The collection block compressor passed into the 'create' command has the highest precedence
 *    for all types of collections.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */
(function() {
"use strict";

jsTestLog("Scenario 1a: testing the default compressor for regular collections");
let conn = MongoRunner.runMongod({});

// The default for regular collections is snappy.
assert.commandWorked(conn.getDB("db").createCollection("a"));
let stats = conn.getDB("db").getCollection("a").stats();
assert(stats["wiredTiger"]["creationString"].search("block_compressor=snappy") > -1);

MongoRunner.stopMongod(conn);

jsTestLog("Scenario 1b: testing the globally configured compressor for regular collections");
conn = MongoRunner.runMongod({wiredTigerCollectionBlockCompressor: "none"});

assert.commandWorked(conn.getDB("db").createCollection("a"));
stats = conn.getDB("db").getCollection("a").stats();
assert(stats["wiredTiger"]["creationString"].search("block_compressor=none") > -1);

MongoRunner.stopMongod(conn);

jsTestLog("Scenario 2a: testing the default compressor for time-series collections");
conn = MongoRunner.runMongod({});

// The default for time-series collections is zstd.
const timeFieldName = 'time';
assert.commandWorked(
    conn.getDB("db").createCollection("a", {timeseries: {timeField: timeFieldName}}));
stats = conn.getDB("db").getCollection("a").stats();
assert(stats["wiredTiger"]["creationString"].search("block_compressor=zstd") > -1);

MongoRunner.stopMongod(conn);

jsTestLog("Scenario 2b: testing the globally configured compressor for time-series collections");
conn = MongoRunner.runMongod({wiredTigerCollectionBlockCompressor: "none"});

// Time-series collections ignore the globally configured compressor
assert.commandWorked(
    conn.getDB("db").createCollection("a", {timeseries: {timeField: timeFieldName}}));
stats = conn.getDB("db").getCollection("a").stats();
assert(stats["wiredTiger"]["creationString"].search("block_compressor=zstd") > -1);

MongoRunner.stopMongod(conn);

jsTestLog("Scenario 3: testing the compressor passed into the 'create' command");

// The globally configured compressor will be ignored.
conn = MongoRunner.runMongod({wiredTigerCollectionBlockCompressor: "none"});
assert.commandWorked(conn.getDB("db").createCollection(
    "a", {storageEngine: {wiredTiger: {configString: "block_compressor=zlib"}}}));
assert.commandWorked(conn.getDB("db").createCollection("b", {
    storageEngine: {wiredTiger: {configString: "block_compressor=zlib"}},
    timeseries: {timeField: timeFieldName}
}));

stats = conn.getDB("db").getCollection("a").stats();
jsTestLog(stats);
assert(stats["wiredTiger"]["creationString"].search("block_compressor=zlib") > -1);

stats = conn.getDB("db").getCollection("b").stats();
assert(stats["wiredTiger"]["creationString"].search("block_compressor=zlib") > -1);

MongoRunner.stopMongod(conn);
}());

// Tests that $changeStream aggregations against time-series collections fail cleanly.
// @tags: [
//  requires_timeseries,
//  requires_replication,
// ]
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");  // For TimeseriesTest.
load("jstests/libs/fixture_helpers.js");             // For FixtureHelpers.
load("jstests/libs/change_stream_util.js");          // For ChangeStreamTest and
                                                     // assert[Valid|Invalid]ChangeStreamNss.

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const timeFieldName = "time";
const metaFieldName = "tags";
const testDB = rst.getPrimary().getDB(jsTestName());
assert.commandWorked(testDB.dropDatabase());

if (!TimeseriesTest.timeseriesCollectionsEnabled(testDB.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const tsColl = testDB.getCollection("ts_point_data");
tsColl.drop();

assert.commandWorked(testDB.createCollection(
    tsColl.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

const nMeasurements = 10;

for (let i = 0; i < nMeasurements; i++) {
    const docToInsert = {
        time: ISODate(),
        tags: i.toString(),
        value: i + nMeasurements,
    };
    assert.commandWorked(tsColl.insert(docToInsert));
}

// Test that a changeStream cannot be opened on 'system.buckets.X' collections.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: "system.buckets." + tsColl.getName(),
    pipeline: [{$changeStream: {}}],
    cursor: {}
}),
                             ErrorCodes.InvalidNamespace);

// Test that a changeStream cannot be opened on a time-series collection because it's a view.
assert.commandFailedWithCode(
    testDB.runCommand({aggregate: tsColl.getName(), pipeline: [{$changeStream: {}}], cursor: {}}),
    ErrorCodes.CommandNotSupportedOnView);

rst.stopSet();
})();

/**
 * Tests that running collStats against a time-series collection includes statistics specific to
 * time-series collections.
 *
 * @tags: [
 *   does_not_support_stepdowns,
 *   requires_getmore,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';
const metaFieldName = 'meta';

const expectedStats = {
    bucketsNs: bucketsColl.getFullName()
};

const clearCollection = function() {
    coll.drop();
    assert.commandWorked(testDB.createCollection(
        coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
    assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

    expectedStats.bucketCount = 0;
    expectedStats.numBucketInserts = 0;
    expectedStats.numBucketUpdates = 0;
    expectedStats.numBucketsOpenedDueToMetadata = 0;
    expectedStats.numBucketsClosedDueToCount = 0;
    expectedStats.numBucketsClosedDueToSize = 0;
    expectedStats.numBucketsClosedDueToTimeForward = 0;
    expectedStats.numBucketsClosedDueToTimeBackward = 0;
    expectedStats.numBucketsClosedDueToMemoryThreshold = 0;
    expectedStats.numCommits = 0;
    expectedStats.numWaits = 0;
    expectedStats.numMeasurementsCommitted = 0;
};
clearCollection();

const checkCollStats = function(empty = false) {
    const stats = assert.commandWorked(coll.stats());

    assert.eq(coll.getFullName(), stats.ns);

    for (let [stat, value] of Object.entries(expectedStats)) {
        assert.eq(stats.timeseries[stat],
                  value,
                  "Invalid 'timeseries." + stat + "' value in collStats: " + tojson(stats));
    }

    if (empty) {
        assert(!stats.timeseries.hasOwnProperty('avgBucketSize'));
        assert(!stats.timeseries.hasOwnProperty('avgNumMeasurementsPerCommit'));
    } else {
        assert.gt(stats.timeseries.avgBucketSize, 0);
    }

    assert(!stats.timeseries.hasOwnProperty('count'));
    assert(!stats.timeseries.hasOwnProperty('avgObjSize'));
};

checkCollStats(true);

let docs = Array(2).fill({[timeFieldName]: ISODate(), [metaFieldName]: {a: 1}});
assert.commandWorked(coll.insert(docs, {ordered: false}));
expectedStats.bucketCount++;
expectedStats.numBucketInserts++;
expectedStats.numBucketsOpenedDueToMetadata++;
expectedStats.numCommits++;
expectedStats.numMeasurementsCommitted += 2;
expectedStats.avgNumMeasurementsPerCommit = 2;
checkCollStats();

assert.commandWorked(
    coll.insert({[timeFieldName]: ISODate(), [metaFieldName]: {a: 2}}, {ordered: false}));
expectedStats.bucketCount++;
expectedStats.numBucketInserts++;
expectedStats.numBucketsOpenedDueToMetadata++;
expectedStats.numCommits++;
expectedStats.numMeasurementsCommitted++;
expectedStats.avgNumMeasurementsPerCommit = 1;
checkCollStats();

assert.commandWorked(
    coll.insert({[timeFieldName]: ISODate(), [metaFieldName]: {a: 2}}, {ordered: false}));
expectedStats.numBucketUpdates++;
expectedStats.numCommits++;
expectedStats.numMeasurementsCommitted++;
checkCollStats();

docs = Array(5).fill({[timeFieldName]: ISODate(), [metaFieldName]: {a: 2}});
assert.commandWorked(coll.insert(docs, {ordered: false}));
expectedStats.numBucketUpdates++;
expectedStats.numCommits++;
expectedStats.numMeasurementsCommitted += 5;
expectedStats.avgNumMeasurementsPerCommit = 2;
checkCollStats();

assert.commandWorked(coll.insert(
    {[timeFieldName]: ISODate("2021-01-01T01:00:00Z"), [metaFieldName]: {a: 1}}, {ordered: false}));
expectedStats.bucketCount++;
expectedStats.numBucketInserts++;
expectedStats.numCommits++;
expectedStats.numBucketsClosedDueToTimeBackward++;
expectedStats.numMeasurementsCommitted++;
checkCollStats();

// Assumes each bucket has a limit of 1000 measurements.
const bucketMaxCount = 1000;
let numDocs = bucketMaxCount + 100;
docs = Array(numDocs).fill({[timeFieldName]: ISODate(), [metaFieldName]: {a: 'limit_count'}});
assert.commandWorked(coll.insert(docs, {ordered: false}));
expectedStats.bucketCount += 2;
expectedStats.numBucketInserts += 2;
expectedStats.numBucketsOpenedDueToMetadata++;
expectedStats.numBucketsClosedDueToCount++;
expectedStats.numCommits += 2;
expectedStats.numMeasurementsCommitted += numDocs;
expectedStats.avgNumMeasurementsPerCommit =
    Math.floor(expectedStats.numMeasurementsCommitted / expectedStats.numCommits);
checkCollStats();

// Assumes each bucket has a limit of 125kB on the measurements stored in the 'data' field.
const bucketMaxSizeKB = 125;
numDocs = 2;
// The measurement data should not take up all of the 'bucketMaxSizeKB' limit because we need
// to leave a little room for the _id and the time fields.
let largeValue = 'x'.repeat((bucketMaxSizeKB - 1) * 1024);
docs = Array(numDocs).fill(
    {[timeFieldName]: ISODate(), x: largeValue, [metaFieldName]: {a: 'limit_size'}});
assert.commandWorked(coll.insert(docs, {ordered: false}));
expectedStats.bucketCount += numDocs;
expectedStats.numBucketInserts += numDocs;
expectedStats.numBucketsOpenedDueToMetadata++;
expectedStats.numBucketsClosedDueToSize++;
expectedStats.numCommits += numDocs;
expectedStats.numMeasurementsCommitted += numDocs;
expectedStats.avgNumMeasurementsPerCommit =
    Math.floor(expectedStats.numMeasurementsCommitted / expectedStats.numCommits);
checkCollStats();

// Assumes the measurements in each bucket span at most one hour (based on the time field).
const docTimes = [ISODate("2020-11-13T01:00:00Z"), ISODate("2020-11-13T03:00:00Z")];
numDocs = 2;
docs = [];
for (let i = 0; i < numDocs; i++) {
    docs.push({[timeFieldName]: docTimes[i], [metaFieldName]: {a: 'limit_time_range'}});
}
assert.commandWorked(coll.insert(docs, {ordered: false}));
expectedStats.bucketCount += numDocs;
expectedStats.numBucketInserts += numDocs;
expectedStats.numBucketsOpenedDueToMetadata++;
expectedStats.numBucketsClosedDueToTimeForward++;
expectedStats.numCommits += numDocs;
expectedStats.numMeasurementsCommitted += numDocs;
expectedStats.avgNumMeasurementsPerCommit =
    Math.floor(expectedStats.numMeasurementsCommitted / expectedStats.numCommits);
checkCollStats();

const kIdleBucketExpiryMemoryUsageThreshold = 1024 * 1024 * 100;
numDocs = 70;
largeValue = 'a'.repeat(1024 * 1024);

const testIdleBucketExpiry = function(docFn) {
    clearCollection();

    let shouldExpire = false;
    for (let i = 0; i < numDocs; i++) {
        assert.commandWorked(coll.insert(docFn(i), {ordered: false}));
        const memoryUsage = assert.commandWorked(testDB.serverStatus()).bucketCatalog.memoryUsage;

        expectedStats.bucketCount++;
        expectedStats.numBucketInserts++;
        expectedStats.numBucketsOpenedDueToMetadata++;
        if (shouldExpire) {
            expectedStats.numBucketsClosedDueToMemoryThreshold++;
        }
        expectedStats.numCommits++;
        expectedStats.numMeasurementsCommitted++;
        expectedStats.avgNumMeasurementsPerCommit =
            Math.floor(expectedStats.numMeasurementsCommitted / expectedStats.numCommits);
        checkCollStats();

        shouldExpire = memoryUsage > kIdleBucketExpiryMemoryUsageThreshold;
    }

    assert(shouldExpire, 'Memory usage did not reach idle bucket expiry threshold');
};

testIdleBucketExpiry(i => {
    return {[timeFieldName]: ISODate(), [metaFieldName]: {[i.toString()]: largeValue}};
});
testIdleBucketExpiry(i => {
    return {[timeFieldName]: ISODate(), [metaFieldName]: i, a: largeValue};
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that the create command recognizes the timeseries option and only accepts valid
 * configurations of options in conjunction with and within the timeseries option.
 *
 * @tags: [
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
let collCount = 0;

const testOptions = function(allowed,
                             createOptions,
                             timeseriesOptions = {
                                 timeField: "time"
                             },
                             errorCode = ErrorCodes.InvalidOptions,
                             fixture = {
                                 // This method is run before creating time-series collection.
                                 setUp: (testDB, collName) => {},
                                 // This method is run at the end of this function after
                                 // passing all the test assertions.
                                 tearDown: (testDB, collName) => {},
                             }) {
    const testDB = conn.getDB(dbName);
    const collName = 'timeseries_' + collCount++;
    const bucketsCollName = 'system.buckets.' + collName;

    fixture.setUp(testDB, collName);
    const res = testDB.runCommand(
        Object.extend({create: collName, timeseries: timeseriesOptions}, createOptions));
    if (allowed) {
        assert.commandWorked(res);
        const collections =
            assert.commandWorked(testDB.runCommand({listCollections: 1})).cursor.firstBatch;

        const tsColl = collections.find(coll => coll.name == collName);
        assert(tsColl, collections);
        assert.eq(tsColl.type, "timeseries", tsColl);

        const bucketsColl = collections.find(coll => coll.name == bucketsCollName);
        assert(bucketsColl, collections);
        assert.eq(bucketsColl.type, "collection", bucketsColl);
        assert(bucketsColl.options.clusteredIndex, bucketsColl);
        if (createOptions.expireAfterSeconds) {
            assert.eq(bucketsColl.options.expireAfterSeconds,
                      createOptions.expireAfterSeconds,
                      bucketsColl);
        }

        assert.commandWorked(testDB.runCommand({drop: collName, writeConcern: {w: "majority"}}));
    } else {
        assert.commandFailedWithCode(res, errorCode);
    }

    fixture.tearDown(testDB, collName);

    assert(!testDB.getCollectionNames().includes(collName));
    assert(!testDB.getCollectionNames().includes(bucketsCollName));
};

const testValidTimeseriesOptions = function(timeseriesOptions) {
    testOptions(true, {}, timeseriesOptions);
};

const testInvalidTimeseriesOptions = function(timeseriesOptions, errorCode) {
    testOptions(false, {}, timeseriesOptions, errorCode);
};

const testIncompatibleCreateOptions = function(createOptions, errorCode) {
    testOptions(false, createOptions, {timeField: 'time'}, errorCode);
};

const testCompatibleCreateOptions = function(createOptions) {
    testOptions(true, createOptions);
};

const testTimeseriesNamespaceExists = function(setUp) {
    testOptions(false, {}, {timeField: "time"}, ErrorCodes.NamespaceExists, {
        setUp: setUp,
        tearDown: (testDB, collName) => {
            assert.commandWorked(testDB.dropDatabase());
        }
    });
};

testValidTimeseriesOptions({timeField: "time"});
testValidTimeseriesOptions({timeField: "time", metaField: "meta"});
testValidTimeseriesOptions({timeField: "time", metaField: "meta", granularity: "seconds"});

// A bucketMaxSpanSeconds may be provided, but only if they are the default for the granularity.
testValidTimeseriesOptions(
    {timeField: "time", metaField: "meta", granularity: "seconds", bucketMaxSpanSeconds: 60 * 60});
testValidTimeseriesOptions({
    timeField: "time",
    metaField: "meta",
    granularity: "minutes",
    bucketMaxSpanSeconds: 60 * 60 * 24
});
testValidTimeseriesOptions({
    timeField: "time",
    metaField: "meta",
    granularity: "hours",
    bucketMaxSpanSeconds: 60 * 60 * 24 * 30
});

testValidTimeseriesOptions({timeField: "time", metaField: "meta", granularity: "minutes"});
testValidTimeseriesOptions({timeField: "time", metaField: "meta", granularity: "hours"});

testInvalidTimeseriesOptions("", ErrorCodes.TypeMismatch);
testInvalidTimeseriesOptions({timeField: 100}, ErrorCodes.TypeMismatch);
testInvalidTimeseriesOptions({timeField: "time", metaField: 100}, ErrorCodes.TypeMismatch);

testInvalidTimeseriesOptions({timeField: "time", invalidOption: {}}, 40415);
testInvalidTimeseriesOptions({timeField: "sub.time"}, ErrorCodes.InvalidOptions);
testInvalidTimeseriesOptions({timeField: "time", metaField: "sub.meta"}, ErrorCodes.InvalidOptions);
testInvalidTimeseriesOptions({timeField: "time", metaField: "time"}, ErrorCodes.InvalidOptions);

testInvalidTimeseriesOptions({timeField: "time", metaField: "meta", bucketMaxSpanSeconds: 10},
                             5510500);
testInvalidTimeseriesOptions(
    {timeField: "time", metaField: "meta", granularity: 'minutes', bucketMaxSpanSeconds: 3600},
    5510500);

testCompatibleCreateOptions({expireAfterSeconds: NumberLong(100)});
testCompatibleCreateOptions({storageEngine: {}});
testCompatibleCreateOptions({indexOptionDefaults: {}});
testCompatibleCreateOptions({collation: {locale: "ja"}});
testCompatibleCreateOptions({writeConcern: {}});
testCompatibleCreateOptions({comment: ""});

testIncompatibleCreateOptions({expireAfterSeconds: NumberLong(-10)}, ErrorCodes.InvalidOptions);
testIncompatibleCreateOptions({expireAfterSeconds: NumberLong("4611686018427387904")},
                              ErrorCodes.InvalidOptions);
testIncompatibleCreateOptions({expireAfterSeconds: ""}, ErrorCodes.TypeMismatch);
testIncompatibleCreateOptions({capped: true, size: 100});
testIncompatibleCreateOptions({capped: true, max: 100});
testIncompatibleCreateOptions({autoIndexId: true});
testIncompatibleCreateOptions({idIndex: {key: {_id: 1}, name: "_id_"}});
testIncompatibleCreateOptions({validator: {}});
testIncompatibleCreateOptions({validationLevel: "off"});
testIncompatibleCreateOptions({validationAction: "warn"});
testIncompatibleCreateOptions({viewOn: "coll"});
testIncompatibleCreateOptions({viewOn: "coll", pipeline: []});
testIncompatibleCreateOptions({clusteredIndex: true});
testIncompatibleCreateOptions({clusteredIndex: false});

testTimeseriesNamespaceExists((testDB, collName) => {
    assert.commandWorked(testDB.createCollection(collName));
});
testTimeseriesNamespaceExists((testDB, collName) => {
    assert.commandWorked(testDB.createView(collName, collName + '_source', []));
});

// Tests that schema validation is enabled on the bucket collection.
{
    const testDB = conn.getDB(dbName);
    const coll = testDB.getCollection('timeseries_' + collCount++);
    coll.drop();
    assert.commandWorked(
        testDB.createCollection(coll.getName(), {timeseries: {timeField: "time"}}));
    const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
    assert.commandWorked(bucketsColl.insert(
        {control: {version: 1, min: {time: ISODate()}, max: {time: ISODate()}}, data: {}}));
    assert.commandFailedWithCode(bucketsColl.insert({
        control: {version: 'not a number', min: {time: ISODate()}, max: {time: ISODate()}},
        data: {}
    }),
                                 ErrorCodes.DocumentValidationFailure);
    assert.commandFailedWithCode(
        bucketsColl.insert(
            {control: {version: 1, min: {time: 'not a date'}, max: {time: ISODate()}}, data: {}}),
        ErrorCodes.DocumentValidationFailure);
    assert.commandFailedWithCode(
        bucketsColl.insert(
            {control: {version: 1, min: {time: ISODate()}, max: {time: 'not a date'}}, data: {}}),
        ErrorCodes.DocumentValidationFailure);
    assert.commandFailedWithCode(bucketsColl.insert({
        control: {version: 1, min: {time: ISODate()}, max: {time: ISODate()}},
        data: 'not an object'
    }),
                                 ErrorCodes.DocumentValidationFailure);
    assert.commandFailedWithCode(bucketsColl.insert({invalid_bucket_field: 1}),
                                 ErrorCodes.DocumentValidationFailure);
    assert.commandWorked(testDB.runCommand({drop: coll.getName(), writeConcern: {w: "majority"}}));
}

MongoRunner.stopMongod(conn);
})();

/**
 * Tests creating and dropping timeseries bucket collections and view definitions. Tests that we can
 * recover in both create and drop if a partial create occured where we have a bucket collection but
 * no view definition.
 * @tags: [
 *     assumes_no_implicit_collection_creation_after_drop,
 *     does_not_support_stepdowns,
 *     does_not_support_transactions,
 *     requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const rst = new ReplSetTest({nodes: 2});
rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDb = primary.getDB('test');
if (!TimeseriesTest.timeseriesCollectionsEnabled(primaryDb.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    return;
}

const coll = primaryDb.timeseries_create_drop;
const viewName = coll.getName();
const viewNs = coll.getFullName();

// Disable test if fail point is missing (running in multiversion suite)
const failpoint = 'failTimeseriesViewCreation';
if (primaryDb.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn", data: {ns: viewNs}})
        .ok === 0) {
    jsTestLog("Skipping test because the " + failpoint + " fail point is missing");
    return;
}
assert.commandWorked(primaryDb.adminCommand({configureFailPoint: failpoint, mode: "off"}));

const bucketsColl = primaryDb.getCollection('system.buckets.' + coll.getName());
const bucketsCollName = bucketsColl.getName();
const timeFieldName = 'time';
const expireAfterSecondsNum = 60;

coll.drop();

// Create should create both bucket collection and view
assert.commandWorked(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName}, expireAfterSeconds: expireAfterSecondsNum}));
assert.contains(viewName, primaryDb.getCollectionNames());
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Drop should drop both bucket collection and view
assert(coll.drop());
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == bucketsCollName), -1);

// Enable failpoint to allow bucket collection to be created but fail creation of view definition
assert.commandWorked(
    primaryDb.adminCommand({configureFailPoint: failpoint, mode: "alwaysOn", data: {ns: viewNs}}));
assert.commandFailed(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName}, expireAfterSeconds: expireAfterSecondsNum}));
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Dropping a partially created timeseries where only the bucket collection exists is allowed and
// should clean up the bucket collection
assert(coll.drop());
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == bucketsCollName), -1);

// Trying to create again yields the same result as fail point is still enabled
assert.commandFailed(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName}, expireAfterSeconds: expireAfterSecondsNum}));
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Turn off fail point and test creating view definition with existing bucket collection
assert.commandWorked(primaryDb.adminCommand({configureFailPoint: failpoint, mode: "off"}));

// Different timeField should fail
assert.commandFailed(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName + "2"}, expireAfterSeconds: expireAfterSecondsNum}));
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Different expireAfterSeconds should fail
assert.commandFailed(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName}, expireAfterSeconds: expireAfterSecondsNum + 1}));
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Omitting expireAfterSeconds should fail
assert.commandFailed(
    primaryDb.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.eq(primaryDb.getCollectionNames().findIndex(c => c == viewName), -1);
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

// Same parameters should succeed
assert.commandWorked(primaryDb.createCollection(
    coll.getName(),
    {timeseries: {timeField: timeFieldName}, expireAfterSeconds: expireAfterSecondsNum}));
assert.contains(viewName, primaryDb.getCollectionNames());
assert.contains(bucketsCollName, primaryDb.getCollectionNames());

rst.stopSet();
})();

/**
 * Tests that the indexOptionDefaults collection creation option is applied when creating indexes on
 * a time-series collection.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    MongoRunner.stopMongod(conn);
    return;
}

const testDB = conn.getDB('test');
const coll = testDB.getCollection(jsTestName());

assert.commandFailedWithCode(testDB.createCollection(coll.getName(), {
    timeseries: {timeField: 'tt', metaField: 'mm'},
    indexOptionDefaults: {storageEngine: {wiredTiger: {configString: 'invalid_option=xxx,'}}}
}),
                             ErrorCodes.BadValue);

// Sample wiredtiger configuration option from wt_index_option_defaults.js.
assert.commandWorked(testDB.createCollection(coll.getName(), {
    timeseries: {timeField: 'tt', metaField: 'mm'},
    indexOptionDefaults: {storageEngine: {wiredTiger: {configString: 'split_pct=88,'}}}
}));

assert.commandWorked(coll.insert({tt: ISODate(), mm: 'aaa'}));
assert.commandWorked(coll.createIndex({mm: 1}));

const indexCreationString = coll.stats({indexDetails: true}).indexDetails.mm_1.creationString;
assert.neq(-1, indexCreationString.indexOf(',split_pct=88,'), indexCreationString);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that direct removal in a timeseries bucket collection close the relevant bucket, preventing
 * further inserts from landing in that bucket.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const collName = 'test';

const timeFieldName = 'time';
const times = [
    ISODate('2021-01-01T01:00:00Z'),
    ISODate('2021-01-01T01:10:00Z'),
    ISODate('2021-01-01T01:20:00Z')
];
let docs = [
    {_id: 0, [timeFieldName]: times[0]},
    {_id: 1, [timeFieldName]: times[1]},
    {_id: 2, [timeFieldName]: times[2]}
];

const coll = testDB.getCollection(collName);
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
coll.drop();

assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

assert.commandWorked(coll.insert(docs[0]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 1));

let buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);

let removeResult = assert.commandWorked(bucketsColl.remove({_id: buckets[0]._id}));
assert.eq(removeResult.nRemoved, 1);

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 0);

assert.commandWorked(coll.insert(docs[1]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(1, 2));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[1]);
assert.eq(buckets[0].control.max[timeFieldName], times[1]);

let fpInsert = configureFailPoint(conn, "hangTimeseriesInsertBeforeCommit");
let awaitInsert = startParallelShell(
    funWithArgs(function(dbName, collName, doc) {
        assert.commandWorked(db.getSiblingDB(dbName).getCollection(collName).insert(doc));
    }, dbName, coll.getName(), docs[2]), conn.port);

fpInsert.wait();

removeResult = assert.commandWorked(bucketsColl.remove({_id: buckets[0]._id}));
assert.eq(removeResult.nRemoved, 1);

fpInsert.off();
awaitInsert();

assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(2, 3));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[2]);
assert.eq(buckets[0].control.max[timeFieldName], times[2]);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that direct removal in a timeseries bucket collection close the relevant bucket, preventing
 * further inserts from landing in that bucket, including the case where a concurrent catalog write
 * causes a write conflict.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const collName = 'test';

const timeFieldName = 'time';
const times = [
    ISODate('2021-01-01T01:00:00Z'),
    ISODate('2021-01-01T01:10:00Z'),
    ISODate('2021-01-01T01:20:00Z')
];
let docs = [
    {_id: 0, [timeFieldName]: times[0]},
    {_id: 1, [timeFieldName]: times[1]},
    {_id: 2, [timeFieldName]: times[2]}
];

const coll = testDB.getCollection(collName);
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
coll.drop();

assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

assert.commandWorked(coll.insert(docs[0]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 1));

let buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);

const fpInsert = configureFailPoint(conn, "hangTimeseriesInsertBeforeWrite");
const awaitInsert = startParallelShell(
    funWithArgs(function(dbName, collName, doc) {
        assert.commandWorked(db.getSiblingDB(dbName).getCollection(collName).insert(doc));
    }, dbName, coll.getName(), docs[1]), conn.port);
fpInsert.wait();

const fpRemove = configureFailPoint(conn, "hangTimeseriesDirectModificationBeforeWriteConflict");
const awaitRemove = startParallelShell(
    funWithArgs(function(dbName, collName, id) {
        const removeResult = assert.commandWorked(
            db.getSiblingDB(dbName).getCollection('system.buckets.' + collName).remove({_id: id}));
        assert.eq(removeResult.nRemoved, 1);
    }, dbName, coll.getName(), buckets[0]._id), conn.port);
fpRemove.wait();

fpRemove.off();
fpInsert.off();
awaitRemove();
awaitInsert();

// The expected ordering is that the insert finished, then the remove deleted the bucket document,
// so there should be no documents left.

assert.docEq(coll.find().sort({_id: 1}).toArray().length, 0);

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 0);

// Now another insert should generate a new bucket.

assert.commandWorked(coll.insert(docs[2]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(2, 3));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[2]);
assert.eq(buckets[0].control.max[timeFieldName], times[2]);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that direct updates to a timeseries bucket collection close the bucket, preventing further
 * inserts to land in that bucket.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const collName = 'test';

const timeFieldName = 'time';
const times = [
    ISODate('2021-01-01T01:00:00Z'),
    ISODate('2021-01-01T01:10:00Z'),
    ISODate('2021-01-01T01:20:00Z')
];
let docs = [
    {_id: 0, [timeFieldName]: times[0]},
    {_id: 1, [timeFieldName]: times[1]},
    {_id: 2, [timeFieldName]: times[2]}
];

const coll = testDB.getCollection(collName);
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
coll.drop();

assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

assert.commandWorked(coll.insert(docs[0]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 1));

let buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);

let modified = buckets[0];
modified.control.closed = true;
let updateResult = assert.commandWorked(bucketsColl.update({_id: buckets[0]._id}, modified));
assert.eq(updateResult.nMatched, 1);
assert.eq(updateResult.nModified, 1);

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);
assert(buckets[0].control.closed);

assert.commandWorked(coll.insert(docs[1]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 2));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 2);
assert.eq(buckets[1].control.min[timeFieldName], times[1]);
assert.eq(buckets[1].control.max[timeFieldName], times[1]);

let fpInsert = configureFailPoint(conn, "hangTimeseriesInsertBeforeCommit");
let awaitInsert = startParallelShell(
    funWithArgs(function(dbName, collName, doc) {
        assert.commandWorked(db.getSiblingDB(dbName).getCollection(collName).insert(doc));
    }, dbName, coll.getName(), docs[2]), conn.port);

fpInsert.wait();

modified = buckets[1];
modified.control.closed = true;
updateResult = assert.commandWorked(bucketsColl.update({_id: buckets[1]._id}, modified));
assert.eq(updateResult.nMatched, 1);
assert.eq(updateResult.nModified, 1);

fpInsert.off();
awaitInsert();

assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 3));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 3);
assert.eq(buckets[1].control.min[timeFieldName], times[1]);
assert.eq(buckets[1].control.max[timeFieldName], times[1]);
assert(buckets[1].control.closed);
assert.eq(buckets[2].control.min[timeFieldName], times[2]);
assert.eq(buckets[2].control.max[timeFieldName], times[2]);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that direct updates to a timeseries bucket collection close the bucket, preventing further
 * inserts to land in that bucket, including the case where a concurrent catalog write causes
 * a write conflict.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const collName = 'test';

const timeFieldName = 'time';
const times = [
    ISODate('2021-01-01T01:00:00Z'),
    ISODate('2021-01-01T01:10:00Z'),
    ISODate('2021-01-01T01:20:00Z')
];
let docs = [
    {_id: 0, [timeFieldName]: times[0]},
    {_id: 1, [timeFieldName]: times[1]},
    {_id: 2, [timeFieldName]: times[2]}
];

const coll = testDB.getCollection(collName);
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
coll.drop();

assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

assert.commandWorked(coll.insert(docs[0]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 1));

let buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);

const fpInsert = configureFailPoint(conn, "hangTimeseriesInsertBeforeWrite");
const awaitInsert = startParallelShell(
    funWithArgs(function(dbName, collName, doc) {
        assert.commandWorked(
            db.getSiblingDB(dbName).getCollection(collName).insert(doc, {ordered: false}));
    }, dbName, coll.getName(), docs[1]), conn.port);
fpInsert.wait();

const modified = buckets[0];
modified.control.closed = true;

const fpUpdate = configureFailPoint(conn, "hangTimeseriesDirectModificationBeforeWriteConflict");
const awaitUpdate = startParallelShell(
    funWithArgs(function(dbName, collName, update) {
        const updateResult = assert.commandWorked(db.getSiblingDB(dbName)
                                                      .getCollection('system.buckets.' + collName)
                                                      .update({_id: update._id}, update));
        assert.eq(updateResult.nMatched, 1);
        assert.eq(updateResult.nModified, 1);
    }, dbName, coll.getName(), modified), conn.port);
fpUpdate.wait();

fpUpdate.off();
fpInsert.off();
awaitUpdate();
awaitInsert();

// The expected ordering is that the insert finished, then the update overwrote the bucket document,
// so there should be one document, and a closed flag.

assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 1));

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 1);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);
assert(buckets[0].control.closed);

// Now another insert should generate a new bucket.

assert.commandWorked(coll.insert(docs[2]));
assert.docEq(coll.find().sort({_id: 1}).toArray(), [docs[0], docs[2]]);

buckets = bucketsColl.find().sort({_id: 1}).toArray();
assert.eq(buckets.length, 2);
assert.eq(buckets[0].control.min[timeFieldName], times[0]);
assert.eq(buckets[0].control.max[timeFieldName], times[0]);
assert.eq(buckets[1].control.min[timeFieldName], times[2]);
assert.eq(buckets[1].control.max[timeFieldName], times[2]);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that $_unpackBucket can still work properly if put after other stages like $match.
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const measurementsPerBucket = 10;
const nBuckets = 10;
const conn =
    MongoRunner.runMongod({setParameter: {timeseriesBucketMaxCount: measurementsPerBucket}});
const testDB = conn.getDB(jsTestName());

if (!TimeseriesTest.timeseriesCollectionsEnabled(testDB.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

assert.commandWorked(testDB.dropDatabase());
const tsColl = testDB.getCollection("tsColl");
assert.commandWorked(testDB.createCollection(
    tsColl.getName(), {timeseries: {timeField: "start", metaField: "meta"}}));
const sysColl = testDB.getCollection("system.buckets." + tsColl.getName());

const bulk = tsColl.initializeUnorderedBulkOp();
const aMinuteInMs = 60 * 1000;
const seedDate = new Date("2020-11-30T12:10:05Z");
for (let i = 0; i < nBuckets; i++) {
    for (let j = 0; j < measurementsPerBucket; j++) {
        const seqNumber = i * measurementsPerBucket + j;
        bulk.insert({
            start: new Date(seedDate.valueOf() + seqNumber * aMinuteInMs),
            end: new Date(seedDate.valueOf() + (seqNumber + 1) * aMinuteInMs),
            meta: "bucket_" + i,
            value: seqNumber,
        });
    }
}
assert.commandWorked(bulk.execute());
assert.eq(nBuckets, sysColl.find().itcount());

// Use a filter to get some bucket IDs.
const bucketIds = sysColl
                      .aggregate([
                          {
                              $match: {
                                  _id: {
                                      $gt: ObjectId("5fc4e5c80000000000000000"),
                                      $lt: ObjectId("5fc4ea7e0000000000000000")
                                  },
                                  "control.max.end": {$lte: ISODate("2020-11-30T13:00:05Z")}
                              }
                          },
                          {$sort: {_id: 1}}
                      ])
                      .toArray();
// Should only get the IDs for 3 out of 10 buckets (bucket 2, 3 and 4).
assert.eq(3, bucketIds.length, bucketIds);
let ids = [];
for (let i = 0; i < bucketIds.length; i++) {
    ids.push(bucketIds[i]._id);
}
// Only unpack the designated buckets.
let getFilteredMeasurements = () =>
    sysColl
        .aggregate([
            {$match: {_id: {$in: ids}}},
            {$_unpackBucket: {timeField: "start", metaField: "meta"}},
            {$sort: {value: 1}},
            {$project: {_id: 0}}
        ])
        .toArray();
let filteredMeasurements = getFilteredMeasurements();
let assertMeasurementsInBuckets = (lo, hi, measurements) => {
    let k = 0;
    // Only measurements from bucket 2, 3, and 4 are unpacked.
    for (let i = lo; i <= hi; i++) {
        for (let j = 0; j < measurementsPerBucket; j++, k++) {
            const seqNumber = i * measurementsPerBucket + j;
            assert.docEq({
                start: new Date(seedDate.valueOf() + seqNumber * aMinuteInMs),
                end: new Date(seedDate.valueOf() + (seqNumber + 1) * aMinuteInMs),
                meta: "bucket_" + i,
                value: seqNumber,
            },
                         measurements[k]);
        }
    }
};
assertMeasurementsInBuckets(2, 4, filteredMeasurements);

// Delete bucket 2.
assert.commandWorked(sysColl.deleteOne({_id: ids[0]}));
filteredMeasurements = getFilteredMeasurements();
assertMeasurementsInBuckets(3, 4, filteredMeasurements);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that time-series inserts are properly handled when a node steps down from primary and then
 * later steps back up.
 *
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');

const replTest = new ReplSetTest({nodes: 2});
replTest.startSet();
replTest.initiate();

if (!TimeseriesTest.timeseriesCollectionsEnabled(replTest.getPrimary())) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    replTest.stopSet();
    return;
}

const dbName = 'test';
const numColls = 3;

const testDB = function() {
    return replTest.getPrimary().getDB(dbName);
};

const coll = function(num) {
    return testDB()[jsTestName() + '_' + num];
};

const bucketsColl = function(num) {
    return testDB()['system.buckets.' + coll(num).getName()];
};

const timeFieldName = 'time';
const metaFieldName = 'meta';

const createColl = function(num) {
    assert.commandWorked(testDB().createCollection(
        coll(num).getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
};

for (let i = 0; i < numColls; i++) {
    createColl(i);
}

const docs = [
    {_id: 0, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 1, [timeFieldName]: ISODate(), [metaFieldName]: 0},
];

for (let i = 0; i < numColls; i++) {
    assert.commandWorked(coll(i).insert(docs[0]));
}

replTest.stepUp(replTest.getSecondary());

// Manually update the bucket for collection 1.
assert.commandWorked(bucketsColl(1).update({}, {$set: {meta: 1}}));
assert.commandWorked(bucketsColl(1).update({}, {$set: {meta: 0}}));

// Drop, recreate, and reinsert the bucket for collection 2.
assert(coll(2).drop());
createColl(2);
assert.commandWorked(coll(2).insert(docs[0]));

// Step back up the original primary.
replTest.stepUp(replTest.getSecondary());

for (let i = 0; i < numColls; i++) {
    assert.commandWorked(coll(i).insert(docs[1]));
}

const checkColl = function(num, numBuckets) {
    jsTestLog('Checking collection ' + num);
    assert.docEq(coll(num).find().sort({_id: 1}).toArray(), docs);
    const buckets = bucketsColl(num).find().toArray();
    assert.eq(buckets.length,
              numBuckets,
              'Expected ' + numBuckets + ' bucket(s) but found: ' + tojson(buckets));
};

// For collection 0, the original bucket should still be usable.
checkColl(0, 1);
// For collections 1 and 2, the original bucket should have been closed.
checkColl(1, 2);
checkColl(2, 2);

replTest.stopSet();
})();

/**
 * Tests that a failed time-series insert does not leave behind any invalid state.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load('jstests/libs/fail_point_util.js');

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    MongoRunner.stopMongod(conn);
    return;
}

const testDB = conn.getDB(jsTestName());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';
const metaFieldName = 'meta';

const resetColl = function() {
    coll.drop();
    assert.commandWorked(testDB.createCollection(
        coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
    assert.contains(bucketsColl.getName(), testDB.getCollectionNames());
};

const docs = [
    {_id: 0, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 1, [timeFieldName]: ISODate(), [metaFieldName]: 0},
];

const runTest = function(ordered) {
    jsTestLog('Running test with {ordered: ' + ordered + '} inserts');
    resetColl();

    const fp1 = configureFailPoint(conn, 'failAtomicTimeseriesWrites');
    const fp2 = configureFailPoint(conn, 'failUnorderedTimeseriesInsert', {metadata: 0});

    assert.commandFailed(coll.insert(docs[0], {ordered: ordered}));

    fp1.off();
    fp2.off();

    // Insert a document that belongs in the same bucket that the failed insert would have gone
    // into.
    assert.commandWorked(coll.insert(docs[1], {ordered: ordered}));

    // There should not be any leftover state from the failed insert.
    assert.docEq(coll.find().toArray(), [docs[1]]);
    const buckets = bucketsColl.find().sort({['control.min.' + timeFieldName]: 1}).toArray();
    jsTestLog('Checking buckets: ' + tojson(buckets));
    assert.eq(buckets.length, 1);
    assert.eq(buckets[0].control.min._id, docs[1]._id);
};

runTest(true);
runTest(false);

MongoRunner.stopMongod(conn);
})();
/**
 * Tests that a time-series collection rejects documents with invalid timeField values
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
coll.drop();

const timeFieldName = 'time';
const metaFieldName = 'meta';

assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

// first test a good doc just in case
const goodDocs = [
    {
        _id: 0,
        time: ISODate("2020-11-26T00:00:00.000Z"),
        meta: "A",
        data: true,
    },
    {
        _id: 1,
        time: ISODate("2020-11-27T00:00:00.000Z"),
        meta: "A",
        data: true,
    }
];
assert.commandWorked(coll.insert(goodDocs[0]));
assert.eq(1, coll.count());
assert.docEq(coll.find().toArray(), [goodDocs[0]]);

// now make sure we reject if timeField is missing or isn't a valid BSON datetime
let mixedDocs = [{meta: "B", data: true}, goodDocs[1], {time: "invalid", meta: "B", data: false}];
assert.commandFailedWithCode(coll.insert(mixedDocs, {ordered: false}), ErrorCodes.BadValue);
assert.eq(coll.count(), 2);
assert.docEq(coll.find().toArray(), goodDocs);
assert.eq(null, coll.findOne({meta: mixedDocs[0].meta}));
assert.eq(null, coll.findOne({meta: mixedDocs[2].meta}));

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that inserting into a time-series collection fails if the corresponding buckets collection
 * does not exist.
 */
(function() {
'use strict';

load('jstests/libs/fail_point_util.js');
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();
const testDB = conn.getDB('test');

const timeFieldName = 'time';

let testCounter = 0;
const runTest = function(ordered, insertBeforeDrop, dropBucketsColl) {
    const coll = testDB[jsTestName() + '_' + testCounter++];

    assert.commandWorked(
        testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));

    if (insertBeforeDrop) {
        assert.commandWorked(coll.insert({_id: 0, [timeFieldName]: ISODate()}));
    }

    const fp = configureFailPoint(conn, 'hangTimeseriesInsertBeforeWrite');

    const awaitDrop = startParallelShell(
        funWithArgs(
            function(collName, fpName, fpTimesEntered) {
                load("jstests/libs/fail_point_util.js");

                assert.commandWorked(db.adminCommand({
                    waitForFailPoint: fpName,
                    timesEntered: fpTimesEntered + 1,
                    maxTimeMS: kDefaultWaitForFailPointTimeout,
                }));

                assert(db[collName].drop());

                assert.commandWorked(db.adminCommand({configureFailPoint: fpName, mode: 'off'}));
            },
            dropBucketsColl ? 'system.buckets.' + coll.getName() : coll.getName(),
            fp.failPointName,
            fp.timesEntered),
        conn.port);

    assert.commandFailedWithCode(
        coll.insert({_id: 1, [timeFieldName]: ISODate()}, {ordered: ordered}),
        ErrorCodes.NamespaceNotFound);

    awaitDrop();
};

for (const dropBucketsColl of [false, true]) {
    runTest(false /* ordered */, false /* insertBeforeDrop */, dropBucketsColl);
    runTest(false /* ordered */, true /* insertBeforeDrop */, dropBucketsColl);
    runTest(true /* ordered */, false /* insertBeforeDrop */, dropBucketsColl);
    runTest(true /* ordered */, true /* insertBeforeDrop */, dropBucketsColl);
}

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that time-series inserts respect {ordered: false}.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load('jstests/libs/fail_point_util.js');

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    MongoRunner.stopMongod(conn);
    return;
}

const testDB = conn.getDB(jsTestName());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';
const metaFieldName = 'meta';

coll.drop();
assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

const docs = [
    {_id: 0, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 1, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 2, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 3, [timeFieldName]: ISODate(), [metaFieldName]: 1},
    {_id: 4, [timeFieldName]: ISODate(), [metaFieldName]: 1},
];

assert.commandWorked(coll.insert(docs[0]));

const fp = configureFailPoint(conn, 'failUnorderedTimeseriesInsert', {metadata: 0});

// Insert two documents that would go into the existing bucket and two documents that go into a new
// bucket.
const res = assert.commandFailed(coll.insert(docs.slice(1), {ordered: false}));

jsTestLog('Checking insert result: ' + tojson(res));
assert.eq(res.nInserted, 2);
assert.eq(res.getWriteErrors().length, docs.length - res.nInserted - 1);
for (let i = 0; i < res.getWriteErrors().length; i++) {
    assert.eq(res.getWriteErrors()[i].index, i);
    assert.docEq(res.getWriteErrors()[i].getOperation(), docs[i + 1]);
}

assert.docEq(coll.find().sort({_id: 1}).toArray(), [docs[0], docs[3], docs[4]]);
assert.eq(bucketsColl.count(),
          2,
          'Expected two buckets but found: ' + tojson(bucketsColl.find().toArray()));

fp.off();

// The documents should go into two new buckets due to the failed insert on the existing bucket.
assert.commandWorked(coll.insert(docs.slice(1, 3), {ordered: false}));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs);
assert.eq(bucketsColl.count(),
          3,
          'Expected three buckets but found: ' + tojson(bucketsColl.find().toArray()));

MongoRunner.stopMongod(conn);
})();
/**
 * Tests that time-series inserts respect {ordered: true}.
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load('jstests/libs/fail_point_util.js');

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    MongoRunner.stopMongod(conn);
    return;
}

const testDB = conn.getDB(jsTestName());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';
const metaFieldName = 'meta';

coll.drop();
assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

const docs = [
    {_id: 0, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 1, [timeFieldName]: ISODate(), [metaFieldName]: 1},
    {_id: 2, [timeFieldName]: ISODate(), [metaFieldName]: 0},
    {_id: 5, [timeFieldName]: ISODate(), [metaFieldName]: 1},
    {_id: 6, [timeFieldName]: ISODate(), [metaFieldName]: 2},
];

assert.commandWorked(coll.insert(docs.slice(0, 2)));

const fp1 = configureFailPoint(conn, 'failAtomicTimeseriesWrites');
const fp2 = configureFailPoint(conn, 'failUnorderedTimeseriesInsert', {metadata: 1});

const res = assert.commandFailed(coll.insert(docs.slice(2), {ordered: true}));

jsTestLog('Checking insert result: ' + tojson(res));
assert.eq(res.nInserted, 1);
assert.eq(res.getWriteErrors().length, 1);
assert.eq(res.getWriteErrors()[0].index, 1);
assert.docEq(res.getWriteErrors()[0].getOperation(), docs[3]);

// The document that successfully inserted should go into a new bucket due to the failed insert on
// the existing bucket.
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs.slice(0, 3));
assert.eq(bucketsColl.count(),
          3,
          'Expected two buckets but found: ' + tojson(bucketsColl.find().toArray()));

fp1.off();
fp2.off();

// The documents should go into two new buckets due to the failed insert on the existing bucket.
assert.commandWorked(coll.insert(docs.slice(3), {ordered: true}));
assert.docEq(coll.find().sort({_id: 1}).toArray(), docs);
assert.eq(bucketsColl.count(),
          5,
          'Expected four buckets but found: ' + tojson(bucketsColl.find().toArray()));

MongoRunner.stopMongod(conn);
})();
/**
 * Tests that time-series inserts behave properly after previous time-series inserts were rolled
 * back.
 *
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/replsets/libs/rollback_test.js');

const rollbackTest = new RollbackTest(jsTestName());

const primary = rollbackTest.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB[jsTestName()];
const bucketsColl = testDB['system.buckets.' + coll.getName()];

const timeFieldName = 'time';
const metaFieldName = 'meta';

assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

rollbackTest.transitionToRollbackOperations();

const docs = [
    {_id: 0, [timeFieldName]: ISODate(), [metaFieldName]: 'ordered'},
    {_id: 1, [timeFieldName]: ISODate(), [metaFieldName]: 'unordered'},
    {_id: 2, [timeFieldName]: ISODate(), [metaFieldName]: 'ordered'},
    {_id: 3, [timeFieldName]: ISODate(), [metaFieldName]: 'unordered'},
];

// Insert new buckets that will be rolled back.
assert.commandWorked(coll.insert(docs[0], {ordered: true}));
assert.commandWorked(coll.insert(docs[1], {ordered: false}));

// Perform the rollback.
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

// Cycle through the rollback test phases so that the original primary becomes primary again.
rollbackTest.transitionToRollbackOperations();
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

// The in-memory bucket catalog should have been cleared by the rollback, so inserts will not
// attempt to go into now-nonexistent buckets.
assert.commandWorked(coll.insert(docs[2], {ordered: true}));
assert.commandWorked(coll.insert(docs[3], {ordered: false}));

assert.docEq(coll.find().toArray(), docs.slice(2));
const buckets = bucketsColl.find().toArray();
assert.eq(buckets.length, 2, 'Expected two bucket but found: ' + tojson(buckets));

rollbackTest.stop();
})();

/**
 * Tests inserting sample data into the time-series buckets collection.
 * This test is for the simple case of only one measurement per bucket.
 * @tags: [
 *   assumes_no_implicit_collection_creation_after_drop,
 *   does_not_support_stepdowns,
 *   requires_getmore,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const testDB = conn.getDB('test');
const coll = testDB.timeseries_latency_stats;
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

coll.drop();

const timeFieldName = 'time';
assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

const getLatencyStats = () => {
    const stats = coll.aggregate([{$collStats: {latencyStats: {}}}]).next();
    assert(stats.hasOwnProperty("latencyStats"));
    assert(stats.latencyStats.hasOwnProperty("writes"));
    return stats.latencyStats.writes;
};

const stats1 = getLatencyStats();
assert.eq(stats1.ops, 0);
assert.eq(stats1.latency, 0);

assert.commandWorked(coll.insert({[timeFieldName]: new Date(), x: 1}));

const stats2 = getLatencyStats();
assert.eq(stats2.ops, 1);
assert.gt(stats2.latency, stats1.latency);

const reps = 10;
for (let i = 0; i < reps; ++i) {
    assert.commandWorked(coll.insert({[timeFieldName]: new Date(), x: 1}));
}

const stats3 = getLatencyStats();
assert.eq(stats3.ops, 1 + reps);
assert.gt(stats3.latency, stats2.latency);

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that the cluster cannot be downgraded when there are secondary indexes on time-series
 * measurements present. Additionally, this verifies that only indexes that are incompatible for
 * downgrade have the "originalSpec" field present on the buckets index definition.
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();
const db = conn.getDB("test");

if (!TimeseriesTest.timeseriesMetricIndexesEnabled(db.getMongo())) {
    jsTestLog(
        "Skipped test as the featureFlagTimeseriesMetricIndexes feature flag is not enabled.");
    MongoRunner.stopMongod(conn);
    return;
}

const collName = "timeseries_measurement_indexes_downgrade";
const coll = db.getCollection(collName);
const bucketsColl = db.getCollection("system.buckets." + collName);

const timeFieldName = "tm";
const metaFieldName = "mm";

assert.commandWorked(db.createCollection("regular"));
assert.commandWorked(db.createCollection("system.buckets.abc"));

assert.commandWorked(db.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

function checkIndexForDowngrade(isCompatible, createdOnBucketsCollection) {
    const index = bucketsColl.getIndexes()[0];

    if (isCompatible) {
        assert(!index.hasOwnProperty("originalSpec"));
    } else {
        if (createdOnBucketsCollection) {
            // Indexes created directly on the buckets collection do not have the original user
            // index definition.
            assert(!index.hasOwnProperty("originalSpec"));
        } else {
            assert(index.hasOwnProperty("originalSpec"));
        }

        assert.commandFailedWithCode(db.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}),
                                     ErrorCodes.CannotDowngrade);
        assert.commandWorked(coll.dropIndexes("*"));
    }

    assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}));
    assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion: latestFCV}));

    assert.commandWorked(coll.dropIndexes("*"));
}

assert.commandWorked(coll.createIndex({[timeFieldName]: 1}));
checkIndexForDowngrade(true, false);

assert.commandWorked(coll.createIndex({[metaFieldName]: 1}));
checkIndexForDowngrade(true, false);

assert.commandWorked(coll.createIndex({[metaFieldName]: 1, a: 1}));
checkIndexForDowngrade(false, false);

assert.commandWorked(coll.createIndex({b: 1}));
checkIndexForDowngrade(false, false);

assert.commandWorked(bucketsColl.createIndex({"control.min.c.d": 1, "control.max.c.d": 1}));
checkIndexForDowngrade(false, true);

assert.commandWorked(bucketsColl.createIndex({"control.min.e": 1, "control.min.f": 1}));
checkIndexForDowngrade(false, true);

assert.commandWorked(coll.createIndex({g: "2dsphere"}));
checkIndexForDowngrade(false, false);

assert.commandWorked(coll.createIndex({[metaFieldName]: "2d"}));
checkIndexForDowngrade(true, false);

assert.commandWorked(coll.createIndex({[metaFieldName]: "2dsphere"}));
checkIndexForDowngrade(true, false);

MongoRunner.stopMongod(conn);
}());

/**
 * Tests retrying of time-series insert operations.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');

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
if (!TimeseriesTest.timeseriesCollectionsEnabled(primary)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    rst.stopSet();
    return;
}

const timeFieldName = 'time';
let collCount = 0;

let retriedCommandsCount = 0;
let retriedStatementsCount = 0;

/**
 * Accepts three arrays of measurements. The first set of measurements is used to create a new
 * bucket. The second and third sets of measurements are used to append to the bucket that was just
 * created. We should see one bucket created in the time-series collection.
 */
const runTest = function(docsInsert, docsUpdateA, docsUpdateB) {
    const session = primary.startSession({retryWrites: true});
    const testDB = session.getDatabase('test');

    const coll = testDB.getCollection('t_' + collCount++);
    const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
    coll.drop();

    jsTestLog('Running test: collection: ' + coll.getFullName() + '; bucket collection: ' +
              bucketsColl.getFullName() + '; initial measurements: ' + tojson(docsInsert) +
              '; measurements to append A: ' + tojson(docsUpdateA) +
              '; measurements to append B: ' + tojson(docsUpdateB));

    assert.commandWorked(
        testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
    assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

    // For retryable writes, the server uses 'txnNumber' as the key to look up previously executed
    // operations in the sesssion.
    assert.commandWorked(
        testDB.runCommand({
            insert: coll.getName(),
            documents: docsInsert,
            lsid: session.getSessionId(),
            txnNumber: NumberLong(0),
        }),
        'failed to create bucket with initial docs (first write): ' + tojson(docsInsert));
    assert.commandWorked(
        testDB.runCommand({
            insert: coll.getName(),
            documents: docsInsert,
            lsid: session.getSessionId(),
            txnNumber: NumberLong(0),
        }),
        'failed to create bucket with initial docs (retry write): ' + tojson(docsInsert));

    assert.commandWorked(testDB.runCommand({
        insert: coll.getName(),
        documents: docsUpdateA,
        lsid: session.getSessionId(),
        txnNumber: NumberLong(1),
    }),
                         'failed to append docs A to bucket (first write): ' + tojson(docsUpdateA));
    assert.commandWorked(testDB.runCommand({
        insert: coll.getName(),
        documents: docsUpdateA,
        lsid: session.getSessionId(),
        txnNumber: NumberLong(1),
    }),
                         'failed to append docs A to bucket (retry write): ' + tojson(docsUpdateA));

    assert.commandWorked(testDB.runCommand({
        insert: coll.getName(),
        documents: docsUpdateB,
        lsid: session.getSessionId(),
        txnNumber: NumberLong(2),
    }),
                         'failed to append docs B to bucket (first write): ' + tojson(docsUpdateB));
    assert.commandWorked(testDB.runCommand({
        insert: coll.getName(),
        documents: docsUpdateB,
        lsid: session.getSessionId(),
        txnNumber: NumberLong(2),
    }),
                         'failed to append docs B to bucket (retry write): ' + tojson(docsUpdateB));

    // This test case ensures that the batch size error handling is consistent with non-time-series
    // collections.
    assert.commandFailedWithCode(testDB.runCommand({
        insert: coll.getName(),
        documents: [],  // No documents
        lsid: session.getSessionId(),
        txnNumber: NumberLong(4),
    }),
                                 ErrorCodes.InvalidLength);

    const docs = docsInsert.concat(docsUpdateA, docsUpdateB);

    // Check view.
    const viewDocs = coll.find({}).sort({_id: 1}).toArray();
    assert.eq(docs.length, viewDocs.length, viewDocs);
    for (let i = 0; i < docs.length; i++) {
        assert.docEq(docs[i], viewDocs[i], 'unexpected doc from view: ' + i);
    }

    // Check bucket collection.
    const bucketDocs = bucketsColl.find().sort({_id: 1}).toArray();
    assert.eq(1, bucketDocs.length, bucketDocs);

    const bucketDoc = bucketDocs[0];
    jsTestLog('Bucket for test collection: ' + coll.getFullName() +
              ': bucket collection: ' + bucketsColl.getFullName() + ': ' + tojson(bucketDoc));

    // Check bucket.
    assert.eq(docs.length,
              Object.keys(bucketDoc.data[timeFieldName]).length,
              'invalid number of measurements in first bucket: ' + tojson(bucketDoc));

    // Keys in data field should match element indexes in 'docs' array.
    for (let i = 0; i < docs.length; i++) {
        assert(bucketDoc.data[timeFieldName].hasOwnProperty(i.toString()),
               'missing element for index ' + i + ' in data field: ' + tojson(bucketDoc));
        assert.eq(docs[i][timeFieldName],
                  bucketDoc.data[timeFieldName][i.toString()],
                  'invalid time for measurement ' + i + ' in data field: ' + tojson(bucketDoc));
    }

    const transactionsServerStatus = testDB.serverStatus().transactions;
    assert.eq(retriedCommandsCount += 3,
              transactionsServerStatus.retriedCommandsCount,
              'Incorrect statistic in db.serverStatus(): ' + tojson(transactionsServerStatus));
    assert.eq(retriedStatementsCount += docs.length,
              transactionsServerStatus.retriedStatementsCount,
              'Incorrect statistic in db.serverStatus(): ' + tojson(transactionsServerStatus));

    session.endSession();
};

const t = [
    ISODate("2021-01-20T00:00:00.000Z"),
    ISODate("2021-01-20T00:10:00.000Z"),
    ISODate("2021-01-20T00:20:00.000Z"),
    ISODate("2021-01-20T00:30:00.000Z"),
    ISODate("2021-01-20T00:40:00.000Z"),
    ISODate("2021-01-20T00:50:00.000Z"),
];

// One measurement per write operation.
runTest([{_id: 0, time: t[0], x: 0}], [{_id: 1, time: t[1], x: 1}], [{_id: 2, time: t[2], x: 2}]);
runTest([{_id: 0, time: t[0], x: 0}, {_id: 1, time: t[1], x: 1}],
        [{_id: 2, time: t[2], x: 2}, {_id: 3, time: t[3], x: 3}],
        [{_id: 4, time: t[4], x: 4}, {_id: 5, time: t[5], x: 5}]);

rst.stopSet();
})();

/**
 * Tests time-series retryable writes oplog entries are correctly chained together so that a retry
 * after restarting the server doesn't perform a write that was already executed.
 *
 * @tags: [
 *     requires_replication,
 *     requires_persistence,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

function testRetryableRestart(ordered) {
    const replTest = new ReplSetTest({nodes: 1});
    replTest.startSet();
    replTest.initiate();

    const primary = replTest.getPrimary();

    if (!TimeseriesTest.timeseriesCollectionsEnabled(primary)) {
        jsTestLog("Skipping test because the time-series collection feature flag is disabled");
        replTest.stopSet();
        return;
    }

    const testDB = primary.startSession({retryWrites: true}).getDatabase("test");
    const coll = testDB[jsTestName()];

    assert.commandWorked(testDB.createCollection(
        coll.getName(), {timeseries: {timeField: "time", metaField: "meta"}}));

    function setupRetryableWritesForCollection(collName) {
        jsTestLog("Setting up the test collection");
        assert.commandWorked(coll.insert(
            [
                {time: ISODate(), x: 0, meta: 0},
                {time: ISODate(), x: 1, meta: 0},
                {time: ISODate(), x: 0, meta: 1},
                {time: ISODate(), x: 1, meta: 1},
            ],
            {writeConcern: {w: "majority"}}));

        const insertTag = "retryable insert " + collName;
        const updateTag = "retryable update " + collName;
        return {
            collName: collName,
            insertTag: insertTag,
            updateTag: updateTag,
            retryableInsertCommand: {
                insert: collName,
                documents: [
                    // Batched inserts resulting in "inserts".
                    {x: 0, time: ISODate(), tag: insertTag, meta: 2},
                    {x: 1, time: ISODate(), tag: insertTag, meta: 2},
                    {x: 0, time: ISODate(), tag: insertTag, meta: 3},
                    {x: 1, time: ISODate(), tag: insertTag, meta: 3},
                    // Batched inserts resulting in "updates".
                    {x: 2, time: ISODate(), tag: updateTag, meta: 0},
                    {x: 3, time: ISODate(), tag: updateTag, meta: 0},
                    {x: 2, time: ISODate(), tag: updateTag, meta: 1},
                    {x: 3, time: ISODate(), tag: updateTag, meta: 1},
                ],
                txnNumber: NumberLong(0),
                lsid: {id: UUID()},
                ordered: ordered,
            },
        };
    }

    function testRetryableWrites(writes) {
        const kCollName = writes.collName;
        jsTestLog("Testing retryable inserts");
        assert.commandWorked(testDB.runCommand(writes.retryableInsertCommand));
        // If retryable inserts don't work, we will see 8 here.
        assert.eq(4, testDB[kCollName].find({tag: writes.insertTag}).itcount());
        assert.eq(4, testDB[kCollName].find({tag: writes.updateTag}).itcount());
    }

    const retryableWrites = setupRetryableWritesForCollection(coll.getName());
    jsTestLog("Run retryable writes");
    assert.commandWorked(testDB.runCommand(retryableWrites.retryableInsertCommand));

    jsTestLog("Restarting the server to reconstruct retryable writes info");
    replTest.restart(primary);
    // Forces to block until the primary becomes writable.
    replTest.getPrimary();

    testRetryableWrites(retryableWrites);

    replTest.stopSet();
}

testRetryableRestart(true);
testRetryableRestart(false);
})();

/**
 * Tests inserting sample data into the time-series buckets collection. This test is for the
 * exercising the optimized $sample implementation for $_internalUnpackBucket.
 * @tags: [
 *     requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");
load("jstests/libs/analyze_plan.js");

let conn = MongoRunner.runMongod({setParameter: {timeseriesBucketMaxCount: 100}});

// Although this test is tagged with 'requires_wiredtiger', this is not sufficient for ensuring
// that the parallel suite runs this test only on WT configurations.
if (jsTest.options().storageEngine && jsTest.options().storageEngine !== "wiredTiger") {
    jsTest.log("Skipping test on non-WT storage engine: " + jsTest.options().storageEngine);
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
let testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

if (!TimeseriesTest.timeseriesCollectionsEnabled(testDB.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const nBuckets = 40;

const timeFieldName = "time";
const metaFieldName = "m";

let assertUniqueDocuments = function(docs) {
    let seen = new Set();
    docs.forEach(doc => {
        assert.eq(seen.has(doc._id), false);
        seen.add(doc._id);
    });
};

let assertPlanForSample = (explainRes, backupPlanSelected) => {
    // The trial stage should always appear in the output, regardless of which plan won.
    assert(aggPlanHasStage(explainRes, "TRIAL"), explainRes);

    if (backupPlanSelected) {
        assert(aggPlanHasStage(explainRes, "UNPACK_BUCKET"), explainRes);
        assert(!aggPlanHasStage(explainRes, "$_internalUnpackBucket"));
        assert(aggPlanHasStage(explainRes, "$sample"));

        // Verify that execution stats are reported correctly for the UNPACK_BUCKET stage in
        // explain.
        const unpackBucketStage = getAggPlanStage(explainRes, "UNPACK_BUCKET");
        assert.neq(unpackBucketStage, null, explainRes);
        assert(unpackBucketStage.hasOwnProperty("nBucketsUnpacked"));
        // In the top-k plan, all of the buckets need to be unpacked.
        assert.eq(unpackBucketStage.nBucketsUnpacked, nBuckets, unpackBucketStage);
    } else {
        // When the trial plan succeeds, any data produced during the trial period will be queued
        // and returned via the QUEUED_DATA stage. If the trial plan being assessed reached EOF,
        // then we expect only a QUEUED_DATA stage to appear in explain because all of the necessary
        // data has already been produced. If the plan is not EOF, then we expect OR
        // (QUEUED_DATA, <trial plan>). Either way, the presence of the QUEUED_DATA stage indicates
        // that the trial plan was selected over the backup plan.
        assert(aggPlanHasStage(explainRes, "QUEUED_DATA"), explainRes);
        assert(!aggPlanHasStage(explainRes, "$_internalUnpackBucket"));
        assert(!aggPlanHasStage(explainRes, "$sample"));
    }
};

/**
 * Creates the collection 'coll' as a time-series collection, and inserts data such that there are
 * the given number of measurementsPerBucket (assuming there are 'nBuckets'). Returns the total
 * number of measurement documents inserted into the collection.
 */
function fillBuckets(coll, measurementsPerBucket) {
    assert.commandWorked(testDB.createCollection(
        coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

    const bucketsColl = testDB.getCollection("system.buckets." + coll.getName());
    assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

    let numDocs = nBuckets * measurementsPerBucket;
    const bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < numDocs; i++) {
        bulk.insert(
            {_id: ObjectId(), [timeFieldName]: ISODate(), [metaFieldName]: i % nBuckets, x: i});
    }
    assert.commandWorked(bulk.execute());

    let buckets = bucketsColl.find().toArray();
    assert.eq(nBuckets, buckets.length, buckets);

    return numDocs;
}

let runSampleTests = (measurementsPerBucket, backupPlanSelected) => {
    const coll = testDB.getCollection("timeseries_sample");
    coll.drop();

    let numDocs = fillBuckets(coll, measurementsPerBucket);

    // Check the time-series view to make sure we have the correct number of docs and that there are
    // no duplicates after sampling.
    const viewDocs = coll.find({}, {x: 1}).toArray();
    assert.eq(numDocs, viewDocs.length, viewDocs);

    let sampleSize = 20;
    let result = coll.aggregate([{$sample: {size: sampleSize}}]).toArray();
    assert.eq(sampleSize, result.length, result);
    assertUniqueDocuments(result);

    // Check that we have executed the correct branch of the TrialStage.
    const optimizedSamplePlan =
        coll.explain("executionStats").aggregate([{$sample: {size: sampleSize}}]);
    assertPlanForSample(optimizedSamplePlan, backupPlanSelected);

    // Run an agg pipeline with optimization disabled.
    result = coll.aggregate([{$_internalInhibitOptimization: {}}, {$sample: {size: 1}}]).toArray();
    assert.eq(1, result.length, result);

    // Check that $sample hasn't been absorbed by $_internalUnpackBucket when the
    // sample size is sufficiently large. The server will never try to use random cursor-based
    // sampling for timeseries collections when the requested sample exceeds 1% of the maximum
    // measurement count. Since the maximum number of measurements per bucket is 100, this means
    // that we expect to use a top-k plan (without using 'TrialStage') when the sample size exceeds
    // 'nBuckets'.
    sampleSize = nBuckets + 10;
    const unoptimizedSamplePlan = coll.explain().aggregate([{$sample: {size: sampleSize}}]);
    let bucketStage = getAggPlanStage(unoptimizedSamplePlan, "$_internalUnpackBucket");
    assert.neq(bucketStage, null, unoptimizedSamplePlan);
    assert.eq(bucketStage["$_internalUnpackBucket"]["sample"], undefined);
    assert(aggPlanHasStage(unoptimizedSamplePlan, "$sample"));
    assert(!aggPlanHasStage(unoptimizedSamplePlan, "TRIAL"));

    const unoptimizedResult = coll.aggregate([{$sample: {size: sampleSize}}]).toArray();
    assert.eq(Math.min(sampleSize, numDocs), unoptimizedResult.length, unoptimizedResult);
    assertUniqueDocuments(unoptimizedResult);

    // Check that a sampleSize greater than the number of measurements doesn't cause an infinte
    // loop.
    result = coll.aggregate([{$sample: {size: numDocs + 1}}]).toArray();
    assert.eq(numDocs, result.length, result);

    // Check that $lookup against a time-series collection doesn't cache inner pipeline results if
    // it contains a $sample stage.
    result =
        coll.aggregate(
                {$lookup: {from: coll.getName(), as: "docs", pipeline: [{$sample: {size: 1}}]}})
            .toArray();

    // Each subquery should be an independent sample by checking that we didn't sample the same
    // document repeatedly. It's sufficient for now to make sure that the seen set contains at least
    // two distinct samples.
    let seen = new Set();
    result.forEach(r => {
        assert.eq(r.docs.length, 1);
        seen.add(r.docs[0]._id);
    });
    assert.gte(seen.size, 2);
};

// Test the case where the buckets are only 1% full. Due to the mostly empty buckets, we expect to
// fall back to the non-optimized top-k algorithm for sampling from a time-series collection.
runSampleTests(1, true);

// Test the case where the buckets are 95% full. Here we expect the optimized
// SAMPLE_FROM_TIMESERIES_BUCKET plan to be used.
runSampleTests(95, false);

// Restart the mongod in order to raise the maximum bucket size to 1000.
MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({setParameter: {timeseriesBucketMaxCount: 1000}});
testDB = conn.getDB(dbName);
const coll = testDB.getCollection("timeseries_sample");

// Create a timeseries collection that has 40 buckets, each with 900 documents.
const measurementsPerBucket = 900;
let numDocs = fillBuckets(coll, measurementsPerBucket);
assert.eq(numDocs, measurementsPerBucket * nBuckets);

// Run a sample query where the sample size is large enough to merit multiple batches.
assert.eq(150, coll.aggregate([{$sample: {size: 150}}]).itcount());

// Explain the $sample. Given that the buckets are mostly full, we expect the trial to succeed. We
// should see a TRIAL stage, and it should have selected the SAMPLE_FROM_TIMESERIES_BUCKET plan. The
// initial batch of data collected during the trial period will be returned via a QUEUED_DATA_STAGE.
const explainRes = coll.explain("executionStats").aggregate([{$sample: {size: 150}}]);
const trialStage = getAggPlanStage(explainRes, "TRIAL");
assert.neq(trialStage, null, explainRes);
const orStage = getPlanStage(trialStage, "OR");
assert.neq(orStage, null, explainRes);
const queuedDataStage = getPlanStage(orStage, "QUEUED_DATA");
assert.neq(queuedDataStage, null, explainRes);

// Verify that the SAMPLE_FROM_TIMESERIES_BUCKET stage exists in the plan and has reasonable
// runtime stats.
const sampleFromBucketStage = getPlanStage(orStage, "SAMPLE_FROM_TIMESERIES_BUCKET");
assert.neq(sampleFromBucketStage, null, explainRes);
assert(sampleFromBucketStage.hasOwnProperty("nBucketsDiscarded"), sampleFromBucketStage);
assert.gte(sampleFromBucketStage.nBucketsDiscarded, 0, sampleFromBucketStage);
assert(sampleFromBucketStage.hasOwnProperty("dupsDropped"), sampleFromBucketStage);
assert.gte(sampleFromBucketStage.dupsDropped, 0, sampleFromBucketStage);
// Since we are returning a sample size of 150, we expect to test at least that many dups.
assert(sampleFromBucketStage.hasOwnProperty("dupsTested"));
assert.gte(sampleFromBucketStage.dupsTested, 150, sampleFromBucketStage);

// The SAMPLE_FROM_TIMESERIES_BUCKET stage reads from a MULTI_ITERATOR stage, which in turn reads
// from a storage-provided random cursor.
const multiIteratorStage = getPlanStage(sampleFromBucketStage, "MULTI_ITERATOR");
assert.neq(multiIteratorStage, null, explainRes);

MongoRunner.stopMongod(conn);
})();

/**
 * Verifies that a direct $sample stage on system.buckets collection works.
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const conn = MongoRunner.runMongod();

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

if (!TimeseriesTest.timeseriesCollectionsEnabled(testDB.getMongo())) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

// Prepares a timeseries collection.
assert.commandWorked(
    testDB.createCollection("t", {timeseries: {timeField: "time", metaField: "meta"}}));
assert.commandWorked(
    testDB.t.insert([{time: ISODate(), meta: 1, a: 1}, {time: ISODate(), meta: 1, a: 2}]));

// Verifies that a direct $sample stage on system.buckets collection works.
const kNoOfSamples = 1;
const res = testDB.system.buckets.t.aggregate([{$sample: {size: kNoOfSamples}}]).toArray();
assert.eq(res.length, kNoOfSamples);

MongoRunner.stopMongod(conn);
})();

/*
 * Tests time-series server parameter settings on server startup.

 * @tags: [
 *   requires_replication
 * ]
 */

(function() {
'use strict';

load("jstests/core/timeseries/libs/timeseries.js");
load("jstests/noPassthrough/libs/server_parameter_helpers.js");

// Valid parameter values are in the range [0, infinity).
testNumericServerParameter('timeseriesBucketMaxCount',
                           true /*isStartupParameter*/,
                           false /*isRuntimeParameter*/,
                           1000 /*defaultValue*/,
                           100 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           0 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);

// Valid parameter values are in the range [0, infinity).
testNumericServerParameter('timeseriesBucketMaxSize',
                           true /*isStartupParameter*/,
                           false /*isRuntimeParameter*/,
                           1024 * 125 /*defaultValue*/,
                           1024 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           0 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);

// Valid parameter values are in the range [0, infinity).
testNumericServerParameter('timeseriesIdleBucketExpiryMemoryUsageThreshold',
                           true /*isStartupParameter*/,
                           false /*isRuntimeParameter*/,
                           1024 * 1024 * 100 /*defaultValue*/,
                           1024 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           0 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);
})();

/**
 * Tests that serverStatus contains a bucketCatalog section.
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

const conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
assert.commandWorked(testDB.dropDatabase());

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';
const metaFieldName = 'meta';

assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

const expectedMetrics = {
    numBuckets: 0,
    numOpenBuckets: 0,
    numIdleBuckets: 0,
};

const checkServerStatus = function() {
    const metrics = assert.commandWorked(testDB.serverStatus()).bucketCatalog;

    const invalidMetricMsg = function(metric) {
        return "Invalid '" + metric + "' value in serverStatus: " + tojson(metrics);
    };

    for (let [metric, value] of Object.entries(expectedMetrics)) {
        assert.eq(metrics[metric], value, invalidMetricMsg(metric));
    }

    assert.gt(metrics.memoryUsage, 0, invalidMetricMsg('memoryUsage'));
};

const checkNoServerStatus = function() {
    const serverStatus = assert.commandWorked(testDB.serverStatus());
    assert(!serverStatus.hasOwnProperty('bucketCatalog'),
           'Found unexpected bucketCatalog section in serverStatus: ' +
               tojson(serverStatus.bucketCatalog));
};

const testWithInsertPaused = function(docs) {
    const fp = configureFailPoint(conn, "hangTimeseriesInsertBeforeCommit");

    const awaitInsert = startParallelShell(
        funWithArgs(function(dbName, collName, docs) {
            assert.commandWorked(
                db.getSiblingDB(dbName).getCollection(collName).insert(docs, {ordered: false}));
        }, dbName, coll.getName(), docs), conn.port);

    fp.wait();
    checkServerStatus();
    fp.off();

    awaitInsert();
};

checkNoServerStatus();

// Inserting the first measurement will open a new bucket.
expectedMetrics.numBuckets++;
expectedMetrics.numOpenBuckets++;
testWithInsertPaused({[timeFieldName]: ISODate(), [metaFieldName]: {a: 1}});

// Once the insert is complete, the bucket becomes idle.
expectedMetrics.numIdleBuckets++;
checkServerStatus();

// Insert two measurements: one which will go into the existing bucket and a second which will close
// that existing bucket. Thus, until the measurements are committed, the number of buckets is
// than the number of open buckets.
expectedMetrics.numBuckets++;
expectedMetrics.numIdleBuckets--;
testWithInsertPaused([
    {[timeFieldName]: ISODate(), [metaFieldName]: {a: 1}},
    {[timeFieldName]: ISODate("2021-01-02T01:00:00Z"), [metaFieldName]: {a: 1}}
]);

// Once the insert is complete, the closed bucket goes away and the open bucket becomes idle.
expectedMetrics.numBuckets--;
expectedMetrics.numIdleBuckets++;
checkServerStatus();

// Insert a measurement which will close the existing bucket right away.
expectedMetrics.numIdleBuckets--;
testWithInsertPaused({[timeFieldName]: ISODate("2021-01-01T01:00:00Z"), [metaFieldName]: {a: 1}});

// Once the insert is complete, the new bucket becomes idle.
expectedMetrics.numIdleBuckets++;
checkServerStatus();

assert(coll.drop());
checkNoServerStatus();

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that the server can startup with time-series collections present.
 *
 * @tags: [
 *   requires_persistence,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

let conn = MongoRunner.runMongod();

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);
const coll = testDB.getCollection('t');

const timeFieldName = 'time';
const metaFieldName = 'meta';

assert.commandWorked(testDB.createCollection(
    coll.getName(), {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

MongoRunner.stopMongod(conn);

// Restarting the server with a time-series collection present should startup, even though
// time-series collections do not have an _id index.
conn = MongoRunner.runMongod({dbpath: conn.dbpath, noCleanData: true});
assert(conn);
MongoRunner.stopMongod(conn);
})();

/**
 * Tests the behavior of TTL _id on time-series collections. Ensures that data is only expired when
 * it is guaranteed to be past the maximum time range of a bucket.
 *
 * @tags: [
 *   assumes_no_implicit_collection_creation_after_drop,
 *   does_not_support_stepdowns,
 *   requires_getmore,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

// Run TTL monitor constantly to speed up this test.
const conn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});

if (!TimeseriesTest.timeseriesCollectionsEnabled(conn)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    MongoRunner.stopMongod(conn);
    return;
}

const dbName = jsTestName();
const testDB = conn.getDB(dbName);

const timeFieldName = 'time';
const metaFieldName = 'host';
const expireAfterSeconds = 5;
// Default maximum range of time for a bucket.
const defaultBucketMaxRange = 3600;

const waitForTTL = () => {
    // The 'ttl.passes' metric is incremented when the TTL monitor starts processing the indexes, so
    // we wait for it to be incremented twice to know that the TTL monitor finished processing the
    // indexes at least once.
    const ttlPasses = testDB.serverStatus().metrics.ttl.passes;
    assert.soon(function() {
        return testDB.serverStatus().metrics.ttl.passes > ttlPasses + 1;
    });
};

const testCase = (testFn) => {
    const coll = testDB.getCollection('ts');
    const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
    assert.commandWorked(testDB.createCollection(coll.getName(), {
        timeseries: {
            timeField: timeFieldName,
            metaField: metaFieldName,
        },
        expireAfterSeconds: expireAfterSeconds,
    }));

    testFn(coll, bucketsColl);
    assert(coll.drop());
};

testCase((coll, bucketsColl) => {
    // Insert two measurements that end up in the same bucket, but where the minimum is 5 minutes
    // earlier. Expect that the TTL monitor does not delete the data even though the bucket minimum
    // is past the expiry.
    const maxTime = new Date();
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insert({[timeFieldName]: minTime, [metaFieldName]: "localhost"}));
    assert.commandWorked(coll.insert({[timeFieldName]: maxTime, [metaFieldName]: "localhost"}));
    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());

    waitForTTL();
    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());
});

testCase((coll, bucketsColl) => {
    // Insert two measurements 5 minutes apart that end up in the same bucket and both are older
    // than the TTL expiry. Expect that the TTL monitor does not delete the data even though the
    // bucket minimum is past the expiry because it is waiting for the maximum bucket range to
    // elapse.
    const maxTime = new Date((new Date()).getTime() - (1000 * expireAfterSeconds));
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insert({[timeFieldName]: minTime, [metaFieldName]: "localhost"}));
    assert.commandWorked(coll.insert({[timeFieldName]: maxTime, [metaFieldName]: "localhost"}));
    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());

    waitForTTL();
    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());
});

testCase((coll, bucketsColl) => {
    // Insert two measurements 5 minutes apart that end up in the same bucket and both are older
    // than the TTL expiry and the maximum bucket range. Expect that the TTL monitor deletes the
    // data because the bucket minimum is past the expiry plus the maximum bucket range.
    const maxTime = new Date((new Date()).getTime() - (1000 * defaultBucketMaxRange));
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insert({[timeFieldName]: minTime, [metaFieldName]: "localhost"}));
    assert.commandWorked(coll.insert({[timeFieldName]: maxTime, [metaFieldName]: "localhost"}));

    waitForTTL();
    assert.eq(0, coll.find().itcount());
    assert.eq(0, bucketsColl.find().itcount());
});

testCase((coll, bucketsColl) => {
    // Insert two measurements using insertMany that end up in the same bucket, but where the
    // minimum is 5 minutes earlier. Expect that the TTL monitor does not delete the data even
    // though the bucket minimum is past the expiry.
    const maxTime = new Date();
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insertMany([
        {[timeFieldName]: minTime, [metaFieldName]: "localhost"},
        {[timeFieldName]: maxTime, [metaFieldName]: "localhost"}
    ]));

    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());

    waitForTTL();
    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());
});

testCase((coll, bucketsColl) => {
    // Insert two measurements with insertMany 5 minutes apart that end up in the same bucket and
    // both are older than the TTL expiry and the maximum bucket range. Expect that the TTL monitor
    // deletes the data because the bucket minimum is past the expiry plus the maximum bucket range.
    const maxTime = new Date((new Date()).getTime() - (1000 * defaultBucketMaxRange));
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insertMany([
        {[timeFieldName]: minTime, [metaFieldName]: "localhost"},
        {[timeFieldName]: maxTime, [metaFieldName]: "localhost"}
    ]));

    waitForTTL();
    assert.eq(0, coll.find().itcount());
    assert.eq(0, bucketsColl.find().itcount());
});

// Make a collection TTL using collMod. Ensure data expires correctly.
(function newlyTTLWithCollMod() {
    const coll = testDB.getCollection('ts');
    const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());
    assert.commandWorked(testDB.createCollection(coll.getName(), {
        timeseries: {
            timeField: timeFieldName,
            metaField: metaFieldName,
        }
    }));

    // Insert two measurements 5 minutes apart that end up in the same bucket and both are older
    // than the TTL expiry and the maximum bucket range.
    const maxTime = new Date((new Date()).getTime() - (1000 * defaultBucketMaxRange));
    const minTime = new Date(maxTime.getTime() - (1000 * 5 * 60));
    assert.commandWorked(coll.insert({[timeFieldName]: minTime, [metaFieldName]: "localhost"}));
    assert.commandWorked(coll.insert({[timeFieldName]: maxTime, [metaFieldName]: "localhost"}));

    assert.eq(2, coll.find().itcount());
    assert.eq(1, bucketsColl.find().itcount());

    // Make the collection TTL and expect the data to be deleted because the bucket minimum is past
    // the expiry plus the maximum bucket range.
    assert.commandWorked(testDB.runCommand({
        collMod: 'system.buckets.ts',
        expireAfterSeconds: expireAfterSeconds,
    }));

    waitForTTL();
    assert.eq(0, coll.find().itcount());
    assert.eq(0, bucketsColl.find().itcount());
})();

MongoRunner.stopMongod(conn);
})();

/**
 * Tests that deleting and updating time-series collections from a multi-document transaction is
 * disallowed.
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
"use strict";

load("jstests/core/timeseries/libs/timeseries.js");

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const testDB = rst.getPrimary().getDB(jsTestName());

if (!TimeseriesTest.timeseriesUpdatesAndDeletesEnabled(testDB.getMongo())) {
    jsTestLog("Skipping test because the time-series updates and deletes feature flag is disabled");
    rst.stopSet();
    return;
}

const metaFieldName = "meta";
const timeFieldName = "time";
const collectionName = "t";

assert.commandWorked(testDB.dropDatabase());
assert.commandWorked(
    testDB.createCollection(testDB.getCollection(collectionName).getName(),
                            {timeseries: {timeField: timeFieldName, metaField: metaFieldName}}));

const session = testDB.getMongo().startSession();
const sessionColl = session.getDatabase(jsTestName()).getCollection(collectionName);
session.startTransaction();
// Time-series delete in a multi-document transaction should fail.
assert.commandFailedWithCode(sessionColl.remove({[metaFieldName]: "a"}),
                             ErrorCodes.OperationNotSupportedInTransaction);
assert.commandFailedWithCode(session.commitTransaction_forTesting(), ErrorCodes.NoSuchTransaction);

session.startTransaction();
// Time-series update in a multi-document transaction should fail.
assert.commandFailedWithCode(sessionColl.update({[metaFieldName]: "a"}, {"$set": {"b": "a"}}),
                             ErrorCodes.OperationNotSupportedInTransaction);
assert.commandFailedWithCode(session.commitTransaction_forTesting(), ErrorCodes.NoSuchTransaction);
session.endSession();
rst.stopSet();
})();

/**
 * Tests that different write concerns are respected for time-series inserts, even if they are in
 * the same bucket.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
'use strict';

load('jstests/core/timeseries/libs/timeseries.js');
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/write_concern_util.js");

const replTest = new ReplSetTest({nodes: 2});
replTest.startSet();
replTest.initiate();

const primary = replTest.getPrimary();

if (!TimeseriesTest.timeseriesCollectionsEnabled(primary)) {
    jsTestLog('Skipping test because the time-series collection feature flag is disabled');
    replTest.stopSet();
    return;
}

const dbName = jsTestName();
const testDB = primary.getDB(dbName);

const coll = testDB.getCollection('t');
const bucketsColl = testDB.getCollection('system.buckets.' + coll.getName());

const timeFieldName = 'time';

coll.drop();
assert.commandWorked(
    testDB.createCollection(coll.getName(), {timeseries: {timeField: timeFieldName}}));
assert.contains(bucketsColl.getName(), testDB.getCollectionNames());

stopReplicationOnSecondaries(replTest);

const docs = [
    {_id: 0, [timeFieldName]: ISODate()},
    {_id: 1, [timeFieldName]: ISODate()},
];

const awaitInsert = startParallelShell(funWithArgs(function(dbName, collName, doc) {
                                           assert.commandWorked(db.getSiblingDB(dbName).runCommand({
                                               insert: collName,
                                               documents: [doc],
                                               ordered: false,
                                               writeConcern: {w: 2},
                                               comment: '{w: 2} insert',
                                           }));
                                       }, dbName, coll.getName(), docs[0]), primary.port);

// Wait for the {w: 2} insert to open a bucket.
assert.soon(() => {
    const serverStatus = assert.commandWorked(testDB.serverStatus());
    return serverStatus.hasOwnProperty('bucketCatalog') &&
        serverStatus.bucketCatalog.numOpenBuckets === 1;
});

// A {w: 1} insert should still be able to complete despite going into the same bucket as the {w: 2}
// insert, which is still outstanding.
assert.commandWorked(coll.insert(docs[1], {writeConcern: {w: 1}, ordered: false}));

// Ensure the {w: 2} insert has not yet completed.
assert.eq(
    assert.commandWorked(testDB.currentOp())
        .inprog.filter(op => op.ns === coll.getFullName() && op.command.comment === '{w: 2} insert')
        .length,
    1);

restartReplicationOnSecondaries(replTest);
awaitInsert();

assert.docEq(coll.find().toArray(), docs);
const buckets = bucketsColl.find().toArray();
assert.eq(buckets.length, 1, 'Expected one bucket but found: ' + tojson(buckets));
const serverStatus = assert.commandWorked(testDB.serverStatus()).bucketCatalog;
assert.eq(serverStatus.numOpenBuckets, 1, 'Expected one bucket but found: ' + tojson(serverStatus));

replTest.stopSet();
})();
/**
 * This test exercises a few important properties of timestamping index builds. First, it ensures
 * background index builds on primaries and secondaries timestamp both `init` and `commit`
 * operations on the catalog.
 *
 * Second, this test ensures the entire index build is ahead of the majority point. Thus when
 * restarting as a standalone, those indexes are not observed. When restarted with `--replSet`,
 * the indexes will be rebuilt. Currently performing a background index build at startup before
 * the logical clock is initialized will fail to timestamp index completion. The code currently
 * fixes this by foregrounding those index builds. We can observe the correctness of this behavior
 * by once again restarting the node as a standalone and not seeing any evidence of the second
 * index.
 *
 * This test does not guarantee that background index builds are foregrounded to correct
 * timestamping, merely that the catalog state is not corrupted due to the existence of background
 * index builds.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
    name: "timestampingIndexBuilds",
    nodes: 2,
    nodeOptions: {setParameter: {logComponentVerbosity: tojsononeline({storage: {recovery: 2}})}}
});
const nodes = rst.startSet();
rst.initiateWithHighElectionTimeout();

if (!rst.getPrimary().adminCommand("serverStatus").storageEngine.supportsSnapshotReadConcern) {
    // Only snapshotting storage engines require correct timestamping of index builds.
    rst.stopSet();
    return;
}

// The default WC is majority and disableSnapshotting failpoint will prevent satisfying any majority
// writes.
assert.commandWorked(rst.getPrimary().adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

function getColl(conn) {
    return conn.getDB("timestampingIndexBuild")["coll"];
}

let coll = getColl(rst.getPrimary());

// Create a collection and wait for the stable timestamp to exceed its creation on both nodes.
assert.commandWorked(
    coll.insert({}, {writeConcern: {w: "majority", wtimeout: rst.kDefaultTimeoutMS}}));

// Wait for the stable timestamp to match the latest oplog entry on both nodes.
rst.awaitLastOpCommitted();

// Disable snapshotting on all members of the replica set so that further operations do not
// enter the majority snapshot.
nodes.forEach(node => assert.commandWorked(node.adminCommand(
                  {configureFailPoint: "disableSnapshotting", mode: "alwaysOn"})));

// This test create indexes with majority of nodes not available for replication. So, disabling
// index build commit quorum.
assert.commandWorked(coll.createIndexes([{foo: 1}], {background: true}, 0));
rst.awaitReplication();

rst.stopSet(undefined, true);

// The `disableSnapshotting` failpoint is no longer in effect. Bring up and analyze each node
// separately. The client does not need to perform any writes from here on out.
for (let nodeIdx = 0; nodeIdx < 2; ++nodeIdx) {
    let node = nodes[nodeIdx];
    let nodeIdentity = tojsononeline({nodeIdx: nodeIdx, dbpath: node.dbpath, port: node.port});

    // Bringing up the node as a standalone should only find the `_id` index.
    {
        jsTestLog("Starting as a standalone. Ensure only the `_id` index exists. Node: " +
                  nodeIdentity);
        let conn = rst.start(nodeIdx, {noReplSet: true, noCleanData: true});
        assert.neq(null, conn, "failed to restart node");
        IndexBuildTest.assertIndexes(getColl(conn), 1, ['_id_']);
        rst.stop(nodeIdx);
    }

    // Bringing up the node with `--replSet` will run oplog recovery. The `foo` index will be
    // rebuilt, but not become "stable".
    {
        jsTestLog("Starting as a replica set. Both indexes should exist. Node: " + nodeIdentity);
        let conn = rst.start(nodeIdx, {startClean: false}, true);
        rst.waitForState(conn, ReplSetTest.State.SECONDARY);
        conn.setSecondaryOk();
        IndexBuildTest.assertIndexes(getColl(conn), 2, ['_id_', 'foo_1']);
        rst.stop(nodeIdx);
    }

    // Restarting the node as a standalone once again only shows the `_id` index.
    {
        jsTestLog(
            "Starting as a standalone after replication startup recovery. Ensure only the `_id` index exists. Node: " +
            nodeIdentity);
        let conn = rst.start(nodeIdx, {noReplSet: true, noCleanData: true});
        assert.neq(null, conn, "failed to restart node");
        IndexBuildTest.assertIndexes(getColl(conn), 1, ['_id_']);
        rst.stop(nodeIdx);
    }
}

rst.stopSet();
}());

// tests for the traffic_recording commands.
(function() {
// Variables for this test
const recordingDir = MongoRunner.toRealDir("$dataDir/traffic_recording/");
const recordingFile = "recording.txt";
const recordingFilePath = MongoRunner.toRealDir(recordingDir + "/" + recordingFile);
const replayFilePath = MongoRunner.toRealDir(recordingDir + "/replay.txt");

assert.throws(function() {
    convertTrafficRecordingToBSON("notarealfileatall");
});

// Create the recording directory if it does not already exist
mkdir(recordingDir);

// Create the options and run mongod
var opts = {auth: "", setParameter: "trafficRecordingDirectory=" + recordingDir};
m = MongoRunner.runMongod(opts);

// Get the port of the host
var serverPort = m.port;

// Create necessary users
adminDB = m.getDB("admin");
const testDB = m.getDB("test");
const coll = testDB.getCollection("foo");
adminDB.createUser({user: "admin", pwd: "pass", roles: jsTest.adminUserRoles});
adminDB.auth("admin", "pass");

// Start recording traffic
assert.commandWorked(adminDB.runCommand({'startRecordingTraffic': 1, 'filename': 'recording.txt'}));

// Run a few commands
assert.commandWorked(testDB.runCommand({"serverStatus": 1}));
assert.commandWorked(coll.insert({"name": "foo biz bar"}));
assert.eq("foo biz bar", coll.findOne().name);
assert.commandWorked(coll.insert({"name": "foo bar"}));
assert.eq("foo bar", coll.findOne({"name": "foo bar"}).name);
assert.commandWorked(coll.deleteOne({}));
assert.eq(1, coll.aggregate().toArray().length);
assert.commandWorked(coll.update({}, {}));

// Stop recording traffic
assert.commandWorked(testDB.runCommand({'stopRecordingTraffic': 1}));

// Shutdown Mongod
MongoRunner.stopMongod(m, null, {user: 'admin', pwd: 'password'});

// Counters
var numRequest = 0;
var numResponse = 0;
var opTypes = {};

// Pass filepath to traffic_reader helper method to get recorded info in BSON
var res = convertTrafficRecordingToBSON(recordingFilePath);

// Iterate through the results and assert the above commands are properly recorded
res.forEach((obj) => {
    assert.eq(obj["rawop"]["header"]["opcode"], 2013);
    assert.eq(obj["seenconnectionnum"], 1);
    var responseTo = obj["rawop"]["header"]["responseto"];
    if (responseTo == 0) {
        assert.eq(obj["destendpoint"], serverPort.toString());
        numRequest++;
    } else {
        assert.eq(obj["srcendpoint"], serverPort.toString());
        numResponse++;
    }
    opTypes[obj["opType"]] = (opTypes[obj["opType"]] || 0) + 1;
});

// Assert there is a response for every request
assert.eq(numResponse, numRequest);

// Assert the opTypes were correct
assert.eq(opTypes['isMaster'], opTypes["ismaster"]);
assert.eq(opTypes['find'], 2);
assert.eq(opTypes['insert'], 2);
assert.eq(opTypes['delete'], 1);
assert.eq(opTypes['update'], 1);
assert.eq(opTypes['aggregate'], 1);
assert.eq(opTypes['stopRecordingTraffic'], 1);
})();

// tests for the traffic_recording commands.
(function() {
function getDB(client) {
    let db = client.getDB("admin");
    db.auth("admin", "pass");

    return db;
}

function runTest(client, restartCommand) {
    let db = getDB(client);

    let res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording directory not set");

    const path = MongoRunner.toRealDir("$dataDir/traffic_recording/");
    mkdir(path);

    if (!jsTest.isMongos(client)) {
        TestData.enableTestCommands = false;
        client = restartCommand({
            trafficRecordingDirectory: path,
            AlwaysRecordTraffic: "notARealPath",
            enableTestCommands: 0,
        });
        TestData.enableTestCommands = true;
        assert.eq(null, client, "AlwaysRecordTraffic and not enableTestCommands should fail");
    }

    client = restartCommand({
        trafficRecordingDirectory: path,
        AlwaysRecordTraffic: "notARealPath",
        enableTestCommands: 1
    });
    assert.neq(null, client, "AlwaysRecordTraffic and with enableTestCommands should suceed");
    db = getDB(client);

    assert(db.runCommand({"serverStatus": 1}).trafficRecording.running);

    client = restartCommand({trafficRecordingDirectory: path});
    db = getDB(client);

    res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, true);

    // Running the command again should fail
    res = db.runCommand({'startRecordingTraffic': 1, 'filename': 'notARealPath'});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording already active");

    // Running the serverStatus command should return the relevant information
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    let trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], true);

    // Assert that the current file size is growing
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    let trafficStats2 = res["trafficRecording"];
    assert.eq(trafficStats2["running"], true);
    assert(trafficStats2["currentFileSize"] >= trafficStats["currentFileSize"]);

    // Running the stopRecordingTraffic command should succeed
    res = db.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, true);

    // Running the stopRecordingTraffic command again should fail
    res = db.runCommand({'stopRecordingTraffic': 1});
    assert.eq(res.ok, false);
    assert.eq(res["errmsg"], "Traffic recording not active");

    // Running the serverStatus command should return running is false
    res = db.runCommand({"serverStatus": 1});
    assert("trafficRecording" in res);
    trafficStats = res["trafficRecording"];
    assert.eq(trafficStats["running"], false);

    return client;
}

{
    let m = MongoRunner.runMongod({auth: ""});

    let db = m.getDB("admin");

    db.createUser({user: "admin", pwd: "pass", roles: jsTest.adminUserRoles});
    db.auth("admin", "pass");

    m = runTest(m, function(setParams) {
        if (m) {
            MongoRunner.stopMongod(m, null, {user: 'admin', pwd: 'pass'});
        }
        try {
            m = MongoRunner.runMongod({auth: "", setParameter: setParams});
        } catch (e) {
            return null;
        }

        m.getDB("admin").createUser({user: "admin", pwd: "pass", roles: jsTest.adminUserRoles});

        return m;
    });

    MongoRunner.stopMongod(m, null, {user: 'admin', pwd: 'pass'});
}

{
    let shardTest = new ShardingTest({
        config: 1,
        mongos: 1,
        shards: 0,
    });

    runTest(shardTest.s, function(setParams) {
        shardTest.restartMongos(0, {
            restart: true,
            setParameter: setParams,
        });

        return shardTest.s;
    });

    shardTest.stop();
}
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

function commitTxn(st, lsid, txnNumber, expectedError = null) {
    let cmd = "db.adminCommand({" +
        "commitTransaction: 1," +
        "lsid: " + tojson(lsid) + "," +
        "txnNumber: NumberLong(" + txnNumber + ")," +
        "stmtId: NumberInt(0)," +
        "autocommit: false," +
        "})";
    if (expectedError) {
        cmd = "assert.commandFailedWithCode(" + cmd + "," + String(expectedError) + ");";
    } else {
        cmd = "assert.commandWorked(" + cmd + ");";
    }
    return startParallelShell(cmd, st.s.port);
}

function curOpAfterFailpoint(failPoint, filter, timesEntered = 1) {
    jsTest.log(`waiting for failpoint '${failPoint.failPointName}' to be entered ${
        timesEntered} time(s).`);
    if (timesEntered > 1) {
        const expectedLog = "Hit " + failPoint.failPointName + " failpoint";
        waitForFailpoint(expectedLog, timesEntered);
    } else {
        failPoint.wait();
    }

    jsTest.log(`Running curOp operation after '${failPoint.failPointName}' failpoint.`);
    let result =
        adminDB.aggregate([{$currentOp: {'idleConnections': true}}, {$match: filter}]).toArray();

    jsTest.log(`${result.length} matching curOp entries after '${failPoint.failPointName}':\n${
        tojson(result)}`);

    jsTest.log(`disable '${failPoint.failPointName}' failpoint.`);
    failPoint.off();

    return result;
}

function makeWorkerFilterWithAction(session, action, txnNumber) {
    return {
        'twoPhaseCommitCoordinator.lsid.id': session.getSessionId().id,
        'twoPhaseCommitCoordinator.txnNumber': NumberLong(txnNumber),
        'twoPhaseCommitCoordinator.action': action,
        'twoPhaseCommitCoordinator.startTime': {$exists: true}
    };
}

function enableFailPoints(shard, failPointNames) {
    let failPoints = {};

    jsTest.log(`enabling the following failpoints: ${tojson(failPointNames)}`);
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
}

// Setup test
const numShards = 2;
const st = new ShardingTest({shards: numShards, config: 1});
const dbName = "test";
const collectionName = 'currentop_two_phase';
const ns = dbName + "." + collectionName;
const adminDB = st.s.getDB('admin');
const coordinator = st.shard0;
const participant = st.shard1;
const failPointNames = [
    'hangAfterStartingCoordinateCommit',
    'hangBeforeWritingParticipantList',
    'hangBeforeSendingPrepare',
    'hangBeforeWritingDecision',
    'hangBeforeSendingCommit',
    'hangBeforeDeletingCoordinatorDoc',
    'hangBeforeSendingAbort'
];

assert.commandWorked(st.s.adminCommand({enableSharding: dbName}));
assert.commandWorked(st.s.adminCommand({movePrimary: dbName, to: coordinator.shardName}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns, key: {_id: 1}}));
assert.commandWorked(st.s.adminCommand({split: ns, middle: {_id: 0}}));
assert.commandWorked(st.s.adminCommand({moveChunk: ns, find: {_id: 0}, to: participant.shardName}));
assert.commandWorked(coordinator.adminCommand({_flushRoutingTableCacheUpdates: ns}));
assert.commandWorked(participant.adminCommand({_flushRoutingTableCacheUpdates: ns}));
st.refreshCatalogCacheForNs(st.s, ns);

let failPoints = enableFailPoints(coordinator, failPointNames);

jsTest.log("Testing that coordinator threads show up in currentOp for a commit decision");
{
    let session = adminDB.getMongo().startSession();
    startTransaction(session, collectionName, 1);
    let txnNumber = session.getTxnNumber_forTesting();
    let lsid = session.getSessionId();
    let commitJoin = commitTxn(st, lsid, txnNumber);

    const coordinateCommitFilter = {
        active: true,
        'command.coordinateCommitTransaction': 1,
        'command.lsid.id': session.getSessionId().id,
        'command.txnNumber': NumberLong(txnNumber),
        'command.coordinator': true,
        'command.autocommit': false
    };
    let createCoordinateCommitTxnOp = curOpAfterFailpoint(
        failPoints["hangAfterStartingCoordinateCommit"], coordinateCommitFilter);
    assert.eq(1, createCoordinateCommitTxnOp.length);

    const writeParticipantFilter =
        makeWorkerFilterWithAction(session, "writingParticipantList", txnNumber);
    let writeParticipantOp =
        curOpAfterFailpoint(failPoints['hangBeforeWritingParticipantList'], writeParticipantFilter);
    assert.eq(1, writeParticipantOp.length);

    const sendPrepareFilter = makeWorkerFilterWithAction(session, "sendingPrepare", txnNumber);
    let sendPrepareOp =
        curOpAfterFailpoint(failPoints['hangBeforeSendingPrepare'], sendPrepareFilter, numShards);
    assert.eq(numShards, sendPrepareOp.length);

    const writingDecisionFilter = makeWorkerFilterWithAction(session, "writingDecision", txnNumber);
    let writeDecisionOp =
        curOpAfterFailpoint(failPoints['hangBeforeWritingDecision'], writingDecisionFilter);
    assert.eq(1, writeDecisionOp.length);

    const sendCommitFilter = makeWorkerFilterWithAction(session, "sendingCommit", txnNumber);
    let sendCommitOp =
        curOpAfterFailpoint(failPoints['hangBeforeSendingCommit'], sendCommitFilter, numShards);
    assert.eq(numShards, sendCommitOp.length);

    const deletingCoordinatorFilter =
        makeWorkerFilterWithAction(session, "deletingCoordinatorDoc", txnNumber);
    let deletingCoordinatorDocOp = curOpAfterFailpoint(
        failPoints['hangBeforeDeletingCoordinatorDoc'], deletingCoordinatorFilter);
    assert.eq(1, deletingCoordinatorDocOp.length);

    commitJoin();
}

jsTest.log("Testing that coordinator threads show up in currentOp for an abort decision.");
{
    let session = adminDB.getMongo().startSession();
    startTransaction(session, collectionName, 2);
    let txnNumber = session.getTxnNumber_forTesting();
    let lsid = session.getSessionId();
    // Manually abort the transaction on one of the participants, so that the participant fails to
    // prepare and failpoint is triggered on the coordinator.
    assert.commandWorked(participant.adminCommand({
        abortTransaction: 1,
        lsid: lsid,
        txnNumber: NumberLong(txnNumber),
        stmtId: NumberInt(0),
        autocommit: false,
    }));
    let commitJoin = commitTxn(st, lsid, txnNumber, ErrorCodes.NoSuchTransaction);

    const sendAbortFilter = makeWorkerFilterWithAction(session, "sendingAbort", txnNumber);
    let sendingAbortOp =
        curOpAfterFailpoint(failPoints['hangBeforeSendingAbort'], sendAbortFilter, numShards);
    assert.eq(numShards, sendingAbortOp.length);

    commitJoin();
}

st.stop();
})();

// @tags: [
//   requires_replication,
//   requires_sharding,
// ]
(function() {
'use strict';

// This test makes assertions about the number of sessions, which are not compatible with
// implicit sessions.
TestData.disableImplicitSessions = true;

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

function Repl(lifetime) {
    this.rst = new ReplSetTest({
        nodes: 1,
        nodeOptions: {setParameter: {TransactionRecordMinimumLifetimeMinutes: lifetime}},
    });
    this.rst.startSet();
    this.rst.initiate();
}

Repl.prototype.stop = function() {
    this.rst.stopSet();
};

Repl.prototype.getConn = function() {
    return this.rst.getPrimary();
};

Repl.prototype.getTransactionConn = function() {
    return this.rst.getPrimary();
};

function Sharding(lifetime) {
    this.st = new ShardingTest({
        shards: 1,
        mongos: 1,
        config: 1,
        other: {
            rs: true,
            rsOptions: {setParameter: {TransactionRecordMinimumLifetimeMinutes: lifetime}},
            rs0: {nodes: 1},
        },
    });

    this.st.s0.getDB("admin").runCommand({enableSharding: "test"});
    this.st.s0.getDB("admin").runCommand({shardCollection: "test.test", key: {_id: 1}});

    // Ensure that the sessions collection exists.
    assert.commandWorked(this.st.c0.getDB("admin").runCommand({refreshLogicalSessionCacheNow: 1}));
    assert.commandWorked(
        this.st.rs0.getPrimary().getDB("admin").runCommand({refreshLogicalSessionCacheNow: 1}));

    // Remove the session created by the above shardCollection
    this.st.s.getDB("config").system.sessions.remove({});
}

Sharding.prototype.stop = function() {
    this.st.stop();
};

Sharding.prototype.getConn = function() {
    return this.st.s0;
};

Sharding.prototype.getTransactionConn = function() {
    return this.st.rs0.getPrimary();
};

const nSessions = 1500;

function Fixture(impl) {
    this.impl = impl;
    this.conn = impl.getConn();
    this.transactionConn = impl.getTransactionConn();

    this.sessions = [];

    for (var i = 0; i < nSessions; i++) {
        // make a session and get it to the collection
        var session = this.conn.startSession({retryWrites: 1});
        session.getDatabase("test").test.count({});
        this.sessions.push(session);
    }

    this.refresh();
    this.assertOutstandingTransactions(0);
    this.assertOutstandingSessions(nSessions);

    for (var i = 0; i < nSessions; i++) {
        // make a session and get it to the collection
        var session = this.sessions[i];
        assert.commandWorked(session.getDatabase("test").test.save({a: 1}));
    }

    // Ensure a write flushes a transaction
    this.assertOutstandingTransactions(nSessions);
    this.assertOutstandingSessions(nSessions);

    // Ensure a refresh/reap doesn't remove the transaction
    this.refresh();
    this.reap();
    this.assertOutstandingTransactions(nSessions);
    this.assertOutstandingSessions(nSessions);
}

Fixture.prototype.assertOutstandingTransactions = function(count) {
    assert.eq(count, this.transactionConn.getDB("config").transactions.count());
};

Fixture.prototype.assertOutstandingSessions = function(count) {
    assert.eq(count, this.getDB("config").system.sessions.count());
};

Fixture.prototype.refresh = function() {
    assert.commandWorked(this.getDB("admin").runCommand({refreshLogicalSessionCacheNow: 1}));
};

Fixture.prototype.reap = function() {
    assert.commandWorked(
        this.transactionConn.getDB("admin").runCommand({reapLogicalSessionCacheNow: 1}));
};

Fixture.prototype.getDB = function(db) {
    return this.conn.getDB(db);
};

Fixture.prototype.stop = function() {
    this.sessions.forEach(function(session) {
        session.endSession();
    });
    return this.impl.stop();
};

[Repl, Sharding].forEach(function(Impl) {
    {
        var fixture = new Fixture(new Impl(-1));
        // Remove a session
        fixture.getDB("config").system.sessions.remove({});
        fixture.assertOutstandingTransactions(nSessions);
        fixture.assertOutstandingSessions(0);

        // See the transaction get reaped as a result
        fixture.reap();
        fixture.assertOutstandingTransactions(0);
        fixture.assertOutstandingSessions(0);

        fixture.stop();
    }

    {
        var fixture = new Fixture(new Impl(30));
        // Remove a session
        fixture.getDB("config").system.sessions.remove({});
        fixture.assertOutstandingTransactions(nSessions);
        fixture.assertOutstandingSessions(0);

        // See the transaction was not reaped as a result
        fixture.reap();
        fixture.assertOutstandingTransactions(nSessions);
        fixture.assertOutstandingSessions(0);

        fixture.stop();
    }
});
})();

/**
 * Test that write errors in a transaction due to SnapshotUnavailable are labelled
 * TransientTransactionError and the error is reported at the top level, not in a writeErrors array.
 *
 * Other transient transaction errors are tested elsewhere: WriteConflict is tested in
 * transactions_write_conflicts.js, NotWritablePrimary is tested in transient_txn_error_labels.js,
 * and NoSuchTransaction is tested in transient_txn_error_labels_with_write_concern.js.
 *
 * @tags: [uses_transactions]
 */
(function() {
"use strict";

const name = "transaction_write_with_snapshot_unavailable";
const replTest = new ReplSetTest({name: name, nodes: 1});
replTest.startSet();
replTest.initiate();

const dbName = name;
const dbNameB = dbName + "B";
const collName = "collection";
const collNameB = collName + "B";

const primary = replTest.getPrimary();
const primaryDB = primary.getDB(dbName);

assert.commandWorked(primaryDB[collName].insertOne({}, {writeConcern: {w: "majority"}}));

function testOp(cmd) {
    let op = Object.getOwnPropertyNames(cmd)[0];
    let session = primary.startSession();
    let sessionDB = session.getDatabase(name);

    jsTestLog(
        `Testing that SnapshotUnavailable during ${op} is labelled TransientTransactionError`);

    session.startTransaction({readConcern: {level: "snapshot"}});
    assert.commandWorked(sessionDB.runCommand({insert: collName, documents: [{}]}));
    // Create collection outside transaction, cannot write to it in the transaction
    assert.commandWorked(primaryDB.getSiblingDB(dbNameB).runCommand({create: collNameB}));

    let res;
    try {
        res = sessionDB.getSiblingDB(dbNameB).runCommand(cmd);
        assert.commandFailedWithCode(res, ErrorCodes.SnapshotUnavailable);
        assert.eq(res.ok, 0);
        assert(!res.hasOwnProperty("writeErrors"));
        assert.eq(res.errorLabels, ["TransientTransactionError"]);
    } catch (ex) {
        printjson(cmd);
        printjson(res);
        throw ex;
    }

    assert.commandFailedWithCode(session.abortTransaction_forTesting(),
                                 ErrorCodes.NoSuchTransaction);
    assert.commandWorked(primaryDB.getSiblingDB(dbNameB).runCommand(
        {dropDatabase: 1, writeConcern: {w: "majority"}}));
}

testOp({insert: collNameB, documents: [{_id: 0}]});
testOp({update: collNameB, updates: [{q: {}, u: {$set: {x: 1}}}]});
testOp({delete: collNameB, deletes: [{q: {_id: 0}, limit: 1}]});

replTest.stopSet();
})();

// Test server validation of the 'transactionLifetimeLimitSeconds' server parameter setting on
// startup and via setParameter command. Valid parameter values are in the range [1, infinity).

(function() {
'use strict';

load("jstests/noPassthrough/libs/server_parameter_helpers.js");

// transactionLifetimeLimitSeconds is set to be higher than its default value in test suites.
delete TestData.transactionLifetimeLimitSeconds;

testNumericServerParameter("transactionLifetimeLimitSeconds",
                           true /*isStartupParameter*/,
                           true /*isRuntimeParameter*/,
                           60 /*defaultValue*/,
                           30 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           0 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);
})();

/**
 * Verify that transactions can be run on the in-memory storage engine. inMemory transactions are
 * not fully supported, but should work for basic MongoDB user testing.
 *
 * TODO: remove this test when general transaction testing is turned on with the inMemory storage
 * engine (SERVER-36023).
 */
(function() {
"use strict";

if (jsTest.options().storageEngine !== "inMemory") {
    jsTestLog("Skipping test because storageEngine is not inMemory");
    return;
}

const dbName = "test";
const collName = "transactions_work_with_in_memory_engine";

const replTest = new ReplSetTest({name: collName, nodes: 1});
replTest.startSet({storageEngine: "inMemory"});
replTest.initiate();

const primary = replTest.getPrimary();

// Initiate a session.
const sessionOptions = {
    causalConsistency: false
};
const session = primary.getDB(dbName).getMongo().startSession(sessionOptions);
const sessionDb = session.getDatabase(dbName);

// Create collection.
assert.commandWorked(sessionDb[collName].insert({x: 0}));

// Execute a transaction that should succeed.
session.startTransaction();
assert.commandWorked(sessionDb[collName].insert({x: 1}));
assert.commandWorked(session.commitTransaction_forTesting());

session.endSession();
replTest.stopSet();
}());

/**
 * Test that a TTL index on a capped collection doesn't crash the server or cause the TTL monitor
 * to skip processing other (non-capped) collections on the database.
 * @tags: [requires_capped]
 */
(function() {
"use strict";

var dbpath = MongoRunner.dataPath + "ttl_capped";
resetDbpath(dbpath);

var conn = MongoRunner.runMongod({
    dbpath: dbpath,
    noCleanData: true,
    setParameter: "ttlMonitorSleepSecs=1",
});
assert.neq(null, conn, "mongod was unable to start up");

var testDB = conn.getDB("test");

assert.commandWorked(testDB.adminCommand({setParameter: 1, ttlMonitorEnabled: false}));

var now = Date.now();
var expireAfterSeconds = 10;

var numCollectionsToCreate = 20;
var width = numCollectionsToCreate.toString().length;

// Create 'numCollectionsToCreate' collections with a TTL index, where every third collection is
// capped. We create many collections with a TTL index to increase the odds that the TTL monitor
// would process a non-capped collection after a capped collection. This allows us to verify
// that the TTL monitor continues processing the remaining collections after encountering an
// error processing a capped collection.
for (var i = 0; i < numCollectionsToCreate; i++) {
    var collName = "ttl" + i.zeroPad(width);
    if (i % 3 === 1) {
        assert.commandWorked(testDB.createCollection(collName, {capped: true, size: 4096}));
    }

    // Create a TTL index on the 'date' field of the collection.
    var res = testDB[collName].createIndex({date: 1}, {expireAfterSeconds: expireAfterSeconds});
    assert.commandWorked(res);

    // Insert a single document with a 'date' field that is already expired according to the
    // index definition.
    assert.commandWorked(
        testDB[collName].insert({date: new Date(now - expireAfterSeconds * 1000)}));
}

// Increase the verbosity of the TTL monitor's output.
assert.commandWorked(testDB.adminCommand({setParameter: 1, logComponentVerbosity: {index: 1}}));

// Enable the TTL monitor and wait for it to run.
var ttlPasses = testDB.serverStatus().metrics.ttl.passes;
assert.commandWorked(testDB.adminCommand({setParameter: 1, ttlMonitorEnabled: true}));

var timeoutSeconds = 60;
assert.soon(
    function checkIfTTLMonitorRan() {
        // The 'ttl.passes' metric is incremented when the TTL monitor starts processing the
        // indexes, so we wait for it to be incremented twice to know that the TTL monitor
        // finished processing the indexes at least once.
        return testDB.serverStatus().metrics.ttl.passes >= ttlPasses + 2;
    },
    function msg() {
        return "TTL monitor didn't run within " + timeoutSeconds + " seconds";
    },
    timeoutSeconds * 1000);

for (var i = 0; i < numCollectionsToCreate; i++) {
    var coll = testDB["ttl" + i.zeroPad(width)];
    var count = coll.count();
    if (i % 3 === 1) {
        assert.eq(1,
                  count,
                  "the TTL monitor shouldn't have removed expired documents from" +
                      " the capped collection '" + coll.getFullName() + "'");
    } else {
        assert.eq(0,
                  count,
                  "the TTL monitor didn't removed expired documents from the" +
                      " collection '" + coll.getFullName() + "'");
    }
}

MongoRunner.stopMongod(conn);
})();

// Make sure the TTL index still work after we hide it
(function() {
"use strict";
let runner = MongoRunner.runMongod({setParameter: "ttlMonitorSleepSecs=1"});
let coll = runner.getDB("test").ttl_hiddenl_index;
coll.drop();

// Create TTL index.
assert.commandWorked(coll.createIndex({x: 1}, {expireAfterSeconds: 0}));
let now = new Date();

assert.commandWorked(coll.hideIndex("x_1"));

// Insert docs after having set hidden index in order to prevent inserted docs being expired out
// before the hidden index is set.
assert.commandWorked(coll.insert({x: now}));
assert.commandWorked(coll.insert({x: now}));

// Wait for the TTL monitor to run at least twice (in case we weren't finished setting up our
// collection when it ran the first time).
var ttlPass = coll.getDB().serverStatus().metrics.ttl.passes;
assert.soon(function() {
    return coll.getDB().serverStatus().metrics.ttl.passes >= ttlPass + 2;
}, "TTL monitor didn't run before timing out.");

assert.eq(coll.count(), 0, "We should get 0 documents after TTL monitor run");

MongoRunner.stopMongod(runner);
})();

/**
 * Ensures that the TTLMonitor does not remove the cached index information from the
 * TTLCollectionCache object for a newly created index before the implicitly created collection is
 * registered and visible in the CollectionCatalog.
 * Removing this cached index information prevents the TTLMonitor from removing expired documents
 * for that collection.
 */
(function() {
'use strict';

const conn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});

const dbName = "test";
const collName = "ttlMonitor";

const db = conn.getDB(dbName);
const coll = db.getCollection(collName);

TestData.dbName = dbName;
TestData.collName = collName;

coll.drop();

const failPoint = "hangTTLCollectionCacheAfterRegisteringInfo";
assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "alwaysOn"}));

// Create an index on a non-existent collection. This will implicitly create the collection.
let awaitcreateIndex = startParallelShell(() => {
    const testDB = db.getSiblingDB(TestData.dbName);
    assert.commandWorked(
        testDB.getCollection(TestData.collName).createIndex({x: 1}, {expireAfterSeconds: 0}));
}, db.getMongo().port);

// Wait for the TTL monitor to run and register the index in the TTL collection cache.
checkLog.containsJson(db.getMongo(), 4664000);

// Let the TTL monitor run once. It should not remove the index from the cached TTL information
// until the collection is committed.
let ttlPass = assert.commandWorked(db.serverStatus()).metrics.ttl.passes;
assert.soon(function() {
    return coll.getDB().serverStatus().metrics.ttl.passes >= ttlPass + 2;
}, "TTL monitor didn't run.");

// Finish the index build.
assert.commandWorked(db.adminCommand({configureFailPoint: failPoint, mode: "off"}));
awaitcreateIndex();

// Insert documents, which should expire immediately and be removed on the next TTL pass.
const now = new Date();
for (let i = 0; i < 10; i++) {
    assert.commandWorked(coll.insert({x: now}));
}

// Let the TTL monitor run once to remove the expired documents.
ttlPass = assert.commandWorked(db.serverStatus()).metrics.ttl.passes;
assert.soon(function() {
    return coll.getDB().serverStatus().metrics.ttl.passes >= ttlPass + 2;
}, "TTL monitor didn't run.");

assert.eq(0, coll.find({}).count());

MongoRunner.stopMongod(conn);
}());

/**
 * Tests resource consumption metrics for TTL indexes.
 *
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

load('jstests/noPassthrough/libs/index_build.js');  // For IndexBuildTest
load("jstests/libs/fail_point_util.js");

var rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {
        setParameter: {
            "aggregateOperationResourceConsumptionMetrics": true,
            "ttlMonitorSleepSecs": 1,
        }
    }
});
rst.startSet();
rst.initiate();

const dbName = 'test';
const collName = 'test';
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const secondaryDB = secondary.getDB(dbName);

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

const waitForTtlPass = (db) => {
    // Wait for the TTL monitor to run at least twice (in case we weren't finished setting up our
    // collection when it ran the first time).
    let ttlPass = db.serverStatus().metrics.ttl.passes;
    assert.soon(function() {
        return db.serverStatus().metrics.ttl.passes >= ttlPass + 2;
    }, "TTL monitor didn't run before timing out.");
};

// Create a TTL index and pause the thread.
assert.commandWorked(primaryDB[collName].createIndex({x: 1}, {expireAfterSeconds: 0}));

let pauseTtl = configureFailPoint(primary, 'hangTTLMonitorWithLock');
pauseTtl.wait();

clearMetrics(primary);

let now = new Date();
let later = new Date(now.getTime() + 1000 * 60 * 60);
assert.commandWorked(primaryDB[collName].insert({_id: 0, x: now}));
assert.commandWorked(primaryDB[collName].insert({_id: 1, x: now}));
assert.commandWorked(primaryDB[collName].insert({_id: 2, x: later}));

assertMetrics(primary, (metrics) => {
    // With replication enabled, oplog writes are counted towards bytes written. Only assert that we
    // insert at least as many bytes in the documents.
    // Document size is 29 bytes.
    assert.gte(metrics[dbName].docBytesWritten, 29 * 3);
    assert.gte(metrics[dbName].docUnitsWritten, 3);
    assert.gte(metrics[dbName].totalUnitsWritten, 3);
});

// Clear metrics and wait for a TTL pass to delete the documents.
clearMetrics(primary);
pauseTtl.off();
waitForTtlPass(primaryDB);

// Ensure that the TTL monitor deleted 2 documents on the primary and recorded read and write
// metrics.
assertMetrics(primary, (metrics) => {
    // The TTL monitor generates oplog entries for each deletion on the primary. Assert that we
    // write at least as many bytes in the documents. Document size is 29 bytes.
    assert.gte(metrics[dbName].primaryMetrics.docBytesRead, 29 * 2);
    assert.gte(metrics[dbName].primaryMetrics.docUnitsRead, 2);
    assert.gte(metrics[dbName].docBytesWritten, 29 * 2);
    assert.gte(metrics[dbName].docUnitsWritten, 2);
    assert.gte(metrics[dbName].totalUnitsWritten, 2);
    // Key size is 12 bytes.
    assert.gte(metrics[dbName].primaryMetrics.idxEntryBytesRead, 12 * 2);
    assert.gte(metrics[dbName].primaryMetrics.idxEntryUnitsRead, 2);
    // At least 2 keys (_id and x) should be deleted for each document.
    assert.gte(metrics[dbName].idxEntryUnitsWritten, 2 * 2);
    assert.gte(metrics[dbName].idxEntryBytesWritten, 12 * 2);
});

rst.awaitReplication();

// There should be no activity on the secondary.
assertMetrics(secondary, (metrics) => {
    assert(!metrics.hasOwnProperty(dbName));
});

// Ensure the last document was not deleted.
assert.eq(primaryDB[collName].count({}), 1);
assert.eq(secondaryDB[collName].count({}), 1);

rst.stopSet();
}());

// Test that the TTL monitor will correctly use TTL indexes that are also partial indexes.
// SERVER-17984.
(function() {
"use strict";
// Launch mongod with shorter TTL monitor sleep interval.
var runner = MongoRunner.runMongod({setParameter: "ttlMonitorSleepSecs=1"});
var coll = runner.getDB("test").ttl_partial_index;
coll.drop();

// Create TTL partial index.
assert.commandWorked(coll.createIndex(
    {x: 1}, {expireAfterSeconds: 0, partialFilterExpression: {z: {$exists: true}}}));

var now = new Date();
assert.commandWorked(coll.insert({x: now, z: 2}));
assert.commandWorked(coll.insert({x: now}));

// Wait for the TTL monitor to run at least twice (in case we weren't finished setting up our
// collection when it ran the first time).
var ttlPass = coll.getDB().serverStatus().metrics.ttl.passes;
assert.soon(function() {
    return coll.getDB().serverStatus().metrics.ttl.passes >= ttlPass + 2;
}, "TTL monitor didn't run before timing out.");

assert.eq(0,
          coll.find({z: {$exists: true}}).hint({x: 1}).itcount(),
          "Wrong number of documents in partial index, after TTL monitor run");
assert.eq(
    1, coll.find().itcount(), "Wrong number of documents in collection, after TTL monitor run");
MongoRunner.stopMongod(runner);
})();

// Tests that the TTL Monitor is disabled for <database>.system.resharding.* namespaces.
(function() {
"use strict";
// Launch mongod with shorter TTL monitor sleep interval.
const runner = MongoRunner.runMongod({setParameter: "ttlMonitorSleepSecs=1"});
const collName = "system.resharding.mycoll";
const coll = runner.getDB(jsTestName())[collName];
coll.drop();

assert.commandWorked(coll.createIndex({x: 1}, {expireAfterSeconds: 0}));

const now = new Date();
assert.commandWorked(coll.insert({x: now}));

// Wait for the TTL monitor to run at least twice (in case we weren't finished setting up our
// collection when it ran the first time).
const ttlPass = coll.getDB().serverStatus().metrics.ttl.passes;
assert.soon(function() {
    return coll.getDB().serverStatus().metrics.ttl.passes >= ttlPass + 2;
}, "TTL monitor didn't run before timing out.");

// Confirm the document was not removed because it was in a <database>.system.resharding.*
// namespace.
assert.eq(
    1, coll.find().itcount(), "Wrong number of documents in collection, after TTL monitor run");
MongoRunner.stopMongod(runner);
})();

/**
 * Verify the behavior of dropping TTL index.
 */
(function() {
'use strict';

let conn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});
let db = conn.getDB('test');
let coll = db.ttl_coll;
coll.drop();
let now = (new Date()).getTime();

// Insert 50 docs with timestamp 'now - 24h'.
let past = new Date(now - (3600 * 1000 * 24));
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll', documents: [{x: past}]}));
}

assert.eq(coll.find().itcount(), 50);

// Create TTL index: expire docs older than 20000 seconds (~5.5h).
coll.createIndex({x: 1}, {expireAfterSeconds: 20000});

assert.soon(function() {
    return coll.find().itcount() == 0;
}, 'TTL index on x didn\'t delete');

// Drop the TTL index.
assert.commandWorked(coll.dropIndex({x: 1}));

// Re-insert 50 docs with timestamp 'now - 24h'.
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll', documents: [{x: past}]}));
}

var ttlPasses = db.serverStatus().metrics.ttl.passes;
assert.soon(function() {
    return db.serverStatus().metrics.ttl.passes > ttlPasses;
});

assert.eq(coll.find().itcount(), 50);

MongoRunner.stopMongod(conn);
})();

/**
 * Since ttl mechanism uses UUIDs to keep track of collections that have TTL indexes, nothing needs
 * to be done during a collection rename. This test is to verify the TTL behavior after a collection
 * rename.
 */
(function() {
'use strict';

let conn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});
let db = conn.getDB('test');
let coll = db.ttl_coll;
coll.drop();
let now = (new Date()).getTime();

// Insert 50 docs with timestamp 'now - 24h'.
let past = new Date(now - (3600 * 1000 * 24));
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll', documents: [{x: past}]}));
}

assert.eq(coll.find().itcount(), 50);

// Create TTL index: expire docs older than 20000 seconds (~5.5h).
coll.createIndex({x: 1}, {expireAfterSeconds: 20000});

assert.soon(function() {
    return coll.find().itcount() == 0;
}, 'TTL index on x didn\'t delete');

// Rename the collection
assert.commandWorked(db.adminCommand(
    {renameCollection: 'test.ttl_coll', to: 'test.ttl_coll_renamed', dropTarget: true}));

// Re-insert 50 docs with timestamp 'now - 24h'.
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll_renamed', documents: [{x: past}]}));
}

// Assert that the TTL mechanism still works on this collection.
assert.soon(function() {
    return coll.find().itcount() == 0;
}, 'TTL index on x didn\'t delete');

MongoRunner.stopMongod(conn);
})();

/**
 * Verify the TTL index behavior after restart.
 * @tags: [requires_persistence]
 */
(function() {
'use strict';
let oldConn = MongoRunner.runMongod({setParameter: 'ttlMonitorSleepSecs=1'});
let db = oldConn.getDB('test');

let coll = db.ttl_coll;
coll.drop();
let now = (new Date()).getTime();

// Insert 50 docs with timestamp 'now - 24h'.
let past = new Date(now - (3600 * 1000 * 24));
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll', documents: [{x: past}]}));
}

assert.eq(coll.find().itcount(), 50);

// Create TTL index: expire docs older than 20000 seconds (~5.5h).
coll.createIndex({x: 1}, {expireAfterSeconds: 20000});

assert.soon(function() {
    return coll.find().itcount() == 0;
}, 'TTL index on x didn\'t delete');

// Restart the server.
MongoRunner.stopMongod(oldConn);
let newConn = MongoRunner.runMongod({
    restart: true,
    dbpath: oldConn.dbpath,
    cleanData: false,
    setParameter: "ttlMonitorSleepSecs=1"
});
db = newConn.getDB('test');
coll = db.ttl_coll;

// Re-insert 50 docs with timestamp 'now - 24h'.
for (let i = 0; i < 50; i++) {
    assert.commandWorked(db.runCommand({insert: 'ttl_coll', documents: [{x: past}]}));
}

assert.soon(function() {
    return coll.find().itcount() == 0;
}, 'TTL index on x didn\'t delete');

MongoRunner.stopMongod(newConn);
})();

// Tests the ttlMonitorSleepSecs parameter

(function() {
'use strict';

load('jstests/noPassthrough/libs/server_parameter_helpers.js');

testNumericServerParameter(
    'ttlMonitorSleepSecs',
    true,     // is Startup Param
    false,    // is runtime param
    60,       // default value
    30,       // valid, non-default value
    true,     // has lower bound
    0,        // out of bound value (below lower bound)
    false,    // has upper bound
    'unused'  // out of bounds value (above upper bound)
);
})();

/**
 * Ensures that oplog entries specific to two-phase index builds are not allow when run through
 * applyOps.
 *
 * @tags: [
 *   requires_replication,
 * ]
 */

(function() {

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

const testDB = replSet.getPrimary().getDB('test');
const coll = testDB.twoPhaseIndexBuild;
const cmdNs = testDB.getName() + ".$cmd";

coll.insert({a: 1});

assert.commandFailedWithCode(testDB.adminCommand({
    applyOps: [{op: "c", ns: cmdNs, o: {startIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}}]
}),
                             [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);

assert.commandFailedWithCode(testDB.adminCommand({
    applyOps:
        [{op: "c", ns: cmdNs, o: {commitIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}}]
}),
                             [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);

assert.commandFailedWithCode(testDB.adminCommand({
    applyOps: [{op: "c", ns: cmdNs, o: {abortIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}}]
}),
                             [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);

replSet.stopSet();
})();

/**
 * Verifies the network_error_and_txn_override passthrough respects the causal consistency setting
 * on TestData when starting a transaction.
 *
 * @tags: [requires_replication, uses_transactions]
 */
(function() {
"use strict";

const dbName = "test";
const collName = "foo";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
const conn = new Mongo(rst.getPrimary().host);

// Create the collection so the override doesn't try to when it is not expected.
assert.commandWorked(conn.getDB(dbName).createCollection(collName));

// Override runCommand to add each command it sees to a global array that can be inspected by
// this test and to allow mocking certain responses.
let cmdObjsSeen = [];
let mockNetworkError, mockFirstResponse, mockFirstCommitResponse;
const mongoRunCommandOriginal = Mongo.prototype.runCommand;
Mongo.prototype.runCommand = function runCommandSpy(dbName, cmdObj, options) {
    cmdObjsSeen.push(cmdObj);

    if (mockNetworkError) {
        mockNetworkError = undefined;
        throw new Error("network error");
    }

    if (mockFirstResponse) {
        const mockedRes = mockFirstResponse;
        mockFirstResponse = undefined;
        return mockedRes;
    }

    const cmdName = Object.keys(cmdObj)[0];
    if (cmdName === "commitTransaction" && mockFirstCommitResponse) {
        const mockedRes = mockFirstCommitResponse;
        mockFirstCommitResponse = undefined;
        return mockedRes;
    }

    return mongoRunCommandOriginal.apply(this, arguments);
};

// Runs the given function with a collection from a session made with the sessionOptions on
// TestData and asserts the seen commands that would start a transaction have or do not have
// afterClusterTime.
function inspectFirstCommandForAfterClusterTime(conn, cmdName, isCausal, expectRetry, func) {
    const session = conn.startSession(TestData.sessionOptions);
    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB[collName];

    cmdObjsSeen = [];
    func(sessionColl);

    // Find all requests sent with the expected command name, in case the scenario allows
    // retrying more than once or expects to end with a commit.
    let cmds = [];
    if (!expectRetry) {
        assert.eq(1, cmdObjsSeen.length);
        cmds.push(cmdObjsSeen[0]);
    } else {
        assert.lt(1, cmdObjsSeen.length);
        cmds = cmdObjsSeen.filter(obj => Object.keys(obj)[0] === cmdName);
    }

    for (let cmd of cmds) {
        if (isCausal) {
            assert(cmd.hasOwnProperty("$clusterTime"),
                   "Expected " + tojson(cmd) + " to have a $clusterTime.");
            assert(cmd.hasOwnProperty("readConcern"),
                   "Expected " + tojson(cmd) + " to have a read concern.");
            assert(cmd.readConcern.hasOwnProperty("afterClusterTime"),
                   "Expected " + tojson(cmd) + " to have an afterClusterTime.");
        } else {
            if (TestData.hasOwnProperty("enableMajorityReadConcern") &&
                TestData.enableMajorityReadConcern === false) {
                // Commands not allowed in a transaction without causal consistency will not
                // have a read concern on variants that don't enable majority read concern.
                continue;
            }

            assert(cmd.hasOwnProperty("readConcern"),
                   "Expected " + tojson(cmd) + " to have a read concern.");
            assert(!cmd.readConcern.hasOwnProperty("afterClusterTime"),
                   "Expected " + tojson(cmd) + " to not have an afterClusterTime.");
        }
    }

    // Run a command not runnable in a transaction to reset the override's transaction state.
    assert.commandWorked(sessionDB.runCommand({ping: 1}));

    session.endSession();
}

// Helper methods for testing specific commands.

function testInsert(conn, isCausal, expectRetry) {
    inspectFirstCommandForAfterClusterTime(conn, "insert", isCausal, expectRetry, (coll) => {
        assert.commandWorked(coll.insert({x: 1}));
    });
}

function testFind(conn, isCausal, expectRetry) {
    inspectFirstCommandForAfterClusterTime(conn, "find", isCausal, expectRetry, (coll) => {
        assert.eq(0, coll.find({y: 1}).itcount());
    });
}

function testCount(conn, isCausal, expectRetry) {
    inspectFirstCommandForAfterClusterTime(conn, "count", isCausal, expectRetry, (coll) => {
        assert.eq(0, coll.count({y: 1}));
    });
}

function testCommit(conn, isCausal, expectRetry) {
    inspectFirstCommandForAfterClusterTime(conn, "find", isCausal, expectRetry, (coll) => {
        assert.eq(0, coll.find({y: 1}).itcount());
        assert.commandWorked(coll.getDB().runCommand({ping: 1}));  // commits the transaction.
    });
}

// Load the txn_override after creating the spy, so the spy will see commands after being
// transformed by the override. Also configure network error retries because several suites use
// both.
TestData.networkErrorAndTxnOverrideConfig = {
    wrapCRUDinTransactions: true,
    retryOnNetworkErrors: true
};
load("jstests/libs/override_methods/network_error_and_txn_override.js");

TestData.logRetryAttempts = true;

// Run a command to guarantee operation time is initialized on the database's session.
assert.commandWorked(conn.getDB(dbName).runCommand({ping: 1}));

function runTest() {
    for (let isCausal of [false, true]) {
        jsTestLog("Testing with isCausal = " + isCausal);
        TestData.sessionOptions = {causalConsistency: isCausal};

        // Commands that accept read and write concern allowed in a transaction.
        testInsert(conn, isCausal, false /*expectRetry*/);
        testFind(conn, isCausal, false /*expectRetry*/);

        // Command that can accept read concern not allowed in a transaction.
        testCount(conn, isCausal, false /*expectRetry*/);

        // Command that attempts to implicitly create a collection.
        conn.getDB(dbName)[collName].drop();
        testInsert(conn, isCausal, false /*expectRetry*/);

        // Command that can accept read concern with retryable error.
        mockFirstResponse = {ok: 0, code: ErrorCodes.CursorKilled};
        testFind(conn, isCausal, true /*expectRetry*/);

        // Commands that can accept read and write concern with network error.
        mockNetworkError = true;
        testInsert(conn, isCausal, true /*expectRetry*/);

        mockNetworkError = true;
        testFind(conn, isCausal, true /*expectRetry*/);

        // Command that can accept read concern not allowed in a transaction with network error.
        mockNetworkError = true;
        testCount(conn, isCausal, true /*expectRetry*/);

        // Commands that can accept read and write concern with transient transaction error.
        mockFirstResponse = {
            ok: 0,
            code: ErrorCodes.NoSuchTransaction,
            errorLabels: ["TransientTransactionError"]
        };
        testFind(conn, isCausal, true /*expectRetry*/);

        mockFirstResponse = {
            ok: 0,
            code: ErrorCodes.NoSuchTransaction,
            errorLabels: ["TransientTransactionError"]
        };
        testInsert(conn, isCausal, true /*expectRetry*/);

        // Transient transaction error on commit attempt.
        mockFirstCommitResponse = {
            ok: 0,
            code: ErrorCodes.NoSuchTransaction,
            errorLabels: ["TransientTransactionError"]
        };
        testCommit(conn, isCausal, true /*expectRetry*/);

        // Network error on commit attempt.
        mockFirstCommitResponse = {ok: 0, code: ErrorCodes.NotWritablePrimary};
        testCommit(conn, isCausal, true /*expectRetry*/);
    }
}

runTest();

// With read concern majority disabled.
TestData.enableMajorityReadConcern = false;
runTest();
delete TestData.enableMajorityReadConcern;

rst.stopSet();
})();

/*
 * This test makes sure that the log files created by the server correctly honor the server's umask
 * as set in SERVER-22829
 *
 * @tags: [ requires_wiredtiger ]
 */
(function() {
'use strict';
// We only test this on POSIX since that's the only platform where umasks make sense
if (_isWindows()) {
    return;
}

const oldUmask = new Number(umask(0));
jsTestLog("Setting umask to really permissive 000 mode, old mode was " + oldUmask.toString(8));

const defaultUmask = Number.parseInt("600", 8);
const permissiveUmask = Number.parseInt("666", 8);

// Any files that have some explicit permissions set on them should be added to this list
const exceptions = [
    // The lock file gets created with explicit 644 permissions
    'mongod.lock',
];

let mongodOptions = MongoRunner.mongodOptions({
    useLogFiles: true,
    cleanData: true,
});

if (buildInfo()["modules"].some((mod) => {
        return mod == "enterprise";
    })) {
    mongodOptions.auditDestination = "file";
    mongodOptions.auditPath = mongodOptions.dbpath + "/audit.log";
    mongodOptions.auditFormat = "JSON";
}

function checkMask(topDir, expected, honoringUmask, customUmask = false) {
    const processDirectory = (dir) => {
        jsTestLog(`Checking ${dir}`);
        ls(dir).forEach((file) => {
            if (file.endsWith("/")) {
                return processDirectory(file);
            } else if (exceptions.some((exception) => {
                           return file.endsWith(exception);
                       })) {
                return;
            }
            const mode = new Number(getFileMode(file));
            const modeStr = mode.toString(8);
            let msg = `Mode for ${file} is ${modeStr} when `;
            if (customUmask) {
                msg += ' using custom umask';
            } else {
                msg += (honoringUmask ? '' : 'not ') + ' honoring system umask';
            }
            assert.eq(mode.valueOf(), expected, msg);
        });
    };

    processDirectory(topDir);
}

// First we start up the mongod normally, all the files except mongod.lock should have the mode
// 0600
let conn = MongoRunner.runMongod(mongodOptions);
MongoRunner.stopMongod(conn);
checkMask(conn.fullOptions.dbpath, defaultUmask, false);

// Restart the mongod with honorSystemUmask, all files should have the mode 0666
mongodOptions.setParameter = {
    honorSystemUmask: true
};
conn = MongoRunner.runMongod(mongodOptions);
MongoRunner.stopMongod(conn);
checkMask(conn.fullOptions.dbpath, permissiveUmask, true);

// Restart the mongod with custom umask as string, all files should have the mode 0644
const worldReadableUmask = Number.parseInt("644", 8);
mongodOptions.setParameter = {
    processUmask: '022',
};
conn = MongoRunner.runMongod(mongodOptions);
MongoRunner.stopMongod(conn);
checkMask(conn.fullOptions.dbpath, worldReadableUmask, false, true);

// Fail to start up with both honorSystemUmask and processUmask set.
mongodOptions.setParameter = {
    honorSystemUmask: true,
    processUmask: '022',
};
assert.throws(() => MongoRunner.runMongod(mongodOptions));

// Okay to start with both if honorSystemUmask is false.
mongodOptions.setParameter = {
    honorSystemUmask: false,
    processUmask: '022',
};
conn = MongoRunner.runMongod(mongodOptions);
MongoRunner.stopMongod(conn);
checkMask(conn.fullOptions.dbpath, worldReadableUmask, false, true);

umask(oldUmask.valueOf());
})();

// Test that sharded $unionWith can resolve sharded views correctly when target shards are on
// different, non-primary shards.
// @tags: [requires_sharding, requires_fcv_50]
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertArrayEq.

const sharded = new ShardingTest({mongos: 1, shards: 4, config: 1});
assert(sharded.adminCommand({enableSharding: "test"}));

const testDBName = "test";
const testDB = sharded.getDB(testDBName);

const local = testDB.local;
local.drop();
assert.commandWorked(local.createIndex({shard_key: 1}));

const foreign = testDB.foreign;
foreign.drop();
assert.commandWorked(foreign.createIndex({shard_key: 1}));

const otherForeign = testDB.otherForeign;
otherForeign.drop();
assert.commandWorked(otherForeign.createIndex({shard_key: 1}));

assert.commandWorked(local.insertMany([
    {_id: 1, shard_key: "shard1"},
    {_id: 2, shard_key: "shard1"},
    {_id: 3, shard_key: "shard1"},
]));

assert.commandWorked(foreign.insertMany([
    {_id: 4, shard_key: "shard2"},
    {_id: 5, shard_key: "shard2"},
    {_id: 6, shard_key: "shard2"},
]));

assert.commandWorked(otherForeign.insertMany([
    {_id: 7, shard_key: "shard3"},
    {_id: 8, shard_key: "shard3"},
]));

sharded.ensurePrimaryShard(testDBName, sharded.shard0.shardName);
assert(sharded.s.adminCommand({shardCollection: local.getFullName(), key: {shard_key: 1}}));
assert(sharded.s.adminCommand({shardCollection: foreign.getFullName(), key: {shard_key: 1}}));
assert(sharded.s.adminCommand({shardCollection: otherForeign.getFullName(), key: {shard_key: 1}}));

function testUnionWithView(pipeline, expected) {
    assertArrayEq({actual: local.aggregate(pipeline).toArray(), expected});
}

function checkView(viewName, expected) {
    assertArrayEq({actual: testDB[viewName].find({}).toArray(), expected});
}

// Place all of local on shard1 and all of foreign on shard2 to force
// CommandOnShardedViewNotSupportedOnMongod exceptions where a shard cannot resolve a view
// definition.
assert.commandWorked(testDB.adminCommand(
    {moveChunk: local.getFullName(), find: {shard_key: "shard1"}, to: sharded.shard1.shardName}));
assert.commandWorked(testDB.adminCommand(
    {moveChunk: foreign.getFullName(), find: {shard_key: "shard2"}, to: sharded.shard2.shardName}));
assert.commandWorked(testDB.adminCommand({
    moveChunk: otherForeign.getFullName(),
    find: {shard_key: "shard3"},
    to: sharded.shard3.shardName
}));

// Create a view on foreign with a pipeline that references a namespace that the top-level unionWith
// has not yet encountered and verify that the view can be queried correctly.
assert.commandWorked(
    testDB.createView("unionView", foreign.getName(), [{$unionWith: "otherForeign"}]));
checkView("unionView", [
    {_id: 4, shard_key: "shard2"},
    {_id: 5, shard_key: "shard2"},
    {_id: 6, shard_key: "shard2"},
    {_id: 7, shard_key: "shard3"},
    {_id: 8, shard_key: "shard3"},
]);

testUnionWithView(
    [
        {$unionWith: "unionView"},
    ],
    [
        {_id: 1, shard_key: "shard1"},
        {_id: 2, shard_key: "shard1"},
        {_id: 3, shard_key: "shard1"},
        {_id: 4, shard_key: "shard2"},
        {_id: 5, shard_key: "shard2"},
        {_id: 6, shard_key: "shard2"},
        {_id: 7, shard_key: "shard3"},
        {_id: 8, shard_key: "shard3"},
    ]);

sharded.stop();
}());

/**
 * Test that $unionWith works when unioning unsharded with sharded collections, and vice versa.
 *
 * @tags: [
 *   assumes_unsharded_collection,
 *   do_not_wrap_aggregations_in_facets,
 *   requires_replication,
 *   requires_sharding,
 * ]
 */

(function() {
"use strict";
load("jstests/aggregation/extras/utils.js");  // arrayEq

function getDocsFromCollection(collObj) {
    return collObj.find().toArray();
}
function checkResults(resObj, expectedResult) {
    assert(arrayEq(resObj.cursor.firstBatch, expectedResult),
           "Expected:\n" + tojson(expectedResult) + "Got:\n" + tojson(resObj.cursor.firstBatch));
}
const st = new ShardingTest({shards: 2, mongos: 1, config: 1});
const mongos = st.s;
const dbName = jsTestName();
assert.commandWorked(mongos.adminCommand({enableSharding: dbName}));
const testDB = mongos.getDB(dbName);
const shardedCollOne = testDB.shardedCollOne;
shardedCollOne.drop();
const shardedCollTwo = testDB.shardedCollTwo;
shardedCollTwo.drop();
const unshardedCollOne = testDB.unshardedCollOne;
unshardedCollOne.drop();
const unshardedCollTwo = testDB.unshardedCollTwo;
unshardedCollTwo.drop();
for (let i = 0; i < 5; i++) {
    assert.commandWorked(shardedCollOne.insert({val: i}));
    assert.commandWorked(shardedCollTwo.insert({val: i * 2}));
    assert.commandWorked(unshardedCollOne.insert({val: i * 3}));
    assert.commandWorked(unshardedCollTwo.insert({val: i * 4}));
}
assert.commandWorked(
    mongos.adminCommand({shardCollection: shardedCollOne.getFullName(), key: {_id: 1}}));
assert.commandWorked(
    mongos.adminCommand({shardCollection: shardedCollTwo.getFullName(), key: {_id: 1}}));

// Run each test against both the primary and non-primary shards.
// Make sure the primary is always shard0.
st.ensurePrimaryShard(dbName, st.shard0.shardName);
const shardNames = [st.shard0.shardName, st.shard1.shardName];
shardNames.forEach(function(shardName) {
    jsTestLog("Testing with docs on " + shardName);
    testDB.adminCommand({moveChunk: shardedCollOne.getFullName(), find: {_id: 0}, to: shardName});
    testDB.adminCommand({moveChunk: shardedCollTwo.getFullName(), find: {_id: 0}, to: shardName});
    // Test one sharded and one unsharded collection.
    let resSet =
        getDocsFromCollection(shardedCollOne).concat(getDocsFromCollection(unshardedCollOne));
    let resObj = assert.commandWorked(testDB.runCommand({
        aggregate: shardedCollOne.getName(),
        pipeline: [{$unionWith: unshardedCollOne.getName()}],
        cursor: {}
    }));
    checkResults(resObj, resSet);
    resObj = assert.commandWorked(testDB.runCommand({
        aggregate: unshardedCollOne.getName(),
        pipeline: [{$unionWith: shardedCollOne.getName()}],
        cursor: {}
    }));
    checkResults(resObj, resSet);

    // Test a union of two sharded collections and one unsharded collection.
    resSet = getDocsFromCollection(shardedCollOne)
                 .concat(getDocsFromCollection(unshardedCollOne))
                 .concat(getDocsFromCollection(shardedCollTwo));
    resObj = assert.commandWorked(testDB.runCommand({
        aggregate: shardedCollOne.getName(),
        pipeline: [{
            $unionWith: {
                coll: unshardedCollOne.getName(),
                pipeline: [{$unionWith: shardedCollTwo.getName()}]
            }
        }],
        cursor: {}
    }));
    checkResults(resObj, resSet);
    // Test a union of two unsharded collections and one sharded collection.
    resSet = getDocsFromCollection(unshardedCollOne)
                 .concat(getDocsFromCollection(shardedCollOne))
                 .concat(getDocsFromCollection(unshardedCollTwo));
    resObj = assert.commandWorked(testDB.runCommand({
        aggregate: unshardedCollOne.getName(),
        pipeline: [{
            $unionWith: {
                coll: shardedCollOne.getName(),
                pipeline: [{$unionWith: unshardedCollTwo.getName()}]
            }
        }],
        cursor: {}
    }));
    checkResults(resObj, resSet);

    // Test a union of two sharded collections when the documents are on different shards.
    jsTestLog("Testing with docs on two different shards");
    testDB.adminCommand({
        moveChunk: shardedCollTwo.getFullName(),
        find: {_id: 0},
        to: st.shard0.shardName == shardName ? st.shard1.shardName : st.shard0.shardName
    });
    resSet = getDocsFromCollection(shardedCollOne).concat(getDocsFromCollection(shardedCollTwo));
    resObj = assert.commandWorked(testDB.runCommand({
        aggregate: shardedCollOne.getName(),
        pipeline: [{$unionWith: shardedCollTwo.getName()}],
        cursor: {}
    }));
    checkResults(resObj, resSet);
    // Test a union of two sharded collections on different shards with an additional unsharded
    // collection.
    resSet = resSet.concat(getDocsFromCollection(unshardedCollOne));
    resObj = assert.commandWorked(testDB.runCommand({
        aggregate: shardedCollOne.getName(),
        pipeline: [{
            $unionWith: {
                coll: unshardedCollOne.getName(),
                pipeline: [{$unionWith: shardedCollTwo.getName()}]
            }
        }],
        cursor: {}
    }));
    checkResults(resObj, resSet);
});
st.stop();
})();

/*
 * This test checks that when mongod is started with UNIX sockets enabled or disabled,
 * that we are able to connect (or not connect) and run commands:
 * 1) There should be a default unix socket of /tmp/mongod-portnumber.sock
 * 2) If you specify a custom socket in the bind_ip param, that it shows up as
 *    /tmp/custom_socket.sock
 * 3) That bad socket paths, like paths longer than the maximum size of a sockaddr
 *    cause the server to exit with an error (socket names with whitespace are now supported)
 * 4) That the default unix socket doesn't get created if --nounixsocket is specified
 */
// @tags: [
//   live_record_incompatible,
//   requires_sharding,
// ]
(function() {
'use strict';
// This test will only work on POSIX machines.
if (_isWindows()) {
    return;
}

// Checking index consistency involves reconnecting to the mongos.
TestData.skipCheckingIndexesConsistentAcrossCluster = true;
TestData.skipCheckOrphans = true;

// Do not fail if this test leaves unterminated processes because testSockOptions
// is expected to throw before it calls stopMongod.
TestData.failIfUnterminatedProcesses = false;

var doesLogMatchRegex = function(logArray, regex) {
    for (let i = (logArray.length - 1); i >= 0; i--) {
        var regexInLine = regex.exec(logArray[i]);
        if (regexInLine != null) {
            return true;
        }
    }
    return false;
};

var checkSocket = function(path) {
    assert.eq(fileExists(path), true);
    var conn = new Mongo(path);
    assert.commandWorked(conn.getDB("admin").runCommand("ping"),
                         `Expected ping command to succeed for ${path}`);
};

var testSockOptions = function(bindPath, expectSockPath, optDict, bindSep = ',', optMongos) {
    var optDict = optDict || {};
    if (bindPath) {
        optDict["bind_ip"] = `${MongoRunner.dataDir}/${bindPath}${bindSep}127.0.0.1`;
    }

    var conn, shards;
    if (optMongos) {
        shards = new ShardingTest({shards: 1, mongos: 1, other: {mongosOptions: optDict}});
        assert.neq(shards, null, "Expected cluster to start okay");
        conn = shards.s0;
    } else {
        conn = MongoRunner.runMongod(optDict);
    }

    assert.neq(conn, null, `Expected ${optMongos ? "mongos" : "mongod"} to start okay`);

    const defaultUNIXSocket = `/tmp/mongodb-${conn.port}.sock`;
    var checkPath = defaultUNIXSocket;
    if (expectSockPath) {
        checkPath = `${MongoRunner.dataDir}/${expectSockPath}`;
    }

    checkSocket(checkPath);

    // Test the naming of the unix socket
    var log = conn.adminCommand({getLog: 'global'});
    assert.commandWorked(log, "Expected getting the log to work");
    var ll = log.log;
    var re = new RegExp("anonymous unix socket");
    assert(doesLogMatchRegex(ll, re), "Log message did not contain 'anonymous unix socket'");

    if (optMongos) {
        shards.stop();
    } else {
        MongoRunner.stopMongod(conn);
    }

    assert.eq(fileExists(checkPath), false);
};

// Check that the default unix sockets work
testSockOptions();
testSockOptions(undefined, undefined, undefined, ',', true);

// Check that a custom unix socket path works
testSockOptions("testsock.socket", "testsock.socket");
testSockOptions("testsock.socket", "testsock.socket", undefined, ',', true);

// Check that a custom unix socket path works with spaces
testSockOptions("test sock.socket", "test sock.socket");
testSockOptions("test sock.socket", "test sock.socket", undefined, ',', true);

// Check that a custom unix socket path works with spaces before the comma and after
testSockOptions("testsock.socket ", "testsock.socket", undefined, ', ');
testSockOptions("testsock.socket ", "testsock.socket", undefined, ', ', true);

// Check that a bad UNIX path breaks
assert.throws(function() {
    var badname = "a".repeat(200) + ".socket";
    testSockOptions(badname, badname);
});

// Check that if UNIX sockets are disabled that we aren't able to connect over UNIX sockets
assert.throws(function() {
    testSockOptions(undefined, undefined, {nounixsocket: ""});
});

// Check the unixSocketPrefix option
var socketPrefix = `${MongoRunner.dataDir}/socketdir`;
mkdir(socketPrefix);
var port = allocatePort();
testSockOptions(
    undefined, `socketdir/mongodb-${port}.sock`, {unixSocketPrefix: socketPrefix, port: port});

port = allocatePort();
testSockOptions(undefined,
                `socketdir/mongodb-${port}.sock`,
                {unixSocketPrefix: socketPrefix, port: port},
                ',',
                true);
})();

// Verify error is produced when specifying an invalid set parameter.

(function() {
'use strict';

TestData.enableTestCommands = false;

function tryRun(arg) {
    // runMongoProgram helpfully makes certain that we pass a port when invoking mongod.
    return runMongoProgram('mongod', '--port', 0, '--setParameter', arg, '--outputConfig');
}

// Positive case, valid setparam.
clearRawMongoProgramOutput();
const valid = tryRun('enableTestCommands=1');
assert.eq(valid, 0);
const validOutput = rawMongoProgramOutput();
assert.gte(validOutput.search(/enableTestCommands: 1/), 0, validOutput);

// Negative case, invalid setparam.
clearRawMongoProgramOutput();
const foo = tryRun('foo=bar');
assert.neq(foo, 0);
const fooOutput = rawMongoProgramOutput();
assert.gte(fooOutput.search(/Unknown --setParameter 'foo'/), 0, fooOutput);

// Negative case, valid but unavailable setparam.
clearRawMongoProgramOutput();
const graph = tryRun('roleGraphInvalidationIsFatal=true');
assert.neq(graph, 0);
const graphOutput = rawMongoProgramOutput();
assert.gte(
    graphOutput.search(
        /--setParameter 'roleGraphInvalidationIsFatal' only available when used with 'enableTestCommands'/),
    0,
    fooOutput);
}());

// Tests that the $changeStream stage returns an error when run against a standalone mongod.
// @tags: [requires_sharding, uses_change_streams, requires_majority_read_concern]

(function() {
"use strict";
load("jstests/aggregation/extras/utils.js");  // For assertErrorCode.

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

function assertChangeStreamNotSupportedOnConnection(conn) {
    const notReplicaSetErrorCode = 40573;
    assertErrorCode(conn.getDB("test").non_existent, [{$changeStream: {}}], notReplicaSetErrorCode);
    assertErrorCode(conn.getDB("test").non_existent,
                    [{$changeStream: {fullDocument: "updateLookup"}}],
                    notReplicaSetErrorCode);
}

const conn = MongoRunner.runMongod({enableMajorityReadConcern: ""});
assert.neq(null, conn, "mongod was unable to start up");
// $changeStream cannot run on a non-existent database.
assert.commandWorked(conn.getDB("test").ensure_db_exists.insert({}));
assertChangeStreamNotSupportedOnConnection(conn);
assert.eq(0, MongoRunner.stopMongod(conn));
}());

/**
 * Tests that the $$NOW and $$CLUSTER_TIME system variables can be used when performing updates on a
 * replica set.
 *
 * The 'requires_replication' tag prevents the test from running on variants with storage options
 * which cannot support a replica set.
 * @tags: [
 *   requires_replication,
 * ]
 */
(function() {
"use strict";

const rst = new ReplSetTest({name: jsTestName(), nodes: 1});
rst.startSet();
rst.initiate();

const db = rst.getPrimary().getDB(jsTestName());
const otherColl = db.other;
const coll = db.test;
otherColl.drop();
coll.drop();

// Insert N docs, with the _id field set to the current Date. We sleep for a short period
// between insertions, such that the Date value increases for each successive document.
let bulk = coll.initializeUnorderedBulkOp();
const _idStart = new Date();
const numDocs = 10;
for (let i = 0; i < numDocs; ++i) {
    bulk.insert({_id: new Date(), insertClusterTime: new Timestamp(0, 0)});
    if (i < numDocs - 1) {
        sleep(100);
    }
}
const _idEnd = new Date();

assert.commandWorked(bulk.execute());

// Test that $$NOW and $$CLUSTER_TIME are available and remain constant across all updated
// documents.
let writeResult =
    assert.commandWorked(coll.update({$where: "sleep(10); return true"},
                                     [{$addFields: {now: "$$NOW", ctime: "$$CLUSTER_TIME"}}],
                                     {multi: true}));

assert.eq(writeResult.nMatched, numDocs);
assert.eq(writeResult.nModified, numDocs);

let results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now instanceof Date);
assert(results[0].ctime instanceof Timestamp);
for (let result of results) {
    assert.eq(result.now, results[0].now);
    assert.eq(result.ctime, results[0].ctime);
}

// Test that $$NOW and $$CLUSTER_TIME advance between updates but remain constant across all
// updates in a given batch.
writeResult = assert.commandWorked(db.runCommand({
    update: coll.getName(),
    updates: [
        {
            q: {$where: "sleep(10); return true"},
            u: [{$addFields: {now2: "$$NOW", ctime2: "$$CLUSTER_TIME"}}],
            multi: true
        },
        {
            q: {$where: "sleep(10); return true"},
            u: [{$addFields: {now3: "$$NOW", ctime3: "$$CLUSTER_TIME"}}],
            multi: true
        }
    ]
}));

assert.eq(writeResult.n, numDocs * 2);
assert.eq(writeResult.nModified, numDocs * 2);

results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now2 instanceof Date);
assert(results[0].ctime2 instanceof Timestamp);
for (let result of results) {
    // The now2 and ctime2 fields are greater than the values from the previous update.
    assert.gt(result.now2, result.now);
    assert.gt(result.ctime2, result.ctime);
    // The now2 and ctime2 fields are the same across all documents.
    assert.eq(result.now2, results[0].now2);
    assert.eq(result.ctime2, results[0].ctime2);
    // The now2 and ctime2 fields are the same as now3 and ctime3 across all documents.
    assert.eq(result.now2, result.now3);
    assert.eq(result.ctime2, result.ctime3);
}

// Test that $$NOW and $$CLUSTER_TIME can be used in the query portion of an update.
const _idMidpoint = new Date(_idStart.getTime() + (_idEnd.getTime() - _idStart.getTime()) / 2);
writeResult =
    assert.commandWorked(coll.update({
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
                                     [{$addFields: {now4: "$$NOW", ctime4: "$$CLUSTER_TIME"}}],
                                     {multi: true}));

assert.lt(writeResult.nMatched, numDocs);
assert.lt(writeResult.nModified, numDocs);

results = coll.find().sort({_id: 1}).toArray();
assert.eq(results.length, numDocs);
assert(results[0].now4 instanceof Date);
assert(results[0].ctime4 instanceof Timestamp);
for (let result of results) {
    if (result._id.getTime() < _idMidpoint.getTime()) {
        assert.eq(result.now4, results[0].now4);
        assert.eq(result.ctime4, results[0].ctime4);
        assert.gt(result.now4, result.now3);
        assert.gt(result.ctime4, result.ctime3);
    } else {
        assert.eq(result.now4, undefined);
        assert.eq(result.ctime4, undefined);
    }
}

// Test that we can explain() an update command that uses $$NOW and $$CLUSTER_TIME.
assert.commandWorked(
    coll.explain().update(
        {
            $expr: {
                $and: [
                    {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                    {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
                ]
            }
        },
        [{$addFields: {explainDoesNotWrite1: "$$NOW", explainDoesNotWrite2: "$$CLUSTER_TIME"}}],
        {multi: true}));

// Test that $$NOW and $$CLUSTER_TIME can be used when issuing updates via the Bulk API, and
// remain constant across all updates within a single bulk operation.
// TODO SERVER-41174: Note that if the bulk update operation exceeds the maximum BSON command
// size, it may issue two or more separate update commands. $$NOW and $$CLUSTER_TIME will be
// constant within each update command, but not across commands.
bulk = coll.initializeUnorderedBulkOp();
bulk.find({$where: "sleep(10); return true"}).update([
    {$addFields: {now5: "$$NOW", ctime5: "$$CLUSTER_TIME"}}
]);
bulk.find({$where: "sleep(10); return true"}).update([
    {$addFields: {now6: "$$NOW", ctime6: "$$CLUSTER_TIME"}}
]);
writeResult = assert.commandWorked(bulk.execute());

assert.eq(writeResult.nMatched, numDocs * 2);
assert.eq(writeResult.nModified, numDocs * 2);

results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now5 instanceof Date);
assert(results[0].ctime5 instanceof Timestamp);
for (let result of results) {
    // The now5 and ctime5 fields are the same across all documents.
    assert.eq(result.now5, results[0].now5);
    assert.eq(result.ctime5, results[0].ctime5);
    // The now5 and ctime5 fields are the same as now6 and ctime6 across all documents.
    assert.eq(result.now5, result.now6);
    assert.eq(result.ctime5, result.ctime6);
}

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify query and update.
let returnedDoc = coll.findAndModify({
    query: {
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
    update: [{$addFields: {nowFAM: "$$NOW", ctimeFAM: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    new: true
});
assert(returnedDoc.nowFAM instanceof Date);
assert(returnedDoc.ctimeFAM instanceof Timestamp);
assert.gt(returnedDoc.nowFAM, returnedDoc.now4);
assert.gt(returnedDoc.ctimeFAM, returnedDoc.ctime4);

results = coll.find({nowFAM: {$exists: true}, ctimeFAM: {$exists: true}}).toArray();
assert.eq(results.length, 1);
assert.docEq(results[0], returnedDoc);

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify upsert.
returnedDoc = coll.findAndModify({
    query: {fieldDoesNotExist: {$exists: true}},
    update: [{$addFields: {_id: "$$NOW", nowFAMUpsert: "$$NOW", ctimeFAMUpsert: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    upsert: true,
    new: true
});
assert(returnedDoc.nowFAMUpsert instanceof Date);
assert(returnedDoc.ctimeFAMUpsert instanceof Timestamp);

assert.eq(coll.find().itcount(), numDocs + 1);
results = coll.find({nowFAMUpsert: {$exists: true}, ctimeFAMUpsert: {$exists: true}}).toArray();
assert.eq(results.length, 1);
assert.docEq(results[0], returnedDoc);

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify delete.
returnedDoc = coll.findAndModify({
    query: {
        nowFAMUpsert: {$exists: true},
        ctimeFAMUpsert: {$exists: true},
        $expr: {
            $and:
                [{$lt: ["$nowFAMUpsert", "$$NOW"]}, {$gt: ["$$CLUSTER_TIME", "$ctimeFAMUpsert"]}]
        }
    },
    sort: {_id: 1},
    remove: true
});
assert.eq(coll.find({nowFAMUpsert: {$exists: true}}).itcount(), 0);
assert.eq(coll.find().itcount(), numDocs);
assert.neq(returnedDoc, null);

// Test that we can explain() a findAndModify command that uses $$NOW and $$CLUSTER_TIME.
assert.commandWorked(coll.explain().findAndModify({
    query: {
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
    update: [{$addFields: {explainDoesNotWrite1: "$$NOW", explainDoesNotWrite2: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    new: true
}));

// Test that we can use $$NOW and $$CLUSTER_TIME in an update via a $merge aggregation. We first
// use $merge to copy the current contents of 'coll' into 'otherColl'.
assert.commandWorked(db.createCollection(otherColl.getName()));
assert.doesNotThrow(
    () => coll.aggregate(
        [{$merge: {into: otherColl.getName(), whenMatched: "fail", whenNotMatched: "insert"}}]));
// Run an aggregation which adds $$NOW and $$CLUSTER_TIME fields into the pipeline document,
// then do the same to the documents in the output collection via a pipeline update.
assert.doesNotThrow(() => coll.aggregate([
    {$addFields: {aggNow: "$$NOW", aggCT: "$$CLUSTER_TIME"}},
    {
        $merge: {
            into: otherColl.getName(),
            let : {aggNow: "$aggNow", aggCT: "$aggCT"},
            whenMatched: [{
                $addFields: {
                    aggNow: "$$aggNow",
                    aggCT: "$$aggCT",
                    mergeNow: "$$NOW",
                    mergeCT: "$$CLUSTER_TIME"
                }
            }],
            whenNotMatched: "fail"
        }
    }
]));
// Verify that the agg pipeline's $$NOW and $$CLUSTER_TIME match the $merge update pipeline's.
results = otherColl.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].mergeNow instanceof Date);
assert(results[0].mergeCT instanceof Timestamp);
for (let result of results) {
    // The mergeNow and mergeCT fields are greater than the values from the previous updates.
    assert.gt(result.mergeNow, result.now5);
    assert.gt(result.mergeCT, result.ctime5);
    // The mergeNow and mergeCT fields are the same across all documents.
    assert.eq(result.mergeNow, results[0].mergeNow);
    assert.eq(result.mergeCT, results[0].mergeCT);
    // The mergeNow and mergeCT fields are the same as aggNow and aggCT across all documents.
    assert.eq(result.mergeNow, result.aggNow);
    assert.eq(result.mergeCT, result.aggCT);
}

rst.stopSet();
}());

/**
 * Tests that the $$NOW and $$CLUSTER_TIME system variables can be used when performing updates on a
 * sharded cluster.
 *
 * The 'requires_sharding' tag prevents the test from running on variants with storage options which
 * cannot support a sharded cluster.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

const st = new ShardingTest({name: jsTestName(), mongos: 1, shards: 2, rs: {nodes: 1}});

const db = st.s.getDB(jsTestName());
const otherColl = db.other;
const coll = db.test;
otherColl.drop();
coll.drop();

// Enable sharding on the test DB and ensure its primary is shard0.
assert.commandWorked(db.adminCommand({enableSharding: db.getName()}));
st.ensurePrimaryShard(db.getName(), st.shard0.shardName);

// Create a sharded collection on {shard: 1}, split across the cluster at {shard: 1}. Do this
// for both 'coll' and 'otherColl' so that the latter can be used for $merge tests later.
for (let collToShard of [coll, otherColl]) {
    st.shardColl(collToShard, {shard: 1}, {shard: 1}, {shard: 1});
}

// Insert N docs, with the _id field set to the current Date. Sleep for a short period between
// insertions, such that the Date value increases for each successive document. We additionally
// ensure that the insertions alternate between the two shards by setting the shard key to
// either 0 or 1.
let bulk = coll.initializeUnorderedBulkOp();
const _idStart = new Date();
const numDocs = 10;
for (let i = 0; i < numDocs; ++i) {
    bulk.insert({_id: new Date(), insertClusterTime: new Timestamp(0, 0), shard: (i % 2)});
    if (i < numDocs - 1) {
        sleep(100);
    }
}
const _idEnd = new Date();

assert.commandWorked(bulk.execute());

// Test that we cannot issue an update to mongoS with runtime constants already present.
assert.commandFailedWithCode(db.runCommand({
    update: coll.getName(),
    updates: [{q: {}, u: {$set: {operationFailsBeforeApplyingUpdates: true}}}],
    runtimeConstants: {localNow: new Date(), clusterTime: new Timestamp(0, 0)}
}),
                             51195);

// Test that $$NOW and $$CLUSTER_TIME are available and remain constant across all updated
// documents.
let writeResult =
    assert.commandWorked(coll.update({$where: "sleep(10); return true"},
                                     [{$addFields: {now: "$$NOW", ctime: "$$CLUSTER_TIME"}}],
                                     {multi: true}));

assert.eq(writeResult.nMatched, numDocs);
assert.eq(writeResult.nModified, numDocs);

let results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now instanceof Date);
assert(results[0].ctime instanceof Timestamp);
for (let result of results) {
    assert.eq(result.now, results[0].now);
    assert.eq(result.ctime, results[0].ctime);
}

// Test that $$NOW and $$CLUSTER_TIME advance between updates but remain constant across all
// updates in a given batch.
writeResult = assert.commandWorked(db.runCommand({
    update: coll.getName(),
    updates: [
        {
            q: {$where: "sleep(10); return true"},
            u: [{$addFields: {now2: "$$NOW", ctime2: "$$CLUSTER_TIME"}}],
            multi: true
        },
        {
            q: {$where: "sleep(10); return true"},
            u: [{$addFields: {now3: "$$NOW", ctime3: "$$CLUSTER_TIME"}}],
            multi: true
        }
    ]
}));

assert.eq(writeResult.n, numDocs * 2);
assert.eq(writeResult.nModified, numDocs * 2);

results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now2 instanceof Date);
assert(results[0].ctime2 instanceof Timestamp);
for (let result of results) {
    // The now2 and ctime2 fields are greater than the values from the previous update.
    assert.gt(result.now2, result.now);
    assert.gt(result.ctime2, result.ctime);
    // The now2 and ctime2 fields are the same across all documents.
    assert.eq(result.now2, results[0].now2);
    assert.eq(result.ctime2, results[0].ctime2);
    // The now2 and ctime2 fields are the same as now3 and ctime3 across all documents.
    assert.eq(result.now2, result.now3);
    assert.eq(result.ctime2, result.ctime3);
}

// Test that $$NOW and $$CLUSTER_TIME can be used in the query portion of an update.
const _idMidpoint = new Date(_idStart.getTime() + (_idEnd.getTime() - _idStart.getTime()) / 2);
writeResult =
    assert.commandWorked(coll.update({
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
                                     [{$addFields: {now4: "$$NOW", ctime4: "$$CLUSTER_TIME"}}],
                                     {multi: true}));

assert.lt(writeResult.nMatched, numDocs);
assert.lt(writeResult.nModified, numDocs);

results = coll.find().sort({_id: 1}).toArray();
assert.eq(results.length, numDocs);
assert(results[0].now4 instanceof Date);
assert(results[0].ctime4 instanceof Timestamp);
for (let result of results) {
    if (result._id.getTime() < _idMidpoint.getTime()) {
        assert.eq(result.now4, results[0].now4);
        assert.eq(result.ctime4, results[0].ctime4);
        assert.gt(result.now4, result.now3);
        assert.gt(result.ctime4, result.ctime3);
    } else {
        assert.eq(result.now4, undefined);
        assert.eq(result.ctime4, undefined);
    }
}

// Test that we can explain() an update command that uses $$NOW and $$CLUSTER_TIME.
assert.commandWorked(
    coll.explain().update(
        {
            $expr: {
                $and: [
                    {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                    {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
                ]
            }
        },
        [{$addFields: {explainDoesNotWrite1: "$$NOW", explainDoesNotWrite2: "$$CLUSTER_TIME"}}],
        {multi: true}));

// Test that $$NOW and $$CLUSTER_TIME can be used when issuing updates via the Bulk API, and
// remain constant across all updates within a single bulk operation.
// TODO SERVER-41174: Note that if the bulk update operation exceeds the maximum BSON command
// size, it may issue two or more separate update commands. $$NOW and $$CLUSTER_TIME will be
// constant within each update command, but not across commands.
bulk = coll.initializeUnorderedBulkOp();
bulk.find({$where: "sleep(10); return true"}).update([
    {$addFields: {now5: "$$NOW", ctime5: "$$CLUSTER_TIME"}}
]);
bulk.find({$where: "sleep(10); return true"}).update([
    {$addFields: {now6: "$$NOW", ctime6: "$$CLUSTER_TIME"}}
]);
writeResult = assert.commandWorked(bulk.execute());

assert.eq(writeResult.nMatched, numDocs * 2);
assert.eq(writeResult.nModified, numDocs * 2);

results = coll.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].now5 instanceof Date);
assert(results[0].ctime5 instanceof Timestamp);
for (let result of results) {
    // The now5 and ctime5 fields are the same across all documents.
    assert.eq(result.now5, results[0].now5);
    assert.eq(result.ctime5, results[0].ctime5);
    // The now5 and ctime5 fields are the same as now6 and ctime6 across all documents.
    assert.eq(result.now5, result.now6);
    assert.eq(result.ctime5, result.ctime6);
}

// Test that we cannot issue a findAndModify to mongoS with runtime constants already present.
assert.commandFailedWithCode(db.runCommand({
    findAndModify: coll.getName(),
    query: {},
    update: {$set: {operationFailsBeforeApplyingUpdates: true}},
    runtimeConstants: {localNow: new Date(), clusterTime: new Timestamp(0, 0)}
}),
                             51196);

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify query and update.
let returnedDoc = coll.findAndModify({
    query: {
        shard: 0,
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
    update: [{$addFields: {nowFAM: "$$NOW", ctimeFAM: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    new: true
});
assert(returnedDoc.nowFAM instanceof Date);
assert(returnedDoc.ctimeFAM instanceof Timestamp);
assert.gt(returnedDoc.nowFAM, returnedDoc.now4);
assert.gt(returnedDoc.ctimeFAM, returnedDoc.ctime4);

results = coll.find({nowFAM: {$exists: true}, ctimeFAM: {$exists: true}}).toArray();
assert.eq(results.length, 1);
assert.docEq(results[0], returnedDoc);

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify upsert.
returnedDoc = coll.findAndModify({
    query: {shard: 0, fieldDoesNotExist: {$exists: true}},
    update: [{$addFields: {_id: "$$NOW", nowFAMUpsert: "$$NOW", ctimeFAMUpsert: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    upsert: true,
    new: true
});
assert(returnedDoc.nowFAMUpsert instanceof Date);
assert(returnedDoc.ctimeFAMUpsert instanceof Timestamp);

assert.eq(coll.find().itcount(), numDocs + 1);
results = coll.find({nowFAMUpsert: {$exists: true}, ctimeFAMUpsert: {$exists: true}}).toArray();
assert.eq(results.length, 1);
assert.docEq(results[0], returnedDoc);

// Test that $$NOW and $$CLUSTER_TIME can be used in a findAndModify delete.
returnedDoc = coll.findAndModify({
    query: {
        shard: 0,
        nowFAMUpsert: {$exists: true},
        ctimeFAMUpsert: {$exists: true},
        $expr: {
            $and:
                [{$lt: ["$nowFAMUpsert", "$$NOW"]}, {$gt: ["$$CLUSTER_TIME", "$ctimeFAMUpsert"]}]
        }
    },
    sort: {_id: 1},
    remove: true
});
assert.eq(coll.find({nowFAMUpsert: {$exists: true}}).itcount(), 0);
assert.eq(coll.find().itcount(), numDocs);
assert.neq(returnedDoc, null);

// Test that we can explain() a findAndModify command that uses $$NOW and $$CLUSTER_TIME.
assert.commandWorked(coll.explain().findAndModify({
    query: {
        shard: 0,
        $expr: {
            $and: [
                {$lt: ["$_id", {$min: [_idMidpoint, "$$NOW"]}]},
                {$gt: ["$$CLUSTER_TIME", "$insertClusterTime"]}
            ]
        }
    },
    update: [{$addFields: {explainDoesNotWrite1: "$$NOW", explainDoesNotWrite2: "$$CLUSTER_TIME"}}],
    sort: {_id: 1},
    new: true
}));

// Test that we can use $$NOW and $$CLUSTER_TIME in an update via a $merge aggregation. We first
// use $merge to copy the current contents of 'coll' into 'otherColl'.
assert.doesNotThrow(
    () => coll.aggregate(
        [{$merge: {into: otherColl.getName(), whenMatched: "fail", whenNotMatched: "insert"}}]));
// Run an aggregation which adds $$NOW and $$CLUSTER_TIME fields into the pipeline document,
// then do the same to the documents in the output collection via a pipeline update.
assert.doesNotThrow(() => coll.aggregate([
    {$addFields: {aggNow: "$$NOW", aggCT: "$$CLUSTER_TIME"}},
    {
        $merge: {
            into: otherColl.getName(),
            let : {aggNow: "$aggNow", aggCT: "$aggCT"},
            whenMatched: [{
                $addFields: {
                    aggNow: "$$aggNow",
                    aggCT: "$$aggCT",
                    mergeNow: "$$NOW",
                    mergeCT: "$$CLUSTER_TIME"
                }
            }],
            whenNotMatched: "fail"
        }
    }
]));
// Verify that the agg pipeline's $$NOW and $$CLUSTER_TIME match the $merge update pipeline's.
results = otherColl.find().toArray();
assert.eq(results.length, numDocs);
assert(results[0].mergeNow instanceof Date);
assert(results[0].mergeCT instanceof Timestamp);
for (let result of results) {
    // The mergeNow and mergeCT fields are greater than the values from the previous updates.
    assert.gt(result.mergeNow, result.now5);
    assert.gt(result.mergeCT, result.ctime5);
    // The mergeNow and mergeCT fields are the same across all documents.
    assert.eq(result.mergeNow, results[0].mergeNow);
    assert.eq(result.mergeCT, results[0].mergeCT);
    // The mergeNow and mergeCT fields are the same as aggNow and aggCT across all documents.
    assert.eq(result.mergeNow, result.aggNow);
    assert.eq(result.mergeCT, result.aggCT);
}

st.stop();
}());

// Verify that the update system correctly rejects invalid entries during post-image validation.
// @tags: [
// ]
(function() {
"use strict";

const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod was unable to start up");

const testDB = conn.getDB("test");

// Test validation of elements added to an array that is represented in a "deserialized" format
// in mutablebson. The added element is valid.
assert.commandWorked(testDB.coll.insert({_id: 0, a: []}));
assert.commandWorked(
    testDB.coll.update({_id: 0}, {$set: {"a.1": 0, "a.0": {$ref: "coll", $db: "test"}}}));
assert.docEq(testDB.coll.findOne({_id: 0}), {_id: 0, a: [{$ref: "coll", $db: "test"}, 0]});

// Test validation of modified array elements that are accessed using a string that is
// numerically equivalent to their fieldname. The modified element is valid.
assert.commandWorked(testDB.coll.insert({_id: 1, a: [0]}));
assert.commandWorked(testDB.coll.update({_id: 1}, {$set: {"a.00": {$ref: "coll", $db: "test"}}}));
assert.docEq(testDB.coll.findOne({_id: 1}), {_id: 1, a: [{$ref: "coll", $db: "test"}]});

MongoRunner.stopMongod(conn);
}());

var db;
(function() {
"use strict";
const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start.");
db = conn.getDB("test");

const t = db.foo;
t.drop();

const N = 10000;

var bulk = t.initializeUnorderedBulkOp();
for (let i = 0; i < N; i++) {
    bulk.insert({_id: i, x: 1});
}
assert.commandWorked(bulk.execute());

const join = startParallelShell(
    "while( db.foo.findOne( { _id : 0 } ).x == 1 ); db.foo.createIndex( { x : 1 } );");

assert.commandWorked(t.update({
    $where: function() {
        sleep(1);
        return true;
    }
},
                              {$set: {x: 5}},
                              false,
                              true));

join();

assert.eq(N, t.find({x: 5}).count());

MongoRunner.stopMongod(conn);
})();

/**
 * When two concurrent identical upsert operations are performed, for which a unique index exists on
 * the query values, it is possible that they will both attempt to perform an insert with one of
 * the two failing on the unique index constraint. This test confirms that the failed insert will be
 * retried, resulting in an update.
 *
 * @tags: [requires_replication]
 */

(function() {
"use strict";

load("jstests/libs/curop_helpers.js");  // For waitForCurOpByFailPoint().

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const testDB = rst.getPrimary().getDB("test");
const adminDB = testDB.getSiblingDB("admin");
const collName = "upsert_duplicate_key_retry";
const testColl = testDB.getCollection(collName);

testDB.runCommand({drop: collName});

function performUpsert() {
    // This function is called from startParallelShell(), so closed-over variables will not be
    // available. We must re-obtain the value of 'testColl' in the function body.
    const testColl = db.getMongo().getDB("test").getCollection("upsert_duplicate_key_retry");
    assert.commandWorked(testColl.update({x: 3}, {$inc: {y: 1}}, {upsert: true}));
}

assert.commandWorked(testColl.createIndex({x: 1}, {unique: true}));

// Will hang upsert operations just prior to performing an insert.
assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "hangBeforeUpsertPerformsInsert", mode: "alwaysOn"}));

const awaitUpdate1 = startParallelShell(performUpsert, rst.ports[0]);
const awaitUpdate2 = startParallelShell(performUpsert, rst.ports[0]);

// Query current operations until 2 matching operations are found.
assert.soon(() => {
    const curOps = waitForCurOpByFailPointNoNS(adminDB, "hangBeforeUpsertPerformsInsert");
    return curOps.length === 2;
});

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: "hangBeforeUpsertPerformsInsert", mode: "off"}));

awaitUpdate1();
awaitUpdate2();

const cursor = testColl.find({}, {_id: 0});
assert.eq(cursor.next(), {x: 3, y: 2});
assert(!cursor.hasNext(), cursor.toArray());

// Confirm that oplog entries exist for both insert and update operation.
const oplogColl = testDB.getSiblingDB("local").getCollection("oplog.rs");
assert.eq(1, oplogColl.find({"op": "i", "ns": "test.upsert_duplicate_key_retry"}).itcount());
assert.eq(1, oplogColl.find({"op": "u", "ns": "test.upsert_duplicate_key_retry"}).itcount());

//
// Confirm DuplicateKey error for cases that should not be retried.
//
assert.commandWorked(testDB.runCommand({drop: collName}));
assert.commandWorked(testColl.createIndex({x: 1}, {unique: true}));

// DuplicateKey error on replacement-style upsert, where the unique index key value to be
// written does not match the value of the query predicate.
assert.commandWorked(testColl.createIndex({x: 1}, {unique: true}));
assert.commandWorked(testColl.insert({_id: 1, 'a': 12345}));
assert.commandFailedWithCode(testColl.update({x: 3}, {}, {upsert: true}), ErrorCodes.DuplicateKey);

// DuplicateKey error on update-style upsert, where the unique index key value to be written
// does not match the value of the query predicate.
assert.commandWorked(testColl.remove({}));
assert.commandWorked(testColl.insert({x: 3}));
assert.commandWorked(testColl.insert({x: 4}));
assert.commandFailedWithCode(testColl.update({x: 3}, {$inc: {x: 1}}, {upsert: true}),
                             ErrorCodes.DuplicateKey);

rst.stopSet();
})();

/**
 * When two concurrent identical upsert operations are performed, for which a unique index exists on
 * the query values, it is possible that they will both attempt to perform an insert with one of
 * the two failing on the unique index constraint. This test confirms that the failed insert will be
 * retried, resulting in an update.
 *
 * @tags: [requires_replication]
 */

(function() {
"use strict";

load("jstests/libs/curop_helpers.js");  // For waitForCurOpByFailPoint().

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const testDB = rst.getPrimary().getDB("test");
const adminDB = testDB.getSiblingDB("admin");
const collName = "upsert_duplicate_key_retry_findAndModify";
const testColl = testDB.getCollection(collName);

testDB.runCommand({drop: collName});

function performUpsert() {
    // This function is called from startParallelShell(), so closed-over variables will not be
    // available. We must re-obtain the value of 'testColl' in the function body.
    const testColl =
        db.getMongo().getDB("test").getCollection("upsert_duplicate_key_retry_findAndModify");
    testColl.findAndModify({query: {x: 3}, update: {$inc: {y: 1}}, upsert: true});
}

assert.commandWorked(testColl.createIndex({x: 1}, {unique: true}));

// Will hang upsert operations just prior to performing an insert.
assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: "hangBeforeFindAndModifyPerformsUpdate", mode: "alwaysOn"}));

const awaitUpdate1 = startParallelShell(performUpsert, rst.ports[0]);
const awaitUpdate2 = startParallelShell(performUpsert, rst.ports[0]);

// Query current operations until 2 matching operations are found.
assert.soon(() => {
    const curOps = waitForCurOpByFailPointNoNS(adminDB, "hangBeforeFindAndModifyPerformsUpdate");
    return curOps.length === 2;
});

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: "hangBeforeFindAndModifyPerformsUpdate", mode: "off"}));

awaitUpdate1();
awaitUpdate2();

const cursor = testColl.find({}, {_id: 0});
assert.eq(cursor.next(), {x: 3, y: 2});
assert(!cursor.hasNext(), cursor.toArray());

// Confirm that oplog entries exist for both insert and update operation.
const oplogColl = testDB.getSiblingDB("local").getCollection("oplog.rs");
assert.eq(
    1,
    oplogColl.find({"op": "i", "ns": "test.upsert_duplicate_key_retry_findAndModify"}).itcount());
assert.eq(
    1,
    oplogColl.find({"op": "u", "ns": "test.upsert_duplicate_key_retry_findAndModify"}).itcount());

//
// Confirm DuplicateKey error for cases that should not be retried.
//
assert.commandWorked(testDB.runCommand({drop: collName}));
assert.commandWorked(testColl.createIndex({x: 1}, {unique: true}));

// DuplicateKey error on replacement-style upsert, where the unique index key value to be
// written does not match the value of the query predicate.
assert.commandWorked(testColl.insert({_id: 1, 'a': 12345}));
assert.throws(function() {
    testColl.findAndModify({query: {x: 3}, update: {}, upsert: true});
}, []);

// DuplicateKey error on update-style upsert, where the unique index key value to be written
// does not match the value of the query predicate.
assert.commandWorked(testColl.remove({}));
assert.commandWorked(testColl.insert({x: 3}));
assert.commandWorked(testColl.insert({x: 4}));
assert.throws(function() {
    testColl.findAndModify({query: {x: 3}, update: {$inc: {x: 1}}, upsert: true});
}, []);

rst.stopSet();
})();

// @tags: [
//   does_not_support_stepdowns,
//   requires_profiling,
//   requires_sharding,
// ]

// Confirms that profiled aggregation execution contains expected values for usedDisk.

(function() {
"use strict";

// For getLatestProfilerEntry and getProfilerProtocolStringForCommand
load("jstests/libs/profiler.js");
const conn = MongoRunner.runMongod({setParameter: "maxBSONDepth=8"});
const testDB = conn.getDB("profile_agg");
const coll = testDB.getCollection("test");

testDB.setProfilingLevel(2);

function resetCollection() {
    coll.drop();
    for (var i = 0; i < 10; ++i) {
        assert.commandWorked(coll.insert({a: i}));
    }
}
function resetForeignCollection() {
    testDB.foreign.drop();
    const forColl = testDB.getCollection("foreign");
    for (var i = 4; i < 18; i += 2)
        assert.commandWorked(forColl.insert({b: i}));
}
//
// Confirm hasSortStage with in-memory sort.
//
resetCollection();
//
// Confirm 'usedDisk' is not set if 'allowDiskUse' is set but no stages need to use disk.
//
coll.aggregate([{$match: {a: {$gte: 2}}}], {allowDiskUse: true});
var profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.hasOwnProperty("usedDisk"), tojson(profileObj));

resetCollection();
coll.aggregate([{$match: {a: {$gte: 2}}}, {$sort: {a: 1}}], {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.hasOwnProperty("usedDisk"), tojson(profileObj));
assert.eq(profileObj.hasSortStage, true, tojson(profileObj));

assert.commandWorked(
    testDB.adminCommand({setParameter: 1, internalQueryMaxBlockingSortMemoryUsageBytes: 10}));
assert.eq(
    8, coll.aggregate([{$match: {a: {$gte: 2}}}, {$sort: {a: 1}}], {allowDiskUse: true}).itcount());
profileObj = getLatestProfilerEntry(testDB);

assert.eq(profileObj.usedDisk, true, tojson(profileObj));
assert.eq(profileObj.hasSortStage, true, tojson(profileObj));

//
// Confirm that disk use is correctly detected for the $facet stage.
//
resetCollection();
coll.aggregate([{$facet: {"aSort": [{$sortByCount: "$a"}]}}], {allowDiskUse: true});

profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Confirm that usedDisk is correctly detected for the $group stage.
//
resetCollection();

coll.aggregate([{$group: {"_id": {$avg: "$a"}}}], {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.hasOwnProperty("usedDisk"), tojson(profileObj));

assert.commandWorked(
    testDB.adminCommand({setParameter: 1, internalDocumentSourceGroupMaxMemoryBytes: 10}));
resetCollection();
coll.aggregate([{$group: {"_id": {$avg: "$a"}}}], {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Confirm that usedDisk is correctly detected for the $lookup stage with a subsequent $unwind.
//
resetCollection();
resetForeignCollection();
coll.aggregate(
    [
        {$lookup: {let : {var1: "$a"}, pipeline: [{$sort: {a: 1}}], from: "foreign", as: "same"}},
        {$unwind: "$same"}
    ],
    {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Confirm that usedDisk is correctly detected for the $lookup stage without a subsequent
// $unwind.
//
resetCollection();
resetForeignCollection();
coll.aggregate(
    [{$lookup: {let : {var1: "$a"}, pipeline: [{$sort: {a: 1}}], from: "foreign", as: "same"}}],
    {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Confirm that usedDisk is correctly detected when $limit is set after the $lookup stage.
//
resetCollection();
resetForeignCollection();
coll.aggregate(
    [
        {$lookup: {let : {var1: "$a"}, pipeline: [{$sort: {a: 1}}], from: "foreign", as: "same"}},
        {$unwind: "$same"},
        {$limit: 3}
    ],
    {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Confirm that usedDisk is correctly detected when $limit is set before the $lookup stage.
//
resetCollection();
resetForeignCollection();
coll.aggregate(
    [
        {$limit: 1},
        {$lookup: {let : {var1: "$a"}, pipeline: [{$sort: {a: 1}}], from: "foreign", as: "same"}},
        {$unwind: "$same"}
    ],
    {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Test that usedDisk is not set for a $lookup with a pipeline that does not use disk.
//
assert.commandWorked(testDB.adminCommand(
    {setParameter: 1, internalQueryMaxBlockingSortMemoryUsageBytes: 100 * 1024 * 1024}));
resetCollection();
resetForeignCollection();
coll.aggregate(
    [{$lookup: {let : {var1: "$a"}, pipeline: [{$sort: {a: 1}}], from: "otherTest", as: "same"}}],
    {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.hasOwnProperty("usedDisk"), tojson(profileObj));

//
// Test that aggregate command fails when 'allowDiskUse:false' because of insufficient available
// memory to perform group.
//
assert.throws(() => coll.aggregate(
                  [{$unionWith: {coll: "foreign", pipeline: [{$group: {"_id": {$avg: "$b"}}}]}}],
                  {allowDiskUse: false}));

//
// Test that the above command succeeds with 'allowDiskUse:true'. 'usedDisk' is correctly detected
// when a sub-pipeline of $unionWith stage uses disk.
//
resetCollection();
resetForeignCollection();
coll.aggregate([{$unionWith: {coll: "foreign", pipeline: [{$group: {"_id": {$avg: "$b"}}}]}}],
               {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert.eq(profileObj.usedDisk, true, tojson(profileObj));

//
// Test that usedDisk is not set for a $unionWith with a sub-pipeline that does not use disk.
//
coll.aggregate([{$unionWith: {coll: "foreign", pipeline: [{$sort: {b: 1}}]}}],
               {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.usedDisk, tojson(profileObj));

coll.aggregate([{$unionWith: {coll: "foreign", pipeline: [{$match: {a: 1}}]}}],
               {allowDiskUse: true});
profileObj = getLatestProfilerEntry(testDB);
assert(!profileObj.usedDisk, tojson(profileObj));

MongoRunner.stopMongod(conn);

//
// Tests on a sharded cluster.
//
const st = new ShardingTest({shards: 2});
const shardedDB = st.s.getDB(jsTestName());
const shardedSourceColl = shardedDB.coll1;
const shardedForeignColl = shardedDB.coll2;

const shard0DB = st.shard0.getDB(jsTestName());
const shard1DB = st.shard1.getDB(jsTestName());

assert.commandWorked(st.s0.adminCommand({enableSharding: shardedDB.getName()}));
st.ensurePrimaryShard(shardedDB.getName(), st.shard0.shardName);

// Shard 'shardedSourceColl' and 'shardedForeignColl' on {x:1}, split it at {x:0}, and move
// chunk {x:0} to shard1.
st.shardColl(shardedSourceColl, {x: 1}, {x: 0}, {x: 0});
st.shardColl(shardedForeignColl, {x: 1}, {x: 0}, {x: 0});

// Insert few documents on each shard.
for (let i = 0; i < 10; ++i) {
    assert.commandWorked(shardedSourceColl.insert({x: i}));
    assert.commandWorked(shardedSourceColl.insert({x: -i}));
    assert.commandWorked(shardedForeignColl.insert({x: i}));
    assert.commandWorked(shardedForeignColl.insert({x: -i}));
    assert.commandWorked(shardedDB.unshardedColl.insert({x: i}));
}

// Restart profiler.
function restartProfiler() {
    for (let shardDB of [shard0DB, shard1DB]) {
        shardDB.setProfilingLevel(0);
        shardDB.system.profile.drop();

        // Enable profiling and changes the 'slowms' threshold to -1ms. This will log all the
        // commands.
        shardDB.setProfilingLevel(2, -1);
    }
}

assert.commandWorked(
    shard0DB.adminCommand({setParameter: 1, internalDocumentSourceGroupMaxMemoryBytes: 10}));
restartProfiler();
// Test that 'usedDisk' doesn't get populated on the profiler entry of the base pipeline, when the
// $unionWith'd pipeline needs to use disk on a sharded collection.
assert.commandWorked(shardedDB.runCommand({
    aggregate: shardedSourceColl.getName(),
    pipeline: [{
        $unionWith:
            {coll: shardedForeignColl.getName(), pipeline: [{$group: {"_id": {$avg: "$x"}}}]}
    }],
    cursor: {},
    allowDiskUse: true,
}));
// Verify that the $unionWith'd pipeline always has the profiler entry.
profilerHasSingleMatchingEntryOrThrow({
    profileDB: shard0DB,
    filter:
        {'command.getMore': {$exists: true}, usedDisk: true, ns: shardedForeignColl.getFullName()}
});

// If the $mergeCursor is ran on the shard0DB, then the profiler entry should have the 'usedDisk'
// set.
if (shard0DB.system.profile
        .find({
            ns: shardedSourceColl.getFullName(),
            'command.pipeline.$mergeCursors': {$exists: true}
        })
        .itcount() > 0) {
    profilerHasSingleMatchingEntryOrThrow({
        profileDB: shard0DB,
        filter: {
            'command.pipeline.$mergeCursors': {$exists: true},
            usedDisk: true,
            ns: shardedSourceColl.getFullName()
        }
    });
    profilerHasZeroMatchingEntriesOrThrow(
        {profileDB: shard1DB, filter: {usedDisk: true, ns: shardedSourceColl.getFullName()}});
} else {
    // If the $mergeCursors is ran on mongos or shard1DB, then the profiler shouldn't have the
    // 'usedDisk' set.
    profilerHasZeroMatchingEntriesOrThrow(
        {profileDB: shard0DB, filter: {usedDisk: true, ns: shardedSourceColl.getFullName()}});
    profilerHasZeroMatchingEntriesOrThrow(
        {profileDB: shard1DB, filter: {usedDisk: true, ns: shardedSourceColl.getFullName()}});
}

// Verify that the 'usedDisk' is always set correctly on base pipeline.
restartProfiler();
assert.commandWorked(shardedDB.runCommand({
    aggregate: shardedSourceColl.getName(),
    pipeline: [
        {$group: {"_id": {$avg: "$x"}}},
        {$unionWith: {coll: shardedForeignColl.getName(), pipeline: []}}
    ],
    cursor: {},
    allowDiskUse: true,
}));
profilerHasSingleMatchingEntryOrThrow({
    profileDB: shard0DB,
    filter:
        {'command.getMore': {$exists: true}, usedDisk: true, ns: shardedSourceColl.getFullName()}
});
profilerHasZeroMatchingEntriesOrThrow(
    {profileDB: shard0DB, filter: {usedDisk: true, ns: shardedForeignColl.getFullName()}});

// Set the 'internalDocumentSourceGroupMaxMemoryBytes' to a higher value so that st.stop()
// doesn't fail.
assert.commandWorked(shard0DB.adminCommand(
    {setParameter: 1, internalDocumentSourceGroupMaxMemoryBytes: 100 * 1024 * 1024}));

st.stop();
})();

/**
 * Test that verifies mongod can start using paths that contain UTF-8 characters that are not ASCII.
 */
(function() {
'use strict';
var db_name = "ελληνικά";
var path = MongoRunner.dataPath + "Росси́я";

mkdir(path);

// Test MongoD
let testMongoD = function() {
    let options = {
        dbpath: path,
        useLogFiles: true,
        pidfilepath: path + "/pidfile",
    };

    // directoryperdb is only supported with the wiredTiger storage engine
    if (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger") {
        options["directoryperdb"] = "";
    }

    let conn = MongoRunner.runMongod(options);
    assert.neq(null, conn, 'mongod was unable to start up');

    let coll = conn.getCollection(db_name + ".foo");
    assert.commandWorked(coll.insert({_id: 1}));

    MongoRunner.stopMongod(conn);
};

testMongoD();

// Start a second time to test things like log rotation.
testMongoD();
})();

/**
 * Tests foreground validation's ability to fix up allowable multikey metadata problems.
 */
(function() {
load("jstests/libs/analyze_plan.js");  // For getWinningPlan to analyze explain() output.

const conn = MongoRunner.runMongod();
const dbName = jsTestName();
const collName = 'test';
const db = conn.getDB(dbName);

const assertValidate = (coll, assertFn) => {
    let res = assert.commandWorked(coll.validate());
    assertFn(res);
};

const assertIndexMultikey = (coll, hint, expectMultikey) => {
    const explain = coll.find().hint(hint).explain();
    const plan = getWinningPlan(explain.queryPlanner);
    assert.eq("FETCH", plan.stage, explain);
    assert.eq("IXSCAN", plan.inputStage.stage, explain);
    assert.eq(expectMultikey,
              plan.inputStage.isMultiKey,
              `Index multikey state "${plan.inputStage.isMultiKey}" was not "${expectMultikey}"`);
};

const runTest = (testCase) => {
    db[collName].drop();
    db.createCollection(collName);
    testCase(db[collName]);
};

// Test that validate will modify an index's multikey paths if they change.
runTest((coll) => {
    // Create an index, make the index multikey on 'a', and expect normal validation behavior.
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));
    assert.commandWorked(coll.insert({_id: 1, a: [0, 1]}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(!res.repaired);
        assert.eq(0, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, {a: 1, b: 1}, true);

    // Insert a document that makes the index multikey on 'b', and remove the document that
    // makes the index multikey on 'a'. Expect repair to adjust the paths.
    assert.commandWorked(coll.insert({_id: 2, b: [0, 1]}));
    assert.commandWorked(coll.remove({_id: 1}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(res.repaired);
        assert.eq(1, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, {a: 1, b: 1}, true);
});

// Test that validate will unset an index's multikey flag if it no longer has multikey documents.
runTest((coll) => {
    // Create an index, make the index multikey on 'a', and expect normal validation behavior.
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));
    assert.commandWorked(coll.insert({_id: 1, a: [0, 1]}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(!res.repaired);
        assert.eq(0, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, {a: 1, b: 1}, true);

    // Insert a document, remove the document that makes the index multikey. Expect repair to
    // unset the multikey flag.
    assert.commandWorked(coll.insert({_id: 2, a: 1, b: 1}));
    assert.commandWorked(coll.remove({_id: 1}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(res.repaired);
        assert.eq(1, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, {a: 1, b: 1}, false);
});

// Test that validate will unset the multikey flag for an index that doesn't track path-level
// metadata.
runTest((coll) => {
    // Create an index, make the index multikey on 'a', and expect normal validation behavior.
    assert.commandWorked(coll.createIndex({a: 'text'}));
    assert.commandWorked(coll.insert({_id: 1, a: 'hello world'}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(!res.repaired);
        assert.eq(0, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, 'a_text', true);

    // Insert a document, remove the document that makes the index multikey. Expect repair to
    // unset the multikey flag.
    assert.commandWorked(coll.insert({_id: 2, a: 'test'}));
    assert.commandWorked(coll.remove({_id: 1}));
    assertValidate(coll, (res) => {
        assert(res.valid);
        assert(res.repaired);
        assert.eq(1, res.warnings.length);
        assert.eq(0, res.errors.length);
    });
    assertIndexMultikey(coll, 'a_text', false);
});

MongoRunner.stopMongod(conn);
})();

/**
 * Tests the validateDBMetaData commands when running on the entire cluster.
 * @tags: [
 *   requires_sharding,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/fixture_helpers.js");             // For FixtureHelpers.
load("jstests/core/timeseries/libs/timeseries.js");  // For TimeseriesTest.

const dbName = jsTestName();
const collName = "coll1";

function runTest(conn) {
    const testDB = conn.getDB(dbName);
    assert.commandWorked(testDB.dropDatabase());
    const coll1 = testDB.coll1;
    const coll2 = testDB.coll2;

    function validate({dbName, coll, apiStrict, error}) {
        dbName = dbName ? dbName : null;
        coll = coll ? coll : null;
        const res = assert.commandWorked(testDB.runCommand({
            validateDBMetadata: 1,
            db: dbName,
            collection: coll,
            apiParameters: {version: "1", strict: apiStrict}
        }));

        assert(res.apiVersionErrors);
        const foundError = res.apiVersionErrors.length > 0;

        // Verify that 'apiVersionErrors' is not empty when 'error' is true, and vice versa.
        assert((!error && !foundError) || (error && foundError), res);

        if (error) {
            for (let apiError of res.apiVersionErrors) {
                assert(apiError.ns);
                if (error.code) {
                    assert.eq(apiError.code, error.code);
                }

                if (FixtureHelpers.isMongos(testDB)) {
                    // Check that every error has an additional 'shard' field on sharded clusters.
                    assert(apiError.shard);
                }
            }
        }
    }

    validate({apiStrict: true});

    //
    // Tests for indexes.
    //
    assert.commandWorked(coll1.createIndex({p: "text"}));

    validate({apiStrict: true, error: {code: ErrorCodes.APIStrictError}});

    //
    // Tests for views.
    //
    assert.commandWorked(coll1.dropIndexes());
    validate({apiStrict: true});

    // Create a view which uses unstable expression and verify that validateDBMetadata commands
    // throws an assertion.
    const viewName = "view1";
    const view = testDB.createView(
        viewName, coll2.getName(), [{$project: {v: {$_testApiVersion: {unstable: true}}}}]);

    validate({apiStrict: true, error: {code: ErrorCodes.APIStrictError}});
    validate({apiStrict: false});

    //
    // Tests for validator.
    //
    assert.commandWorked(testDB.dropDatabase());

    const validatorCollName = "validator";
    assert.commandWorked(testDB.createCollection(
        validatorCollName, {validator: {$expr: {$_testApiVersion: {unstable: true}}}}));

    validate({apiStrict: true, error: {code: ErrorCodes.APIStrictError}});

    assert.commandWorked(testDB.runCommand({drop: validatorCollName}));
}

const conn = MongoRunner.runMongod();
runTest(conn);
MongoRunner.stopMongod(conn);

const st = new ShardingTest({shards: 2});
st.shardColl(dbName + "." + collName, {_id: 1}, {_id: 1});
runTest(st.s);
st.stop();
}());

/**
 * Tests to verify that the validateDBMetadata command returns response correctly when the expected
 * output data is larger than the max BSON size.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();
const testDB = conn.getDB("validate_db_metadaba");
const coll = testDB.getCollection("test");

for (let i = 0; i < 100; i++) {
    // Create a large index name. As the index name is returned in the output validateDBMetadata
    // command, it can cause the output size to exceed max BSON size.
    let largeName = "a".repeat(200000);
    assert.commandWorked(testDB.runCommand(
        {createIndexes: "test" + i, indexes: [{key: {p: 1}, name: largeName, sparse: true}]}));
}

const res = assert.commandWorked(
    testDB.runCommand({validateDBMetadata: 1, apiParameters: {version: "1", strict: true}}));

assert(res.hasMoreErrors, res);
assert(res.apiVersionErrors, res);
assert(res.apiVersionErrors.length < 100, res);

MongoRunner.stopMongod(conn);
})();
/**
 * Tests that the validate command reports documents not adhering to collection schema rules.
 */
(function() {
"use strict";

const conn = MongoRunner.runMongod();

const dbName = "test";
const collName = "validate_doc_schema";

const db = conn.getDB(dbName);

assert.commandWorked(db.createCollection(collName, {validator: {a: {$exists: true}}}));
const coll = db.getCollection(collName);

assert.commandWorked(db.runCommand(
    {insert: collName, documents: [{a: 1}, {b: 1}, {c: 1}], bypassDocumentValidation: true}));

// Validation detects documents not adhering to the collection schema rules.
let res = assert.commandWorked(coll.validate());
assert(res.valid);

// Even though there are two documents violating the collection schema rules, the warning should
// only be shown once.
assert.eq(res.warnings.length, 1);

checkLog.containsJson(conn, 5363500, {recordId: "2"});
checkLog.containsJson(conn, 5363500, {recordId: "3"});

// Remove the documents violating the collection schema rules.
assert.commandWorked(coll.remove({b: 1}));
assert.commandWorked(coll.remove({c: 1}));

res = assert.commandWorked(coll.validate());
assert(res.valid);
assert.eq(res.warnings.length, 0);

MongoRunner.stopMongod(conn);
}());

/**
 * Verifies that the validate hook is able to upgrade the feature compatibility version of the
 * server regardless of what state any previous upgrades or downgrades have left it in.
 */

load("jstests/libs/logv2_helpers.js");

// The global 'db' variable is used by the data consistency hooks.
var db;

(function() {
"use strict";

// We skip doing the data consistency checks while terminating the cluster because they conflict
// with the counts of the number of times the "validate" command is run.
TestData.skipCollectionAndIndexValidation = true;

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

function makePatternForSetFCV(targetVersion) {
    if (isJsonLogNoConn()) {
        return new RegExp(
            `Slow query.*"appName":"MongoDB Shell","command":{"setFeatureCompatibilityVersion":"${
                targetVersion}"`,
            "g");
    }
    return new RegExp(
        "COMMAND.*command.*appName: \"MongoDB Shell\" command: setFeatureCompatibilityVersion" +
            " { setFeatureCompatibilityVersion: \"" + targetVersion + "\"",
        "g");
}

function makePatternForSetParameter(paramName) {
    if (isJsonLogNoConn()) {
        return new RegExp(
            `Slow query.*"appName":"MongoDB Shell","command":{"setParameter":1,"${paramName}":`,
            "g");
    }
    return new RegExp("COMMAND.*command.*appName: \"MongoDB Shell\" command: setParameter" +
                          " { setParameter: 1\\.0, " + paramName + ":",
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

function runValidateHook(testCase) {
    db = testCase.conn.getDB("test");
    TestData.forceValidationWithFeatureCompatibilityVersion = latestFCV;
    try {
        clearRawMongoProgramOutput();

        load("jstests/hooks/run_validate_collections.js");

        // We terminate the processes to ensure that the next call to rawMongoProgramOutput()
        // will return all of their output.
        testCase.teardown();
        return rawMongoProgramOutput();
    } finally {
        db = undefined;
        TestData.forceValidationWithFeatureCompatibilityVersion = undefined;
    }
}

function testStandalone(additionalSetupFn, {
    expectedAtTeardownFCV,
    expectedSetLastLTSFCV: expectedSetLastLTSFCV = 0,
    expectedSetLatestFCV: expectedSetLatestFCV = 0
} = {}) {
    const conn =
        MongoRunner.runMongod({setParameter: {logComponentVerbosity: tojson({command: 1})}});
    assert.neq(conn, "mongod was unable to start up");

    // Insert a document so the "validate" command has some actual work to do.
    assert.commandWorked(conn.getDB("test").mycoll.insert({}));

    // Run the additional setup function to put the server into the desired state.
    additionalSetupFn(conn);

    const output = runValidateHook({
        conn: conn,
        teardown: () => {
            // The validate hook should leave the server with a feature compatibility version of
            // 'expectedAtTeardownFCV' and no targetVersion.
            checkFCV(conn.getDB("admin"), expectedAtTeardownFCV);
            MongoRunner.stopMongod(conn);
        }
    });

    let pattern = makePatternForValidate("test", "mycoll");
    assert.eq(1,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from mongod in the log output");

    for (let [targetVersion, expectedCount] of [[lastLTSFCV, expectedSetLastLTSFCV],
                                                [latestFCV, expectedSetLatestFCV]]) {
        // Since the additionalSetupFn() function may run the setFeatureCompatibilityVersion
        // command and we don't have a guarantee those log messages were cleared when
        // clearRawMongoProgramOutput() was called, we assert 'expectedSetLastLTSFCV' and
        // 'expectedSetLatestFCV' as lower bounds.
        const pattern = makePatternForSetFCV(targetVersion);
        assert.lte(expectedCount,
                   countMatches(pattern, output),
                   "expected to find " + tojson(pattern) + " from mongod in the log output");
    }

    pattern = makePatternForSetParameter("transactionLifetimeLimitSeconds");
    assert.eq(2,
              countMatches(pattern, output),
              "expected to find " + tojson(pattern) + " from mongod in the log output twice");
}

function forceInterruptedUpgradeOrDowngrade(conn, targetVersion) {
    // We create a separate connection to the server exclusively for running the
    // setFeatureCompatibilityVersion command so only that operation is ever interrupted by
    // the checkForInterruptFail failpoint.
    const setFCVConn = new Mongo(conn.host);
    const myUriRes = assert.commandWorked(setFCVConn.adminCommand({whatsmyuri: 1}));
    const myUri = myUriRes.you;

    const curOpRes = assert.commandWorked(setFCVConn.adminCommand({currentOp: 1, client: myUri}));
    const threadName = curOpRes.inprog[0].desc;

    assert.commandWorked(conn.adminCommand({
        configureFailPoint: "checkForInterruptFail",
        mode: "alwaysOn",
        data: {threadName, chance: 0.05},
    }));

    let attempts = 0;
    assert.soon(
        function() {
            let res = setFCVConn.adminCommand({setFeatureCompatibilityVersion: targetVersion});

            if (res.ok === 1) {
                assert.commandWorked(res);
            } else {
                assert.commandFailedWithCode(res, ErrorCodes.Interrupted);
            }

            ++attempts;

            res = assert.commandWorked(
                conn.adminCommand({getParameter: 1, featureCompatibilityVersion: 1}));

            if (res.featureCompatibilityVersion.hasOwnProperty("targetVersion")) {
                checkFCV(conn.getDB("admin"), lastLTSFCV, targetVersion);
                jsTest.log(`Reached partially downgraded state after ${attempts} attempts`);
                return true;
            }

            // Either upgrade the feature compatibility version so we can try downgrading again,
            // or downgrade the feature compatibility version so we can try upgrading again.
            // Note that we're using 'conn' rather than 'setFCVConn' to avoid the upgrade being
            // interrupted.
            assert.commandWorked(conn.adminCommand({
                setFeatureCompatibilityVersion: targetVersion === lastLTSFCV ? latestFCV
                                                                             : lastLTSFCV
            }));
        },
        "failed to get featureCompatibilityVersion document into a partially downgraded" +
            " state");

    assert.commandWorked(conn.adminCommand({
        configureFailPoint: "checkForInterruptFail",
        mode: "off",
    }));
}

(function testStandaloneInLatestFCV() {
    testStandalone(conn => {
        checkFCV(conn.getDB("admin"), latestFCV);
    }, {expectedAtTeardownFCV: latestFCV});
})();

(function testStandaloneInLastLTSFCV() {
    testStandalone(conn => {
        assert.commandWorked(conn.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}));
        checkFCV(conn.getDB("admin"), lastLTSFCV);
    }, {expectedAtTeardownFCV: lastLTSFCV, expectedSetLastLTSFCV: 1, expectedSetLatestFCV: 1});
})();

(function testStandaloneWithInterruptedFCVDowngrade() {
    testStandalone(conn => {
        forceInterruptedUpgradeOrDowngrade(conn, lastLTSFCV);
    }, {expectedAtTeardownFCV: lastLTSFCV, expectedSetLastLTSFCV: 2, expectedSetLatestFCV: 1});
})();

(function testStandaloneWithInterruptedFCVUpgrade() {
    testStandalone(conn => {
        assert.commandWorked(conn.adminCommand({setFeatureCompatibilityVersion: lastLTSFCV}));
        forceInterruptedUpgradeOrDowngrade(conn, latestFCV);
    }, {expectedAtTeardownFCV: lastLTSFCV, expectedSetLastLTSFCV: 1, expectedSetLatestFCV: 1});
})();
})();

/**
 * Test that the memory usage of validate is properly limited according to the
 * maxValidateMemoryUsageMB parameter.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("jstests/disk/libs/wt_file_helper.js");

const kIndexKeyLength = 1024 * 1024;

const baseName = "validate_memory_limit";
const dbpath = MongoRunner.dataPath + baseName + "/";
let conn = MongoRunner.runMongod({dbpath: dbpath});
let coll = conn.getDB("test").getCollection("corrupt");

function corruptIndex() {
    const uri = getUriForIndex(coll, "_id_");
    conn = truncateUriAndRestartMongod(uri, conn);
    coll = conn.getDB("test").getCollection("corrupt");
}

function checkValidate(errorPrefix, numMissingIndexEntries) {
    conn.getDB("test").adminCommand({setParameter: 1, maxValidateMemoryUsageMB: 1});
    const res = coll.validate();
    assert.commandWorked(res);
    assert(!res.valid);
    assert.containsPrefix(errorPrefix, res.errors);
    assert.eq(res.missingIndexEntries.length, numMissingIndexEntries);
}

function checkValidateRepair(expectRepair) {
    const res = coll.validate({repair: true});
    assert.commandWorked(res);
    assert(!res.valid, printjson(res));
    assert.eq(res.repaired, expectRepair, printjson(res));
}

const noneReportedPrefix =
    "Unable to report index entry inconsistencies due to memory limitations.";
const notAllReportedPrefix =
    "Not all index entry inconsistencies are reported due to memory limitations.";

// Insert a document with a key larger than maxValidateMemoryUsageMB so that validate does not
// report any missing index entries.
const indexKey = "a".repeat(kIndexKeyLength);
assert.commandWorked(coll.insert({_id: indexKey}));
corruptIndex();
checkValidate(noneReportedPrefix, 0);

// Can't repair successfully if there aren't any index inconsistencies reported.
checkValidateRepair(false);

// Insert a document with a small key so that validate reports one missing index entry.
assert.commandWorked(coll.insert({_id: 1}));
corruptIndex();
checkValidate(notAllReportedPrefix, 1);

// Repair, but incompletely if only some inconsistencies are reported.
checkValidateRepair(true);

MongoRunner.stopMongod(conn, null, {skipValidation: true});
})();
/**
 * This test validates a reader concurrent with a multikey update observes a proper snapshot of
 * data. Specifically, if the data can be multikey, the in-memory `isMultikey()` method must return
 * true. The `validate` command ensures this relationship.
 *
 * The failpoint used just widens a window, instead of pausing execution. Thus the test is
 * technically non-deterministic. However, if the server had a bug that caused a regression, the
 * non-determinism would cause the test to sometimes pass when it should fail. A properly behaving
 * server should never cause the test to accidentally fail.
 *
 * @tags: [
 *     requires_replication,
 *     requires_persistence,
 * ]
 */
(function() {
'use strict';

load('jstests/libs/parallel_shell_helpers.js');

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let testColl = primary.getCollection('test.validate_multikey');

assert.commandWorked(testColl.createIndex({a: 1}));
let validation = assert.commandWorked(testColl.validate({background: true}));
jsTestLog({validation: validation});

const args = [testColl.getDB().getName(), testColl.getName()];
let func = function(args) {
    const [dbName, collName] = args;
    const testColl = db.getSiblingDB(dbName)[collName];
    jsTestLog('Enabling failpoint to delay storage transaction completion');
    assert.commandWorked(testColl.getDB().adminCommand(
        {configureFailPoint: "widenWUOWChangesWindow", mode: "alwaysOn"}));
    // This first insert flips multikey.
    jsTestLog('Inserting first document to flip multikey state');
    assert.commandWorked(testColl.insert({a: [1, 2]}));
    // This second insert is just a signal to allow the following validation loop to gracefully
    // break.
    jsTestLog('Inserting second document to signal condition to exit validation loop.');
    assert.commandWorked(testColl.insert({}));
    jsTestLog('Parallel shell for insert operations completed.');
};
let join = startParallelShell(funWithArgs(func, args), primary.port);

while (testColl.count() < 2) {
    validation = assert.commandWorked(testColl.validate({background: true}));
    jsTestLog({background: validation});
    assert(validation.valid);
}

assert.commandWorked(
    testColl.getDB().adminCommand({configureFailPoint: "widenWUOWChangesWindow", mode: "off"}));
join();

rst.stopSet();
})();

/**
 * Validates multikey compound index in a collection that also contains a hashed index.
 * The scenario tested here involves restarting the node after a failed insert due
 * to contraints imposed by the hashed index. The validation behavior observed after
 * restarting is that, even though we inserted documents that contain arrays in every
 * indexed field in the compound index, we fail to save all the multikey path information
 * in the catalog.
 * @tags: [
 *     requires_replication,
 *     requires_persistence,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let testColl = primary.getCollection('test.validate_multikey_compound');

assert.commandWorked(testColl.getDB().createCollection(testColl.getName()));

assert.commandWorked(testColl.createIndex({a: 1, b: 1}));
assert.commandWorked(testColl.createIndex({c: 'hashed'}));

// Insert 3 documents. Only the first and last documents are valid.
assert.commandWorked(testColl.insert({_id: 0, a: [1, 2, 3], b: 'abc', c: 'valid_hash_0'}));

// 16766 is the error code returned by ExpressionKeysPrivate::getHashKeys() for
// "hashed indexes do not currently support array values".
assert.commandFailedWithCode(
    testColl.insert({_id: 1, a: 456, b: ['d', 'e', 'f'], c: ['invalid', 'hash']}), 16766);

assert.commandWorked(testColl.insert({_id: 2, a: 789, b: ['g', 'h', ' i'], c: 'valid_hash_2'}));

jsTestLog('Checking documents in collection before restart');
let docs = testColl.find().sort({_id: 1}).toArray();
assert.eq(2, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(0, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));
assert.eq(2, docs[1]._id, 'unexpected document content in collection: ' + tojson(docs));

// For the purpose of reproducing the validation error in a_1, it is important to skip validation
// when restarting the primary node. Enabling validation here has an effect on the validate
// command's behavior after restarting.
primary = rst.restart(primary, {skipValidation: true}, /*signal=*/undefined, /*wait=*/true);
testColl = primary.getCollection(testColl.getFullName());

jsTestLog('Checking documents in collection after restart');
rst.awaitReplication();
docs = testColl.find().sort({_id: 1}).toArray();
assert.eq(2, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(0, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));
assert.eq(2, docs[1]._id, 'unexpected document content in collection: ' + tojson(docs));

jsTestLog('Validating collection after restart');
const result = assert.commandWorked(testColl.validate({full: true}));

jsTestLog('Validation result: ' + tojson(result));
assert.eq(testColl.getFullName(), result.ns, tojson(result));
assert.eq(0, result.nInvalidDocuments, tojson(result));
assert.eq(2, result.nrecords, tojson(result));
assert.eq(3, result.nIndexes, tojson(result));

// Check non-multikey indexes.
assert.eq(2, result.keysPerIndex._id_, tojson(result));
assert.eq(2, result.keysPerIndex.c_hashed, tojson(result));
assert(result.indexDetails._id_.valid, tojson(result));
assert(result.indexDetails.c_hashed.valid, tojson(result));

// Check multikey index.
assert.eq(6, result.keysPerIndex.a_1_b_1, tojson(result));
assert(result.indexDetails.a_1_b_1.valid, tojson(result));

assert(result.valid, tojson(result));

rst.stopSet();
})();

/**
 * Validates multikey compound index in a collection with a single insert command
 * containing documents that update different paths in the multikey index.
 *
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let testColl = primary.getCollection('test.validate_multikey_compound_batch');

assert.commandWorked(testColl.getDB().createCollection(testColl.getName()));

assert.commandWorked(testColl.createIndex({a: 1, b: 1}));

// Insert 2 documents. Only the first and last documents are valid.
assert.commandWorked(
    testColl.insert([{_id: 0, a: [1, 2, 3], b: 'abc'}, {_id: 1, a: 456, b: ['d', 'e', 'f']}]));

jsTestLog('Checking documents in collection');
let docs = testColl.find().sort({_id: 1}).toArray();
assert.eq(2, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(0, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));
assert.eq(1, docs[1]._id, 'unexpected document content in collection: ' + tojson(docs));

jsTestLog('Validating collection');
const result = assert.commandWorked(testColl.validate({full: true}));

jsTestLog('Validation result: ' + tojson(result));
assert.eq(testColl.getFullName(), result.ns, tojson(result));
assert.eq(0, result.nInvalidDocuments, tojson(result));
assert.eq(2, result.nrecords, tojson(result));
assert.eq(2, result.nIndexes, tojson(result));

// Check non-multikey indexes.
assert.eq(2, result.keysPerIndex._id_, tojson(result));
assert(result.indexDetails._id_.valid, tojson(result));

// Check multikey index.
assert.eq(6, result.keysPerIndex.a_1_b_1, tojson(result));
assert(result.indexDetails.a_1_b_1.valid, tojson(result));

assert(result.valid, tojson(result));

rst.stopSet();
})();

/**
 * Validates multikey index in a collection that also contains a hashed index.
 * The scenario tested here involves restarting the node after a failed insert due
 * to contraints imposed by the hashed index. The validation behavior observed after
 * restarting is that, even though we inserted another document with an array in the
 * indexed field for the non-hashed index, we fail to save the index state as multikey.
 * @tags: [
 *     requires_replication,
 *     requires_persistence,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let testColl = primary.getCollection('test.validate_multikey_restart');

assert.commandWorked(testColl.getDB().createCollection(testColl.getName()));

assert.commandWorked(testColl.createIndex({a: 1}));
assert.commandWorked(testColl.createIndex({b: 'hashed'}));

// 16766 is the error code returned by ExpressionKeysPrivate::getHashKeys() for
// "hashed indexes do not currently support array values".
assert.commandFailedWithCode(testColl.insert({_id: 0, a: [1, 2, 3], b: ['a', 'b', 'c']}), 16766);

assert.commandWorked(testColl.insert({_id: 1, a: [4, 5, 6], b: 'def'}));

jsTestLog('Checking documents in collection before restart');
let docs = testColl.find().sort({_id: 1}).toArray();
assert.eq(1, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(1, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));

jsTestLog('Checking multikey query before restart');
let multikeyQueryDocs = testColl.find({a: {$in: [4, 5, 6]}}).toArray();
assert.eq(1,
          multikeyQueryDocs.length,
          'too many docs in multikey query result: ' + tojson(multikeyQueryDocs));
assert.eq(1,
          multikeyQueryDocs[0]._id,
          'unexpected document content in multikey query result: ' + tojson(multikeyQueryDocs));

// For the purpose of reproducing the validation error in a_1, it is important to skip validation
// when restarting the primary node. Enabling validation here has an effect on the validate
// command's behavior after restarting.
primary = rst.restart(primary, {skipValidation: true}, /*signal=*/undefined, /*wait=*/true);
testColl = primary.getCollection(testColl.getFullName());

jsTestLog('Checking documents in collection after restart');
rst.awaitReplication();
docs = testColl.find().sort({_id: 1}).toArray();
assert.eq(1, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(1, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));

jsTestLog('Checking multikey query after restart');
multikeyQueryDocs = testColl.find({a: {$in: [4, 5, 6]}}).toArray();
assert.eq(1,
          multikeyQueryDocs.length,
          'too many docs in multikey query result: ' + tojson(multikeyQueryDocs));
assert.eq(1,
          multikeyQueryDocs[0]._id,
          'unexpected document content in multikey query result: ' + tojson(multikeyQueryDocs));

jsTestLog('Validating collection after restart');
const result = assert.commandWorked(testColl.validate({full: true}));

jsTestLog('Validation result: ' + tojson(result));
assert.eq(testColl.getFullName(), result.ns, tojson(result));
assert.eq(0, result.nInvalidDocuments, tojson(result));
assert.eq(1, result.nrecords, tojson(result));
assert.eq(3, result.nIndexes, tojson(result));

// Check non-multikey indexes.
assert.eq(1, result.keysPerIndex._id_, tojson(result));
assert.eq(1, result.keysPerIndex.b_hashed, tojson(result));
assert(result.indexDetails._id_.valid, tojson(result));
assert(result.indexDetails.b_hashed.valid, tojson(result));

// Check multikey index.
assert.eq(3, result.keysPerIndex.a_1, tojson(result));
assert(result.indexDetails.a_1.valid, tojson(result));

assert(result.valid, tojson(result));

rst.stopSet();
})();

/**
 * Validates multikey index in a collection after retrying a 2dsphere insert.
 * The scenario tested here involves stepping down the node so that the first insert attempt fails.
 * The insert is retried after the node becomes primary again. At this point, we will validate the
 * collection after restarting the node.
 * @tags: [
 *     requires_replication,
 *     requires_persistence,
 * ]
 */
(function() {
'use strict';

load('jstests/libs/fail_point_util.js');
load('jstests/libs/parallel_shell_helpers.js');

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let testColl = primary.getCollection('test.validate_multikey_stepdown');

assert.commandWorked(testColl.getDB().createCollection(testColl.getName()));

assert.commandWorked(testColl.createIndex({geo: '2dsphere'}));

// Sample polygon and point from geo_s2dedupnear.js
const polygon = {
    type: 'Polygon',
    coordinates: [[
        [100.0, 0.0],
        [101.0, 0.0],
        [101.0, 1.0],
        [100.0, 1.0],
        [100.0, 0.0],
    ]],
};
const point = {
    type: 'Point',
    coordinates: [31, 41],
};
const geoDocToInsert = {
    _id: 1,
    geo: polygon,
};
const geoQuery = {
    geo: {$geoNear: point}
};

const failPoint = configureFailPoint(
    primary, 'hangAfterCollectionInserts', {collectionNS: testColl.getFullName()});

let awaitInsert;
try {
    const args = [testColl.getDB().getName(), testColl.getName(), geoDocToInsert];
    let func = function(args) {
        const [dbName, collName, geoDocToInsert] = args;
        jsTestLog("Insert a document that will hang before the insert completes.");
        const testColl = db.getSiblingDB(dbName)[collName];
        // This should fail with ErrorCodes.InterruptedDueToReplStateChange.
        const result = testColl.insert(geoDocToInsert);
        jsTestLog('Async insert result = ' + tojson(result));
        assert.commandFailedWithCode(result, ErrorCodes.InterruptedDueToReplStateChange);
    };
    awaitInsert = startParallelShell(funWithArgs(func, args), primary.port);

    jsTest.log("Wait for async insert to hit the failpoint.");
    failPoint.wait();

    // Step down the primary. This will interrupt the async insert.
    // Since there is no other electable node, the replSetStepDown command will time out and the
    // node will be re-elected.
    jsTest.log("Step down primary temporarily to interrupt async insert.");
    assert.commandFailedWithCode(primary.adminCommand({replSetStepDown: 10}),
                                 ErrorCodes.ExceededTimeLimit);
} finally {
    // Turn off the failpoint before allowing the test to end, so nothing hangs while the server
    // shuts down or in post-test hooks.
    failPoint.off();
}

// Wait until the async insert is completed.
jsTest.log("Wait for async insert to complete.");
awaitInsert();

jsTest.log("Retrying insert after stepping up.");
assert.commandWorked(testColl.insert(geoDocToInsert));

jsTestLog('Checking documents in collection before restart');
let docs = testColl.find(geoQuery).sort({_id: 1}).toArray();
assert.eq(1, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(
    geoDocToInsert._id, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));

// For the purpose of reproducing the validation error in geo_2dsphere, it is important to skip
// validation when restarting the primary node. Enabling validation here has an effect on the
// validate command's behavior after restarting.
primary = rst.restart(primary, {skipValidation: true}, /*signal=*/undefined, /*wait=*/true);
testColl = primary.getCollection(testColl.getFullName());

jsTestLog('Checking documents in collection after restart');
rst.awaitReplication();
docs = testColl.find(geoQuery).sort({_id: 1}).toArray();
assert.eq(1, docs.length, 'too many docs in collection: ' + tojson(docs));
assert.eq(
    geoDocToInsert._id, docs[0]._id, 'unexpected document content in collection: ' + tojson(docs));

jsTestLog('Validating collection after restart');
const result = assert.commandWorked(testColl.validate({full: true}));

jsTestLog('Validation result: ' + tojson(result));
assert.eq(testColl.getFullName(), result.ns, tojson(result));
assert.eq(0, result.nInvalidDocuments, tojson(result));
assert.eq(1, result.nrecords, tojson(result));
assert.eq(2, result.nIndexes, tojson(result));

// Check non-geo indexes.
assert.eq(1, result.keysPerIndex._id_, tojson(result));
assert(result.indexDetails._id_.valid, tojson(result));

// Check geo index.
assert.lt(1, result.keysPerIndex.geo_2dsphere, tojson(result));
assert(result.indexDetails.geo_2dsphere.valid, tojson(result));

assert(result.valid, tojson(result));

rst.stopSet();
})();

/**
 * Test that the validate command properly limits the index entry inconsistencies reported when
 * there is corruption on an index with a long name.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("jstests/disk/libs/wt_file_helper.js");

// 64 * 1024 * 1024 = 64MB worth of index names ensures that we test against the maximum BSONObj
// size lmit.
const kNumDocs = 64;
const kIndexNameLength = 1024 * 1024;

const baseName = "validate_with_long_index_name";
const dbpath = MongoRunner.dataPath + baseName + "/";
const indexName = "a".repeat(kIndexNameLength);
let conn = MongoRunner.runMongod({dbpath: dbpath});
let coll = conn.getDB("test").getCollection("corrupt");

function insertDocsAndBuildIndex() {
    const bulk = coll.initializeUnorderedBulkOp();
    for (let i = 0; i < kNumDocs; i++) {
        bulk.insert({_id: i});
    }
    bulk.execute();
    coll.createIndex({a: 1}, {name: indexName});
}

insertDocsAndBuildIndex();
let uri = getUriForIndex(coll, indexName);
conn = truncateUriAndRestartMongod(uri, conn);
coll = conn.getDB("test").getCollection("corrupt");

const missingIndexEntries = "Detected " + kNumDocs + " missing index entries.";
const missingSizeLimitations =
    "Not all missing index entry inconsistencies are listed due to size limitations.";

let res = coll.validate();
assert.commandWorked(res);
assert(!res.valid);
assert.contains(missingIndexEntries, res.warnings);
assert.contains(missingSizeLimitations, res.errors);

coll.drop();
insertDocsAndBuildIndex();
uri = getUriForColl(coll);
conn = truncateUriAndRestartMongod(uri, conn);
coll = conn.getDB("test").getCollection("corrupt");

const extraIndexEntries = "Detected " + 2 * kNumDocs + " extra index entries.";
const extraSizeLimitations =
    "Not all extra index entry inconsistencies are listed due to size limitations.";

res = coll.validate();
assert.commandWorked(res);
assert(!res.valid);
assert.contains(extraIndexEntries, res.warnings);
assert.contains(extraSizeLimitations, res.errors);

MongoRunner.stopMongod(conn, null, {skipValidation: true});
})();
// Test collection validation related server parameter settings on server startup
// and via the setParameter command.

(function() {
'use strict';

load("jstests/noPassthrough/libs/server_parameter_helpers.js");

// Valid parameter values are in the range [0, infinity).
testNumericServerParameter('maxValidateMBperSec',
                           true /*isStartupParameter*/,
                           true /*isRuntimeParameter*/,
                           0 /*defaultValue*/,
                           60 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           -1 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);

// Valid parameter values are in the range (0, infinity).
testNumericServerParameter('maxValidateMemoryUsageMB',
                           true /*isStartupParameter*/,
                           true /*isRuntimeParameter*/,
                           200 /*defaultValue*/,
                           50 /*nonDefaultValidValue*/,
                           true /*hasLowerBound*/,
                           0 /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);
})();

// @tags: [requires_sharding]

(function() {
'use strict';

// This test makes assertions about the number of sessions, which are not compatible with
// implicit sessions.
TestData.disableImplicitSessions = true;

function runTest(conn) {
    for (var i = 0; i < 10; ++i) {
        conn.getDB("test").test.save({a: i});
    }

    function countSessions(conn, since) {
        conn.adminCommand({refreshLogicalSessionCacheNow: 1});
        return conn.getDB("config").system.sessions.countDocuments({lastUse: {"$gt": since}});
    }

    function getLatestSessionTime(conn) {
        conn.getDB("admin").runCommand({refreshLogicalSessionCacheNow: 1});
        let lastSession = conn.getDB("config")
                              .system.sessions.aggregate([{"$sort": {lastUse: -1}}, {$limit: 1}])
                              .toArray();
        return (lastSession.length ? lastSession[0].lastUse : new Date(0));
    }

    let origSessTime = getLatestSessionTime(conn);

    // initially we have no sessions
    assert.eq(0, countSessions(conn, origSessTime));

    // Calling startSession in the shell doesn't initiate the session
    var session = conn.startSession();
    assert.eq(0, countSessions(conn, origSessTime));

    // running a command that doesn't require auth does touch
    session.getDatabase("admin").runCommand("hello");
    assert.eq(1, countSessions(conn, origSessTime));

    // running a session updating command does touch
    session.getDatabase("admin").runCommand({serverStatus: 1});
    assert.eq(1, countSessions(conn, origSessTime));

    // running a session updating command updates last use
    {
        var lastUse = getLatestSessionTime(conn);
        sleep(200);
        session.getDatabase("admin").runCommand({serverStatus: 1});
        assert.eq(1, countSessions(conn, origSessTime));
        assert.gt(getLatestSessionTime(conn), lastUse);
    }

    // verify that reading from a cursor updates last use
    {
        var cursor = session.getDatabase("test").test.find({}).batchSize(1);
        cursor.next();
        var lastUse = getLatestSessionTime(conn);
        sleep(200);
        assert.eq(1, countSessions(conn, origSessTime));
        cursor.next();
        assert.gt(getLatestSessionTime(conn), lastUse);
    }

    session.endSession();
}

{
    var mongod = MongoRunner.runMongod({nojournal: ""});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);
}

{
    var st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    st.rs0.getPrimary().getDB("admin").runCommand({refreshLogicalSessionCacheNow: 1});

    runTest(st.s0);
    st.stop();
}
})();

// Tests valid coordination of the expiration and vivification of documents between the
// config.system.sessions collection and the logical session cache.
//
// 1. Sessions should be removed from the logical session cache when they expire from
//    the config.system.sessions collection.
// 2. getMores run on open cursors should update the lastUse field on documents in the
//    config.system.sessions collection, prolonging the time for expiration on said document
//    and corresponding session.
// 3. Open cursors that are not currently in use should be killed when their corresponding sessions
//    expire from the config.system.sessions collection.
// 4. Currently running operations corresponding to a session should prevent said session from
//    expiring from the config.system.sessions collection. If the expiration date has been reached
//    during a currently running operation, the logical session cache should vivify the session and
//    replace it in the config.system.sessions collection.

(function() {
"use strict";

// This test makes assertions about the number of logical session records.
TestData.disableImplicitSessions = true;

load("jstests/libs/pin_getmore_cursor.js");  // For "withPinnedCursor".

const refresh = {
    refreshLogicalSessionCacheNow: 1
};
const startSession = {
    startSession: 1
};
const failPointName = "waitAfterPinningCursorBeforeGetMoreBatch";

function refreshSessionsAndVerifyCount(config, expectedCount) {
    config.runCommand(refresh);
    assert.eq(config.system.sessions.count(), expectedCount);
}

function getSessions(config) {
    return config.system.sessions.aggregate([{'$listSessions': {allUsers: true}}]).toArray();
}

function verifyOpenCursorCount(db, expectedCount) {
    assert.eq(db.serverStatus().metrics.cursor.open.total, expectedCount);
}

const dbName = "test";
const testCollName = "verify_sessions_find_get_more";

let conn = MongoRunner.runMongod();
let db = conn.getDB(dbName);
let config = conn.getDB("config");

// 1. Verify that sessions expire from config.system.sessions after the timeout has passed.
for (let i = 0; i < 5; i++) {
    let res = db.runCommand(startSession);
    assert.commandWorked(res, "unable to start session");
}
refreshSessionsAndVerifyCount(config, 5);

// Manually delete entries in config.system.sessions to simulate TTL expiration.
assert.commandWorked(config.system.sessions.remove({}));
refreshSessionsAndVerifyCount(config, 0);

// 2. Verify that getMores after finds will update the 'lastUse' field on documents in the
// config.system.sessions collection.
for (let i = 0; i < 10; i++) {
    db[testCollName].insert({_id: i, a: i, b: 1});
}

let cursors = [];
for (let i = 0; i < 5; i++) {
    let session = db.getMongo().startSession({});
    assert.commandWorked(session.getDatabase("admin").runCommand({usersInfo: 1}),
                         "initialize the session");
    cursors.push(session.getDatabase(dbName)[testCollName].find({b: 1}).batchSize(1));
    assert(cursors[i].hasNext());
}

refreshSessionsAndVerifyCount(config, 5);
verifyOpenCursorCount(config, 5);

let sessionsCollectionArray;
let lastUseValues = [];
for (let i = 0; i < 3; i++) {
    for (let j = 0; j < cursors.length; j++) {
        cursors[j].next();
    }

    refreshSessionsAndVerifyCount(config, 5);
    verifyOpenCursorCount(config, 5);

    sessionsCollectionArray = getSessions(config);

    if (i == 0) {
        for (let j = 0; j < sessionsCollectionArray.length; j++) {
            lastUseValues.push(sessionsCollectionArray[j].lastUse);
        }
    } else {
        for (let j = 0; j < sessionsCollectionArray.length; j++) {
            assert.gt(sessionsCollectionArray[j].lastUse, lastUseValues[j]);
            lastUseValues[j] = sessionsCollectionArray[j].lastUse;
        }
    }
}

// 3. Verify that letting sessions expire (simulated by manual deletion) will kill their
// cursors.
assert.commandWorked(config.system.sessions.remove({}));

refreshSessionsAndVerifyCount(config, 0);
verifyOpenCursorCount(config, 0);

for (let i = 0; i < cursors.length; i++) {
    assert.commandFailedWithCode(
        db.runCommand({getMore: cursors[i]._cursor._cursorid, collection: testCollName}),
        ErrorCodes.CursorNotFound,
        'expected getMore to fail because the cursor was killed');
}

// 4. Verify that an expired session (simulated by manual deletion) that has a currently running
// operation will be vivified during the logical session cache refresh.
let pinnedCursorSession = db.getMongo().startSession();
withPinnedCursor({
    conn: conn,
    db: pinnedCursorSession.getDatabase(dbName),
    assertFunction: (cursorId, coll) => {
        assert.commandWorked(config.system.sessions.remove({}));

        refreshSessionsAndVerifyCount(config, 1);
        verifyOpenCursorCount(config, 1);

        let db = coll.getDB();
        assert.commandWorked(db.runCommand({killCursors: coll.getName(), cursors: [cursorId]}));
    },
    sessionId: pinnedCursorSession,
    runGetMoreFunc: () => {
        assert.commandFailed(
            db.runCommand({getMore: cursorId, collection: collName, lsid: sessionId}));
    },
    failPointName: failPointName,
});

MongoRunner.stopMongod(conn);
})();

// Tests that a view dependency chain is correctly logged in the global log for a find command
// against the view.
// @tags: [requires_profiling]

// The entry in the log should contain a field that looks like this:
// "resolvedViews":
// [
//   {
//     "viewNamespace":"<full name of the view>",
//     "dependencyChain":[<names of views/collection in the dep chain, db name omitted>],
//     "resolvedPipeline":[<the resolved pipeline of the view>]
//   },
//   {
//     <repeat for all other views that had to be resolved for the query>
//   }
// ],
(function() {
'use strict';

load("jstests/libs/logv2_helpers.js");

function resetProfiler(db) {
    assert.commandWorked(db.setProfilingLevel(0, {slowms: 0}));
    db.system.profile.drop();
    assert.commandWorked(db.setProfilingLevel(1, {slowms: 0}));
}

function assertResolvedView(resolvedView, nss, dependencyDepth, pipelineSize) {
    assert.eq(nss, resolvedView["viewNamespace"], resolvedView);
    assert.eq(dependencyDepth, resolvedView["dependencyChain"].length, resolvedView);
    assert.eq(pipelineSize, resolvedView["resolvedPipeline"].length, resolvedView);
}

// Check that the profiler has the expected record.
function checkProfilerLog(db) {
    const result = db.system.profile.find({ns: "views_log_depchain_db.c_view"}).toArray();
    assert.eq(1, result.length, result);
    const record = result[0];
    assert(record.hasOwnProperty("resolvedViews"), record);
    const resolvedViews = record["resolvedViews"];
    assert.eq(2, resolvedViews.length, resolvedViews);

    // The views are logged sorted by their original namespace, not in the order of resolution.
    assertResolvedView(resolvedViews[0], `${db.getName()}.b_view`, 2, 1);
    assertResolvedView(resolvedViews[1], `${db.getName()}.c_view`, 3, 3);
}

const mongodOptions = {};
const conn = MongoRunner.runMongod(mongodOptions);
assert.neq(null, conn, `mongod failed to start with options ${tojson(mongodOptions)}`);

// Setup the tree of views.
const db = conn.getDB(`${jsTest.name()}_db`);
assert.commandWorked(db.dropDatabase());
assert.commandWorked(db.createCollection("base"));
assert.commandWorked(db.createView("a_view", "base", [{$project: {"aa": 0}}, {$sort: {"a": 1}}]));
assert.commandWorked(db.createView("b_view", "base", [{$project: {"bb": 0}}]));
assert.commandWorked(db.createView("c_view", "a_view", [
    {$lookup: {from: "b_view", localField: "x", foreignField: "x", as: "y"}}
]));

// Run a simple query against the top view.
assert.commandWorked(db.setProfilingLevel(1, {slowms: 0}));
assert.commandWorked(db.runCommand({find: "c_view", filter: {}}));
checkProfilerLog(db);

// Sanity-check the "slow query" log (we do it only once, because the data between the profiler
// and "slow query" log are identical).
checkLog.containsWithCount(conn,
                           `"resolvedViews":[{"viewNamespace":"${db.getName()}.b_view",` +
                               `"dependencyChain":["b_view","base"],"resolvedPipeline":[{"$project`,
                           1);

// Run an aggregate query against the top view which uses one of the views from its dependency
// chain, so the logged data is the same as above.
resetProfiler(db);
const lookup = {
    $lookup: {from: "b_view", localField: "x", foreignField: "x", as: "y"}
};
assert.commandWorked(db.runCommand({aggregate: "c_view", pipeline: [lookup], cursor: {}}));
checkProfilerLog(db);

// If a view is modified, the logs should reflect that.
assert.commandWorked(db.runCommand({drop: "c_view"}));
assert.commandWorked(db.createView("c_view", "b_view", [
    {$lookup: {from: "a_view", localField: "x", foreignField: "x", as: "y"}}
]));
resetProfiler(db);
assert.commandWorked(db.runCommand({find: "c_view", filter: {}}));

const result = db.system.profile.find({ns: "views_log_depchain_db.c_view"}).toArray();
assert.eq(1, result.length, result);
const record = result[0];
assert(record.hasOwnProperty("resolvedViews"), record);
const resolvedViews = record["resolvedViews"];
assert.eq(2, resolvedViews.length, resolvedViews);
assertResolvedView(resolvedViews[0], `${db.getName()}.a_view`, 2, 2);
assertResolvedView(resolvedViews[1], `${db.getName()}.c_view`, 3, 2);

assert.commandWorked(db.dropDatabase());
MongoRunner.stopMongod(conn);
})();

// Tests the waitInHello failpoint.
// @tags: [requires_replication]
(function() {
"use strict";
load("jstests/libs/fail_point_util.js");

function runTest(conn) {
    function runHelloCommand() {
        const now = new Date();
        assert.commandWorked(db.runCommand({hello: 1}));
        const helloDuration = new Date() - now;
        assert.gte(helloDuration, 100);
    }

    // Do a find to make sure that the shell has finished running hello while establishing its
    // initial connection.
    assert.eq(0, conn.getDB("test").c.find().itcount());

    // Use a skip of 1, since the parallel shell runs hello when it starts.
    const helloFailpoint = configureFailPoint(conn, "waitInHello", {}, {skip: 1});
    const awaitHello = startParallelShell(runHelloCommand, conn.port);
    helloFailpoint.wait();
    sleep(100);
    helloFailpoint.off();
    awaitHello();
}

const standalone = MongoRunner.runMongod({});
assert.neq(null, standalone, "mongod was unable to start up");
runTest(standalone);
MongoRunner.stopMongod(standalone);

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();
runTest(rst.getPrimary());
rst.stopSet();

const st = new ShardingTest({mongos: 1, shards: [{nodes: 1}], config: 1});
runTest(st.s);
st.stop();
}());

/**
 * Test server validation of the 'wiredTigerMaxCacheOverflowSizeGB' server parameter setting via
 * the setParameter command.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

(function() {
'use strict';

load("jstests/noPassthrough/libs/server_parameter_helpers.js");

// Valid parameter values are in the range [0.1, infinity) or 0 (unbounded).
testNumericServerParameter("wiredTigerMaxCacheOverflowSizeGB",
                           false /*isStartupParameter*/,
                           true /*isRuntimeParameter*/,
                           0 /*defaultValue*/,
                           0.1 /*nonDefaultValidValue*/,
                           false /*hasLowerBound*/,
                           "unused" /*lowerOutOfBounds*/,
                           false /*hasUpperBound*/,
                           "unused" /*upperOutOfBounds*/);
})();

/**
 * Tests that wildcard indexes are prepared to handle and retry WriteConflictExceptions while
 * interacting with the storage layer to retrieve multikey paths.
 *
 * TODO SERVER-56443: This test is specific to the classic engine. If/when the classic engine is
 * deleted, this test should be removed as well.
 */
(function() {
"strict";

load("jstests/libs/sbe_util.js");

const conn = MongoRunner.runMongod();
const testDB = conn.getDB("test");

if (checkSBEEnabled(testDB)) {
    jsTestLog("Skipping test as SBE is not resilient to WCEs");
    MongoRunner.stopMongod(conn);
    return;
}

const coll = testDB.write_conflict_wildcard;
coll.drop();

assert.commandWorked(coll.createIndex({"$**": 1}));

assert.commandWorked(testDB.adminCommand(
    {configureFailPoint: 'WTWriteConflictExceptionForReads', mode: {activationProbability: 0.01}}));
for (let i = 0; i < 1000; ++i) {
    // Insert documents with a couple different multikey paths to increase the number of records
    // scanned during multikey path computation in the wildcard index.
    assert.commandWorked(coll.insert({
        _id: i,
        i: i,
        a: [{x: i - 1}, {x: i}, {x: i + 1}],
        b: [],
        longerName: [{nested: [1, 2]}, {nested: 4}]
    }));
    assert.eq(coll.find({i: i}).hint({"$**": 1}).itcount(), 1);
    if (i > 0) {
        assert.eq(coll.find({"a.x": i}).hint({"$**": 1}).itcount(), 2);
    }
}

assert.commandWorked(
    testDB.adminCommand({configureFailPoint: 'WTWriteConflictExceptionForReads', mode: "off"}));
MongoRunner.stopMongod(conn);
})();

// SERVER-22011: Deadlock in ticket distribution
// @tags: [requires_replication, requires_capped]
(function() {
'use strict';

// Limit concurrent WiredTiger transactions to maximize locking issues, harmless for other SEs.
var options = {verbose: 1};

// Create a new single node replicaSet
var replTest = new ReplSetTest({name: "write_local", nodes: 1, oplogSize: 1, nodeOptions: options});
replTest.startSet();
replTest.initiate();
var mongod = replTest.getPrimary();
mongod.adminCommand({setParameter: 1, wiredTigerConcurrentWriteTransactions: 1});

var local = mongod.getDB('local');

// Start inserting documents in test.capped and local.capped capped collections.
var shells = ['test', 'local'].map(function(dbname) {
    var mydb = local.getSiblingDB(dbname);
    mydb.capped.drop();
    mydb.createCollection('capped', {capped: true, size: 20 * 1000});
    return startParallelShell('var mydb=db.getSiblingDB("' + dbname + '"); ' +
                                  '(function() { ' +
                                  '    for(var i=0; i < 10*1000; i++) { ' +
                                  '        mydb.capped.insert({ x: i }); ' +
                                  '    } ' +
                                  '})();',
                              mongod.port);
});

// The following causes inconsistent locking order in the ticket system, depending on
// timeouts to avoid deadlock.
var oldObjects = 0;
for (var i = 0; i < 1000; i++) {
    print(local.stats().objects);
    sleep(1);
}

// Wait for parallel shells to terminate and stop our replset.
shells.forEach((function(f) {
    f();
}));
replTest.stopSet();
}());

/**
 * Fills WiredTiger cache during steady state oplog application.
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({
    nodes: [
        {
            slowms: 30000,  // Don't log slow operations on primary.
        },
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
            // Constrain the storage engine cache size to make it easier to fill it up with
            // unflushed modifications.
            wiredTigerCacheSizeGB: 1,
        },
    ]
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const mydb = primary.getDB('test');
const coll = mydb.getCollection('t');

// The default WC is majority and rsSyncApplyStop failpoint will prevent satisfying any majority
// writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

const numDocs = 2;
const minDocSizeMB = 10;

for (let i = 0; i < numDocs; ++i) {
    assert.commandWorked(
        coll.save({_id: i, i: 0, x: 'x'.repeat(minDocSizeMB * 1024 * 1024)},
                  {writeConcern: {w: nodes.length, wtimeout: ReplSetTest.kDefaultTimeoutMS}}));
}
assert.eq(numDocs, coll.find().itcount());

const numUpdates = 500;
const secondary = rst.getSecondary();
const batchOpsLimit =
    assert.commandWorked(secondary.adminCommand({getParameter: 1, replBatchLimitOperations: 1}))
        .replBatchLimitOperations;
jsTestLog('Oplog application on secondary ' + secondary.host + ' is limited to ' + batchOpsLimit +
          ' operations per batch.');

jsTestLog('Buffering ' + numUpdates + ' updates to ' + numDocs + ' documents on secondary.');
assert.commandWorked(
    secondary.adminCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'alwaysOn'}));
for (let i = 0; i < numDocs; ++i) {
    for (let j = 0; j < numUpdates; ++j) {
        assert.commandWorked(coll.update({_id: i}, {$inc: {i: 1}}));
    }
}

jsTestLog('Applying updates on secondary ' + secondary.host);
assert.commandWorked(secondary.adminCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'off'}));
rst.awaitReplication();

rst.stopSet();
})();

/**
 * Fills WiredTiger cache during steady state oplog application.
 * @tags: [
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({
    nodes: [
        {
            slowms: 30000,  // Don't log slow operations on primary.
        },
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
        },
    ],
    nodeOptions: {
        // Constrain the storage engine cache size to make it easier to fill it up with
        // unflushed modifications.
        wiredTigerCacheSizeGB: 1,
    },
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const mydb = primary.getDB('test');
const coll = mydb.getCollection('t');

const numDocs = 2;
const minDocSizeMB = 10;

for (let i = 0; i < numDocs; ++i) {
    assert.commandWorked(
        coll.save({_id: i, i: 0, x: 'x'.repeat(minDocSizeMB * 1024 * 1024)},
                  {writeConcern: {w: nodes.length, wtimeout: ReplSetTest.kDefaultTimeoutMS}}));
}
assert.eq(numDocs, coll.find().itcount());

const numUpdates = 500;
const secondary = rst.getSecondary();
const batchOpsLimit =
    assert.commandWorked(secondary.adminCommand({getParameter: 1, replBatchLimitOperations: 1}))
        .replBatchLimitOperations;
jsTestLog('Oplog application on secondary ' + secondary.host + ' is limited to ' + batchOpsLimit +
          ' operations per batch.');

jsTestLog('Buffering ' + numUpdates + ' updates to ' + numDocs + ' documents on secondary.');
const session = primary.startSession();
const sessionDB = session.getDatabase(mydb.getName());
const sessionColl = sessionDB.getCollection(coll.getName());
session.startTransaction();
for (let i = 0; i < numDocs; ++i) {
    for (let j = 0; j < numUpdates; ++j) {
        assert.commandWorked(sessionColl.update({_id: i}, {$inc: {i: 1}}));
    }
}
assert.commandWorked(session.commitTransaction_forTesting());
session.endSession();

jsTestLog('Applying updates on secondary ' + secondary.host);

// If the secondary is unable to apply all the operations in the unprepared transaction within
// a single batch with the constrained cache settings, the replica set will not reach a stable
// state.
rst.awaitReplication();

rst.stopSet();
})();

/**
 * Fills WiredTiger cache during steady state oplog application.
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
'use strict';

const rst = new ReplSetTest({
    nodes: [
        {
            slowms: 30000,  // Don't log slow operations on primary.
        },
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
            // Constrain the storage engine cache size to make it easier to fill it up with
            // unflushed modifications.
            wiredTigerCacheSizeGB: 1,
        },
    ]
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const mydb = primary.getDB('test');
const coll = mydb.getCollection('t');

const numDocs = 2;
const minDocSizeMB = 10;

for (let i = 0; i < numDocs; ++i) {
    assert.commandWorked(
        coll.save({_id: i, i: 0, x: 'x'.repeat(minDocSizeMB * 1024 * 1024)},
                  {writeConcern: {w: nodes.length, wtimeout: ReplSetTest.kDefaultTimeoutMS}}));
}
assert.eq(numDocs, coll.find().itcount());

const numUpdates = 500;
let secondary = rst.getSecondary();
const batchOpsLimit =
    assert.commandWorked(secondary.adminCommand({getParameter: 1, replBatchLimitOperations: 1}))
        .replBatchLimitOperations;
jsTestLog('Oplog application on secondary ' + secondary.host + ' is limited to ' + batchOpsLimit +
          ' operations per batch.');

jsTestLog('Stopping secondary ' + secondary.host + '.');
rst.stop(1);
jsTestLog('Stopped secondary. Writing ' + numUpdates + ' updates to ' + numDocs +
          ' documents on primary ' + primary.host + '.');
const startTime = Date.now();
for (let i = 0; i < numDocs; ++i) {
    for (let j = 0; j < numUpdates; ++j) {
        assert.commandWorked(coll.update({_id: i}, {$inc: {i: 1}}));
    }
}
const totalTime = Date.now() - startTime;
jsTestLog('Wrote ' + numUpdates + ' updates to ' + numDocs + ' documents on primary ' +
          primary.host + '. Elapsed: ' + totalTime + ' ms.');

secondary = rst.restart(1);
jsTestLog('Restarted secondary ' + secondary.host +
          '. Waiting for secondary to apply updates from primary.');
rst.awaitReplication();

rst.stopSet();
})();

/**
 * SERVER-31099 WiredTiger uses lookaside (LAS) file when the cache contents are
 * pinned and can not be evicted for example in the case of a delayed secondary
 * with read concern majority. This test inserts enough data where not using the
 * lookaside file results in a stall we can't recover from.
 *
 * This test is labeled resource intensive because its total io_write is 900MB.
 * @tags: [
 *   resource_intensive,
 * ]
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");

// Skip this test if running with --nojournal and WiredTiger.
if (jsTest.options().noJournal &&
    (!jsTest.options().storageEngine || jsTest.options().storageEngine === "wiredTiger")) {
    print("Skipping test because running WiredTiger without journaling isn't a valid" +
          " replica set configuration");
    return;
}

// Skip db hash check because delayed secondary will not catch up to primary.
TestData.skipCheckDBHashes = true;

// Skip this test if not running with the "wiredTiger" storage engine.
var storageEngine = jsTest.options().storageEngine || "wiredTiger";
if (storageEngine !== "wiredTiger") {
    print('Skipping test because storageEngine is not "wiredTiger"');
    return;
} else if (jsTest.options().wiredTigerCollectionConfigString === "type=lsm") {
    // Readers of old data, such as a lagged secondary, can lead to stalls when using
    // WiredTiger's LSM tree.
    print("WT-3742: Skipping test because we're running with WiredTiger's LSM tree");
    return;
} else {
    var rst = new ReplSetTest({
        nodes: 2,
        // We are going to insert at least 100 MB of data with a long secondary
        // delay. Configure an appropriately large oplog size.
        oplogSize: 200,
    });

    var conf = rst.getReplSetConfig();
    conf.members[1].votes = 1;
    conf.members[1].priority = 0;

    rst.startSet();
    conf.members[1].secondaryDelaySecs = 24 * 60 * 60;

    // We cannot wait for a stable recovery timestamp, oplog replication, or config replication due
    // to the secondary delay.
    rst.initiateWithAnyNodeAsPrimary(conf, "replSetInitiate", {
        doNotWaitForStableRecoveryTimestamp: true,
        doNotWaitForReplication: true,
        doNotWaitForNewlyAddedRemovals: true
    });
    var primary = rst.getPrimary();  // Waits for PRIMARY state.

    // The default WC is majority and we want the delayed secondary to fall behind in replication.
    assert.commandWorked(primary.adminCommand(
        {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: 1}}));

    // Reconfigure primary with a small cache size so less data needs to be
    // inserted to make the cache full while trying to trigger a stall.
    assert.commandWorked(primary.adminCommand(
        {setParameter: 1, "wiredTigerEngineRuntimeConfig": "cache_size=100MB"}));

    var coll = primary.getCollection("test.coll");
    var bigstr = "a".repeat(4000);

    // Do not insert with a writeConcern because we want the delayed secondary
    // to fall behind in replication. This is crucial apart from having a
    // readConcern to pin updates in memory on the primary. To prevent the
    // slave from falling off the oplog, we configure the oplog large enough
    // to accomodate all the inserts.
    for (var i = 0; i < 250; i++) {
        let batch = coll.initializeUnorderedBulkOp();
        for (var j = 0; j < 100; j++) {
            batch.insert({a: bigstr});
        }
        assert.commandWorked(batch.execute());
    }
    rst.stopSet();
}
})();
