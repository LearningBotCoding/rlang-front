// Storage Node Watchdog test cases
// - Validate set parameter functions correctly.
(function() {
'use strict';
const admin = db.getSiblingDB("admin");

// Check the defaults are correct
//
function getparam(adminDb, field) {
    let q = {getParameter: 1};
    q[field] = 1;

    const ret = adminDb.runCommand(q);
    return ret[field];
}

// Verify the defaults are as we documented them
assert.eq(getparam(admin, "watchdogPeriodSeconds"), -1);

function setparam(adminDb, obj) {
    const ret = adminDb.runCommand(Object.extend({setParameter: 1}, obj));
    return ret;
}

// Negative tests
// Negative: set it too low.
assert.commandFailed(setparam(admin, {"watchdogPeriodSeconds": 1}));
// Negative: set it the min value but fail since it was not enabled.
assert.commandFailed(setparam(admin, {"watchdogPeriodSeconds": 60}));
// Negative: set it the min value + 1 but fail since it was not enabled.
assert.commandFailed(setparam(admin, {"watchdogPeriodSeconds": 61}));

// Now test MongoD with it enabled at startup
//
const conn = MongoRunner.runMongod({setParameter: "watchdogPeriodSeconds=60"});
assert.neq(null, conn, 'mongod was unable to start up');

const admin2 = conn.getDB("admin");

// Validate defaults
assert.eq(getparam(admin2, "watchdogPeriodSeconds"), 60);

// Negative: set it too low.
assert.commandFailed(setparam(admin2, {"watchdogPeriodSeconds": 1}));
// Positive: set it the min value
assert.commandWorked(setparam(admin2, {"watchdogPeriodSeconds": 60}));
// Positive: set it the min value + 1
assert.commandWorked(setparam(admin2, {"watchdogPeriodSeconds": 61}));

// Positive: disable it
assert.commandWorked(setparam(admin2, {"watchdogPeriodSeconds": -1}));

assert.eq(getparam(admin2, "watchdogPeriodSeconds"), -1);

// Positive: enable it again
assert.commandWorked(setparam(admin2, {"watchdogPeriodSeconds": 60}));

MongoRunner.stopMongod(conn);
})();

#!/bin/bash
# Script to setup charybdefs
set -euo pipefail
IFS=$'\n\t'

if [ "$#" -ne 0 ]; then
    echo "This script does not take any arguments"
    exit 1
fi

echo Start - charybdefs_setup.sh

cd /data

rm -rf /data/charybdefs
rm -rf /data/thrift

# Use the mongo branch and fork from here
git clone -b mongo_42 https://github.com/markbenvenuto/charybdefs.git

# Run the build script in the mongo branch
cd charybdefs/mongo

# Build and setup thrift and charybdefs
PATH=/opt/mongodbtoolchain/v3/bin:$PATH bash ./build.sh

echo Done - charybdefs_setup.sh

// Storage Node Watchdog - validate watchdog monitors --auditpath
//
load("jstests/watchdog/lib/wd_test_common.js");

(function() {
'use strict';

if (assert.commandWorked(db.runCommand({buildInfo: 1})).modules.includes("enterprise")) {
    let control = new CharybdefsControl("auditpath_hang");

    const auditPath = control.getMountPath();

    testFuseAndMongoD(control, {

        auditDestination: 'file',
        auditFormat: 'JSON',
        auditPath: auditPath + "/auditLog.json"
    });
}
})();

// Storage Node Watchdog - validate --dbpath
//
load("jstests/watchdog/lib/wd_test_common.js");

(function() {
'use strict';

let control = new CharybdefsControl("dbpath_hang");

const dbPath = control.getMountPath() + "/db";

testFuseAndMongoD(control, {dbpath: dbPath});
})();

// Storage Node Watchdog - validate watchdog monitors --dbpath /journal
// @tags: [requires_wiredtiger,requires_journaling]
//
load("jstests/watchdog/lib/wd_test_common.js");

(function() {
'use strict';

function trimTrailingSlash(dir) {
    if (dir.endsWith('/')) {
        return dir.substring(0, dir.length - 1);
    }

    return dir;
}

let control = new CharybdefsControl("journalpath_hang");

const journalFusePath = control.getMountPath();

const dbPath = MongoRunner.toRealDir("$dataDir/mongod-journal");

const journalLinkPath = dbPath + "/journal";

resetDbpath(dbPath);

// Create a symlink from the non-fuse journal directory to the fuse mount.
const ret = run("ln", "-s", trimTrailingSlash(journalFusePath), journalLinkPath);
assert.eq(ret, 0);

// Set noCleanData so that the dbPath is not cleaned because we want to use the journal symlink.
testFuseAndMongoD(control, {dbpath: dbPath, noCleanData: true});
})();

// Storage Node Watchdog - validate watchdog monitors --logpath
//
load("jstests/watchdog/lib/wd_test_common.js");

(function() {
'use strict';

let control = new CharybdefsControl("logpath_hang");

const logpath = control.getMountPath();

testFuseAndMongoD(control, {logpath: logpath + "/foo.log"});
})();
