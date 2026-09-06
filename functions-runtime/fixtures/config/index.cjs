'use strict';
const functions = require('firebase-functions');
const fs = require('node:fs');
if (process.env.FIXTURE_CONFIG_PID_FILE) fs.writeFileSync(process.env.FIXTURE_CONFIG_PID_FILE, String(process.pid));

exports.readConfig = functions.https.onCall(() => ({
  nested: functions.config().fixture.nested,
  projectId: JSON.parse(process.env.FIREBASE_CONFIG).projectId,
  databaseURL: JSON.parse(process.env.FIREBASE_CONFIG).databaseURL,
  localValue: process.env.LOCAL_FIXTURE_VALUE,
}));
