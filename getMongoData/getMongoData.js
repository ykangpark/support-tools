/* global db, tojson, tojsononeline, rs, print, printjson */

/* =================================================
 * getMongoData.js: MongoDB Config and Schema Report
 * =================================================
 *
 * Copyright MongoDB, Inc, 2015
 *
 * Gather MongoDB configuration and schema information.
 *
 * To execute on a locally running mongod on default port (27017) without
 * authentication, run:
 *
 *     mongo getMongoData.js > getMongoData.log
 *
 * To execute on a remote mongod or mongos with authentication, run:
 *
 *     mongo HOST:PORT/admin -u ADMIN_USER -p ADMIN_PASSWORD getMongoData.js > getMongoData.log
 *
 * For details, see
 * https://github.com/mongodb/support-tools/tree/master/getMongoData.
 *
 *
 * DISCLAIMER
 *
 * Please note: all tools/ scripts in this repo are released for use "AS
 * IS" without any warranties of any kind, including, but not limited to
 * their installation, use, or performance. We disclaim any and all
 * warranties, either express or implied, including but not limited to
 * any warranty of noninfringement, merchantability, and/ or fitness for
 * a particular purpose. We do not warrant that the technology will
 * meet your requirements, that the operation thereof will be
 * uninterrupted or error-free, or that any errors will be corrected.
 *
 * Any use of these scripts and tools is at your own risk. There is no
 * guarantee that they have been through thorough testing in a
 * comparable environment and we are not responsible for any damage
 * or data loss incurred with their use.
 *
 * You are responsible for reviewing and testing any scripts you run
 * thoroughly before use in any non-testing environment.
 *
 *
 * LICENSE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var _version = "3.1.0";

(function () {
   "use strict";
}());

// Taken from the >= 3.1.9 shell to capture print output
if (typeof print.captureAllOutput === "undefined") {
    print.captureAllOutput = function (fn, args) {
        var res = {};
        res.output = [];
        var __orig_print = print;
        print = function () {
            Array.prototype.push.apply(res.output, Array.prototype.slice.call(arguments).join(" ").split("\n"));
        };
        try {
            res.result = fn.apply(undefined, args);
        }
        finally {
            // Stop capturing print() output
            print = __orig_print;
        }
        return res;
    };
}

// Convert NumberLongs to strings to save precision
function longmangle(n) {
    if (! n instanceof NumberLong)
        return null;
    var s = n.toString();
    s = s.replace("NumberLong(","").replace(")","");
    if (s[0] == '"')
        s = s.slice(1, s.length-1)
    return s;
}

// For use in JSON.stringify to properly serialize known types
function jsonStringifyReplacer(k, v){
    if (v instanceof ObjectId)
        return { "$oid" : v.valueOf() };
    if (v instanceof NumberLong)
        return { "$numberLong" : longmangle(v) };
    if (v instanceof NumberInt)
        return v.toNumber();
    // For ISODates; the $ check prevents recursion
    if (typeof v === "string" && k.startsWith('$') == false){
        try {
            iso = ISODate(v);
            return { "$date" : iso.valueOf() };
        }
        // Nothing to do here, we'll get the return at the end
        catch(e) {}
    }
    return v;
}

// Copied from Mongo Shell
function printShardInfo(){
    section = "shard_info";
    var configDB = db.getSiblingDB("config");

    printInfo("Sharding version", function(){
        return configDB.getCollection('version').findOne();
    }, section);

    printInfo("Sharding settings", function(){
        return configDB.settings.find().sort({ _id : 1 }).toArray();
    }, section);

    printInfo("Routers", function(){
        return configDB.mongos.find().sort({ _id : 1 }).toArray();
    }, section);

    printInfo("Shards", function(){
        return configDB.shards.find().sort({ _id : 1 }).toArray();
    }, section);

    printInfo("Sharded databases", function(){
        var ret = [];
        configDB.databases.find().sort( { name : 1 } ).forEach(
            function(db) {
                doc = {};
                for (k in db) {
                    if (db.hasOwnProperty(k)) doc[k] = db[k];
                }
                if (db.partitioned) {
                    doc['collections'] = [];
                    configDB.collections.find( { _id : new RegExp( "^" +
                        RegExp.escape(db._id) + "\\." ) } ).
                        sort( { _id : 1 } ).forEach( function( coll ) {
                            if ( coll.dropped !== true ){
                                collDoc = {};
                                collDoc['_id'] = coll._id;
                                collDoc['key'] = coll.key;
                                collDoc['unique'] = coll.unique;

                                var res = configDB.chunks.aggregate(
                                    { "$match": { ns: coll._id } },
                                    { "$group": { _id: "$shard", nChunks: { "$sum": 1 } } }
                                );
                                // MongoDB 2.6 and above returns a cursor instead of a document
                                res = (res.result ? res.result : res.toArray());

                                collDoc['distribution'] = [];
                                res.forEach( function(z) {
                                    chunkDistDoc = {'shard': z._id, 'nChunks': z.nChunks};
                                    collDoc['distribution'].push(chunkDistDoc);
                                } );

                                if (_printChunkDetails) {
                                    collDoc['chunks'] = [];
                                    configDB.chunks.find( { "ns" : coll._id } ).sort( { min : 1 } ).forEach(
                                        function(chunk) {
                                            chunkDoc = {}
                                            chunkDoc['min'] = chunk.min;
                                            chunkDoc['max'] = chunk.max;
                                            chunkDoc['shard'] = chunk.shard;
                                            chunkDoc['jumbo'] = chunk.jumbo ? true : false;
                                            collDoc['chunks'].push(chunkDoc);
                                        }
                                    );
                                }

                                collDoc['tags'] = [];
                                configDB.tags.find( { ns : coll._id } ).sort( { min : 1 } ).forEach(
                                    function(tag) {
                                        tagDoc = {}
                                        tagDoc['tag'] = tag.tag;
                                        tagDoc['min'] = tag.min;
                                        tagDoc['max'] = tag.max;
                                        collDoc['tags'].push(tagDoc);
                                    }
                                );
                                doc['collections'].push(collDoc);
                            }
                        }
                    );
                }
                ret.push(doc);
            }
        );
        return ret;
    }, section);

    printInfo('Balancer status', function(){return db.adminCommand({balancerStatus: 1})}, section);

    if (sh.getRecentMigrations) { // Function does not exist in older shell versions (2.6 and below)
        printInfo('Recent chunk migrations', function(){return sh.getRecentMigrations()}, section);
    } else {
        if (! _printJSON) print("\n** Recent chunk migrations: n/a")
    }

    if (sh.getRecentFailedRounds) { // Function does not exist in older shell versions (2.6 and below)
        printInfo('Recent failed balancer rounds', function(){return sh.getRecentFailedRounds()}, section);
    } else {
        if (! _printJSON) print("\n** Recent failed balancer rounds: n/a")
    }
}

function printInfo(message, command, section, printCapture, commandParameters) {
    var result = false;
    if (typeof printCapture === "undefined") var printCapture = false;
    if (! _printJSON) print("\n** " + message + ":");
    startTime = new Date();
    try {
        if (printCapture) {
            result = print.captureAllOutput(command);
        } else {
            result = command();
        }
        err = null
    } catch(err) {
        if (! _printJSON) {
            print("Error running '" + command + "':");
            print(err);
        } else {
            throw("Error running '" + command + "': " + err);
        }
    }
    endTime = new Date();
    doc = {};
    doc['command'] = command.toString();
    doc['error'] = err;
    doc['host'] = _host;
    doc['ref'] = _ref;
    doc['tag'] = _tag;
    doc['output'] = result;
    if (typeof(section) !== "undefined") {
        doc['section'] = section;
        doc['subsection'] = message.toLowerCase().replace(/ /g, "_");
    } else {
        doc['section'] = message.toLowerCase().replace(/ /g, "_");
    }
    doc['ts'] = {'start': startTime, 'end': endTime};
    doc['version'] = _version;
    if (typeof commandParameters !== undefined) {
      doc['commandParameters'] = commandParameters
    }
    _output.push(doc);
    if (! _printJSON) printjson(result);
    return result;
}

function calculateWritesPerSecond(initialServerStatus) {
  const initialInsert = initialServerStatus.opcounters.insert;
  const initialUpdate = initialServerStatus.opcounters.update;
  const initialDelete = initialServerStatus.opcounters.delete;
  const initialWrites = initialInsert + initialUpdate + initialDelete;
  sleep(1000);
  const newServerStatus = db.serverStatus();
  const newInsert = newServerStatus.opcounters.insert;
  const newUpdate = newServerStatus.opcounters.update;
  const newDelete = newServerStatus.opcounters.delete;
  const newWrites = newInsert + newUpdate + newDelete;
  return newWrites - initialWrites;
}

function printServerInfo() {
    section = "server_info";
    printInfo('Shell version',      version, section);
    printInfo('Shell hostname',     hostname, section);
    printInfo('db',                 function(){return db.getName()}, section);
    let serverStatus = db.serverStatus();
    const writesPerSecond = calculateWritesPerSecond(serverStatus);
    printInfo('Writes Per Second', function() { 
      return writesPerSecond
    }, section);

    // Update serverStatus to get latest status
    serverStatus = db.serverStatus();
    printInfo('Server status info', function() {
      return serverStatus
    }, section);
    printInfo('Host info',          function(){return db.hostInfo()}, section);
    printInfo('Command line info',  function(){return db.serverCmdLineOpts()}, section);
    printInfo('Server build info',  function(){return db.serverBuildInfo()}, section);
    printInfo('Server parameters',  function(){return db.adminCommand({getParameter: '*'})}, section);
}

function printReplicaSetInfo() {
    section = "replicaset_info";
    printInfo('Replica set config', function(){return rs.conf()}, section);
    printInfo('Replica status',     function(){return rs.status()}, section);
    printInfo('Replica info',       function(){return db.getReplicationInfo()}, section);
    printInfo('Replica slave info', function(){return db.printSlaveReplicationInfo()}, section, true);
}

function printUserAuthInfo() {
  section = "user_auth_info";
  db = db.getSiblingDB('admin');
  if (typeof db.system.users.countDocuments === 'function') {
    printInfo('Database user count', function(){return db.system.users.countDocuments({})}, section);
    printInfo('Custom role count', function(){return db.system.roles.countDocuments({})}, section);
  } else {
    printInfo('Database user count', function(){return db.system.users.count()}, section);
    printInfo('Custom role count', function(){return db.system.roles.count()}, section);
  }
}

function printDriverVersions() {
  section = 'driverVersions';
  printInfo('Driver Versions', function() {
    return db.getSiblingDB('admin')
        .aggregate([
          {
            $currentOp: {
              allUsers: true,
              idleConnections: true,
              idleSessions: true,
              localOps: true
            },
          },
          {$match: {clientMetadata: {$exists: true}}},
          {
            $group: {
              _id: {
                application: '$clientMetadata.application',
                driver: '$clientMetadata.driver',
                platform: '$clientMetadata.platform',
                os: '$clientMetadata.os',
              },
              count: {$sum: 1},
              last: {$max: '$currentOpTime'},
            },
          },
        ])
        .toArray();
  }, section);
}

function printOplogChurn(isMongoS, topology) {
  const section = 'Oplog Churn'
  
  if (isMongoS === false && topology !== 'standalone') {
    (typeof readPref === 'undefined') &&
        !!(readPref = (db.isMaster().secondary == false) ?
               'primaryPreferred' :
               'secondaryPreferred');
    let opSize = 0, docs = 0, date = new Date();
    let intervalHrs = 1;
    let t2 = Math.floor(date.getTime() / 1000.0), d2 = date.toISOString(),
        t1 = Math.floor(date.setHours(date.getHours() - intervalHrs) / 1000.0),
        d1 = date.toISOString(), $match = {
          '$match': {'ts': {'$gt': Timestamp(t1, 0), '$lte': Timestamp(t2, 0)}}
        },
        $project = {'$unset': '_id'};
    let pipeline = [$match, $project], options = {
      'allowDiskUse': true,
      'cursor': {'batchSize': 0},
      'comment': 'Oplog churn calculation'
    };

    db.getMongo().setReadPref(readPref);
    let oplog = db.getSiblingDB('local').getCollection('oplog.rs');

    pipeline.push({
      '$group': {
        '_id': null,
        '_bsonDataSize': {'$sum': {'$bsonSize': '$$ROOT'}},
        '_documentCount': {'$sum': 1}
      }
    });

    ([{'_bsonDataSize': opSize, '_documentCount': docs}] =
         oplog.aggregate(pipeline, options).toArray());

    let {
      'wiredTiger': {
        creationString,
        'block-manager': {
          'file bytes available for reuse': blocksFree,
          'file size in bytes': storageSize
        }
      },
      size,
      internalPageSize =
          (creationString.match(/internal_page_max=(\d+)/)[1] * 1024)
    } = oplog.stats();
    let overhead = internalPageSize;
    let ratio = +((size / (storageSize - blocksFree - overhead)).toFixed(2));
    let oplogChurn = opSize / ratio / intervalHrs;

    printInfo('Oplog Churn Rate', function() {
      return oplogChurn;
    }, section);
  } else {
    printInfo('Oplog Churn Rate', function() {
      'Oplog does not exist on standalone nodes or mongos';
    }, section);
  }
}

// find all QE collections
// Outputs the following JSON:
/*
[
    {
        namespace: "qe_db.qe_coll",
        escCollectionInfo: {
            namespace: "qe_db.enxcol_.qe_coll.esc",
            exists: true/false,
            isClusteredCollection: true/false,
            documentCount: 1078,
            anchorCount: 78,
            nonAnchorCount: 1000,
        },
        ecocCollectionInfo: {
            namespace: "qe_db.enxcol_.qe_coll.ecoc",
            exists: true/false,
            documentCount: 10780,
            isClusteredCollection: true/false,
            compactTempCollectionExists: true/false,
        },
        safeContentInfo: {
            safeContentIndexed: true/false,
            indexedEncryptedDocumentsWithMissingSafeContentTags: 778,
        },
        shardingInfo: {
            isSharded: true/false,
            shardingStatus: {}
        },
        fields: {...},
    },
    {
        ...
    },
    ...
]
*/
function collectQueryableEncryptionInfo(isMongoS) {
    const output = [];
    const dbs = db.getMongo().getDBs();
    if (!dbs.databases) {
        return output;
    }
    const getAuxiliaryCollectionInfo = function(db, collName) {
        let collInfos = db.getCollectionInfos({name: collName});
        let exists = (collInfos.length > 0);
        let isClusteredCollection = exists && collInfos[0].hasOwnProperty("options") &&
            collInfos[0].options.hasOwnProperty("clusteredIndex");
        return {exists, isClusteredCollection};
    };

    dbs.databases.forEach(function(someDbInfo) {
        const someDb = db.getSiblingDB(someDbInfo.name);
        const qeCollInfos = someDb.getCollectionInfos(
            {"type": "collection", "options.encryptedFields": {$exists: true}});

        qeCollInfos.forEach(function(someCollInfo) {
            const edcColl = someDb.getCollection(someCollInfo.name);
            const qeEntry = {};

            qeEntry["namespace"] = edcColl.getFullName();
            qeEntry["escCollectionInfo"] = (() => {
                const escColl =
                    someDb.getCollection(someCollInfo.options.encryptedFields.escCollection);
                const {exists, isClusteredCollection} =
                    getAuxiliaryCollectionInfo(someDb, escColl.getName());
                if (!exists) {
                    return {namespace: escColl.getFullName(), exists};
                }
                const documentCount = escColl.countDocuments({});
                const anchorCount = escColl.countDocuments({"value": {"$exists": true}});
                const nonAnchorCount = escColl.countDocuments({"value": {"$exists": false}});
                return {
                    namespace: escColl.getFullName(),
                    exists,
                    isClusteredCollection,
                    documentCount,
                    anchorCount,
                    nonAnchorCount,
                };
            })();
            qeEntry["ecocCollectionInfo"] = (() => {
                const ecocColl =
                    someDb.getCollection(someCollInfo.options.encryptedFields.ecocCollection);
                const {exists, isClusteredCollection} =
                    getAuxiliaryCollectionInfo(someDb, ecocColl.getName());
                if (!exists) {
                    return {namespace: ecocColl.getFullName(), exists};
                }
                const documentCount = ecocColl.countDocuments({});
                const compactTempCollectionExists =
                    someDb.getCollectionInfos({name: ecocColl.getName() + ".compact"}).length > 0;
                return {
                    namespace: ecocColl.getFullName(),
                    exists,
                    isClusteredCollection,
                    documentCount,
                    compactTempCollectionExists
                };
            })();
            qeEntry["safeContentInfo"] = (() => {
                const safeContentIndexed = (edcColl.getIndexes().find(
                    doc => doc.key.hasOwnProperty("__safeContent__")) !== undefined);
                const encryptedFieldPaths = someCollInfo.options.encryptedFields.fields.filter(
                    field => field.hasOwnProperty("queries")).map(field => field.path);
                const missingTags = edcColl.aggregate([
                    {$match: {$and: [
                        {$or: encryptedFieldPaths.map((field) => {return {[field]: {$exists: true}};})},
                        {$or: [{__safeContent__: {$exists: false}}, {__safeContent__: {$size: 0}} ]}
                    ]}},
                    {$count: "count"}
                ]).toArray();
                const indexedEncryptedDocumentsWithMissingSafeContentTags =
                    (missingTags.length > 0) ? missingTags[0].count : 0;
                return {
                    safeContentIndexed,
                    indexedEncryptedDocumentsWithMissingSafeContentTags
                };
            })();
            if (isMongoS) {
                qeEntry["shardingInfo"] = (() => {
                    const configDB = someDb.getSiblingDB("config");
                    let shardDoc = configDB.collections.findOne({_id: edcColl.getFullName()});
                    if (!shardDoc) {
                        return {isSharded: false};
                    }
                    return {
                        isSharded: true,
                        shardKey: shardDoc.key,
                        unique: shardDoc.unique,
                        balancing: !shardDoc.noBalance,
                    };
                })();
            }
            qeEntry["fields"] = someCollInfo.options.encryptedFields.fields;
            output.push(qeEntry);
        });
    });
    return output;
}

function updateDataInfoAsIncomplete(isMongoS) {
  for (i = 0; i < _output.length; i++) {
    if(_output[i].section != "data_info") { continue; }
    _output[i].subsection = "INCOMPLETE_"+ _output[i].subsection;
  }
}

function printDataInfo(isMongoS) {
    section = "data_info";
    var dbs = printInfo('List of databases', function(){return db.getMongo().getDBs()}, section);
    var collections_counter = 0;
    
    printInfo('Number of databases', function() {
      return dbs.databases.length;
    });

    let specialCollectionTypes = {capped: 0, timeseries: 0};

    let indexTypes = {
      '2d': {
        single: 0,
        compound: 0,
      },
      '2dsphere': {
        single: 0,
        compound: 0,
      },
      text: {
        single: 0,
        compound: 0,
      },
      hashed: {
        single: 0,
        compound: 0,
      },
      wildcard: {
        single: 0,
        compound: 0,
      },
      standard: {
        single: 0,
        compound: 0,
      },
    };

    if (dbs.databases) {
        dbs.databases.forEach(function(mydb) {
            var collections = printInfo("List of collections for database '"+ mydb.name +"'",
                function() {
                    var collectionNames = []

                    // Filter out views
                    db.getSiblingDB(mydb.name).getCollectionInfos({"type": "collection"}).forEach(function(collectionInfo) {
                        collectionNames.push(collectionInfo['name']);

                        if (collectionInfo.options.capped === true) {
                          specialCollectionTypes['capped'] += 1;
                        } else if (collectionInfo.type === 'timeseries') {
                          specialCollectionTypes['timeseries'] += 1;
                        }
                    })

                    // Filter out the collections with the "system." prefix in the system databases
                    if (mydb.name == "config" || mydb.name == "local" || mydb.name == "admin") {
                        return collectionNames.filter(function (str) { return str.indexOf("system.") != 0; });
                    } else {
                        return collectionNames;
                    }
                }, section);

            printInfo('Database stats (MB)',
                      function(){return db.getSiblingDB(mydb.name).stats(1024*1024)}, section);
            if (!isMongoS) {
                printInfo("Database profiler for database '"+ mydb.name + "'",
                          function(){return db.getSiblingDB(mydb.name).getProfilingStatus()}, section, false, {"db": mydb.name})
            }

            if (collections) {
                collections.forEach(function(col) {

                  const collectionObject =
                      db.getSiblingDB(mydb.name).getCollection(col);

                    printInfo('Collection stats (MB)',
                              function(){return db.getSiblingDB(mydb.name).getCollection(col).stats(1024*1024)}, section);
                    collections_counter++;
                    if (collections_counter > _maxCollections) {
                        var err_msg = 'Already asked for stats on '+collections_counter+' collections ' +
                          'which is above the max allowed for this script. No more database and ' +
                          'collection-level stats will be gathered, so the overall data is ' +
                          'incomplete. '
                        if (_printJSON) {
                          err_msg += 'The "subsection" fields have been prefixed with "INCOMPLETE_" ' +
                          'to indicate that partial data has been outputted.'
                        }

                        throw {
                          name: 'MaxCollectionsExceededException',
                          message: err_msg
                        }
                    }
                    if (mydb.name == "config" || mydb.name == "local" || mydb.name == "admin") {
                        // The following command doesn't make any sense to config, local and admin
                        return
                    }
                    if (isMongoS) {
                        printInfo('Shard distribution', function() {
                            try {
                                var result = db.getSiblingDB(mydb.name).getCollection(col).getShardDistribution();
                            } catch(e) {
                                var result = '';
                            }
                            return result;
                        }, section, true);
                    }

                    const indexes = collectionObject.getIndexes();
                    printInfo('Indexes',
                              function(){return indexes}, section, false, {"db": mydb.name, "collection": col});
                    
                              indexes.forEach((index) => {
                                if (index.key) {
                                  let indexShape =
                                      (Object.keys(index.key).length > 1) ? 'compound' : 'single';
                                  let indexType =
                                      Object.values(index.key).find(x => typeof x === 'string');
                                  if (indexType === undefined) {
                                    if (Object.keys(index.key).some(
                                            (key) => key === '$**' || key.endsWith('.$**'))) {
                                      indexType = 'wildcard';
                                    } else {
                                      indexType = 'standard';
                                    }
                                  }

                                  // Single text indexes keys are stored as { _fts: 'text', _ftsx:
                                  // 1 } and so compound index keys will be longer
                                  if (indexType === 'text' && Object.keys(index.key).length < 3) {
                                    indexShape = 'single';
                                  }
                                  indexTypes[indexType][indexShape] += 1;
                                } else {
                                  indexesNotCounted.indexes.push(index);
                                }
                              });

                    printInfo('Index Stats',
                              function(){
                                var res = db.getSiblingDB(mydb.name).runCommand( {
                                  aggregate: col,
                                  pipeline: [
                                    {$indexStats: {}},
                                    {$group: {_id: "$key", stats: {$push: {accesses: "$accesses.ops", host: "$host", since: "$accesses.since"}}}},
                                    {$project: {key: "$_id", stats: 1, _id: 0}}
                                  ],
                                  cursor: {}
                                });

                                //It is assumed that there always will be a single batch as collections
                                //are limited to 64 indexes and usage from all shards is grouped
                                //into a single document
                                if (res.hasOwnProperty('cursor') && res.cursor.hasOwnProperty('firstBatch')) {
                                  res.cursor.firstBatch.forEach(
                                    function(d){
                                      d.stats.forEach(
                                        function(d){
                                          d.since = d.since.toUTCString();
                                        })
                                    });
                                }

                                return res;
                              }, section);
                });
            }
        });
    }

    printInfo('Special Collection Types', function() {
      return specialCollectionTypes;
    }, section);

    printInfo('Index Types', function() {
      return indexTypes;
    }, section);

    printInfo("Queryable Encryption Info", function(){
        return collectQueryableEncryptionInfo(isMongoS);}, section, false);
}

function printShardOrReplicaSetInfo() {
    let topology;
    section = "shard_or_replicaset_info";
    printInfo('isMaster', function(){return db.isMaster()}, section);
    var state;

    // Compatible with mongosh
    try {
        var stateInfo = rs.status();
    } catch (e) {
        var stateInfo = e.errorResponse;
    }
    if (stateInfo.ok) {
        stateInfo.members.forEach( function( member ) { if ( member.self ) { state = member.stateStr; } } );
        if ( !state ) state = stateInfo.myState;
    } else {
        var info = stateInfo.info;
        if ( info && info.length < 20 ) {
            state = info; // "mongos", "configsvr"
        }
        if ( ! state ) {
          state = "standalone";
          topology = 'standalone';
        }
    }
    if (! _printJSON) print("\n** Connected to " + state);
    if (state == "mongos") {
        topology = 'Sharded Cluster';
        printShardInfo();
    } else if (state != "standalone" && state != "configsvr") {
        if (state == "SECONDARY" || state == 2) {
            if (rs.secondaryOk) {
                rs.secondaryOk();
            } else {
                rs.slaveOk();
            }
        }
        topology = 'Replica Set';
        printReplicaSetInfo();
    }
    return topology;
}

if (typeof _printJSON === "undefined") var _printJSON = true;
if (typeof _printChunkDetails === "undefined") var _printChunkDetails = false;
if (typeof _ref === "undefined") var _ref = null;

// Limit the number of collections this script gathers stats on in order
// to avoid the possibility of running out of file descriptors. This has
// been set to an extremely conservative number but can be overridden
// by setting _maxCollections to a higher number prior to running this
// script.
if (typeof _maxCollections === "undefined") var _maxCollections = 2500;

// Compatibility issues between mongo and mongosh
if (typeof hostname === 'undefined') hostname = function() {return os.hostname();}
if (typeof RegExp.escape === 'undefined') {
    RegExp.escape = function (string) {
        return string.replace(/[/\-\\^$*+?.()|[\]{}]/g, '\\$&');
    }
}
if (typeof db.printSecondaryReplicationInfo === 'function') {
    db.printSlaveReplicationInfo = db.printSecondaryReplicationInfo;
}

var _total_collection_ct = 0;
var _output = [];
var _tag = ObjectId();
if (! _printJSON) {
    print("================================");
    print("MongoDB Config and Schema Report");
    print("getMongoData.js version " + _version);
    print("================================");
}

var _host = hostname();

try {
  printServerInfo();
  let topology = printShardOrReplicaSetInfo();
  let isMongoS = false;
  if (topology === 'Sharded Cluster') {
    isMongoS = true;
  }
  printInfo('Topology', function() {
    return topology;
  }, 'topology');
  printUserAuthInfo();
  printDataInfo(isMongoS);
  printDriverVersions();
  printOplogChurn(isMongoS, topology);
} catch(e) {
    // To ensure that the operator knows there was an error, print the error
    // even when outputting JSON to make it invalid JSON.
    print('\nERROR: '+e.message);

    if (e.name === 'MaxCollectionsExceededException') {
      // Prefix the "subsection" fields with "INCOMPLETE_" to make
      // it clear that the database and collection info are likely to be
      // incomplete.
      updateDataInfoAsIncomplete(isMongoS);
    } else {
      quit(1);
   }
}

// Print JSON output
if (_printJSON) print(JSON.stringify(_output, jsonStringifyReplacer, 4));
