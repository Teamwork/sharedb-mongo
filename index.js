const async = require('async');
const mongodb = require('mongodb');
const DB = require('@teamwork/sharedb').DB;


class ShareDbMongo extends DB {
  constructor(mongo, options) {
    super();

    if (typeof mongo === 'object') {
      options = mongo;
      mongo = options.mongo;
    }
    if (!options) options = {};

    // pollDelay is a dodgy hack to work around race conditions replicating the
    // data out to the polling target secondaries. If a separate db is specified
    // for polling, it defaults to 300ms
    this.pollDelay = (options.pollDelay != null) ? options.pollDelay :
      (options.mongoPoll) ? 300 : 0;

    // By default, we create indexes on any ops collection that is used
    this.disableIndexCreation = options.disableIndexCreation || false;

    // The getOps() method depends on a separate operations collection, and that
    // collection should have an index on the operations stored there. We could
    // ask people to make these indexes themselves, but by default the mongo
    // driver will do it automatically. This approach will leak memory relative
    // to the number of collections you have. This should be OK, as we are not
    // expecting thousands of mongo collections.

    // Map from collection name -> true for op collections we've ensureIndex'ed
    this.opIndexes = {};

    // Allow $while and $mapReduce queries. These queries let you run arbitrary
    // JS on the server. If users make these queries from the browser, there's
    // security issues.
    this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || false;

    // Aggregate queries are less dangerous, but you can use them to access any
    // data in the mongo database.
    this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries || false;

    // Track whether the close method has been called
    this.closed = false;

    if (typeof mongo === 'string' || typeof mongo === 'function') {
      // We can only get the mongodb client instance in a callback, so
      // buffer up any requests received in the meantime
      this.mongo = null;
      this.mongoPoll = null;
      this.pendingConnect = [];
      this._connect(mongo, options);
    } else {
      throw new Error('deprecated: pass mongo as url string or function with callback');
    }
  }

  getCollection(collectionName, callback) {
    // Check the collection name
    const err = this.validateCollectionName(collectionName);
    if (err) return callback(err);
    // Gotcha: calls back sync if connected or async if not
    this.getDbs((err, mongo) => {
      if (err) return callback(err);
      const collection = mongo.db().collection(collectionName);
      return callback(null, collection);
    });
  }

  _getCollectionPoll(collectionName, callback) {
    // Check the collection name
    const err = this.validateCollectionName(collectionName);
    if (err) return callback(err);
    // Gotcha: calls back sync if connected or async if not
    this.getDbs((err, mongo, mongoPoll) => {
      if (err) return callback(err);
      const collection = (mongoPoll || mongo).db().collection(collectionName);
      return callback(null, collection);
    });
  }

  getCollectionPoll(collectionName, callback) {
    if (this.pollDelay) {
      setTimeout(() => {
        this._getCollectionPoll(collectionName, callback);
      }, this.pollDelay);
      return;
    }
    this._getCollectionPoll(collectionName, callback);
  }

  getDbs(callback) {
    if (this.closed) {
      const err = ShareDbMongo.alreadyClosedError();
      return callback(err);
    }
    // We consider ourself ready to reply if this.mongo is defined and don't check
    // this.mongoPoll, since it is optional and is null by default. Thus, it's
    // important that these two properties are only set together synchronously
    if (this.mongo) return callback(null, this.mongo, this.mongoPoll);
    this.pendingConnect.push(callback);
  }

  _flushPendingConnect() {
    const pendingConnect = this.pendingConnect;
    this.pendingConnect = null;
    for (let i = 0; i < pendingConnect.length; i++) {
      pendingConnect[i](null, this.mongo, this.mongoPoll);
    }
  }

  _mongodbOptions(options) {
    if(options instanceof Object) {
      return Object.assign(Object.assign({}, options.mongoOptions), { useNewUrlParser: true })
    } else {
      return  { useNewUrlParser: true };
    }
  }

  _connect(mongo, options) {
    // Create the mongo connection client connections if needed
    //
    // Throw errors in this function if we fail to connect, since we aren't
    // implementing a way to retry
    if (options.mongoPoll) {
      let tasks;
      if (typeof mongo === 'function') {
        tasks = {mongo, mongoPoll: options.mongoPoll};
      } else {
        tasks = {
          mongo: (parallelCb) => {
            mongodb.connect(mongo, this._mongodbOptions(options.mongoOptions), parallelCb);
          },
          mongoPoll: (parallelCb) => {
            mongodb.connect(options.mongoPoll, this._mongodbOptions(options.mongoPollOptions), parallelCb);
          }
        };
      }
      async.parallel(tasks, (err, results) => {
        if (err) throw err;
        this.mongo = results.mongo;
        this.mongoPoll = results.mongoPoll;
        this._flushPendingConnect();
      });
      return;
    }
    const finish = (err, db) => {
      if (err) throw err;
      this.mongo = db;
      this._flushPendingConnect();
    };
    if (typeof mongo === 'function') {
      mongo(finish);
      return;
    }
    mongodb.connect(mongo, this._mongodbOptions(options), finish);
  }

  close(callback) {
    if (!callback) {
      callback = err => {
        if (err) throw err;
      };
    }
    this.getDbs((err, mongo, mongoPoll) => {
      if (err) return callback(err);
      this.closed = true;
      mongo.close(err => {
        if (err) return callback(err);
        if (!mongoPoll) return callback();
        mongoPoll.close(callback);
      });
    });
  }

  // **** Commit methods

  commit(collectionName, id, op, snapshot, options, callback) {
    this._writeOp(collectionName, id, op, snapshot, (err, result) => {
      if (err) return callback(err);
      const opId = result.insertedId;
      this._writeSnapshot(collectionName, id, snapshot, opId, (err, succeeded) => {
        if (succeeded) return callback(err, succeeded);
        // Cleanup unsuccessful op if snapshot write failed. This is not
        // necessary for data correctness, but it gets rid of clutter
        this._deleteOp(collectionName, opId, removeErr => {
          callback(err || removeErr, succeeded);
        });
      });
    });
  }

  _writeOp(collectionName, id, op, snapshot, callback) {
    if (typeof op.v !== 'number') {
      const err = ShareDbMongo.invalidOpVersionError(collectionName, id, op.v);
      return callback(err);
    }
    this.getOpCollection(collectionName, (err, opCollection) => {
      if (err) return callback(err);
      const doc = shallowClone(op);
      doc.d = id;
      doc.o = snapshot._opLink;
      opCollection.insertOne(doc, callback);
    });
  }

  _deleteOp(collectionName, opId, callback) {
    this.getOpCollection(collectionName, (err, opCollection) => {
      if (err) return callback(err);
      opCollection.deleteOne({_id: opId}, callback);
    });
  }

  _writeSnapshot(collectionName, id, snapshot, opLink, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const doc = castToDoc(id, snapshot, opLink);
      if (doc._v === 1) {
        collection.insertOne(doc, (err, result) => {
          if (err) {
            // Return non-success instead of duplicate key error, since this is
            // expected to occur during simultaneous creates on the same id
            if (err.code === 11000 && /\b_id_\b/.test(err.message)) {
              return callback(null, false);
            }
            return callback(err);
          }
          callback(null, true);
        });
      } else {
        collection.replaceOne({_id: id, _v: doc._v - 1}, doc, (err, result) => {
          if (err) return callback(err);
          const succeeded = !!result.modifiedCount;
          callback(null, succeeded);
        });
      }
    });
  }

  // **** Snapshot methods

  getSnapshot(collectionName, id, fields, options, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const query = {_id: id};
      const projection = getProjection(fields, options);
      collection.find(query).limit(1).project(projection).next((err, doc) => {
        if (err) return callback(err);
        const snapshot = (doc) ? castToSnapshot(doc) : new MongoSnapshot(id, 0, null, undefined);
        callback(null, snapshot);
      });
    });
  }

  getSnapshotBulk(collectionName, ids, fields, options, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const query = {_id: {$in: ids}};
      const projection = getProjection(fields, options);
      collection.find(query).project(projection).toArray((err, docs) => {
        if (err) return callback(err);
        const snapshotMap = {};
        for (let i = 0; i < docs.length; i++) {
          const snapshot = castToSnapshot(docs[i]);
          snapshotMap[snapshot.id] = snapshot;
        }
        const uncreated = [];
        for (let i = 0; i < ids.length; i++) {
          const id = ids[i];
          if (snapshotMap[id]) continue;
          snapshotMap[id] = new MongoSnapshot(id, 0, null, undefined);
        }
        callback(null, snapshotMap);
      });
    });
  }

  // **** Oplog methods

  // Overwrite me if you want to change this behavior.
  getOplogCollectionName(collectionName) {
    return `o_${collectionName}`;
  }

  validateCollectionName(collectionName) {
    if (
      collectionName === 'system' || (
        collectionName[0] === 'o' &&
        collectionName[1] === '_'
      )
    ) {
      return ShareDbMongo.invalidCollectionError(collectionName);
    }
  }

  // Get and return the op collection from mongo, ensuring it has the op index.
  getOpCollection(collectionName, callback) {
    this.getDbs((err, mongo) => {
      if (err) return callback(err);
      const name = this.getOplogCollectionName(collectionName);
      const collection = mongo.db().collection(name);
      // Given the potential problems with creating indexes on the fly, it might
      // be preferable to disable automatic creation
      if (this.disableIndexCreation) {
        return callback(null, collection);
      }
      if (this.opIndexes[collectionName]) {
        return callback(null, collection);
      }
      // WARNING: Creating indexes automatically like this is quite dangerous in
      // production if we are starting with a lot of data and no indexes
      // already. If new indexes were added or definition of these indexes were
      // changed, users upgrading this module could unsuspectingly lock up their
      // databases. If indexes are created as the first ops are added to a
      // collection this won't be a problem, but this is a dangerous mechanism.
      // Perhaps we should only warn instead of creating the indexes, especially
      // when there is a lot of data in the collection.
      collection.createIndex({d: 1, v: 1}, {background: true}, err => {
        if (err) return callback(err);
        collection.createIndex({src: 1, seq: 1, v: 1}, {background: true}, err => {
          if (err) return callback(err);
          this.opIndexes[collectionName] = true;
          callback(null, collection);
        });
      });
    });
  }

  getOpsToSnapshot(collectionName, id, from, snapshot, options, callback) {
    if (snapshot._opLink == null) {
      const err = ShareDbMongo.missingLastOperationError(collectionName, id);
      return callback(err);
    }
    this._getOps(collectionName, id, from, options, (err, ops) => {
      if (err) return callback(err);
      const filtered = getLinkedOps(ops, null, snapshot._opLink);
      err = checkOpsFrom(collectionName, id, filtered, from);
      if (err) return callback(err);
      callback(null, filtered);
    });
  }

  getOps(collectionName, id, from, to, options, callback) {
    this._getSnapshotOpLink(collectionName, id, (err, doc) => {
      if (err) return callback(err);
      if (doc) {
        if (isCurrentVersion(doc, from)) {
          return callback(null, []);
        }
        err = doc && checkDocHasOp(collectionName, id, doc);
        if (err) return callback(err);
      }
      this._getOps(collectionName, id, from, options, (err, ops) => {
        if (err) return callback(err);
        const filtered = filterOps(ops, doc, to);
        err = checkOpsFrom(collectionName, id, filtered, from);
        if (err) return callback(err);
        callback(null, filtered);
      });
    });
  }

  getOpsBulk(collectionName, fromMap, toMap, options, callback) {
    const ids = Object.keys(fromMap);
    this._getSnapshotOpLinkBulk(collectionName, ids, (err, docs) => {
      if (err) return callback(err);
      const docMap = getDocMap(docs);
      // Add empty array for snapshot versions that are up to date and create
      // the query conditions for ops that we need to get
      const conditions = [];
      const opsMap = {};
      for (let i = 0; i < ids.length; i++) {
        const id = ids[i];
        const doc = docMap[id];
        const from = fromMap[id];
        if (doc) {
          if (isCurrentVersion(doc, from)) {
            opsMap[id] = [];
            continue;
          }
          var err = checkDocHasOp(collectionName, id, doc);
          if (err) return callback(err);
        }
        const condition = getOpsQuery(id, from);
        conditions.push(condition);
      }
      // Return right away if none of the snapshot versions are newer than the
      // requested versions
      if (!conditions.length) return callback(null, opsMap);
      // Otherwise, get all of the ops that are newer
      this._getOpsBulk(collectionName, conditions, options, (err, opsBulk) => {
        if (err) return callback(err);
        for (let i = 0; i < conditions.length; i++) {
          const id = conditions[i].d;
          const ops = opsBulk[id];
          const doc = docMap[id];
          const from = fromMap[id];
          const to = toMap && toMap[id];
          const filtered = filterOps(ops, doc, to);
          var err = checkOpsFrom(collectionName, id, filtered, from);
          if (err) return callback(err);
          opsMap[id] = filtered;
        }
        callback(null, opsMap);
      });
    });
  }

  _getOps(collectionName, id, from, options, callback) {
    this.getOpCollection(collectionName, (err, opCollection) => {
      if (err) return callback(err);
      const query = getOpsQuery(id, from);
      // Exclude the `d` field, which is only for use internal to livedb-mongo.
      // Also exclude the `m` field, which can be used to store metadata on ops
      // for tracking purposes
      const projection = (options && options.metadata) ? {d: 0} : {d: 0, m: 0};
      const sort = {v: 1};
      opCollection.find(query).project(projection).sort(sort).toArray(callback);
    });
  }

  _getOpsBulk(collectionName, conditions, options, callback) {
    this.getOpCollection(collectionName, (err, opCollection) => {
      if (err) return callback(err);
      const query = {$or: conditions};
      // Exclude the `m` field, which can be used to store metadata on ops for
      // tracking purposes
      const projection = (options && options.metadata) ? null : {m: 0};
      const stream = opCollection.find(query).project(projection).stream();
      readOpsBulk(stream, callback);
    });
  }

  _getSnapshotOpLink(collectionName, id, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const query = {_id: id};
      const projection = {_id: 0, _o: 1, _v: 1};
      collection.find(query).limit(1).project(projection).next(callback);
    });
  }

  _getSnapshotOpLinkBulk(collectionName, ids, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const query = {_id: {$in: ids}};
      const projection = {_o: 1, _v: 1};
      collection.find(query).project(projection).toArray(callback);
    });
  }

  // **** Query methods

  _query(collection, inputQuery, projection, callback) {
    const parsed = this._getSafeParsedQuery(inputQuery, callback);
    if (!parsed) return;

    // Collection operations such as $aggregate run on the whole
    // collection. Only one operation is run. The result goes in the
    // "extra" argument in the callback.
    if (parsed.collectionOperationKey) {
      collectionOperationsMap[parsed.collectionOperationKey](
        collection,
        parsed.query,
        parsed.collectionOperationValue,
        (err, extra) => {
          if (err) return callback(err);
          callback(null, [], extra);
        }
      );
      return;
    }

    // No collection operations were used. Create an initial cursor for
    // the query, that can be transformed later.
    let cursor = collection.find(parsed.query).project(projection);

    // Cursor transforms such as $skip transform the cursor into a new
    // one. If multiple transforms are specified on inputQuery, they all
    // run.
    for (const key in parsed.cursorTransforms) {
      const transform = cursorTransformsMap[key];
      cursor = transform(cursor, parsed.cursorTransforms[key]);
      if (!cursor) {
        const err = ShareDbMongo.malformedQueryOperatorError(key);
        return callback(err);
      }
    }

    // Cursor operations such as $count run on the cursor, after all
    // transforms. Only one operation is run. The result goes in the
    // "extra" argument in the callback.
    if (parsed.cursorOperationKey) {
      cursorOperationsMap[parsed.cursorOperationKey](
        cursor,
        parsed.cursorOperationValue,
        (err, extra) => {
          if (err) return callback(err);
          callback(null, [], extra);
        }
      );
      return;
    }

    // If no collection operation or cursor operations were used, return
    // an array of snapshots that are passed in the "results" argument
    // in the callback
    cursor.toArray(callback);
  }

  query(collectionName, inputQuery, fields, options, callback) {
    this.getCollection(collectionName, (err, collection) => {
      if (err) return callback(err);
      const projection = getProjection(fields, options);
      this._query(collection, inputQuery, projection, (err, results, extra) => {
        if (err) return callback(err);
        const snapshots = [];
        for (let i = 0; i < results.length; i++) {
          const snapshot = castToSnapshot(results[i]);
          snapshots.push(snapshot);
        }
        callback(null, snapshots, extra);
      });
    });
  }

  queryPoll(collectionName, inputQuery, options, callback) {
    this.getCollectionPoll(collectionName, (err, collection) => {
      if (err) return callback(err);
      const projection = {_id: 1};
      this._query(collection, inputQuery, projection, (err, results, extra) => {
        if (err) return callback(err);
        const ids = [];
        for (let i = 0; i < results.length; i++) {
          ids.push(results[i]._id);
        }
        callback(null, ids, extra);
      });
    });
  }

  queryPollDoc(collectionName, id, inputQuery, options, callback) {
    this.getCollectionPoll(collectionName, (err, collection) => {
      const parsed = this._getSafeParsedQuery(inputQuery, callback);
      if (!parsed) return;

      // Run the query against a particular mongo document by adding an _id filter
      const queryId = parsed.query._id;
      if (queryId && typeof queryId === 'object') {
        // Check if the query contains the id directly in the common pattern of
        // a query for a specific list of ids, such as {_id: {$in: [1, 2, 3]}}
        if (Array.isArray(queryId.$in) && Object.keys(queryId).length === 1) {
          if (queryId.$in.indexOf(id) === -1) {
            // If the id isn't in the list of ids, then there is no way this
            // can be a match
            return callback(null, false);
          } else {
            // If the id is in the list, then it is equivalent to restrict to our
            // particular id and override the current value
            parsed.query._id = id;
          }
        } else {
          delete parsed.query._id;
          parsed.query.$and = (parsed.query.$and) ?
            parsed.query.$and.concat({_id: id}, {_id: queryId}) :
            [{_id: id}, {_id: queryId}];
        }
      } else if (queryId && queryId !== id) {
        // If queryId is a primitive value such as a string or number and it
        // isn't equal to the id, then there is no way this can be a match
        return callback(null, false);
      } else {
        // Restrict the query to this particular document
        parsed.query._id = id;
      }

      collection.find(parsed.query).limit(1).project({_id: 1}).next((err, doc) => {
        callback(err, !!doc);
      });
    });
  }

  // **** Polling optimization

  // Can we poll by checking the query limited to the particular doc only?
  canPollDoc(collectionName, query) {
    for (let operation in collectionOperationsMap) {
      if (query.hasOwnProperty(operation)) return false;
    }
    for (let operation in cursorOperationsMap) {
      if (query.hasOwnProperty(operation)) return false;
    }

    if (
      query.hasOwnProperty('$sort') ||
      query.hasOwnProperty('$orderby') ||
      query.hasOwnProperty('$limit') ||
      query.hasOwnProperty('$skip') ||
      query.hasOwnProperty('$max') ||
      query.hasOwnProperty('$min') ||
      query.hasOwnProperty('$returnKey')
    ) {
      return false;
    }

    return true;
  }

  // Return true to avoid polling if there is no possibility that an op could
  // affect a query's results
  skipPoll(collectionName, id, op, query) {
    // ShareDB is in charge of doing the validation of ops, so at this point we
    // should be able to assume that the op is structured validly
    if (op.create || op.del) return false;
    if (!op.op) return true;

    // Right now, always re-poll if using a collection operation such as
    // $distinct or a cursor operation such as $count. This could be
    // optimized further in some cases.
    for (let operation in collectionOperationsMap) {
      if (query.hasOwnProperty(operation)) return false;
    }
    for (let operation in cursorOperationsMap) {
      if (query.hasOwnProperty(operation)) return false;
    }

    // ShareDB calls `skipPoll` inside a try/catch block. If an error is
    // thrown, it skips polling -- we can't poll an invalid query. So in
    // the code below, we work under the assumption that `query` is
    // valid. If an error is thrown, that's fine.
    const fields = getFields(query);

    return !opContainsAnyField(op.op, fields);
  }

  // Utility methods

  // Return {code: ..., message: ...}  on error. Call before parseQuery.
  checkQuery(query) {
    if (query.$query) {
      return ShareDbMongo.$queryDeprecatedError();
    }

    const validMongoErr = checkValidMongo(query);
    if (validMongoErr) return validMongoErr;

    if (!this.allowJSQueries) {
      if (query.$where != null) {
        return ShareDbMongo.$whereDisabledError();
      }
      if (query.$mapReduce != null) {
        return ShareDbMongo.$mapReduceDisabledError();
      }
    }

    if (!this.allowAggregateQueries && query.$aggregate) {
      return ShareDbMongo.$aggregateDisabledError();
    }
  }

  // Parses a query and makes it safe against deleted docs. On error,
  // call the callback and return null.
  _getSafeParsedQuery(inputQuery, callback) {
    let err = this.checkQuery(inputQuery);
    if (err) {
      callback(err);
      return null;
    }

    try {
      const parsed = parseQuery(inputQuery);
      makeQuerySafe(parsed.query);
      return parsed;
    } catch (err) {
      err = ShareDbMongo.parseQueryError(err);
      callback(err);
      return null;
    }

    
  }

  // Bad request errors
  static invalidOpVersionError(collectionName, id, v) {
    return {
      code: 4101,
      message: `Invalid op version ${collectionName}.${id} ${op.v}`
    };
  }

  static invalidCollectionError(collectionName) {
    return {code: 4102, message: `Invalid collection name ${collectionName}`};
  }

  static $whereDisabledError() {
    return {code: 4103, message: '$where queries disabled'};
  }

  static $mapReduceDisabledError() {
    return {code: 4104, message: '$mapReduce queries disabled'};
  }

  static $aggregateDisabledError() {
    return {code: 4105, message: '$aggregate queries disabled'};
  }

  static $queryDeprecatedError() {
    return {code: 4106, message: '$query property deprecated in queries'};
  }

  static malformedQueryOperatorError(operator) {
    return {code: 4107, message: `Malformed query operator: ${operator}`};
  }

  static onlyOneCollectionOperationError(operation1, operation2) {
    return {
      code: 4108,
      message: `Only one collection operation allowed. Found ${operation1} and ${operation2}`
    };
  }

  static onlyOneCursorOperationError(operation1, operation2) {
    return {
      code: 4109,
      message: `Only one cursor operation allowed. Found ${operation1} and ${operation2}`
    };
  }

  static cursorAndCollectionMethodError(collectionOperation) {
    return {
      code: 4110,
      message: `Cursor methods can't run after collection method ${collectionOperation}`
    };
  }

  // Internal errors
  static alreadyClosedError() {
    return {code: 5101, message: 'Already closed'};
  }

  static missingLastOperationError(collectionName, id) {
    return {
      code: 5102,
      message: `Snapshot missing last operation field "_o" ${collectionName}.${id}`
    };
  }

  static missingOpsError(collectionName, id, from) {
    return {
      code: 5103,
      message: `Missing ops from requested version ${collectionName}.${id} ${from}`
    };
  }

  // Modifies 'err' argument
  static parseQueryError(err) {
    err.code = 5104;
    return err;
  }
}

ShareDbMongo.prototype.projectsSnapshots = true;

DB.prototype.getCommittedOpVersion = function(collectionName, id, snapshot, op, options, callback) {
  this.getOpCollection(collectionName, (err, opCollection) => {
    if (err) return callback(err);
    const query = {
      src: op.src,
      seq: op.seq
    };
    const projection = {v: 1, _id: 0};
    const sort = {v: 1};
    // Find the earliest version at which the op may have been committed.
    // Since ops are optimistically written prior to writing the snapshot, the
    // op could end up being written multiple times or have been written but
    // not count as committed if not back-referenced from the snapshot
    opCollection.find(query).project(projection).sort(sort).limit(1).next((err, doc) => {
      if (err) return callback(err);
      // If we find no op with the same src and seq, we definitely don't have
      // any match. This should prevent us from accidentally querying a huge
      // history of ops
      if (!doc) return callback();
      // If we do find an op with the same src and seq, we still have to get
      // the ops from the snapshot to figure out if the op was actually
      // committed already, and at what version in case of multiple matches
      const from = doc.v;
      this.getOpsToSnapshot(collectionName, id, from, snapshot, options, (err, ops) => {
        if (err) return callback(err);
        for (let i = ops.length; i--;) {
          const item = ops[i];
          if (op.src === item.src && op.seq === item.seq) {
            return callback(null, item.v);
          }
        }
        callback();
      });
    });
  });
};

function checkOpsFrom(collectionName, id, ops, from) {
  if (ops.length === 0) return;
  if (ops[0] && ops[0].v === from) return;
  if (from == null) return;
  return ShareDbMongo.missingOpsError(collectionName, id, from);
}

function checkDocHasOp(collectionName, id, doc) {
  if (doc._o) return;
  return ShareDbMongo.missingLastOperationError(collectionName, id);
}

function isCurrentVersion(doc, version) {
  return doc._v === version;
}

function getDocMap(docs) {
  const docMap = {};
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    docMap[doc._id] = doc;
  }
  return docMap;
}

function filterOps(ops, doc, to) {
  // Always return in the case of no ops found whether or not consistent with
  // the snapshot
  if (!ops) return [];
  if (!ops.length) return ops;
  if (!doc) {
    // There is no snapshot currently. We already returned if there are no
    // ops, so this could happen if:
    //   1. The doc was deleted
    //   2. The doc create op is written but not the doc snapshot
    //   3. Same as 3 for a recreate
    //   4. We are in an inconsistent state because of an error
    //
    // We treat the snapshot as the canonical version, so if the snapshot
    // doesn't exist, the doc should be considered deleted. Thus, a delete op
    // should be in the last version if no commits are inflight or second to
    // last version if commit(s) are inflight. Rather than trying to detect
    // ops inconsistent with a deleted state, we are simply returning ops from
    // the last delete. Inconsistent states will ultimately cause write
    // failures on attempt to commit.
    //
    // Different delete ops must be identical and must link back to the same
    // prior version in order to be inserted, so if there are multiple delete
    // ops at the same version, we can grab any of them for this method.
    // However, the _id of the delete op might not ultimately match the delete
    // op that gets maintained if two are written as a result of two
    // simultaneous delete commits. Thus, the _id of the op should *not* be
    // assumed to be consistent in the future.
    const deleteOp = getLatestDeleteOp(ops);
    // Don't return any ops if we don't find a delete operation, which is the
    // correct thing to do if the doc was just created and the op has been
    // written but not the snapshot. Note that this will simply return no ops
    // if there are ops but the snapshot doesn't exist.
    if (!deleteOp) return [];
    return getLinkedOps(ops, to, deleteOp._id);
  }
  return getLinkedOps(ops, to, doc._o);
}

function getLatestDeleteOp(ops) {
  for (let i = ops.length; i--;) {
    const op = ops[i];
    if (op.del) return op;
  }
}

function getLinkedOps(ops, to, link) {
  const linkedOps = [];
  for (let i = ops.length; i-- && link;) {
    const op = ops[i];
    if (link.equals ? !link.equals(op._id) : link !== op._id) continue;
    link = op.o;
    if (to == null || op.v < to) {
      delete op._id;
      delete op.o;
      linkedOps.push(op);
    }
  }
  return linkedOps.reverse();
}

function getOpsQuery(id, from) {
  return (from == null) ?
    {d: id} :
    {d: id, v: {$gte: from}};
}

function readOpsBulk(stream, callback) {
  const opsMap = {};
  let errored;
  stream.on('error', err => {
    errored = true;
    return callback(err);
  });
  stream.on('end', () => {
    if (errored) return;
    // Sort ops for each doc in ascending order by version
    for (const id in opsMap) {
      opsMap[id].sort((a, b) => a.v - b.v);
    }
    callback(null, opsMap);
  });
  // Read each op and push onto a list for the appropriate doc
  stream.on('data', op => {
    const id = op.d;
    if (opsMap[id]) {
      opsMap[id].push(op);
    } else {
      opsMap[id] = [op];
    }
    delete op.d;
  });
}

function getFields(query) {
  const fields = {};
  getInnerFields(query.$orderby, fields);
  getInnerFields(query.$sort, fields);
  getInnerFields(query, fields);
  return fields;
}

function getInnerFields(params, fields) {
  if (!params) return;
  for (const key in params) {
    const value = params[key];
    if (key === '$not') {
      getInnerFields(value, fields);
    } else if (key === '$or' || key === '$and' || key === '$nor') {
      for (let i = 0; i < value.length; i++) {
        const item = value[i];
        getInnerFields(item, fields);
      }
    } else if (key[0] !== '$') {
      const property = key.split('.')[0];
      fields[property] = true;
    }
  }
}

function opContainsAnyField(op, fields) {
  for (let i = 0; i < op.length; i++) {
    const component = op[i];
    if (component.p.length === 0) {
      return true;
    } else if (fields[component.p[0]]) {
      return true;
    }
  }
  return false;
}


// Check that any keys starting with $ are valid Mongo methods. Verify
// that:
// * There is at most one collection operation like $mapReduce
// * If there is a collection operation then there are no cursor methods
// * There is at most one cursor operation like $count
//
// Return {code: ..., message: ...} on error.
function checkValidMongo(query) {
  let collectionOperationKey = null; // only one allowed
  let foundCursorMethod = false; // transform or operation
  let cursorOperationKey = null; // only one allowed

  for (const key in query) {
    if (key[0] === '$') {
      if (collectionOperationsMap[key]) {
        // Found collection operation. Check that it's unique.

        if (collectionOperationKey) {
          return ShareDbMongo.onlyOneCollectionOperationError(
            collectionOperationKey, key
          );
        }
        collectionOperationKey = key;
      } else if (cursorOperationsMap[key]) {
        if (cursorOperationKey) {
          return ShareDbMongo.onlyOneCursorOperationError(
            cursorOperationKey, key
          );
        }
        cursorOperationKey = key;
        foundCursorMethod = true;
      } else if (cursorTransformsMap[key]) {
        foundCursorMethod = true;
      }
    }
  }

  if (collectionOperationKey && foundCursorMethod) {
    return ShareDbMongo.cursorAndCollectionMethodError(
      collectionOperationKey
    );
  }

  return null;
}

function ParsedQuery(
  query,
  collectionOperationKey,
  collectionOperationValue,
  cursorTransforms,
  cursorOperationKey,
  cursorOperationValue
) {
  this.query = query;
  this.collectionOperationKey = collectionOperationKey;
  this.collectionOperationValue = collectionOperationValue;
  this.cursorTransforms = cursorTransforms;
  this.cursorOperationKey = cursorOperationKey;
  this.cursorOperationValue = cursorOperationValue;
}

function parseQuery(inputQuery) {
  // Parse sharedb-mongo query format into an object with these keys:
  // * query: The actual mongo query part of the input query
  // * collectionOperationKey, collectionOperationValue: Key and value of the
  //   single collection operation (eg $mapReduce) defined in the input query,
  //   or null
  // * cursorTransforms: Map of all the cursor transforms in the input query
  //   (eg $sort)
  // * cursorOperationKey, cursorOperationValue: Key and value of the single
  //   cursor operation (eg $count) defined in the input query, or null
  //
  // Examples:
  //
  // parseQuery({foo: {$ne: 'bar'}, $distinct: {field: 'x'}}) ->
  // {
  //   query: {foo: {$ne: 'bar'}},
  //   collectionOperationKey: '$distinct',
  //   collectionOperationValue: {field: 'x'},
  //   cursorTransforms: {},
  //   cursorOperationKey: null,
  //   cursorOperationValue: null
  // }
  //
  // parseQuery({foo: 'bar', $limit: 2, $count: true}) ->
  // {
  //   query: {foo: 'bar'},
  //   collectionOperationKey: null,
  //   collectionOperationValue: null
  //   cursorTransforms: {$limit: 2},
  //   cursorOperationKey: '$count',
  //   cursorOperationValue: 2
  // }

  const query = {};
  let collectionOperationKey = null;
  let collectionOperationValue = null;
  const cursorTransforms = {};
  let cursorOperationKey = null;
  let cursorOperationValue = null;

  if (inputQuery.$query) {
    throw new Error("unexpected $query: should have called checkQuery");
  } else {
    for (const key in inputQuery) {
      if (collectionOperationsMap[key]) {
        collectionOperationKey = key;
        collectionOperationValue = inputQuery[key];
      } else if (cursorTransformsMap[key]) {
        cursorTransforms[key] = inputQuery[key];
      } else if (cursorOperationsMap[key]) {
        cursorOperationKey = key;
        cursorOperationValue = inputQuery[key];
      } else {
        query[key] = inputQuery[key];
      }
    }
  }

  return new ParsedQuery(
    query,
    collectionOperationKey,
    collectionOperationValue,
    cursorTransforms,
    cursorOperationKey,
    cursorOperationValue
  );
}
ShareDbMongo._parseQuery = parseQuery; // for tests

// Call on a query after it gets parsed to make it safe against
// matching deleted documents.
function makeQuerySafe(query) {
  // Don't modify the query if the user explicitly sets _type already
  if (query.hasOwnProperty('_type')) return;
  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. When docs are deleted, their data
  // properties are cleared and _type is set to null. Filter out deleted docs
  // by requiring that _type is a string if the query does not naturally
  // restrict the results with other keys
  if (deletedDocCouldSatisfyQuery(query)) {
    query._type = {$type: 2};
  }
}
ShareDbMongo._makeQuerySafe = makeQuerySafe; // for tests

// Could a deleted doc (one that contains {_type: null} and no other
// fields) satisfy a query?
//
// Return true if it definitely can, or if we're not sure. (This
// function is used as an optimization to see whether we can avoid
// augmenting the query to ignore deleted documents)
function deletedDocCouldSatisfyQuery(query) {
  // Any query with `{foo: value}` with non-null `value` will never
  // match deleted documents (that are empty other than the `_type`
  // field).
  //
  // This generalizes to additional classes of queries. Here’s a
  // recursive description of queries that can't match a deleted doc:
  // In general, a query with `{foo: X}` can't match a deleted doc
  // if `X` is guaranteed to not match null or undefined. In addition
  // to non-null values, the following clauses are guaranteed to not
  // match null or undefined:
  //
  // * `{$in: [A, B, C]}}` where all of A, B, C are non-null.
  // * `{$ne: null}`
  // * `{$exists: true}`
  // * `{$gt: not null}`, `{gte: not null}`, `{$lt: not null}`, `{$lte: not null}`
  //
  // In addition, some queries that have `$and` or `$or` at the
  // top-level can't match deleted docs:
  // * `{$and: [A, B, C]}`, where at least one of A, B, C are queries
  //   guaranteed to not match `{_type: null}`
  // * `{$or: [A, B, C]}`, where all of A, B, C are queries guaranteed
  //   to not match `{_type: null}`
  //
  // There are more queries that can't match deleted docs but they
  // aren’t that common, e.g. ones using `$type` or bit-wise
  // operators.
  if (query.hasOwnProperty('$and')) {
    if (Array.isArray(query.$and)) {
      for (let i = 0; i < query.$and.length; i++) {
        if (!deletedDocCouldSatisfyQuery(query.$and[i])) {
          return false;
        }
      }
      return true;
    } else {
      // Malformed? Play it safe.
      return true;
    }
  }

  if (query.hasOwnProperty('$or')) {
    if (Array.isArray(query.$or)) {
      for (let i = 0; i < query.$or.length; i++) {
        if (deletedDocCouldSatisfyQuery(query.$or[i])) {
          return true;
        }
      }
      return false;
    } else {
      // Malformed? Play it safe.
      return true;
    }
  }

  for (const prop in query) {
    // Ignore fields that remain set on deleted docs
    if (
      prop === '_id' ||
      prop === '_v' ||
      prop === '_o' ||
      prop === '_m' || (
        prop[0] === '_' &&
        prop[1] === 'm' &&
        prop[2] === '.'
      )
    ) {
      continue;
    }
    // When using top-level operators that we don't understand, play
    // it safe
    if (prop[0] === '$') {
      return true;
    }
    if (!couldMatchNull(query[prop])) {
      return false;
    }
  }

  return true;
}

function couldMatchNull(clause) {
  if (
    typeof clause === 'number' ||
    typeof clause === 'boolean' ||
    typeof clause === 'string'
  ) {
    return false;
  } else if (clause === null) {
    return true;
  } else if (isPlainObject(clause)) {
    // Mongo interprets clauses with multiple properties with an
    // implied 'and' relationship, e.g. {$gt: 3, $lt: 6}. If every
    // part of the clause could match null then the full clause could
    // match null.
    for (const prop in clause) {
      const value = clause[prop];
      if (prop === '$in' && Array.isArray(value)) {
        let partCouldMatchNull = false;
        for (let i = 0; i < value.length; i++) {
          if (value[i] === null) {
            partCouldMatchNull = true;
            break;
          }
        }
        if (!partCouldMatchNull) {
          return false;
        }
      } else if (prop === '$ne') {
        if (value === null) {
          return false;
        }
      } else if (prop === '$exists') {
        if (value) {
          return false;
        }
      } else if (prop === '$gt' || prop === '$gte' || prop === '$lt' || prop === '$lte') {
        if (value !== null) {
          return false;
        }
      } else {
        // Not sure what to do with this part of the clause; assume it
        // could match null.
      }
    }

    // All parts of the clause could match null.
    return true;
  } else {
    // Not a POJO, string, number, or boolean. Not sure what it is,
    // but play it safe.
    return true;
  }
}

function castToDoc(id, snapshot, opLink) {
  const data = snapshot.data;
  const doc =
    (isObject(data)) ? shallowClone(data) :
    (data === undefined) ? {} :
    {_data: data};
  doc._id = id;
  doc._type = snapshot.type;
  doc._v = snapshot.v;
  doc._m = snapshot.m;
  doc._o = opLink;
  return doc;
}

function castToSnapshot(doc) {
  const id = doc._id;
  const version = doc._v;
  const type = doc._type;
  let data = doc._data;
  const meta = doc._m;
  const opLink = doc._o;
  if (type == null) {
    return new MongoSnapshot(id, version, null, undefined, meta, opLink);
  }
  if (doc.hasOwnProperty('_data')) {
    return new MongoSnapshot(id, version, type, data, meta, opLink);
  }
  data = shallowClone(doc);
  delete data._id;
  delete data._v;
  delete data._type;
  delete data._m;
  delete data._o;
  return new MongoSnapshot(id, version, type, data, meta, opLink);
}
function MongoSnapshot(id, version, type, data, meta, opLink) {
  this.id = id;
  this.v = version;
  this.type = type;
  this.data = data;
  if (meta) this.m = meta;
  if (opLink) this._opLink = opLink;
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function shallowClone(object) {
  const out = {};
  for (const key in object) {
    out[key] = object[key];
  }
  return out;
}

function isPlainObject(value) {
  return (
    typeof value === 'object' && (
      Object.getPrototypeOf(value) === Object.prototype ||
      Object.getPrototypeOf(value) === null
    )
  );
}

// Convert a simple map of fields that we want into a mongo projection. This
// depends on the data being stored at the top level of the document. It will
// only work properly for json documents--which are the only types for which
// we really want projections.
function getProjection(fields, options) {
  // When there is no projection specified, still exclude returning the
  // metadata that is added to a doc for querying or auditing
  if (!fields) {
    return (options && options.metadata) ? {_o: 0} : {_m: 0, _o: 0};
  }
  // Do not project when called by ShareDB submit
  if (fields.$submit) return;

  const projection = {};
  for (const key in fields) {
    projection[key] = 1;
  }
  projection._type = 1;
  projection._v = 1;
  if (options && options.metadata) projection._m = 1;
  return projection;
}

const collectionOperationsMap = {
  '$distinct': function(collection, query, value, cb) {
    collection.distinct(value.field, query, cb);
  },
  '$aggregate': function(collection, query, value, cb) {
    collection.aggregate(value, (err, cursor) => {
      if(err) {
        return cb(err);
      }
      cursor.toArray((err, res) => {
        if(err) {
          return cb(err);
        }
        return cb(null, res);
      });
      
    });
  },
  '$mapReduce': function(collection, query, value, cb) {
    if (typeof value !== 'object') {
      const err = ShareDbMongo.malformedQueryOperatorError('$mapReduce');
      return cb(err);
    }
    const mapReduceOptions = {
      query,
      out: {inline: 1},
      scope: value.scope || {}
    };
    collection.mapReduce(
      value.map, value.reduce, mapReduceOptions, cb);
  }
};

const cursorOperationsMap = {
  '$count': function(cursor, value, cb) {
    cursor.count(cb);
  },
  '$explain': function(cursor, verbosity, cb) {
    cursor.explain(verbosity, cb);
  },
  '$map': function(cursor, fn, cb) {
    cursor.map(fn, cb);
  }
};

const cursorTransformsMap = {
  '$batchSize': function(cursor, size) { return cursor.batchSize(size); },
  '$comment': function(cursor, text) { return cursor.comment(text); },
  '$hint': function(cursor, index) { return cursor.hint(index); },
  '$max': function(cursor, value) { return cursor.max(value); },
  '$maxScan': function(cursor, value) { return cursor.maxScan(value); },
  '$maxTimeMS': function(cursor, milliseconds) {
    return cursor.maxTimeMS(milliseconds);
  },
  '$min': function(cursor, value) { return cursor.min(value); },
  '$noCursorTimeout': function(cursor) {
    // no argument to cursor method
    return cursor.noCursorTimeout();
  },
  '$orderby': function(cursor, value) {
    console.warn('Deprecated: $orderby; Use $sort.');
    return cursor.sort(value);
  },
  '$readConcern': function(cursor, level) {
    return cursor.readConcern(level);
  },
  '$readPref': function(cursor, value) {
    // The Mongo driver cursor method takes two arguments. Our queries
    // have a single value for the '$readPref' property. Interpret as
    // an object with {mode, tagSet}.
    if (typeof value !== 'object') return null;
    return cursor.readPref(value.mode, value.tagSet);
  },
  '$returnKey': function(cursor) {
    // no argument to cursor method
    return cursor.returnKey();
  },
  '$snapshot': function(cursor) {
    // no argument to cursor method
    return cursor.snapshot();
  },
  '$sort': function(cursor, value) { return cursor.sort(value); },
  '$skip': function(cursor, value) { return cursor.skip(value); },
  '$limit': function(cursor, value) { return cursor.limit(value); },
  '$showDiskLoc': function(cursor, value) {
    console.warn('Deprecated: $showDiskLoc; Use $showRecordId.');
    return cursor.showRecordId(value);
  },
  '$showRecordId': function(cursor) {
    // no argument to cursor method
    return cursor.showRecordId();
  }
};

module.exports = ShareDbMongo;
