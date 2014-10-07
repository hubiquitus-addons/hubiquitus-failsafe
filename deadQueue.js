var MongoClient = require('mongodb').MongoClient;
var h = require('hubiquitus-core');
var properties = require('./lib/properties');

var logger = h.logger('hubiquitus:addons:falisafe:deadqueue');

var db;

/**
 * Available opts :
 *    * deadQueue : deadQueue aid
 *    * mongo : mongo url
 * @param opts
 */
module.exports = exports = function (opts) {
  opts = opts || {};
  properties.mongo = opts.mongo || properties.mongo;
  properties.deadQueue = opts.deadQueue || properties.deadQueue;
  properties.collection = opts.collection || properties.collection;

  MongoClient.connect(properties.mongo, {mongos: {'auto_reconnect': true}} , function (err, _db) {
    if (err) {
      throw new Error('Could\'nt connect to mongo');
    }

    logger.info('Connected to mongodb');
    db = _db;

    h.addActor(properties.deadQueue, onMsg);
  });

  return exports;
};

/**
 * Save message when dead receive it
 * @param req
 */
function onMsg(req) {
  db.collection(properties.collection).save(req.content, {w:0}, function (err) {
    if (err) {
      logger.warn('Mongo saving error : ', err);
    }

    req.reply(err);
  });
}


