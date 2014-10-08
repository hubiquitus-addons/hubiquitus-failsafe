var h = require('hubiquitus-core');
var logger = h.logger('hubiquitus:addons:falisafe');
var _ = require('lodash');
var properties = require('./lib/properties');
var path = require('path');
var program = require('commander');
var levelup = require('levelup');
var deadQueue = require('./deadQueue');

/**
 * Db path. Fetch from command line or use default.
 * @type {string}
 */
var dbpath;

/**
 * Level db options
 * @type {object}
 */
const leveldbOpts = {keyEncoding: 'json', valueEncoding: 'json'};

/**
 * Number of item being sent (items that couldn't be sent)
 * @type {number}
 */
var queueConcurrentSend = 0;

/**
 * Container id
 * @type {string}
 */
var cid = h.properties.container.id;

/**
 * Monitoring actor id
 * @type {string}
 */
var mid = h.properties.container.id + ':failsafe:monitoring';

/**
 * db handle
 */
var db;

program
  .option('--fsdb [path]', 'Failsafe database path')
  .parse(process.argv);

/**
 * Available opts :
 *   * sendTimeout : Hubiquitus send timeout
 *   * maxTimeout : Delay before forwarding to deadQueue
 *   * deadQueue : deadQueue aid
 *   * maxQueueConcurrentSend : Max concurrent send for failures
 * @param opts
 */
module.exports = exports = function (opts) {
  opts = opts || {};
  properties.sendTimeout = opts.sendTimeout || properties.sendTimeout;
  properties.maxTimeout = opts.maxTimeout || properties.maxTimeout;
  properties.deadQueue = opts.deadQueue || properties.deadQueue;
  properties.maxQueueConcurrentSend = opts.maxQueueConcurrentSend || properties.maxQueueConcurrentSend;

  dbpath = opts.db || program.fsdb;
  dbpath = dbpath ? path.resolve(dbpath) : path.resolve(process.cwd(), './failsafedb');
  db = levelup(dbpath, leveldbOpts);
  logger.debug('Using db at path ' + dbpath);

  setTimeout(processFailures, properties.sendTimeout);

  h.addActor(mid, monitoring);
  return exports;
};

exports.deadQueue = deadQueue;

/**
 * Send a message (stored in localdb in case of process failure to avoid losing a message)
 * @param {String} from
 * @param {String} to
 * @param {Object} [content]
 * @param {Function} [cb]
 */
exports.send = function (from, to, content, cb) {
  var key = {time: Date.now(), id: h.utils.uuid()};
  var msg = {from: from, to: to, content: content};
  internalSend(from, to, key, msg, cb);
};

function internalSend(from, to, key, msg, cb) {
  var value = {msg: msg, sendAt: Date.now()};
  db.put(key, value, {sync: true}, function (err) {
    if (err) {
      logger.warn('Couldn\'t store message. Aborting send.', value);
      cb && cb(err);
      return;
    }

    cb && cb();

    var content = msg.content;
    if (to === properties.deadQueue) {
      content = {key: key, value: value};
    }
    h.send(from, to, content, properties.sendTimeout, function (err) {
      if (err) {
        logger.debug('Couldn\'t send message : ', err);
        return;
      }

      db.del(key, function (err) {
        if (err) logger.debug('Couldn\'t remove entry : ', err);
      });
    });
  });
}

/**
 * Load msgs not send and send it back or throw it do deadQueue
 */
function processFailures() {
  var now = Date.now();
  var cb = _.once(function () {
    setTimeout(processFailures, properties.sendTimeout + 1000); // 1000 is to avoid running again before all responses were processed
  });

  var stream = db.createReadStream({end: {time: now - properties.sendTimeout}});

  stream.on('data', function (data) {
    if (queueConcurrentSend > properties.maxQueueConcurrentSend) {
      stream.pause();
    }

    if ((data.value.sendAt + properties.sendTimeout) < now) { // if we need to re-send
      if (data.key.time + properties.maxTimeout < now) { // too old, send to dead queue
        queueConcurrentSend++;
        internalSend(cid, properties.deadQueue, data.key, data.value.msg, function () {
          queueConcurrentSend--;
          if (queueConcurrentSend === (properties.maxQueueConcurrentSend/2)) stream.resume();
        });
      } else {
        internalSend(data.value.msg.from, data.value.msg.to, data.key, data.value.msg, function () {
          queueConcurrentSend--;
          if (queueConcurrentSend === (properties.maxQueueConcurrentSend/2)) stream.resume();
        });
      }
    }
  });

  stream.on('error', function (err) {
    logger.warn('Read stream error : ', err);
    cb();
  });

  stream.on('end', cb);
  stream.on('close', cb);
}

/**
 * Monitoring
 * @param req
 */
function monitoring(req) {
  req.reply(null, exports.stats());
}

/**
 * Monitoring infos
 * @param req
 */
exports.stats = function () {
  if (!db) return {};

  return {
    dbpath: dbpath,
    queueConcurrentSend: queueConcurrentSend,
    leveldbStats: db.db.getProperty ? db.db.getProperty('leveldb.stats') : null
  };
};
