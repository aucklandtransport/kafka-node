'use strict';

const Readable = require('stream').Readable;
const ConsumerGroup = require('./consumerGroup');
const _ = require('lodash');
const logger = require('./logging')('kafka-node:ConsumerGroupStream');
const async = require('async');
const DEFAULT_HIGH_WATER_MARK = 100;

const DEFAULTS = {
  autoCommit: true
};

class ConsumerGroupStream extends Readable {
  constructor (options, topics) {
    super({ objectMode: true, highWaterMark: options.highWaterMark || DEFAULT_HIGH_WATER_MARK });

    _.defaultsDeep(options || {}, DEFAULTS);
    const self = this;

    this.autoCommit = options.autoCommit;

    options.connectOnReady = false;
    options.autoCommit = false;
    const originalOnRebalance = options.onRebalance;
    options.onRebalance = function (isAlreadyMember, callback) {
      const autoCommit = _.once(function (err) {
        if (err) {
          callback(err);
        } else {
          self.commit(null, true, callback);
        }
      });
      if (typeof originalOnRebalance === 'function') {
        try {
          originalOnRebalance(isAlreadyMember, autoCommit);
        } catch (e) {
          autoCommit(e);
        }
      } else {
        autoCommit();
      }
    };

    this.consumerGroup = new ConsumerGroup(options, topics);

    this.messageBuffer = [];
    this.commitQueue = {};
    this.commitGenericCallbacks = [];
    this.commitPartitionCallbacks = new Map();

    this.consumerGroup.on('error', error => this.emit('error', error));
    this.consumerGroup.on('connect', () => this.emit('connect'));
    this.consumerGroup.on('message', message => {
      this.messageBuffer.push(message);
      this.consumerGroup.pause();
    });
    this.consumerGroup.on('done', message => {
      setImmediate(() => this.transmitMessages());
    });
  }

  emit (event, value) {
    if (event === 'data' && this.autoCommit && !_.isEmpty(value)) {
      setImmediate(() => this.commit(value));
    }
    super.emit.apply(this, arguments);
  }

  _read () {
    logger.debug('_read called');
    if (!this.consumerGroup.ready) {
      logger.debug('consumerGroup is not ready, calling consumerGroup.connect');
      this.consumerGroup.connect();
    }
    this._reading = true;
    this.transmitMessages();
  }

  commit (message, force, callback) {
    if (typeof callback === 'function') {
      callback = _.once(callback);
      if (message == null) {
        this.commitGenericCallbacks.push(callback);
      } else {
        const key = message.topic + ':' + message.partition;
        let callbacks = this.commitPartitionCallbacks.get(key);
        if (callbacks == null) {
          callbacks = [];
          this.commitPartitionCallbacks.set(key, callbacks);
        }
        callbacks.push(callback);
      }
    }
    if (message != null && message.offset !== -1) {
      _.set(this.commitQueue, [message.topic, message.partition], message.offset + 1);
    }

    if (this.autoCommitPending && !force) {
      logger.debug('commit is pending, delaying');
      return;
    }

    this.commitQueued(force);
  }

  commitQueued (force) {
    if (!force) {
      if (!this.autoCommitPending) {
        this.autoCommitPending = true;
        this.autoCommitTimer = setTimeout(() => {
          logger.debug('registering auto commit to allow batching');
          this.autoCommitPending = false;

          this.commitQueued(true);
        }, this.consumerGroup.options.autoCommitIntervalMs);
        this.autoCommitTimer.unref();
      }
      return;
    }

    if (this.commitPendingPromise) {
      logger.debug('delaying commit as another commit is in progress');
      this.commitPendingPromise.then(() => this.commitQueued(true));
      return;
    }

    const commits = [];
    _.forEach(this.commitQueue, function (partitionOffset, topic) {
      _.forEach(partitionOffset, function (offset, partition) {
        if (offset != null) {
          commits.push({
            topic: topic,
            partition: partition,
            offset: offset,
            metadata: 'm'
          });
        }
      });
    });

    const genericCallbacks = this.commitGenericCallbacks;
    const partitionCallbacks = this.commitPartitionCallbacks;
    // These are pending, don't let another request send them too
    this.commitQueue = {};
    this.commitGenericCallbacks = [];
    this.commitPartitionCallbacks = new Map();

    if (_.isEmpty(commits)) {
      // we wouldn't have partition callbacks here if there was nothing to commit
      genericCallbacks.forEach(cb => cb(null));
      logger.debug('commit ignored. no commits to make.');
      return;
    }

    this.commitPendingPromise = new Promise((resolve, reject) => {
      logger.debug('committing', commits);
      this.consumerGroup.sendOffsetCommitRequest(commits, (error, topics) => {
        this.commitPendingPromise = null;
        if (error) {
          // Nothing known triggers this unless something in the request stack might throw an error
          logger.error('commit request failed', error);
          this.emit('error', error);
          partitionCallbacks.forEach(callbacks => {
            callbacks.forEach(cb => cb(error));
          });
          return;
        }
        Object.keys(topics).forEach(topic => {
          const partitions = topics[topic];
          Object.keys(partitions).forEach(partition => {
            const partitionError = partitions[partition];
            const key = topic + ':' + partition;
            const callbacks = partitionCallbacks.get(key);
            if (callbacks) {
              partitionCallbacks.delete(key);
              callbacks.forEach(cb => cb(partitionError));
            } else if (partitionError) {
              logger.error('partition error with no callback receiving it', partitionError);
              this.emit('error', partitionError);
            }
          });
        });

        genericCallbacks.forEach(cb => cb(null));
        // Any remaining partition callbacks left did not get included in the commit result. Throw an error to them.
        partitionCallbacks.forEach((callbacks, key) => {
          callbacks.forEach(cb => cb(new Error('topic ' + key + ' did not get returned in commit response'));
        });
        resolve();
      });
    }).catch(error => {
      logger.error('error in commit request', error);
      partitionCallbacks.forEach(callbacks => {
        callbacks.forEach(cb => cb(error));
      });
      genericCallbacks.forEach(cb => cb(error));
    });
  }

  transmitMessages () {
    while (this._reading && this.messageBuffer.length > 0) {
      this._reading = this.push(this.messageBuffer.shift());
    }
    if (this.messageBuffer.length === 0 && this._reading) {
      this.consumerGroup.resume();
    }
  }

  close (callback) {
    clearTimeout(this.autoCommitTimer);
    this.autoCommitPending = false;
    async.series(
      [
        callback => {
          if (this.autoCommit) {
            this.commit(null, true, callback);
          } else {
            callback(null);
          }
        },
        callback => {
          this.consumerGroup.close(false, () => {
            callback();
            this.emit('close');
          });
        }
      ],
      callback || _.noop
    );
  }

  _destroy () {
    this.close();
  }
}

module.exports = ConsumerGroupStream;
