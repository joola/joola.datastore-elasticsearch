module.exports = Provider;

function Provider(options, helpers, callback) {
  if (!(this instanceof Provider)) return new Provider(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'Provider';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  return this.init(options, callback);
}

Provider.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Initializing connection to provider [' + self.name + '].');

  return self.openConnection(options, callback);
};

Provider.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  return callback(null);
};

Provider.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

Provider.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

Provider.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

Provider.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;
  var result = [];

  return callback(null, result);
};

Provider.prototype.buildQueryPlan = function (query, callback) {
  callback = callback || function () {
  };

  var self = this;
  var plan = {};

  return callback(null, plan);
};

Provider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };

  var self = this;
  var results = [];

  return callback(null, results);
};

Provider.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, connection);
};

Provider.prototype.closeConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

Provider.prototype.checkConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, connection);
};

Provider.prototype.stats = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;
  var stats = {};

  return callback(null, stats);
};

Provider.prototype.drop = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

Provider.prototype.purge = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};