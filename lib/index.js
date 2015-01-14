module.exports = ElasticSearch;

function ElasticSearch(options, helpers, callback) {
  if (!(this instanceof ElasticSearch)) return new ElasticSearch(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'ElasticSearch';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  return this.init(options, callback);
}

ElasticSearch.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Initializing connection to ElasticSearch [' + self.name + '].');

  return self.openConnection(options, callback);
};

ElasticSearch.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to ElasticSearch [' + self.name + '].');

  return callback(null);
};

ElasticSearch.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

ElasticSearch.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

ElasticSearch.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

ElasticSearch.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;
  var result = [];

  return callback(null, result);
};

ElasticSearch.prototype.buildQueryPlan = function (query, callback) {
  callback = callback || function () {
  };

  var self = this;
  var plan = {};

  return callback(null, plan);
};

ElasticSearch.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };

  var self = this;
  var results = [];

  return callback(null, results);
};

ElasticSearch.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, connection);
};

ElasticSearch.prototype.closeConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

ElasticSearch.prototype.checkConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, connection);
};

ElasticSearch.prototype.stats = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;
  var stats = {};

  return callback(null, stats);
};

ElasticSearch.prototype.drop = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

ElasticSearch.prototype.purge = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};