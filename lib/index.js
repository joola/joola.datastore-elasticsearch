var
  ce = require('cloneextend'),
  traverse = require('traverse'),
  async = require('async'),
  elasticsearch = require('elasticsearch');

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

  var index = (collection.key || collection).toLowerCase();
  async.map(documents, function (doc, cb) {
    self.client.create({
      index: index,
      refresh: true,
      type: 'jdocument',
      //id: self.common.uuid(),
      body: doc
    }, function (err, response) {
      if (err)
        return cb(err);
      return cb(null, response);
    });
  }, function (err, results) {
    if (err)
      return callback(err);
    return callback(null, results);
  });
};

ElasticSearch.prototype.buildQueryPlan = function (query, callback) {
  var self = this;
  var plan = {
    uid: self.common.uuid(),
    cost: 0,
    colQueries: {},
    query: query
  };
  var $match = {};
  var $project = {};
  var $group = {};
  var $sort = {};
  var $limit;

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    //$match.timestamp = {$gte: query.timeframe.start, $lt: query.timeframe.end};
    $match.filter = {
      range: {
        timestamp: {
          from: query.timeframe.start,
          to: query.timeframe.end
        }
      }
    }
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = {$limit: query.timeframe.last_n_items};
  }

  if (query.limit)
    $limit = {$limit: parseInt(query.limit)};

  if (query.filter) {
    query.filter.forEach(function (f) {
      if (f[1] == 'eq')
        $match[f[0]] = f[2];
      else {
        $match[f[0]] = {};
        $match[f[0]]['$' + f[1]] = f[2];
      }
    });
  }

  $group._id = {};
  $group.fields = [];
  $group.aggs = {};

  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        $group.fields.push(dimension.key);
        //$group._id[dimension.key] = '$' + dimension.key + '_' + query.interval;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $group.fields.push(dimension.key);
        //$group._id[dimension.key] = '$' + (dimension.attribute || dimension.key);
        break;
      case 'geo':
        break;
      default:
        return setImmediate(function () {
          return callback(new Error('Dimension [' + dimension.key + '] has unknown type of [' + dimension.datatype + ']'));
        });
    }
  });

  if (query.metrics.length === 0) {
    try {
      query.metrics.push({
        key: 'fake',
        dependsOn: 'fake',
        collection: query.collection.key || query.dimensions ? query.dimensions[0].collection : null
      });
    }
    catch (ex) {
      query.metrics = [];
    }
  }

  query.sort = query.sort || query.orderby;
  if (query.sort && Array.isArray(query.sort)) {
    query.sort.forEach(function (s) {
      $sort[s[0]] = s[1].toUpperCase() === 'DESC' ? -1 : 1;
    });
  }
  else
    $sort['timestamp'] = -1;

  query.metrics.forEach(function (metric) {
    var colQuery = {
      collection: metric.collection ? metric.collection.key : null,
      query: []
    };

    if (!metric.formula && metric.collection) {
      metric.aggregation = (metric.aggregation || 'sum').toLowerCase();
      if (metric.aggregation == 'ucount')
        colQuery.type = 'cardinality';
      else
        colQuery.type = 'plain';

      var _$match = self.common.extend({}, $match);

      var _$unwind;// = '$' + metric.dependsOn || metric._key;
      if (metric.dependsOn.indexOf('.') > 0 && self.common.checkNestedArray(metric.collection, metric.dependsOn))
        _$unwind = '$' + metric.dependsOn.substring(0, metric.dependsOn.indexOf('.')) || metric._key;
      var _$project = self.common.extend({}, $project);
      var _$group = self.common.extend({}, $group);
      var _$sort = self.common.extend({}, $sort);

      if (metric.filter) {
        metric.filter.forEach(function (f) {
          if (f[1] == 'eq')
            _$match[f[0]] = f[2];
          else {
            _$match[f[0]] = {};
            _$match[f[0]]['$' + f[1]] = f[2];
          }
        });
      }
      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match) + '_' + JSON.stringify(_$unwind) + '_' + metric.key);


      if (plan.colQueries[colQuery.key]) {
        if (_$unwind)
          _$group = self.common.extend({}, plan.colQueries[colQuery.key].query.$group);
        else
          _$group = self.common.extend({}, plan.colQueries[colQuery.key].query.$group);
      }

      _$group.aggs = {};

      if (metric.key !== 'fake') {
        _$group[metric.key] = {};
        if (metric.aggregation === 'count')
          _$group[metric.key].$sum = 1;
        else if (metric.aggregation === 'ucount') {
          _$group.aggs[metric.key] = {};
          _$group.aggs[metric.key].cardinality = {field: metric.dependsOn, precision_threshold: 100};
        }
        else {
          _$group.aggs[metric.key] = {};
          _$group.aggs[metric.key][metric.aggregation || 'sum'] = {field: metric.key};
        }
      }

      colQuery.query =
      {
        $match: _$match,
        $group: _$group,
        $sort: _$sort
      };

      if ($limit) {
        colQuery.query.push($limit);
      }

      plan.colQueries[colQuery.key] = colQuery;
    }
  });

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  return setImmediate(function () {
    return callback(null, plan);
  });
};

ElasticSearch.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };
  var self = this;

  return self.buildQueryPlan(query, function (err, plan) {
    async.mapSeries(Object.keys(plan.colQueries), function (key, cb) {
      var _plan = ce.clone(plan.colQueries[key]);
      var queryPlan = _plan.query;
      //console.log(require('util').inspect(queryPlan, {depth: null, colors: true}));
      Object.keys(queryPlan.$group.aggs).forEach(function (key) {
        queryPlan.$group.fields.push(queryPlan.$group.aggs[key]);
      });
      var searchObject = {
        index: _plan.collection.toLowerCase(),
        type: 'jdocument',
        searchType: 'count',
        query: {
          filtered: {
            query: {match_all: {}},
            filter: queryPlan.$match.filter
          }
        },
        body: {
          aggs: function () {
            var build = function (fields) {
              var result = null;
              var field = fields.shift();
              if (field && typeof field === 'object') {
                result = {};
                var key = Object.keys(field)[0];
                var metric = field[key];
                var aggregation = Object.keys(metric)[0];
                result[metric[aggregation]] = {};
                result[metric[aggregation]][key] = metric;
                if (fields.length > 0) {
                  result[metric[aggregation]]['aggs'] = function () {

                    return build(fields);

                  }()
                }
              }
              else if (field) {
                result = {};
                result[field] = {
                  terms: {
                    field: field
                  },
                  aggs: function () {
                    if (fields.length > 0)
                      return build(fields);
                  }()
                }
              }
              return result;
            };

            var result = build(queryPlan.$group.fields.slice(0));
            return result;
          }()
        }
      };
      //console.log(require('util').inspect(searchObject, {depth: null, colors: true}));
      self.client.search(searchObject, function (err, results) {
        if (err)
          return cb(err);
        return cb(null, results);
      });
    }, function (err, results) {
      if (err)
        console.log(err);
      console.log(require('util').inspect(results, {depth: null, colors: true}));

      var output = {
        dimensions: query.dimensions,
        metrics: query.metrics,
        documents: [],
        queryplan: plan
      };

      var flatResults = [];

      var processBuckets = function (path, buckets) {
        buckets.forEach(function (bucket) {
          if (bucket.buckets)
            return processBuckets(path, bucket.buckets)

        });
      };


      if (results[0].aggregations && results[0].aggregations.buckets)
        var docs = processBuckets(results[0].aggregations.buckets);
      //console.log(docs);


      var path = [];
      var values = [];
      var last = '';
      traverse(results[0]).map(function (x) {
        var point = this;
        if (x.key) {
          if (!x.buckets) {
            if (x['key_as_string'])
              values.push(x['key_as_string']);
            else if (x['key'])
              values.push(x['key']);
            Object.keys(x).forEach(function (key) {
              if (['key', 'key_as_string', 'doc_count'].indexOf(key) === -1) {
                //console.log('found leaf value', key, x);
                if (x[key].buckets) {
                  path.push(point.path);
                  last = 'middle';
                  //console.log('middle', path);
                }
                else {
                  path.push(point.path);
                  //console.log('end', path, values);
                  var pos = 0;
                  var row = {};
                  var walkParents = function (item) {
                    //console.log('walking parent', item.node);
                    if (item.parent)
                      walkParents(item.parent);

                    if (item.node.key_as_string || item.node.key)
                    //console.log('dimension', query.dimensions[pos++].key, item.node.key_as_string || item.node.key);
                      row[query.dimensions[pos++].key] = item.node.key_as_string || item.node.key;
                  };
                  walkParents(point.parent);
                  //console.log('dimension', query.dimensions[pos].key, x.key_as_string || x.key);
                  row[query.dimensions[pos].key] = x.key_as_string || x.key;
                  row[query.metrics[0].key] = x[key][key];
                  console.log('row', row);
                  output.documents.push(row);
                }
              }
            });

          }
        }
      });
      if (err)
        return callback(err);
      return callback(null, output);
    })
  });

};

ElasticSearch.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.client = new elasticsearch.Client(ce.clone(options));
  return callback(null, self);
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