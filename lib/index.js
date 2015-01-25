var
  ce = require('cloneextend'),
  traverse = require('traverse'),
  async = require('async'),
  _ = require('underscore'),
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

  self.logger.debug('Initializing connection to ElasticSearch [' + self.name + '].');

  return self.openConnection(options, callback);
};

ElasticSearch.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.debug('Destroying connection to ElasticSearch [' + self.name + '].');

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

  //create the index
  var index = (collection.key || collection).toLowerCase();
  async.map(documents, function (doc, cb) {
    //self.client.indices.create({index: index}, function (err) {
    // if (err)
    //   return callback(err);
    self.client.indices.create({
      index: index,
      type: 'jdocument',
      body: {
        mappings: {
          _default_: {
            _source: {
              enabled: true
            },
            _all: {
              enabled: false
            },
            _type: {
              index: "no",
              store: false
            },
            dynamic_templates: [
              {
                el: {
                  match: "*",
                  match_mapping_type: "long",
                  mapping: {
                    type: "long",
                    index: "not_analyzed"
                  }
                }
              },
              {
                es: {
                  match: "*",
                  match_mapping_type: "string",
                  mapping: {
                    type: "string",
                    index: "not_analyzed"
                  }
                }
              }
            ]
          }
        }
      }
    }, function (err) {
      //if (err)
      //return callback(err);
      self.client.index({index: index, refresh: true, type: 'jdocument', body: doc}, function (err, response) {
        if (err)
          return cb(err);

        return cb(null, response);
      });
    });
    // });
  }, function (err, results) {
    if (err)
      return callback(err);
    return callback(null, results);
  });
}
;

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
      $match.query = $match.query || {};
      if (f[1] === 'eq') {
        var matchPhrase = {};
        matchPhrase[f[0]] = f[2];
        $match.query.bool = $match.query.bool || {
          "must": []
        };
        $match.query.bool['must'].push({match: matchPhrase});
      }
      else if (f[1] === 'regex') {
        var matchPhrase = {};
        matchPhrase[f[0]] = f[2];
        $match.query.bool = $match.query.bool || {
          "must": []
        };
        $match.query.bool['must'].push({regexp: matchPhrase});
      }

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
        $group.fields.push(dimension.attribute);
        //$group._id[dimension.key] = '$' + dimension.key + '_' + query.interval;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $group.fields.push(dimension.attribute);
        //$group._id[dimension.key] = '$' + (dimension._key || dimension.key);
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
      if (metric.aggregation === 'ucount')
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
          _$group.aggs[metric.key].cardinality = {};
          _$group.aggs[metric.key].cardinality[metric.key] = {
            field: Array.isArray(metric.dependsOn) ? metric.dependsOn[0] : metric.dependsOn,
            precision_threshold: 100
          };
        }
        else {
          _$group.aggs[metric.key] = {};
          _$group.aggs[metric.key][metric.aggregation || 'sum'] = {field: metric._key};
        }
      }

      colQuery.query =
      {
        $match: _$match,
        $group: _$group,
        $sort: _$sort
      };

      if ($limit) {
        colQuery.query.$limit = {from: 0, size: $limit.$limit};
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

  var templateRow = {_key: null};
  if (!query.dimensions)
    query.dimensions = [];
  query.dimensions.forEach(function (d) {
    templateRow[d.key] = null;
  });

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
        _source: false,
        search_type: 'count',
        body: {
          query: {
            filtered: {
              filter: queryPlan.$match.filter || {}
            }
          },
          aggs: function () {
            var build = function (fields) {
              var result = null;
              var field = fields.shift();
              if (field && typeof field === 'object') {
                result = {};

                var key, metric, aggregation;
                //check for cardinality
                if (field.cardinality) {
                  field = field.cardinality;
                  key = Object.keys(field)[0];
                  metric = field[Object.keys(field)[0]];
                  aggregation = 'cardinality';
                  result[key] = {};
                  result[key][aggregation] = metric;
                }
                else {
                  key = Object.keys(field)[0];
                  metric = field[key];
                  aggregation = Object.keys(metric)[0];
                  result[metric[aggregation]] = {};
                  result[metric[aggregation]][key] = metric;
                }
                if (fields.length > 0)
                  result[metric[aggregation]]['aggs'] = build(fields);
              }
              else if (field && field ==='timestamp') {
                result = {};
                result[field] = {
                  date_histogram: {
                    field: field,
                    interval: '1d'
                  },
                  aggs: function () {
                    if (fields.length > 0)
                      return build(fields);
                  }()
                };
                if (field === 'timestamp') {
                  /*
                  result[field].terms.format = function () {
                    switch (query.interval) {
                      case 'timebucket.raw':
                        return 'yyy-MM-dd\'T\'HH:mm:ss.SSS+0000';
                      case 'timebucket.second':
                        return 'yyy-MM-dd\'T\'HH:mm:ss.000+0000';
                      case 'timebucket.minute':
                        return 'yyy-MM-dd\'T\'HH:mm:00.000+0000';
                      case 'timebucket.hour':
                        return 'yyy-MM-dd\'T\'HH:00:00.000+0000';
                      case 'timebucket.date':
                      case 'timebucket.day':
                      case 'timebucket.ddate':
                        return 'yyy-MM-dd\'T\'00:00:00.000+0000';
                      case 'timebucket.month':
                        return 'yyy-MM-01\'T\'00:00:00.000+0000';
                      case 'timebucket.year':
                        return 'yyy-01-01\'T\'00:00:00.000+0000';
                      default:
                        //console.log('unknown interval', query.interval);
                        return 'yyy-MM-dd\'T\'HH:mm:ss.SSSZ';
                    }
                  }()*/
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
                };
                if (field === 'timestamp') {
                  result[field].terms.format = function () {
                    switch (query.interval) {
                      case 'timebucket.raw':
                        return 'yyy-MM-dd\'T\'HH:mm:ss.SSS+0000';
                      case 'timebucket.second':
                        return 'yyy-MM-dd\'T\'HH:mm:ss.000+0000';
                      case 'timebucket.minute':
                        return 'yyy-MM-dd\'T\'HH:mm:00.000+0000';
                      case 'timebucket.hour':
                        return 'yyy-MM-dd\'T\'HH:00:00.000+0000';
                      case 'timebucket.date':
                      case 'timebucket.day':
                      case 'timebucket.ddate':
                        return 'yyy-MM-dd\'T\'00:00:00.000+0000';
                      case 'timebucket.month':
                        return 'yyy-MM-01\'T\'00:00:00.000+0000';
                      case 'timebucket.year':
                        return 'yyy-01-01\'T\'00:00:00.000+0000';
                      default:
                        //console.log('unknown interval', query.interval);
                        return 'yyy-MM-dd\'T\'HH:mm:ss.SSSZ';
                    }
                  }()
                }
              }
              return result;
            };
            return build(queryPlan.$group.fields.slice(0));
          }()
        }
      };
      if (queryPlan.$match.query)
        searchObject.body.query.filtered.query = queryPlan.$match.query;
      if (queryPlan.$limit) {
        searchObject.body.from = queryPlan.$limit.from;
        searchObject.body.size = queryPlan.$limit.size;
      }
      else {
        searchObject.body.from = 0;
        searchObject.body.size = 1000;
      }
      if (queryPlan.$sort) {
        Object.keys(queryPlan.$sort).forEach(function (key) {
          var exist = _.find(function (item) {
            return item.key === key;
          });
          if (exist) {
            searchObject.body.sort = searchObject.body.sort || [];
            var sort = queryPlan.$sort[key];
            var sortPair = {};
            sortPair[key] = {order: sort === -1 ? 'desc' : 'asc'};
            searchObject.body.sort.push(sortPair);
          }
        });
      }
      /*else{
       var sortPair = {timestamp:{order: 'desc'}};
       searchObject.body.sort.push(sortPair);
       }*/
      console.log(require('util').inspect(searchObject, {depth: null, colors: true}));
      self.client.search(searchObject, function (err, results) {
        if (err)
          return cb(err);
        return cb(null, results);
      });
    }, function (err, results) {
      if (err)
        return callback(err);

      console.log(require('util').inspect(results, {depth: null, colors: true}));
      var output = {
        dimensions: query.dimensions,
        metrics: query.metrics,
        documents: [],
        queryplan: plan
      };

      var midstep = [];
      results.forEach(function (result) {
        var interim = [];
        if (query.dimensions.length > 0) {
          traverse(result).map(function (x) {
            var point = this;
            if (x && x.key) {
              if (!x.buckets) {
                Object.keys(x).forEach(function (key) {
                  if (['key', 'key_as_string', 'doc_count'].indexOf(key) === -1) {
                    if (!x[key].buckets) {
                      var pos = 0;
                      var row = ce.clone(templateRow);
                      var walkParents = function (item) {
                        if (item.parent)
                          walkParents(item.parent);

                        if (item.node.key_as_string || item.node.key)
                          row[query.dimensions[pos++].key] = item.node.key_as_string || item.node.key;
                      };
                      walkParents(point.parent);
                      row[query.dimensions[pos].key] = x.key_as_string || x.key;
                      row[key] = x[key]['value'];

                      var rowKey = '';
                      query.dimensions.forEach(function (d) {
                        rowKey += row[d.key];
                      });
                      row._key = self.common.hash(rowKey);
                      interim.push(row);
                    }
                  }
                });
              }
            }
          });
          midstep.push(interim);
        }
        else {
          interim = [];
          var row = ce.clone(templateRow);
          query.metrics.forEach(function (m) {
            var value, rowKey;
            if (result.aggregations[m._key]) {
              value = result.aggregations[m._key];
              rowKey = m._key;
            }
            else {
              value = result.aggregations[m.key];
              rowKey = m.key;
            }
            if (value) {
              self.common.flatGetSet(row, rowKey, value.value);
              //row[m.key] = value.value;
            }
          });
          interim.push(row);
          midstep.push(interim);
        }
      });

      midstep.forEach(function (result) {
        result.forEach(function (row) {
          var exist = _.find(output.documents, function (item) {
            return item._key === row._key;
          });
          if (exist) {
            var metrics = _.filter(Object.keys(row), function (item) {
              if (_.find(query.metrics, function (m) {
                  return m.key === item;
                }))
                return item;
            });
            metrics.forEach(function (key) {
              exist[key] = row[key];
            });
          }
          else {
            output.documents.push(row);
          }
        });
      });
      output.documents.forEach(function (doc) {
        delete doc._key;
      });

      var sortKey;
      if (query.dimensions && query.dimensions.length > 0 && query.metrics && query.metrics.length > 0) {
        sortKey = query.metrics[0].key;
        output.documents = _.sortBy(output.documents, function (item) {
          return item[sortKey];
        });
        //if (sortDir.toLowerCase() === 'desc')
        output.documents.reverse();
      }
      else if (query.dimensions && query.dimensions.length > 0) {
        sortKey = query.dimensions[0].key;
        output.documents = _.sortBy(output.documents, function (item) {
          return item[sortKey];
        });
        //if (sortDir.toLowerCase() === 'desc')
        output.documents.reverse();
      }

      //console.log(require('util').inspect(output.documents, {depth: null, colors: true}));
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