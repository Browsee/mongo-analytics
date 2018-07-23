const models = require('../models');

MongoAnalytics = exports;

const MS_IN_WEEK = 7*24*60*60*1000;
const MS_IN_HALF_HOUR = 30*60*1000;
const MAX_TOP_RESULTS = 100;

function _getTimeBucketWithGranularity(time, granularity) {
  if (granularity === 'day') {
    var halfHourBucket = (int) (time / MS_IN_HALF_HOUR);
  } else if (granularity === 'week') {
    var halfHourBucket = (int) (time / MS_IN_WEEK);
  } 
}

function _getHalfHourBucket(time) {
  return Math.floor(time / MS_IN_HALF_HOUR);
}

/* Save a data row in the database.
*/
MongoAnalytics.saveMetric = function(time, granularity, metricName,
                                     dimensions, metric, count) {
  var query = {};
  query.time_bucket = _getHalfHourBucket(time);
  query.metric_name = metricName;
  for (var dim in dimensions) {
    if (dim.match(/dimension[0-9]/)) {
      query[dim] = dimensions[dim];
    }
  }
  if (!count) {
    count = 1;
  }
  var update = {"$inc": {"metric": metric, "count": 1}};
  return models.DataRow.update(query, update, {upsert: true});
};

MongoAnalytics.findMetricGroupedBy = function(metricName,
                                              constraintDimensions,
                                              pivotDimensions,
                                              groupBy,
                                              timeRange) {
  let start, end;
  if (timeRange && timeRange.length > 1) { 
    start = _getHalfHourBucket(timeRange[0]);
    end = _getHalfHourBucket(timeRange[1]);
  } else {
    var now = (new Date()).getTime();
    var then = now - MS_IN_WEEK;
    start = _getHalfHourBucket(then);
    end = _getHalfHourBucket(now);
  }
  
  var match = { 'metric_name': metricName,
                'time_bucket': { '$gte': start, '$lte': end}};
  for (var dim in constraintDimensions) {
    if (dim.match(/dimension[0-9]/)) {
      match[dim] = constraintDimensions[dim];
    }
  }

  var _id = groupBy
  for (var dim in pivotDimensions) {
    if (dim.match(/dimension[0-9]/)) {
      _id[pivotDimensions[dim]] = '$' + dim;
    }
  }
  return models.DataRow.aggregate([{ '$match': match },
      { '$group': { '_id' : _id,
                    [metricName]: {'$sum': "$metric"},
                    'count': {'$sum': "$count"}}},
      { '$sort': { 'time': 1 } }]);
  
};

/* Fetch metric count for a time range with certain dimensions
*  added up as a total over time
*/
MongoAnalytics.findMetricTotal = function(metricName,
                                          constraintDimensions,
                                          pivotDimensions,
                                          timeRange) {
  return MongoAnalytics.findMetricGroupedBy(metricName,
                                            constraintDimensions,
                                            pivotDimensions,
                                            {},
                                            timeRange) 
};

/* Fetch metric count for a time range with certain dimensions
*  time range (defaults to last one week)
*/
MongoAnalytics.findMetric = function(metricName,
                                     constraintDimensions,
                                     pivotDimensions,
                                     timeRange) {
  return MongoAnalytics.findMetricGroupedBy(metricName,
                                            constraintDimensions,
                                            pivotDimensions,
                                            {'time': '$time_bucket'},
                                            timeRange) 
};

/* Fetch top N entries for a time range with certain dimensions
*  time range (defaults to last one week)
*  We can further add constraint dimensions, which will be fixed for the query
*  and we can add pivot dimensions which basically make unique keys for each
*  row.
*/
MongoAnalytics.findTop = function(metricName,
                                  constraintDimensions,
                                  pivotDimensions,
                                  timeRange,
                                  numResults) {
  let start, end;
  if (timeRange && timeRange.length > 1) { 
    start = _getHalfHourBucket(timeRange[0]);
    end = _getHalfHourBucket(timeRange[1]);
  } else {
    var now = (new Date()).getTime();
    var then = now - MS_IN_WEEK;
    start = _getHalfHourBucket(then);
    end = _getHalfHourBucket(now);
  }
  
  var match = { 'metric_name': metricName,
                'time_bucket': { '$gte': start, '$lte': end}};
  for (var dim in constraintDimensions) {
    if (dim.match(/dimension[0-9]/)) {
      match[dim] = constraintDimensions[dim];
    }
  }

  var _id = {'time': '$time_bucket'}
  for (var dim in pivotDimensions) {
    if (dim.match(/dimension[0-9]/)) {
      _id[pivotDimensions[dim]] = '$' + dim;
    }
  }

  var limit = numResults || MAX_TOP_RESULTS
  return models.DataRow.aggregate([{ '$match': match },
      { '$group': { '_id' : _id, [metricName]: {'$sum': "$metric"}}},
      { '$sort': { [metricName]: -1 } },
      { '$limit': limit}]);
  
};
