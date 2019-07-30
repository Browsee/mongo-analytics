const models = require('../models');

MongoAnalytics = exports;

const MS_IN_WEEK = 7*24*60*60*1000;
const MS_IN_DAY = 24*30*60*1000;
const MS_IN_HALF_HOUR = 30*60*1000;
const MAX_TOP_RESULTS = 100;

function _getTimeBucketWithGranularity(time, granularity) {
  if (granularity === 'halfhour') {
    return Math.floor(time / MS_IN_HALF_HOUR);
  } else if (granularity === 'day') {
    return Math.floor(time / MS_IN_DAY);
  } else if (granularity === 'week') {
    return Math.floor(time / MS_IN_WEEK);
  } 
  return Math.floor(time / MS_IN_HALF_HOUR);
}

/* Save a data row in the database.
*/
MongoAnalytics.saveMetric = function(time, granularity, metricName,
                                     dimensions, metric, count) {
  var query = {};
  query.time_bucket = _getTimeBucketWithGranularity(time, granularity);
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
                                              granularity,
                                              constraintDimensions,
                                              pivotDimensions,
                                              groupBy,
                                              timeRange) {
  let start, end;
  if (timeRange && timeRange.length > 1) { 
    start = _getTimeBucketWithGranularity(timeRange[0], granularity);
    end = _getTimeBucketWithGranularity(timeRange[1], granularity);
  } else {
    var now = (new Date()).getTime();
    var then = now - MS_IN_WEEK;
    start = _getTimeBucketWithGranularity(then, granularity);
    end = _getTimeBucketWithGranularity(now, granularity);
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
                                          granularity,
                                          constraintDimensions,
                                          pivotDimensions,
                                          timeRange) {
  return MongoAnalytics.findMetricGroupedBy(metricName,
                                            granularity,
                                            constraintDimensions,
                                            pivotDimensions,
                                            {},
                                            timeRange) 
};

/* Fetch metric count for a time range with certain dimensions
*  time range (defaults to last one week)
*/
MongoAnalytics.findMetric = function(metricName,
                                     granularity,
                                     constraintDimensions,
                                     pivotDimensions,
                                     timeRange) {
  return MongoAnalytics.findMetricGroupedBy(metricName,
                                            granularity,
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
                                  granularity,
                                  constraintDimensions,
                                  pivotDimensions,
                                  timeRange,
                                  numResults) {
  let start, end;
  if (timeRange && timeRange.length > 1) { 
    start = _getTimeBucketWithGranularity(timeRange[0], granularity);
    end = _getTimeBucketWithGranularity(timeRange[1], granularity);
  } else {
    var now = (new Date()).getTime();
    var then = now - MS_IN_WEEK;
    start = _getTimeBucketWithGranularity(then, granularity);
    end = _getTimeBucketWithGranularity(now, granularity);
  }
  
  var match = { 'metric_name': metricName,
                'time_bucket': { '$gte': start, '$lte': end}};
  for (var dim in constraintDimensions) {
    if (dim.match(/dimension[0-9]/)) {
      match[dim] = constraintDimensions[dim];
    }
  }

  var _id = {}
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
