use strict;

const models = require('../models');

AnalyticsService = exports;

const MS_IN_WEEK = 7*24*60*60*1000;
const MS_IN_HALF_HOUR = 30*60*1000;

_getTimeBucketWithGranularity(time, granularity) {
  if (granularity === 'day') {
    var halfHourBucket = (int) (time / MS_IN_HALF_HOUR);
  } else if (granularity === 'week') {
    var halfHourBucket = (int) (time / MS_IN_HALF_HOUR);
  } 
}

/* Save a data row in the database.
*/
AnalyticsService.saveDataRow = function(time, granularity, dimensions,
                                        metricName, metric, callback) {
  var row = new models.DataRow();
  row.time_bucket = _getTimeBucketWithGranularity(time, granularity);
  row.metric_name = metricName;
  for (var dim in dimensions) {
    if (dim.match(/dimension[0-9]/)) {
      row[dim] = dimensions[dim];
    }
  }
  row.metric = metric;
  row.save(callback);
};

/* Fetch metric count for a time range with certain dimensions
*  for a given granularity (defaults to day)
*  time range (defaults to last one week)
*/
AnalyticsService.findMetric = function(metricName, timeRange, granularity,
                                       dimensions, callback) {
  
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
  for (var dim in dimensions) {
    if (dim.match(/dimension[0-9]/)) {
      match[dim] = dimensions[dim];
    }
  }
  models.DataRow.aggregate([{ '$match': match },
                            { '$group': { 'time': 'time_bucket', 'total': {'$sum': "$metric"}}},
                            { '$sort': { 'time': 1 } }], 
                          callback);
  
};
