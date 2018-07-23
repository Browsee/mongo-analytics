var mongoose = require('../../node_modules/mongoose');
var Schema = mongoose.Schema,
ObjectId = Schema.ObjectId;

var DataRowModel = {
  metric_name: String,
  time_bucket: Number,
  dimension1: String,
  dimension2: String,
  dimension3: String,
  dimension4: String,
  dimension5: String,
  dimension6: String,
  dimension7: Number,
  dimension8: Number,
  metric: Number,
  count: Number,
}

var DataRowSchema = new mongoose.Schema(DataRowModel);
DataRowSchema.index({ metric_name: 1,
                      time_bucket: 1,
                      dimension1: 1,
                      dimension2: 1,
                      dimension3: 1,
                      dimension4: 1,
                      dimension5: 1,
                      dimension6: 1,
                      dimension7: 1,
                      dimension8: 1}, {name: 'DataRowIndex'});

var DataRow = mongoose.model('DataRow', DataRowSchema);
module.exports = DataRow;
