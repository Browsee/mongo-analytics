
function makeNewConnection(mongoUrl, mongoose) {
    const db = mongoose.createConnection(mongoUrl, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    });

    db.on('error', function (error) {
        console.log(`Analytics MongoDB :: connection ${this.name} ${JSON.stringify(error)}`);
        db.close().catch(() => console.log(`MongoDB :: failed to close connection ${this.name}`));
    });

    db.on('connected', function () {
        console.log(`Analytics MongoDB :: connected ${this.name}`);
    });

    db.on('disconnected', function () {
        console.log(`Analytics MongoDB :: disconnected ${this.name}`);
    });

    return db;
}

exports.getCollection = function(mongoUrl, mongoDB, callback) {
  var tokens = mongoUrl.split('/');
  var dbName = tokens.pop();
  var host = tokens.join('/');
  const client = new mongoDB.MongoClient(host, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  client.connect(function(err) {
    const db = client.db(dbName);
    const collection = db.collection("datarows");
    callback(db, collection);
  });
}

exports.getModel = function(mongoUrl, mongoose, mongoDB) {
  const connection = makeNewConnection(mongoUrl, mongoose);

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
    dimension6: Number,
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
  
  var DataRow = connection.model('DataRow', DataRowSchema);
  return DataRow;
}
