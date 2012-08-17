var sqlite3 = require('sqlite3');
var Table = require('../table');
var Column = require('./column');
var Index = require('./index');

exports.connect = function (options, callback) {
  try {
    var client = new sqlite3.Database(options);
    callback(null, new Driver(client));
  } catch (err) {
    callback(err);
  }
};

exports.connectToExistingConnection = function(client, callback){
  callback(null, new Driver(client));
};

function Driver(client) {
  this.client = client;
}

Driver.prototype.getVersion = function (callback) {
  this.client.all('select sqlite_version() as version', onResult);

  function onResult(err, result) {
    if (err) {
      return callback(err);
    }

    callback(null, result[0].version);
  }
};

Driver.prototype.getSchemata = function(callback) {
  return callback("Sqlite doesn't support schemata.");
};

Driver.prototype.getTables = function (schemaName, callback) {
  var handler = handleResults.bind(this, Table, callback);
  this.client.all("SELECT tbl_name as table_name, * from sqlite_master where type = 'table';", handler);
};

Driver.prototype.getColumns = function (schemaName, tableName, callback) {
  var handler = handleResults.bind(this, Column, onResult.bind(this));
  this.client.all("PRAGMA table_info(" + tableName + ")", handler);


  function onResult(err, result){
    if (err) return callback(err);
    var previous = result;
    return this.client.all("SELECT sql from sqlite_master where name = \'" + tableName + "\';", process.bind(this, previous));
  }

  function process(previous,err,result){
    if (err) return callback(err);

    var sql = result[0]['sql'];
    sql = sql.substring( sql.indexOf('\(')+1,sql.lastIndexOf('\)') );
    var strings = sql.split(',');
    var colNumber = 0;
    strings.forEach(function(string){
      string = string.trim();
      string = string.toUpperCase();
      if (string.indexOf('UNIQUE') >= 0){
        // TODO: does not work in case of (a integer, b integer, ... , unique(a, b)) or similar.
        previous[colNumber].meta.unique = true;
      }

      if (string.indexOf('AUTOINCREMENT') >= 0){
        previous[colNumber].meta.auto_increment = true;
      }
      colNumber++;
    });
    return callback(err,previous);
  }

};

Driver.prototype.getIndexes = function (schemaName, tableName, callback) {
  this.client.all("pragma index_list(" + tableName + ")", function (err, result) {
    if (err) return callback(err);

    var indexCount = 0;
    var indexes = [];
    var numIndexes = result.length;

    if (numIndexes === 0) return callback(null, []);

    for (var i = 0; i < numIndexes; i++) {
      var indexName = result[i].name;
      this.client.all("pragma index_info(" + indexName + ")", function (err, result) {
        if (err) return callback(err);

        var indexMeta = {
          index_name : indexName,
          table_name : tableName,
          column_names : []
        }
        for (var j = 0; j < result.length; j++) {
          indexMeta.column_names.push(result[j].name)
        }
        indexes.push(new Index(indexMeta));

        if (++indexCount === numIndexes) {
          return callback(null, indexes);
        }
      });
    }
  }.bind(this));

};

Driver.prototype.getForeignKeys = function(schemaName, tableName, callback) {
  return callback("Foreign Key Analysis isn't implemented for Sqlite.");
};

Driver.prototype.close = function (callback) {
  this.client.close();
  callback();
};

function handleResults(obj, callback, err, result) {
  if (err) return callback(err);

  var objects = result.map(function (row) {
    return new obj(row);
  });
  return callback(null, objects);
}
