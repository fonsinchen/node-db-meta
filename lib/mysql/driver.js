var mysql = require('mysql');
var Table = require('../table');
var Column = require('./column');
var Index = require('./index');
var ForeignKey = require('./foreign-key');
var dbmUtil = require('../util');

exports.connect = function (options, callback) {
  try {
    var client = mysql.createClient(options);
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
  this.client.query('select version() as version', onResult);

  function onResult(err, result) {
    if (err) return callback(err);
    return callback(null, result[0].version);
  }
};

Driver.prototype.getTables = function (callback) {
  var handler = handleResults.bind(this, Table, callback);
  this.client.query("select * from information_schema.tables", handler);
};

Driver.prototype.getColumns = function (schemaName, tableName, callback) {
  var handler = handleResults.bind(this, Column, callback);
  this.client.query("select * from information_schema.columns where table_schema = ? and table_name = ?", [schemaName, tableName], handler);
};

Driver.prototype.getIndexes = function(schemaName, tableName, callback) {
  var createIndexes = function(err, result) {
    if (err) return callback(err);
    var indexes = {};
    result.forEach(function(row) {
      row = dbmUtil.lowercaseKeys(row);
      if (indexes[row.key_name] === undefined) {
        row.column_names = [row.column_name];
        delete row.column_name;
        row.schema = schemaName;
        indexes[row.key_name] = row; 
      } else {
        indexes[row.key_name].column_names.push(row.column_name);
      }
    });
    var ret = [];
    for (var name in indexes) {
      if (indexes.hasOwnProperty(name)) ret.push(new Index(indexes[name]));
    }
    return callback(null, ret);
  };
  this.client.query("show indexes from " + schemaName + '.' + tableName, createIndexes);
};

Driver.prototype.getForeignKeys = function(schemaName, tableName, callback) {
  this.client.query("select * from information_schema.key_column_usage where table_schema = ? and table_name = ? and referenced_table_name is not null",
      [schemaName, tableName], function(err, result) {
    if (err) return callback(err);
    var fkeys = {};
    result.forEach(function(row) {
      row = dbmUtil.lowercaseKeys(row);
      if (fkeys[row.constraint_name] === undefined) {
        row.column_names = [row.column_name];
        row.referenced_column_names = [row.referenced_column_name];
        delete row.column_name;
        delete row.referenced_column_name;
        fkeys[row.constraint_name] = row; 
      } else {
        fkeys[row.constraint_name].column_names.push(row.column_name);
        fkeys[row.constraint_name].referenced_column_names.push(row.referenced_column_name);
      }
    });
    var ret = [];
    for (var name in fkeys) {
      if (fkeys.hasOwnProperty(name)) ret.push(new ForeignKey(fkeys[name]));
    }
    return callback(null, ret);
  });
};

Driver.prototype.close = function(callback) {
  this.client.end(callback);
};

function handleResults(obj, callback, err, result) {
  if (err) return callback(err);

  return callback(null, result.map(function (row) {
    return new obj(row);
  }));
}
