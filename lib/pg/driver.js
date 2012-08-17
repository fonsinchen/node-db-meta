var pg = require('pg');
var Table = require('../table');
var Column = require('./column');
var Index = require('./index');
var ForeignKey = require('./foreign-key');

exports.connect = function (options, callback) {
  var client = new pg.Client(options);
  client.connect(onConnect);
  function onConnect(err) {
    callback(err, new Driver(client));
  }
};

exports.connectToExistingConnection = function(client, callback){
  callback(null, new Driver(client));
};

function Driver(client) {
  this.client = client;
}

Driver.prototype.getVersion = function (callback) {
  this.client.query('select version()', onResult);

  function onResult(err, result) {
    if (err) {
      return callback(err);
    }

    return callback(null, result.rows[0].version);
  }
};

Driver.prototype.getSchemata = function (callback) {
  this.client.query("SELECT * FROM information_schema.schemata", function(err, result) {
    if (err) return callback(err);
    result.rows.push({schema_name : "public"});
    return callback(null, result.rows.map(function (row) {
      return new Schema(row);
    }));
  });
};

Driver.prototype.getTables = function (schemaName, callback) {
  var handler = handleResults.bind(this, Table, callback);
  this.client.query("SELECT * FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = $1",
      [schemaName], handler);
};

Driver.prototype.getColumns = function (schemaName, tableName, callback) {
  var handler = handleResults.bind(this, Column, findPrimaryKeys.bind(this));
  this.client.query("SELECT * FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
      [schemaName, tableName], handler);

  function findPrimaryKeys(err, columns) {
    if (err) {
      return callback(err);
    }

    return this.client.query("select cu.column_name " +
      "from information_schema.key_column_usage cu, " +
      "information_schema.table_constraints tc " +
      "where tc.table_name = cu.table_name " +
      "and tc.constraint_type = 'PRIMARY KEY' " +
      "and tc.constraint_name = cu.constraint_name " +
      "and tc.table_schema = $1" +
      "and tc.table_name = $2",
      [schemaName, tableName],
      onPrimaryKeysResult.bind(this, columns, this.client)
    );

  }

  function onPrimaryKeysResult(columns, client, err, result) {
    if (err) {
      return callback(err);
    }

    var primaryKeys = result.rows.map(function (row) {
      return row.column_name;
    });

    columns.forEach(function (column) {
      if (arrayContains(primaryKeys, column.getName())) {
        column.meta.primary_key = true;
      }
    });
    return findUniqueColumns(null, columns, client);
  }

  function findUniqueColumns(err, columns, client) {
    if (err) {
      return callback(err);
    }
    return client.query("select cu.column_name " +
      "from information_schema.key_column_usage cu, " +
      "information_schema.table_constraints tc " +
      "where tc.table_name = cu.table_name " +
      "and tc.constraint_type = 'UNIQUE' " +
      "and tc.constraint_name = cu.constraint_name " +
      "and tc.table_schema = $1" +
      "and tc.table_name = $2",
      [schemaName, tableName],
      onUniqueColumnsResult.bind(this, columns, client)
    );
  }

  function onUniqueColumnsResult(columns, client, err, result) {
    if (err) {
      return callback(err);
    }
    var uniqueColumns = result.rows.map(function (row) {
      return row.column_name;
    });

    columns.forEach(function (column) {
      if (arrayContains(uniqueColumns, column.getName())) {
        column.meta.unique = true;
      }
    });
    return checkAutoincrement(null, columns, client);
  }

  function checkAutoincrement(err, columns, client) {
    if (err) {
      return callback(err);
    }
    return client.query("SELECT pg_class.relname FROM pg_class JOIN pg_namespace " +
        "ON pg_class.relnamespace=pg_namespace.oid " +
        "WHERE nspname=$1 AND relkind='S'",
        [schemaName], onAutoIncrementResult.bind(this, columns)
    );
  }

  function onAutoIncrementResult(columns, err, result) {
    if (err) {
      return callback(err);
    }
    var autoIncrementingColumns = new Array();
    result.rows.forEach(function(sequence){
      var seqString = sequence.relname;
      if (seqString.indexOf(tableName) >= 0){
        var index = seqString.indexOf('_')+1;
        var colName = seqString.substring(index, seqString.indexOf('_',index));
        autoIncrementingColumns.push(colName);
      }
    });
    columns.forEach(function(column){
      if(arrayContains(autoIncrementingColumns, column.meta.column_name)){
        column.meta.autoincrement = true;
      }
    });
    return callback(null, columns);
  }
};

Driver.prototype.getForeignKeys = function(schemaName, tableName, callback) {
  var columns = {};
  var self = this;
  var queryColumnPositions = function(schemaName, tableName, callback) {
    if (columns[schemaName] === undefined) {
      columns[schemaName] = {};
    }
    if (columns[schemaName][tableName] !== undefined) {
      callback(columns[schemaName][tableName]);
    } else {
      self.client.query("SELECT table_name, column_name, ordinal_position " + 
          "FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
          [schemaName, tableName], function(err, result) {
        if (err) return callback(err);
        columns[schemaName][tableName] = [];
        result.rows.forEach(function (row) {
          columns[schemaName][tableName][row.ordinal_position] = row.column_name;
        });
        return callback(null, columns[schemaName][tableName]);
      });
    }
  };

  var sql = "SELECT c.conname AS name, c.conkey AS columns, c.confkey AS foreign_columns, " +
      "n.nspname AS schema_name, t.relname AS table_name, " +
      "n2.nspname AS foreign_schema_name, t2.relname AS foreign_table_name " +
      "FROM pg_constraint c " +
      "JOIN pg_class t ON c.conrelid  = t.oid " + 
      "JOIN pg_class t2 ON c.confrelid = t2.oid " + 
      "JOIN pg_namespace n ON t.relnamespace=n.oid " +
      "JOIN pg_namespace n2 ON t2.relnamespace=n2.oid " +
      "WHERE c.contype='f' AND n.nspname=$1 AND t.relname= $2";
  return self.client.query(sql, [schemaName, tableName], function(err, result) {
    if (err) return callback(err);
    var objects = [];
    return queryColumnPositions(schemaName, tableName, function(err, columns) {
      if (err) {
        return callback(err);
      } else if (result.rows.length === 0) {
        return callback(null, objects);
      } else {
        return result.rows.forEach(function(row) {
          row.table_name = tableName;
          queryColumnPositions(row.foreign_schema_name, row.foreign_table_name, function(err, fcolumns) {
            if (err) return callback(err);
            row.column_names = [];
            row.columns.forEach(function(index) {
              row.column_names.push(columns[index]);
            });
            row.foreign_column_names = [];
            row.foreign_columns.forEach(function(index) {
              row.foreign_column_names.push(fcolumns[index]);
            });
            objects.push(new ForeignKey(row));
            if (objects.length === result.rows.length) {
              return callback(null, objects);
            } else {
              return null;
            }
          });
        });
      }
    });
  });
};

Driver.prototype.getIndexes = function(schemaName, tableName, callback) {
  function createIndexes(err, result) {
    if (err) return callback(err);
    var indexes = {};
    result.rows.forEach(function(row) {
      if (indexes[row.index_name] === undefined) {
        row.column_names = [row.column_name];
        delete row.column_name;
        indexes[row.index_name] = row; 
      } else {
        indexes[row.index_name].column_names.push(row.column_name);
      }
    });
    var ret = [];
    for (var name in indexes) {
      if (indexes.hasOwnProperty(name)) {
        ret.push(new Index(indexes[name]));
      }
    }
    return callback(null, ret);
  }
  
  var sql = "select n.nspname as schema_name, t.relname as table_name, " +
    "i.relname as index_name, " +
    "a.attname as column_name " +
    "from pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n " +
    "where t.oid = ix.indrelid " +
    "and t.relnamespace = n.oid " +
    "and i.oid = ix.indexrelid " +
    "and a.attrelid = t.oid " +
    "and a.attnum = ANY(ix.indkey) " +
    "and t.relkind = 'r' " +
    "and n.nspname = $1 " +
    "and t.relname = $2";

  this.client.query(sql, [schemaName, tableName], createIndexes);
};

Driver.prototype.close = function(callback) {
  this.client.end();
  callback();
};

function handleResults(obj, callback, err, result) {
  if (err) return callback(err);

  return callback(null, result.rows.map(function (row) {
    return new obj(row);
  }));
}

function arrayContains(arr, item) {
  for (var i = 0; i < arr.length; i++) {
    if (arr[i] === item) {
      return true;
    }
  }
  return false;
}
