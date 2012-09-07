var path = require('path');

module.exports = function (driverName, options, callback) {
  if (arguments.length < 3) {
    callback = options;
    options = {};
  }

  try {
    var driverPath = path.join(__dirname, driverName, 'driver');
    var driver = require(driverPath);
    
    if (options.hasOwnProperty("connection")) {
      driver.connectToExistingConnection(options["connection"], callback);
    } else {
      driver.connect(options, callback);
    }
  } catch (e) {
    console.log(e);
    callback(new Error('Unsupported driver: ' + driverName));
  }
};

module.exports.collect = function(driverName, options, user_callback) {
  module.exports(driverName, options, function(err, meta) {
    if (err) return user_callback(err);
    var callback = function(err, structure) {
      meta.close(function() {
        user_callback(err, structure);
      })
    };

    var result = {};
    var jobs = 0;

    meta.getSchemata(function(err, schemata) {
      if (err) schemata = [{getName : function() {return "";}}];
      schemata.forEach(function(schema) {
        var tschema = schema.getName();
        var schemaDesc = (result[tschema] = {});
        
        meta.getTables(schema.getName(), function(err, tables) {
          tables.forEach(function(table) {
            var tname = table.getName();
            var tableDesc = (schemaDesc[tname] = {});

            jobs += 3;

            meta.getColumns(tschema, tname, function(err, columns) {
              if (err) return callback(err);
              var tcolumns = (tableDesc.columns = {});
              columns.forEach(function(column) {
                tcolumns[column.getName()] = {
                  type : column.getDataType(),
                  length : column.getMaxLength(),
                  nullable : column.isNullable(),
                  pkey : column.isPrimaryKey(),
                  inc : column.isAutoIncrementing(),
                  unique : column.isUnique(),
                  "default" : column.getDefaultValue()
                };
              });
              if (--jobs === 0) return callback(null, result);
              return null;
            });

            meta.getIndexes(tschema, tname, function(err, indexes) {
              if (err) return callback(err);
              var tindexes = (tableDesc.indexes = {});
              indexes.forEach(function(index) {
                tindexes[index.getName()] = index.getColumnNames();
              });
              if (--jobs === 0) return callback(null, result);
              return null;
            });

            meta.getForeignKeys(tschema, tname, function(err, fkeys) {
              if (err) return callback(err);
              var tfkeys = (tableDesc.fkeys = {});
              fkeys.forEach(function(fkey) {
                tfkeys[fkey.getName()] = {
                  columns : fkey.getColumnNames(),
                  foreign_schema : fkey.getForeignSchemaName(),
                  foreign_table : fkey.getForeignTableName(),
                  foreign_columns : fkey.getForeignColumnNames()
                };
              });
              if (--jobs === 0) return callback(null, result);
              return null;
            });

          });
        });
      });
    });
    return null;
  });
};