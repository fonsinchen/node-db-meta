module.exports = ForeignKey;

var util = require('util');
var dbmUtil = require('../util');
var baseForeignKey = require('../foreign-key');

function ForeignKey(props) {
  this.meta = dbmUtil.lowercaseKeys(props);
}
util.inherits(ForeignKey, baseForeignKey);

ForeignKey.prototype.getName = function() {
  return this.meta.name;
};

ForeignKey.prototype.getSchemaName = function() {
  return this.meta.schema_name;
};

ForeignKey.prototype.getTableName = function() {
  return this.meta.table_name;
};

ForeignKey.prototype.getColumnNames = function() {
  return this.meta.column_names;
}

ForeignKey.prototype.getForeignSchemaName = function() {
  return this.meta.foreign_schema_name;
}

ForeignKey.prototype.getForeignTableName = function() {
  return this.meta.foreign_table_name;
}

ForeignKey.prototype.getForeignColumnNames = function() {
  return this.meta.foreign_column_names;
}