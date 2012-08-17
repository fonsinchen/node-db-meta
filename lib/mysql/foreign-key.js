module.exports = ForeignKey;

var util = require('util');
var baseForeignKey = require('../foreign-key');

function ForeignKey(props) {
  this.meta = props;
}
util.inherits(ForeignKey, baseForeignKey);

ForeignKey.prototype.getName = function() {
  return this.meta.constraint_name;
};

ForeignKey.prototype.getSchemaName = function() {
  return this.meta.table_schema;
};

ForeignKey.prototype.getTableName = function() {
  return this.meta.table_name;
};

ForeignKey.prototype.getColumnNames = function() {
  return this.meta.column_names;
}

ForeignKey.prototype.getForeignSchemaName = function() {
  return this.meta.referenced_table_schema;
}

ForeignKey.prototype.getForeignTableName = function() {
  return this.meta.referenced_table_name;
}

ForeignKey.prototype.getForeignColumnNames = function() {
  return this.meta.referenced_column_names;
}