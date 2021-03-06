module.exports = Index;

var util = require('util');
var dbmUtil = require('../util');
var BaseIndex = require('../index');

function Index(props) {
  this.meta = dbmUtil.lowercaseKeys(props);
}
util.inherits(Index, BaseIndex);

Index.prototype.getName = function() {
  return this.meta.index_name;
};

Index.prototype.getSchemaName = function() {
  return this.meta.schema_name;
};

Index.prototype.getTableName = function() {
  return this.meta.table_name;
};

Index.prototype.getColumnNames = function() {
  return this.meta.column_names;
};