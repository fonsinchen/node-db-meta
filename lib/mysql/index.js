module.exports = Index;

var util = require('util');
var dbmUtil = require('../util');
var BaseIndex = require('../index');

function Index(props) {
  this.meta = dbmUtil.lowercaseKeys(props);
}
util.inherits(Index, BaseIndex);

Index.prototype.getName = function() {
  return this.meta.key_name;
};

Index.prototype.getSchemaName = function() {
  return this.meta.schema;
};

Index.prototype.getTableName = function() {
  return this.meta.table;
};

Index.prototype.getColumnNames = function() {
  return this.meta.column_names;
};