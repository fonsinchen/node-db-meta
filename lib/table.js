module.exports = Table;

var dbmUtil = require('./util');

function Table(props) {
  this.meta = dbmUtil.lowercaseKeys(props);
}

Table.prototype.getName = function() {
  return this.meta.table_schema + '.' + this.meta.table_name;
};

Table.prototype.getLocalName = function() {
  return this.meta.table_name;
};

Table.prototype.getSchemaName = function() {
  return this.meta.table_schema;
};