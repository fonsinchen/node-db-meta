module.exports = Schema;

var dbmUtil = require('./util');

function Schema(props) {
  this.meta = dbmUtil.lowercaseKeys(props);
}

Schema.prototype.getName = function() {
  return this.meta.schema_name;
};
