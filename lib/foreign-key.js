module.exports = ForeignKey;

function ForeignKey() {}

ForeignKey.iface = ['getName', 'getTableName', 'getColumnNames', 'getForeignTableName', 'getForeignColumnNames'];

ForeignKey.iface.forEach(function(method) {
  ForeignKey.prototype[method] = function() {
    throw new Error(method + ' not yet implemented');
  };
});