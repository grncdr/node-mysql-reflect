var refify = require('refify')
var lookup = require('lookup');
var pluck = require('pluck');

module.exports = reflect;

var query = [
  "SELECT",
  [ "t.table_name as tableName",
    "c.ordinal_position AS position",
    "c.column_name AS columnName",
    "c.data_type AS \"type\"",
    "c.column_key AS columnKey",
    "(NOT c.is_nullable) AS notNull",
    "c.column_default AS \"default\"",
    "k.referenced_table_name AS refersToTable",
    "k.referenced_column_name AS refersToColumn"
  ].join(', '),
  "FROM information_schema.tables AS t",
  "JOIN information_schema.columns AS c ON",
  [ "c.table_schema = t.table_schema",
    "c.table_name = t.table_name" ].join(" AND "),
  "LEFT OUTER JOIN information_schema.key_column_usage AS k ON",
  [ "k.table_schema = t.table_schema",
    "k.table_name = c.table_name",
    "k.column_name = c.column_name" ].join(" AND "),
  "WHERE t.table_schema = (SELECT DATABASE())"
].join(' ')

function reflect (connection, callback) {
  connection.query(query, function (err, rows) {
    if (err) return callback(err);
    var tableLookup = lookup.reduce(rows, 'tableName', 'concat')
    var schema = mapO(tableLookup, makeTable)
    callback(null, refify.parse(schema))
  })
}

function makeTable(name, columns) {
  var table = {name: name, primaryKey: [], indexes: []}
  table.columns = mapO(lookup.reduce(columns, 'columnName', 'concat'),
                       columnSchema,
                       table)
  return table;
}

function columnSchema (name, references) {
  if (references[0].columnKey) {
    this.primaryKey.push(ref(this.name, 'columns', name))
  }
  return {
    name: name,
    table: ref(this.name),
    position: references[0].position,
    type: references[0].type,
    notNull: Boolean(references[0].notNull),
    default: references[0].default,
    refersTo: references.filter(pluck('refersToTable')).map(function (r) {
      return ref(r.refersToTable, "columns", r.refersToColumn)
    })
  }
}

function mapO (o, f, ctx) {
  var res = {}
  Object.keys(o).map(function (k) {
    return res[k] = f.call(this, k, o[k])
  }, ctx || o);
  return res;
}

function _reflect (connection, callback) {
  var self = this;
  connection.query(queries.tableNames, function (err, rows) {
    if (err) return callback(err);
    map(rows, reflectTable, function (err, tables) {
      if (err) return callback(err)
      callback(null, lookup.reduce(tables, 'name'))
    })
  })
 
  function reflectTable (row, i, nextTable) {
    var table = {
      name: row.TABLE_NAME,
      columns: {},
      foreignKeys: [],
      indexes: []
    }

    map(tableProperties, function (getter, propertyName, nextProperty) {
      getter(connection, table.name, function (err, result) {
        if (err) return nextProperty(err)
        table[propertyName] = result
        nextProperty()
      })
    }, function (err) {
      nextTable(err, table);
    })
  }
}

function getColumns (connection, tablename, cb) {
  connection.query(queries.columns, tablename, function (err, rows) {
    if (err) return cb(err);
    cb(null, rows.map(function (row) {
      return {
        position: row.ORDINAL_POSITION,
        name: row.COLUMN_NAME,
        type: row.DATA_TYPE, // TODO - normalize types
        primaryKey: row.COLUMN_KEY.toUpperCase() == 'PRI',
        notNull: !row.IS_NULLABLE,
        default: row.COLUMN_DEFAULT
      }
    }).reduce(lookup('name'), {}))
  })
}

function getIndexes (connection, tablename, cb) {
  connection.query(queries.indexes, tablename, function (err, rows) {
    if (err) return cb(err)
    cb(err, rows.map(function (row) {
      return {
        name: row.INDEX_NAME,
        type: row.INDEX_TYPE,
        columns: row.COLUMN_NAME
      }
    }))
  })
}

function getForeignKeys (connection, tablename, cb) {
  connection.query(queries.foreignKeys, tablename, function (err, rows) {
    if (err) return cb(err)
    rows.forEach(function (row) {
      return {
        type: row.INDEX_TYPE,
        columns: row.COLUMN_NAME
      }
    })
    cb(err, foreignKeys)
  })
}

var queries = {
  tableNames: "SELECT * FROM INFORMATION_SCHEMA.TABLES " +
    "WHERE TABLE_SCHEMA = (SELECT DATABASE())",

  columns: 'SELECT * FROM information_schema.columns ' +
    'WHERE TABLE_SCHEMA = (SELECT DATABASE()) AND TABLE_NAME = ?',

  indexes: 'SELECT * FROM INFORMATION_SCHEMA.STATISTICS ' +
    'WHERE TABLE_SCHEMA = (SELECT DATABASE()) AND TABLE_NAME = ?',

  foreignKeys: 'SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ' +
    'WHERE TABLE_SCHEMA = (SELECT DATABASE()) AND TABLE_NAME = ?' + 
    'AND REFERENCED_TABLE_NAME IS NOT NULL'
}

var tableProperties = {
  columns: getColumns,
  indexes: getIndexes,
  foreignKeys: getForeignKeys
}


function ref () {
  var parts = [].slice.call(arguments)
  parts.unshift('#')
  return {$ref: parts.join('/')}
}
