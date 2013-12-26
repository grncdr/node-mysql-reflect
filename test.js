var reflect = require('./')

var tape = require('tape')
var mysql = require('mysql')
var refify = require('refify')


var db = mysql.createConnection({user: 'root', database: 'test'})
db.connect(function (err) {
  if (err) throw err
  reflect(db, function (err, schema) {
    if (err) throw err
      debugger
    console.log(refify.stringify(schema, null, 2))
    db.end()
  })
})
