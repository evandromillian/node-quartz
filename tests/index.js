'use strict';

let fs          = require('fs');
let path        = require('path');
let Sequelize   = require('sequelize');
let basename    = path.basename(module.filename);
var db          = {};

let sequelize = new Sequelize('quartz',
                              'root',
                              '123456',
                              {
                                  host: '192.168.234.130',
                                  port: '3306',
                                  dialect: 'mysql'
                              });

fs
  .readdirSync(__dirname)
  .filter(function(file) {
    return (file.indexOf('.') !== 0) && (file !== basename) && (file.slice(-3) === '.js');
  })
  .forEach(function(file) {
    var model = sequelize['import'](path.join(__dirname, file));
    db[model.name] = model;
  });

Object.keys(db).forEach(function(modelName) {
  if (db[modelName].associate) {
    db[modelName].associate(db);
  }
});

db.db = sequelize;

module.exports = db;