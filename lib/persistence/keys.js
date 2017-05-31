"use strict";

module.exports = function(sequelize, DataTypes) {
  var Keys = sequelize.define("Keys", {
    ky: { type: DataTypes.STRING(1000) },
    val: { type: DataTypes.STRING(2000) },
    dt: { type: DataTypes.DATE },
    idx: { type: DataTypes.INTEGER, allowNull: true }
  }, {
    tableName: 'key_values',
    timestamps: false
  });

  return Keys;
};