'use strict';

let util          = require('util');
let fs            = require('fs');
let path          = require('path');
let basename      = path.basename(module.filename);
let Sequelize     = require('sequelize');

let default_port  = 3306,
    default_host  = "127.0.0.1",
    db            = {};

function createClient(db_name, user, password, host, port, type) {
    let options = {
                    host: host || default_host,
                    port: port || default_port,
                    dialect: type
                };
    let db = new Sequelize(db_name, user, password, options);

    fs
    .readdirSync(__dirname)
    .filter(function(file) {
        return (file.indexOf('.') !== 0) && (file !== basename) && (file.slice(-3) === '.js');
    })
    .forEach(function(file) {
        var model = db['import'](path.join(__dirname, file));
        db[model.name] = model;
    });

    Object.keys(db).forEach(function(modelName) {
        if (db[modelName].associate) {
            db[modelName].associate(db);
        }
    });

    // Update master
    // Use a t

    let set = function(key, value, set_cond, expire_type, expire_time, callback) {
        set_cond = set_cond || '';
        expire_type = expire_type || '';
        expire_time = Math.abs(expire_time || 0);

        var query;
        var query2;
        if (set_cond == 'NX') {
            // Set if not exists (or, if 'expired')
            query = util.format("INSERT IGNORE INTO key_values (ky, val) \
                                 VALUES('%s', '%s')",
                                 key, value);

            query2 = util.format("UPDATE key_values \
                                 SET val = '%s', \
                                     dt = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL %d SECOND) \
                                 WHERE ky = '%s' \
                                 AND dt < CURRENT_TIMESTAMP",
                                 value, expire_time, key);

        } else if (set_cond == 'XX') {
            // Set if exists
            query = util.format("UPDATE key_values \
                                 SET val = '%s', \
                                     dt = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL %d SECOND) \
                                 WHERE ky = '%s'",
                                 value, expire_time, key);

        } else {
            // Just set
            query = util.format("INSERT INTO key_values (ky, val, dt) \
                                 VALUES('%s', '%s', DATE_ADD(CURRENT_TIMESTAMP, INTERVAL %d SECOND)) \
                                 ON DUPLICATE KEY UPDATE val = '%s', \
                                                         dt = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL %d SECOND)",
                                 key, value, expire_time, value, expire_time);

        }        

        db.query(query)
          .spread(function(results, metadata) {
                if (callback != undefined) {
                    let affectedRows = results.affectedRows;

                    if (affectedRows < 0 && query2 != undefined) {
                            db.query(query2)
                            .spread(function(results, metadata) { 
                                callback(undefined, (affectedRows > 0 ? 'Ok' : '-1' ));
                            });
                    } 

                    callback(undefined, (affectedRows > 0 ? 'Ok' : '-1' ));
                }
            });
    }

    let get = function(key, callback) {
        let query = 'SELECT val \
                     FROM key_values \
                     WHERE ky = :key \
                     AND dt > CURRENT_TIMESTAMP';

        db.query(query, { replacements: { key: key } })
          .spread(function(results, metadata) {
              console.log("Get results: %s", JSON.stringify(results));
              if (callback != undefined) {
                if (results.length > 0) {
                    callback(null, results[0].val);
                } else {
                    callback(null, null);
                }
              }
          });
    }

    let del = function(key, callback) {
        db.Keys.destroy({
            where: {
                ky: key
            }
        }).then(function() {
            if (callback != undefined) {
                callback();
            }
        }).catch(function(err) {
            if (callback != undefined) {
                callback(err);
            }
        });
    }

    let keys = function(key, callback) {
        key = key.replace('*', '%');

        db.Keys.findAndCountAll({
            where: {
                ky: {
                    $like: key
                }
            },
            attributes: [ 'ky' ]
        }).then(function(result) {
            if (callback != undefined) {
                var results = [];
                for (var i = 0; i < result.count; i++) {
                    results.push(result.rows[i].ky)
                }
                
                callback(undefined, results);
            }

        }).catch(function(err) {
            callback(err);
        });
    }

    let expire = function(key, callback) {
        db.query('UPDATE key_values SET dt = CURRENT_TIMESTAMP where ky = :key',
                { replacements: { key: key } })
          .spread(function(results, metadata) {
                if (callback != undefined) {
                    let affectedRows = results.affectedRows;
                    callback(undefined, (affectedRows > 0 ? 'Ok' : '-1' ));
                }
           });
    }

    /**
     * 
     * Obs: not taking into account expired values, only insert
     * @param {String} key 
     * @param {String} value 
     */
    let rpush = function(key, value, callback) {
        let list_name = key.substr(0, key.lastIndexOf(':') + 1);
        db.query('INSERT INTO key_values (idx, ky, val) SELECT MAX(idx) + 1, :key, :value from key_values where ky like :list_name',
                 { replacements: {
                    key: key,
                    value: value,
                    list_name: list_name + '%'
                 }})
            .spread(function(results, metadata) {
                if (callback != undefined) {
                    let affectedRows = results.affectedRows;

                    
                }
            });
    }

    return {
        set: set,
        get: get,
        del: del,
        keys: keys,
        expire: expire
    };

}

module.exports = {
    createClient: createClient
}