'use strict';

let util          = require('util');
let fs            = require('fs');
let path          = require('path');
let basename      = path.basename(module.filename);
let winston       = require('winston');
let Sequelize     = require('sequelize');

let max_date_value = '9999-12-31 00:00:00';

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

    let set = function(key, value, set_cond, expire_type, expire_time, callback) {
        set_cond = set_cond || '';
        expire_type = expire_type || '';
        
        expire_time = Math.abs(expire_time) || 0;
        if (expire_type == 'PX') {
            expire_time = expire_time / 1000
        }

        var query;
        var query2;
        if (set_cond == 'NX') {
            console.log('Set if not exists (or, if "expired")')

            let promise = db.transaction(function(t) {
                return Promise.all([
                    db.query('INSERT IGNORE INTO key_values (ky, val) VALUES(:key, :value)', {
                        type: 'INSERT',
                        replacements: {
                            key: key,
                            value: value
                        }
                    }),
                    db.query('UPDATE key_values \
                                SET val = :value, dt = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL :seconds SECOND) \
                                WHERE ky = :key AND dt < CURRENT_TIMESTAMP', {
                        type: 'UPDATE',
                        replacements: {
                            key: key,
                            value: value,
                            seconds: expire_time
                        }
                    })
                ])
            
            });

            if (callback != undefined) {
                promise.then(function(results) {
                    if (callback != undefined) {
                        callback(undefined, 'Ok');
                    }
                }).catch(function(err) {
                    if (callback != undefined) {
                        callback(err, undefined);
                    }
                });
            }

        } else if (set_cond == 'XX') {
            console.log('Set if exists');

            let promise = db.query('UPDATE key_values \
                    SET val = :value, dt = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL :seconds SECOND) \
                    WHERE ky = :key', {
                        type: 'UPDATE',
                        replacements: {
                            key: key,
                            value: value,
                            seconds: expire_time
                        }
                    })

            if (callback != undefined) {      
                promise.then(function(result) {
                    callback(undefined, (result == true ? 'Ok' : '-1' ));
                }).catch(function(err) {
                    callback(err, undefined);
                });
            }

        } else {
            // Just set
            let promise = db.Keys.upsert({
                ky: key,
                val: value,
                dt: '9999-01-01 00:00:00'
            });
            
            if (callback != undefined) {
                promise.then(function(result) {
                    callback(undefined, (result == true ? 'Ok' : '-1' ));
                }).catch(function(err) {
                    callback(err, undefined);
                });
            }

        }        
    }

    const get_query = 'SELECT val FROM key_values \
                        WHERE ky = :key \
                        AND dt > CURRENT_TIMESTAMP \
                        AND idx is NULL';
    /**
     * Get value from key
     * Only values, not lists
     * @param {String} key 
     * @param {function(err, result)} callback 
     */
    let get = function(key, callback) {
        let promise = db.query(get_query, {
            type: 'SELECT',
            replacements: { 
                key: key 
            } 
        });
            
        if (callback != undefined) {
            promise.then(function(results) {
                if (results.length > 0) {
                    callback(undefined, results[0].val);
                } else {
                    callback(undefined, null);
                }
            }).catch(function(err) {
                winston.error("Error in lrem: " + err);

                callback(err, undefined);
            });
        }
    }

    /**
     * 
     * @param {String} key 
     * @param {function(err, result)} callback 
     */
    let del = function(key, callback) {
        let promise = db.Keys.destroy({
            where: {
                ky: key
            }
        });
        
        if (callback != undefined) {
            promise.then(function() {
                callback();
            }).catch(function(err) {
                winston.error("Error in del: " + err);
                callback(err);
            });
        }
    }

    /**
     * 
     * @param {String} key 
     * @param {function(err, result)} callback 
     */
    let keys = function(key, callback) {
        key = key.replace('*', '%');

        let promise = db.Keys.findAndCountAll({
            where: {
                ky: {
                    $like: key
                }
            },
            attributes: [ 'ky' ]
        });
        
        if (callback != undefined) {
            promise.then(function(result) {
                var results = [];
                for (var i = 0; i < result.count; i++) {
                    results.push(result.rows[i].ky)
                }
                
                callback(undefined, results);

            }).catch(function(err) {
                winston.error("Error in keys: " + err);

                callback(err);
            });
        }   
    }


    const expire_query = 'UPDATE key_values SET dt = CURRENT_TIMESTAMP where ky = :key';

    /**
     * 
     * @param {String} key 
     * @param {function(err, result)} callback 
     */
    let expire = function(key, callback) {
        let promise = db.query(expire_query, { 
            replacements: { key: key } 
        });
        
        if (callback != undefined) {
            promise.spread(function(results, metadata) {
                let affectedRows = results.affectedRows;
                callback(undefined, (affectedRows > 0 ? 'Ok' : '-1' ));
            });
        }
    }

    const rpush_query = 'INSERT INTO key_values (idx, ky, val) \
                            SELECT IFNULL(MAX(idx) + 1, 0), :key, :value \
                            FROM key_values \
                            WHERE ky = :key';

    /**
     * 
     * Obs: not taking into account expired values, only insert
     * @param {String} key 
     * @param {String[]} values
     */
    let rpush = function(key, values, callback) {
        let is_array = values.constructor === Array;

        let promise = db.transaction(function(t) {
            var promises = [];
            if (is_array) {
                for (var i = 0; i < values.length; i++) {
                    let p = db.query(rpush_query, { 
                            type: 'INSERT',
                            transaction: t, 
                            replacements: {
                                key: key,
                                value: values[i]
                            }
                        });
                    promises.push(p);
                }

            } else {
                let p = db.query(rpush_query, { 
                            type: 'INSERT',
                            transaction: t, 
                            replacements: {
                                key: key,
                                value: values,
                            }
                        });
                promises.push(p);
            }

            return Promise.all(promises);
        })
        
        if (callback != undefined) {
            promise.then(function (result) {
                db.Keys.count({
                    where: {
                        ky: key
                    }
                }).then(function(result) {
                    callback(undefined, result);
                });

            }).catch(function (err) {
                winston.error("Error in rpush: " + err);
                callback(err, undefined);
            });
        }
    }

    const rpoplpush_select_min_idx = 
            'SELECT IFNULL(MIN(idx) - 1, 0) AS idx \
            FROM key_values \
            WHERE ky = :dest_list \
            GROUP BY ky \
            ORDER BY MIN(idx) ASC LIMIT 1';

    const rpoplpush_select_pop_item = 
            'SELECT id, ky, val \
            FROM key_values \
            WHERE ky = :source_list \
            GROUP BY id, ky \
            ORDER BY MAX(idx) DESC LIMIT 1';

    let rpoplpush = function(list_to_pop, list_to_push, callback) {
        var popped_item = null;

        let promise = db.transaction(function(t) {
            return Promise.all([
                db.query(rpoplpush_select_min_idx, {
                        type: 'SELECT',
                        transaction: t,
                        replacements: {
                            dest_list: list_to_push
                        }
                    }
                ),
                db.query(rpoplpush_select_pop_item, {
                        type: 'SELECT',
                        transaction: t,
                        replacements: {
                            source_list: list_to_pop
                        }
                    }
                )
            ]).then(function(results) {
                let push_idx = (results[0][0] == undefined) ? 0 : results[0][0].idx;
                popped_item = results[1][0];

                if (popped_item == undefined) {
                    return new Promise(function(resolve, reject) {
                        resolve(undefined);
                    })
                }
                
                return Promise.all([
                    db.Keys.create({
                        ky: list_to_push,
                        val: popped_item.val,
                        idx: push_idx
                    }, { transaction: t }),
                    db.Keys.destroy({ 
                        where: { 
                            id: popped_item.id 
                        }
                    }, { transaction: t })
                ])
            })
        });
        
        if (callback != undefined) {
            promise.then(function (result) {
                callback(null, popped_item.val);
            }).catch(function (err) {
                winston.error("Error in rpoplpush: " + err);
                callback(err, null);
            });
        }
    }

    const lrem_query_desc = 'DELETE FROM key_values WHERE ky = :list_key AND val = :value ORDER BY idx DESC LIMIT :count';
    const lrem_query_asc = 'DELETE FROM key_values WHERE ky = :list_key AND val = :value ORDER BY idx ASC LIMIT :count';
    const lrem_query_equal = 'DELETE FROM key_values WHERE ky = :list_key AND val = :value';

    /**
     * 
     * @param {String} list_key 
     * @param {Number} count 
     * @param {String} value 
     * @param {function(err: any, res: any)} callback 
     */
    let lrem = function(list_key, count, value, callback) {
        let query = (count > 0) ? lrem_query_desc :
                    (count < 0) ? lrem_query_asc :
                                  lrem_query_equal;
        let promise = db.query(query, {
            replacements: {
                list_key: list_key,
                value: value,
                count: Math.abs(count),
            }
        })
        
        if (callback != undefined) {
            promise.then(function(result, meta) {
                callback(null, result[0].affectedRows);
            }).catch(function (err) {
                winston.error("Error in lrem: " + err);

                callback(err, null);
            });
        }
    }


    let psubscribe = function(channel) {
        // Record event type
        channel_events_map[channel] = 1;

        // If not started, start pooling to query events from database
        if (subscribe_timer == null) {
            subscribe_timer = setInterval(function() {
                if (psubscribe_callback == null)
                    return;

                // Search for events
                for (var i = 0; i < channel_events_map; i++) {
                    db.Keys.findAll({
                        where: {
                            ky: channel_events_map[i]
                        }
                    }).then(function(results) {
                        for (var j = 0; j < results.length; j++) {
                            let ret = results[j];
                            winston.debug(util.format("Subscribe callback with channel %s and message %s", ret.ky. ret.val));

                            db.Keys.destroy({
                                where: {
                                    ky: ret.ky
                                }
                            })

                            psubscribe_callback('', ret.ky, ret.val);
                        }
                    })
                }

            }, 2000 + (Math.random() * 1000));
        }
    }

    /**
     * 
     * @param {String} type 
     * @param {function(pattern, channel, message)} callback 
     */
    let on = function(type, callback) {
        // TODO store callback per type
        psubscribe_callback = callback;
    }

    
    let publish = function(channel, message) {
        rpush(channel, message);
    }

    var subscribe_timer = null;
    var psubscribe_callback = null;
    let channel_events_map = {};

    return {
        set: set,
        get: get,
        del: del,
        keys: keys,
        expire: expire,
        rpush: rpush,
        rpoplpush: rpoplpush,
        lrem: lrem,
        psubscribe: psubscribe,
        publish: publish
    };

}

module.exports = {
    createClient: createClient
}