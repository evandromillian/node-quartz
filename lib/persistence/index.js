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

    const query = 'SELECT val FROM key_values WHERE ky = :key AND dt > CURRENT_TIMESTAMP';

    let get = function(key, callback) {
        db.query(query, 
            {
                type: 'SELECT',
                replacements: { 
                    key: key 
                } 
            }).then(function(results) {
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


    const expire_query = 'UPDATE key_values SET dt = CURRENT_TIMESTAMP where ky = :key';

    let expire = function(key, callback) {
        db.query(expire_query,
                { replacements: { key: key } })
          .spread(function(results, metadata) {
                if (callback != undefined) {
                    let affectedRows = results.affectedRows;
                    callback(undefined, (affectedRows > 0 ? 'Ok' : '-1' ));
                }
           });
    }

    const rpush_query = 'INSERT INTO key_values (idx, ky, val) \
                            SELECT MAX(idx) + 1, :key, :value \
                            FROM key_values \
                            WHERE ky like :list_name';

    /**
     * 
     * Obs: not taking into account expired values, only insert
     * @param {String} key 
     * @param {String[]} values
     */
    let rpush = function(key, values, callback) {
        let is_array = values.constructor === Array;

        let list_name = key.substr(0, key.lastIndexOf(':') + 1);
        db.transaction(function(t) {
            var promises = [];
            if (is_array) {
                for (var i = 0; i < values.length; i++) {
                    let p = db.query(rpush_query, { 
                            type: 'SELECT',
                            transaction: t, 
                            replacements: {
                                key: key,
                                value: values[i],
                                list_name: list_name + '%'
                            }
                        });
                    promises.push(p);
                }

            } else {
                let p = db.query(rpush_query, { 
                            type: 'SELECT',
                            transaction: t, 
                            replacements: {
                                key: key,
                                value: values,
                                list_name: list_name + '%'
                            }
                        });
                promises.push(p);
            }

            return Promise.all(promises);
        }).then(function (result) {
            console.log('Foi');
        }).catch(function (err) {
            console.log('Não foi: %s', err);
        });
    }

    const rpoplpush_insert = 
            'insert into key_values(idx, ky, val) \
             select	ifnull(minid.idx - 1, 0), \
                concat(:insert_list, SUBSTRING_INDEX(tb.ky, ":", -1)), \
                tb.val \
             from key_values tb \
             left join ( \
                select id, min(idx) as idx \
                from key_values \
                where ky like :insert_list_like \
                group by id, ky \
                order by min(idx) desc \
                limit 1 \
             ) minid on minid.id = tb.id \
             where tb.id = :id_to_pop';

    const rpoplpush_select = 
            'select id, ky, val \
             from key_values \
                where ky like :select_list_like \
                group by id, ky \
                order by max(idx) desc \
                limit 1';

    let rpoplpush = function(list_key_to_pop, list_key_to_push, callback) {
        var popped_item = null;

        db.transaction(function(t) {
            return Promise.all([
                db.query('SELECT IFNULL(MIN(idx) - 1, 0) AS idx FROM key_values WHERE ky LIKE :dest_list GROUP BY ky ORDER BY MIN(idx) ASC LIMIT 1', {
                    type: 'SELECT',
                    transaction: t,
                    replacements: {
                        dest_list: list_key_to_push + ':%'
                    }
                }),
                db.query('SELECT id, ky, val FROM key_values WHERE ky LIKE :source_list GROUP BY id, ky ORDER BY MAX(idx) DESC LIMIT 1', {
                    type: 'SELECT',
                    transaction: t,
                    replacements: {
                        source_list: list_key_to_pop + ':%'
                    }
                })
            ]).then(function(results) {
                let push_idx = (results[0][0] == undefined) ? 0 : results[0][0].idx;
                let popped_row = results[1][0];
                popped_item = popped_row.val;
                let json_popped_item = JSON.parse(popped_item);
                
                return Promise.all([
                    db.Keys.create({
                        ky: list_key_to_push + ':' + json_popped_item.jobid,
                        val: popped_item,
                        idx: push_idx
                    }, { transaction: t }),
                    db.Keys.destroy({ where: { id: popped_row.id } }, { transaction: t })
                ])
            })
        }).then(function (result) {
            if (callback != undefined) {
                callback(null, popped_item);
            }
        }).catch(function (err) {
            if (callback != undefined) {
                callback(err, null);
            }
        });
    }

    const lrem_query = 'delete from key_values where ky like :list_name order by idx asc limit :count';

    let lrem = function(list_key, count, value, callback) {

    }

    return {
        set: set,
        get: get,
        del: del,
        keys: keys,
        expire: expire,
        rpush: rpush,
        rpoplpush: rpoplpush,
        lrem: lrem
    };

}

module.exports = {
    createClient: createClient
}