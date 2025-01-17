/**
 * methods for multiple DBs connections and initialization
 */
module.exports = function (conf, logger) {
    const { Pool } = require('pg');
    var pool = {};
    
    Object.keys(conf.db).forEach( name=> {
        let db_config = conf.db[name];

        pool[name] = new Pool(db_config);
        
        if(conf.db[name].schema) pool[name].on('connect', (client) => {
            client.query(`SET search_path TO ${conf.db[name].schema}`);
        });
        
        pool[name].execute = async query => {
            let result = (await pool[name].query(query));
            return Array.isArray(result)? result.map(e => e.rows) : result.rows;
        }
    });

    return pool;
}