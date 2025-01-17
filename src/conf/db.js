/**
 * methods for DB connection and initialization
 */
module.exports = function (conf, logger) {
    const util = require('util');  
    const { Pool } = require('pg')
    const pool = new Pool(conf.db);

    if(conf.db.schema) pool.on('connect', (client) => {
        client.query(`SET search_path TO ${conf.db.schema}`);
    });
    var result = {
        pool: pool,
        execute: (query) => execute(query),
        end: pool.end
    };
    async function execute(query){
        let result = (await pool.query(query));
        return Array.isArray(result)? result.map(e => e.rows) : result.rows;
    }
    return result;
}
