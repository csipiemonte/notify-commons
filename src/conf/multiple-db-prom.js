/**
 * methods for multiple MySql DB connection and initialize the connection pool
 */
module.exports = async function (conf, logger) {
    var mysql = require('promise-mysql');    
    
    var pool = {};
    for( const db of Object.keys(conf.mysql)){
        let db_config = {
            host: conf.mysql[db].host,
            user: conf.mysql[db].user,
            password: conf.mysql[db].password,
            database: conf.mysql[db].database,
            multipleStatements: true,
            waitForConnections: false,
            queueLimit: 3
        };

        pool[db] = await mysql.createPool(db_config);                     

    }    

    return pool;
}