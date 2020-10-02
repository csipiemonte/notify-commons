/**
 * methods for DB connection and initialize the connection pool
 */
module.exports = function (conf, logger) {
    var mysql_cache = require('mysql-cache');
    var mysql = new mysql_cache({...conf.mysql,...conf.cache});

    var conn;
    mysql.connectAsync().then(result => {
        conn = result;
        console.log("connection ", result);
    }).catch( err => {
       logger.error("Error in database connection: ", err.message);
    });

    mysql.event.on('hit', (query, hash, result) => {
        // query  = the sql code that was used
        // hash   = the hash that was generated for the cache key
        // result = the result that was found in the cache
        console.log('mysql-cache hit a cache object!')
    })

    var result = {
        execute: async (query) => {
            if(typeof query !== 'object') query = {sql:query, cache: true};
            let res = await mysql.queryAsync(query);
            return res[0];
        }
    };

    return result;
}
