/**
 * methods for ElasticSearch DB
 */
module.exports = function (conf, logger) {
    var elasticsearch = require('elasticsearch');


    var client = new elasticsearch.Client({
        host: conf.elasticsearch.host,
        log: conf.elasticsearch.log.level
    });

    return client;
}