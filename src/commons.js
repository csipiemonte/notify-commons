/**
 * Main class that return all the utilities in commons
 */


function isObject(item) {
    return (item && typeof item === 'object' && !Array.isArray(item));
}


/* merge json object recursively, the second json properties will overwrite the first*/
function merge_in(target, ...sources) {

    if (!sources.length) return target;
    const source = sources.shift();

    if (isObject(target) && isObject(source)) {
        for (const key in source) {
            if (isObject(source[key])) {
                if (!target[key]) Object.assign(target, {[key]: {}});
                merge_in(target[key], source[key]);
            } else {
                Object.assign(target, {[key]: source[key]});
            }
        }
    }
    return merge_in(target, ...sources);
}

function merge(target, ...sources) {
    var result = merge_in(target, ...sources);
    Object.keys(process.env)
        .filter(e => e.toUpperCase().startsWith(result.app_name.toUpperCase()))
        .forEach(e => { result = merge_in(result, toObj(e.substring(result.app_name.length +1).toLowerCase(), process.env[e]))} );
    return result;
}

function toObj (k, v, result) {
    if(!result) result = {};
    var _ = k.indexOf("_");
    if(_ == -1)
    {
        result[k] = v;
        return result;
    }
    result[k.substring(0, _)] = toObj(k.substring(_ + 1), v, result[k.substring(0, _)]);
    return result ;
}

function hasOwnNestedProperty(obj, propertyPath){
    if(!propertyPath)
        return false;

    var properties = propertyPath.split('.');

    for (var i = 0; i < properties.length; i++) {
        var prop = properties[i];

        if(!obj || !obj.hasOwnProperty(prop)){
            return false;
        } else {
            obj = obj[prop];
        }
    }

    return true;
};

module.exports = {
    hasOwnNestedProperty:hasOwnNestedProperty,
    merge : merge,
    locales: require("./utilities/locales"), // list of available locales
    obj : function (conf) {
        /* the default configuration of the logger is in "commons.json" and in case the passed conf contains log4js properties, they'll overwrite the first one */
        var logger = require("./conf/logger")(merge(require("./conf/commons"),conf));
        /* event-handler */
        if(hasOwnNestedProperty(conf, "mb.queues.events")) var eh = require("./utilities/event-handler")(conf,logger);
        if(hasOwnNestedProperty(conf, "mb.queues.audit")) var ah = require("./utilities/audit-handler")(conf,logger);
        return {
            db: () => require("./conf/db")(conf, logger),
            db_cache: () => require("./conf/db-cache")(conf, logger),
            multiple_db: () => require("./conf/multiple-db")(conf, logger),
            logger: (category) => {
              logger = require("./conf/logger")(merge(require("./conf/commons"),conf),category);
              return logger;
            },
            security_checks: () =>  require("./security/security-checks")(conf,logger,eh),
            security: (permissionMap, app) => require("./security/security")(conf,logger,permissionMap,app),
            blacklist: (app) => require("./security/blacklist")(conf,logger,app),
            cryptoAES_cbc: () => require("./security/cryptoAES_cbc"),
            event_handler: () => eh,
            audit_handler: () => ah,
            query_builder: () => require("./utilities/query-builder"),
            utility: () => require("./utilities/utility")(logger),
            consumer: (message_section,checkFunction,checkTo,sendFunction,skipPreferences) => require("./utilities/consumer")(conf,logger,eh,message_section,checkFunction,checkTo,sendFunction,skipPreferences),
            response_handler : (app) => require("./utilities/response-handler")(eh,ah,logger,app),
            basic_auth : (app) => require("./security/basic-auth")(conf,logger,app),
            elastic_search_db: () => require("./conf/elasticsearch-db")(conf, logger),
            cache: (conf) =>  require("./cache/cache")(conf),
            healthcheck: (app) => require("./utilities/healthcheck")(conf,logger,app),
            promised_multiple_db: () => require("./conf/multiple-db-prom")(conf, logger)
        }
    }
}
