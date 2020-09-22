const uuid = require('uuid/v4');

/**
 * generale utility functions
 * @param logger
 */
module.exports = function (logger) {

    var crypto = require('crypto');

    var module = {};

    /**
     * function used by APIs to handle the error status
     * @param res response
     * @param status status code
     * @param message message of error ( string or object)
     */
    module.errorHandler = function (res, status, message) {
        logger.error("errorHandler, status: " + status + "\nmessage: " + message);
        res.status(status);
        if ("string" === typeof message) res.end(message);
        else res.end(JSON.stringify(message));
    };

    module.uuid = uuid;

    /**
     * format date in yyyy-MM-dd hh:mm:ss
     * @param date The Date Object
     * @returns {string} return strings with formatted date
     */
    module.getDateFormatted = function (date) {

        return date.getFullYear() + '-' +
            (date.getMonth() + 1) + '-' +
            date.getDate() + ' ' +
            date.getHours() + ':' +
            date.getMinutes() + ':' +
            (date.getSeconds())
    };

    /**
     * Remove the empty properties from a json
     * @param obj json object
     * @returns {any}
     */
    module.remove_empty = function (obj) {
        const o = JSON.parse(JSON.stringify(obj)); // Clone source oect.

        Object.keys(o).forEach(key => {
            if (o[key] && typeof o[key] === 'object')
                o[key] = module.remove_empty(o[key]);  // Recurse.
            else if (o[key] === undefined || o[key] === null)
                delete o[key]; // Delete undefined and null.
            else
                o[key] = o[key];  // Copy value.
        });

        return o; // Return new object.
    };

    module.compareJSON = function (obj1, obj2) {
        var ret = {};

        for (const key in obj1) {
            if (isObject(obj1[key])) {
                if (!obj2[key]) ;
                else {
                    ret[key] = {};
                    Object.assign(ret[key], module.compareJSON(obj1[key], obj2[key]));
                }
            } else {
                if (!obj1.hasOwnProperty(key) || obj2[key] !== obj1[key]) {
                    ret[key] = obj2[key];
                }
            }
        }
        return ret;
    };

    module.checkNested = function (obj, property) {
        var array = "[\"" + property.replace(/\./g,"\",\"") + "\"]";
        var args = JSON.parse(array);


        for (var i = 0; i < args.length; i++) {
            if (!obj || !obj.hasOwnProperty(args[i])) {
                return false;
            }
            obj = obj[args[i]];
        }
        return true;
    }

    module.hashMD5 = function (string) {
        return crypto.createHash('md5').update(string).digest('hex');

    }
    module.isCircular = function(object) {
       const seenObjects = new WeakMap(); // use to keep track of which objects have been seen.

       function detectCycle(obj) {
          // If 'obj' is an actual object (i.e., has the form of '{}'), check
          // if it's been seen already.
          if (Object.prototype.toString.call(obj) == '[object Object]') {

             if (seenObjects.has(obj)) {
                return true;
             }

             // If 'obj' hasn't been seen, add it to 'seenObjects'.
             // Since 'obj' is used as a key, the value of 'seenObjects[obj]'
             // is irrelevent and can be set as literally anything you want. I
             // just went with 'undefined'.
             seenObjects.set(obj, undefined);

             // Recurse through the object, looking for more circular references.
             for (var key in obj) {
                if (detectCycle(obj[key])) {
                   return true;
                }
             }

          // If 'obj' is an array, check if any of it's elements are
          // an object that has been seen already.
          } else if (Array.isArray(obj)) {
             for (var i in obj) {
                if (detectCycle(obj[i])) {
                   return true;
                }
             }
          }

          return false;
       }

       return detectCycle(object);
    }

    function isObject(item) {
        return (item && typeof item === 'object' && !Array.isArray(item));
    }
    /*module.checkHeader = function checkHeader(req, res, next) {
        if(req.user.permissions.includes("user") && req.get("Shib-Iride-IdentitaDigitale") === req.params.user_id) return next();
        var err = {name: "SecurityError", message: "Security context not valid"};
        return next({type: "security_error", status: 401, message: err});
    }*/

    return module;
}
