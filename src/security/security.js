/**
 *  checks if the token JWT contains authorization for the application specified in conf.app_name, if it's expired and put the decoded token in request ovject
 */
module.exports = function (conf, logger, permissionMap, app) {

    var crypto = require("./cryptoAES_cbc");
    var utility = require("../utilities/utility")();
    var jwt = require('express-jwt');

    app.use(jwt({
        secret: conf.security.secret,
        getToken: function fromHeaderOrQuerystring(req) {
            try {
                return utility.checkNested(conf,"security.crypto.password") ? crypto.decrypt(req.headers['x-authentication'], conf.security.crypto.password):req.headers['x-authentication'];
            } catch (e) {
                logger.info("error decrypting JWT token:", e);
                e.name = "Decrypting Error";
                e.status = 401;
                e.type = "security_error";
                e.message = "Wrong Token";
                throw e;
            }
        }
    }));

    /**
     * check for application authorization and expire date
     */
    app.use(function (req, res, next) {
            logger.debug("check applications");
            if (!req.user.applications || !(Object.keys(req.user.applications)).includes(conf.app_name)) {
                var err = {name: "SecurityError", message: "User doesn't have the application '" + conf.app_name + "'"};
                return next({type: "security_error", status: 401, message: err});
            }
            next();
        }
    );


    if (permissionMap) permissionMap.forEach(permission => {
        logger.debug("setting permissions " + permission.permissions + " for [" + permission.method + "] " + permission.url);
        app[permission.method](permission.url, function (req, res, next) {
            logger.debug("check '" + permission.permissions + "'  permission at path: [" + permission.method + "] " + permission.url);
            if (permission.permissions.filter(p => (req.user.applications[conf.app_name]).includes(p)).length === 0) {
                var err = {
                    name: "SecurityError",
                    message: "User doesn't have the permission '" + permission.permissions + "': " + "[" + req.method + "] " + req.path
                };
                return next({type: "security_error", status: 401, message: err});
            }
            next();
        })
    });

}
