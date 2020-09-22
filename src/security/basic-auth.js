module.exports = function (conf, logger, app) {

    var basicAuth = require('basic-auth');

    var auth = function (req, res, next) {
        var user = basicAuth(req);
        if (user === undefined || user['name'] !== conf.security.basic_auth.username || user['pass'] !== conf.security.basic_auth.password) {
            /*res.statusCode = 401;
            res.setHeader('WWW-Authenticate', 'Basic realm="MyRealmName"');
            res.end('Unauthorized');*/
            var err = {
                name: "SecurityError",
                message: "Unauthorized"
            };
            res.setHeader('WWW-Authenticate', 'Basic realm="MyRealmName"');
            return next({type: "security_error", status: 401, message: err});
        } else {
            next();
        }
    };

    app.use(auth);

}
