/* security checks in the user token JWT for manage the profiling */
/**
 *
 * @param logger
 * @param eh
 */
module.exports = function (conf, logger, eh) {

    function checkHeader(req, res, next) {
        if (req.auth.applications[conf.app_name].includes("admin") || req.get("Shib-Iride-IdentitaDigitale") === req.params.user_id) return next();
        var err = { name: "SecurityError", message: "Security context not valid" };
        return next({ type: "security_error", status: 401, message: err });
    }

    function checkServiceAuthType(req, res, next) {
        if (req.auth.applications[conf.app_name].includes("admin") ||
            (req.auth.agent_auth && req.get("Shib-Iride-IdentitaDigitale") ) ||
            req.get("Shib-Iride-IdentitaDigitale") === req.params.user_id) return next();
        var err = { name: "SecurityError", message: "Security context not valid" };
        return next({ type: "security_error", status: 401, message: err });
    }

    function checkBasicAuth(req, res, next) {
        // -----------------------------------------------------------------------
        // basic authentication middleware

        const auth = { login: conf.security.basicauth.username, password: conf.security.basicauth.password }

        // parse login and password from headers
        const b64auth = (req.headers.authorization || '').split(' ')[1] || ''
        const [login, password] = Buffer.from(b64auth, 'base64').toString().split(':')

        // Verify login and password are set and correct
        if (login && password && login === auth.login && password === auth.password) {
            // Access granted...
            return next()
        }

        // Access denied...
        // res.set('WWW-Authenticate', 'Basic realm="Notify"') // change this
        next({ type: "security_error", status: 401, message: {
            "name": "SecurityError",
            "message": "Basic authentication required"
        } });

        // -----------------------------------------------------------------------
    }

    return {
        checkHeader: (req, res, next) => checkHeader(req, res, next),
        checkServiceAuthType: (req, res, next) => checkServiceAuthType(req, res, next),
        checkBasicAuth: (req, res, next) => checkBasicAuth(req, res, next)
    }
}