/* security checks in the user token JWT for manage the profiling */
/**
 *
 * @param logger
 * @param eh
 */
module.exports = function(conf,logger,eh) {

    function checkHeader(req, res, next) {
        if(req.user.applications[conf.app_name].includes("admin") || req.get("Shib-Iride-IdentitaDigitale") === req.params.user_id) return next();
        var err = {name: "SecurityError", message: "Security context not valid"};
        return next({type: "security_error", status: 401, message: err});
    }

    return {
        checkHeader : (req,res,next) => checkHeader(req, res, next)
    }


}