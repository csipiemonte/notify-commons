/**
 *  blacklist check jwt
 */
module.exports = function (conf, logger, app) {

    const req_promise = require('request-promise');

    var blacklist = {};

    async function updateBlacklist(){
        logger.debug("updating blacklist");
        let options = {
            url: conf.security.blacklist.url,
            method: "GET",
            headers: {
                'x-authentication': conf.mb.token
            },
            json: true
        };
        try{
            blacklist = await req_promise(options);
            logger.debug("blacklist: ",blacklist);
        }catch(e){
            logger.error("error in updating blacklist: ",e);
        }
    }

    updateBlacklist();

    setInterval(updateBlacklist,60 * 1000);

    app.use(async function (req,res,next){
        logger.debug("check blacklist");
        let tok_array = Object.values(blacklist);
        let err = {
          error: "The token has been blacklisted"
        }
        if(tok_array.includes(req.headers['x-authentication']))
            return next({type: "security_error", status: 403,
                message: err});
        next();
    });

}
