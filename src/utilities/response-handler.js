 function remove_empty(obj)  {
    const o = JSON.parse(JSON.stringify(obj)); // Clone source oect.

    Object.keys(o).forEach(key => {
        if (o[key] && typeof o[key] === 'object')
            o[key] = remove_empty(o[key]);  // Recurse.
        else if (o[key] === undefined || o[key] === null)
            delete o[key]; // Delete undefined and null.
        else
            o[key] = o[key];  // Copy value.
    });

    return o; // Return new object.
};

const uuid = require('uuid/v4');
module.exports = function(eh,ah,logger, app){
    var not_audit = ["emailconsumer","pushconsumer","smsconsumer"];
    app.use((err, req, res, next) => {
        if (err.code) err.type = "security_error";
        if(!err.type) err.type = "system_error";
        if(!err.status) err.status = 500;

        next(err);
    });

    app.use((err, req, res, next) => {
        res.status(err.status);
        res.unp_body = err.message ? remove_empty(err.message) : err.message;
        if (err.status >= 500 ) logger.info(err);
        if(eh) {
            let err_type = eh[err.type] ? err.type : "system_error";
            eh[err_type] (req, JSON.stringify(err.message));
        }

        let uuid_local = req.header("X-Request-ID") || uuid();
        req.header["X-Request-ID"] = uuid_local;
        res.set("X-Request-ID", uuid_local );
        res.set("X-Response-Time", Number(new Date().getTime()) - Number(res.get('X-Response-Time')) + "ms");

        if(ah && (!req.user || !not_audit.includes(req.user.preference_service_name))){
            ah.trace_request(req);
            ah.trace_response(req, res);
        }
        res.send(res.unp_body);
    });

}