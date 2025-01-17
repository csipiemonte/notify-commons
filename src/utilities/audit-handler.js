var utility;
const util = require("util");
const request_promise = util.promisify(require('request'));
var jwt = require('jsonwebtoken');
const requestIp = require('request-ip');
var os = require('os');
var crypto = require("../security/cryptoAES_cbc");

/**
 * Class to create events used by applications and send the event to the queue.
 * You can add events typologies in return statement or use the "new_event" method from the applications.
 * @param conf configuration JSON
 * @param logger logger
 */
module.exports = function (conf, logger) {
    module.mb_url = conf.mb.queues.audit;
    module.token = conf.mb.token;
    module.retriesNum = conf.auditHandler.retries.num;
    module.retriesDelay = conf.auditHandler.retries.delay;
    module.source = conf.app_name;
    module.logger = logger;
    module.conf = conf;
    utility = require("./utility")(logger);
    return {
        trace_request: trace,
        trace_response: trace
    };
}

function getNetInterface() {
    let net_interface = os.networkInterfaces().Ethernet || os.networkInterfaces().ens192;
    if (!net_interface) return "0.0.0.0"; // can't get the ethernet network interface info
    return net_interface.filter(e => e.family === "IPv4").pop(); // filter only IPv4 version
}

var fields = ['uuid', 'x_request_id', 'timestamp', 'resource', 'http_method', 'query_params', 'body', 'http_protocol', 'forwarded', 'from_header', 'host', 'origin', 'user_agent', 'x_forwarded_for', 'x_forwarded_host', 'x_forwarded_proto', 'headers', 'http_status', 'request_ip_address', 'server_name', 'server_ipaddress', 'server_port', 'tenant'];

function logResponseBody(req, res) {
    var oldWrite = res.write,
        oldEnd = res.end;

    var chunks = [];

    res.write = function (chunk) {
        chunks.push(chunk);

        oldWrite.apply(res, arguments);
    };

    res.end = function (chunk) {
        if (chunk)
            chunks.push(chunk);

        var body = Buffer.concat(chunks).toString('utf8');

        oldEnd.apply(res, arguments);
    };
}

function createAudit(req, res) {

    let audit = {};
    if (module.conf.security) {

        var token;
        try {
            token = utility.checkNested(module.conf, "security.crypto.password") ? crypto.decrypt(req.headers['x-authentication'], module.conf.security.crypto.password) : req.headers['x-authentication'];
        } catch (e) {
            module.logger.warn("error decrypting JWT token:", e.message);
        }

        token = jwt.decode(token);
        audit.client_name = token ? token.preference_service_name : "UNKNOWN";
    };

    fields.forEach(f => audit[f] = req.get(f.replace(/_/g, "-")));

    audit.tenant = token && token.tenant ? token.tenant : module.conf.defaulttenant;
    audit.x_request_id = req.header["X-Request-ID"];
    audit.uuid = utility.uuid();
    audit.timestamp = (new Date()).toISOString();
    audit.resource = req.path;
    audit.http_method = req.method;
    audit.query_params = req.query;
    audit.body = req.body;
    audit.http_protocol = req.protocol;
    audit.headers = req.headers;
    audit.server_ipaddress = getNetInterface().address;
    audit.server_name = os.hostname();
    audit.request_ip_address = requestIp.getClientIp(req);
    audit.server_port = module.conf.server_port;
    audit.app_name = module.conf.app_name;

    if(res) {
        audit.http_status = res.statusCode;
        let headers = Object.assign({}, res.getHeaders(), {msg_uuid: req.headers.msg_uuid });
        audit.headers = headers;
        audit.body = res.unp_body;
    }
    return audit;
}

async function trace(req, res) {

    let audit = createAudit(req, res);

    var optionsToMb = {
        url: module.mb_url,
        method: 'POST',
        json: {
            uuid: utility.uuid(),
            payload: audit
        },
        headers: {
            'x-authentication': module.token,
            'connection':'close'
        }
    }

    sendAudit(optionsToMb, module.retriesNum);
}


async function sendAudit(options, retries) {
    try {
        let data = await request_promise(options);

        if(data.statusCode === 201) {
            module.logger.debug("audit successfully sent:", data.body);
            return;
        }

        if (retries > 0 && (data.statusCode == 408 || data.statusCode >= 500)) {
            module.logger.warn("error sending audit: status code [%s] audit url [%s] retries left [%s] error [%s]", data.statusCode, options.url, retries - 1, data.body);
            setTimeout(sendAudit, module.retriesDelay, options, retries - 1);
        } else {
            module.logger.error("cannot send audit: status code [%s] audit url [%s] audit [x_request_id: %s, client_name: %s] error [%s]", data.statusCode, options.url, options.json.payload.x_request_id, options.json.payload.client_name, data.body);
        }
    } catch(err) {
        module.logger.error("cannot send audit: status code [exception] audit url [%s] audit [x_request_id: %s, client_name: %s] error [%s]", options.url, options.json.payload.x_request_id, options.json.payload.client_name, err.message);
    }
}