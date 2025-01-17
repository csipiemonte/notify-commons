var util = require('util');
const logger = require('../conf/logger');
var request = util.promisify(require("request"));

/**
 * Class to create events used by applications and send the event to the queue.
 * You can add events typologies in return statement or use the "new_event" method from the applications.
 * @param conf configuration JSON
 * @param logger logger
 */
module.exports = function (conf, logger) {
    module.mb_url = conf.mb.queues.events;
    module.token = conf.mb.token;
    module.source = conf.app_name;
    module.retriesNum = conf.eventHandler.retries.num;
    module.retriesDelay = conf.eventHandler.retries.delay;
    module.logger = logger;
    module.utility = require("./utility")(logger);
    return {
        ok: async (description, payload) => new_event(description, "OK", payload) ,
        client_request: async (description, payload) => new_event(description, "CLIENT_REQUEST", payload),
        client_error: async (description, payload) => new_event(description, "CLIENT_ERROR", payload),
        db_error: (description, payload) => new_event(description, "DB_ERROR", payload),
        system_error: async (description, payload) => new_event(description, "SYSTEM_ERROR", payload),
        external_error: (description, payload) => new_event(description, "EXTERNAL_ERROR", payload),
        retry: async (description, payload) => new_event(description, "RETRY", payload),
        security_error: (description, payload) => new_event(description, "SECURITY_ERROR", payload),
        info: async (description, payload) => new_event(description, "INFO", payload),
        new_event: new_event
    };
}

async function new_event(description, type, payload) {
    var event = {
        uuid: module.utility.uuid(),
        payload: {
            source: module.source,
            description: typeof description === 'object' ? "[" + description.method + "] " + description.path : description,
            payload: payload,
            type: type || "OK",
            created_at: new Date().getTime()
        }
    }

    var optionsToMb = {
        url: module.mb_url,
        method: 'POST',
        json: event,
        headers: {
            'x-authentication': module.token,
            'connection':'close'
        }
    }

    sendEvent(optionsToMb, module.retriesNum);
}

function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
}

/**
 * 
 * @param options http call options
 * @param retries number of retries before aborts sending an event
 * @returns 
 */
async function sendEvent(options, retries) {
    var to_continue = true;
    while (to_continue) {
        try {
            let data = await request(options);

            if(data.statusCode === 201) {
                module.logger.debug("event successfully sent:", data.body);
                to_continue = false;
            } else if (data.statusCode == 408 || data.statusCode >= 500) {
                module.logger.warn("error sending event: status code [%s] event url [%s] error [%s], retry in %s ms", data.statusCode, options.url, data.body, module.retriesDelay);
                await sleep(module.retriesDelay);
            } else {
                to_continue = false;
                module.logger.error("cannot send event: status code [%s] event url [%s] event [uuid:%s, source: %s, description: %s] error [%s]", data.statusCode, options.url, options.json.uuid, options.json.payload.source, options.json.payload.description, data.body);
            }
        } catch(err) {
            module.logger.warn("error sending event: status code [exception] event url [%s] event [uuid:%s, source: %s, description: %s] error [%s], retry in %s ms", options.url, options.json.uuid, options.json.payload.source, options.json.payload.description, err.message, module.retriesDelay);
            await sleep(module.retriesDelay);
        }
    }
}
