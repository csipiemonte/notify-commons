/**
 * class that implement the main consumer logic from the implementation of checkFuntion,checkTo, and sendFunction functions
 * @param conf configuration file (JSON)
 * @param logger
 * @param eh event handler instance
 * @param message_section section of message to analyze ( ex. mex,sms,push,events,audit or email)
 * @param checkFunction The logic of validation for the message section
 * @param checkTo The logic to find the recipient
 * @param sendFunction The logic for send the message
 * @param skipPreference flag for skip request to preferences. true: will skip preferences call, false or undefined will not.
 * @returns {execute} Return the completed function that can be executed
 */
module.exports = function (conf, logger, eh, message_section, checkFunction, checkTo, sendFunction, skipPreference) {
    const util = require("util");
    const request = util.promisify(require('request'));
    const utility = require("./utility")(logger);
    const circular_json = require('circular-json');
    const messages_channels = ['sms','push','email','mex','io'];


    /**
     * Options for contacting message Broker (mb)
     */
    var optionsToMb = {
        url: conf.mb.queues.messages,
        headers: {
            'x-authentication': conf.mb.token,
            'connection': 'close'
        }
    };


    async function getBodies() {
        let from_mb = null;
        try {
            //logger.debug("reading from mb");
            from_mb = await request(optionsToMb);
            if (from_mb.statusCode === 401) {
                logger.error("not authorized to contact the message broker", from_mb.body);
                process.exit(1);
            }
            if (from_mb.statusCode === 204) {
                logger.debug("no data from message broker");
                return [];
            }
            if (from_mb.statusCode !== 200) {
                //if (eh) eh.system_error(optionsToMb.url, "error from message broker: [" + from_mb.statusCode + "]  " + from_mb.body);
                logger.error("error from message broker: [" + from_mb.statusCode + "] " + from_mb.body);
                await sleep(10000);
                return [];
            }
        } catch (err) {
            //let error = {};            
            //error.error = err;
            //if (eh) eh.system_error("Error", circular_json.stringify(error));
            logger.error("ERROR: ", circular_json.stringify(err));
            await sleep(10000);
            return [];
        }

        let bodies = JSON.parse(from_mb.body);
        if (!Array.isArray(bodies)) bodies = [bodies];
        return bodies;
    }

    /**
     * Main function
     */
    var to_continue = true;
    async function execute() {
        while (to_continue) {
            let bodies = await getBodies();

            for(let body of bodies)
            {
                try {

                    if (body.payload.dry_run) {
                        logger.debug("the message has dry_run set");
                        continue;
                    }

                    if (messages_channels.includes(message_section) && !body.payload[message_section]) continue;

                    if (new Date(body.expire_at).getTime() < new Date().getTime() ) {
                        logger.debug("the message " + body.payload.id + " is expired in date: " + new Date(body.expire_at).toLocaleString() + ", it will not be send");
                        if(eh) eh.info("the message " + body.payload.id + " is expired, it will not be send",JSON.stringify({
                            message:body.payload,
                            user: body.user
                        }));
                        continue;
                    }
                    var check_result = checkFunction(body.payload);
                    var errors = check_result.filter(e => e !== "");
                    if(messages_channels.includes(message_section) && (!body.payload.id || !body.payload.id.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i))) errors.push("id must be a valid uuid");
                    if (errors.length > 0) {
                        errors.forEach(e => {
                            logger.info(e)
                        });
                        if (eh) eh.client_error("the message is malformed : " + errors.join(","), JSON.stringify({
                            message: body.payload,
                            user: body.user,
                            error: "the message is malformed : " + errors.join(",")
                        }));
                        logger.info("the message is malformed:", body.payload);
                        continue;
                    }
                    var preferences = null;

                    if (!skipPreference) {
                        preferences = await getPreferences(body);
                        if (preferences == null) continue;
                    }

                    await sendFunction(body, preferences);
                } catch (err) {
                    let e = {};
                    if (body) {
                        e.user = body.user;
                        e.message = body.payload;
                    }                     
                     e.error = err;
                     e.description = err.description || "Error";                    
                    if(eh) eh[analizeError(err,body)](e.description, circular_json.stringify(e));                                        
                    logger[err.level ||"error"](JSON.stringify(e.error, null, 4));             
                    if (isErrorToRetry(err,body)) await postMessageToMB(body);
                    //await sleep(10000);
                }

            }

        }
        logger.info("stopped gracefully");
        process.exit(0);
    }

    function analizeError(err,body){

        let stringedErr = typeof err === "object" ? JSON.stringify(err) : err;

        if(err.type_error) return err.type_error;

        // if is a error from smtp
        if(typeof err === "object" && err.code && err.code === "EENVELOPE"){
            if(err.responseCode == 450) return "client_error";
        }

        if(typeof err === "object" && err.client_source === "emailconsumer"){
            return "client_error";
        }

        if(typeof err === "object" && err.client_source === "ioconsumer" && err.type === "client_error" ){
            return "client_error";
        }

        return "system_error";
    }


    function isErrorToRetry(err,body) {
        if(body.to_be_retried === false) return false;
        //let stringedErr = typeof err === "object" ? JSON.stringify(err) : err;
        //if (JSON.stringify(stringedErr).includes("ER_DUP_ENTRY")) return false;

        // db error: duplicate key value violates unique constraint
        if(typeof err === "object" && err.code === "23505"){
            return false;
        }
        // if is a error from smtp
        if(typeof err === "object" && err.code && err.code === "EENVELOPE"){
            if(err.responseCode >= 300) return false;
        }

        if(typeof err === "object" && err.client_source === "emailconsumer"){
            return false;
        }

        if(typeof err === "object" && err.client_source === "ioconsumer" && err.type === "client_error" ){
            return false;
        }

        return true;
    }

    /**
     * contact preferences system to obtain the user and service preferences
     * @param body message
     */
    async function getPreferences(body) {
        /**
         * if the service did not give availability for this channel, it will not send the message
         */
        if (!Object.keys(body.user.preferences).includes(message_section)) {
            logger.debug("The service " + body.user.preference_service_name + " doesn't have " + message_section + " channel available, msg uuid: %s payload id: %s user: %s", body.uuid, body.payload.id, JSON.stringify(body.user.preferences));
            return null;
        }

        if (body.payload.trusted) {
            let preferences = {body: {}};
            if (utility.checkNested(body, "payload.sms.phone")) preferences.body.sms = body.payload.sms.phone;
            if (utility.checkNested(body, "payload.push.token")) preferences.body.push = body.payload.push.token;
            if (utility.checkNested(body, "payload.email.to")) preferences.body.email = body.payload.email.to;

            if (!preferences.body[message_section]) {
                if (eh) eh.client_error("the trusted service " + body.user.preference_service_name + " didn't fill the recipient section", JSON.stringify({
                    message: body.payload,
                    user: body.user,
                    error: "the trusted service " + body.user.preference_service_name + " didn't fill the recipient section"
                }));
                logger.debug("the trusted service " + body.user.preference_service_name + " didn't fill the recipient section");
                return null;
            }

            return preferences;
        }

        var optionsUserPreferences = {
            url: conf.preferences.url + "/" + body.payload.user_id + "/contacts/" + body.user.preference_service_name,
            headers: {
                'x-authentication': conf.preferences.token,
                'msg_uuid': body.payload.id
            },
            json: true
        };

        var preferences = await request(optionsUserPreferences);

        /**
         * if user doesn't exist, I'll should be able to send message if the recipient section is filled.
         */
        if (preferences.statusCode === 404) {
            if (!checkTo(body.payload)) {
                if (eh) eh.client_error("the user " + body.payload.user_id + " doesn't exist and the recipient section is not set in the message", JSON.stringify({
                    message: body.payload,
                    user: body.user,
                    error: "the user " + body.payload.user_id + " doesn't exist and the recipient section is not set in the message"
                }));
                logger.info("the user " + body.payload.user_id + " doesn't exist and the recipient section is not set in the message");
                return null;
            }

            preferences = {body: {}};
            if (utility.checkNested(body, "payload.sms.phone")) preferences.body.sms = body.payload.sms.phone;
            if (utility.checkNested(body, "payload.push.token")) preferences.body.push = body.payload.push.token;
            if (utility.checkNested(body, "payload.email.to")) preferences.body.email = body.payload.email.to;

            return preferences;
        }

        /**
         * If user exists but it doesn't have preferences for the service, I won't send messages.
         */
        if (preferences.statusCode === 204) {
            if (eh) eh.client_error("the user " + body.payload.user_id + " has not preferences for the service: "
                + body.user.preference_service_name, JSON.stringify({
                message: body.payload,
                user: body.user,
                error: "the user " + body.payload.user_id + " has not preferences for the service: " + body.user.preference_service_name
            }));
            logger.info("the user " + body.payload.user_id + " has not preferences for the service: " + body.user.preference_service_name);
            return null;
        }
        /**
         * if user exists but he doesn't have setted the contact for this channel, the message will not be sent
         */
        if (preferences.statusCode === 200 && !preferences.body[message_section]) {
            logger.info("the user " + body.payload.user_id + " doesn't want receive " + message_section + " from " + body.user.preference_service_name);
            if (eh) eh.client_error("the user " + body.payload.user_id + " doesn't want receive " + message_section + " from " + body.user.preference_service_name, JSON.stringify({
                message: body.payload,
                user: body.user,
                error: "the user " + body.payload.user_id + " doesn't want receive " + message_section + " from " + body.user.preference_service_name
            }));
            return null;
        }
        /**
         * The message can be sent
         */
        if (preferences.statusCode !== 200 && preferences.statusCode !== 404) {
            if (eh) eh.system_error("error from preferences: [" + preferences.statusCode + "] ", JSON.stringify({
                error: preferences.body,
                message: body.payload,
                user: body.user
            }));
            logger.error("error from preferences: [" + preferences.statusCode + "]: ", preferences.body);
            await postMessageToMB(body);
            await sleep(10000);
            return null;
        }

        return preferences;
    }

    function sleep(ms) {
        return new Promise(resolve => {
            setTimeout(resolve, ms)
        })
    }

    /**
     * in case of failure, insert in queue "to_be_retried" of the message section queue
     * @param data total message
     */
    async function postMessageToMB(data) {

        logger.debug("send message to: to_be_retried");
        //let queue_name = data.priority === "high" ? conf.mb.queues.messages + "_priority:to_be_retried" : conf.mb.queues.messages + ":to_be_retried";
        var optionsToMbPost = {
            url: conf.mb.queues.messages + ":to_be_retried",
            headers: {
                'x-authentication': conf.mb.token
            },
            method: "POST",
            json: data
        };

        var ok = false;
        do {
            try {
                var response = await request(optionsToMbPost);
                if (response.statusCode === 201) ok = true;
                else {
                    logger.error("data not inserted: ", JSON.stringify(data));
                    if (eh) eh.system_error("error while putting the message in the message broker [" + response.statusCode + "] ", response.body);
                    logger.error("error while putting the message in the message broker [" + response.statusCode + "] ", response.body);
                    await sleep(10000);
                }
            } catch (err) {
                logger.error("data not inserted: ", JSON.stringify(data));
                if (eh) eh.system_error("error while putting the message in the message broker", JSON.stringify(err));
                logger.error("error while putting the message in the message broker", err.message);
                await sleep(10000);
            }
        } while (!ok);
    }


    var kill_handler;
    function shutdown(signal){
        console.log("gracefully stopping: " + signal + " received");
        logger.info("gracefully stopping: " + signal + " received");

        kill_handler = setTimeout(() => {
            logger.debug("kill handler");
            process.exit(1);
        },10 * 1000);
        to_continue = false;
    }

    process.on("SIGINT",shutdown);
    process.on("SIGTERM",shutdown);

    return execute;
}
