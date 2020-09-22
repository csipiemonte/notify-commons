/* logger configuration */
module.exports = function (conf,category = 'default') {
    var log4js = require('log4js');
    log4js.configure(conf.log4js);

    const logger = log4js.getLogger(category);
    return logger;
};
