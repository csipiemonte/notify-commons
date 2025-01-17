var crypto = require('crypto');

var AESCrypt = {};

AESCrypt.encrypt = function(cleardata, password) {
    var cryptedKey = crypto.createHash('md5').update(password).digest("hex");
    var iv = cryptedKey.slice(0,16);
    var encipher = crypto.createCipheriv('aes-256-cbc', cryptedKey, iv),
        encryptdata = encipher.update(cleardata, 'utf8', 'binary');
    encryptdata += encipher.final('binary');
    var encode_encryptdata = Buffer.from(encryptdata, 'binary').toString('base64');
    return encode_encryptdata;
}

AESCrypt.decrypt = function(encryptdata, password) {
    var cryptedKey = crypto.createHash('md5').update(password).digest("hex")
    var iv = cryptedKey.slice(0,16);
    encryptdata = Buffer.from(encryptdata, 'base64').toString('binary');
    var decipher = crypto.createDecipheriv('aes-256-cbc', cryptedKey, iv),
        decoded = decipher.update(encryptdata, 'binary', 'utf8');
    decoded += decipher.final('utf8');
    return decoded;
}

module.exports = AESCrypt;

/*let tokenBase64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1dWlkIjoiNGRlZjJkYzUtOGM1Ny00NDE5LTg1MGYtOTg4ZjhkN2QzNWQwIiwiY2xpZW50X25hbWUiOiJhcHByZWZlcnRpIiwiY2xpZW50X3V1aWQiOiIxYzIzZmUxYy0xMDhkLTQxOTQtOWM3OS1hNTU1NjVjMDYxMTYiLCJwcmVmZXJlbmNlX3NlcnZpY2VfbmFtZSI6InByb2ZfY2l0dCIsImV4cCI6MjUzNDAyMjEwODAwMDAwLCJpYXQiOjE1MTk4MjI3MDQ0MzgsImFwcGxpY2F0aW9ucyI6WyJub3RpZnkiLCJtZXgiLCJwcmVmZXJlbmNlcyJdLCJwZXJtaXNzaW9ucyI6WyJ1c2VyIiwiYWRtaW4iLCJiYWNrZW5kIl0sInByZWZlcmVuY2VzIjp7InB1c2giOiJBQUFBV214Y2Z6WTpBUEE5MWJISVVaNzhSQlRIYUotaUtOT3pXUXcteTg4SVQ1SXp4UDAxR0V3emtFWlZVejAzTWFrLTJDSHZKV1FkQ2F0RHlqTVRhZEhzUEtVYTBld3pKMHNmMVRBVEZBSFd4d0taNHNhYVhfMFg4czVHVUhNMG5tUG0tMVdLci1XQl9ONWNiMkhWa1RJaiIsImVtYWlsIjoiYXBwcmVmZXJ0b0Bhc2QuY29tIiwibWV4IjoiYXNkYWQiLCJzbXMiOnsidXNlcm5hbWUiOiJub3RpZnkiLCJwYXNzd29yZCI6Im1lZ2Fsb21hbl8xOCIsInByb2plY3RfY29kZSI6IjM3In19fQ.EhoX264aKHRhx-oSXcDfmK1TR56H3GJaFreVw-wykMI";

    buf        = "Here is some data for the encrypt"; // 32 chars

var enc        = AESCrypt.encrypt(tokenBase64,"dev");
var dec        = AESCrypt.decrypt( enc,"dev");

console.log("encrypt length: ", enc.length);
console.log("encrypt in Base64:", enc);
console.log("decrypt all: " + dec);*/