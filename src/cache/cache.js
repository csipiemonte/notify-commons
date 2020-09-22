const Redis = require('ioredis');


var cache = {
    prefix: "",
    async get(key) {
        let result = await redis.cache_get(this.prefix, key);
        if (!result) return null;
        return JSON.parse(result);
    },
    async put(key, value) {
        await redis.cache_put(this.prefix, key, JSON.stringify(value));
    },
    async invalidate(key) {
        await redis.cache_invalidate(this.prefix, key);
    }
};

function api(prefix){
  cache.prefix = prefix;
  return cache;
}

function middleware(prefix, pattern) {
  cache.prefix = prefix;
  if(!pattern) pattern = (x) => x + "*";
  return async function(req, res, next) {
    if (req.method != "GET") {
      await cache.invalidate(pattern(req.path));
      return next();
    }
    let result = await cache.get(req.url);
    if (result) {
      res.set('cached','true');
      res.set(result.headers);
      //res.json(result.body);
      return next({
          type: "ok", status: 200, message: result.body
      });
    }
    res.jsonResponse = res.json;
    res.json = async function(body) {
      if (res.statusCode == 200) {
        await cache.put(req.url, { body: body, headers: res.getHeaders() });
      }
      res.jsonResponse(body);
    }
    next();
  }
}
var redis = null;
module.exports = function(conf){
    redis = new Redis(conf.redis);
    const fs = require('fs');

    redis.defineCommand("cache_get", {
        numberOfKeys: 0,
        lua: fs.readFileSync(process.cwd() + "/../../commons/src/cache/lua/cache_get.lua", "utf-8")
    });
    redis.defineCommand("cache_put", {
        numberOfKeys: 0,
        lua: fs.readFileSync(process.cwd() + "/../../commons/src/cache/lua/cache_put.lua", "utf-8")
    });
    redis.defineCommand("cache_invalidate", {
        numberOfKeys: 0,
        lua: fs.readFileSync(process.cwd() + "/../../commons/src/cache/lua/cache_invalidate.lua", "utf-8")
    });
    return {
        api:api,
        middleware:middleware
    }
}
