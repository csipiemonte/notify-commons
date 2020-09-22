redis.replicate_commands();
local prefix = ARGV[1];
local key = prefix .. ":cache:" .. ARGV[2];
redis.call("zadd", prefix .. ":LRU", redis.call("time")[1], key)
return redis.call("get", key);
