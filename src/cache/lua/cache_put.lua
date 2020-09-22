redis.replicate_commands();
local prefix = ARGV[1];
local key = prefix .. ":cache:" .. ARGV[2];
local value = ARGV[3];
local size = redis.call("get", prefix .. ":size");
if not size
then
  size = "10000";
  redis.call("set", prefix .. ":size", size);
end

size = tonumber(size);

while(redis.call("zcard", prefix .. ":LRU") > size)
do
  local k = redis.call("zrange", prefix .. ":LRU", 0, 0)[1];
  redis.call("ZREM", prefix .. ":LRU", k);
  redis.call("del",  k)
end
redis.call("set", key, value);
