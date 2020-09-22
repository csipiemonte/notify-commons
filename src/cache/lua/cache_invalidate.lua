redis.replicate_commands();
local prefix = ARGV[1];
local key_pattern = prefix .. ":cache:" .. ARGV[2];
for _,k in ipairs(redis.call("keys",  key_pattern))
do
  redis.call("del", k);
  redis.call("zrem", prefix .. ":LRU", k);
end
return key_pattern
