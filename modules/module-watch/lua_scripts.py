from config.redis_client import get_redis_client

# We will store registered Script objects here
_scripts = {}

# KEYS[1] = proxy_states hash (stores state of each proxy)
# KEYS[2] = global down_count key
# ARGV[1] = proxy_id
# ARGV[2] = new state ('up' or 'down')
# ARGV[3] = total pool size
# ARGV[4] = threshold (e.g. 0.20)
INCREMENT_AND_CHECK_SCRIPT = """
local proxy_states_key = KEYS[1]
local down_count_key = KEYS[2]
local proxy_id = ARGV[1]
local new_state = ARGV[2]
local total_pool = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])

-- Get the previous state of this proxy
local old_state = redis.call('HGET', proxy_states_key, proxy_id)

-- If the state hasn't changed, we don't need to recalculate if we just crossed the threshold
if old_state == new_state then
    return 0
end

-- Update the state
redis.call('HSET', proxy_states_key, proxy_id, new_state)

-- Atomically update the down count
local current_down = tonumber(redis.call('GET', down_count_key) or 0)

if new_state == 'down' then
    current_down = redis.call('INCR', down_count_key)
elseif old_state == 'down' and new_state == 'up' then
    current_down = redis.call('DECR', down_count_key)
end

-- Calculate failure rate
if total_pool > 0 then
    local failure_rate = current_down / total_pool
    
    -- Check if we just crossed the threshold upwards
    if new_state == 'down' and failure_rate >= threshold then
        local previous_down = current_down - 1
        local previous_rate = previous_down / total_pool
        if previous_rate < threshold then
            return 1 -- Threshold just crossed (FIRED)
        end
    end
    
    -- Check if we just crossed the threshold downwards
    if new_state == 'up' and failure_rate < threshold then
        local previous_down = current_down + 1
        local previous_rate = previous_down / total_pool
        if previous_rate >= threshold then
            return 2 -- Threshold just dropped (RESOLVED)
        end
    end
end

return 0
"""

def register_scripts():
    """
    Registers all Lua scripts with the Redis server.
    This parses the scripts once and caches the SHA1 hash on the client,
    improving execution speed and saving bandwidth.
    """
    client = get_redis_client()
    
    _scripts['increment_and_check'] = client.register_script(INCREMENT_AND_CHECK_SCRIPT)
    print("Redis Lua scripts registered.")

def get_script(name: str):
    """
    Retrieves a registered Lua script by name.
    
    :param name: The identifier of the script.
    :return: The redis.client.Script object.
    """
    if name not in _scripts:
        raise ValueError(f"Lua script '{name}' not found. Make sure register_scripts() was called.")
    return _scripts[name]
