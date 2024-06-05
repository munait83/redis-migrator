from rediscluster import RedisCluster

# Function to fetch all keys from the source Redis cluster
def fetch_all_keys(source_redis):
    keys = set()
    for key in source_redis.scan_iter('*'):
        keys.add(key)
    return list(keys)

# Function to copy data from source to destination
def copy_data(source_redis, dest_redis, keys):
    for key in keys:
        try:
            value = source_redis.dump(key)
            ttl = source_redis.ttl(key)
            if ttl > 0:
                dest_redis.restore(key, ttl * 1000, value, replace=True)
            else:
                dest_redis.restore(key, 0, value, replace=True)
        except redis.exceptions.ResponseError as e:
            print(f"Failed to copy key {key}: {e}")

# Configuration for the source Redis cluster
source_redis_cluster = {
    'startup_nodes': [
        {'host': 'redis-cluster-1697815485.redis', 'port': '6379'}
    ],
    'decode_responses': False,  # Set to False to handle binary data
}

# Configuration for the destination Redis cluster (AWS ElastiCache)
dest_redis_cluster = {
    'startup_nodes': [
        {'host': 'clustercfg.depot360-redis-cluster.ui6pmw.use1.cache.amazonaws.com', 'port': '6379'}
    ],
    'decode_responses': False,  # Set to False to handle binary data
}

if __name__ == "__main__":
    # Connect to the source Redis cluster
    source_redis = RedisCluster(**source_redis_cluster)
    
    # Connect to the destination Redis cluster
    dest_redis = RedisCluster(**dest_redis_cluster)

    # Fetch all keys from the source Redis cluster
    keys = fetch_all_keys(source_redis)
    print(f"Fetched {len(keys)} keys from the source Redis cluster.")

    # Copy data from source to destination
    copy_data(source_redis, dest_redis, keys)
    print(f"Copied {len(keys)} keys to the destination Redis cluster.")
