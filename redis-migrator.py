import redis
from redis.cluster import RedisCluster

def migrate_data(redis_cluster_a_nodes, redis_cluster_b_nodes, db=0):
    """
    Migrates data from Redis Cluster A to Redis Cluster B, handling various data types.

    Args:
        redis_cluster_a_nodes (list): List of startup nodes for Redis Cluster A.
        redis_cluster_b_nodes (list): List of startup nodes for Redis Cluster B.
        db (int, optional): Database number to migrate data from. Defaults to 0.
    """

    try:
        # Connect to Redis Cluster A
        redis_cluster_a = RedisCluster(startup_nodes=redis_cluster_a_nodes)

        # Connect to AWS ElastiCache for Redis
        redis_cluster_b = redis.RedisCluster(connection_pool=redis.ConnectionPool(
            host=...,  # Replace with your ElastiCache endpoint
            port=...,  # Replace with your ElastiCache port
            db=db,
            password=...,  # Replace with your ElastiCache password (if applicable)
        ))

        # Iterate through keys in Redis Cluster A using SCAN
        cursor = 0
        while True:
            cursor, keys = redis_cluster_a.scan(cursor=cursor, match='*', count=1000)
            if not keys:
                break

            # Migrate data in batches using a pipeline for efficiency
            with redis_cluster_b.pipeline() as pipe:
                for key in keys:
                    try:
                        # Get the data type and value from Redis Cluster A
                        data_type = redis_cluster_a.type(key)
                        value = redis_cluster_a.get(key)

                        # Handle different data types
                        if data_type == b'string':
                            pipe.set(key, value)
                        elif data_type == b'list':
                            pipe.lpush(key, *value)  # Unpack list elements for LPUSH
                        elif data_type == b'set':
                            pipe.sadd(key, *value)  # Unpack set elements for SADD
                        elif data_type == b'zset':
                            # Handle sorted sets with scores: (member, score) tuples
                            for member, score in redis_cluster_a.zrange(key, 0, -1, withscores=True):
                                pipe.zadd(key, score, member)
                        elif data_type == b'hash':
                            pipe.hmset(key, redis_cluster_a.hgetall(key))
                        else:
                            print(f"Warning: Unsupported data type '{data_type.decode()}' for key '{key.decode()}'")

                        # Execute the pipeline commands for efficient data transfer
                        pipe.execute()
                    except Exception as e:
                        print(f"Error migrating key '{key.decode()}': {e}")

        print("Data migration complete!")

    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis clusters: {e}")
    except Exception as e:
        print(f"Unexpected error during migration: {e}")

# Replace with the actual startup nodes and ElastiCache configuration
redis_cluster_a_nodes = [
    {'host': 'cluster-a-node1', 'port': 6379},
    {'host': 'cluster-a-node2', 'port': 6379},
    # ... add more nodes if needed
]
redis_cluster_b_nodes = []  # Not used for ElastiCache connection

# Adjust database number if necessary
db = 0

migrate_data(redis_cluster_a_nodes, redis_cluster_b_nodes, db)
