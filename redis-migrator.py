import redis
from redis.cluster import RedisCluster

def migrate_data(redis_cluster_a_nodes, redis_cluster_b_endpoint, redis_cluster_b_port, db=0, redis_cluster_b_password=None):
    """
    Migrates data from Redis Cluster A to AWS ElastiCache for Redis, handling various data types.

    Args:
        redis_cluster_a_nodes (list): List of startup nodes for Redis Cluster A.
        redis_cluster_b_endpoint (str): Endpoint of your AWS ElastiCache for Redis cluster.
        redis_cluster_b_port (int): Port of your AWS ElastiCache for Redis cluster.
        db (int, optional): Database number to migrate data from. Defaults to 0.
        redis_cluster_b_password (str, optional): Password for your AWS ElastiCache for Redis cluster (if applicable).
    """

    try:
        # Connect to Redis Cluster A
        redis_cluster_a = RedisCluster(startup_nodes=redis_cluster_a_nodes)

        # Connect to AWS ElastiCache for Redis
        redis_cluster_b = redis.RedisCluster(connection_pool=redis.ConnectionPool(
            host=redis_cluster_b_endpoint,
            port=redis_cluster_b_port,
            db=db,
            password=redis_cluster_b_password,
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
                            value_to_migrate = value
                        elif data_type == b'list':
                            value_to_migrate = value  # List itself is the value
                        elif data_type == b'set':
                            value_to_migrate = value  # Set itself is the value
                        elif data_type == b'zset':
                            # Handle sorted sets with scores: (member, score) tuples
                            for member, score in redis_cluster_a.zrange(key, 0, -1, withscores=True):
                                pipe.zadd(key, score, member)
                        elif data_type == b'hash':
                            value_to_migrate = value  # Hash itself is the value
                        else:
                            print(f"Warning: Unsupported data type '{data_type.decode()}' for key '{key.decode()}'")
                            continue  # Skip migrating this key

                        # Use value_to_migrate for migration to ElastiCache
                        pipe.set(key, value_to_migrate)  # Or use appropriate command for other data types

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
    {'host': 'redis-cluster-1697815485.redis', 'port': 6379},
    # ... add more nodes if needed
]
redis_cluster_b_endpoint = "your-elastiache-endpoint"
redis_cluster_b_port = 6379

# Adjust database number if necessary
db = 0

migrate_data(redis_cluster_a_nodes, redis_cluster_b_endpoint, redis_cluster_b_port, db, redis_cluster_b_password)
