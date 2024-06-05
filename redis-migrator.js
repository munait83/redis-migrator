const Redis = require('ioredis');

// Function to fetch all keys from the source Redis cluster
async function fetchAllKeys(sourceRedis) {
    const keys = await sourceRedis.keys('*');
    return keys;
}

// Function to copy data from source to destination
async function copyData(sourceRedis, destRedis, keys) {
    for (const key of keys) {
        try {
            const type = await sourceRedis.type(key);
            console.log(`Copying key: ${key} of type: ${type}`);
            
            if (type === 'string') {
                const value = await sourceRedis.get(key);
                const ttl = await sourceRedis.ttl(key);
                if (ttl > 0) {
                    await destRedis.set(key, value, 'EX', ttl);
                } else {
                    await destRedis.set(key, value);
                }
            } else if (type === 'list') {
                const length = await sourceRedis.llen(key);
                for (let i = 0; i < length; i++) {
                    const value = await sourceRedis.lindex(key, i);
                    await destRedis.rpush(key, value);
                }
            } else if (type === 'set') {
                const members = await sourceRedis.smembers(key);
                await destRedis.sadd(key, ...members);
            } else if (type === 'zset') {
                const members = await sourceRedis.zrange(key, 0, -1, 'WITHSCORES');
                for (let i = 0; i < members.length; i += 2) {
                    await destRedis.zadd(key, members[i + 1], members[i]);
                }
            } else if (type === 'hash') {
                const hash = await sourceRedis.hgetall(key);
                await destRedis.hmset(key, hash);
            }
            console.log(`Successfully copied key: ${key}`);
        } catch (error) {
            console.error(`Failed to copy key ${key}: ${error.message}`);
        }
    }
}

// Configuration for the source Redis cluster
const sourceRedisCluster = [
    { host: 'redis-cluster-1697815485.redis', port: '6379' }
];

// Configuration for the destination Redis cluster (AWS ElastiCache)
const destRedisCluster = [
    { host: 'depot360-redis-cluster.ui6pmw.clustercfg.use1.cache.amazonaws.com', port: '6379' }
];

(async () => {
    // Connect to the source Redis cluster
    const sourceRedis = new Redis.Cluster(sourceRedisCluster);

    // Connect to the destination Redis cluster
    const destRedis = new Redis.Cluster(destRedisCluster);

    try {
        // Fetch all keys from the source Redis cluster
        const keys = await fetchAllKeys(sourceRedis);
        console.log(`Fetched ${keys.length} keys from the source Redis cluster.`);

        // Copy data from source to destination
        await copyData(sourceRedis, destRedis, keys);
        console.log(`Copied ${keys.length} keys to the destination Redis cluster.`);
    } finally {
        // Close connections
        sourceRedis.disconnect();
        destRedis.disconnect();
    }
})();
