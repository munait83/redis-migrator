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
            const value = await sourceRedis.dump(key);
            const ttl = await sourceRedis.ttl(key);
            if (ttl > 0) {
                await destRedis.restore(key, ttl * 1000, value, 'REPLACE');
            } else {
                await destRedis.restore(key, 0, value, 'REPLACE');
            }
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
