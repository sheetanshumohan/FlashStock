const { redisClient } = require("../config/redis");

const VERSION_KEY = "flashstock:products:catalog:version";
const CACHE_TTL_SECONDS = 30;

const buildCacheKey = (version, page, limit) => {
    return `flashstock:products:list:v${version}:page:${page}:limit:${limit}`;
};

const getProductListCache = async (page, limit) => {
    if (!redisClient.isReady) {
        return null;
    }

    try {
        // Initialize the version key once.
        await redisClient.set(VERSION_KEY, "1", {
            NX: true,
        });

        const version = await redisClient.get(VERSION_KEY);

        if (!version) {
            return null;
        }

        const key = buildCacheKey(version, page, limit);
        const cachedValue = await redisClient.get(key);

        return {
            key,
            data: cachedValue ? JSON.parse(cachedValue) : null,
        };
    } catch (error) {
        console.error(
            "Product cache read failed:",
            error.message
        );

        // Cache failure should not prevent reading MongoDB.
        return null;
    }
};

const setProductListCache = async (key, data) => {
    if (!key || !redisClient.isReady) {
        return;
    }

    try {
        await redisClient.set(
            key,
            JSON.stringify(data),
            {
                EX: CACHE_TTL_SECONDS,
            }
        );
    } catch (error) {
        console.error(
            "Product cache write failed:",
            error.message
        );
    }
};

const invalidateProductListCache = async () => {
    if (!redisClient.isReady) {
        return;
    }

    try {
        await redisClient.set(VERSION_KEY, "1", {
            NX: true,
        });

        // New requests will now use a different cache namespace.
        await redisClient.incr(VERSION_KEY);
    } catch (error) {
        console.error(
            "Product cache invalidation failed:",
            error.message
        );
    }
};

module.exports = {
    getProductListCache,
    setProductListCache,
    invalidateProductListCache,
};