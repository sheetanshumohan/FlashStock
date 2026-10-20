const { redisClient } = require("../config/redis");

const FIXED_WINDOW_SCRIPT = `
    local current = redis.call("INCR", KEYS[1])

    if current == 1 then
        redis.call("PEXPIRE", KEYS[1], ARGV[1])
    end

    local ttl = redis.call("PTTL", KEYS[1])

    return { current, ttl }
`;

const createRateLimiter = ({
    windowMs = 60_000,
    max = 60,
    keyPrefix = "flashstock:ratelimit",
    failClosed = false,
} = {}) => {
    return async (req, res, next) => {
        const clientIp =
            req.ip || req.socket?.remoteAddress || "unknown";

        const redisKey = `${keyPrefix}:${clientIp}`;

        try {
            if (!redisClient.isReady) {
                throw new Error("Redis is not ready");
            }

            // Increment and set expiry atomically in Redis.
            const [countResult, ttlResult] = await redisClient.eval(
                FIXED_WINDOW_SCRIPT,
                {
                    keys: [redisKey],
                    arguments: [String(windowMs)],
                }
            );

            const count = Number(countResult);
            const ttlMs = Math.max(0, Number(ttlResult));

            const retryAfterSeconds = Math.max(
                1,
                Math.ceil(ttlMs / 1000)
            );

            res.set("RateLimit-Limit", String(max));
            res.set(
                "RateLimit-Remaining",
                String(Math.max(0, max - count))
            );
            res.set(
                "RateLimit-Reset",
                String(retryAfterSeconds)
            );

            if (count > max) {
                res.set(
                    "Retry-After",
                    String(retryAfterSeconds)
                );

                return res.status(429).json({
                    success: false,
                    message:
                        "Too many requests. Please try again later.",
                    retryAfterSeconds,
                });
            }

            return next();
        } catch (error) {
            console.error(
                "Rate limiter error:",
                error.message
            );

            // Choose whether the API should be blocked when
            // the rate-limiting dependency is unavailable.
            if (failClosed) {
                return res.status(503).json({
                    success: false,
                    message:
                        "Rate limiting is temporarily unavailable.",
                });
            }

            // Fail-open: allow the request if rate limiting fails.
            return next();
        }
    };
};

module.exports = {
    createRateLimiter,
};