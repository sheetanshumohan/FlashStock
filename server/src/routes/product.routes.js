const express = require("express");

const {
    createProduct,
    getProducts,
} = require("../controllers/product.controller");
const {
    createRateLimiter,
} = require("../middleware/rateLimiter");

const productReadLimiter = createRateLimiter({
    windowMs: 60_000,
    max: Number(
        process.env.PRODUCT_READ_RATE_LIMIT_MAX || 120
    ),
    keyPrefix: "flashstock:ratelimit:products-read",
    failClosed: false,
});

const productWriteLimiter = createRateLimiter({
    windowMs: 60_000,
    max: 20,
    keyPrefix: "flashstock:ratelimit:products-write",
    failClosed: true,
});
const router = express.Router();

router.post("/", productWriteLimiter, createProduct);
router.get("/", productReadLimiter, getProducts);

module.exports = router;