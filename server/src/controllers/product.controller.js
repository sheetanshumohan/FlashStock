const Product = require("../models/Product");
const {
    getProductListCache,
    setProductListCache,
    invalidateProductListCache,
} = require("../services/product-cache.service");
const {
    createProductSchema,
} = require("../validators/product.validators");

const createProduct = async (req, res, next) => {
    const result = createProductSchema.safeParse(req.body);

    if (!result.success) {
        return res.status(400).json({
            success: false,
            message: "Invalid product data",
            errors: result.error.issues.map((issue) => ({
                field: issue.path.join("."),
                message: issue.message,
            })),
        });
    }

    try {
        const {
            name,
            sku,
            description,
            price,
            initialStock,
        } = result.data;

        const product = await Product.create({
            name,
            sku,
            description,
            price,
            availableStock: initialStock,
        });
        await invalidateProductListCache();
        return res.status(201).json({
            success: true,
            message: "Product created successfully",
            data: product,
        });
    } catch (error) {
        if (error.code === 11000) {
            return res.status(409).json({
                success: false,
                message: "A product with this SKU already exists",
            });
        }

        next(error);
    }
};

const getProducts = async (req, res, next) => {
    try {
        const pageValue = Number(req.query.page ?? 1);
        const limitValue = Number(req.query.limit ?? 10);

        if (
            !Number.isInteger(pageValue) ||
            !Number.isInteger(limitValue) ||
            pageValue < 1 ||
            limitValue < 1 ||
            limitValue > 100
        ) {
            return res.status(400).json({
                success: false,
                message:
                    "Page must be positive and limit must be between 1 and 100",
            });
        }

        // 1. Check Redis.
        const cache = await getProductListCache(
            pageValue,
            limitValue
        );

        if (cache?.data) {
            return res.status(200).json(cache.data);
        }

        // 2. Cache miss: fetch from MongoDB.
        const filter = { isActive: true };

        const [products, total] = await Promise.all([
            Product.find(filter)
                .sort({ createdAt: -1 })
                .skip((pageValue - 1) * limitValue)
                .limit(limitValue)
                .lean(),

            Product.countDocuments(filter),
        ]);

        const response = {
            success: true,
            data: products,
            pagination: {
                page: pageValue,
                limit: limitValue,
                total,
                totalPages: Math.ceil(total / limitValue),
            },
        };

        // 3. Cache the result for future requests.
        await setProductListCache(cache?.key, response);

        return res.status(200).json(response);
    } catch (error) {
        next(error);
    }
};

module.exports = {
    createProduct,
    getProducts,
};