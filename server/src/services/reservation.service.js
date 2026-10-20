const mongoose = require("mongoose");

const Product = require("../models/Product");
const Reservation = require("../models/Reservation");
const {
    invalidateProductListCache,
} = require("./product-cache.service");

const createHttpError = (statusCode, message) => {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
};

const serializeReservation = (reservation) => ({
    id: String(reservation._id),
    product: String(reservation.product),
    quantity: reservation.quantity,
    status: reservation.status,
    expiresAt: reservation.expiresAt,
});

const getInventory = (product) => ({
    availableStock: product.availableStock,
    reservedStock: product.reservedStock,
    soldStock: product.soldStock,
});

const getProductInventory = async (productId, session) => {
    const product = await Product.findById(productId).session(session);

    return product ? getInventory(product) : null;
};

const confirmReservation = async (reservationId) => {
    const session = await mongoose.startSession();

    try {
        let result;

        await session.withTransaction(async () => {
            const reservation = await Reservation.findById(
                reservationId
            ).session(session);

            if (!reservation) {
                throw createHttpError(
                    404,
                    "Reservation not found"
                );
            }

            // Repeated confirmation must not sell the item twice.
            if (reservation.status === "confirmed") {
                result = {
                    reservation: serializeReservation(reservation),
                    inventory: await getProductInventory(
                        reservation.product,
                        session
                    ),
                    replayed: true,
                };

                return;
            }

            if (reservation.status !== "pending") {
                throw createHttpError(
                    409,
                    `Cannot confirm a ${reservation.status} reservation`
                );
            }

            const now = new Date();

            if (reservation.expiresAt <= now) {
                throw createHttpError(
                    409,
                    "Reservation has expired"
                );
            }

            // Move units from reserved stock to sold stock.
            const updatedProduct = await Product.findOneAndUpdate(
                {
                    _id: reservation.product,
                    reservedStock: {
                        $gte: reservation.quantity,
                    },
                },
                {
                    $inc: {
                        reservedStock: -reservation.quantity,
                        soldStock: reservation.quantity,
                    },
                },
                {
                    returnDocument: "after",
                    session,
                }
            );

            if (!updatedProduct) {
                throw createHttpError(
                    500,
                    "Inventory consistency error while confirming reservation"
                );
            }

            const updatedReservation =
                await Reservation.findOneAndUpdate(
                    {
                        _id: reservation._id,
                        status: "pending",
                        expiresAt: { $gt: now },
                    },
                    {
                        $set: {
                            status: "confirmed",
                        },
                    },
                    {
                        returnDocument: "after",
                        session,
                    }
                );

            if (!updatedReservation) {
                throw createHttpError(
                    409,
                    "Reservation state changed; please retry"
                );
            }

            result = {
                reservation: serializeReservation(
                    updatedReservation
                ),
                inventory: getInventory(updatedProduct),
                replayed: false,
            };
        });
        if (!result.replayed) {
            await invalidateProductListCache();
        }
        return result;
    } finally {
        await session.endSession();
    }
};

const releaseReservation = async (reservationId) => {
    const session = await mongoose.startSession();

    try {
        let result;

        await session.withTransaction(async () => {
            const reservation = await Reservation.findById(
                reservationId
            ).session(session);

            if (!reservation) {
                throw createHttpError(
                    404,
                    "Reservation not found"
                );
            }

            // A repeated release must not restore stock twice.
            if (
                reservation.status === "released" ||
                reservation.status === "expired"
            ) {
                result = {
                    reservation: serializeReservation(reservation),
                    inventory: await getProductInventory(
                        reservation.product,
                        session
                    ),
                    replayed: true,
                };

                return;
            }

            if (reservation.status !== "pending") {
                throw createHttpError(
                    409,
                    `Cannot release a ${reservation.status} reservation`
                );
            }

            const now = new Date();

            const finalStatus =
                reservation.expiresAt <= now
                    ? "expired"
                    : "released";

            // Return reserved units to available stock.
            const updatedProduct = await Product.findOneAndUpdate(
                {
                    _id: reservation.product,
                    reservedStock: {
                        $gte: reservation.quantity,
                    },
                },
                {
                    $inc: {
                        availableStock: reservation.quantity,
                        reservedStock: -reservation.quantity,
                    },
                },
                {
                    returnDocument: "after",
                    session,
                }
            );

            if (!updatedProduct) {
                throw createHttpError(
                    500,
                    "Inventory consistency error while releasing reservation"
                );
            }

            const updatedReservation =
                await Reservation.findOneAndUpdate(
                    {
                        _id: reservation._id,
                        status: "pending",
                    },
                    {
                        $set: {
                            status: finalStatus,
                        },
                    },
                    {
                        returnDocument: "after",
                        session,
                    }
                );

            if (!updatedReservation) {
                throw createHttpError(
                    409,
                    "Reservation state changed; please retry"
                );
            }

            result = {
                reservation: serializeReservation(
                    updatedReservation
                ),
                inventory: getInventory(updatedProduct),
                replayed: false,
            };
        });
        if (!result.replayed) {
            await invalidateProductListCache();
        }
        return result;
    } finally {
        await session.endSession();
    }
};

module.exports = {
    confirmReservation,
    releaseReservation,
};