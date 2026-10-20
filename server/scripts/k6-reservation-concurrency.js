import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const reservationsCreated = new Counter("reservations_created");
const stockRejections = new Counter("stock_rejections");
const rateLimitedRequests = new Counter("rate_limited_requests");
const unexpectedResponses = new Counter("unexpected_responses");

const baseURL = __ENV.BASE_URL || "http://localhost:5000";
const productId = __ENV.PRODUCT_ID;

// For this test, 201 and a genuine insufficient-stock 409
// are expected responses.
http.setResponseCallback(http.expectedStatuses(201, 409));

export const options = {
    scenarios: {
        reservation_burst: {
            executor: "per-vu-iterations",
            vus: 20,
            iterations: 1,
            maxDuration: "30s",
        },
    },

    thresholds: {
        http_req_failed: ["rate<0.01"],
        rate_limited_requests: ["count==0"],
        unexpected_responses: ["count==0"],
    },
};

export default function () {
    if (!productId) {
        throw new Error("Pass PRODUCT_ID using the k6 -e option.");
    }

    const response = http.post(
        `${baseURL}/api/products/${productId}/reservations`,
        JSON.stringify({ quantity: 1 }),
        {
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key":
                    `k6-concurrency-${productId}-${__VU}-${__ITER}-${Date.now()}`,
            },
        }
    );

    let body = null;

    try {
        body = response.json();
    } catch (_) {
        // Unexpected response format is handled below.
    }

    const created =
        response.status === 201 &&
        body?.success === true &&
        Boolean(body?.result?.reservation?.id);

    const rejectedForStock =
        response.status === 409 &&
        body?.message === "Insufficient available stock";

    if (created) {
        reservationsCreated.add(1);
    } else if (rejectedForStock) {
        stockRejections.add(1);
    } else if (response.status === 429) {
        rateLimitedRequests.add(1);
    } else {
        unexpectedResponses.add(1);

        console.log(
            `Unexpected HTTP ${response.status}: ` +
            `${String(response.body || "").slice(0, 250)}`
        );
    }

    check(response, {
        "reservation created or stock exhausted": () =>
            created || rejectedForStock,
    });
}