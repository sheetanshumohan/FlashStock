import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const created = new Counter("reservation_created");
const unexpected = new Counter("unexpected_responses");
const productIds = (__ENV.PRODUCT_IDS || "").split(",").filter(Boolean);

export const options = {
    scenarios: {
        reservations: {
            executor: "per-vu-iterations",
            vus: Number(__ENV.VUS || 5),
            iterations: 1,
            maxDuration: "30s",
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
        unexpected_responses: ["count==0"],
    },
};

export default function () {
    const productId = productIds[__VU - 1];

    if (!productId) {
        throw new Error(`No product configured for VU ${__VU}`);
    }

    const baseUrl = __ENV.BASE_URL || "http://localhost:5000";
    const key = `different-${__ENV.RUN_ID}-${__VU}-${Date.now()}-${Math.random().toString(16).slice(2)}`;

    const response = http.post(
        `${baseUrl}/api/products/${productId}/reservations`,
        JSON.stringify({ quantity: 1 }),
        {
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": key,
            },
        }
    );

    if (response.status === 201) {
        created.add(1);
    } else {
        unexpected.add(1);

        console.error(
            `Unexpected reservation response: VU=${__VU}, ` +
            `status=${response.status}, body=${response.body}`
        );
    }

    check(response, {
        "reservation returns HTTP 201": (r) => r.status === 201,
    });
}
