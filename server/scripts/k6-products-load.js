import http from "k6/http";
import { check } from "k6";

export const options = {
    stages: [
        { duration: "10s", target: 1 },
        { duration: "10s", target: 5 },
        { duration: "10s", target: 10 },
        { duration: "5s", target: 0 },
    ],

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<500"],
        checks: ["rate>0.99"],
    },
};

export default function () {
    const response = http.get(
        "http://localhost:5000/api/products"
    );

    let body = null;

    try {
        body = response.json();
    } catch (_) {
        // Invalid JSON will cause the success check to fail.
    }

    check(response, {
        "status is 200": (r) => r.status === 200,
        "API reports success": () => body?.success === true,
        "products is an array": () => Array.isArray(body?.data),
    });
}