import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
    vus: 5,
    duration: "20s",

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
        // The checks below will report an invalid response.
    }

    check(response, {
        "status is 200": (r) => r.status === 200,
        "API reports success": () => body?.success === true,
        "products is an array": () => Array.isArray(body?.data),
        "pagination is present": () =>
            typeof body?.pagination?.totalPages === "number",
    });

    sleep(1);
}