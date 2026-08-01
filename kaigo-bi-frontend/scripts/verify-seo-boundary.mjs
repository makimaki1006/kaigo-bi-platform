/**
 * 公開SEOページとログイン後領域の境界をHTTPで検証する。
 *
 * 使用例:
 *   $env:SEO_BASE_URL="https://kaigo-bi.onrender.com"
 *   npm run verify:seo-boundary
 *
 * SEO_BASE_URL未指定時は http://127.0.0.1:3000 を使用する。
 * 統合環境（nginx + Rust）では代表データAPIの未認証拒否も検査する。
 */

const baseUrl = (process.env.SEO_BASE_URL ?? "http://127.0.0.1:3000").replace(
  /\/+$/,
  ""
);
const canonicalBaseUrl = (
  process.env.SEO_CANONICAL_URL ?? "https://kaigo-bi.onrender.com"
).replace(/\/+$/, "");
const expectIndex = process.env.SEO_EXPECT_INDEX === "true";

const publicPages = [
  "/",
  "/features/management",
  "/features/sales",
  "/features/ma",
  "/data",
  "/methodology",
  "/pricing",
];

const noindexPages = ["/login", "/signup", "/dashboard", "/facilities", "/ma-screening"];

const protectedApis = [
  "/api/dashboard/kpi",
  "/api/facilities/search",
  "/api/ma/screening",
];

const failures = [];

function robotsContent(html) {
  return (
    html.match(/<meta[^>]+name=["']robots["'][^>]+content=["']([^"']+)["']/i)?.[1] ??
    html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']robots["']/i)?.[1] ??
    ""
  ).toLowerCase();
}

function canonicalHref(html) {
  return (
    html.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i)?.[1] ??
    html.match(/<link[^>]+href=["']([^"']+)["'][^>]+rel=["']canonical["']/i)?.[1] ??
    ""
  );
}

async function fetchPage(path) {
  const response = await fetch(`${baseUrl}${path}`, {
    redirect: "manual",
    headers: { "user-agent": "kaigo-bi-seo-boundary-check/1.0" },
  });
  return { response, body: await response.text() };
}

for (const path of publicPages) {
  try {
    const { response, body } = await fetchPage(path);
    const robots = robotsContent(body);
    const canonical = canonicalHref(body);

    if (response.status !== 200) {
      failures.push(`${path}: expected 200, got ${response.status}`);
    }
    if (expectIndex && (!robots.includes("index") || robots.includes("noindex"))) {
      failures.push(`${path}: expected index robots, got "${robots || "missing"}"`);
    }
    if (!expectIndex && !robots.includes("noindex")) {
      failures.push(`${path}: expected draft noindex, got "${robots || "missing"}"`);
    }
    if (!canonical) {
      failures.push(`${path}: canonical is missing`);
    }
    if (!/<h1(?:\s|>)/i.test(body)) {
      failures.push(`${path}: h1 is missing from initial HTML`);
    }
  } catch (error) {
    failures.push(`${path}: request failed (${error.message})`);
  }
}

for (const path of noindexPages) {
  try {
    const { response, body } = await fetchPage(path);
    const robots = robotsContent(body);

    if (response.status !== 200) {
      failures.push(`${path}: expected 200, got ${response.status}`);
    }
    if (!robots.includes("noindex")) {
      failures.push(`${path}: expected noindex, got "${robots || "missing"}"`);
    }
  } catch (error) {
    failures.push(`${path}: request failed (${error.message})`);
  }
}

try {
  const robots = await fetch(`${baseUrl}/robots.txt`).then(async (response) => ({
    status: response.status,
    body: await response.text(),
  }));
  if (robots.status !== 200) failures.push(`/robots.txt: got ${robots.status}`);
  if (!/^Disallow:\s*\/api\/\s*$/im.test(robots.body)) {
    failures.push(`/robots.txt: /api/ is not disallowed`);
  }
  for (const path of publicPages) {
    if (path !== "/" && robots.body.includes(`Disallow: ${path}`)) {
      failures.push(`/robots.txt: public page ${path} is disallowed`);
    }
  }
} catch (error) {
  failures.push(`/robots.txt: request failed (${error.message})`);
}

try {
  const sitemap = await fetch(`${baseUrl}/sitemap.xml`).then(async (response) => ({
    status: response.status,
    body: await response.text(),
  }));
  if (sitemap.status !== 200) failures.push(`/sitemap.xml: got ${sitemap.status}`);
  for (const path of publicPages) {
    const expected =
      path === "/" ? canonicalBaseUrl : `${canonicalBaseUrl}${path}`;
    if (expectIndex && !sitemap.body.includes(`<loc>${expected}</loc>`)) {
      failures.push(`/sitemap.xml: missing ${expected}`);
    }
    if (!expectIndex && sitemap.body.includes(`<loc>${expected}</loc>`)) {
      failures.push(`/sitemap.xml: draft URL must not be listed (${expected})`);
    }
  }
} catch (error) {
  failures.push(`/sitemap.xml: request failed (${error.message})`);
}

// Next.js単体開発サーバーでは/apiが存在しないため、404は検査対象外。
// nginx + Rustの統合環境では、未認証アクセスが401/403で拒否されることを要求する。
for (const path of protectedApis) {
  try {
    const response = await fetch(`${baseUrl}${path}`, {
      redirect: "manual",
      headers: { "user-agent": "kaigo-bi-seo-boundary-check/1.0" },
    });
    if (response.status !== 404 && ![401, 403].includes(response.status)) {
      failures.push(`${path}: unauthenticated request returned ${response.status}`);
    }
    if ([401, 403].includes(response.status)) {
      const xRobots = response.headers.get("x-robots-tag") ?? "";
      const cacheControl = response.headers.get("cache-control") ?? "";
      if (!xRobots.toLowerCase().includes("noindex")) {
        failures.push(`${path}: X-Robots-Tag noindex is missing`);
      }
      if (!cacheControl.toLowerCase().includes("no-store")) {
        failures.push(`${path}: Cache-Control no-store is missing`);
      }
    }
  } catch (error) {
    failures.push(`${path}: request failed (${error.message})`);
  }
}

if (failures.length > 0) {
  console.error("SEO/security boundary verification failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log(`SEO/security boundary verified: ${baseUrl}`);
console.log(
  expectIndex
    ? `${publicPages.length} approved public pages indexable; ${noindexPages.length} app/auth pages noindex`
    : `${publicPages.length} draft public pages noindex; ${noindexPages.length} app/auth pages noindex`
);
