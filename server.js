const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { DELTA_BASE, getDashboardPayload } = require("./lib/dashboard");

const HOST = "127.0.0.1";
const PORT = process.env.PORT || 3000;

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(JSON.stringify(payload));
}

function sendFile(res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || "application/octet-stream";
  const stream = fs.createReadStream(filePath);

  stream.on("error", () => {
    sendJson(res, 404, { success: false, error: { message: "File not found" } });
  });

  res.writeHead(200, { "Content-Type": contentType });
  stream.pipe(res);
}

async function proxyDelta(req, res, pathname, searchParams) {
  const upstreamUrl = new URL(`${DELTA_BASE}${pathname.replace(/^\/api/, "")}`);
  upstreamUrl.search = searchParams.toString();

  try {
    const upstream = await fetch(upstreamUrl, {
      headers: {
        Accept: "application/json",
        "User-Agent": "market-bias-dashboard/1.0",
      },
    });

    const text = await upstream.text();
    let body;

    try {
      body = JSON.parse(text);
    } catch {
      body = { success: false, error: { message: text || "Non-JSON upstream response" } };
    }

    if (!upstream.ok) {
      sendJson(res, upstream.status, body);
      return;
    }

    sendJson(res, 200, body);
  } catch (error) {
    sendJson(res, 502, {
      success: false,
      error: {
        message: `Delta proxy request failed: ${error.message}`,
      },
    });
  }
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = requestUrl.pathname;

  if (pathname === "/api/dashboard") {
    try {
      const payload = await getDashboardPayload({
        forceFresh: requestUrl.searchParams.get("fresh") === "1",
      });
      sendJson(res, 200, payload);
    } catch (error) {
      sendJson(res, 502, {
        success: false,
        error: { message: `Dashboard build failed: ${error.message}` },
      });
    }
    return;
  }

  if (pathname.startsWith("/api/")) {
    await proxyDelta(req, res, pathname, requestUrl.searchParams);
    return;
  }

  const requestedPath = pathname === "/" ? "/index.html" : pathname;
  const safePath = path.normalize(path.join(process.cwd(), requestedPath));

  if (!safePath.startsWith(process.cwd())) {
    sendJson(res, 403, { success: false, error: { message: "Forbidden" } });
    return;
  }

  if (!fs.existsSync(safePath) || fs.statSync(safePath).isDirectory()) {
    sendJson(res, 404, { success: false, error: { message: "Not found" } });
    return;
  }

  sendFile(res, safePath);
});

server.listen(PORT, HOST, () => {
  console.log(`Market Bias dashboard running at http://${HOST}:${PORT}`);
});
