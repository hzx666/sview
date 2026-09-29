import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createLogger, defineConfig, loadEnv } from "vite";

// Keep frontend Vite logs in the repo-root project log (same as Python).
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const logFile = path.resolve(repoRoot, process.env.LOG_FILE || "sview.log");

function appendLog(level: string, msg: string) {
  const line = `${new Date().toISOString()} [${level}] vite: ${msg}\n`;
  try {
    fs.appendFileSync(logFile, line, "utf8");
  } catch {
    // ignore disk errors; console still works
  }
}

const logger = createLogger();
const { info, warn, warnOnce, error } = logger;
logger.info = (msg, options) => {
  appendLog("INFO", String(msg));
  info(msg, options);
};
logger.warn = (msg, options) => {
  appendLog("WARN", String(msg));
  warn(msg, options);
};
logger.warnOnce = (msg, options) => {
  appendLog("WARN", String(msg));
  warnOnce(msg, options);
};
logger.error = (msg, options) => {
  appendLog("ERROR", String(msg));
  error(msg, options);
};

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, repoRoot, "");
  // Local dev: browser → Vite (same-origin) → Master → (follow 302) OSS.
  // Avoids browser CORS on OSS after Master redirect.
  const proxyTarget =
    env.VITE_SVIEW_PROXY_TARGET ||
    process.env.VITE_SVIEW_PROXY_TARGET ||
    "http://47.117.66.71:8080";

  return {
    customLogger: logger,
    server: {
      host: "127.0.0.1",
      port: 5173,
      strictPort: true,
      proxy: {
        // Browser cannot read the private bucket directly when Bucket CORS is
        // missing. Keep the signed query string and proxy only in local dev.
        "/sview-oss": {
          target: "https://ss-oss-data-platform.oss-cn-shanghai.aliyuncs.com",
          changeOrigin: true,
          secure: true,
          rewrite: (p) => p.replace(/^\/sview-oss/, "") || "/",
          timeout: 0,
          proxyTimeout: 0,
        },
        // Use Base URL: http://127.0.0.1:5173/sview
        "/sview": {
          target: proxyTarget,
          changeOrigin: true,
          secure: false,
          rewrite: (p) => p.replace(/^\/sview/, "") || "/",
          // Node follows Master→OSS 302; browser only sees same-origin 200.
          // SSE (/events) must not buffer — long-lived stream.
          followRedirects: true,
          timeout: 0,
          proxyTimeout: 0,
          configure: (proxy) => {
            proxy.on("proxyReq", (proxyReq, req) => {
              const u = req.url || "";
              if (u.includes("/events")) {
                proxyReq.setHeader("Accept", "text/event-stream");
                proxyReq.setHeader("Cache-Control", "no-cache");
                // Avoid following redirects on the event stream.
                proxyReq.removeHeader("accept-encoding");
              }
            });
          },
        },
      },
    },
  };
});
