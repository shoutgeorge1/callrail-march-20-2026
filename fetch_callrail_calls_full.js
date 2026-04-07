require("dotenv").config();
const fs = require("fs");
const path = require("path");
const axios = require("axios");

const CALLRAIL_API_KEY = process.env.CALLRAIL_API_KEY;
const CALLRAIL_ACCOUNT_ID = process.env.CALLRAIL_ACCOUNT_ID;

if (!CALLRAIL_API_KEY || !CALLRAIL_ACCOUNT_ID) {
  throw new Error("Missing CALLRAIL_API_KEY or CALLRAIL_ACCOUNT_ID");
}

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

async function main() {
  const end = new Date("2026-03-31T23:59:59Z");
  const start = new Date("2026-01-26T00:00:00Z");

  const base = `https://api.callrail.com/v3/a/${CALLRAIL_ACCOUNT_ID}/calls.json`;
  const params = {
    start_date: isoDate(start),
    end_date: isoDate(end),
    per_page: "100",
    relative_pagination: "true",
    direction: "inbound",
    fields:
      "customer_phone_number,source,formatted_tracking_source,start_time,direction,id,utm_campaign,campaign,tracking_phone_number,duration,created_at",
  };
  const headers = { Authorization: `Token token="${CALLRAIL_API_KEY}"` };

  let url = base;
  let first = true;
  let page = 0;
  const calls = [];

  while (url) {
    console.log(`Fetching page ${page + 1}...`);
    const res = await axios.get(url, {
      headers,
      params: first ? params : undefined,
      timeout: 120000,
    });
    const data = res.data || {};
    calls.push(...(data.calls || []));
    page += 1;
    console.log(`  got ${data.calls ? data.calls.length : 0} calls (running ${calls.length})`);
    url = data.has_next_page && data.next_page ? data.next_page : null;
    first = false;
    if (url) await new Promise((resolve) => setTimeout(resolve, 1400));
  }

  const outPath = path.join(__dirname, "data", "callrail_calls_full.json");
  fs.writeFileSync(outPath, JSON.stringify(calls, null, 2), "utf8");
  console.log(JSON.stringify({ pages: page, calls: calls.length, outPath }, null, 2));
}

main().catch((err) => {
  console.error(err.response?.data || err.message);
  process.exit(1);
});
