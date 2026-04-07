require("dotenv").config();
const axios = require("axios");
const id = process.env.CALLRAIL_ACCOUNT_ID;
const k = process.env.CALLRAIL_API_KEY;
const params = new URLSearchParams({
  per_page: "1",
  start_date: "2026-03-01",
  end_date: "2026-03-27",
  fields:
    "customer_phone_number,source,formatted_tracking_source,start_time,direction,id,utm_campaign,campaign",
  direction: "inbound",
});
axios
  .get(`https://api.callrail.com/v3/a/${id}/calls.json?${params}`, {
    headers: { Authorization: `Token token="${k}"` },
  })
  .then((r) => console.log(JSON.stringify(r.data.calls[0], null, 2)))
  .catch((e) => console.error(e.response?.data || e.message));
