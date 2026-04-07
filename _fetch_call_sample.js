require("dotenv").config();
const axios = require("axios");
const id = process.env.CALLRAIL_ACCOUNT_ID;
const k = process.env.CALLRAIL_API_KEY;
const callId = process.argv[2] || "CAL019d30fa57ac70e49449f7cc8b4ccff1";
const fields =
  "customer_phone_number,start_time,source,formatted_tracking_source,campaign,utm_campaign,tracking_phone_number,duration,direction,id";
axios
  .get(`https://api.callrail.com/v3/a/${id}/calls/${callId}.json`, {
    headers: { Authorization: `Token token="${k}"` },
    params: { fields },
  })
  .then((r) => {
    console.log(JSON.stringify(r.data, null, 2));
  })
  .catch((e) => console.error(e.response?.data || e.message));
