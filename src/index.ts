import WebSocket from "ws";
import fs from "fs";
import { createObjectCsvWriter } from "csv-writer";

const csvFilePath = "binance_orderbook.csv";
const WSS = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";
const TRADES_WSS = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const MAX_LVL_EXPORT = 10;

// Headers for Order Book and Trade Data
const headers = [
  "Last Update",
  "Coin",
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Price`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Size`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Cumulative BTC`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Price`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Size`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Cumulative BTC`),
  "Mark Price",
  "Oracle Price",
  "Funding Rate",
  "Open Interest",
  "24h Volume",
  "Trade Side",
  "Trade Price",
  "Trade Size",
];

const csvWriter = createObjectCsvWriter({
  path: csvFilePath,
  header: headers.map((title) => ({ id: title, title })),
  append: true,
});

if (!fs.existsSync(csvFilePath)) {
  fs.writeFileSync(csvFilePath, headers.join(",") + "\n");
}

const webSocketConnection = () => {
  const ws = new WebSocket(WSS);
  const tradeWs = new WebSocket(TRADES_WSS);

  ws.on("open", () => {
    console.log("Binance Order Book WebSocket connection established.");
  });

  tradeWs.on("open", () => {
    console.log("Binance Trade WebSocket connection established.");
  });

  ws.on("message", async (data: any) => {
    try {
      const parsedData = JSON.parse(data);
      const { E, s, b, a } = parsedData; // E: Event time, s: Symbol, b: Bids, a: Asks

      const asks = a.slice(0, MAX_LVL_EXPORT);
      const bids = b.slice(0, MAX_LVL_EXPORT);

      let askSum = 0;
      let bidSum = 0;

      const orderBookEntry = {
        "Last Update": new Date(E).toLocaleString(),
        Coin: s,
        ...Object.fromEntries(
          asks.flatMap((ask: any, i: number) => {
            askSum += parseFloat(ask[1]);
            return [
              [`Ask L${i + 1} Price`, ask[0]],
              [`Ask L${i + 1} Size`, ask[1]],
              [`Ask L${i + 1} Cumulative BTC`, askSum.toFixed(4)],
            ];
          })
        ),
        ...Object.fromEntries(
          bids.flatMap((bid: any, i: number) => {
            bidSum += parseFloat(bid[1]);
            return [
              [`Bid L${i + 1} Price`, bid[0]],
              [`Bid L${i + 1} Size`, bid[1]],
              [`Bid L${i + 1} Cumulative BTC`, bidSum.toFixed(4)],
            ];
          })
        ),
        "Mark Price": "", 
        "Oracle Price": "", 
        "Funding Rate": "", 
        "Open Interest": "", 
        "24h Volume": "", 
        "Trade Side": "",
        "Trade Price": "",
        "Trade Size": "",
      };

      await csvWriter.writeRecords([orderBookEntry]);
    } catch (err) {
      console.error("Failed to parse message:", data, err);
    }
  });

  tradeWs.on("message", async (data: any) => {
    try {
      const parsedTrade = JSON.parse(data);
      const { E, s, p, q, m } = parsedTrade; // E: Event time, s: Symbol, p: Price, q: Quantity, m: Maker (true=Sell, false=Buy)

      const tradeEntry = {
        "Last Update": new Date(E).toLocaleString(),
        Coin: s,
        ...Object.fromEntries(
          Array.from({ length: MAX_LVL_EXPORT }, (_, i) => [
            [`Ask L${i + 1} Price`, ""],
            [`Ask L${i + 1} Size`, ""],
            [`Ask L${i + 1} Cumulative BTC`, ""],
            [`Bid L${i + 1} Price`, ""],
            [`Bid L${i + 1} Size`, ""],
            [`Bid L${i + 1} Cumulative BTC`, ""],
          ])
        ),
        "Mark Price": "",
        "Oracle Price": "",
        "Funding Rate": "",
        "Open Interest": "",
        "24h Volume": "",
        "Trade Side": m ? "Sell (Ask)" : "Buy (Bid)",
        "Trade Price": p,
        "Trade Size": q,
      };

      await csvWriter.writeRecords([tradeEntry]);
    } catch (err) {
      console.error("Failed to parse trade message:", data, err);
    }
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error);
  });

  tradeWs.on("error", (error) => {
    console.error("Trade WebSocket error:", error);
  });

  ws.on("close", () => {
    console.log("Order Book WebSocket connection closed.");
  });

  tradeWs.on("close", () => {
    console.log("Trade WebSocket connection closed.");
  });
};

webSocketConnection();