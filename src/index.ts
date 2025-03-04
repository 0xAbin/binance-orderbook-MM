import WebSocket from 'ws';
import fs from 'fs';
import axios from 'axios';
import { createObjectCsvWriter } from 'csv-writer';

// --------------------
// Configuration
// --------------------
const SYMBOL = 'btcusdt'; // trading pair (lowercase)
const MAX_LVL_EXPORT = 10; // number of order book levels to export

// Subscribe to streams: depth, markPrice, trade, and ticker (24h volume)
// Note: markPrice update includes index price ("i") and funding rate ("r")
const BINANCE_WS_URL = `wss://fstream.binance.com/stream?streams=${SYMBOL}@depth@100ms/${SYMBOL}@markPrice@1s/${SYMBOL}@trade/${SYMBOL}@ticker`;

// REST endpoint for open interest
const OPEN_INTEREST_URL = `https://fapi.binance.com/fapi/v1/openInterest?symbol=${SYMBOL.toUpperCase()}`;

// --------------------
// CSV Setup
// --------------------
const csvFilePath = 'binance_perp_orderbook.csv';
const headers = [
  'Last Update',
  'Coin',
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Price`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Size`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Ask L${i + 1} Cumulative BTC`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Price`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Size`),
  ...Array.from({ length: MAX_LVL_EXPORT }, (_, i) => `Bid L${i + 1} Cumulative BTC`),
  'Mark Price',
  'Index Price',
  'Funding Rate',
  'Open Interest',
  '24h Volume',
  'Trade Side',
  'Trade Price',
  'Trade Size',
];

const csvWriter = createObjectCsvWriter({
  path: csvFilePath,
  header: headers.map(title => ({ id: title, title })),
  append: fs.existsSync(csvFilePath) && fs.statSync(csvFilePath).size > 0,
});

// Write header row if file is new
if (!fs.existsSync(csvFilePath) || fs.statSync(csvFilePath).size === 0) {
  fs.writeFileSync(csvFilePath, headers.join(',') + '\n');
}

// --------------------
// TypeScript Interfaces
// --------------------
interface DepthUpdate {
  E: number;        // Event time
  s: string;        // Symbol
  b: [string, string][]; // Bids: [price, quantity]
  a: [string, string][]; // Asks: [price, quantity]
}

interface MarkPriceUpdate {
  e: string;  // Event type
  E: number;  // Event time
  s: string;  // Symbol
  p: string;  // Mark price
  i: string;  // Index price
  r: string;  // Funding rate
  T: number;  // Next funding time
}

interface TradeUpdate {
  e: string; // Event type
  E: number; // Event time
  s: string; // Symbol
  t: number; // Trade ID
  p: string; // Price
  q: string; // Quantity
  b: number; // Buyer order ID
  a: number; // Seller order ID
  m: boolean; // Is buyer the market maker? (true means sell, false means buy)
}

interface TickerUpdate {
  e: string; // Event type
  E: number; // Event time
  s: string; // Symbol
  v: string; // 24h volume of base asset
}

// --------------------
// Global Variables for Data
// --------------------
let markPrice = '';
let indexPrice = '';
let fundingRate = '';
let tradeSide = '';  // "Buy (Bid)" or "Sell (Ask)"
let tradePrice = '';
let tradeSize = '';
let tickerVolume24h = '';
let openInterest = '';

// --------------------
// REST Polling for Open Interest
// --------------------
const pollOpenInterest = async () => {
  try {
    const response = await axios.get(OPEN_INTEREST_URL);
    // Response example: { openInterest: "12345.6789", symbol: "BTCUSDT" }
    openInterest = response.data.openInterest;
    console.log(`📊 Open Interest: ${openInterest}`);
  } catch (error) {
    console.error('🚨 Error fetching open interest:', error);
  }
};
setInterval(pollOpenInterest, 30000);
pollOpenInterest();

// --------------------
// WebSocket Connection
// --------------------
const startWebSocket = () => {
  const ws = new WebSocket(BINANCE_WS_URL);

  ws.on('open', () => {
    console.log('✅ Binance Perp WebSocket connected.');
  });

  ws.on('message', async (data: WebSocket.Data) => {
    try {
      const parsedData = JSON.parse(data.toString());
      const { stream, data: eventData } = parsedData;
      console.log(`📡 Received stream: ${stream}`);

      // 1. Depth Updates (Order Book Data)
      if (stream.endsWith('@depth@100ms')) {
        const { E, s, b, a } = eventData as DepthUpdate;
        if (!a.length || !b.length) {
          console.warn('⚠️ Order book data missing! Skipping write.');
          return;
        }

        // Sort bids and asks to ensure proper L1, L2, etc.
        const sortedBids = b.sort((a, b) => parseFloat(b[0]) - parseFloat(a[0])); // Sort bids descending
        const sortedAsks = a.sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])); // Sort asks ascending

        let askSum = 0;
        let bidSum = 0;
        const orderBookEntry: Record<string, any> = {
          'Last Update': new Date(E).toLocaleString(),
          Coin: s,
          ...Object.fromEntries(
            sortedAsks.slice(0, MAX_LVL_EXPORT).flatMap((ask, i) => {
              askSum += parseFloat(ask[1]);
              return [
                [`Ask L${i + 1} Price`, ask[0]],
                [`Ask L${i + 1} Size`, ask[1]],
                [`Ask L${i + 1} Cumulative BTC`, askSum.toFixed(4)],
              ];
            })
          ),
          ...Object.fromEntries(
            sortedBids.slice(0, MAX_LVL_EXPORT).flatMap((bid, i) => {
              bidSum += parseFloat(bid[1]);
              return [
                [`Bid L${i + 1} Price`, bid[0]],
                [`Bid L${i + 1} Size`, bid[1]],
                [`Bid L${i + 1} Cumulative BTC`, bidSum.toFixed(4)],
              ];
            })
          ),
          'Mark Price': markPrice || 'N/A',
          'Index Price': indexPrice || 'N/A',
          'Funding Rate': fundingRate || 'N/A',
          'Open Interest': openInterest || 'N/A',
          '24h Volume': tickerVolume24h || 'N/A',
          'Trade Side': tradeSide || '',
          'Trade Price': tradePrice || '',
          'Trade Size': tradeSize || '',
        };

        // Ensure all headers exist in the entry
        headers.forEach(header => {
          if (!(header in orderBookEntry)) {
            orderBookEntry[header] = '';
          }
        });

        console.log('✅ Writing order book entry to CSV:', orderBookEntry);
        await csvWriter.writeRecords([orderBookEntry]);

        // Log L1 spread for debugging
        const l1Bid = sortedBids[0][0];
        const l1Ask = sortedAsks[0][0];
        console.log(`📊 L1 Spread: ${l1Ask} - ${l1Bid} = ${(parseFloat(l1Ask) - parseFloat(l1Bid)).toFixed(2)}`);
      }

      // 2. Mark Price Updates (Also includes index price & funding rate)
      if (stream.endsWith('@markPrice@1s')) {
        // Note: The markPrice update message contains both mark price ("p") and index price ("i")
        const { p, i, r } = eventData as MarkPriceUpdate;
        markPrice = p;
        indexPrice = i;
        fundingRate = r;
        console.log(`📈 Mark Price: ${markPrice}, Index Price: ${indexPrice}, Funding Rate: ${fundingRate}`);
      }

      // 3. Trade Updates
      if (stream.endsWith('@trade')) {
        const { p, q, m } = eventData as TradeUpdate;
        tradePrice = p;
        tradeSize = q;
        // In futures, if m is true, the buyer is the market maker → Sell order; otherwise, Buy order.
        tradeSide = m ? 'Sell (Ask)' : 'Buy (Bid)';
        console.log(`💰 Trade - Side: ${tradeSide}, Price: ${tradePrice}, Size: ${tradeSize}`);
      }

      // 4. Ticker Updates (24h Volume)
      if (stream.endsWith('@ticker')) {
        const { v } = eventData as TickerUpdate;
        tickerVolume24h = v;
        console.log(`📉 24h Volume: ${tickerVolume24h}`);
      }
    } catch (err) {
      console.error('🚨 Failed to parse WebSocket message:', err);
    }
  });

  ws.on('error', (error) => {
    console.error('❌ WebSocket error:', error);
  });

  ws.on('close', () => {
    console.log('⚠️ WebSocket connection closed. Reconnecting in 5 seconds...');
    setTimeout(startWebSocket, 5000);
  });
};

startWebSocket();