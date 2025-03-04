import axios from 'axios';
import WebSocket from 'ws';
import fs from 'fs';
import { createObjectCsvWriter } from 'csv-writer';

// --------------------
// Configuration
// --------------------
const SYMBOL = 'BTCUSDT'; // symbol in uppercase for REST; lowercase for WS URL below
const symbolLower = SYMBOL.toLowerCase();
const MAX_LVL_EXPORT = 10; // number of levels to output

// Combined futures streams:
// - Depth updates at 100ms
// - Mark price updates (1s) (includes index price and funding rate)
// - Trade updates (for trade side/price/size)
// - Ticker updates (for 24h volume)
const BINANCE_WS_URL = `wss://fstream.binance.com/stream?streams=${symbolLower}@depth@100ms/${symbolLower}@markPrice@1s/${symbolLower}@trade/${symbolLower}@ticker`;

// REST endpoint for snapshot and open interest
const SNAPSHOT_URL = `https://fapi.binance.com/fapi/v1/depth?symbol=${SYMBOL}&limit=1000`;
const OPEN_INTEREST_URL = `https://fapi.binance.com/fapi/v1/openInterest?symbol=${SYMBOL}`;

// CSV file configuration
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

if (!fs.existsSync(csvFilePath) || fs.statSync(csvFilePath).size === 0) {
  fs.writeFileSync(csvFilePath, headers.join(',') + '\n');
}

// --------------------
// Interfaces
// --------------------
interface OrderBookSnapshot {
  lastUpdateId: number;
  bids: [string, string][];
  asks: [string, string][];
}

interface DepthUpdate {
  e: string; // event type ("depthUpdate")
  E: number; // event time
  s: string; // symbol
  U: number; // first update ID in event
  u: number; // final update ID in event
  b: [string, string][]; // bids: [price, quantity]
  a: [string, string][]; // asks: [price, quantity]
}

interface MarkPriceUpdate {
  e: string; // event type
  E: number; // event time
  s: string; // symbol
  p: string; // mark price
  i: string; // index price
  r: string; // funding rate
  T: number; // next funding time
}

interface TradeUpdate {
  e: string; // event type
  E: number; // event time
  s: string; // symbol
  t: number; // trade ID
  p: string; // trade price
  q: string; // trade quantity
  b: number; // buyer order ID
  a: number; // seller order ID
  m: boolean; // if true, buyer is market maker (indicates sell)
}

interface TickerUpdate {
  e: string; // event type
  E: number; // event time
  s: string; // symbol
  v: string; // 24h volume of base asset
}

// --------------------
// Global Variables
// --------------------
let lastUpdateId = 0;
let orderBook = {
  bids: new Map<number, number>(), // price -> quantity
  asks: new Map<number, number>()
};
let depthUpdatesBuffer: DepthUpdate[] = []; // buffer updates if snapshot not ready

// Global market data from WS
let markPrice = '';
let indexPrice = '';
let fundingRate = '';
let tradeSide = ''; // "Buy (Bid)" or "Sell (Ask)"
let tradePrice = '';
let tradeSize = '';
let tickerVolume24h = '';
let openInterest = '';

// --------------------
// Functions
// --------------------

// Get initial order book snapshot via REST
async function getOrderBookSnapshot(): Promise<OrderBookSnapshot> {
  const response = await axios.get<OrderBookSnapshot>(SNAPSHOT_URL);
  return response.data;
}

// Initialize local order book from snapshot
async function initOrderBook(): Promise<void> {
  const snapshot = await getOrderBookSnapshot();
  lastUpdateId = snapshot.lastUpdateId;
  snapshot.bids.forEach(bid => {
    orderBook.bids.set(parseFloat(bid[0]), parseFloat(bid[1]));
  });
  snapshot.asks.forEach(ask => {
    orderBook.asks.set(parseFloat(ask[0]), parseFloat(ask[1]));
  });
  console.log(`Snapshot loaded with lastUpdateId: ${lastUpdateId}`);
}

// Process a depth update
function processDepthUpdate(update: DepthUpdate): void {
  // Binance recommends ignoring any update where u <= lastUpdateId.
  if (update.u <= lastUpdateId) return;

  // Only process update if update.U <= lastUpdateId+1 <= update.u
  if (update.U <= lastUpdateId + 1 && update.u >= lastUpdateId + 1) {
    update.b.forEach(([priceStr, qtyStr]) => {
      const price = parseFloat(priceStr);
      const qty = parseFloat(qtyStr);
      if (qty === 0) {
        orderBook.bids.delete(price);
      } else {
        orderBook.bids.set(price, qty);
      }
    });
    update.a.forEach(([priceStr, qtyStr]) => {
      const price = parseFloat(priceStr);
      const qty = parseFloat(qtyStr);
      if (qty === 0) {
        orderBook.asks.delete(price);
      } else {
        orderBook.asks.set(price, qty);
      }
    });
    lastUpdateId = update.u;
  }
}

// Build a CSV row from current order book and market data
function buildCsvRow(timestamp: number): Record<string, string> {
  // Get sorted asks (ascending by price) and bids (descending by price)
  const asksArr = Array.from(orderBook.asks.entries()).sort((a, b) => a[0] - b[0]).slice(0, MAX_LVL_EXPORT);
  const bidsArr = Array.from(orderBook.bids.entries()).sort((a, b) => b[0] - a[0]).slice(0, MAX_LVL_EXPORT);

  let askCumulative = 0;
  let bidCumulative = 0;
  const row: Record<string, string> = {
    'Last Update': new Date(timestamp).toLocaleString(),
    'Coin': SYMBOL,
  };

  asksArr.forEach((ask, i) => {
    const [price, qty] = ask;
    askCumulative += qty;
    row[`Ask L${i + 1} Price`] = price.toFixed(2);
    row[`Ask L${i + 1} Size`] = qty.toFixed(4);
    row[`Ask L${i + 1} Cumulative BTC`] = askCumulative.toFixed(4);
  });
  // If less than MAX_LVL_EXPORT, fill remaining with empty strings
  for (let i = asksArr.length; i < MAX_LVL_EXPORT; i++) {
    row[`Ask L${i + 1} Price`] = '';
    row[`Ask L${i + 1} Size`] = '';
    row[`Ask L${i + 1} Cumulative BTC`] = '';
  }

  bidsArr.forEach((bid, i) => {
    const [price, qty] = bid;
    bidCumulative += qty;
    row[`Bid L${i + 1} Price`] = price.toFixed(2);
    row[`Bid L${i + 1} Size`] = qty.toFixed(4);
    row[`Bid L${i + 1} Cumulative BTC`] = bidCumulative.toFixed(4);
  });
  for (let i = bidsArr.length; i < MAX_LVL_EXPORT; i++) {
    row[`Bid L${i + 1} Price`] = '';
    row[`Bid L${i + 1} Size`] = '';
    row[`Bid L${i + 1} Cumulative BTC`] = '';
  }

  // Append global market data
  row['Mark Price'] = markPrice || 'N/A';
  row['Index Price'] = indexPrice || 'N/A';
  row['Funding Rate'] = fundingRate || 'N/A';
  row['Open Interest'] = openInterest || 'N/A';
  row['24h Volume'] = tickerVolume24h || 'N/A';
  row['Trade Side'] = tradeSide || '';
  row['Trade Price'] = tradePrice || '';
  row['Trade Size'] = tradeSize || '';

  return row;
}

// Poll open interest via REST every 30 seconds
async function pollOpenInterest(): Promise<void> {
  try {
    const response = await axios.get(OPEN_INTEREST_URL);
    openInterest = response.data.openInterest;
    console.log(`📊 Open Interest: ${openInterest}`);
  } catch (error) {
    console.error('🚨 Error fetching open interest:', error);
  }
}
setInterval(pollOpenInterest, 30000);
pollOpenInterest();

// --------------------
// WebSocket Connection and Sync
// --------------------
async function start() {
  // 1. Initialize local order book snapshot
  await initOrderBook();
  // Process any buffered updates
  depthUpdatesBuffer.forEach(update => processDepthUpdate(update));
  depthUpdatesBuffer = [];
  console.log('Local order book initialized.');

  // 2. Open WebSocket connection for combined streams
  const ws = new WebSocket(BINANCE_WS_URL);

  ws.on('open', () => {
    console.log('✅ Binance Futures WebSocket connected.');
  });

  ws.on('message', async (data: WebSocket.Data) => {
    try {
      const parsedData = JSON.parse(data.toString());
      const { stream, data: eventData } = parsedData;

      // Process depth updates
      if (stream && stream.endsWith('@depth@100ms')) {
        const depthUpdate = eventData as DepthUpdate;
        if (lastUpdateId === 0) {
          // Buffer updates until snapshot is loaded
          depthUpdatesBuffer.push(depthUpdate);
        } else {
          processDepthUpdate(depthUpdate);
          // After processing, build a CSV row using current timestamp
          const row = buildCsvRow(depthUpdate.E);
          console.log('✅ Writing CSV row:', row);
          await csvWriter.writeRecords([row]);
        }
      }
      // Process markPrice updates (includes index price & funding rate)
      if (stream && stream.endsWith('@markPrice@1s')) {
        const mpUpdate = eventData as MarkPriceUpdate;
        markPrice = mpUpdate.p;
        indexPrice = mpUpdate.i;
        fundingRate = mpUpdate.r;
        console.log(`📈 Mark Price: ${markPrice}, Index Price: ${indexPrice}, Funding Rate: ${fundingRate}`);
      }
      // Process trade updates
      if (stream && stream.endsWith('@trade')) {
        const tradeUpdate = eventData as TradeUpdate;
        tradePrice = tradeUpdate.p;
        tradeSize = tradeUpdate.q;
        tradeSide = tradeUpdate.m ? 'Sell (Ask)' : 'Buy (Bid)';
        console.log(`💰 Trade: Side: ${tradeSide}, Price: ${tradePrice}, Size: ${tradeSize}`);
      }
      // Process ticker updates (24h volume)
      if (stream && stream.endsWith('@ticker')) {
        const tickerUpdate = eventData as TickerUpdate;
        tickerVolume24h = tickerUpdate.v;
        console.log(`📉 24h Volume: ${tickerVolume24h}`);
      }
    } catch (err) {
      console.error('🚨 Error processing WebSocket message:', err);
    }
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket error:', err);
  });

  ws.on('close', () => {
    console.log('⚠️ WebSocket closed. Reconnecting in 5 seconds...');
    setTimeout(start, 5000);
  });
}

start();