# Crypto Bias Dashboard

Static dashboard for perpetual futures using only technical analysis and price action.

## What it does

- Pulls live product universe from `https://api.india.delta.exchange/v2/products`
- Pulls ticker data from `https://api.india.delta.exchange/v2/tickers`
- Pulls OHLC candles from `https://api.india.delta.exchange/v2/history/candles`
- Scores each pair across `15m`, `1h`, `4h`, `1d`, `1w`, and `1M`
- Uses a short-lived live snapshot so the page opens quickly while still tracking current market movement
- Classifies each pair as `Strong Bullish`, `Bullish`, `Neutral`, `Bearish`, or `Strong Bearish`

## Bias model

The score is built from:

- EMA trend stack: `20 / 50 / 200`
- RSI regime: `14`
- ADX trend strength: `14`
- Swing structure: higher highs and higher lows / lower highs and lower lows
- Breakout location: position inside the recent range and range breaks

Higher timeframes have higher weight.

## Constraint

No technical-analysis dashboard can deliver literal `100% accuracy`. This project is deterministic and auditable, but market outcomes remain probabilistic.

## Run

Start the local proxy server:

```powershell
node server.js
```

Then open:

`http://localhost:3000`

Do not open the HTML file with `file://...`. The dashboard uses a local `/api` proxy so the browser does not need direct upstream access.

## Vercel

This project now includes a serverless endpoint at `api/dashboard.js` and a `vercel.json` config. Static files can be deployed directly, and Vercel will serve the dashboard API as a Node function.
