const { URL, URLSearchParams } = require("url");

const DELTA_BASE = "https://api.india.delta.exchange/v2";
const PRIORITY_ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "LTC"];
const LEADERBOARD_CONCURRENCY = 18;
const SNAPSHOT_TTL_MS = 15_000;
const MIN_SETUP_RATIO = 5;
const LOCAL_SETUP_BARS = 16;
const LOCAL_STOP_BARS = 8;
const STRUCTURE_BARS = 24;

const SOURCE_SERIES = [
  { key: "15m", resolution: "15m", candles: 220 },
  { key: "1h", resolution: "1h", candles: 480 },
  { key: "4h", resolution: "4h", candles: 220 },
];

const TIMEFRAMES = [
  { key: "15m", source: "15m", weight: 4, minCandles: 60 },
  { key: "1h", source: "1h", weight: 5, minCandles: 60 },
  { key: "4h", source: "4h", weight: 3, minCandles: 60 },
  { key: "12h", source: "1h", aggregateDays: 12, weight: 2, minCandles: 20 },
  { key: "24h", source: "1h", aggregateDays: 24, weight: 1, minCandles: 14 },
];

let cachedSnapshot = null;
let cachedAt = 0;
let inFlightRefresh = null;

function withFreshness(snapshot) {
  return {
    ...snapshot,
    freshnessMs: cachedAt ? Math.max(0, Date.now() - cachedAt) : 0,
  };
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

async function fetchDelta(pathname, searchParams = new URLSearchParams()) {
  const upstreamUrl = new URL(`${DELTA_BASE}${pathname}`);
  upstreamUrl.search = searchParams.toString();
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
    throw new Error(text || "Delta returned a non-JSON response");
  }

  if (!upstream.ok) {
    throw new Error(body?.error?.message || `Delta request failed with ${upstream.status}`);
  }

  return body;
}

function getUnixRange(candleCount, resolution) {
  const secondsPerCandle = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
  }[resolution];

  const end = Math.floor(Date.now() / 1000);
  const start = end - candleCount * secondsPerCandle;
  return { start, end };
}

function normalizeCandles(raw) {
  return (Array.isArray(raw) ? raw : [])
    .map((candle) => ({
      time: Number(candle.time ?? candle[0]),
      open: Number(candle.open ?? candle[1]),
      high: Number(candle.high ?? candle[2]),
      low: Number(candle.low ?? candle[3]),
      close: Number(candle.close ?? candle[4]),
    }))
    .filter((candle) => Number.isFinite(candle.close))
    .sort((left, right) => left.time - right.time);
}

function aggregateCandles(candles, groupSize) {
  if (!groupSize || candles.length === 0) {
    return candles;
  }

  const aggregated = [];

  for (let index = 0; index < candles.length; index += groupSize) {
    const chunk = candles.slice(index, index + groupSize);
    if (chunk.length === 0) {
      continue;
    }

    aggregated.push({
      time: chunk.at(-1).time,
      open: chunk[0].open,
      high: Math.max(...chunk.map((candle) => candle.high)),
      low: Math.min(...chunk.map((candle) => candle.low)),
      close: chunk.at(-1).close,
    });
  }

  return aggregated;
}

function ema(values, period) {
  if (values.length < period) {
    return [];
  }

  const multiplier = 2 / (period + 1);
  const seed = values.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
  const result = Array(period - 1).fill(null);
  result.push(seed);

  for (let index = period; index < values.length; index += 1) {
    result.push((values[index] - result[index - 1]) * multiplier + result[index - 1]);
  }

  return result;
}

function rsi(values, period = 14) {
  if (values.length <= period) {
    return [];
  }

  let gains = 0;
  let losses = 0;
  for (let index = 1; index <= period; index += 1) {
    const change = values[index] - values[index - 1];
    if (change >= 0) {
      gains += change;
    } else {
      losses -= change;
    }
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;
  const output = Array(period).fill(null);
  let rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
  output.push(100 - 100 / (1 + rs));

  for (let index = period + 1; index < values.length; index += 1) {
    const change = values[index] - values[index - 1];
    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    output.push(100 - 100 / (1 + rs));
  }

  return output;
}

function adx(candles, period = 14) {
  if (candles.length <= period * 2) {
    return [];
  }

  const trueRanges = [];
  const plusDMs = [];
  const minusDMs = [];

  for (let index = 1; index < candles.length; index += 1) {
    const current = candles[index];
    const previous = candles[index - 1];
    const upMove = current.high - previous.high;
    const downMove = previous.low - current.low;

    plusDMs.push(upMove > downMove && upMove > 0 ? upMove : 0);
    minusDMs.push(downMove > upMove && downMove > 0 ? downMove : 0);
    trueRanges.push(
      Math.max(
        current.high - current.low,
        Math.abs(current.high - previous.close),
        Math.abs(current.low - previous.close),
      ),
    );
  }

  let tr14 = trueRanges.slice(0, period).reduce((sum, value) => sum + value, 0);
  let plus14 = plusDMs.slice(0, period).reduce((sum, value) => sum + value, 0);
  let minus14 = minusDMs.slice(0, period).reduce((sum, value) => sum + value, 0);
  const dxValues = [];

  for (let index = period; index < trueRanges.length; index += 1) {
    if (index > period) {
      tr14 = tr14 - tr14 / period + trueRanges[index];
      plus14 = plus14 - plus14 / period + plusDMs[index];
      minus14 = minus14 - minus14 / period + minusDMs[index];
    }

    const plusDI = tr14 === 0 ? 0 : (plus14 / tr14) * 100;
    const minusDI = tr14 === 0 ? 0 : (minus14 / tr14) * 100;
    const denominator = plusDI + minusDI;
    dxValues.push(denominator === 0 ? 0 : (Math.abs(plusDI - minusDI) / denominator) * 100);
  }

  if (dxValues.length < period) {
    return [];
  }

  const output = Array(period * 2).fill(null);
  let adxValue = dxValues.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
  output.push(adxValue);

  for (let index = period; index < dxValues.length; index += 1) {
    adxValue = ((adxValue * (period - 1)) + dxValues[index]) / period;
    output.push(adxValue);
  }

  return output;
}

function averageTrueRange(candles, period = 14) {
  if (candles.length <= period) {
    return null;
  }

  const ranges = [];

  for (let index = 1; index < candles.length; index += 1) {
    const current = candles[index];
    const previous = candles[index - 1];
    ranges.push(
      Math.max(
        current.high - current.low,
        Math.abs(current.high - previous.close),
        Math.abs(current.low - previous.close),
      ),
    );
  }

  const recentRanges = ranges.slice(-period);
  if (recentRanges.length === 0) {
    return null;
  }

  return recentRanges.reduce((sum, value) => sum + value, 0) / recentRanges.length;
}

function findPivots(values, lookback = 3) {
  const highs = [];
  const lows = [];

  for (let index = lookback; index < values.length - lookback; index += 1) {
    const sample = values.slice(index - lookback, index + lookback + 1);
    if (values[index] === Math.max(...sample)) {
      highs.push(values[index]);
    }
    if (values[index] === Math.min(...sample)) {
      lows.push(values[index]);
    }
  }

  return { highs, lows };
}

function roundPriceLevel(value) {
  if (!Number.isFinite(value)) {
    return null;
  }

  return Number(value.toFixed(8));
}

function emptySetup(side) {
  return {
    side,
    ratio: 0,
    qualifies: false,
    minimumRatio: MIN_SETUP_RATIO,
    risk: null,
    reward: null,
    stop: null,
    target: null,
  };
}

function evaluateSetupPotential(side, entry, localCandles, structureCandles) {
  if (!Number.isFinite(entry) || localCandles.length < LOCAL_SETUP_BARS || structureCandles.length < STRUCTURE_BARS) {
    return emptySetup(side);
  }

  const localWindow = localCandles.slice(-LOCAL_SETUP_BARS);
  const stopWindow = localCandles.slice(-LOCAL_STOP_BARS);
  const structureWindow = structureCandles.slice(-STRUCTURE_BARS);
  const atrValue = averageTrueRange(localCandles, 14);
  const localHigh = Math.max(...localWindow.map((candle) => candle.high));
  const localLow = Math.min(...localWindow.map((candle) => candle.low));
  const structureHigh = Math.max(...structureWindow.map((candle) => candle.high));
  const structureLow = Math.min(...structureWindow.map((candle) => candle.low));
  const localRange = localHigh - localLow;
  const structureRange = structureHigh - structureLow;
  const bufferUnit = Math.max(localRange * 0.8, structureRange * 0.2, (atrValue ?? 0) * 3, entry * 0.01);
  const stopPadding = Math.max((atrValue ?? localRange ?? 0) * 0.25, entry * 0.001);

  if (!Number.isFinite(bufferUnit) || bufferUnit <= 0 || !Number.isFinite(stopPadding) || stopPadding <= 0) {
    return emptySetup(side);
  }

  if (side === "long") {
    const breakoutLevel = Math.max(...localWindow.slice(0, -1).map((candle) => candle.high));
    const pivotLows = findPivots(localWindow.map((candle) => candle.low), 2).lows.filter((value) => value < entry);
    const swingLow = pivotLows.at(-1) ?? Math.min(...stopWindow.map((candle) => candle.low));
    const stop = swingLow - stopPadding;
    const targetBase = Math.max(structureHigh, breakoutLevel + bufferUnit);
    const target = entry >= breakoutLevel * 0.992
      ? Math.max(targetBase, structureHigh + localRange)
      : targetBase;
    const risk = entry - stop;
    const reward = target - entry;
    const ratio = reward > 0 && risk > 0 ? reward / risk : 0;

    return {
      side,
      ratio: Number(ratio.toFixed(2)),
      qualifies: ratio >= MIN_SETUP_RATIO,
      minimumRatio: MIN_SETUP_RATIO,
      risk: roundPriceLevel(risk),
      reward: roundPriceLevel(reward),
      stop: roundPriceLevel(stop),
      target: roundPriceLevel(target),
    };
  }

  const breakdownLevel = Math.min(...localWindow.slice(0, -1).map((candle) => candle.low));
  const pivotHighs = findPivots(localWindow.map((candle) => candle.high), 2).highs.filter((value) => value > entry);
  const swingHigh = pivotHighs.at(-1) ?? Math.max(...stopWindow.map((candle) => candle.high));
  const stop = swingHigh + stopPadding;
  const targetBase = Math.min(structureLow, breakdownLevel - bufferUnit);
  const target = entry <= breakdownLevel * 1.008
    ? Math.min(targetBase, structureLow - localRange)
    : targetBase;
  const risk = stop - entry;
  const reward = entry - target;
  const ratio = reward > 0 && risk > 0 ? reward / risk : 0;

  return {
    side,
    ratio: Number(ratio.toFixed(2)),
    qualifies: ratio >= MIN_SETUP_RATIO,
    minimumRatio: MIN_SETUP_RATIO,
    risk: roundPriceLevel(risk),
    reward: roundPriceLevel(reward),
    stop: roundPriceLevel(stop),
    target: roundPriceLevel(target),
  };
}

function scoreStructure(candles) {
  const highs = findPivots(candles.map((candle) => candle.high)).highs.slice(-2);
  const lows = findPivots(candles.map((candle) => candle.low)).lows.slice(-2);

  if (highs.length < 2 || lows.length < 2) {
    return 0;
  }

  const higherHigh = highs[1] > highs[0];
  const lowerHigh = highs[1] < highs[0];
  const higherLow = lows[1] > lows[0];
  const lowerLow = lows[1] < lows[0];

  if (higherHigh && higherLow) {
    return 25;
  }
  if (lowerHigh && lowerLow) {
    return -25;
  }
  if (higherHigh || higherLow) {
    return 10;
  }
  if (lowerHigh || lowerLow) {
    return -10;
  }

  return 0;
}

function scoreBreakout(candles, period = 20) {
  if (candles.length < period + 1) {
    return 0;
  }

  const recent = candles.slice(-period - 1, -1);
  const close = candles.at(-1).close;
  const highestHigh = Math.max(...recent.map((candle) => candle.high));
  const lowestLow = Math.min(...recent.map((candle) => candle.low));
  const range = highestHigh - lowestLow || 1;

  if (close > highestHigh) {
    return 18;
  }
  if (close < lowestLow) {
    return -18;
  }

  return ((close - lowestLow) / range - 0.5) * 24;
}

function pushComponent(components, label, points, detail) {
  components.push({
    label,
    points,
    detail,
  });
}

function evaluateTimeframe(candles, minCandles = 60) {
  if (candles.length < minCandles) {
    return {
      label: "Neutral",
      score: 0,
      trendLabel: "Range",
      close: candles.at(-1)?.close ?? null,
      components: [
        { label: "Data", points: 0, detail: "Not enough candles" },
      ],
    };
  }

  const closes = candles.map((candle) => candle.close);
  const shortPeriod = Math.min(20, Math.max(10, Math.floor(candles.length / 6)));
  const mediumPeriod = Math.min(50, Math.max(20, Math.floor(candles.length / 3)));
  const longPeriod = Math.min(200, Math.max(50, Math.floor(candles.length * 0.65)));
  const ema20 = ema(closes, shortPeriod).at(-1);
  const ema50 = ema(closes, mediumPeriod).at(-1);
  const ema200 = ema(closes, longPeriod).at(-1);
  const rsi14 = rsi(closes, 14).at(-1) ?? 50;
  const adx14 = adx(candles, 14).at(-1) ?? 15;
  const latestClose = closes.at(-1);
  let score = 0;
  const components = [];

  if (latestClose > ema20) {
    score += 10;
    pushComponent(components, "Price vs EMA 20", 10, "Above EMA 20");
  } else {
    score -= 10;
    pushComponent(components, "Price vs EMA 20", -10, "Below EMA 20");
  }

  if (ema20 > ema50) {
    score += 8;
    pushComponent(components, "EMA 20 vs EMA 50", 8, "EMA 20 above EMA 50");
  } else {
    score -= 8;
    pushComponent(components, "EMA 20 vs EMA 50", -8, "EMA 20 below EMA 50");
  }

  if (ema50 > ema200) {
    score += 12;
    pushComponent(components, "EMA 50 vs EMA 200", 12, "EMA 50 above EMA 200");
  } else {
    score -= 12;
    pushComponent(components, "EMA 50 vs EMA 200", -12, "EMA 50 below EMA 200");
  }

  if (rsi14 >= 60) {
    score += 16;
    pushComponent(components, "RSI", 16, `Strong at ${Math.round(rsi14)}`);
  } else if (rsi14 >= 52) {
    score += 8;
    pushComponent(components, "RSI", 8, `Constructive at ${Math.round(rsi14)}`);
  } else if (rsi14 <= 40) {
    score -= 16;
    pushComponent(components, "RSI", -16, `Weak at ${Math.round(rsi14)}`);
  } else if (rsi14 <= 48) {
    score -= 8;
    pushComponent(components, "RSI", -8, `Soft at ${Math.round(rsi14)}`);
  } else {
    pushComponent(components, "RSI", 0, `Neutral at ${Math.round(rsi14)}`);
  }

  if (adx14 >= 25) {
    const adxPoints = score >= 0 ? 8 : -8;
    score += adxPoints;
    pushComponent(components, "ADX", adxPoints, `Trend strength at ${Math.round(adx14)}`);
  } else {
    pushComponent(components, "ADX", 0, `Weak at ${Math.round(adx14)}`);
  }

  const structureScore = scoreStructure(candles);
  const breakoutScore = scoreBreakout(candles);

  score += structureScore;
  score += breakoutScore;

  if (structureScore >= 20) {
    pushComponent(components, "Structure", structureScore, "Higher highs and higher lows");
  } else if (structureScore <= -20) {
    pushComponent(components, "Structure", structureScore, "Lower highs and lower lows");
  } else if (structureScore > 0) {
    pushComponent(components, "Structure", structureScore, "Improving swing structure");
  } else if (structureScore < 0) {
    pushComponent(components, "Structure", structureScore, "Weakening swing structure");
  } else {
    pushComponent(components, "Structure", 0, "No clear edge");
  }

  if (breakoutScore >= 12) {
    pushComponent(components, "Breakout", Math.round(breakoutScore), "Near range breakout");
  } else if (breakoutScore <= -12) {
    pushComponent(components, "Breakout", Math.round(breakoutScore), "Near range breakdown");
  } else {
    pushComponent(components, "Breakout", Math.round(breakoutScore), "Inside recent range");
  }

  score = clamp(Math.round(score), -100, 100);

  let label = "Neutral";
  if (score >= 45) {
    label = "Bullish";
  } else if (score <= -45) {
    label = "Bearish";
  }

  let trendLabel = "Range";
  if (ema20 > ema50 && ema50 > ema200) {
    trendLabel = adx14 >= 25 ? "Trend Up" : "Up";
  } else if (ema20 < ema50 && ema50 < ema200) {
    trendLabel = adx14 >= 25 ? "Trend Down" : "Down";
  }

  return { label, score, trendLabel, close: latestClose, components };
}

function summarizeBias(score) {
  if (score >= 60) {
    return "Strong Bullish";
  }
  if (score >= 20) {
    return "Bullish";
  }
  if (score <= -60) {
    return "Strong Bearish";
  }
  if (score <= -20) {
    return "Bearish";
  }

  return "Neutral";
}

function tradeReadiness(weightedScore, timeframes, activeSetup) {
  const bullish15m = timeframes["15m"] === "Bullish";
  const bullish1h = timeframes["1h"] === "Bullish";
  const bullish4h = timeframes["4h"] === "Bullish";
  const bearish15m = timeframes["15m"] === "Bearish";
  const bearish1h = timeframes["1h"] === "Bearish";
  const bearish4h = timeframes["4h"] === "Bearish";
  const bearish12h = timeframes["12h"] === "Bearish";
  const bullish12h = timeframes["12h"] === "Bullish";

  if (Math.abs(weightedScore) < 20) {
    return "Avoid";
  }

  if (bullish15m && bullish1h && !bearish4h && !bearish12h && weightedScore >= 30) {
    if (!activeSetup || activeSetup.side !== "long" || !activeSetup.qualifies) {
      return "Wait";
    }
    return bullish4h ? "Ready Long" : "Long Watch";
  }

  if (bearish15m && bearish1h && !bullish4h && !bullish12h && weightedScore <= -30) {
    if (!activeSetup || activeSetup.side !== "short" || !activeSetup.qualifies) {
      return "Wait";
    }
    return bearish4h ? "Ready Short" : "Short Watch";
  }

  return "Wait";
}

function readinessRank(label) {
  if (label === "Ready Long" || label === "Ready Short") {
    return 3;
  }
  if (label === "Long Watch" || label === "Short Watch") {
    return 2;
  }
  if (label === "Wait") {
    return 1;
  }

  return 0;
}

function dominantTrend(trends) {
  const up = trends.filter((trend) => trend.includes("Up")).length;
  const down = trends.filter((trend) => trend.includes("Down")).length;

  if (up > down) {
    return "Uptrend";
  }
  if (down > up) {
    return "Downtrend";
  }

  return "Range";
}

async function fetchCandles(symbol, resolution, candleCount) {
  const { start, end } = getUnixRange(candleCount, resolution);
  const params = new URLSearchParams({
    symbol,
    resolution,
    start: String(start),
    end: String(end),
  });

  const data = await fetchDelta("/history/candles", params);
  return normalizeCandles(data.result);
}

function tickerLiquidityScore(ticker) {
  return Number(
    ticker?.turnover_24h ??
    ticker?.volume ??
    ticker?.volume_24h ??
    ticker?.open_interest ??
    0,
  );
}

async function asyncPool(items, limit, worker) {
  const results = [];
  const executing = new Set();

  for (const item of items) {
    const promise = Promise.resolve().then(() => worker(item));
    results.push(promise);
    executing.add(promise);
    promise.finally(() => executing.delete(promise));

    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }

  return Promise.allSettled(results);
}

async function buildRow(ticker) {
  const sourceSeries = await Promise.all(
    SOURCE_SERIES.map(async (series) => ({
      key: series.key,
      candles: await fetchCandles(ticker.symbol, series.resolution, series.candles),
    })),
  );

  const sourceMap = Object.fromEntries(sourceSeries.map((series) => [series.key, series.candles]));
  const timeframeResults = {};

  for (const timeframe of TIMEFRAMES) {
    const baseCandles = sourceMap[timeframe.source] || [];
    const preparedCandles = timeframe.aggregateDays
      ? aggregateCandles(baseCandles, timeframe.aggregateDays)
      : baseCandles;
    timeframeResults[timeframe.key] = evaluateTimeframe(preparedCandles, timeframe.minCandles);
  }

  const weightedScore = Math.round(
    TIMEFRAMES.reduce((sum, timeframe) => sum + timeframeResults[timeframe.key].score * timeframe.weight, 0) /
    TIMEFRAMES.reduce((sum, timeframe) => sum + timeframe.weight, 0),
  );
  const price = ticker?.close ?? ticker?.mark_price ?? timeframeResults["15m"].close ?? null;
  const timeframes = Object.fromEntries(TIMEFRAMES.map((timeframe) => [timeframe.key, timeframeResults[timeframe.key].label]));
  const scoreBreakdown = TIMEFRAMES.map((timeframe) => ({
    timeframe: timeframe.key,
    score: timeframeResults[timeframe.key].score,
    bias: timeframeResults[timeframe.key].label,
    components: timeframeResults[timeframe.key].components,
  }));
  const longSetup = evaluateSetupPotential("long", price, sourceMap["1h"] || [], sourceMap["4h"] || []);
  const shortSetup = evaluateSetupPotential("short", price, sourceMap["1h"] || [], sourceMap["4h"] || []);
  const activeSetup = weightedScore >= 0 ? longSetup : shortSetup;

  return {
    symbol: ticker.symbol,
    underlying: ticker.underlying_asset_symbol || ticker.symbol.replace(/USD$|INR$/i, ""),
    description: ticker.description || "Perpetual futures",
    score: weightedScore,
    bias: summarizeBias(weightedScore),
    trend: dominantTrend(TIMEFRAMES.map((timeframe) => timeframeResults[timeframe.key].trendLabel)),
    price,
    liquidity: tickerLiquidityScore(ticker),
    fundingRate: ticker?.funding_rate ?? null,
    change24h: ticker?.ltp_change_24h ?? ticker?.mark_change_24h ?? null,
    timeframes,
    scoreBreakdown,
    setup: activeSetup,
    setups: {
      long: longSetup,
      short: shortSetup,
    },
    tradeReadiness: tradeReadiness(
      weightedScore,
      timeframes,
      activeSetup,
    ),
  };
}

async function buildDashboardSnapshot() {
  const tickersResponse = await fetchDelta("/tickers");
  const perpetualTickers = (tickersResponse.result || []).filter((ticker) =>
    ticker?.contract_type === "perpetual_futures" &&
    typeof ticker?.symbol === "string" &&
    ticker?.product_trading_status === "operational",
  );

  const settled = await asyncPool(perpetualTickers, LEADERBOARD_CONCURRENCY, buildRow);
  const rows = settled
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return right.liquidity - left.liquidity;
    })
    .map((row, index) => ({ ...row, rank: index + 1 }));

  const bullish = rows.filter((row) => row.bias.includes("Bull")).length;
  const bearish = rows.filter((row) => row.bias.includes("Bear")).length;
  const neutral = rows.length - bullish - bearish;
  const strongestBull = rows[0] || { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, rank: 0 };
  const strongestBear = [...rows].reverse().find((row) => row.score < 0) ||
    rows.at(-1) ||
    { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, rank: 0 };
  const strongest24hLeader = [...rows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(right.change24h) - Number(left.change24h))[0] ||
    strongestBull;
  const strongest24hLaggard = [...rows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(left.change24h) - Number(right.change24h))[0] ||
    strongestBear;
  const majorIndex = new Map(PRIORITY_ASSETS.map((asset, index) => [asset, index]));
  const majors = rows
    .filter((row) => majorIndex.has(String(row.underlying).toUpperCase()))
    .sort((left, right) => majorIndex.get(String(left.underlying).toUpperCase()) - majorIndex.get(String(right.underlying).toUpperCase()));

  const headline =
    strongestBull.score >= 25 || strongestBear.score <= -25
      ? `${strongestBull.symbol} is strongest while ${strongestBear.symbol} is weakest across the ranked perpetual universe.`
      : "Market structure is mixed across the full perpetual universe.";
  const qualifiedBullLeaders = rows
    .filter((row) =>
      row.score > 0 &&
      row.tradeReadiness !== "Avoid" &&
      row.setup?.side === "long" &&
      row.setup.qualifies,
    )
    .sort((left, right) => {
      if (readinessRank(right.tradeReadiness) !== readinessRank(left.tradeReadiness)) {
        return readinessRank(right.tradeReadiness) - readinessRank(left.tradeReadiness);
      }
      if (right.setup.ratio !== left.setup.ratio) {
        return right.setup.ratio - left.setup.ratio;
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 5);
  const qualifiedBearLeaders = rows
    .filter((row) =>
      row.score < 0 &&
      row.tradeReadiness !== "Avoid" &&
      row.setup?.side === "short" &&
      row.setup.qualifies,
    )
    .sort((left, right) => {
      if (readinessRank(right.tradeReadiness) !== readinessRank(left.tradeReadiness)) {
        return readinessRank(right.tradeReadiness) - readinessRank(left.tradeReadiness);
      }
      if (right.setup.ratio !== left.setup.ratio) {
        return right.setup.ratio - left.setup.ratio;
      }
      if (left.score !== right.score) {
        return left.score - right.score;
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 5);

  return {
    generatedAt: new Date().toISOString(),
    headline,
    minimumSetupRatio: MIN_SETUP_RATIO,
    breadth: {
      tracked: rows.length,
      bullish,
      bearish,
      neutral,
      bullishPct: rows.length ? Math.round((bullish / rows.length) * 100) : 0,
      bearishPct: rows.length ? Math.round((bearish / rows.length) * 100) : 0,
      neutralPct: rows.length ? Math.round((neutral / rows.length) * 100) : 0,
    },
    strongestBull,
    strongestBear,
    strongest24hLeader,
    strongest24hLaggard,
    majors,
    leaders: {
      bulls: qualifiedBullLeaders,
      bears: qualifiedBearLeaders,
    },
    allRows: rows,
  };
}

async function refreshSnapshot() {
  const snapshot = await buildDashboardSnapshot();
  cachedSnapshot = snapshot;
  cachedAt = Date.now();
  return cachedSnapshot;
}

function triggerBackgroundRefresh() {
  if (!inFlightRefresh) {
    inFlightRefresh = refreshSnapshot().finally(() => {
      inFlightRefresh = null;
    });
  }

  return inFlightRefresh;
}

async function getDashboardPayload(options = {}) {
  const { forceFresh = false } = options;
  const age = Date.now() - cachedAt;
  const hasFreshSnapshot = cachedSnapshot && age <= SNAPSHOT_TTL_MS;

  if (forceFresh) {
    const snapshot = await triggerBackgroundRefresh();
    return withFreshness(snapshot);
  }

  if (hasFreshSnapshot) {
    return withFreshness(cachedSnapshot);
  }

  if (cachedSnapshot) {
    triggerBackgroundRefresh();
    return withFreshness(cachedSnapshot);
  }

  const snapshot = await triggerBackgroundRefresh();
  return withFreshness(snapshot);
}

module.exports = {
  DELTA_BASE,
  getDashboardPayload,
};
