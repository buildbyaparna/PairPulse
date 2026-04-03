const { URL, URLSearchParams } = require("url");

const DELTA_BASE = "https://api.india.delta.exchange/v2";
const PRIORITY_ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "LTC"];
const LEADERBOARD_CONCURRENCY = 18;
const SNAPSHOT_TTL_MS = 15_000;
const MIN_SETUP_RATIO = 5;
const MIN_CONTINUATION_RATIO = 3;
const BEST_SETUP_DISPLAY_LIMIT = 5;
const LOCAL_SETUP_BARS = 16;
const STRUCTURE_BARS = 24;

const SOURCE_SERIES = [
  { key: "5m", resolution: "5m", candles: 360 },
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
const CORE_TIMEFRAME_KEYS = ["15m", "1h", "4h"];

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
    "5m": 5 * 60,
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
      volume: Number(candle.volume ?? candle[5] ?? 0),
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
      volume: chunk.reduce((sum, candle) => sum + Number(candle.volume || 0), 0),
    });
  }

  return aggregated;
}

function sma(values, period) {
  if (values.length < period) {
    return null;
  }

  const sample = values.slice(-period);
  return sample.reduce((sum, value) => sum + value, 0) / sample.length;
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

function formatSignedNumber(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return "N/A";
  }

  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

function dedupeLevels(levels, referencePrice) {
  const numericLevels = levels
    .map((level) => Number(level))
    .filter((level) => Number.isFinite(level) && level > 0)
    .sort((left, right) => left - right);

  const deduped = [];
  const tolerance = Math.max(Number(referencePrice || 0) * 0.0025, 0.00000001);

  for (const level of numericLevels) {
    if (deduped.length === 0 || Math.abs(level - deduped.at(-1)) > tolerance) {
      deduped.push(level);
    }
  }

  return deduped;
}

function candleStats(candle) {
  const range = Math.max(Number(candle?.high) - Number(candle?.low), 0.00000001);
  const body = Math.abs(Number(candle?.close) - Number(candle?.open));
  const upperWick = Number(candle?.high) - Math.max(Number(candle?.open), Number(candle?.close));
  const lowerWick = Math.min(Number(candle?.open), Number(candle?.close)) - Number(candle?.low);

  return {
    range,
    body,
    upperWick,
    lowerWick,
    closeLocation: (Number(candle?.close) - Number(candle?.low)) / range,
  };
}

function emptySetup(side) {
  return {
    side,
    ratio: 0,
    qualifies: false,
    minimumRatio: MIN_SETUP_RATIO,
    stopBasis: "Last 15m candle",
    risk: null,
    reward: null,
    stop: null,
    target: null,
    support: null,
    resistance: null,
    relativeVolume: null,
    volumeTrend: null,
    qualityScore: 0,
    checks: [],
  };
}

function emptyFrameContinuation(timeframe, detail = "Not enough candles") {
  return {
    timeframe,
    qualifies: false,
    score: 0,
    bos: false,
    choch: false,
    emaRetest: false,
    trendAligned: false,
    continuationReady: false,
    breakoutLevel: null,
    consolidationRange: null,
    consolidationBaseRange: null,
    expansionMultiple: null,
    sizeQualified: false,
    relativeVolume: null,
    volumeTrend: null,
    detail,
  };
}

function emptyContinuation(side) {
  return {
    side,
    qualifies: false,
    ratio: 0,
    minimumRatio: MIN_CONTINUATION_RATIO,
    score: 0,
    checks: [],
    frames: {
      "5m": emptyFrameContinuation("5m"),
      "15m": emptyFrameContinuation("15m"),
    },
  };
}

function formatRatioValue(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) {
    return "N/A";
  }

  return `1:${ratio.toFixed(Number.isInteger(ratio) ? 0 : 1)}`;
}

function evaluateFrameContinuation(side, candles, timeframe) {
  if (candles.length < 60) {
    return emptyFrameContinuation(timeframe);
  }

  const window = candles.slice(-64);
  const signal = window.at(-1);
  const pullbackWindow = window.slice(-8, -1);
  const impulseWindow = window.slice(-32, -8);

  if (!signal || pullbackWindow.length < 4 || impulseWindow.length < 12) {
    return emptyFrameContinuation(timeframe);
  }

  const closes = window.map((candle) => candle.close);
  const ema9Series = ema(closes, 9);
  const ema15Series = ema(closes, 15);
  const ema9 = ema9Series.at(-1);
  const ema15 = ema15Series.at(-1);
  const ema9Prev = ema9Series.at(-2);
  const ema15Prev = ema15Series.at(-2);

  if (
    !Number.isFinite(ema9) ||
    !Number.isFinite(ema15) ||
    !Number.isFinite(ema9Prev) ||
    !Number.isFinite(ema15Prev)
  ) {
    return emptyFrameContinuation(timeframe, "EMA data unavailable");
  }

  const atrSafe = Math.max(
    averageTrueRange(window, 14) ?? Number(signal.close || 0) * 0.001,
    Number(signal.close || 0) * 0.0008,
  );
  const previousVolumes = window.slice(-21, -1).map((item) => Number(item.volume || 0));
  const volumeAverage20 = sma(previousVolumes, 20) ?? sma(window.map((item) => Number(item.volume || 0)), 20) ?? 0;
  const currentVolume = Number(signal.volume || 0);
  const recentThreeVolume = sma(window.map((item) => Number(item.volume || 0)), 3) ?? currentVolume;
  const relativeVolume = volumeAverage20 > 0 ? currentVolume / volumeAverage20 : 0;
  const volumeTrend = volumeAverage20 > 0 ? recentThreeVolume / volumeAverage20 : 0;
  const candle = candleStats(signal);
  const consolidationWindow = pullbackWindow;
  const consolidationRanges = consolidationWindow.map((candleItem) => candleStats(candleItem).range);
  const pullbackHigh = Math.max(...pullbackWindow.map((candleItem) => candleItem.high));
  const pullbackLow = Math.min(...pullbackWindow.map((candleItem) => candleItem.low));
  const consolidationRange = pullbackHigh - pullbackLow;
  const consolidationAverageRange = sma(consolidationRanges, consolidationRanges.length) ?? candle.range;
  const consolidationBaseRange = Math.max(
    consolidationAverageRange,
    consolidationRange / Math.max(consolidationWindow.length, 1),
    Number(signal.close || 0) * 0.0002,
  );
  const expansionMultiple = consolidationBaseRange > 0 ? candle.range / consolidationBaseRange : 0;
  const sizeQualified = expansionMultiple >= 3;
  const pullbackDirection = Number(pullbackWindow.at(-1)?.close || 0) - Number(pullbackWindow[0]?.close || 0);
  const pullbackStructure = scoreStructure(window.slice(-12, -1));
  const trendStructure = scoreStructure(impulseWindow);
  const zonePadding = Math.max(atrSafe * 0.22, Number(signal.close || 0) * 0.0012);
  const bandLow = Math.min(ema9, ema15) - zonePadding;
  const bandHigh = Math.max(ema9, ema15) + zonePadding;
  const intersectsBand = signal.low <= bandHigh && signal.high >= bandLow;
  const closeAboveEmas = signal.close > ema9 && signal.close > ema15;
  const closeBelowEmas = signal.close < ema9 && signal.close < ema15;
  const microHigh = Math.max(...window.slice(-5, -1).map((candleItem) => candleItem.high));
  const microLow = Math.min(...window.slice(-5, -1).map((candleItem) => candleItem.low));

  let trendAligned = false;
  let bos = false;
  let choch = false;
  let emaRetest = false;
  let notExhausted = false;
  let breakoutLevel = null;

  if (side === "long") {
    trendAligned =
      ema9 > ema15 &&
      ema9 >= ema9Prev &&
      ema15 >= ema15Prev &&
      (trendStructure >= 10 || impulseWindow.at(-1).close >= impulseWindow[0].close);
    bos =
      signal.close > pullbackHigh + zonePadding * 0.15 &&
      signal.close > microHigh &&
      candle.closeLocation >= 0.6 &&
      candle.body >= candle.range * 0.38;
    choch = (pullbackStructure <= -10 || pullbackDirection < 0) && signal.close > microHigh;
    emaRetest =
      intersectsBand &&
      closeAboveEmas &&
      signal.low <= Math.max(ema9, ema15) + zonePadding;
    notExhausted =
      (signal.close - ema15) / atrSafe <= 1.65 &&
      candle.upperWick <= Math.max(candle.body * 0.9, candle.range * 0.22);
    breakoutLevel = pullbackHigh;
  } else {
    trendAligned =
      ema9 < ema15 &&
      ema9 <= ema9Prev &&
      ema15 <= ema15Prev &&
      (trendStructure <= -10 || impulseWindow.at(-1).close <= impulseWindow[0].close);
    bos =
      signal.close < pullbackLow - zonePadding * 0.15 &&
      signal.close < microLow &&
      candle.closeLocation <= 0.4 &&
      candle.body >= candle.range * 0.38;
    choch = (pullbackStructure >= 10 || pullbackDirection > 0) && signal.close < microLow;
    emaRetest =
      intersectsBand &&
      closeBelowEmas &&
      signal.high >= Math.min(ema9, ema15) - zonePadding;
    notExhausted =
      (ema15 - signal.close) / atrSafe <= 1.65 &&
      candle.lowerWick <= Math.max(candle.body * 0.9, candle.range * 0.22);
    breakoutLevel = pullbackLow;
  }

  const checks = [
    {
      label: `${timeframe} trend`,
      passed: trendAligned,
      score: trendAligned ? 100 : 28,
      detail: `EMA9 ${roundPriceLevel(ema9)} | EMA15 ${roundPriceLevel(ema15)} | structure ${trendStructure}`,
    },
    {
      label: `${timeframe} BOS`,
      passed: bos,
      score: bos ? 100 : 20,
      detail: `${side === "long" ? "Breakout" : "Breakdown"} ${roundPriceLevel(breakoutLevel)} | close ${(candle.closeLocation * 100).toFixed(0)}%`,
    },
    {
      label: `${timeframe} CHoCH`,
      passed: choch,
      score: choch ? 100 : 22,
      detail: `Pullback structure ${pullbackStructure} | drift ${formatSignedNumber(pullbackDirection / atrSafe)}`,
    },
    {
      label: `${timeframe} EMA 9/15`,
      passed: emaRetest,
      score: emaRetest ? 100 : 18,
      detail: `EMA9 ${roundPriceLevel(ema9)} | EMA15 ${roundPriceLevel(ema15)} | close ${roundPriceLevel(signal.close)}`,
    },
    {
      label: `${timeframe} size`,
      passed: sizeQualified,
      score: clamp(Math.round((expansionMultiple / 3) * 100), 0, 100),
      detail: `Signal ${expansionMultiple.toFixed(2)}x vs last consolidation base`,
    },
    {
      label: `${timeframe} extension`,
      passed: notExhausted,
      score: notExhausted ? 100 : 20,
      detail: `${side === "long" ? "Above" : "Below"} EMA15 ${formatSignedNumber(side === "long" ? (signal.close - ema15) / atrSafe : (ema15 - signal.close) / atrSafe)} ATR`,
    },
  ];
  const score = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);
  const continuationReady = trendAligned && bos && choch && emaRetest && sizeQualified && notExhausted;

  return {
    timeframe,
    qualifies: continuationReady,
    score,
    bos,
    choch,
    emaRetest,
    trendAligned,
    continuationReady,
    breakoutLevel: roundPriceLevel(breakoutLevel),
    consolidationRange: roundPriceLevel(consolidationRange),
    consolidationBaseRange: roundPriceLevel(consolidationBaseRange),
    expansionMultiple: Number(expansionMultiple.toFixed(2)),
    sizeQualified,
    relativeVolume: Number(relativeVolume.toFixed(2)),
    volumeTrend: Number(volumeTrend.toFixed(2)),
    detail: `${side === "long" ? "Breakout" : "Breakdown"} ${roundPriceLevel(breakoutLevel)} | size ${expansionMultiple.toFixed(2)}x | RVOL ${relativeVolume.toFixed(2)}x`,
  };
}

function evaluateContinuationPotential(side, lowerCandles, higherCandles, biasScore, timeframes, setup) {
  if (lowerCandles.length < 60 || higherCandles.length < 60) {
    return emptyContinuation(side);
  }

  const lowerFrame = evaluateFrameContinuation(side, lowerCandles, "5m");
  const higherFrame = evaluateFrameContinuation(side, higherCandles, "15m");
  const ratio = Number(setup?.ratio || 0);
  const biasAligned = side === "long"
    ? biasScore >= 20 &&
      timeframes["15m"] === "Bullish" &&
      (timeframes["1h"] === "Bullish" || timeframes["4h"] === "Bullish")
    : biasScore <= -20 &&
      timeframes["15m"] === "Bearish" &&
      (timeframes["1h"] === "Bearish" || timeframes["4h"] === "Bearish");
  const roomToRun = ratio >= MIN_CONTINUATION_RATIO;
  const checks = [
    {
      label: "5m continuation",
      passed: lowerFrame.qualifies,
      score: lowerFrame.score,
      detail: lowerFrame.detail,
    },
    {
      label: "15m continuation",
      passed: higherFrame.qualifies,
      score: higherFrame.score,
      detail: higherFrame.detail,
    },
    {
      label: "Trend continuation",
      passed: biasAligned && roomToRun,
      score: clamp(
        Math.round(
          Math.min(Math.abs(biasScore), 100) * 0.7 +
          (roomToRun ? 30 : Math.min(ratio / MIN_CONTINUATION_RATIO, 1) * 30),
        ),
        0,
        100,
      ),
      detail: `Bias ${biasScore >= 0 ? "+" : ""}${biasScore}/100 | setup ${formatRatioValue(ratio)}`,
    },
  ];
  const score = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);

  return {
    side,
    qualifies: lowerFrame.qualifies && higherFrame.qualifies && biasAligned && roomToRun,
    ratio: Number.isFinite(ratio) ? Number(ratio.toFixed(2)) : 0,
    minimumRatio: MIN_CONTINUATION_RATIO,
    score,
    checks,
    frames: {
      "5m": lowerFrame,
      "15m": higherFrame,
    },
  };
}

function evaluateSetupPotential(side, entry, stopCandles, localCandles, structureCandles) {
  if (
    !Number.isFinite(entry) ||
    stopCandles.length < 50 ||
    localCandles.length < LOCAL_SETUP_BARS ||
    structureCandles.length < STRUCTURE_BARS
  ) {
    return emptySetup(side);
  }

  const executionWindow = stopCandles.slice(-64);
  const stopCandle = stopCandles.at(-1);
  const localWindow = localCandles.slice(-LOCAL_SETUP_BARS);
  const structureWindow = structureCandles.slice(-STRUCTURE_BARS);
  const executionCloses = executionWindow.map((candle) => candle.close);
  const atrValue = averageTrueRange(executionWindow, 14) ?? averageTrueRange(localCandles, 14);
  const ema9 = ema(executionCloses, 9).at(-1);
  const ema15 = ema(executionCloses, 15).at(-1);
  const rsi15m = rsi(executionCloses, 14).at(-1) ?? 50;
  const executionAdx = adx(executionWindow, 14).at(-1) ?? 0;
  const localAdx = adx(localCandles.slice(-60), 14).at(-1) ?? adx(localCandles, 14).at(-1) ?? 0;
  const executionStructure = scoreStructure(executionWindow.slice(-24));
  const localStructureScore = scoreStructure(localCandles.slice(-24));
  const higherStructureScore = scoreStructure(structureCandles.slice(-18));
  const executionBreakout = scoreBreakout(executionWindow, 20);
  const localBreakout = scoreBreakout(localCandles.slice(-36), 20);
  const candle = candleStats(stopCandle);
  const previousVolumes = executionWindow.slice(-21, -1).map((item) => Number(item.volume || 0));
  const volumeAverage20 = sma(previousVolumes, 20) ?? sma(executionWindow.map((item) => Number(item.volume || 0)), 20) ?? 0;
  const currentVolume = Number(stopCandle.volume || 0);
  const recentThreeVolume = sma(executionWindow.map((item) => Number(item.volume || 0)), 3) ?? currentVolume;
  const relativeVolume = volumeAverage20 > 0 ? currentVolume / volumeAverage20 : 0;
  const volumeTrend = volumeAverage20 > 0 ? recentThreeVolume / volumeAverage20 : 0;
  const localHigh = Math.max(...localWindow.map((candle) => candle.high));
  const localLow = Math.min(...localWindow.map((candle) => candle.low));
  const structureHigh = Math.max(...structureWindow.map((candle) => candle.high));
  const structureLow = Math.min(...structureWindow.map((candle) => candle.low));
  const executionPivotHighs = findPivots(executionWindow.map((candle) => candle.high), 2).highs;
  const executionPivotLows = findPivots(executionWindow.map((candle) => candle.low), 2).lows;
  const localPivotHighs = findPivots(localWindow.map((candle) => candle.high), 2).highs;
  const localPivotLows = findPivots(localWindow.map((candle) => candle.low), 2).lows;
  const structurePivotHighs = findPivots(structureWindow.map((candle) => candle.high), 2).highs;
  const structurePivotLows = findPivots(structureWindow.map((candle) => candle.low), 2).lows;
  const swingSupportLevels = dedupeLevels([
    ...executionPivotLows,
    ...localPivotLows,
    ...structurePivotLows,
    stopCandle.low,
    localLow,
    structureLow,
  ], entry).filter((level) => level < entry);
  const swingResistanceLevels = dedupeLevels([
    ...executionPivotHighs,
    ...localPivotHighs,
    ...structurePivotHighs,
    stopCandle.high,
    localHigh,
    structureHigh,
  ], entry).filter((level) => level > entry);
  const nearestSupport = swingSupportLevels.length ? swingSupportLevels.at(-1) : null;
  const nearestResistance = swingResistanceLevels.length ? swingResistanceLevels[0] : null;
  const atrSafe = Math.max(atrValue ?? 0, entry * 0.001);
  const emaBandLow = Math.min(ema9, ema15);
  const emaBandHigh = Math.max(ema9, ema15);
  const emaZonePadding = Math.max(atrSafe * 0.18, entry * 0.0012);
  const stopBuffer = Math.max(atrSafe * 0.35, entry * 0.0015);
  const targetGap = Math.max(atrSafe * 0.75, entry * 0.003);
  const distanceFromEmaAtr = Number.isFinite(ema9) ? Math.abs(entry - ema9) / atrSafe : Number.POSITIVE_INFINITY;

  if (
    !Number.isFinite(ema9) ||
    !Number.isFinite(ema15) ||
    !Number.isFinite(stopCandle?.low) ||
    !Number.isFinite(stopCandle?.high)
  ) {
    return emptySetup(side);
  }

  if (side === "long") {
    const breakoutLevel = nearestResistance ?? Math.max(...localWindow.slice(0, -1).map((candle) => candle.high));
    const supportAnchor = nearestSupport;
    const stopReference = Math.min(
      stopCandle.low,
      Number.isFinite(supportAnchor) ? supportAnchor : stopCandle.low,
      emaBandLow,
    );
    const stop = stopReference - stopBuffer;
    const target = swingResistanceLevels.find((level) =>
      level > entry + targetGap &&
      (!Number.isFinite(breakoutLevel) || level > breakoutLevel + targetGap * 0.35),
    ) ?? null;
    const risk = entry - stop;
    const reward = Number.isFinite(target) ? target - entry : 0;
    const ratio = reward > 0 && risk > 0 ? reward / risk : 0;
    const anchorAligned = Number.isFinite(supportAnchor) &&
      Math.abs(stopReference - supportAnchor) <= Math.max(atrSafe * 0.65, entry * 0.004);
    const entryNearAnchor = Number.isFinite(supportAnchor) &&
      (entry - supportAnchor) <= Math.max(atrSafe * 1.75, risk * 4, entry * 0.009);
    const emaAligned = entry > ema9 && ema9 > ema15;
    const emaPositionOk =
      stopCandle.low <= emaBandHigh + emaZonePadding &&
      stopCandle.high >= emaBandLow - emaZonePadding &&
      stopCandle.close >= emaBandLow &&
      distanceFromEmaAtr <= 1.15 &&
      candle.closeLocation >= 0.55;
    const trendConfirmed =
      executionStructure >= 8 &&
      localStructureScore >= 6 &&
      higherStructureScore >= -4 &&
      executionBreakout >= 0 &&
      localBreakout >= -4 &&
      Math.max(executionAdx, localAdx) >= 20 &&
      (executionAdx >= 18 || localAdx >= 22);
    const breakoutReady = !Number.isFinite(breakoutLevel) ||
      (breakoutLevel - entry) <= Math.max(atrSafe * 1.2, risk * 4, entry * 0.006);
    const targetReady = Number.isFinite(target);
    const exhausted =
      rsi15m >= 72 ||
      distanceFromEmaAtr > 1.3 ||
      (candle.upperWick > Math.max(candle.body * 1.15, candle.range * 0.28) && candle.closeLocation < 0.65);
    const checks = [
      {
        label: "Support",
        passed: anchorAligned && entryNearAnchor && breakoutReady && targetReady,
        score: clamp(Math.round((anchorAligned ? 40 : 12) + (entryNearAnchor ? 22 : 0) + (breakoutReady ? 18 : 0) + (targetReady ? 20 : 0)), 0, 100),
        detail: `Support ${roundPriceLevel(supportAnchor)} | breakout ${roundPriceLevel(breakoutLevel)} | target ${roundPriceLevel(target)}`,
      },
      {
        label: "EMA position",
        passed: emaAligned && emaPositionOk,
        score: clamp(Math.round((emaAligned ? 60 : 18) + (emaPositionOk ? 40 : Math.max(0, 25 - distanceFromEmaAtr * 18))), 0, 100),
        detail: `15m EMA9 ${formatSignedNumber((entry - ema9) / atrSafe)} ATR | EMA stack ${ema9 > ema15 ? "bullish" : "mixed"}`,
      },
      {
        label: "Trend",
        passed: trendConfirmed,
        score: trendConfirmed
          ? clamp(
            Math.round(
              Math.max(0, executionStructure) * 2 +
              Math.max(0, localStructureScore) * 2 +
              Math.max(executionAdx, localAdx) * 2 +
              Math.max(0, executionBreakout) * 1.5,
            ),
            0,
            100,
          )
          : clamp(
            Math.round(
              Math.max(0, executionStructure) * 2 +
              Math.max(0, localStructureScore) * 2 +
              Math.max(0, executionBreakout) * 1.5,
            ),
            0,
            40,
          ),
        detail: `15m ADX ${Math.round(executionAdx)} | 1h ADX ${Math.round(localAdx)} | 15m structure ${Math.round(executionStructure)} | 1h structure ${Math.round(localStructureScore)}`,
      },
      {
        label: "Exhaustion",
        passed: !exhausted,
        score: exhausted ? 18 : 100,
        detail: `RSI ${Math.round(rsi15m)} | close ${(candle.closeLocation * 100).toFixed(0)}% of candle`,
      },
    ];
    const qualityScore = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);

    return {
      side,
      ratio: Number(ratio.toFixed(2)),
      qualifies:
        ratio >= MIN_SETUP_RATIO &&
        trendConfirmed &&
        emaAligned &&
        emaPositionOk &&
        anchorAligned &&
        entryNearAnchor &&
        breakoutReady &&
        targetReady &&
        !exhausted,
      minimumRatio: MIN_SETUP_RATIO,
      stopBasis: "Last 15m candle",
      risk: roundPriceLevel(risk),
      reward: roundPriceLevel(reward),
      stop: roundPriceLevel(stop),
      target: roundPriceLevel(target),
      support: roundPriceLevel(supportAnchor),
      resistance: roundPriceLevel(breakoutLevel),
      relativeVolume: Number(relativeVolume.toFixed(2)),
      volumeTrend: Number(volumeTrend.toFixed(2)),
      qualityScore,
      checks,
    };
  }

  const breakdownLevel = nearestSupport ?? Math.min(...localWindow.slice(0, -1).map((candle) => candle.low));
  const resistanceAnchor = nearestResistance;
  const stopReference = Math.max(
    stopCandle.high,
    Number.isFinite(resistanceAnchor) ? resistanceAnchor : stopCandle.high,
    emaBandHigh,
  );
  const stop = stopReference + stopBuffer;
  const target = [...swingSupportLevels]
    .reverse()
    .find((level) =>
      level < entry - targetGap &&
      (!Number.isFinite(breakdownLevel) || level < breakdownLevel - targetGap * 0.35),
    ) ?? null;
  const risk = stop - entry;
  const reward = Number.isFinite(target) ? entry - target : 0;
  const ratio = reward > 0 && risk > 0 ? reward / risk : 0;
  const anchorAligned = Number.isFinite(resistanceAnchor) &&
    Math.abs(stopReference - resistanceAnchor) <= Math.max(atrSafe * 0.65, entry * 0.004);
  const entryNearAnchor = Number.isFinite(resistanceAnchor) &&
    (resistanceAnchor - entry) <= Math.max(atrSafe * 1.75, risk * 4, entry * 0.009);
  const emaAligned = entry < ema9 && ema9 < ema15;
  const emaPositionOk =
    stopCandle.high >= emaBandLow - emaZonePadding &&
    stopCandle.low <= emaBandHigh + emaZonePadding &&
    stopCandle.close <= emaBandHigh &&
    distanceFromEmaAtr <= 1.15 &&
    candle.closeLocation <= 0.45;
  const trendConfirmed =
    executionStructure <= -8 &&
    localStructureScore <= -6 &&
    higherStructureScore <= 4 &&
    executionBreakout <= 0 &&
    localBreakout <= 4 &&
    Math.max(executionAdx, localAdx) >= 20 &&
    (executionAdx >= 18 || localAdx >= 22);
  const breakdownReady = !Number.isFinite(breakdownLevel) ||
    (entry - breakdownLevel) <= Math.max(atrSafe * 1.2, risk * 4, entry * 0.006);
  const targetReady = Number.isFinite(target);
  const exhausted =
    rsi15m <= 28 ||
    distanceFromEmaAtr > 1.3 ||
    (candle.lowerWick > Math.max(candle.body * 1.15, candle.range * 0.28) && candle.closeLocation > 0.35);
  const checks = [
    {
      label: "Resistance",
      passed: anchorAligned && entryNearAnchor && breakdownReady && targetReady,
      score: clamp(Math.round((anchorAligned ? 40 : 12) + (entryNearAnchor ? 22 : 0) + (breakdownReady ? 18 : 0) + (targetReady ? 20 : 0)), 0, 100),
      detail: `Resistance ${roundPriceLevel(resistanceAnchor)} | breakdown ${roundPriceLevel(breakdownLevel)} | target ${roundPriceLevel(target)}`,
    },
    {
      label: "EMA position",
      passed: emaAligned && emaPositionOk,
      score: clamp(Math.round((emaAligned ? 60 : 18) + (emaPositionOk ? 40 : Math.max(0, 25 - distanceFromEmaAtr * 18))), 0, 100),
      detail: `15m EMA9 ${formatSignedNumber((entry - ema9) / atrSafe)} ATR | EMA stack ${ema9 < ema15 ? "bearish" : "mixed"}`,
    },
    {
      label: "Trend",
      passed: trendConfirmed,
      score: trendConfirmed
        ? clamp(
          Math.round(
            Math.max(0, -executionStructure) * 2 +
            Math.max(0, -localStructureScore) * 2 +
            Math.max(executionAdx, localAdx) * 2 +
            Math.max(0, -executionBreakout) * 1.5,
          ),
          0,
          100,
        )
        : clamp(
          Math.round(
            Math.max(0, -executionStructure) * 2 +
            Math.max(0, -localStructureScore) * 2 +
            Math.max(0, -executionBreakout) * 1.5,
          ),
          0,
          40,
        ),
      detail: `15m ADX ${Math.round(executionAdx)} | 1h ADX ${Math.round(localAdx)} | 15m structure ${Math.round(executionStructure)} | 1h structure ${Math.round(localStructureScore)}`,
    },
    {
      label: "Exhaustion",
      passed: !exhausted,
      score: exhausted ? 18 : 100,
      detail: `RSI ${Math.round(rsi15m)} | close ${(candle.closeLocation * 100).toFixed(0)}% of candle`,
    },
  ];
  const qualityScore = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);

  return {
    side,
    ratio: Number(ratio.toFixed(2)),
    qualifies:
      ratio >= MIN_SETUP_RATIO &&
      trendConfirmed &&
      emaAligned &&
      emaPositionOk &&
      anchorAligned &&
      entryNearAnchor &&
      breakdownReady &&
      targetReady &&
      !exhausted,
    minimumRatio: MIN_SETUP_RATIO,
    stopBasis: "Last 15m candle",
    risk: roundPriceLevel(risk),
    reward: roundPriceLevel(reward),
    stop: roundPriceLevel(stop),
    target: roundPriceLevel(target),
    support: roundPriceLevel(breakdownLevel),
    resistance: roundPriceLevel(resistanceAnchor),
    relativeVolume: Number(relativeVolume.toFixed(2)),
    volumeTrend: Number(volumeTrend.toFixed(2)),
    qualityScore,
    checks,
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

function evaluateDirectionalFrame(candles, side) {
  const latest = candles.at(-1);
  const closes = candles.map((candle) => candle.close);
  const ema9Series = ema(closes, 9);
  const ema15Series = ema(closes, 15);
  const ema9 = ema9Series.at(-1);
  const ema15 = ema15Series.at(-1);
  const ema9Prev = ema9Series.at(-2);
  const ema15Prev = ema15Series.at(-2);

  if (
    !latest ||
    !Number.isFinite(ema9) ||
    !Number.isFinite(ema15) ||
    !Number.isFinite(ema9Prev) ||
    !Number.isFinite(ema15Prev)
  ) {
    return {
      side,
      score: 0,
      emaStack: false,
      emaSlopeAligned: false,
      bos: false,
      choch: false,
      notChoppy: false,
      directionConfirmed: false,
      trendConfirmed: false,
      adx14: 0,
      structureScore: 0,
      impulseStructure: 0,
      breakoutScore: 0,
      pullbackStructure: 0,
      detail: "EMA data unavailable",
    };
  }

  const recentWindow = candles.slice(-Math.min(64, candles.length));
  const pullbackLength = Math.min(7, Math.max(4, Math.floor(recentWindow.length / 8)));
  const impulseLength = Math.min(24, Math.max(10, Math.floor(recentWindow.length / 3)));
  const signalIndex = recentWindow.length - 1;
  const pullbackStart = Math.max(0, signalIndex - pullbackLength);
  const pullbackWindow = recentWindow.slice(pullbackStart, signalIndex);
  const impulseWindow = recentWindow.slice(Math.max(0, pullbackStart - impulseLength), pullbackStart);
  const structureWindow = candles.slice(-Math.min(24, candles.length));
  const microWindow = recentWindow.slice(-5, -1);
  const atrSafe = Math.max(
    averageTrueRange(recentWindow, 14) ?? averageTrueRange(candles, 14) ?? Number(latest.close || 0) * 0.001,
    Number(latest.close || 0) * 0.0008,
  );
  const adx14 = adx(candles, 14).at(-1) ?? 0;
  const candle = candleStats(latest);
  const structureScore = scoreStructure(structureWindow);
  const impulseStructure = scoreStructure(impulseWindow.length >= 6 ? impulseWindow : structureWindow);
  const pullbackStructure = scoreStructure(
    pullbackWindow.length >= 6 ? pullbackWindow : recentWindow.slice(-Math.min(12, recentWindow.length - 1), -1),
  );
  const breakoutLookback = Math.min(20, Math.max(6, candles.length - 1));
  const breakoutScore = scoreBreakout(candles, breakoutLookback);
  const pullbackDirection = Number(pullbackWindow.at(-1)?.close || latest.close) - Number(pullbackWindow[0]?.close || latest.close);
  const pullbackHigh = pullbackWindow.length
    ? Math.max(...pullbackWindow.map((candleItem) => candleItem.high))
    : Number(latest.high || latest.close);
  const pullbackLow = pullbackWindow.length
    ? Math.min(...pullbackWindow.map((candleItem) => candleItem.low))
    : Number(latest.low || latest.close);
  const microHigh = microWindow.length
    ? Math.max(...microWindow.map((candleItem) => candleItem.high))
    : pullbackHigh;
  const microLow = microWindow.length
    ? Math.min(...microWindow.map((candleItem) => candleItem.low))
    : pullbackLow;
  const zonePadding = Math.max(atrSafe * 0.18, Number(latest.close || 0) * 0.0012);
  const bandLow = Math.min(ema9, ema15) - zonePadding;
  const bandHigh = Math.max(ema9, ema15) + zonePadding;
  const intersectsBand = latest.low <= bandHigh && latest.high >= bandLow;
  const emaSpreadAtr = Math.abs(ema9 - ema15) / atrSafe;

  let emaStack = false;
  let emaSlopeAligned = false;
  let bos = false;
  let choch = false;
  let notChoppy = false;
  let score = 0;

  if (side === "long") {
    emaStack = latest.close > ema9 && ema9 > ema15;
    emaSlopeAligned = ema9 >= ema9Prev && ema15 >= ema15Prev;
    bos =
      latest.close > Math.max(pullbackHigh, microHigh) + zonePadding * 0.1 &&
      candle.closeLocation >= 0.58 &&
      candle.body >= candle.range * 0.33;
    choch =
      bos &&
      (pullbackStructure <= -10 || pullbackDirection < 0 || pullbackWindow.some((candleItem) => candleItem.close < ema15));
    notChoppy =
      adx14 >= 18 &&
      structureScore >= 8 &&
      impulseStructure >= 6 &&
      breakoutScore >= -2 &&
      emaSpreadAtr >= 0.08;
    score += emaStack ? 24 : 0;
    score += emaSlopeAligned ? 10 : 0;
    score += intersectsBand || latest.low <= ema15 + zonePadding ? 8 : 0;
    score += clamp(Math.round((Math.max(0, structureScore) / 25) * 14), 0, 14);
    score += clamp(Math.round((Math.max(0, impulseStructure) / 25) * 10), 0, 10);
    score += clamp(Math.round((Math.max(0, breakoutScore) / 18) * 12), 0, 12);
    score += bos ? 14 : 0;
    score += choch ? 12 : 0;
    score += adx14 >= 18 ? clamp(Math.round(((Math.min(adx14, 38) - 18) / 20) * 12), 0, 12) : 0;
    score += notChoppy ? 10 : 0;
  } else {
    emaStack = latest.close < ema9 && ema9 < ema15;
    emaSlopeAligned = ema9 <= ema9Prev && ema15 <= ema15Prev;
    bos =
      latest.close < Math.min(pullbackLow, microLow) - zonePadding * 0.1 &&
      candle.closeLocation <= 0.42 &&
      candle.body >= candle.range * 0.33;
    choch =
      bos &&
      (pullbackStructure >= 10 || pullbackDirection > 0 || pullbackWindow.some((candleItem) => candleItem.close > ema15));
    notChoppy =
      adx14 >= 18 &&
      structureScore <= -8 &&
      impulseStructure <= -6 &&
      breakoutScore <= 2 &&
      emaSpreadAtr >= 0.08;
    score += emaStack ? 24 : 0;
    score += emaSlopeAligned ? 10 : 0;
    score += intersectsBand || latest.high >= ema15 - zonePadding ? 8 : 0;
    score += clamp(Math.round((Math.max(0, -structureScore) / 25) * 14), 0, 14);
    score += clamp(Math.round((Math.max(0, -impulseStructure) / 25) * 10), 0, 10);
    score += clamp(Math.round((Math.max(0, -breakoutScore) / 18) * 12), 0, 12);
    score += bos ? 14 : 0;
    score += choch ? 12 : 0;
    score += adx14 >= 18 ? clamp(Math.round(((Math.min(adx14, 38) - 18) / 20) * 12), 0, 12) : 0;
    score += notChoppy ? 10 : 0;
  }

  return {
    side,
    score: clamp(score, 0, 100),
    emaStack,
    emaSlopeAligned,
    bos,
    choch,
    notChoppy,
    directionConfirmed: emaStack && emaSlopeAligned && bos && choch,
    trendConfirmed: emaStack && emaSlopeAligned && bos && choch && notChoppy,
    adx14,
    structureScore,
    impulseStructure,
    breakoutScore,
    pullbackStructure,
    detail: `EMA9 ${roundPriceLevel(ema9)} | EMA15 ${roundPriceLevel(ema15)} | ADX ${Math.round(adx14)} | structure ${Math.round(structureScore)} | BOS ${bos ? "yes" : "no"} | CHoCH ${choch ? "yes" : "no"}`,
  };
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

  const latestClose = candles.at(-1)?.close ?? null;
  const bullish = evaluateDirectionalFrame(candles, "long");
  const bearish = evaluateDirectionalFrame(candles, "short");
  const score = clamp(Math.round((bullish.score || 0) - (bearish.score || 0)), -100, 100);
  const winner = score >= 0 ? bullish : bearish;
  const scoreDirection = score >= 0 ? 1 : -1;
  const components = [];

  pushComponent(
    components,
    "EMA 9/15",
    winner.emaStack ? 24 * scoreDirection : 0,
    winner.emaStack
      ? `Price on the ${winner.side} side of EMA 9/15`
      : "Price not aligned with EMA 9/15",
  );
  pushComponent(
    components,
    "EMA slope",
    winner.emaSlopeAligned ? 10 * scoreDirection : 0,
    winner.emaSlopeAligned
      ? `EMA 9 and EMA 15 sloping ${winner.side === "long" ? "up" : "down"}`
      : "EMA slope not clean",
  );
  pushComponent(
    components,
    "Structure",
    Math.round(Math.abs(winner.structureScore || 0)) * scoreDirection,
    `Main structure ${Math.round(winner.structureScore || 0)} | impulse ${Math.round(winner.impulseStructure || 0)}`,
  );
  pushComponent(
    components,
    "Breakout",
    Math.round(Math.abs(winner.breakoutScore || 0)) * scoreDirection,
    `BOS ${winner.bos ? "yes" : "no"} | breakout score ${Math.round(winner.breakoutScore || 0)}`,
  );
  pushComponent(
    components,
    "CHoCH",
    winner.choch ? 12 * scoreDirection : 0,
    `CHoCH ${winner.choch ? "yes" : "no"} | pullback structure ${Math.round(winner.pullbackStructure || 0)}`,
  );
  pushComponent(
    components,
    "Trend quality",
    winner.notChoppy ? 10 * scoreDirection : 0,
    winner.notChoppy ? `One-way trend | ADX ${Math.round(winner.adx14 || 0)}` : `Choppy / weak trend | ADX ${Math.round(winner.adx14 || 0)}`,
  );

  let label = "Neutral";
  if (score >= 28 && bullish.directionConfirmed) {
    label = "Bullish";
  } else if (score <= -28 && bearish.directionConfirmed) {
    label = "Bearish";
  }

  let trendLabel = "Choppy";
  if (label === "Bullish" && bullish.trendConfirmed) {
    trendLabel = "Trend Up";
  } else if (label === "Bullish" && bullish.directionConfirmed) {
    trendLabel = "Up";
  } else if (label === "Bearish" && bearish.trendConfirmed) {
    trendLabel = "Trend Down";
  } else if (label === "Bearish" && bearish.directionConfirmed) {
    trendLabel = "Down";
  } else if (Math.abs(score) < 18) {
    trendLabel = "Range";
  }

  return { label, score, trendLabel, close: latestClose, components };
}

function summarizeBias(score, bullishCoreCount = 0, bearishCoreCount = 0) {
  if (bullishCoreCount >= 2 && score >= 60) {
    return "Strong Bullish";
  }
  if (bullishCoreCount >= 2 && score >= 20) {
    return "Bullish";
  }
  if (bearishCoreCount >= 2 && score <= -60) {
    return "Strong Bearish";
  }
  if (bearishCoreCount >= 2 && score <= -20) {
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

  const activeTrendCheck = findCheck(activeSetup?.checks, "Trend");
  if (!activeTrendCheck?.passed) {
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

function emptyFeaturedLeader(side) {
  const timeframes = Object.fromEntries(TIMEFRAMES.map((timeframe) => [timeframe.key, "Neutral"]));
  const longSetup = emptySetup("long");
  const shortSetup = emptySetup("short");
  const longContinuation = emptyContinuation("long");
  const shortContinuation = emptyContinuation("short");

  return {
    symbol: side === "long" ? "No Ready Long" : "No Ready Short",
    underlying: "--",
    description: "No featured setup",
    score: 0,
    biasScore: 0,
    bias: "Neutral",
    trend: "Range",
    price: null,
    liquidity: 0,
    fundingRate: null,
    change24h: null,
    timeframes,
    scoreBreakdown: [],
    setupScoreBreakdown: [],
    setup: side === "long" ? longSetup : shortSetup,
    setups: {
      long: longSetup,
      short: shortSetup,
    },
    continuation: side === "long" ? longContinuation : shortContinuation,
    continuations: {
      long: longContinuation,
      short: shortContinuation,
    },
    tradeReadiness: "No Setup",
    featuredUnavailable: true,
    featuredReason: side === "long"
      ? `Needs Ready Long, BOS yes, CHoCH yes, EMA 9/15 yes, one-way trend, and Risk:Reward above ${formatRatioValue(MIN_SETUP_RATIO)}.`
      : `Needs Ready Short, BOS yes, CHoCH yes, EMA 9/15 yes, one-way trend, and Risk:Reward above ${formatRatioValue(MIN_SETUP_RATIO)}.`,
    rank: 0,
  };
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

function readinessScore(label) {
  if (label === "Ready Long" || label === "Ready Short") {
    return 100;
  }
  if (label === "Long Watch" || label === "Short Watch") {
    return 78;
  }
  if (label === "Wait") {
    return 46;
  }
  if (label === "Avoid") {
    return 10;
  }

  return 30;
}

function readinessCap(label) {
  if (label === "Ready Long" || label === "Ready Short") {
    return 100;
  }
  if (label === "Long Watch" || label === "Short Watch") {
    return 78;
  }
  if (label === "Wait") {
    return 58;
  }
  if (label === "Avoid") {
    return 32;
  }

  return 40;
}

function ratioScore(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) {
    return 0;
  }

  const cappedRatio = clamp(ratio, 1, 12);
  const normalized = (Math.log(cappedRatio) - Math.log(1)) / (Math.log(12) - Math.log(1));
  return clamp(Math.round(normalized * 100), 0, 100);
}

function directionAgreementScore(timeframes, side, rowTrend) {
  const expectedBias = side === "short" ? "Bearish" : "Bullish";
  const oppositeBias = side === "short" ? "Bullish" : "Bearish";
  const expectedTrend = side === "short" ? "Downtrend" : "Uptrend";
  const coreMatches = CORE_TIMEFRAME_KEYS.filter((key) => timeframes?.[key] === expectedBias).length;
  const coreOpposites = CORE_TIMEFRAME_KEYS.filter((key) => timeframes?.[key] === oppositeBias).length;
  const higherKeys = ["12h", "24h"];
  const higherMatches = higherKeys.filter((key) => timeframes?.[key] === expectedBias).length;
  const higherOpposites = higherKeys.filter((key) => timeframes?.[key] === oppositeBias).length;
  const trendAligned = rowTrend === expectedTrend;

  const score =
    coreMatches * 24 +
    higherMatches * 8 +
    (trendAligned ? 12 : 0) -
    coreOpposites * 24 -
    higherOpposites * 8;

  return clamp(score, 0, 100);
}

function calculateSetupScore(readiness, setup, timeframes, rowTrend) {
  const side = setup?.side === "short" ? "short" : "long";
  const directionValue = directionAgreementScore(timeframes, side, rowTrend);
  const ratioValue = ratioScore(setup?.ratio);
  const readinessValue = readinessScore(readiness);
  const emaValue = clamp(Number(findCheck(setup?.checks, "EMA position")?.score || 0), 0, 100);
  const trendValue = clamp(Number(findCheck(setup?.checks, "Trend")?.score || 0), 0, 100);
  const exhaustionValue = clamp(Number(findCheck(setup?.checks, "Exhaustion")?.score || 0), 0, 100);
  const rawScore = Math.round(
    directionValue * 0.2 +
    readinessValue * 0.15 +
    emaValue * 0.2 +
    trendValue * 0.2 +
    exhaustionValue * 0.1 +
    ratioValue * 0.15,
  );
  const score = Math.min(rawScore, readinessCap(readiness));

  return {
    score: clamp(score, 0, 100),
    breakdown: [
      {
        label: "Direction agreement",
        score: directionValue,
        detail: `15m ${timeframes?.["15m"] || "Neutral"} | 1h ${timeframes?.["1h"] || "Neutral"} | 4h ${timeframes?.["4h"] || "Neutral"} | trend ${rowTrend}`,
      },
      {
        label: "Readiness",
        score: readinessValue,
        detail: readiness,
      },
      {
        label: "Risk:Reward",
        score: ratioValue,
        detail: Number.isFinite(Number(setup?.ratio))
          ? (setup?.qualifies ? `Reward to risk 1:${setup.ratio}` : `Reward to risk 1:${setup.ratio} | blocked by setup filters`)
          : "No valid setup ratio",
      },
      {
        label: "EMA 9/15",
        score: emaValue,
        detail: findCheck(setup?.checks, "EMA position")?.detail || "No EMA setup check",
      },
      {
        label: "Trend / Exhaustion",
        score: Math.round((trendValue + exhaustionValue) / 2),
        detail: `${findCheck(setup?.checks, "Trend")?.detail || "No trend check"} | ${findCheck(setup?.checks, "Exhaustion")?.detail || "No exhaustion check"}`,
      },
    ],
  };
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

function countPassedChecks(checks) {
  return Array.isArray(checks) ? checks.filter((item) => item?.passed).length : 0;
}

function findCheck(checks, label) {
  return Array.isArray(checks) ? checks.find((item) => item?.label === label) : null;
}

function summarizeMissingChecks(checks, limit = 2) {
  const missing = (Array.isArray(checks) ? checks : [])
    .filter((item) => !item?.passed)
    .map((item) => item.label);

  if (missing.length === 0) {
    return "All checks passed";
  }

  return missing.slice(0, limit).join(", ");
}

function summarizeFrameMissingChecks(frame, limit = 3) {
  const missing = [];

  if (!frame?.trendAligned) {
    missing.push("trend");
  }
  if (!frame?.bos) {
    missing.push("BOS");
  }
  if (!frame?.choch) {
    missing.push("CHoCH");
  }
  if (!frame?.emaRetest) {
    missing.push("EMA 9/15");
  }
  if (!frame?.sizeQualified) {
    missing.push("size");
  }
  if (missing.length === 0) {
    return "All checks passed";
  }

  return missing.slice(0, limit).join(", ");
}

function decorateSetupLeader(row, side, mode) {
  const setup = row.setups?.[side] || row.setup;
  const continuation = row.continuations?.[side] || row.continuation;
  const watchLabel = side === "short" ? "Short Watch" : "Long Watch";

  return {
    ...row,
    setup,
    continuation,
    tradeReadiness: mode === "confirmed" ? row.tradeReadiness : watchLabel,
    leaderMode: mode,
    leaderNote: mode === "confirmed"
      ? "All setup filters passed"
      : `Watchlist: missing ${summarizeMissingChecks(setup?.checks)}`,
  };
}

function decorateContinuationLeader(row, side, mode) {
  const continuation = row.continuations?.[side] || row.continuation;
  const setup = row.setups?.[side] || row.setup;
  const watchLabel = side === "short" ? "Short Watch" : "Long Watch";
  const confirmedLabel = side === "short" ? "Short Continuation" : "Long Continuation";

  return {
    ...row,
    setup,
    continuation,
    tradeReadiness: mode === "confirmed" ? confirmedLabel : watchLabel,
    continuationMode: mode,
    continuationNote: mode === "confirmed"
      ? "5m and 15m continuation aligned"
      : `Watchlist: missing ${summarizeMissingChecks(continuation?.checks)}`,
  };
}

function decorateFrameContinuationLeader(row, side, timeframe) {
  const continuation = row.continuations?.[side] || row.continuation;
  const setup = row.setups?.[side] || row.setup;
  const frame = continuation?.frames?.[timeframe];
  const directionLabel = side === "short" ? "Short" : "Long";

  return {
    ...row,
    setup,
    continuation,
    continuationFrame: frame,
    continuationTimeframe: timeframe,
    tradeReadiness: `${timeframe} ${directionLabel}`,
    continuationNote: frame?.qualifies
      ? `${timeframe} candle confirmed`
      : `Missing ${summarizeFrameMissingChecks(frame)}`,
  };
}

function isSetupWatchCandidate(row, side) {
  const setup = row.setups?.[side];
  const biasAligned = side === "long" ? row.biasScore >= 20 : row.biasScore <= -20;
  const checksPassed = countPassedChecks(setup?.checks);
  const emaCheck = findCheck(setup?.checks, "EMA position");
  const exhaustionCheck = findCheck(setup?.checks, "Exhaustion");
  const trendCheck = findCheck(setup?.checks, "Trend");

  return Boolean(
    biasAligned &&
    row.tradeReadiness !== "Avoid" &&
    (setup?.ratio ?? 0) > MIN_SETUP_RATIO &&
    checksPassed >= 2 &&
    emaCheck?.passed &&
    exhaustionCheck?.passed &&
    trendCheck?.passed,
  );
}

function isReadyDirectionalSetupCandidate(row, side) {
  const setup = row.setups?.[side] || row.setup;
  const frame15 = row.continuations?.[side]?.frames?.["15m"];
  const expectedBias = side === "long" ? "Bullish" : "Bearish";
  const oppositeBias = side === "long" ? "Bearish" : "Bullish";
  const readyLabel = side === "long" ? "Ready Long" : "Ready Short";
  const expectedTrend = side === "long" ? "Uptrend" : "Downtrend";

  return Boolean(
    row.tradeReadiness === readyLabel &&
    row.trend === expectedTrend &&
    setup?.side === side &&
    setup?.qualifies &&
    (setup?.ratio ?? 0) > MIN_SETUP_RATIO &&
    row.timeframes?.["15m"] === expectedBias &&
    row.timeframes?.["1h"] === expectedBias &&
    row.timeframes?.["4h"] === expectedBias &&
    row.timeframes?.["12h"] !== oppositeBias &&
    row.timeframes?.["24h"] !== oppositeBias &&
    frame15?.trendAligned &&
    frame15?.bos &&
    frame15?.choch &&
    frame15?.emaRetest,
  );
}

function sortReadyDirectionalCandidates(left, right, side) {
  const leftSetup = left.setups?.[side] || left.setup || {};
  const rightSetup = right.setups?.[side] || right.setup || {};
  const leftFrame = left.continuations?.[side]?.frames?.["15m"] || {};
  const rightFrame = right.continuations?.[side]?.frames?.["15m"] || {};

  if ((rightSetup.ratio ?? 0) !== (leftSetup.ratio ?? 0)) {
    return (rightSetup.ratio ?? 0) - (leftSetup.ratio ?? 0);
  }
  if ((rightFrame.score ?? 0) !== (leftFrame.score ?? 0)) {
    return (rightFrame.score ?? 0) - (leftFrame.score ?? 0);
  }
  if (right.score !== left.score) {
    return right.score - left.score;
  }
  if (side === "long" && right.biasScore !== left.biasScore) {
    return right.biasScore - left.biasScore;
  }
  if (side === "short" && left.biasScore !== right.biasScore) {
    return left.biasScore - right.biasScore;
  }
  return right.liquidity - left.liquidity;
}

function isContinuationWatchCandidate(row, side) {
  const continuation = row.continuations?.[side];
  const setup = row.setups?.[side];
  const frame5 = continuation?.frames?.["5m"];
  const frame15 = continuation?.frames?.["15m"];
  const biasAligned = side === "long" ? row.biasScore >= 20 : row.biasScore <= -20;
  const strongFrameScores = Number(frame5?.score || 0) >= 60 && Number(frame15?.score || 0) >= 55;
  const structurePresent = Boolean(
    (frame5?.bos || frame5?.choch) &&
    (frame15?.bos || frame15?.choch || frame15?.emaRetest || frame15?.trendAligned),
  );

  return Boolean(
    continuation &&
    biasAligned &&
    row.tradeReadiness !== "Avoid" &&
    (setup?.ratio ?? 0) >= MIN_CONTINUATION_RATIO &&
    (continuation.score ?? 0) >= 60 &&
    strongFrameScores &&
    frame5?.sizeQualified &&
    frame15?.sizeQualified &&
    frame5?.trendAligned &&
    frame15?.trendAligned &&
    structurePresent,
  );
}

function isFrameContinuationCandidate(row, side, timeframe) {
  const continuation = row.continuations?.[side];
  const frame = continuation?.frames?.[timeframe];
  const setup = row.setups?.[side];
  const biasAligned = side === "long" ? row.biasScore >= 20 : row.biasScore <= -20;

  return Boolean(
    frame &&
    frame.qualifies &&
    frame.bos &&
    frame.choch &&
    frame.emaRetest &&
    biasAligned &&
    row.tradeReadiness !== "Avoid" &&
    (setup?.ratio ?? 0) >= MIN_CONTINUATION_RATIO,
  );
}

function sortFrameContinuationCandidates(left, right, side, timeframe) {
  const leftFrame = left.continuations?.[side]?.frames?.[timeframe] || {};
  const rightFrame = right.continuations?.[side]?.frames?.[timeframe] || {};

  if (Number(Boolean(rightFrame.qualifies)) !== Number(Boolean(leftFrame.qualifies))) {
    return Number(Boolean(rightFrame.qualifies)) - Number(Boolean(leftFrame.qualifies));
  }
  if ((rightFrame.score ?? 0) !== (leftFrame.score ?? 0)) {
    return (rightFrame.score ?? 0) - (leftFrame.score ?? 0);
  }
  if ((rightFrame.expansionMultiple ?? 0) !== (leftFrame.expansionMultiple ?? 0)) {
    return (rightFrame.expansionMultiple ?? 0) - (leftFrame.expansionMultiple ?? 0);
  }
  if ((right.setups?.[side]?.ratio ?? 0) !== (left.setups?.[side]?.ratio ?? 0)) {
    return (right.setups?.[side]?.ratio ?? 0) - (left.setups?.[side]?.ratio ?? 0);
  }

  return right.liquidity - left.liquidity;
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
  const bullishCoreCount = CORE_TIMEFRAME_KEYS.filter((key) => timeframes[key] === "Bullish").length;
  const bearishCoreCount = CORE_TIMEFRAME_KEYS.filter((key) => timeframes[key] === "Bearish").length;
  const scoreBreakdown = TIMEFRAMES.map((timeframe) => ({
    timeframe: timeframe.key,
    score: timeframeResults[timeframe.key].score,
    bias: timeframeResults[timeframe.key].label,
    components: timeframeResults[timeframe.key].components,
  }));
  const longSetup = evaluateSetupPotential("long", price, sourceMap["15m"] || [], sourceMap["1h"] || [], sourceMap["4h"] || []);
  const shortSetup = evaluateSetupPotential("short", price, sourceMap["15m"] || [], sourceMap["1h"] || [], sourceMap["4h"] || []);
  const longContinuation = evaluateContinuationPotential(
    "long",
    sourceMap["5m"] || [],
    sourceMap["15m"] || [],
    weightedScore,
    timeframes,
    longSetup,
  );
  const shortContinuation = evaluateContinuationPotential(
    "short",
    sourceMap["5m"] || [],
    sourceMap["15m"] || [],
    weightedScore,
    timeframes,
    shortSetup,
  );
  const activeSetup = weightedScore >= 0 ? longSetup : shortSetup;
  const activeContinuation = weightedScore >= 0 ? longContinuation : shortContinuation;
  const rowTrend = dominantTrend(CORE_TIMEFRAME_KEYS.map((key) => timeframeResults[key].trendLabel));
  const readiness = tradeReadiness(
    weightedScore,
    timeframes,
    activeSetup,
  );
  const setupScore = calculateSetupScore(readiness, activeSetup, timeframes, rowTrend);

  return {
    symbol: ticker.symbol,
    underlying: ticker.underlying_asset_symbol || ticker.symbol.replace(/USD$|INR$/i, ""),
    description: ticker.description || "Perpetual futures",
    score: setupScore.score,
    biasScore: weightedScore,
    bias: summarizeBias(weightedScore, bullishCoreCount, bearishCoreCount),
    trend: rowTrend,
    price,
    liquidity: tickerLiquidityScore(ticker),
    fundingRate: ticker?.funding_rate ?? null,
    change24h: ticker?.ltp_change_24h ?? ticker?.mark_change_24h ?? null,
    timeframes,
    scoreBreakdown,
    setupScoreBreakdown: setupScore.breakdown,
    setup: activeSetup,
    setups: {
      long: longSetup,
      short: shortSetup,
    },
    continuation: activeContinuation,
    continuations: {
      long: longContinuation,
      short: shortContinuation,
    },
    tradeReadiness: readiness,
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
  const rawRows = settled
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value);
  const rows = rawRows
    .sort((left, right) => {
      if ((right.setup?.ratio ?? 0) !== (left.setup?.ratio ?? 0)) {
        return (right.setup?.ratio ?? 0) - (left.setup?.ratio ?? 0);
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if (readinessRank(right.tradeReadiness) !== readinessRank(left.tradeReadiness)) {
        return readinessRank(right.tradeReadiness) - readinessRank(left.tradeReadiness);
      }
      if ((right.setup?.ratio ?? 0) !== (left.setup?.ratio ?? 0)) {
        return (right.setup?.ratio ?? 0) - (left.setup?.ratio ?? 0);
      }
      return Math.abs(right.biasScore) - Math.abs(left.biasScore);
    })
    .map((row, index) => ({ ...row, rank: index + 1 }));

  const bullish = rawRows.filter((row) => row.bias.includes("Bull")).length;
  const bearish = rawRows.filter((row) => row.bias.includes("Bear")).length;
  const neutral = rawRows.length - bullish - bearish;
  const rawStrongestBull = [...rawRows].sort((left, right) => right.biasScore - left.biasScore)[0] ||
    { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, biasScore: 0, rank: 0 };
  const rawStrongestBear = [...rawRows].sort((left, right) => left.biasScore - right.biasScore)[0] ||
    { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, biasScore: 0, rank: 0 };
  const strongest24hLeader = [...rawRows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(right.change24h) - Number(left.change24h))[0] ||
    rawStrongestBull;
  const strongest24hLaggard = [...rawRows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(left.change24h) - Number(right.change24h))[0] ||
    rawStrongestBear;
  const majorIndex = new Map(PRIORITY_ASSETS.map((asset, index) => [asset, index]));
  const majors = rawRows
    .filter((row) => majorIndex.has(String(row.underlying).toUpperCase()))
    .sort((left, right) => majorIndex.get(String(left.underlying).toUpperCase()) - majorIndex.get(String(right.underlying).toUpperCase()));

  const strictBullLeaders = rawRows
    .filter((row) => isReadyDirectionalSetupCandidate(row, "long"))
    .sort((left, right) => sortReadyDirectionalCandidates(left, right, "long"))
    .slice(0, BEST_SETUP_DISPLAY_LIMIT);
  const strictBearLeaders = rawRows
    .filter((row) => isReadyDirectionalSetupCandidate(row, "short"))
    .sort((left, right) => sortReadyDirectionalCandidates(left, right, "short"))
    .slice(0, BEST_SETUP_DISPLAY_LIMIT);
  const strongestBull = strictBullLeaders[0] || emptyFeaturedLeader("long");
  const strongestBear = strictBearLeaders[0] || emptyFeaturedLeader("short");
  const headline =
    strictBullLeaders.length && strictBearLeaders.length
      ? `${strongestBull.symbol} is the top ready long while ${strongestBear.symbol} is the top ready short.`
      : strictBullLeaders.length
        ? `${strongestBull.symbol} is the top ready long. No ready short setup passes every featured filter right now.`
        : strictBearLeaders.length
          ? `${strongestBear.symbol} is the top ready short. No ready long setup passes every featured filter right now.`
          : "No ready long or short setup passes the EMA 9/15, BOS/CHoCH, trend, and risk:reward filters right now.";
  const qualifiedBullLeaders = strictBullLeaders.map((row) => decorateSetupLeader(row, "long", "confirmed"));
  const qualifiedBearLeaders = strictBearLeaders.map((row) => decorateSetupLeader(row, "short", "confirmed"));
  const strictContinuationLongLeaders = rawRows
    .filter((row) => row.continuations?.long?.qualifies)
    .sort((left, right) => {
      if ((right.continuations.long.score ?? 0) !== (left.continuations.long.score ?? 0)) {
        return (right.continuations.long.score ?? 0) - (left.continuations.long.score ?? 0);
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if ((right.setups?.long?.ratio ?? 0) !== (left.setups?.long?.ratio ?? 0)) {
        return (right.setups?.long?.ratio ?? 0) - (left.setups?.long?.ratio ?? 0);
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 6);
  const strictContinuationShortLeaders = rawRows
    .filter((row) => row.continuations?.short?.qualifies)
    .sort((left, right) => {
      if ((right.continuations.short.score ?? 0) !== (left.continuations.short.score ?? 0)) {
        return (right.continuations.short.score ?? 0) - (left.continuations.short.score ?? 0);
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if ((right.setups?.short?.ratio ?? 0) !== (left.setups?.short?.ratio ?? 0)) {
        return (right.setups?.short?.ratio ?? 0) - (left.setups?.short?.ratio ?? 0);
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 6);
  const continuationLongWatchlist = rawRows
    .filter((row) => isContinuationWatchCandidate(row, "long"))
    .sort((left, right) => {
      if ((right.continuations?.long?.score ?? 0) !== (left.continuations?.long?.score ?? 0)) {
        return (right.continuations?.long?.score ?? 0) - (left.continuations?.long?.score ?? 0);
      }
      return right.liquidity - left.liquidity;
    });
  const continuationShortWatchlist = rawRows
    .filter((row) => isContinuationWatchCandidate(row, "short"))
    .sort((left, right) => {
      if ((right.continuations?.short?.score ?? 0) !== (left.continuations?.short?.score ?? 0)) {
        return (right.continuations?.short?.score ?? 0) - (left.continuations?.short?.score ?? 0);
      }
      return right.liquidity - left.liquidity;
    });
  const continuationLongLeaders = [
    ...strictContinuationLongLeaders.map((row) => decorateContinuationLeader(row, "long", "confirmed")),
    ...continuationLongWatchlist
      .filter((row) => !strictContinuationLongLeaders.some((candidate) => candidate.symbol === row.symbol))
      .slice(0, Math.max(0, 6 - strictContinuationLongLeaders.length))
      .map((row) => decorateContinuationLeader(row, "long", "watch")),
  ];
  const continuationShortLeaders = [
    ...strictContinuationShortLeaders.map((row) => decorateContinuationLeader(row, "short", "confirmed")),
    ...continuationShortWatchlist
      .filter((row) => !strictContinuationShortLeaders.some((candidate) => candidate.symbol === row.symbol))
      .slice(0, Math.max(0, 6 - strictContinuationShortLeaders.length))
      .map((row) => decorateContinuationLeader(row, "short", "watch")),
  ];
  const frame5LongLeaders = rawRows
    .filter((row) => isFrameContinuationCandidate(row, "long", "5m"))
    .sort((left, right) => sortFrameContinuationCandidates(left, right, "long", "5m"))
    .slice(0, 6)
    .map((row) => decorateFrameContinuationLeader(row, "long", "5m"));
  const frame5ShortLeaders = rawRows
    .filter((row) => isFrameContinuationCandidate(row, "short", "5m"))
    .sort((left, right) => sortFrameContinuationCandidates(left, right, "short", "5m"))
    .slice(0, 6)
    .map((row) => decorateFrameContinuationLeader(row, "short", "5m"));
  const frame15LongLeaders = rawRows
    .filter((row) => isFrameContinuationCandidate(row, "long", "15m"))
    .sort((left, right) => sortFrameContinuationCandidates(left, right, "long", "15m"))
    .slice(0, 6)
    .map((row) => decorateFrameContinuationLeader(row, "long", "15m"));
  const frame15ShortLeaders = rawRows
    .filter((row) => isFrameContinuationCandidate(row, "short", "15m"))
    .sort((left, right) => sortFrameContinuationCandidates(left, right, "short", "15m"))
    .slice(0, 6)
    .map((row) => decorateFrameContinuationLeader(row, "short", "15m"));

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
    continuationLeaders: {
      longs: continuationLongLeaders,
      shorts: continuationShortLeaders,
    },
    continuationFrames: {
      "5m": {
        longs: frame5LongLeaders,
        shorts: frame5ShortLeaders,
      },
      "15m": {
        longs: frame15LongLeaders,
        shorts: frame15ShortLeaders,
      },
    },
    signalDiagnostics: {
      strictLongCount: strictBullLeaders.length,
      strictShortCount: strictBearLeaders.length,
      strictContinuationLongCount: strictContinuationLongLeaders.length,
      strictContinuationShortCount: strictContinuationShortLeaders.length,
      frameContinuation5mLongCount: frame5LongLeaders.length,
      frameContinuation5mShortCount: frame5ShortLeaders.length,
      frameContinuation15mLongCount: frame15LongLeaders.length,
      frameContinuation15mShortCount: frame15ShortLeaders.length,
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
