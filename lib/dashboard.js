const { URL, URLSearchParams } = require("url");

const DELTA_BASE = "https://api.india.delta.exchange/v2";
const PRIORITY_ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "LTC"];
const LEADERBOARD_CONCURRENCY = 18;
const SNAPSHOT_TTL_MS = 15_000;
const MIN_SETUP_RATIO = 5;
const MIN_CONTINUATION_RATIO = 3;
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
    volumeConfirmed: false,
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
    volumeScore: 0,
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
  const microHigh = Math.max(...window.slice(-5, -1).map((candleItem) => candleItem.high));
  const microLow = Math.min(...window.slice(-5, -1).map((candleItem) => candleItem.low));

  let trendAligned = false;
  let bos = false;
  let choch = false;
  let emaRetest = false;
  let volumeConfirmed = false;
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
      signal.close > Math.max(ema9, ema15) - zonePadding * 0.25 &&
      signal.low <= Math.max(ema9, ema15) + zonePadding;
    volumeConfirmed =
      relativeVolume >= 1.18 &&
      volumeTrend >= 1.05 &&
      currentVolume >= Number(pullbackWindow.at(-1)?.volume || 0);
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
      signal.close < Math.min(ema9, ema15) + zonePadding * 0.25 &&
      signal.high >= Math.min(ema9, ema15) - zonePadding;
    volumeConfirmed =
      relativeVolume >= 1.18 &&
      volumeTrend >= 1.05 &&
      currentVolume >= Number(pullbackWindow.at(-1)?.volume || 0);
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
      detail: `Band ${roundPriceLevel(bandLow)}-${roundPriceLevel(bandHigh)}`,
    },
    {
      label: `${timeframe} size`,
      passed: sizeQualified,
      score: clamp(Math.round((expansionMultiple / 3) * 100), 0, 100),
      detail: `Signal ${expansionMultiple.toFixed(2)}x vs last consolidation base`,
    },
    {
      label: `${timeframe} volume`,
      passed: volumeConfirmed,
      score: clamp(Math.round(relativeVolume * 50 + volumeTrend * 20), 0, 100),
      detail: `RVOL ${relativeVolume.toFixed(2)}x | 3-bar vol ${volumeTrend.toFixed(2)}x`,
    },
    {
      label: `${timeframe} extension`,
      passed: notExhausted,
      score: notExhausted ? 100 : 20,
      detail: `${side === "long" ? "Above" : "Below"} EMA15 ${formatSignedNumber(side === "long" ? (signal.close - ema15) / atrSafe : (ema15 - signal.close) / atrSafe)} ATR`,
    },
  ];
  const score = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);
  const continuationReady = trendAligned && bos && choch && emaRetest && sizeQualified && volumeConfirmed && notExhausted;

  return {
    timeframe,
    qualifies: continuationReady,
    score,
    bos,
    choch,
    emaRetest,
    trendAligned,
    volumeConfirmed,
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
  const volumeAgreement =
    lowerFrame.volumeConfirmed &&
    higherFrame.volumeConfirmed &&
    (Number(lowerFrame.relativeVolume || 0) + Number(higherFrame.relativeVolume || 0)) / 2 >= 1.18;
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
    {
      label: "Volume agreement",
      passed: volumeAgreement,
      score: clamp(
        Math.round(
          (((Number(lowerFrame.relativeVolume || 0) + Number(higherFrame.relativeVolume || 0)) / 2) * 50) +
          (((Number(lowerFrame.volumeTrend || 0) + Number(higherFrame.volumeTrend || 0)) / 2) * 18),
        ),
        0,
        100,
      ),
      detail: `5m ${lowerFrame.relativeVolume ?? "N/A"}x | 15m ${higherFrame.relativeVolume ?? "N/A"}x`,
    },
  ];
  const score = Math.round(checks.reduce((sum, item) => sum + item.score, 0) / checks.length);

  return {
    side,
    qualifies: lowerFrame.qualifies && higherFrame.qualifies && biasAligned && roomToRun && volumeAgreement,
    ratio: Number.isFinite(ratio) ? Number(ratio.toFixed(2)) : 0,
    minimumRatio: MIN_CONTINUATION_RATIO,
    score,
    volumeScore: clamp(
      Math.round(
        (((Number(lowerFrame.relativeVolume || 0) + Number(higherFrame.relativeVolume || 0)) / 2) * 50) +
        (((Number(lowerFrame.volumeTrend || 0) + Number(higherFrame.volumeTrend || 0)) / 2) * 18),
      ),
      0,
      100,
    ),
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
  const localRange = localHigh - localLow;
  const structureRange = structureHigh - structureLow;
  const bufferUnit = Math.max(localRange * 0.8, structureRange * 0.2, (atrValue ?? 0) * 3, entry * 0.01);
  const executionPivotHighs = findPivots(executionWindow.map((candle) => candle.high), 2).highs;
  const executionPivotLows = findPivots(executionWindow.map((candle) => candle.low), 2).lows;
  const localPivotHighs = findPivots(localWindow.map((candle) => candle.high), 2).highs;
  const localPivotLows = findPivots(localWindow.map((candle) => candle.low), 2).lows;
  const structurePivotHighs = findPivots(structureWindow.map((candle) => candle.high), 2).highs;
  const structurePivotLows = findPivots(structureWindow.map((candle) => candle.low), 2).lows;
  const supportLevels = dedupeLevels([
    ...executionPivotLows,
    ...localPivotLows,
    ...structurePivotLows,
    stopCandle.low,
    localLow,
    structureLow,
    ema9,
    ema15,
  ], entry).filter((level) => level < entry);
  const resistanceLevels = dedupeLevels([
    ...executionPivotHighs,
    ...localPivotHighs,
    ...structurePivotHighs,
    stopCandle.high,
    localHigh,
    structureHigh,
    ema9,
    ema15,
  ], entry).filter((level) => level > entry);
  const nearestSupport = supportLevels.length ? supportLevels.at(-1) : null;
  const nearestResistance = resistanceLevels.length ? resistanceLevels[0] : null;
  const secondResistance = resistanceLevels.find((level) => level > (nearestResistance ?? entry) + Math.max((atrValue ?? 0) * 0.4, entry * 0.002));
  const lowerSupportLevels = [...supportLevels].reverse();
  const secondSupport = lowerSupportLevels.find((level) => level < (nearestSupport ?? entry) - Math.max((atrValue ?? 0) * 0.4, entry * 0.002));
  const extensionTarget = Math.max(localRange * 0.7, structureRange * 0.14, (atrValue ?? 0) * 6, entry * 0.018);
  const atrSafe = Math.max(atrValue ?? 0, entry * 0.001);
  const emaBandLow = Math.min(ema9, ema15);
  const emaBandHigh = Math.max(ema9, ema15);
  const emaZonePadding = Math.max(atrSafe * 0.18, entry * 0.0012);
  const distanceFromEmaAtr = Number.isFinite(ema9) ? Math.abs(entry - ema9) / atrSafe : Number.POSITIVE_INFINITY;

  if (
    !Number.isFinite(bufferUnit) ||
    bufferUnit <= 0 ||
    !Number.isFinite(ema9) ||
    !Number.isFinite(ema15) ||
    !Number.isFinite(stopCandle?.low) ||
    !Number.isFinite(stopCandle?.high)
  ) {
    return emptySetup(side);
  }

  if (side === "long") {
    const breakoutLevel = nearestResistance ?? Math.max(...localWindow.slice(0, -1).map((candle) => candle.high));
    const stop = stopCandle.low;
    const targetBase = secondResistance ?? Math.max(structureHigh, breakoutLevel + extensionTarget, breakoutLevel + bufferUnit);
    const target = Math.max(targetBase, breakoutLevel + extensionTarget * 0.5);
    const risk = entry - stop;
    const reward = target - entry;
    const ratio = reward > 0 && risk > 0 ? reward / risk : 0;
    const supportAnchor = nearestSupport;
    const anchorAligned = Number.isFinite(supportAnchor) &&
      Math.abs(stop - supportAnchor) <= Math.max(atrSafe * 0.55, entry * 0.0035);
    const entryNearAnchor = Number.isFinite(supportAnchor) &&
      (entry - supportAnchor) <= Math.max(atrSafe * 1.5, risk * 6, entry * 0.009);
    const emaAligned = entry > ema9 && ema9 > ema15;
    const emaPositionOk =
      stopCandle.low <= emaBandHigh + emaZonePadding &&
      stopCandle.high >= emaBandLow - emaZonePadding &&
      stopCandle.close >= emaBandLow &&
      distanceFromEmaAtr <= 1.15 &&
      candle.closeLocation >= 0.55;
    const volumeConfirmed = relativeVolume >= 1.15 && volumeTrend >= 1 && currentVolume >= Number(executionWindow.at(-2)?.volume || 0);
    const breakoutReady = !Number.isFinite(breakoutLevel) ||
      (breakoutLevel - entry) <= Math.max(atrSafe * 1.2, risk * 4, entry * 0.006);
    const exhausted =
      rsi15m >= 72 ||
      distanceFromEmaAtr > 1.3 ||
      (candle.upperWick > Math.max(candle.body * 1.15, candle.range * 0.28) && candle.closeLocation < 0.65);
    const checks = [
      {
        label: "Support",
        passed: anchorAligned && entryNearAnchor && breakoutReady,
        score: clamp(Math.round((anchorAligned ? 45 : 12) + (entryNearAnchor ? 25 : 0) + (breakoutReady ? 30 : 0)), 0, 100),
        detail: `Support ${roundPriceLevel(supportAnchor)} | breakout ${roundPriceLevel(breakoutLevel)}`,
      },
      {
        label: "EMA position",
        passed: emaAligned && emaPositionOk,
        score: clamp(Math.round((emaAligned ? 60 : 18) + (emaPositionOk ? 40 : Math.max(0, 25 - distanceFromEmaAtr * 18))), 0, 100),
        detail: `15m EMA9 ${formatSignedNumber((entry - ema9) / atrSafe)} ATR | EMA stack ${ema9 > ema15 ? "bullish" : "mixed"}`,
      },
      {
        label: "Volume",
        passed: volumeConfirmed,
        score: clamp(Math.round(relativeVolume * 55 + volumeTrend * 20), 0, 100),
        detail: `RVOL ${relativeVolume.toFixed(2)}x | 3-candle vol ${volumeTrend.toFixed(2)}x`,
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
        volumeConfirmed &&
        emaAligned &&
        emaPositionOk &&
        anchorAligned &&
        entryNearAnchor &&
        breakoutReady &&
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
  const stop = stopCandle.high;
  const targetBase = secondSupport ?? Math.min(structureLow, breakdownLevel - extensionTarget, breakdownLevel - bufferUnit);
  const target = Math.min(targetBase, breakdownLevel - extensionTarget * 0.5);
  const risk = stop - entry;
  const reward = entry - target;
  const ratio = reward > 0 && risk > 0 ? reward / risk : 0;
  const resistanceAnchor = nearestResistance;
  const anchorAligned = Number.isFinite(resistanceAnchor) &&
    Math.abs(stop - resistanceAnchor) <= Math.max(atrSafe * 0.55, entry * 0.0035);
  const entryNearAnchor = Number.isFinite(resistanceAnchor) &&
    (resistanceAnchor - entry) <= Math.max(atrSafe * 1.5, risk * 6, entry * 0.009);
  const emaAligned = entry < ema9 && ema9 < ema15;
  const emaPositionOk =
    stopCandle.high >= emaBandLow - emaZonePadding &&
    stopCandle.low <= emaBandHigh + emaZonePadding &&
    stopCandle.close <= emaBandHigh &&
    distanceFromEmaAtr <= 1.15 &&
    candle.closeLocation <= 0.45;
  const volumeConfirmed = relativeVolume >= 1.15 && volumeTrend >= 1 && currentVolume >= Number(executionWindow.at(-2)?.volume || 0);
  const breakdownReady = !Number.isFinite(breakdownLevel) ||
    (entry - breakdownLevel) <= Math.max(atrSafe * 1.2, risk * 4, entry * 0.006);
  const exhausted =
    rsi15m <= 28 ||
    distanceFromEmaAtr > 1.3 ||
    (candle.lowerWick > Math.max(candle.body * 1.15, candle.range * 0.28) && candle.closeLocation > 0.35);
  const checks = [
    {
      label: "Resistance",
      passed: anchorAligned && entryNearAnchor && breakdownReady,
      score: clamp(Math.round((anchorAligned ? 45 : 12) + (entryNearAnchor ? 25 : 0) + (breakdownReady ? 30 : 0)), 0, 100),
      detail: `Resistance ${roundPriceLevel(resistanceAnchor)} | breakdown ${roundPriceLevel(breakdownLevel)}`,
    },
    {
      label: "EMA position",
      passed: emaAligned && emaPositionOk,
      score: clamp(Math.round((emaAligned ? 60 : 18) + (emaPositionOk ? 40 : Math.max(0, 25 - distanceFromEmaAtr * 18))), 0, 100),
      detail: `15m EMA9 ${formatSignedNumber((entry - ema9) / atrSafe)} ATR | EMA stack ${ema9 < ema15 ? "bearish" : "mixed"}`,
    },
    {
      label: "Volume",
      passed: volumeConfirmed,
      score: clamp(Math.round(relativeVolume * 55 + volumeTrend * 20), 0, 100),
      detail: `RVOL ${relativeVolume.toFixed(2)}x | 3-candle vol ${volumeTrend.toFixed(2)}x`,
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
      volumeConfirmed &&
      emaAligned &&
      emaPositionOk &&
      anchorAligned &&
      entryNearAnchor &&
      breakdownReady &&
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

  const activeVolumeCheck = findCheck(activeSetup?.checks, "Volume");
  if (!activeVolumeCheck?.passed) {
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

function ratioScore(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) {
    return 0;
  }

  const cappedRatio = clamp(ratio, 1, 12);
  const normalized = (Math.log(cappedRatio) - Math.log(1)) / (Math.log(12) - Math.log(1));
  return clamp(Math.round(normalized * 100), 0, 100);
}

function calculateSetupScore(biasScore, readiness, setup, continuation) {
  const biasStrength = clamp(Math.abs(Number(biasScore) || 0), 0, 100);
  const readinessValue = readinessScore(readiness);
  const ratioValue = ratioScore(setup?.ratio);
  const passedChecks = Array.isArray(setup?.checks) ? setup.checks.filter((item) => item.passed).length : 0;
  const totalChecks = Array.isArray(setup?.checks) ? setup.checks.length : 0;
  const passRatio = totalChecks > 0 ? passedChecks / totalChecks : 0;
  const setupQuality = clamp(Math.round((Number(setup?.qualityScore) || 0) * passRatio), 0, 100);
  const continuationChecksPassed = Array.isArray(continuation?.checks)
    ? continuation.checks.filter((item) => item.passed).length
    : 0;
  const continuationChecksTotal = Array.isArray(continuation?.checks) ? continuation.checks.length : 0;
  const continuationPassRatio = continuationChecksTotal > 0
    ? continuationChecksPassed / continuationChecksTotal
    : 0;
  const continuationScore = clamp(
    Math.round((Number(continuation?.score) || 0) * continuationPassRatio),
    0,
    100,
  );
  const score = Math.round(
    biasStrength * 0.24 +
    readinessValue * 0.18 +
    ratioValue * 0.2 +
    setupQuality * 0.23 +
    continuationScore * 0.15,
  );

  return {
    score: clamp(score, 0, 100),
    breakdown: [
      {
        label: "Bias strength",
        score: biasStrength,
        detail: `Directional bias ${biasScore >= 0 ? "+" : ""}${biasScore}/100`,
      },
      {
        label: "Readiness",
        score: readinessValue,
        detail: readiness,
      },
      {
        label: "Potential",
        score: ratioValue,
        detail: Number.isFinite(Number(setup?.ratio)) ? `Reward to risk 1:${setup.ratio}` : "No valid setup ratio",
      },
      {
        label: "Setup quality",
        score: setupQuality,
        detail: totalChecks > 0 ? `${passedChecks}/${totalChecks} setup checks passed` : "No setup checks available",
      },
      {
        label: "5m + 15m continuation",
        score: continuationScore,
        detail: continuationChecksTotal > 0
          ? `${continuationChecksPassed}/${continuationChecksTotal} continuation checks passed`
          : "No continuation confirmation",
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
  if (!frame?.volumeConfirmed) {
    missing.push("volume");
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
  const volumeCheck = findCheck(setup?.checks, "Volume");

  return Boolean(
    biasAligned &&
    row.tradeReadiness !== "Avoid" &&
    (setup?.ratio ?? 0) > MIN_SETUP_RATIO &&
    checksPassed >= 2 &&
    emaCheck?.passed &&
    exhaustionCheck?.passed &&
    volumeCheck?.passed,
  );
}

function isContinuationWatchCandidate(row, side) {
  const continuation = row.continuations?.[side];
  const setup = row.setups?.[side];
  const frame5 = continuation?.frames?.["5m"];
  const frame15 = continuation?.frames?.["15m"];
  const biasAligned = side === "long" ? row.biasScore >= 20 : row.biasScore <= -20;
  const averageRvol = (Number(frame5?.relativeVolume || 0) + Number(frame15?.relativeVolume || 0)) / 2;
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
    structurePresent &&
    frame5?.volumeConfirmed &&
    frame15?.volumeConfirmed &&
    averageRvol >= 1.1,
  );
}

function isFrameContinuationCandidate(row, side, timeframe) {
  const continuation = row.continuations?.[side];
  const frame = continuation?.frames?.[timeframe];
  const setup = row.setups?.[side];
  const biasAligned = side === "long" ? row.biasScore >= 20 : row.biasScore <= -20;

  return Boolean(
    frame &&
    frame.sizeQualified &&
    frame.volumeConfirmed &&
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
  if ((rightFrame.relativeVolume ?? 0) !== (leftFrame.relativeVolume ?? 0)) {
    return (rightFrame.relativeVolume ?? 0) - (leftFrame.relativeVolume ?? 0);
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
  const readiness = tradeReadiness(
    weightedScore,
    timeframes,
    activeSetup,
  );
  const setupScore = calculateSetupScore(weightedScore, readiness, activeSetup, activeContinuation);

  return {
    symbol: ticker.symbol,
    underlying: ticker.underlying_asset_symbol || ticker.symbol.replace(/USD$|INR$/i, ""),
    description: ticker.description || "Perpetual futures",
    score: setupScore.score,
    biasScore: weightedScore,
    bias: summarizeBias(weightedScore),
    trend: dominantTrend(TIMEFRAMES.map((timeframe) => timeframeResults[timeframe.key].trendLabel)),
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
  const strongestBull = [...rawRows].sort((left, right) => right.biasScore - left.biasScore)[0] ||
    { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, biasScore: 0, rank: 0 };
  const strongestBear = [...rawRows].sort((left, right) => left.biasScore - right.biasScore)[0] ||
    { symbol: "--", bias: "Neutral", trend: "Range", price: null, score: 0, biasScore: 0, rank: 0 };
  const strongest24hLeader = [...rawRows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(right.change24h) - Number(left.change24h))[0] ||
    strongestBull;
  const strongest24hLaggard = [...rawRows]
    .filter((row) => Number.isFinite(Number(row.change24h)))
    .sort((left, right) => Number(left.change24h) - Number(right.change24h))[0] ||
    strongestBear;
  const majorIndex = new Map(PRIORITY_ASSETS.map((asset, index) => [asset, index]));
  const majors = rawRows
    .filter((row) => majorIndex.has(String(row.underlying).toUpperCase()))
    .sort((left, right) => majorIndex.get(String(left.underlying).toUpperCase()) - majorIndex.get(String(right.underlying).toUpperCase()));

  const headline =
    strongestBull.biasScore >= 25 || strongestBear.biasScore <= -25
      ? `${strongestBull.symbol} is strongest while ${strongestBear.symbol} is weakest across the ranked perpetual universe.`
      : "Market structure is mixed across the full perpetual universe.";
  const strictBullLeaders = rawRows
    .filter((row) =>
      row.biasScore > 0 &&
      row.tradeReadiness !== "Avoid" &&
      row.setup?.side === "long" &&
      (row.setup?.ratio ?? 0) > MIN_SETUP_RATIO &&
      row.setup.qualifies,
    )
    .sort((left, right) => {
      if (readinessRank(right.tradeReadiness) !== readinessRank(left.tradeReadiness)) {
        return readinessRank(right.tradeReadiness) - readinessRank(left.tradeReadiness);
      }
      if (Number(right.continuations?.long?.qualifies) !== Number(left.continuations?.long?.qualifies)) {
        return Number(right.continuations?.long?.qualifies) - Number(left.continuations?.long?.qualifies);
      }
      if ((right.continuations?.long?.score ?? 0) !== (left.continuations?.long?.score ?? 0)) {
        return (right.continuations?.long?.score ?? 0) - (left.continuations?.long?.score ?? 0);
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if (right.setup.ratio !== left.setup.ratio) {
        return right.setup.ratio - left.setup.ratio;
      }
      if (right.biasScore !== left.biasScore) {
        return right.biasScore - left.biasScore;
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 5);
  const strictBearLeaders = rawRows
    .filter((row) =>
      row.biasScore < 0 &&
      row.tradeReadiness !== "Avoid" &&
      row.setup?.side === "short" &&
      (row.setup?.ratio ?? 0) > MIN_SETUP_RATIO &&
      row.setup.qualifies,
    )
    .sort((left, right) => {
      if (readinessRank(right.tradeReadiness) !== readinessRank(left.tradeReadiness)) {
        return readinessRank(right.tradeReadiness) - readinessRank(left.tradeReadiness);
      }
      if (Number(right.continuations?.short?.qualifies) !== Number(left.continuations?.short?.qualifies)) {
        return Number(right.continuations?.short?.qualifies) - Number(left.continuations?.short?.qualifies);
      }
      if ((right.continuations?.short?.score ?? 0) !== (left.continuations?.short?.score ?? 0)) {
        return (right.continuations?.short?.score ?? 0) - (left.continuations?.short?.score ?? 0);
      }
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if (right.setup.ratio !== left.setup.ratio) {
        return right.setup.ratio - left.setup.ratio;
      }
      if (left.biasScore !== right.biasScore) {
        return left.biasScore - right.biasScore;
      }
      return right.liquidity - left.liquidity;
    })
    .slice(0, 5);
  const qualifiedBullLeaders = strictBullLeaders.map((row) => decorateSetupLeader(row, "long", "confirmed"));
  const qualifiedBearLeaders = strictBearLeaders.map((row) => decorateSetupLeader(row, "short", "confirmed"));
  const strictContinuationLongLeaders = rawRows
    .filter((row) => row.continuations?.long?.qualifies)
    .sort((left, right) => {
      if ((right.continuations.long.score ?? 0) !== (left.continuations.long.score ?? 0)) {
        return (right.continuations.long.score ?? 0) - (left.continuations.long.score ?? 0);
      }
      if ((right.continuations.long.volumeScore ?? 0) !== (left.continuations.long.volumeScore ?? 0)) {
        return (right.continuations.long.volumeScore ?? 0) - (left.continuations.long.volumeScore ?? 0);
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
      if ((right.continuations.short.volumeScore ?? 0) !== (left.continuations.short.volumeScore ?? 0)) {
        return (right.continuations.short.volumeScore ?? 0) - (left.continuations.short.volumeScore ?? 0);
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
      if ((right.continuations?.long?.volumeScore ?? 0) !== (left.continuations?.long?.volumeScore ?? 0)) {
        return (right.continuations?.long?.volumeScore ?? 0) - (left.continuations?.long?.volumeScore ?? 0);
      }
      return right.liquidity - left.liquidity;
    });
  const continuationShortWatchlist = rawRows
    .filter((row) => isContinuationWatchCandidate(row, "short"))
    .sort((left, right) => {
      if ((right.continuations?.short?.score ?? 0) !== (left.continuations?.short?.score ?? 0)) {
        return (right.continuations?.short?.score ?? 0) - (left.continuations?.short?.score ?? 0);
      }
      if ((right.continuations?.short?.volumeScore ?? 0) !== (left.continuations?.short?.volumeScore ?? 0)) {
        return (right.continuations?.short?.volumeScore ?? 0) - (left.continuations?.short?.volumeScore ?? 0);
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
