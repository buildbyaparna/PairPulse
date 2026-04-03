const els = {
  searchShell: document.querySelector(".search-shell"),
  searchInput: document.querySelector("#search-input"),
  searchButton: document.querySelector("#search-button"),
  searchSuggestions: document.querySelector("#search-suggestions"),
  loadingState: document.querySelector("#loading-state"),
  emptyState: document.querySelector("#empty-state"),
  heroHeadline: document.querySelector("#hero-headline"),
  heroSubtext: document.querySelector("#hero-subtext"),
  trackedPairs: document.querySelector("#tracked-pairs"),
  bullishCount: document.querySelector("#bullish-count"),
  bearishCount: document.querySelector("#bearish-count"),
  neutralCount: document.querySelector("#neutral-count"),
  lastRefresh: document.querySelector("#last-refresh"),
  strongestBullSymbol: document.querySelector("#strongest-bull-symbol"),
  strongestBullMeta: document.querySelector("#strongest-bull-meta"),
  strongestBullBias: document.querySelector("#strongest-bull-bias"),
  strongestBearSymbol: document.querySelector("#strongest-bear-symbol"),
  strongestBearMeta: document.querySelector("#strongest-bear-meta"),
  strongestBearBias: document.querySelector("#strongest-bear-bias"),
  majorsGrid: document.querySelector("#majors-grid"),
  bullList: document.querySelector("#bull-list"),
  bearList: document.querySelector("#bear-list"),
  continuationLegend: document.querySelector("#continuation-legend"),
  continuation5mLongList: document.querySelector("#continuation-5m-long-list"),
  continuation5mShortList: document.querySelector("#continuation-5m-short-list"),
  continuation15mLongList: document.querySelector("#continuation-15m-long-list"),
  continuation15mShortList: document.querySelector("#continuation-15m-short-list"),
  rankingList: document.querySelector("#ranking-list"),
  resultsNote: document.querySelector("#results-note"),
  rankingLegend: document.querySelector(".ranking-legend span"),
  pairModal: document.querySelector("#pair-modal"),
  pairModalSymbol: document.querySelector("#pair-modal-symbol"),
  pairModalMeta: document.querySelector("#pair-modal-meta"),
  pairModalBias: document.querySelector("#pair-modal-bias"),
  pairModalReadiness: document.querySelector("#pair-modal-readiness"),
  pairModalScore: document.querySelector("#pair-modal-score"),
  pairModalTimeframes: document.querySelector("#pair-modal-timeframes"),
  pairModalClose: document.querySelector("#pair-modal-close"),
  majorCardTemplate: document.querySelector("#major-card-template"),
  signalRowTemplate: document.querySelector("#signal-row-template"),
  rankingRowTemplate: document.querySelector("#ranking-row-template"),
};

const ACTIVE_TIMEFRAMES = ["15m", "1h", "4h", "12h", "24h"];

const state = {
  isLoading: false,
  payload: null,
};

function normalizeSearchValue(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function getSearchRank(row, query) {
  if (!query) {
    return 0;
  }

  const symbol = normalizeSearchValue(row.symbol);
  const underlying = normalizeSearchValue(row.underlying);

  if (symbol === query) {
    return 100;
  }
  if (underlying === query) {
    return 95;
  }
  if (symbol.startsWith(query)) {
    return 80;
  }
  if (underlying.startsWith(query)) {
    return 70;
  }
  if (symbol.includes(query)) {
    return 60;
  }
  if (underlying.includes(query)) {
    return 50;
  }

  return -1;
}

function getSearchRows(query) {
  if (!state.payload) {
    return [];
  }

  if (!query) {
    return [...state.payload.allRows];
  }

  return state.payload.allRows
    .map((row) => ({ row, searchRank: getSearchRank(row, query) }))
    .filter((item) => item.searchRank >= 0)
    .sort((left, right) => {
      if (right.searchRank !== left.searchRank) {
        return right.searchRank - left.searchRank;
      }
      if ((right.row.setup?.ratio ?? 0) !== (left.row.setup?.ratio ?? 0)) {
        return (right.row.setup?.ratio ?? 0) - (left.row.setup?.ratio ?? 0);
      }
      if (right.row.score !== left.row.score) {
        return right.row.score - left.row.score;
      }
      return Math.abs(right.row.biasScore) - Math.abs(left.row.biasScore);
    })
    .map((item) => item.row);
}

async function fetchJson(url) {
  const response = await fetch(url, {
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data?.error?.message || `Request failed: ${response.status}`);
  }
  return data;
}

function toneForLabel(label) {
  if (label.includes("Bull")) {
    return "bull";
  }
  if (label.includes("Bear")) {
    return "bear";
  }
  return "neutral";
}

function formatPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "N/A";
  }
  if (numeric >= 1000) {
    return numeric.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  if (numeric >= 1) {
    return numeric.toLocaleString("en-US", { maximumFractionDigits: 4 });
  }
  return numeric.toLocaleString("en-US", { maximumFractionDigits: 8 });
}

function formatSignedScore(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "N/A";
  }

  return `${numeric > 0 ? "+" : ""}${numeric}`;
}

function formatPotentialRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "N/A";
  }

  return `1:${numeric.toFixed(Number.isInteger(numeric) ? 0 : 1)}`;
}

function formatSetupLabel(row, { includeSide = false } = {}) {
  const setup = row?.setup;
  const ratioLabel = formatPotentialRatio(setup?.ratio);
  if (ratioLabel === "N/A") {
    return "Risk:Reward N/A";
  }

  if (!includeSide) {
    return `Risk:Reward ${ratioLabel}`;
  }

  const sideLabel = setup?.side === "short" ? "Short" : "Long";
  return `${sideLabel} | Risk:Reward ${ratioLabel}`;
}

function formatSetupPlan(row) {
  const setup = row?.setup;
  const setupLabel = formatSetupLabel(row, { includeSide: true });
  if (setupLabel === "Risk:Reward N/A") {
    return setupLabel;
  }

  return `${setupLabel} | 15m SL ${formatPrice(setup?.stop)} | target ${formatPrice(setup?.target)}`;
}

function formatVolumeMultiple(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "N/A";
  }

  return `${numeric.toFixed(2)}x`;
}

function formatSignalMultiple(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "N/A";
  }

  return `${numeric.toFixed(2)}x`;
}

function getContinuation(row, side) {
  return row?.continuations?.[side] || null;
}

function getContinuationFrame(row, side, timeframe) {
  return row?.continuations?.[side]?.frames?.[timeframe] || row?.continuationFrame || null;
}

function formatContinuationLabel(row, side) {
  const continuation = getContinuation(row, side);
  if (!continuation) {
    return "No continuation scan";
  }

  const frame5 = continuation.frames?.["5m"];
  const frame15 = continuation.frames?.["15m"];
  const directionLabel = side === "short" ? "Breakdown" : "Breakout";

  return `${directionLabel} | BOS + CHoCH | EMA 9/15 | 5m size ${formatSignalMultiple(frame5?.expansionMultiple)} | 15m size ${formatSignalMultiple(frame15?.expansionMultiple)} | 5m vol ${formatVolumeMultiple(frame5?.relativeVolume)} | 15m vol ${formatVolumeMultiple(frame15?.relativeVolume)} | room ${formatPotentialRatio(continuation?.ratio)}`;
}

function formatFrameContinuationLabel(row, side, timeframe) {
  const frame = getContinuationFrame(row, side, timeframe);
  if (!frame) {
    return `${timeframe} candle data unavailable`;
  }

  const directionLabel = side === "short" ? "Breakdown" : "Breakout";
  return `${timeframe} ${directionLabel} | size ${formatSignalMultiple(frame?.expansionMultiple)} | vol ${formatVolumeMultiple(frame?.relativeVolume)} | BOS ${frame?.bos ? "yes" : "no"} | CHoCH ${frame?.choch ? "yes" : "no"} | EMA 9/15 ${frame?.emaRetest ? "yes" : "no"} | room ${formatPotentialRatio(row?.setup?.ratio)}`;
}

function buildPill(label, extraClass = "") {
  const span = document.createElement("span");
  span.className = `bias-pill ${toneForLabel(label)} ${extraClass}`.trim();
  span.textContent = label;
  return span;
}

function readinessTone(label) {
  if (label.includes("Long")) {
    return "long";
  }
  if (label.includes("Short")) {
    return "short";
  }
  if (label === "Avoid") {
    return "avoid";
  }
  return "wait";
}

function buildReadinessPill(label, extraClass = "") {
  const span = document.createElement("span");
  span.className = `readiness-pill ${readinessTone(label)} ${extraClass}`.trim();
  span.textContent = label;
  return span;
}

function buildScoreTooltip(row) {
  const checks = row.setup?.checks || [];
  const emaCheck = checks.find((item) => item.label === "EMA position");
  const trendCheck = checks.find((item) => item.label === "Trend");
  const exhaustionCheck = checks.find((item) => item.label === "Exhaustion");
  const setupLine = row.setup && formatPotentialRatio(row.setup?.ratio) !== "N/A"
    ? `Setup: ${formatSetupPlan(row)}${row.setup.qualifies ? "" : " | blocked by trend filters"}`
    : "Setup: Risk:Reward not available yet";
  const timeframeLine = `15m ${row.timeframes?.["15m"] || "N/A"} | 1h ${row.timeframes?.["1h"] || "N/A"} | 4h ${row.timeframes?.["4h"] || "N/A"}`;
  const trendLine = trendCheck
    ? `Trend: ${trendCheck.passed ? "Pass" : "Fail"} | ${trendCheck.detail}`
    : "Trend: N/A";
  const emaLine = emaCheck
    ? `EMA 9/15: ${emaCheck.passed ? "Pass" : "Fail"}`
    : "EMA 9/15: N/A";
  const exhaustionLine = exhaustionCheck
    ? `Exhaustion: ${exhaustionCheck.passed ? "Pass" : "Fail"}`
    : "Exhaustion: N/A";

  return `Score ${row.score}/100 | Bias ${formatSignedScore(row.biasScore)} | ${row.tradeReadiness}\n${setupLine}\n${timeframeLine}\n${trendLine}\n${emaLine} | ${exhaustionLine}`;
}

function buildContinuationTooltip(row, side) {
  const continuation = getContinuation(row, side);
  if (!continuation) {
    return buildScoreTooltip(row);
  }

  const frameLines = ["5m", "15m"]
    .map((timeframe) => {
      const frame = continuation.frames?.[timeframe];
      return `${timeframe}: ${frame?.detail || "No signal"} | vol ${formatVolumeMultiple(frame?.relativeVolume)}`;
    })
    .join("\n");
  const checkLines = (continuation.checks || [])
    .map((item) => `${item.label}: ${item.passed ? "Pass" : "Fail"} | ${item.detail}`)
    .join("\n");

  return `Continuation score: ${continuation.score}/100\nSide: ${side === "short" ? "Short" : "Long"}\nContinuation: ${formatContinuationLabel(row, side)}\nChecks:\n${checkLines}\nFrames:\n${frameLines}`;
}

function buildFrameContinuationTooltip(row, side, timeframe) {
  const frame = getContinuationFrame(row, side, timeframe);
  if (!frame) {
    return buildScoreTooltip(row);
  }

  return `${timeframe} frame score: ${frame.score}/100\nSide: ${side === "short" ? "Short" : "Long"}\nSignal: ${formatFrameContinuationLabel(row, side, timeframe)}\nTrend aligned: ${frame.trendAligned ? "Yes" : "No"}\nBOS: ${frame.bos ? "Yes" : "No"}\nCHoCH: ${frame.choch ? "Yes" : "No"}\nEMA 9/15 retest: ${frame.emaRetest ? "Yes" : "No"}\nSize vs consolidation: ${formatSignalMultiple(frame.expansionMultiple)}\nRelative volume: ${formatVolumeMultiple(frame.relativeVolume)}\n3-bar volume: ${formatVolumeMultiple(frame.volumeTrend)}\n${row.continuationNote || ""}`;
}

function applyScoreInfo(container, row, tooltipOverride = null) {
  const wrap = container.querySelector(".score-wrap");
  const tooltip = tooltipOverride || buildScoreTooltip(row);
  wrap.setAttribute("data-tooltip", tooltip);
}

function renderMajorCards(rows) {
  els.majorsGrid.textContent = "";
  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const card = els.majorCardTemplate.content.firstElementChild.cloneNode(true);
    card.querySelector(".major-symbol").textContent = row.symbol;
    card.querySelector(".major-price").textContent = formatPrice(row.price);
    card.querySelector(".major-badges .readiness-pill").replaceWith(buildReadinessPill(row.tradeReadiness));
    card.querySelector(".major-badges .bias-pill").replaceWith(buildPill(row.bias));
    card.querySelector(".score-pill").textContent = `Score ${row.score}`;
    applyScoreInfo(card, row);

    const timeframeRow = card.querySelector(".timeframe-row");
    for (const timeframe of ACTIVE_TIMEFRAMES) {
      timeframeRow.appendChild(buildPill(`${timeframe} ${row.timeframes[timeframe]}`, "time-pill"));
    }

    fragment.appendChild(card);
  }

  els.majorsGrid.appendChild(fragment);
}

function renderSignalList(container, rows) {
  if (!container) {
    return;
  }

  const visibleRows = rows.slice(0, 5);
  container.textContent = "";
  if (visibleRows.length === 0) {
    const emptyState = document.createElement("p");
    const sideLabel = container === els.bullList ? "long" : "short";
    emptyState.className = "signal-empty";
    emptyState.textContent =
      `No ${sideLabel} setups above ${formatPotentialRatio(state.payload?.minimumSetupRatio || 5)} potential right now.`;
    container.appendChild(emptyState);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const row of visibleRows) {
    const item = els.signalRowTemplate.content.firstElementChild.cloneNode(true);
    item.querySelector(".signal-symbol").textContent = row.symbol;
    item.querySelector(".signal-meta").textContent =
      `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}% | ${formatSetupLabel(row)}`;
    item.querySelector(".readiness-pill").replaceWith(buildReadinessPill(row.tradeReadiness));
    item.querySelector(".bias-pill").replaceWith(buildPill(row.bias));
    item.querySelector(".score-pill").textContent = `Score ${row.score}`;
    applyScoreInfo(item, row);
    fragment.appendChild(item);
  }

  container.appendChild(fragment);
}

function renderContinuationList(container, rows, side) {
  if (!container) {
    return;
  }

  container.textContent = "";
  if (rows.length === 0) {
    const emptyState = document.createElement("p");
    emptyState.className = "signal-empty";
    emptyState.textContent = side === "long"
      ? "No 5m + 15m breakout continuation with BOS, CHoCH, and EMA 9/15 alignment right now."
      : "No 5m + 15m breakdown continuation with BOS, CHoCH, and EMA 9/15 alignment right now.";
    container.appendChild(emptyState);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const continuation = getContinuation(row, side);
    const item = els.signalRowTemplate.content.firstElementChild.cloneNode(true);
    item.querySelector(".signal-symbol").textContent = row.symbol;
    item.querySelector(".signal-meta").textContent =
      `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}% | ${formatContinuationLabel(row, side)}`;
    item.querySelector(".readiness-pill").replaceWith(
      buildReadinessPill(row.tradeReadiness || (side === "long" ? "Long Continuation" : "Short Continuation")),
    );
    item.querySelector(".bias-pill").replaceWith(buildPill(row.bias));
    item.querySelector(".score-pill").textContent = `Score ${continuation?.score ?? row.score}`;
    applyScoreInfo(item, row, buildContinuationTooltip(row, side));
    fragment.appendChild(item);
  }

  container.appendChild(fragment);
}

function renderFrameContinuationList(container, rows, side, timeframe) {
  if (!container) {
    return;
  }

  container.textContent = "";
  if (rows.length === 0) {
    const emptyState = document.createElement("p");
    emptyState.className = "signal-empty";
    emptyState.textContent = `${timeframe} ${side === "long" ? "breakout" : "breakdown"} list has no pairs with BOS yes, CHoCH yes, EMA 9/15 yes, and size above 3x right now.`;
    container.appendChild(emptyState);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const frame = getContinuationFrame(row, side, timeframe);
    const item = els.signalRowTemplate.content.firstElementChild.cloneNode(true);
    item.querySelector(".signal-symbol").textContent = row.symbol;
    item.querySelector(".signal-meta").textContent =
      `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}% | ${formatFrameContinuationLabel(row, side, timeframe)}`;
    item.querySelector(".readiness-pill").replaceWith(buildReadinessPill(row.tradeReadiness || `${timeframe} ${side === "short" ? "Short" : "Long"}`));
    item.querySelector(".bias-pill").replaceWith(buildPill(row.bias));
    item.querySelector(".score-pill").textContent = `Score ${frame?.score ?? row.score}`;
    applyScoreInfo(item, row, buildFrameContinuationTooltip(row, side, timeframe));
    fragment.appendChild(item);
  }

  container.appendChild(fragment);
}

function renderRankingList(rows) {
  els.rankingList.textContent = "";
  els.resultsNote.textContent = `${rows.length} results`;
  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const item = els.rankingRowTemplate.content.firstElementChild.cloneNode(true);
    item.querySelector(".rank-pill").textContent = `#${fragment.childNodes.length + 1}`;
    item.querySelector(".ranking-symbol").textContent = row.symbol;
    item.querySelector(".ranking-meta").textContent =
      `${row.description} | ${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}% | ${formatSetupLabel(row, { includeSide: true })}`;

    const timeframeShell = item.querySelector(".ranking-timeframes");
    for (const timeframe of ACTIVE_TIMEFRAMES) {
      timeframeShell.appendChild(buildPill(`${timeframe} ${row.timeframes[timeframe]}`, "time-pill"));
    }

    item.querySelector(".readiness-pill").replaceWith(buildReadinessPill(row.tradeReadiness));
    item.querySelector(".bias-pill").replaceWith(buildPill(row.bias));
    item.querySelector(".score-pill").textContent = `Score ${row.score}`;
    applyScoreInfo(item, row);
    fragment.appendChild(item);
  }

  els.rankingList.appendChild(fragment);
}

function applySearch() {
  if (!state.payload) {
    return;
  }

  const query = normalizeSearchValue(els.searchInput.value.trim());
  const rows = getSearchRows(query);

  renderRankingList(rows);

  if (query && rows.length > 0) {
    els.resultsNote.textContent = rows.length === 1 ? "1 matching pair" : `${rows.length} matching pairs`;
  }

  if (query && rows.length === 0) {
    els.resultsNote.textContent = "No matching pair";
  }

  renderSuggestions(query, rows);
}

function renderSuggestions(query, rows) {
  els.searchSuggestions.textContent = "";

  if (!query) {
    return;
  }

  const fragment = document.createDocumentFragment();
  rows.slice(0, 5).forEach((row) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "suggestion-item";
    button.textContent = `${row.symbol} | ${row.tradeReadiness} | ${formatSetupLabel(row, { includeSide: true })} | Score ${row.score}`;
    button.addEventListener("click", () => {
      els.searchInput.value = row.symbol;
      openPairModal(row);
      els.searchSuggestions.textContent = "";
    });
    fragment.appendChild(button);
  });

  els.searchSuggestions.appendChild(fragment);
}

function findBestSearchMatch(query) {
  if (!state.payload || !query) {
    return null;
  }

  return getSearchRows(query)[0] || null;
}

function openPairModal(row) {
  if (!row) {
    return;
  }

  els.pairModalSymbol.textContent = row.symbol;
  els.pairModalMeta.textContent =
    `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}% | ${formatSetupPlan(row)}`;
  els.pairModalBias.className = `bias-pill ${toneForLabel(row.bias)}`;
  els.pairModalBias.textContent = row.bias;
  els.pairModalReadiness.replaceWith(buildReadinessPill(row.tradeReadiness));
  els.pairModalReadiness = els.pairModal.querySelector(".readiness-pill");
  els.pairModalScore.textContent = `Score ${row.score}`;
  els.pairModalTimeframes.textContent = "";

  for (const timeframe of ACTIVE_TIMEFRAMES) {
    els.pairModalTimeframes.appendChild(buildPill(`${timeframe} ${row.timeframes[timeframe]}`, "time-pill"));
  }

  els.pairModal.classList.remove("hidden");
  els.pairModal.setAttribute("aria-hidden", "false");
}

function closePairModal() {
  els.pairModal.classList.add("hidden");
  els.pairModal.setAttribute("aria-hidden", "true");
}

function renderSnapshot(payload) {
  const {
    breadth,
    strongestBull,
    strongestBear,
    majors,
    leaders,
    continuationFrames,
    signalDiagnostics,
    generatedAt,
    minimumSetupRatio,
  } = payload;
  state.payload = payload;
  const minimumRatioLabel = formatPotentialRatio(minimumSetupRatio || 5);
  const strictLongCount = Number(signalDiagnostics?.strictLongCount || 0);
  const strictShortCount = Number(signalDiagnostics?.strictShortCount || 0);
  const frameContinuationCount =
    Number(signalDiagnostics?.frameContinuation5mLongCount || 0) +
    Number(signalDiagnostics?.frameContinuation5mShortCount || 0) +
    Number(signalDiagnostics?.frameContinuation15mLongCount || 0) +
    Number(signalDiagnostics?.frameContinuation15mShortCount || 0);

  els.heroHeadline.textContent = `${strongestBull.symbol} leads. ${strongestBear.symbol} lags.`;
  els.heroSubtext.textContent =
    `Score now blends bias strength, readiness, reward:risk, setup quality, and 5m/15m continuation confirmation. Visible pair labels always show the active setup Risk:Reward, while leader lists still only show setups above ${minimumRatioLabel} potential with clean EMA 9/15 position, confirmed trend direction, and no exhaustion on the latest 15m candle.`;
  if (els.continuationLegend) {
    els.continuationLegend.textContent = frameContinuationCount
      ? `Showing 5m and 15m candle data separately. Each list only includes pairs where that timeframe has BOS yes, CHoCH yes, EMA 9/15 yes, and candle size above 3x of its last consolidation base.`
      : `No 5m or 15m candle currently passes the BOS, CHoCH, EMA 9/15, and 3x consolidation-size filter on this snapshot.`;
  }
  els.rankingLegend.textContent =
    strictLongCount || strictShortCount
      ? `Searchable pair ranking is sorted from highest to lowest active Risk:Reward. Bias still comes from 15m, 1h, 4h, 12h, and 24h, while leader lists prefer clean 5m/15m continuation structure on top of support or resistance alignment, setups above ${minimumRatioLabel} potential, and a non-exhausted latest 15m candle.`
      : `No setup passed every strict filter on this snapshot, so the best long and short setup panels are empty. Searchable pair ranking is still sorted from highest to lowest active Risk:Reward.`;
  els.trackedPairs.textContent = String(breadth.tracked);
  els.bullishCount.textContent = String(breadth.bullish);
  els.bearishCount.textContent = String(breadth.bearish);
  els.neutralCount.textContent = String(breadth.neutral);
  els.lastRefresh.textContent = new Date(generatedAt).toLocaleString("en-IN", {
    dateStyle: "medium",
    timeStyle: "short",
  });

  els.strongestBullSymbol.textContent = strongestBull.symbol;
  els.strongestBullMeta.textContent =
    `${strongestBull.trend} | ${formatPrice(strongestBull.price)} | 24h ${Number(strongestBull.change24h || 0).toFixed(2)}% | ${formatSetupLabel(strongestBull)} | Score ${strongestBull.score} | Bias ${formatSignedScore(strongestBull.biasScore)}`;
  els.strongestBullBias.className = `bias-pill ${toneForLabel(strongestBull.bias)}`;
  els.strongestBullBias.textContent = strongestBull.bias;

  els.strongestBearSymbol.textContent = strongestBear.symbol;
  els.strongestBearMeta.textContent =
    `${strongestBear.trend} | ${formatPrice(strongestBear.price)} | 24h ${Number(strongestBear.change24h || 0).toFixed(2)}% | ${formatSetupLabel(strongestBear)} | Score ${strongestBear.score} | Bias ${formatSignedScore(strongestBear.biasScore)}`;
  els.strongestBearBias.className = `bias-pill ${toneForLabel(strongestBear.bias)}`;
  els.strongestBearBias.textContent = strongestBear.bias;

  renderMajorCards(majors);
  renderSignalList(els.bullList, leaders.bulls);
  renderSignalList(els.bearList, leaders.bears);
  renderFrameContinuationList(els.continuation5mLongList, continuationFrames?.["5m"]?.longs || [], "long", "5m");
  renderFrameContinuationList(els.continuation5mShortList, continuationFrames?.["5m"]?.shorts || [], "short", "5m");
  renderFrameContinuationList(els.continuation15mLongList, continuationFrames?.["15m"]?.longs || [], "long", "15m");
  renderFrameContinuationList(els.continuation15mShortList, continuationFrames?.["15m"]?.shorts || [], "short", "15m");
  applySearch();
}

async function loadDashboard() {
  if (state.isLoading) {
    return;
  }

  state.isLoading = true;
  if (!state.payload) {
    els.loadingState.classList.remove("hidden");
  }
  els.emptyState.classList.add("hidden");
  try {
    const payload = await fetchJson(`/api/dashboard?ts=${Date.now()}`);
    renderSnapshot(payload);
  } catch (error) {
    console.error(error);
    els.emptyState.classList.remove("hidden");
    els.emptyState.querySelector("h3").textContent = "Data unavailable.";
    els.emptyState.querySelector("p").textContent = error.message;
  } finally {
    state.isLoading = false;
    els.loadingState.classList.add("hidden");
  }
}

if (window.location.protocol === "file:") {
  els.loadingState.classList.add("hidden");
  els.emptyState.classList.remove("hidden");
  els.emptyState.querySelector("h3").textContent = "Open this dashboard through the local server.";
  els.emptyState.querySelector("p").textContent =
    "Run `node server.js`, then open http://localhost:3000.";
} else {
  loadDashboard();
}

els.searchInput.addEventListener("input", applySearch);
els.searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    const query = normalizeSearchValue(els.searchInput.value.trim());
    const match = findBestSearchMatch(query);

    if (match) {
      openPairModal(match);
      els.searchSuggestions.textContent = "";
      return;
    }

    applySearch();
  }
});
els.searchButton.addEventListener("click", () => {
  const query = normalizeSearchValue(els.searchInput.value.trim());
  const match = findBestSearchMatch(query);

  if (match) {
    openPairModal(match);
    els.searchSuggestions.textContent = "";
    return;
  }

  applySearch();
});
els.pairModalClose.addEventListener("click", closePairModal);
els.pairModal.addEventListener("click", (event) => {
  if (event.target === els.pairModal) {
    closePairModal();
  }
});
document.addEventListener("click", (event) => {
  if (!els.searchShell.contains(event.target)) {
    els.searchSuggestions.textContent = "";
  }
});

setInterval(() => {
  if (window.location.protocol !== "file:") {
    loadDashboard();
  }
}, 20000);
