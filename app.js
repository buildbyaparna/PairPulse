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
  rankingList: document.querySelector("#ranking-list"),
  resultsNote: document.querySelector("#results-note"),
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
const TIMEFRAME_WEIGHTS = {
  "15m": 4,
  "1h": 5,
  "4h": 3,
  "12h": 2,
  "24h": 1,
};

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
    return [...state.payload.allRows].sort((left, right) => left.rank - right.rank);
  }

  return state.payload.allRows
    .map((row) => ({ row, searchRank: getSearchRank(row, query) }))
    .filter((item) => item.searchRank >= 0)
    .sort((left, right) => {
      if (right.searchRank !== left.searchRank) {
        return right.searchRank - left.searchRank;
      }
      return left.row.rank - right.row.rank;
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
  const totalWeight = Object.values(TIMEFRAME_WEIGHTS).reduce((sum, value) => sum + value, 0);
  const lines = row.scoreBreakdown
    .map((item) => {
      const weight = TIMEFRAME_WEIGHTS[item.timeframe] || 1;
      const contribution = Math.round((item.score * weight) / totalWeight);
      const sign = contribution > 0 ? "+" : "";
      return `${item.timeframe}: ${item.score}/100 | weight ${weight} | contribution ${sign}${contribution}`;
    })
    .join("\n");

  return `Total score: ${row.score}/100\n${lines}`;
}

function applyScoreInfo(container, row) {
  const wrap = container.querySelector(".score-wrap");
  const tooltip = buildScoreTooltip(row);
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
  container.textContent = "";
  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const item = els.signalRowTemplate.content.firstElementChild.cloneNode(true);
    item.querySelector(".signal-symbol").textContent = row.symbol;
    item.querySelector(".signal-meta").textContent =
      `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}%`;
    item.querySelector(".readiness-pill").replaceWith(buildReadinessPill(row.tradeReadiness));
    item.querySelector(".bias-pill").replaceWith(buildPill(row.bias));
    item.querySelector(".score-pill").textContent = `Score ${row.score}`;
    applyScoreInfo(item, row);
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
    item.querySelector(".rank-pill").textContent = `#${row.rank}`;
    item.querySelector(".ranking-symbol").textContent = row.symbol;
    item.querySelector(".ranking-meta").textContent =
      `${row.description} | ${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}%`;

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
    button.textContent = `${row.symbol} | ${row.tradeReadiness} | Score ${row.score}`;
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
    `${row.trend} | ${formatPrice(row.price)} | 24h ${Number(row.change24h || 0).toFixed(2)}%`;
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
  const { breadth, strongestBull, strongestBear, majors, leaders, generatedAt } = payload;
  state.payload = payload;

  els.heroHeadline.textContent = `${strongestBull.symbol} leads. ${strongestBear.symbol} lags.`;
  els.heroSubtext.textContent =
    "Score uses EMA trend, RSI momentum, ADX strength, and price action on 15m, 1h, 4h, 12h, and 24h. Shorter charts drive entries, longer charts confirm direction.";
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
    `${strongestBull.trend} | ${formatPrice(strongestBull.price)} | 24h ${Number(strongestBull.change24h || 0).toFixed(2)}% | Score ${strongestBull.score}`;
  els.strongestBullBias.className = `bias-pill ${toneForLabel(strongestBull.bias)}`;
  els.strongestBullBias.textContent = strongestBull.bias;

  els.strongestBearSymbol.textContent = strongestBear.symbol;
  els.strongestBearMeta.textContent =
    `${strongestBear.trend} | ${formatPrice(strongestBear.price)} | 24h ${Number(strongestBear.change24h || 0).toFixed(2)}% | Score ${strongestBear.score}`;
  els.strongestBearBias.className = `bias-pill ${toneForLabel(strongestBear.bias)}`;
  els.strongestBearBias.textContent = strongestBear.bias;

  renderMajorCards(majors);
  renderSignalList(els.bullList, leaders.bulls);
  renderSignalList(els.bearList, leaders.bears);
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
