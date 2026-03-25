const DASHBOARD_CONFIG = window.MOMENTUM_DASHBOARD_CONFIG ?? {};
const DATA_BASE_URL = normalizeDataBaseUrl(DASHBOARD_CONFIG.dataBaseUrl);
const BACKTEST_HISTORY_URL = resolveDataUrl("./data/backtests/index.json");
const LIVE_HISTORY_URL = resolveDataUrl("./data/live/history.json");
const ERROR_HISTORY_URL = resolveDataUrl("./data/errors/history.json");
const LIVE_TIMEFRAME_ORDER = ["1M", "3M", "1A"];

const state = {
    activeTab: "live",
    errors: {
        history: null
    },
    backtest: {
        history: null,
        selectedRunId: null,
        runDetails: {},
        error: null
    },
    live: {
        history: null,
        selectedRunId: null,
        selectedTimeframe: "1M",
        runDetails: {},
        error: null
    }
};

function normalizeDataBaseUrl(value) {
    if (!value) {
        return "";
    }

    return String(value).trim().replace(/\/+$/, "");
}

function resolveDataUrl(path) {
    if (!path) {
        return path;
    }

    if (/^https?:\/\//i.test(path)) {
        return path;
    }

    if (!DATA_BASE_URL) {
        return path;
    }

    const normalizedPath = String(path)
        .trim()
        .replace(/^\.\//, "")
        .replace(/^\/+/, "");

    if (normalizedPath.startsWith("data/")) {
        return `${DATA_BASE_URL}/${normalizedPath.slice("data/".length)}`;
    }

    return `${DATA_BASE_URL}/${normalizedPath}`;
}

function normalizeArtifactPaths(value) {
    if (!value || typeof value !== "object") {
        return value;
    }

    if (Array.isArray(value)) {
        return value.map((entry) => normalizeArtifactPaths(entry));
    }

    const normalized = { ...value };

    ["detail_path", "chart_path", "results_path"].forEach((key) => {
        if (typeof normalized[key] === "string" && normalized[key]) {
            normalized[key] = resolveDataUrl(normalized[key]);
        }
    });

    Object.entries(normalized).forEach(([key, entry]) => {
        if (entry && typeof entry === "object") {
            normalized[key] = normalizeArtifactPaths(entry);
        }
    });

    return normalized;
}

function safeArray(value) {
    return Array.isArray(value) ? value : [];
}

function asNumber(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }

    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

function formatCurrency(value, digits = 0) {
    const numeric = asNumber(value);
    if (numeric === null) {
        return "—";
    }

    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    }).format(numeric);
}

function formatSignedCurrency(value, digits = 0) {
    const numeric = asNumber(value);
    if (numeric === null) {
        return "—";
    }

    const prefix = numeric > 0 ? "+" : "";
    return `${prefix}${formatCurrency(numeric, digits)}`;
}

function formatPercentValue(value, digits = 1, showPlus = true) {
    const numeric = asNumber(value);
    if (numeric === null) {
        return "—";
    }

    const prefix = showPlus && numeric > 0 ? "+" : "";
    return `${prefix}${numeric.toFixed(digits)}%`;
}

function formatRatioAsPercent(value, digits = 1) {
    const numeric = asNumber(value);
    if (numeric === null) {
        return "—";
    }

    return `${(numeric * 100).toFixed(digits)}%`;
}

function formatCompactNumber(value, digits = 0) {
    const numeric = asNumber(value);
    if (numeric === null) {
        return "—";
    }

    return new Intl.NumberFormat("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    }).format(numeric);
}

function formatShortDate(value) {
    if (!value) {
        return "—";
    }

    return new Date(value).toLocaleDateString("en-GB", {
        year: "numeric",
        month: "short",
        day: "2-digit"
    });
}

function formatDateTime(value) {
    if (!value) {
        return "—";
    }

    return new Date(value).toLocaleString("en-GB", {
        year: "numeric",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
    });
}

function toneClass(value) {
    const numeric = asNumber(value);
    if (numeric === null || numeric === 0) {
        return "neutral";
    }

    return numeric > 0 ? "positive" : "negative";
}

function liveStatusClass(run) {
    if (!run) {
        return "status-warning";
    }
    if (run.status === "failed") {
        return "status-error";
    }
    return run.summary?.market_health ? "status-ok" : "status-warning";
}

function formatModeLabel(mode, symbol) {
    if (mode === "treasury_bonds") {
        return symbol ? `Treasury (${symbol})` : "Treasury";
    }
    return "Cash";
}

function summariseActionCounts(summary) {
    if (!summary) {
        return "No actions";
    }

    const buys = (summary.opened_count ?? 0) + (summary.underrisked_count ?? 0) + (summary.defensive_buy_count ?? 0);
    const sells = (summary.closed_count ?? 0) + (summary.overrisked_count ?? 0) + (summary.capped_sells_count ?? 0);
    return `${buys} buys / ${sells} sells`;
}

function emptyStateHtml(message) {
    return `<div class="empty-state">${message}</div>`;
}

function getSelectedBacktestRun() {
    const runs = safeArray(state.backtest.history?.runs);
    if (runs.length === 0) {
        return null;
    }

    const selectedSummary = runs.find((run) => run.id === state.backtest.selectedRunId) ?? runs[0];
    return state.backtest.runDetails[selectedSummary.id] ?? selectedSummary;
}

function getSelectedLiveRun() {
    const runs = safeArray(state.live.history?.runs);
    if (runs.length === 0) {
        return null;
    }

    const selectedSummary = runs.find((run) => run.id === state.live.selectedRunId) ?? runs[0];
    return state.live.runDetails[selectedSummary.id] ?? selectedSummary;
}

function getBacktestSettings(run) {
    if (!run) {
        return {};
    }

    if (run.settings) {
        return run.settings;
    }

    const summary = run.summary ?? {};
    return {
        initial_cash: summary.initial_cash,
        benchmark_symbol: summary.benchmark_symbol,
        raw_rank_consideration_limit: summary.raw_rank_consideration_limit,
        max_position_fraction: summary.max_position_fraction,
        defensive_mode: summary.defensive_mode,
        defensive_symbol: summary.defensive_symbol,
        trade_fee_flat: summary.trade_fee_flat,
        trade_fee_rate: summary.trade_fee_rate
    };
}

function getLiveSettings(run) {
    if (!run) {
        return {};
    }

    if (run.settings) {
        return run.settings;
    }

    const summary = run.summary ?? {};
    return {
        defensive_mode: summary.defensive_mode,
        defensive_symbol: summary.defensive_symbol,
        raw_rank_consideration_limit: summary.raw_rank_consideration_limit,
        max_position_fraction: summary.max_position_fraction,
        is_risk_rebalance_day: false
    };
}

function getAvailableLiveTimeframes(run) {
    const portfolioHistory = run?.portfolio_history ?? {};
    const available = LIVE_TIMEFRAME_ORDER.filter((label) => portfolioHistory[label]);
    return available.length > 0 ? available : Object.keys(portfolioHistory);
}

function ensureSelectedLiveTimeframe(run) {
    const available = getAvailableLiveTimeframes(run);
    if (available.length === 0) {
        state.live.selectedTimeframe = "1M";
        return null;
    }

    if (!available.includes(state.live.selectedTimeframe)) {
        state.live.selectedTimeframe = available[0];
    }
    return state.live.selectedTimeframe;
}

function buildPolylinePoints(values, bounds, dimensions) {
    const usableWidth = dimensions.width - dimensions.left - dimensions.right;
    const usableHeight = dimensions.height - dimensions.top - dimensions.bottom;
    const denominator = Math.max(bounds.max - bounds.min, 1e-9);

    return values
        .map((value, index) => {
            const x = dimensions.left + (usableWidth * index) / Math.max(values.length - 1, 1);
            const y = dimensions.top + ((bounds.max - value) / denominator) * usableHeight;
            return `${x.toFixed(2)},${y.toFixed(2)}`;
        })
        .join(" ");
}

function setText(id, value) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = value;
    }
}

function renderMetricStack(containerId, rows, emptyMessage) {
    const container = document.getElementById(containerId);
    if (!container) {
        return;
    }

    if (!rows || rows.length === 0) {
        container.innerHTML = emptyStateHtml(emptyMessage);
        return;
    }

    container.innerHTML = rows
        .map(([label, value, tone, note]) => `
            <div class="metric-item">
                <div class="metric-row">
                    <span>${label}</span>
                    <strong class="${tone ?? ""}">${value}</strong>
                </div>
                ${note ? `<p>${note}</p>` : ""}
            </div>
        `)
        .join("");
}

function renderHero() {
    const liveLatest = safeArray(state.live.history?.runs)[0] ?? null;
    const backtestLatest = safeArray(state.backtest.history?.runs)[0] ?? null;

    setText("hero-active-view", state.activeTab === "live" ? "Live operations" : "Backtest research");
    setText(
        "hero-live-run",
        liveLatest ? `${formatDateTime(liveLatest.generated_at)} · ${liveLatest.status}` : (state.live.error ?? "Waiting for live data")
    );
    setText(
        "hero-backtest-run",
        backtestLatest ? `${formatShortDate(backtestLatest.generated_at)} · ${backtestLatest.period.label}` : (state.backtest.error ?? "Waiting for backtests")
    );
}

function getAlertEntries() {
    const publishedErrors = safeArray(state.errors.history?.errors).map((error) => ({
        id: error.id,
        title: error.title ?? "Published error",
        message: error.message ?? "No message provided.",
        generated_at: error.generated_at,
        severity: error.severity ?? "error",
        source: error.source ?? "publisher"
    }));

    const dashboardErrors = [
        state.live.error
            ? {
                id: "dashboard-live-error",
                title: "Live dashboard load issue",
                message: state.live.error,
                generated_at: null,
                severity: "warning",
                source: "dashboard"
            }
            : null,
        state.backtest.error
            ? {
                id: "dashboard-backtest-error",
                title: "Backtest dashboard load issue",
                message: state.backtest.error,
                generated_at: null,
                severity: "warning",
                source: "dashboard"
            }
            : null
    ].filter(Boolean);

    const combined = [...dashboardErrors, ...publishedErrors];
    const seen = new Set();
    return combined
        .filter((entry) => {
            if (seen.has(entry.id)) {
                return false;
            }
            seen.add(entry.id);
            return true;
        })
        .slice(0, 4);
}

function renderHeroAlerts() {
    const container = document.getElementById("hero-alert-list");
    const alerts = getAlertEntries();
    setText("hero-alert-count", alerts.length === 0 ? "0 active" : `${alerts.length} active`);

    if (alerts.length === 0) {
        container.innerHTML = emptyStateHtml("No published errors right now. When a live run or local backtest fails, the message will appear here.");
        return;
    }

    container.innerHTML = alerts
        .map((alert) => `
            <article class="hero-alert-item ${alert.severity === "warning" ? "is-warning" : "is-error"}">
                <h4>${alert.title}</h4>
                <p>${alert.message}</p>
                <div class="hero-alert-meta">
                    ${alert.generated_at ? formatDateTime(alert.generated_at) : "Current session"}
                    · ${alert.source}
                </div>
            </article>
        `)
        .join("");
}

function renderTabState() {
    ["live", "backtest"].forEach((tab) => {
        const button = document.getElementById(`tab-${tab}`);
        const view = document.getElementById(`${tab}-view`);
        const isActive = state.activeTab === tab;

        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-selected", String(isActive));
        view.classList.toggle("is-hidden", !isActive);
    });
}

function renderBacktestSummary(run) {
    if (!run) {
        setText("backtest-portfolio-value", "$0");
        setText("backtest-alpha-value", "—");
        setText("backtest-alpha-note", state.backtest.error ?? "Run a backtest to populate this view.");
        setText("backtest-drawdown-value", "—");
        setText("backtest-trades-value", "0");
        setText("backtest-fees-note", "No backtest data yet");
        return;
    }

    const summary = run.summary ?? {};
    setText("backtest-portfolio-value", formatCurrency(summary.final_portfolio_value));
    setText("backtest-alpha-value", formatPercentValue(summary.alpha_percent));
    setText("backtest-alpha-note", `${formatSignedCurrency(summary.alpha_dollars)} versus ${summary.benchmark_symbol ?? "benchmark"}`);
    setText("backtest-drawdown-value", formatPercentValue(summary.max_drawdown_percent, 1, false));
    setText("backtest-trades-value", formatCompactNumber(summary.trade_count));
    setText("backtest-fees-note", `${formatCurrency(summary.fees_paid_cumulative, 2)} in fees`);

    const alphaElement = document.getElementById("backtest-alpha-value");
    const drawdownElement = document.getElementById("backtest-drawdown-value");
    alphaElement.className = toneClass(summary.alpha_percent);
    drawdownElement.className = toneClass(summary.max_drawdown_percent);
}

function renderBacktestHistory() {
    const body = document.getElementById("backtest-history-body");
    const runs = safeArray(state.backtest.history?.runs);
    body.innerHTML = "";

    if (runs.length === 0) {
        setText("backtest-history-tag", "No runs yet");
        body.innerHTML = `<tr><td colspan="6">Run a backtest to start building the archive.</td></tr>`;
        return;
    }

    setText("backtest-history-tag", `Latest ${runs.length} runs`);

    runs.forEach((run) => {
        const summary = run.summary ?? {};
        const row = document.createElement("tr");
        row.dataset.runId = run.id;
        row.classList.toggle("is-selected", run.id === state.backtest.selectedRunId);
        row.innerHTML = `
            <td>${formatShortDate(run.generated_at)}</td>
            <td>${run.period?.label ?? "—"}</td>
            <td>${formatModeLabel(summary.defensive_mode, summary.defensive_symbol)}</td>
            <td class="${toneClass(summary.portfolio_return_percent)}">${formatPercentValue(summary.portfolio_return_percent)}</td>
            <td class="${toneClass(summary.alpha_percent)}">${formatPercentValue(summary.alpha_percent)}</td>
            <td>${formatCompactNumber(summary.trade_count)}</td>
        `;
        row.addEventListener("click", async () => {
            await selectBacktestRun(run.id);
        });
        body.appendChild(row);
    });
}

function renderBacktestResults(run) {
    if (!run) {
        renderMetricStack("backtest-results", [], "Selected backtest results will appear here.");
        return;
    }

    const summary = run.summary ?? {};
    renderMetricStack(
        "backtest-results",
        [
            ["Strategy Return", formatPercentValue(summary.portfolio_return_percent), toneClass(summary.portfolio_return_percent)],
            [`${summary.benchmark_symbol} Return`, formatPercentValue(summary.benchmark_return_percent), toneClass(summary.benchmark_return_percent)],
            ["Alpha", `${formatSignedCurrency(summary.alpha_dollars)} / ${formatPercentValue(summary.alpha_percent)}`, toneClass(summary.alpha_percent)],
            ["Max Drawdown", formatPercentValue(summary.max_drawdown_percent, 1, false), toneClass(summary.max_drawdown_percent)],
            ["Fees Paid", formatCurrency(summary.fees_paid_cumulative, 2)],
            ["Total Trades", formatCompactNumber(summary.trade_count)],
            ["Strategy Run Days", formatCompactNumber(summary.strategy_run_count)],
            ["Runtime", summary.elapsed_label ?? "—"]
        ],
        "Selected backtest results will appear here."
    );
}

function renderBacktestSettings(run) {
    if (!run) {
        renderMetricStack("backtest-settings", [], "Selected backtest settings will appear here.");
        return;
    }

    const settings = getBacktestSettings(run);
    renderMetricStack(
        "backtest-settings",
        [
            ["Initial Cash", formatCurrency(settings.initial_cash)],
            ["Benchmark", settings.benchmark_symbol ?? "—"],
            ["Defense Mode", formatModeLabel(settings.defensive_mode, settings.defensive_symbol)],
            ["Raw Rank Cutoff", settings.raw_rank_consideration_limit ? `Top ${settings.raw_rank_consideration_limit}` : "—"],
            ["Single-Stock Cap", formatRatioAsPercent(settings.max_position_fraction, 0)],
            ["Flat Fee", formatCurrency(settings.trade_fee_flat, 2)],
            ["Fee Rate", formatRatioAsPercent(settings.trade_fee_rate, 2)],
            ["Results CSV", run.artifacts?.results_path ? "Published" : "Not published"]
        ],
        "Selected backtest settings will appear here."
    );
}

function renderBacktestChart(run) {
    const svg = document.getElementById("backtest-chart");
    const empty = document.getElementById("backtest-chart-empty");
    const legend = document.getElementById("backtest-chart-legend");
    svg.innerHTML = "";

    if (!run || !run.series) {
        empty.classList.remove("is-hidden");
        legend.innerHTML = "";
        setText("selected-run-tag", "No run loaded");
        return;
    }

    empty.classList.add("is-hidden");
    setText("selected-run-tag", run.period?.label ?? "Selected run");

    const dimensions = { width: 900, height: 400, left: 62, right: 56, top: 22, bottom: 40 };
    const portfolioValues = safeArray(run.series.portfolio_value);
    const benchmarkValues = safeArray(run.series.benchmark_value);
    const averageValues = safeArray(run.series.benchmark_200dma_value);
    const reserveValues = safeArray(run.series.reserve_percentage);
    const dates = safeArray(run.series.dates);

    const leftValues = [...portfolioValues, ...benchmarkValues, ...averageValues].filter((value) => value !== null && value !== undefined);
    if (leftValues.length === 0) {
        empty.classList.remove("is-hidden");
        legend.innerHTML = "";
        return;
    }

    const leftMin = Math.min(...leftValues);
    const leftMax = Math.max(...leftValues);
    const padding = Math.max((leftMax - leftMin) * 0.08, 1);
    const leftBounds = { min: leftMin - padding, max: leftMax + padding };
    const reserveBounds = { min: 0, max: 100 };

    const gridLines = [0, 0.33, 0.66, 1]
        .map((fraction) => {
            const y = dimensions.top + fraction * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<line x1="${dimensions.left}" y1="${y}" x2="${dimensions.width - dimensions.right}" y2="${y}" stroke="rgba(27,26,23,0.08)" stroke-width="1" />`;
        })
        .join("");

    const leftTicks = [leftBounds.max, (leftBounds.max + leftBounds.min) / 2, leftBounds.min]
        .map((value, index) => {
            const y = dimensions.top + (index / 2) * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<text x="${dimensions.left - 10}" y="${y + 4}" text-anchor="end" fill="rgba(98,91,80,0.92)" font-size="11">${formatCurrency(value)}</text>`;
        })
        .join("");

    const rightTicks = [100, 50, 0]
        .map((value, index) => {
            const y = dimensions.top + (index / 2) * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<text x="${dimensions.width - dimensions.right + 10}" y="${y + 4}" text-anchor="start" fill="rgba(98,91,80,0.92)" font-size="11">${value}%</text>`;
        })
        .join("");

    svg.innerHTML = `
        <rect x="0" y="0" width="${dimensions.width}" height="${dimensions.height}" fill="transparent"></rect>
        ${gridLines}
        <line x1="${dimensions.left}" y1="${dimensions.top}" x2="${dimensions.left}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.16)" stroke-width="1" />
        <line x1="${dimensions.width - dimensions.right}" y1="${dimensions.top}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.16)" stroke-width="1" />
        <line x1="${dimensions.left}" y1="${dimensions.height - dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.16)" stroke-width="1" />
        ${leftTicks}
        ${rightTicks}
        <text x="${dimensions.left}" y="${dimensions.height - 10}" fill="rgba(98,91,80,0.92)" font-size="11">${dates[0] ?? ""}</text>
        <text x="${dimensions.width - dimensions.right}" y="${dimensions.height - 10}" text-anchor="end" fill="rgba(98,91,80,0.92)" font-size="11">${dates[dates.length - 1] ?? ""}</text>
        <polyline fill="none" stroke="#0f6d66" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round" points="${buildPolylinePoints(portfolioValues, leftBounds, dimensions)}"></polyline>
        <polyline fill="none" stroke="#b66a1d" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round" points="${buildPolylinePoints(benchmarkValues, leftBounds, dimensions)}"></polyline>
        <polyline fill="none" stroke="#c54747" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" points="${buildPolylinePoints(averageValues, leftBounds, dimensions)}"></polyline>
        <polyline fill="none" stroke="#1b1a17" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="8 7" points="${buildPolylinePoints(reserveValues, reserveBounds, dimensions)}"></polyline>
    `;

    legend.innerHTML = `
        <span class="legend-item"><span class="legend-swatch legend-portfolio"></span>Portfolio value</span>
        <span class="legend-item"><span class="legend-swatch legend-benchmark"></span>${run.summary?.benchmark_symbol ?? "Benchmark"} buy & hold</span>
        <span class="legend-item"><span class="legend-swatch legend-average"></span>${run.summary?.benchmark_symbol ?? "Benchmark"} 200DMA</span>
        <span class="legend-item"><span class="legend-swatch legend-reserve"></span>${run.summary?.reserve_label ?? "Reserve % of portfolio"}</span>
    `;
}

function renderLiveSummary(run) {
    if (!run) {
        setText("live-portfolio-value", "$0");
        setText("live-cash-reserve", "$0");
        setText("live-cash-reserve-note", "No live cash snapshot yet");
        setText("live-fees-paid", "—");
        setText("live-positions-held", "0");
        setText("live-last-run", state.live.error ?? "Run the live worker to populate this view.");
        setText("live-status-tag", "No live data");
        document.getElementById("live-status-tag").className = "tag";
        return;
    }

    const summary = run.summary ?? {};
    const finalAccount = run.final_account ?? {};
    setText("live-portfolio-value", formatCurrency(finalAccount.portfolio_value));
    setText("live-cash-reserve", formatCurrency(finalAccount.cash));
    setText("live-cash-reserve-note", `${formatPercentValue(summary.reserve_percentage, 1, false)} reserve · ${formatModeLabel(getLiveSettings(run).defensive_mode, getLiveSettings(run).defensive_symbol)}`);
    setText("live-fees-paid", formatCurrency(summary.total_fees_paid, 2));
    setText("live-positions-held", formatCompactNumber(summary.positions_final));
    setText("live-last-run", `Updated ${formatDateTime(run.generated_at)}`);

    const tag = document.getElementById("live-status-tag");
    tag.textContent = run.status === "failed"
        ? "Run failed"
        : (summary.market_health ? "Market healthy" : "Defensive posture");
    tag.className = `tag ${liveStatusClass(run)}`;
}

function renderLiveTimeframeButtons(run) {
    const container = document.getElementById("live-timeframe-switcher");
    const available = getAvailableLiveTimeframes(run);
    container.innerHTML = "";

    if (available.length === 0) {
        return;
    }

    available.forEach((timeframe) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "timeframe-button";
        button.textContent = timeframe;
        button.classList.toggle("is-active", timeframe === state.live.selectedTimeframe);
        button.addEventListener("click", () => {
            state.live.selectedTimeframe = timeframe;
            renderLiveChart(getSelectedLiveRun());
            renderLiveTimeframeButtons(getSelectedLiveRun());
        });
        container.appendChild(button);
    });
}

function renderLiveHistory() {
    const body = document.getElementById("live-history-body");
    const runs = safeArray(state.live.history?.runs);
    body.innerHTML = "";

    if (runs.length === 0) {
        setText("live-history-tag", "No runs yet");
        body.innerHTML = `<tr><td colspan="5">No live run history has been published yet.</td></tr>`;
        return;
    }

    setText("live-history-tag", `Latest ${runs.length} runs`);

    runs.forEach((run) => {
        const row = document.createElement("tr");
        row.classList.toggle("is-selected", run.id === state.live.selectedRunId);
        row.innerHTML = `
            <td>${formatDateTime(run.generated_at)}</td>
            <td>${run.status}</td>
            <td>${run.summary?.market_health ? "Healthy" : "Defensive"}</td>
            <td>${formatCurrency(run.final_account?.portfolio_value ?? run.summary?.final_portfolio_value)}</td>
            <td>${summariseActionCounts(run.summary)}</td>
        `;
        row.addEventListener("click", async () => {
            await selectLiveRun(run.id);
        });
        body.appendChild(row);
    });
}

function renderLivePositions(run) {
    const body = document.getElementById("live-positions-body");
    body.innerHTML = "";

    if (!run || safeArray(run.final_positions).length === 0) {
        setText("live-positions-tag", "Portfolio snapshot");
        body.innerHTML = `<tr><td colspan="4">No position snapshot is available for this run.</td></tr>`;
        return;
    }

    const positions = [...safeArray(run.final_positions)].sort((a, b) => (b.market_value ?? 0) - (a.market_value ?? 0));
    setText("live-positions-tag", `${positions.length} holdings`);

    positions.forEach((position) => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${position.symbol} · ${formatCompactNumber(position.qty, 2)} sh</td>
            <td>${formatPercentValue(position.weight_percent, 1, false)}</td>
            <td>${formatCurrency(position.market_value)}</td>
            <td class="${toneClass(position.unrealized_pl)}">${formatSignedCurrency(position.unrealized_pl, 2)}</td>
        `;
        body.appendChild(row);
    });
}

function renderLiveOrders(run) {
    const container = document.getElementById("live-orders-list");
    const orders = safeArray(run?.recent_orders);
    setText("live-orders-tag", orders.length > 0 ? `${orders.length} recent orders` : "No recent orders");

    if (!run) {
        container.innerHTML = emptyStateHtml("Select a live run to inspect the orders it submitted.");
        return;
    }

    if (orders.length === 0) {
        container.innerHTML = emptyStateHtml("This live run did not publish any recent order records.");
        return;
    }

    container.innerHTML = orders
        .map((order) => `
            <article class="feed-item">
                <div class="feed-row">
                    <h4>${order.symbol ?? "Unknown"} · ${(order.side ?? "order").toUpperCase()}</h4>
                    <span class="feed-badge">${order.status ?? "unknown"}</span>
                </div>
                <p>${order.reason ?? "No reason was attached to this order record yet."}</p>
                <div class="feed-meta">
                    Filled ${formatCompactNumber(order.filled_qty ?? order.qty, 2)} at ${formatCurrency(order.filled_avg_price, 2)}
                    · Notional ${formatCurrency(order.notional, 2)}
                    · ${formatDateTime(order.filled_at ?? order.submitted_at)}
                </div>
            </article>
        `)
        .join("");
}

function renderLiveDecisions(run) {
    const container = document.getElementById("live-decisions-list");
    const details = safeArray(run?.action_details);
    setText("live-decisions-tag", details.length > 0 ? `${details.length} decisions` : "No decisions");

    if (!run) {
        container.innerHTML = emptyStateHtml("Select a live run to inspect the reasoning behind the bot's actions.");
        return;
    }

    if (details.length === 0) {
        const message = run.error_detail
            ? `This run failed before a decision log could be published. Error: ${run.error_detail}`
            : "This live run did not publish any action-detail reasoning.";
        container.innerHTML = emptyStateHtml(message);
        return;
    }

    container.innerHTML = details
        .map((detail) => `
            <article class="feed-item">
                <div class="feed-row">
                    <h4>${detail.symbol} · ${detail.side?.toUpperCase() ?? "ACTION"}</h4>
                    <span class="feed-badge">${detail.category ?? "decision"}</span>
                </div>
                <p>${detail.reason ?? "No reason attached."}</p>
                <div class="feed-meta">
                    Rank ${detail.raw_rank ?? "—"}
                    · Momentum ${formatPercentValue(detail.momentum, 2, false)}
                    · Annualised ${formatPercentValue(detail.annualised_return, 2, false)}
                    · Target shares ${formatCompactNumber(detail.target_shares, 2)}
                </div>
            </article>
        `)
        .join("");
}

function renderLiveChart(run) {
    const svg = document.getElementById("live-chart");
    const empty = document.getElementById("live-chart-empty");
    const legend = document.getElementById("live-chart-legend");
    svg.innerHTML = "";

    if (!run) {
        empty.classList.remove("is-hidden");
        legend.innerHTML = "";
        return;
    }

    const selectedTimeframe = ensureSelectedLiveTimeframe(run);
    const series = selectedTimeframe ? run.portfolio_history?.[selectedTimeframe] : null;

    if (!series || safeArray(series.equity).length === 0) {
        empty.classList.remove("is-hidden");
        legend.innerHTML = "";
        return;
    }

    empty.classList.add("is-hidden");

    const dimensions = { width: 900, height: 380, left: 62, right: 48, top: 22, bottom: 40 };
    const equityValues = safeArray(series.equity);
    const timestamps = safeArray(series.timestamps);
    const baselineValue = asNumber(series.base_value) ?? equityValues[0];
    const baseline = new Array(equityValues.length).fill(baselineValue);

    const leftValues = [...equityValues, ...baseline];
    const leftMin = Math.min(...leftValues);
    const leftMax = Math.max(...leftValues);
    const padding = Math.max((leftMax - leftMin) * 0.08, 1);
    const bounds = { min: leftMin - padding, max: leftMax + padding };

    const gridLines = [0, 0.33, 0.66, 1]
        .map((fraction) => {
            const y = dimensions.top + fraction * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<line x1="${dimensions.left}" y1="${y}" x2="${dimensions.width - dimensions.right}" y2="${y}" stroke="rgba(27,26,23,0.08)" stroke-width="1" />`;
        })
        .join("");

    const leftTicks = [bounds.max, (bounds.max + bounds.min) / 2, bounds.min]
        .map((value, index) => {
            const y = dimensions.top + (index / 2) * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<text x="${dimensions.left - 10}" y="${y + 4}" text-anchor="end" fill="rgba(98,91,80,0.92)" font-size="11">${formatCurrency(value)}</text>`;
        })
        .join("");

    svg.innerHTML = `
        <rect x="0" y="0" width="${dimensions.width}" height="${dimensions.height}" fill="transparent"></rect>
        ${gridLines}
        <line x1="${dimensions.left}" y1="${dimensions.top}" x2="${dimensions.left}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.16)" stroke-width="1" />
        <line x1="${dimensions.left}" y1="${dimensions.height - dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.16)" stroke-width="1" />
        ${leftTicks}
        <text x="${dimensions.left}" y="${dimensions.height - 10}" fill="rgba(98,91,80,0.92)" font-size="11">${formatShortDate(timestamps[0])}</text>
        <text x="${dimensions.width - dimensions.right}" y="${dimensions.height - 10}" text-anchor="end" fill="rgba(98,91,80,0.92)" font-size="11">${formatShortDate(timestamps[timestamps.length - 1])}</text>
        <polyline fill="none" stroke="#0f6d66" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round" points="${buildPolylinePoints(equityValues, bounds, dimensions)}"></polyline>
        <polyline fill="none" stroke="#2f4f7f" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="8 7" points="${buildPolylinePoints(baseline, bounds, dimensions)}"></polyline>
    `;

    const pnlPct = safeArray(series.profit_loss_pct);
    const latestPnlPct = pnlPct.length > 0 ? pnlPct[pnlPct.length - 1] * 100 : null;
    legend.innerHTML = `
        <span class="legend-item"><span class="legend-swatch legend-portfolio"></span>Equity (${selectedTimeframe})</span>
        <span class="legend-item"><span class="legend-swatch legend-baseline"></span>Starting value</span>
        <span class="legend-item">${latestPnlPct === null ? "P/L unavailable" : `Period P/L ${formatPercentValue(latestPnlPct, 2)}`}</span>
    `;
}

function renderLiveView() {
    const run = getSelectedLiveRun();
    renderLiveSummary(run);
    renderLiveTimeframeButtons(run);
    renderLiveChart(run);
    renderLiveHistory();
    renderLivePositions(run);
    renderLiveOrders(run);
    renderLiveDecisions(run);
}

function renderBacktestView() {
    const run = getSelectedBacktestRun();
    renderBacktestSummary(run);
    renderBacktestHistory();
    renderBacktestChart(run);
    renderBacktestResults(run);
    renderBacktestSettings(run);
}

function renderAll() {
    renderHero();
    renderHeroAlerts();
    renderTabState();
    renderLiveView();
    renderBacktestView();
}

async function fetchJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
        throw new Error(`HTTP ${response.status} while loading ${url}`);
    }
    return response.json();
}

async function ensureBacktestDetail(runId) {
    if (!runId || state.backtest.runDetails[runId]) {
        return state.backtest.runDetails[runId] ?? null;
    }

    const summary = safeArray(state.backtest.history?.runs).find((run) => run.id === runId);
    if (!summary?.detail_path) {
        return summary ?? null;
    }

    const payload = normalizeArtifactPaths(await fetchJson(resolveDataUrl(summary.detail_path)));
    state.backtest.runDetails[runId] = payload;
    return payload;
}

async function ensureLiveDetail(runId) {
    if (!runId || state.live.runDetails[runId]) {
        return state.live.runDetails[runId] ?? null;
    }

    const summary = safeArray(state.live.history?.runs).find((run) => run.id === runId);
    if (!summary?.detail_path) {
        return summary ?? null;
    }

    const payload = normalizeArtifactPaths(await fetchJson(resolveDataUrl(summary.detail_path)));
    state.live.runDetails[runId] = payload;
    return payload;
}

async function selectBacktestRun(runId) {
    state.backtest.selectedRunId = runId;
    try {
        await ensureBacktestDetail(runId);
        state.backtest.error = null;
    } catch (error) {
        state.backtest.error = `Failed to load backtest run ${runId}: ${error.message}`;
    }
    renderAll();
}

async function selectLiveRun(runId) {
    state.live.selectedRunId = runId;
    try {
        await ensureLiveDetail(runId);
        state.live.error = null;
    } catch (error) {
        state.live.error = `Failed to load live run ${runId}: ${error.message}`;
    }
    ensureSelectedLiveTimeframe(getSelectedLiveRun());
    renderAll();
}

async function loadBacktestHistory() {
    try {
        const payload = normalizeArtifactPaths(await fetchJson(BACKTEST_HISTORY_URL));
        state.backtest.error = null;
        state.backtest.history = payload;
        state.backtest.selectedRunId = payload.runs?.[0]?.id ?? null;
        if (state.backtest.selectedRunId) {
            try {
                await ensureBacktestDetail(state.backtest.selectedRunId);
            } catch (error) {
                state.backtest.error = `Loaded backtest history but failed to load the latest run detail: ${error.message}`;
            }
        }
    } catch (error) {
        state.backtest.history = { updated_at: null, runs: [] };
        state.backtest.selectedRunId = null;
        state.backtest.runDetails = {};
        state.backtest.error = window.location.protocol === "file:"
            ? "This page is being opened directly from disk. Start a local server such as `python -m http.server 8000` and open `/frontend/`."
            : error.message;
    }
}

async function loadLiveHistory() {
    try {
        const payload = normalizeArtifactPaths(await fetchJson(LIVE_HISTORY_URL));
        state.live.error = null;
        state.live.history = payload;
        state.live.selectedRunId = payload.runs?.[0]?.id ?? null;
        if (state.live.selectedRunId) {
            try {
                await ensureLiveDetail(state.live.selectedRunId);
            } catch (error) {
                state.live.error = `Loaded live history but failed to load the latest run detail: ${error.message}`;
            }
        }
        ensureSelectedLiveTimeframe(getSelectedLiveRun());
    } catch (error) {
        state.live.history = { updated_at: null, runs: [] };
        state.live.selectedRunId = null;
        state.live.runDetails = {};
        state.live.error = window.location.protocol === "file:"
            ? "This page is being opened directly from disk. Start a local server such as `python -m http.server 8000` and open `/frontend/`."
            : error.message;
    }
}

async function loadErrorHistory() {
    try {
        const response = await fetch(ERROR_HISTORY_URL, { cache: "no-store" });
        if (!response.ok) {
            state.errors.history = { errors: [] };
            return;
        }

        state.errors.history = await response.json();
    } catch {
        state.errors.history = { errors: [] };
    }
}

function bindEvents() {
    document.querySelectorAll(".tab-button").forEach((button) => {
        button.addEventListener("click", () => {
            state.activeTab = button.dataset.tab;
            renderAll();
        });
    });
}

async function bootstrap() {
    bindEvents();
    await Promise.all([loadBacktestHistory(), loadLiveHistory(), loadErrorHistory()]);

    if (!safeArray(state.live.history?.runs).length && safeArray(state.backtest.history?.runs).length) {
        state.activeTab = "backtest";
    }

    renderAll();
}

bootstrap();
