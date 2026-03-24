const HISTORY_URL = "./data/backtests/index.json";

const state = {
    history: null,
    selectedRunId: null,
    runDetails: {}
};

function formatCurrency(value) {
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 0
    }).format(value);
}

function formatPercent(value, digits = 1) {
    const prefix = value > 0 ? "+" : "";
    return `${prefix}${value.toFixed(digits)}%`;
}

function formatPercentPlain(value, digits = 1) {
    return `${value.toFixed(digits)}%`;
}

function formatShortDate(value) {
    return new Date(value).toLocaleDateString("en-GB", {
        year: "numeric",
        month: "short",
        day: "2-digit"
    });
}

function formatDateTime(value) {
    return new Date(value).toLocaleString("en-GB", {
        year: "numeric",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
    });
}

function toneClass(value) {
    if (value > 0) {
        return "positive";
    }
    if (value < 0) {
        return "negative";
    }
    return "neutral";
}

function getSelectedRun() {
    if (!state.history || !Array.isArray(state.history.runs) || state.history.runs.length === 0) {
        return null;
    }

    const selectedSummary = state.history.runs.find((run) => run.id === state.selectedRunId) ?? state.history.runs[0];
    return state.runDetails[selectedSummary.id] ?? selectedSummary;
}

function setHeroStatus(label, className) {
    const element = document.getElementById("hero-status");
    element.textContent = label;
    element.className = `status-pill ${className}`;
}

function renderSummary(run) {
    if (!run) {
        document.getElementById("hero-mode").textContent = "No backtests loaded";
        document.getElementById("hero-refresh").textContent = "Waiting for data";
        document.getElementById("portfolio-value").textContent = "$0";
        document.getElementById("reserve-value").textContent = "0%";
        document.getElementById("alpha-value").textContent = "0%";
        document.getElementById("next-run").textContent = "No runs yet";
        setHeroStatus("Idle", "status-pill status-idle");
        return;
    }

    const summary = run.summary;
    const reserveLabel = summary.defensive_mode === "treasury_bonds" ? "Paper / Treasury Defense" : "Paper / Cash Defense";
    const alphaClass = toneClass(summary.alpha_percent);

    document.getElementById("hero-mode").textContent = reserveLabel;
    document.getElementById("hero-refresh").textContent = formatDateTime(state.history.updated_at);
    document.getElementById("portfolio-value").textContent = formatCurrency(summary.final_portfolio_value);
    document.getElementById("reserve-value").textContent = formatPercentPlain(summary.final_reserve_percentage);
    document.getElementById("alpha-value").textContent = formatPercent(summary.alpha_percent);
    document.getElementById("alpha-value").className = alphaClass;
    document.getElementById("next-run").textContent = formatDateTime(run.generated_at);

    if (summary.alpha_percent >= 0) {
        setHeroStatus("Outperforming Benchmark", "status-pill status-live");
    } else {
        setHeroStatus("Lagging Benchmark", "status-pill status-watch");
    }
}

function renderMetrics(run) {
    const container = document.getElementById("run-metrics");
    container.innerHTML = "";

    if (!run) {
        return;
    }

    const summary = run.summary;
    const metricItems = [
        {
            label: "Strategy Return",
            value: formatPercent(summary.portfolio_return_percent),
            tone: toneClass(summary.portfolio_return_percent)
        },
        {
            label: `${summary.benchmark_symbol} Return`,
            value: formatPercent(summary.benchmark_return_percent),
            tone: toneClass(summary.benchmark_return_percent)
        },
        {
            label: "Max Drawdown",
            value: formatPercent(summary.max_drawdown_percent),
            tone: toneClass(summary.max_drawdown_percent)
        },
        {
            label: "Total Trades",
            value: `${summary.trade_count}`,
            tone: "neutral"
        },
        {
            label: "Fees Paid",
            value: formatCurrency(summary.fees_paid_cumulative),
            tone: "neutral"
        },
        {
            label: "Runtime",
            value: summary.elapsed_label || "n/a",
            tone: "neutral"
        }
    ];

    metricItems.forEach((metric) => {
        const item = document.createElement("div");
        item.className = "metric";
        item.innerHTML = `
            <span class="metric-label">${metric.label}</span>
            <strong class="metric-value ${metric.tone}">${metric.value}</strong>
        `;
        container.appendChild(item);
    });
}

function renderRunHistory(runs) {
    const body = document.getElementById("run-history-body");
    const historyTag = document.getElementById("history-tag");
    body.innerHTML = "";

    if (!runs || runs.length === 0) {
        historyTag.textContent = "No Backtests Yet";
        return;
    }

    historyTag.textContent = `Latest ${runs.length} Backtests`;

    runs.forEach((run) => {
        const row = document.createElement("tr");
        const summary = run.summary;
        row.dataset.runId = run.id;
        if (run.id === state.selectedRunId) {
            row.classList.add("is-selected");
        }
        row.innerHTML = `
            <td>${formatShortDate(run.generated_at)}</td>
            <td>${run.period.label}</td>
            <td>${summary.defensive_mode === "treasury_bonds" ? "Treasury" : "Cash"}</td>
            <td class="${toneClass(summary.portfolio_return_percent)}">${formatPercent(summary.portfolio_return_percent)}</td>
            <td class="${toneClass(summary.alpha_percent)}">${formatPercent(summary.alpha_percent)}</td>
            <td>${summary.trade_count}</td>
        `;
        row.addEventListener("click", async () => {
            await selectRun(run.id);
        });
        body.appendChild(row);
    });
}

function renderErrors(run, loadError = null) {
    const container = document.getElementById("error-list");
    container.innerHTML = "";

    const items = [];

    if (loadError) {
        items.push({
            severity: "critical",
            title: "Backtest history could not be loaded",
            message: loadError,
            timestamp: "Check the local server and JSON export path."
        });
    } else if (!run) {
        items.push({
            severity: "warning",
            title: "No backtest history found yet",
            message: "Run Backtesting.py once and it will populate frontend/data/backtests/index.json and the per-run detail files for this dashboard.",
            timestamp: "Waiting for the first exported run."
        });
    } else {
        items.push({
            severity: summarySeverity(run.summary.alpha_percent),
            title: "Selected run summary",
            message: `${run.period.label} finished ${formatPercent(run.summary.alpha_percent)} versus ${run.summary.benchmark_symbol}, with ${run.summary.trade_count} total trades and ${formatCurrency(run.summary.fees_paid_cumulative)} in fees.`,
            timestamp: `Created ${formatDateTime(run.generated_at)}`
        });
        items.push({
            severity: "warning",
            title: "Static frontend mode",
            message: "This page is currently reading published JSON files, not a live backend API. That keeps the site cheap and simple, but it only updates after each published run.",
            timestamp: "Good for early iteration."
        });
    }

    items.forEach((item) => {
        const element = document.createElement("article");
        element.className = `stack-item stack-item-${item.severity}`;
        element.innerHTML = `
            <h4>${item.title}</h4>
            <p>${item.message}</p>
            <div class="stack-meta">${item.timestamp}</div>
        `;
        container.appendChild(element);
    });
}

function summarySeverity(alphaPercent) {
    return alphaPercent >= 0 ? "warning" : "critical";
}

function renderChartLegend(run) {
    const container = document.getElementById("chart-legend");
    if (!run) {
        container.innerHTML = "";
        return;
    }

    const reserveLabel = run.summary.reserve_label || "Reserve % of Portfolio";
    container.innerHTML = `
        <span class="legend-item"><span class="legend-swatch legend-swatch-portfolio"></span>Portfolio Value</span>
        <span class="legend-item"><span class="legend-swatch legend-swatch-benchmark"></span>${run.summary.benchmark_symbol} Buy & Hold</span>
        <span class="legend-item"><span class="legend-swatch legend-swatch-average"></span>${run.summary.benchmark_symbol} 200DMA</span>
        <span class="legend-item"><span class="legend-swatch legend-swatch-reserve"></span>${reserveLabel}</span>
    `;
}

function renderBacktestMeta(run) {
    const container = document.getElementById("backtest-meta");
    const selectedTag = document.getElementById("selected-run-tag");
    container.innerHTML = "";

    if (!run) {
        selectedTag.textContent = "No Run Loaded";
        return;
    }

    const summary = run.summary;
    selectedTag.textContent = run.period.label;

    const rows = [
        ["Created", formatDateTime(run.generated_at)],
        ["Strategy Final Value", formatCurrency(summary.final_portfolio_value)],
        [`${summary.benchmark_symbol} Final Value`, formatCurrency(summary.final_benchmark_value)],
        ["Alpha", `${formatCurrency(summary.alpha_dollars)} / ${formatPercent(summary.alpha_percent)}`],
        [summary.reserve_label, formatPercentPlain(summary.final_reserve_percentage)],
        ["Strategy Run Days", `${summary.strategy_run_count}`],
        ["Rank Cutoff", `Top ${summary.raw_rank_consideration_limit}`],
        ["Single-Stock Cap", formatPercentPlain(summary.max_position_fraction * 100, 0)]
    ];

    container.innerHTML = rows
        .map(([label, value]) => {
            return `
                <div class="meta-row">
                    <span>${label}</span>
                    <strong>${value}</strong>
                </div>
            `;
        })
        .join("");
}

function pointsForSeries(values, bounds, dimensions) {
    const { min, max } = bounds;
    const usableWidth = dimensions.width - dimensions.left - dimensions.right;
    const usableHeight = dimensions.height - dimensions.top - dimensions.bottom;
    const denominator = Math.max(max - min, 1e-9);

    return values
        .map((value, index) => {
            const x = dimensions.left + (usableWidth * index) / Math.max(values.length - 1, 1);
            const y = dimensions.top + ((max - value) / denominator) * usableHeight;
            return `${x.toFixed(2)},${y.toFixed(2)}`;
        })
        .join(" ");
}

function renderChart(run) {
    const svg = document.getElementById("backtest-chart");
    const empty = document.getElementById("chart-empty");
    svg.innerHTML = "";

    if (!run || !run.series) {
        empty.classList.remove("is-hidden");
        return;
    }

    empty.classList.add("is-hidden");

    const dimensions = { width: 760, height: 320, left: 56, right: 48, top: 18, bottom: 34 };
    const portfolioValues = run.series.portfolio_value;
    const benchmarkValues = run.series.benchmark_value;
    const averageValues = run.series.benchmark_200dma_value;
    const reserveValues = run.series.reserve_percentage;

    const leftValues = [...portfolioValues, ...benchmarkValues, ...averageValues];
    const leftMin = Math.min(...leftValues);
    const leftMax = Math.max(...leftValues);
    const leftPadding = Math.max((leftMax - leftMin) * 0.08, 1);
    const leftBounds = { min: leftMin - leftPadding, max: leftMax + leftPadding };
    const reserveBounds = { min: 0, max: 100 };

    const gridLines = [0, 0.33, 0.66, 1].map((fraction) => {
        const y = dimensions.top + fraction * (dimensions.height - dimensions.top - dimensions.bottom);
        return `<line x1="${dimensions.left}" y1="${y}" x2="${dimensions.width - dimensions.right}" y2="${y}" stroke="rgba(27,26,23,0.08)" stroke-width="1" />`;
    }).join("");

    const leftTicks = [leftBounds.max, (leftBounds.max + leftBounds.min) / 2, leftBounds.min]
        .map((value, index) => {
            const y = dimensions.top + (index / 2) * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<text x="${dimensions.left - 8}" y="${y + 4}" text-anchor="end" fill="rgba(94,88,79,0.92)" font-size="11">${formatCurrency(value)}</text>`;
        })
        .join("");

    const rightTicks = [100, 50, 0]
        .map((value, index) => {
            const y = dimensions.top + (index / 2) * (dimensions.height - dimensions.top - dimensions.bottom);
            return `<text x="${dimensions.width - dimensions.right + 8}" y="${y + 4}" text-anchor="start" fill="rgba(94,88,79,0.92)" font-size="11">${value}%</text>`;
        })
        .join("");

    const portfolioPoints = pointsForSeries(portfolioValues, leftBounds, dimensions);
    const benchmarkPoints = pointsForSeries(benchmarkValues, leftBounds, dimensions);
    const averagePoints = pointsForSeries(averageValues, leftBounds, dimensions);
    const reservePoints = pointsForSeries(reserveValues, reserveBounds, dimensions);

    svg.innerHTML = `
        <rect x="0" y="0" width="${dimensions.width}" height="${dimensions.height}" fill="transparent"></rect>
        ${gridLines}
        <line x1="${dimensions.left}" y1="${dimensions.top}" x2="${dimensions.left}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.18)" stroke-width="1" />
        <line x1="${dimensions.width - dimensions.right}" y1="${dimensions.top}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.18)" stroke-width="1" />
        <line x1="${dimensions.left}" y1="${dimensions.height - dimensions.bottom}" x2="${dimensions.width - dimensions.right}" y2="${dimensions.height - dimensions.bottom}" stroke="rgba(27,26,23,0.18)" stroke-width="1" />
        ${leftTicks}
        ${rightTicks}
        <text x="${dimensions.left}" y="${dimensions.height - 8}" fill="rgba(94,88,79,0.92)" font-size="11">${run.series.dates[0]}</text>
        <text x="${dimensions.width - dimensions.right}" y="${dimensions.height - 8}" text-anchor="end" fill="rgba(94,88,79,0.92)" font-size="11">${run.series.dates[run.series.dates.length - 1]}</text>
        <polyline fill="none" stroke="#0f6d66" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round" points="${portfolioPoints}"></polyline>
        <polyline fill="none" stroke="#b5611d" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round" points="${benchmarkPoints}"></polyline>
        <polyline fill="none" stroke="#c54747" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" points="${averagePoints}"></polyline>
        <polyline fill="none" stroke="#1b1a17" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="7 7" points="${reservePoints}"></polyline>
    `;
}

function renderDashboard(loadError = null) {
    const runs = state.history?.runs ?? [];
    const run = getSelectedRun();

    renderSummary(run);
    renderMetrics(run);
    renderRunHistory(runs);
    renderErrors(run, loadError);
    renderChartLegend(run);
    renderBacktestMeta(run);
    renderChart(run);
}

async function ensureRunDetail(runId) {
    if (!runId || state.runDetails[runId]) {
        return state.runDetails[runId] ?? null;
    }

    const runSummary = state.history?.runs?.find((run) => run.id === runId);
    if (!runSummary || !runSummary.detail_path) {
        return runSummary ?? null;
    }

    const response = await fetch(runSummary.detail_path, { cache: "no-store" });
    if (!response.ok) {
        throw new Error(`HTTP ${response.status} while loading ${runSummary.detail_path}`);
    }

    const payload = await response.json();
    state.runDetails[runId] = payload;
    return payload;
}

async function selectRun(runId) {
    state.selectedRunId = runId;
    try {
        await ensureRunDetail(runId);
        renderDashboard();
    } catch (error) {
        renderDashboard(`Failed to load run details for ${runId}: ${error.message}`);
    }
}

async function loadBacktestHistory() {
    try {
        const response = await fetch(HISTORY_URL, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} while loading ${HISTORY_URL}`);
        }

        const payload = await response.json();
        state.history = payload;
        state.selectedRunId = payload.runs?.[0]?.id ?? null;
        if (state.selectedRunId) {
            await ensureRunDetail(state.selectedRunId);
        }
        renderDashboard();
    } catch (error) {
        state.history = { updated_at: null, runs: [] };
        state.selectedRunId = null;
        state.runDetails = {};

        let message = error.message;
        if (window.location.protocol === "file:") {
            message = "This page is being opened directly from the filesystem. Start a small local server such as `python -m http.server 8000` and open `/frontend/` so the browser can fetch the published JSON files.";
        }

        renderDashboard(message);
    }
}

loadBacktestHistory();
