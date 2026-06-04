import type { Chart as ChartType, ChartDataset, Point } from 'chart.js'
import type {} from 'bootstrap'

declare const Chart: typeof ChartType;

const chartCanvas = document.getElementById("analyticsChart") as HTMLCanvasElement;
const chartPlaceholder = document.getElementById("chartPlaceholder") as HTMLDivElement;
const stateTimelinesEl = document.getElementById("stateTimelines") as HTMLDivElement;

const SUMO_COLORS: Record<string, string> = {
    "G": "#00b300",
    "g": "#007700",
    "r": "#cc0000",
    "y": "#ffcc00",
    "o": "#ff8800",
    "O": "#333333",
    "u": "#888888",
    "s": "#996699",
};
const dataTable = document.getElementById("analyticsTable") as HTMLDivElement;
const tablePlaceholder = document.getElementById("tablePlaceholder") as HTMLDivElement;

interface DataPoint {
    x: number;
    y: number | string;
}

type ExtendedDataset = ChartDataset<"line", Point[]> & {
    fullLabel: string;
    displayLabel: string;
    rawData: DataPoint[];
    runId: string;
    dataIndex: number;
};

interface DataItem {
    run_execution: string[];
    fingerprint: string;
    time: number | string;
    value: number | string;
}

interface URLGraphData {
    runs: { [runId: string]: URLDataEntry[] };
    // load?: boolean;
}

interface URLDataEntry {
    subscription: string;
    aggMode?: string;
    runExecutions: string[];
}

let chart: ChartType | null = null;
const datasets: ExtendedDataset[] = [];


document.addEventListener("DOMContentLoaded", () => {
    const currentURL = new URL(window.location.href)

    let currentData: URLGraphData
    try {
        currentData = JSON.parse(
            currentURL.searchParams.get("data") ?? '{ "runs": { } }'
        ) as URLGraphData;
    } catch {
        currentData = { runs: { } };
    }

    addData(currentData);

    for (const [rid, entries] of Object.entries(currentData.runs)) {
        if (!entries || entries.length === 0) continue;
        const lastEntry = entries[entries.length - 1];

        // Locate the form using the submit button's data-run-id
        const submitBtn = document.querySelector<HTMLButtonElement>(
            `button[data-run-id="${rid}"]`
        );
        if (!submitBtn) continue;

        const form = submitBtn.closest<HTMLFormElement>("form");
        if (!form) continue;

        const subscriptionInput = form.querySelector<HTMLSelectElement>(
            '[name="fingerprint"]'
        );
        if (subscriptionInput) {
            subscriptionInput.value = lastEntry.subscription;
        }

        const aggModeInput = form.querySelector<HTMLSelectElement>(
            '[name="agg_mode"]'
        );
        if (aggModeInput && lastEntry.aggMode) {
            aggModeInput.value = lastEntry.aggMode;
        }

        const runExecSelect = form.querySelector<HTMLSelectElement>(
            '[name="run_execution"]'
        );
        if (runExecSelect) {
            const selectedValues = lastEntry.runExecutions;
            Array.from(runExecSelect.options).forEach(opt => {
                // If the last entry was "all run executions", select the blank option
                if (selectedValues.length === 0) {
                    opt.selected = opt.value === "";
                } else {
                    opt.selected = selectedValues.includes(opt.value);
                }
            });
        }
    }

    const forms = document.querySelectorAll('form.analytics_controls');

    (forms as NodeListOf<HTMLFormElement>).forEach((form) => {
        form.addEventListener("submit", async (e) => { 
            e.preventDefault();
            const formData = new FormData(form)
            const submitBtn = 
                form.querySelector('button[type="submit"]') as HTMLButtonElement;

            const currentURL = new URL(window.location.href)

            try {
                currentData = JSON.parse(
                    currentURL.searchParams.get("data") ?? ""
                ) as URLGraphData;
            } catch {
                currentData = { runs: { } };
            }
 
            const rid = submitBtn.dataset.runId;
            if (!rid) throw new Error("Run ID missing on button");

            const selectedExecs = formData.getAll("run_execution")
                .map(re => re.toString())
            const allExecs: string[] = Array.from(
                form.querySelectorAll<HTMLSelectElement>(
                    'select[name="run_execution"] option'
                )
            ).map(opt => opt.value).filter(execId => execId !== "");
            const runExecsToUse = selectedExecs.findIndex(exec => exec === "") === -1
                ? selectedExecs
                : allExecs;

            const aggMode = formData.get("agg_mode")?.toString();

            const dataToLoad = { 
                subscription: formData.get("fingerprint")!.toString(),
                aggMode: aggMode,
                runExecutions: runExecsToUse,
            };

            if (!aggMode || aggMode === "") {
                if (runExecsToUse.length > 0) {
                    runExecsToUse.forEach(runExec => {
                        (currentData.runs[rid] ||= []).push({
                            subscription: dataToLoad.subscription,
                            aggMode: dataToLoad.aggMode,
                            runExecutions: [runExec]
                        });
                    });
                } else {
                    (currentData.runs[rid] ||= []).push({
                        subscription: dataToLoad.subscription,
                        aggMode: dataToLoad.aggMode,
                        runExecutions: [],
                    });
                }
            } else {
                (currentData.runs[rid] ||= []).push(dataToLoad);
            }

            currentURL.searchParams.set("data", JSON.stringify(currentData));
            window.history.pushState({}, "", currentURL)

            addData({ runs: { [rid]: [dataToLoad] } });
        });
    });

    async function addData(dataToLoad: URLGraphData) {
        document.body.classList.add("loading");
        const submitBtns = document
            .querySelectorAll('button[type="submit"]') as NodeListOf<HTMLButtonElement>;
        submitBtns.forEach(btn => btn.disabled = true);

        try {
            for (const [rid, dataEntries] of Object.entries(dataToLoad.runs)) {
                for (const [dataIndex, dataEntry] of dataEntries.entries()) {

                    const dataURL = `analytics/subscriptions_data/${rid}?fingerprint=${dataEntry.subscription}&agg_mode=${dataEntry.aggMode}&run_execution=${dataEntry.runExecutions.join("&run_execution=")}`
                    const response = await fetch(dataURL);
                    const json = await response.json();

                    if (json.warnings) {
                        const aggSkipped = (json.warnings as string[]).some((w: string) =>
                            w.includes("no numeric values found")
                        );
                        if (aggSkipped) {
                            // Patch URL so reload doesn't re-attempt numeric aggregation
                            const patchURL = new URL(window.location.href);
                            try {
                                const patchData = JSON.parse(patchURL.searchParams.get("data") ?? "{}") as URLGraphData;
                                if (patchData.runs[rid]?.[dataIndex]) {
                                    patchData.runs[rid][dataIndex].aggMode = "";
                                    patchURL.searchParams.set("data", JSON.stringify(patchData));
                                    window.history.replaceState({}, "", patchURL);
                                }
                            } catch { /* ignore */ }
                        }
                        const warningBox = document.getElementById("messageBox");
                        (json.warnings as string[]).forEach(w => {
                            const div = document.createElement("div") as HTMLDivElement;
                            div.classList = "alert alert-warning alert-dismissible fade show";
                            div.role = "alert";

                            const i = document.createElement("i") as HTMLImageElement;
                            i.classList = "bi bi-exclamation-triangle-fill me-2";

                            const button = document.createElement("button") as HTMLButtonElement;
                            button.classList = "btn-close";
                            button.type = "button";
                            button.setAttribute("data-bs-dismiss", "alert");
                            button.ariaLabel = "close";

                            const span = document.createElement("span") as HTMLSpanElement;
                            span.textContent = w;

                            div.appendChild(i);
                            div.appendChild(span);
                            div.appendChild(button);

                            warningBox?.appendChild(div);
                        })
                    }

                    // addData(json.run_execution_data, json.aggregated_data, rid, dataIndex, );
                    let isAggregated = null;
                    let data: DataItem[] | null = null

                    if (json.run_execution_data && json.run_execution_data.length !== 0) {
                        data = json.run_execution_data as DataItem[];
                        isAggregated = false;
                    } else if (json.aggregated_data && json.aggregated_data.length !== 0) {
                        data = json.aggregated_data as DataItem[];
                        isAggregated = true;
                    } else {
                        continue;
                    }

                    const grouped: Record<string, Record<string, DataPoint[]> > = {};
                    data.forEach(d => {
                        const combined_executions = d.run_execution.join("+")
                        if (!grouped[combined_executions]) {
                            grouped[combined_executions] = {};
                        }
                        if (!grouped[combined_executions][d.fingerprint]) {
                            grouped[combined_executions][d.fingerprint] = [];
                        }
                        grouped[combined_executions][d.fingerprint].push({x: Number(d.time), y: d.value});
                    });

                    const palette = [
                        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
                        "#17becf", "#bcbd22", "#aec7e8", "#ffbb78",
                        "#98df8a", "#ff9896", "#c5b0d5", "#c49c94",
                    ];

                    for (const [run_execs, fingerprints] of Object.entries(grouped)) {
                        for (const [fp, points] of Object.entries(fingerprints)) {
                            const usedColors = new Set(datasets.map(d => d.borderColor));
                            const color = palette.find(c => !usedColors.has(c)) ?? palette[datasets.length % palette.length];
                            datasets.push({
                                label: isAggregated
                                    ? `${dataEntry.aggMode}:${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`
                                    : `${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`,
                                fullLabel: isAggregated
                                    ? `${dataEntry.aggMode}:${fp} (${run_execs})`
                                    : `${fp} (${run_execs})`,
                                displayLabel: isAggregated
                                    ? `${dataEntry.aggMode}:${fp} (${shortenUUIDs(run_execs)})`
                                    : `${fp} (${shortenUUIDs(run_execs)})`,
                                data: points
                                        .filter(p => !isNaN(Number(p.y)))
                                        .map(p => ({x: p.x, y: Number(p.y)}))
                                        .sort((a, b) => a.x - b.x),
                                runId: rid,
                                dataIndex: dataIndex,
                                rawData: points,
                                borderColor: color,
                                parsing: false
                            });
                        }
                    }
                    updateChart();
                    updateDataTable();
                    updateDatasetControls();
                }
            }
        } finally {
            document.body.classList.remove("loading");
            submitBtns.forEach(btn => btn.disabled = false);
        }
    }


    document.getElementById("downloadButton")?.addEventListener("click", _ => {
        if (datasets.length === 0) return;

        // Collect all unique time points
        const timeSet = new Set<number>();
        datasets.forEach(ds =>
            (ds.rawData as DataPoint[]).forEach(p => timeSet.add(p.x))
        );
        const times = Array.from(timeSet).sort((a, b) => a - b);

        // Prepare CSV header: Time + one column per dataset
        const header = ["Time", ...datasets.map(ds => `"${ds.fullLabel}"`)];
        const rows = [header.join(",")];

        // Build rows for each time
        times.forEach(t => {
            const row = [t.toString()];
            datasets.forEach(ds => {
                const point = (ds.rawData as DataPoint[]).find(p => p.x === t);
                row.push(point ? point.y.toString() : "");
            });
            rows.push(row.join(","));
        });

        const csvContent = "data:text/csv;charset=utf-8," + rows.join("\n");
        const encodedUri = encodeURI(csvContent);

        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", "analytics_data.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    });
 
    const openAddCompareRunModalButton = document.getElementById("openAddCompareRunModal") as HTMLButtonElement
    openAddCompareRunModalButton?.addEventListener("click", () => {
        const url = openAddCompareRunModalButton.dataset.url;
        if (!url) throw new Error("Open Modal URL not found!")

        fetch(url)
        .then(response => response.text())
        .then(html => {
            const modalContent = document.getElementById("addCompareRunModalContent");
            const modalElem = document.getElementById("addCompareRunModal");
            if (!modalContent || !modalElem) throw new Error("Cannot find modal!");
            
            modalContent.innerHTML = html;

            // Independent filters for experiment and scenario
            function applyRunFilter() {
                const filters = Array.from(
                    document.querySelectorAll<HTMLInputElement>("[data-filter-key]:checked")
                );
                let visibleCount = 0;
                document.querySelectorAll<HTMLElement>(".run-option").forEach(el => {
                    const visible = filters.every(f =>
                        el.dataset[f.dataset.filterKey!] === f.dataset.filterValue
                    );
                    el.style.display = visible ? "" : "none";
                    if (visible) visibleCount++;
                });
                const emptyMsg = document.getElementById("runListEmpty");
                if (emptyMsg) emptyMsg.style.display = visibleCount === 0 ? "" : "none";
            }
            document.querySelectorAll<HTMLInputElement>("[data-filter-key]").forEach(cb =>
                cb.addEventListener("change", applyRunFilter)
            );
            applyRunFilter();

            document.getElementById("selectRunForm")?.addEventListener("submit", (e) => {
                e.preventDefault();
                const checked = Array.from(
                    document.querySelectorAll<HTMLInputElement>("#runList input[type=checkbox]:checked")
                ).map(cb => cb.value);
                if (!checked.length) return;
                const url = new URL(window.location.href);
                checked.forEach(id => url.searchParams.append("run_request", id));
                window.location.href = url.toString();
            });

            const modal = new window.bootstrap.Modal(modalElem);
            modal.show()
        })
    });

    function shortenFingerprint(fingerprint: string) {
        return fingerprint.split(".").map(subPart => subPart.slice(0, 4)).join(".")
    }

    function shortenUUIDs(uuid: string) {
        return uuid.split("+").map(subPart => subPart.slice(0, 4)).join("+")
    }

    function allDataTimeRange(): { min: number; max: number } | null {
        const times = datasets.flatMap(ds => (ds.rawData as DataPoint[]).map(p => p.x));
        if (!times.length) return null;
        return { min: Math.min(...times), max: Math.max(...times) + 1 };
    }

    function updateChart() {
        const chartData = datasets.filter(ds => ds.data.length > 0);
        const range = allDataTimeRange();

        if (chart) {
            chart.data.datasets = chartData;
            if (range) {
                (chart.options.scales!["x"] as any).suggestedMin = range.min;
                (chart.options.scales!["x"] as any).suggestedMax = range.max;
            }
            chart.resize();
            chart.update();
        } else {
            chart = new Chart(chartCanvas, {
                type: "line",
                data: {
                    datasets: chartData
                },
                options: {
                    transitions: {
                        zoom: {
                            animation: {
                            duration: 0
                            }
                        }
                    },
                    responsive: true,
                    parsing: false,
                    scales: {
                        x: {
                            type: "linear",
                            title: { display: true, text: "Simulation Time" },
                            suggestedMin: range?.min,
                            suggestedMax: range?.max,
                        },
                        y: {
                            title: { display: true, text: "Value" },
                            afterFit(scale: any) { scale.width = 80; }
                        }
                    },
                    plugins: {
                        tooltip: {
                            callbacks: {
                                label: (item: any) => {
                                    const full = (item.dataset as ExtendedDataset).displayLabel ?? item.dataset.label;
                                    return `${full}: ${item.formattedValue}`;
                                }
                            }
                        },
                        zoom: {
                            zoom: {
                                drag: {
                                    enabled: true,
                                    modifierKey: "alt",
                                    backgroundColor: "rgba(13, 110, 253, 0.1)",
                                    borderColor: "rgba(13, 110, 253, 0.5)",
                                    borderWidth: 1,
                                },
                                wheel: { enabled: true },
                                mode: "xy",
                                onZoom: () => renderStateTimelines(),
                            },
                            pan: {
                                enabled: true,
                                onPan: () => renderStateTimelines(),
                            },
                        }
                    }
                } as any
            });

            document.getElementById("resetZoomBtn")?.addEventListener("click", () => {
                tlView = null;
                (chart as any)?.resetZoom();
                renderStateTimelines();
            });

            new ResizeObserver(() => renderStateTimelines()).observe(chartCanvas);
        }

        const hasTls = datasets.some(ds => /^trafficlight\./.test(ds.fullLabel));
        if (chart.data.datasets.length === 0) {
            chartPlaceholder.style.display = hasTls ? "none" : "block";
            chartCanvas.style.display = "none";
        } else {
            chartPlaceholder.style.display = "none";
            chartCanvas.style.display = "block";
        }

        renderStateTimelines();
    }

    const TL_LABEL_W = 80;
    let tlView: { min: number; max: number } | null = null;

    function renderStateTimelines() {
        if (!chart) { stateTimelinesEl.style.display = "none"; return; }
        const re = /(.*)\..*\.(.*) /

        const tlsDatasets = datasets.filter(ds =>
            ds.fullLabel.match(re)?.[1] === "trafficlight"
        );

        if (tlsDatasets.length === 0) { stateTimelinesEl.style.display = "none"; return; }

        stateTimelinesEl.style.display = "block";
        stateTimelinesEl.innerHTML = "";

        const ROW_H = 24;
        const hasNumeric = chart.data.datasets.length > 0;
        let areaLeft: number, areaWidth: number, viewMin: number, viewMax: number;

        if (hasNumeric) {
            const area = chart.chartArea;
            areaLeft = area.left;
            areaWidth = area.right - area.left;
            viewMin = chart.scales["x"].min;
            viewMax = chart.scales["x"].max;
        } else {
            const allT = tlsDatasets.flatMap(ds => (ds.rawData as DataPoint[]).map(p => p.x));
            if (!tlView) tlView = { min: Math.min(...allT), max: Math.max(...allT) + 1 };
            areaLeft = TL_LABEL_W;
            areaWidth = Math.max(1, stateTimelinesEl.clientWidth - TL_LABEL_W);
            viewMin = tlView.min;
            viewMax = tlView.max;
        }

        const timeToPixel = (t: number) => ((t - viewMin) / (viewMax - viewMin)) * areaWidth;

        for (const ds of tlsDatasets) {
            const row = document.createElement("div");
            row.className = "d-flex align-items-center mb-1";

            const labelEl = document.createElement("div");
            labelEl.className = "small text-muted text-end pe-2 text-truncate";
            labelEl.style.cssText = `width:${areaLeft}px;min-width:${areaLeft}px;`;
            const tlName = document.createElement("span");
            tlName.textContent = ds.fullLabel.match(re)?.[2] ?? "";
            const uuidTag = document.createElement("span");
            uuidTag.textContent = ` ${ds.runId.slice(0, 4)}`;
            uuidTag.style.cssText = "font-size:0.75em;opacity:0.55;";
            labelEl.appendChild(tlName);
            labelEl.appendChild(uuidTag);
            labelEl.title = ds.fullLabel ?? "";

            const canvas = document.createElement("canvas");
            canvas.width = Math.round(areaWidth);
            canvas.height = ROW_H;
            canvas.style.cssText = `width:${areaWidth}px;height:${ROW_H}px;`;

            const ctx = canvas.getContext("2d")!;
            const raw = (ds.rawData as DataPoint[])
                .filter(p => typeof p.y === "string")
                .sort((a, b) => a.x - b.x);

            for (let i = 0; i < raw.length; i++) {
                const t0 = raw[i].x;
                const t1 = i + 1 < raw.length ? raw[i + 1].x : t0 + 1;
                const x0 = timeToPixel(t0);
                const x1 = timeToPixel(t1);
                if (x1 <= 0 || x0 >= areaWidth) continue;

                const state = String(raw[i].y);
                const chars = state.split("");
                const laneW = (Math.min(x1, areaWidth) - Math.max(x0, 0)) / chars.length;
                const drawX0 = Math.max(x0, 0);
                chars.forEach((ch, ci) => {
                    ctx.fillStyle = SUMO_COLORS[ch] ?? "#aaaaaa";
                    ctx.fillRect(drawX0 + ci * laneW, 0, laneW, ROW_H);
                });
            }

            row.append(labelEl, canvas);
            stateTimelinesEl.appendChild(row);
        }

        if (!hasNumeric) {
            const NICE = [1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 10800];
            const range = viewMax - viewMin;
            const targetTicks = 8;
            const rawStep = range / targetTicks;
            const step = NICE.find(n => n >= rawStep) ?? NICE[NICE.length - 1];
            const firstTick = Math.ceil(viewMin / step) * step;

            const axisCanvas = document.createElement("canvas");
            axisCanvas.width = Math.round(areaWidth);
            axisCanvas.height = 20;
            axisCanvas.style.cssText = `width:${areaWidth}px;height:20px;margin-left:${areaLeft}px;`;
            const ax = axisCanvas.getContext("2d")!;
            ax.font = "10px sans-serif";
            ax.fillStyle = "#6c757d";
            ax.textAlign = "center";

            for (let t = firstTick; t <= viewMax; t += step) {
                const x = ((t - viewMin) / range) * areaWidth;
                ax.fillRect(x, 0, 1, 4);
                // const label = step >= 3600 ? `${t / 3600}h`
                //     : step >= 60 ? `${Math.floor(t / 60)}:${String(t % 60).padStart(2, "0")}`
                //     : `${t}s`;
                const label = `${t}s`;
                ax.fillText(label, x, 14);
            }

            stateTimelinesEl.appendChild(axisCanvas);
        }
    }

    // ── Timeline pan / zoom interactions ──────────────────────────────────────

    stateTimelinesEl.style.cursor = "grab";

    function tlDoZoom(clientX: number, factor: number) {
        if (!chart) return;
        const rect = stateTimelinesEl.getBoundingClientRect();
        if (chart.data.datasets.length > 0) {
            const area = chart.chartArea;
            const focalX = area.left + Math.max(0, Math.min(1, (clientX - rect.left - area.left) / (area.right - area.left))) * (area.right - area.left);
            (chart as any).zoom({ x: factor, focalPoint: { x: focalX, y: (area.top + area.bottom) / 2 } });
        } else {
            if (!tlView) return;
            const aw = stateTimelinesEl.clientWidth - TL_LABEL_W;
            const frac = Math.max(0, Math.min(1, (clientX - rect.left - TL_LABEL_W) / aw));
            const t = tlView.min + frac * (tlView.max - tlView.min);
            const span = (tlView.max - tlView.min) / factor;
            tlView = { min: t - frac * span, max: t - frac * span + span };
        }
        renderStateTimelines();
    }

    function tlDoPan(deltaX: number) {
        if (!chart) return;
        if (chart.data.datasets.length > 0) {
            (chart as any).pan({ x: deltaX });
        } else {
            if (!tlView) return;
            const aw = stateTimelinesEl.clientWidth - TL_LABEL_W;
            const dt = -(deltaX / aw) * (tlView.max - tlView.min);
            tlView = { min: tlView.min + dt, max: tlView.max + dt };
        }
        renderStateTimelines();
    }

    stateTimelinesEl.addEventListener("wheel", e => {
        if (!chart) return;
        e.preventDefault();
        tlDoZoom(e.clientX, e.deltaY < 0 ? 1.1 : 0.9);
    }, { passive: false });

    let dragX: number | null = null;
    stateTimelinesEl.addEventListener("mousedown", e => {
        if (!chart) return;
        dragX = e.clientX;
        stateTimelinesEl.style.cursor = "grabbing";
    });
    window.addEventListener("mousemove", e => {
        if (dragX === null) return;
        tlDoPan(e.clientX - dragX);
        dragX = e.clientX;
    });
    window.addEventListener("mouseup", () => {
        dragX = null;
        stateTimelinesEl.style.cursor = "grab";
    });

    if (typeof (window as any).Hammer !== "undefined") {
        const hammer = new (window as any).Hammer(stateTimelinesEl);
        hammer.get("pinch").set({ enable: true });
        let lastScale = 1;
        hammer.on("pinchstart", () => { lastScale = 1; });
        hammer.on("pinch", (e: any) => {
            tlDoZoom(e.center.x, e.scale / lastScale);
            lastScale = e.scale;
        });
    }

    function updateDatasetControls() {
        if (!chart) throw Error("No chart loaded");

        // Build run request metadata from the submit buttons in the sidebar
        const runMeta = new Map<string, { scenario: string; experiment: string }>();
        document.querySelectorAll<HTMLButtonElement>("button[data-run-id][data-scenario]").forEach(btn => {
            runMeta.set(btn.dataset.runId!, {
                scenario: btn.dataset.scenario ?? "",
                experiment: btn.dataset.experiment ?? "",
            });
        });

        const container = document.getElementById("dataset_controls") as HTMLDivElement;
        container.innerHTML = "";

        if (datasets.length === 0) {
            container.innerHTML = '<span class="text-muted">No chart data present</span>';
            return;
        }

        // Group datasets by runId, preserving insertion order
        const groups = new Map<string, ExtendedDataset[]>();
        datasets.forEach(ds => {
            if (!groups.has(ds.runId)) groups.set(ds.runId, []);
            groups.get(ds.runId)!.push(ds);
        });

        groups.forEach((groupDs, runId) => {
            const meta = runMeta.get(runId);
            const header = document.createElement("div");
            header.className = "mb-1 mt-2";
            header.innerHTML = `
                <div class="small fw-semibold text-truncate" title="${meta?.experiment ?? runId}">
                    ${meta?.experiment ?? ""}
                </div>
                <span class="text-muted" style="font-size:0.6em;font-family:monospace;">${runId.slice(0, 8)}</span>`;
            container.appendChild(header);

            const swatchRow = document.createElement("div");
            swatchRow.className = "d-flex flex-wrap gap-1 mb-1";
            container.appendChild(swatchRow);

        groupDs.forEach((ds) => {
            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-outline-danger p-0";
            btn.style.width = "20px";
            btn.style.height = "20px";
            btn.style.borderRadius = "50%";
            btn.style.backgroundColor = ds.borderColor as string || "#000";
            btn.title = ds.displayLabel ?? ds.label ?? "unknown";
            btn.style.cursor = "pointer";
            btn.dataset.runId = ds.runId;
            btn.dataset.dataIndex = ds.dataIndex.toString();
    
            btn.onclick = () => {
                if (!chart) return;

                const runId = btn.dataset.runId!;
                const dataIndex = parseInt(btn.dataset.dataIndex!);
                const currentURL = new URL(window.location.href)

                let currentData: URLGraphData | null = null;
                try {
                    currentData = JSON.parse(
                        currentURL.searchParams.get("data") ?? ""
                    ) as URLGraphData;
                } catch {
                    currentData = { runs: { } };
                }

                const index = datasets.indexOf(ds);
                if (index !== -1) {
                    datasets.splice(index, 1);
                    
                    if (runId === "") throw new Error("Run ID not found during data add");

                    currentData.runs[runId].splice(dataIndex, 1);
                    if (currentData.runs[runId].length === 0) delete currentData.runs[runId];
                }
                if (Object.keys(currentData.runs).length === 0)
                    currentURL.searchParams.delete("data");
                else currentURL.searchParams.set("data", JSON.stringify(currentData));

                window.history.pushState({}, "", currentURL)

                updateChart();
                updateDataTable();
                btn.remove();
            };
    
            swatchRow.appendChild(btn);
        });
        }); // end groups.forEach

        const deleteAllDataButton = document.createElement("button");
        deleteAllDataButton.className = "btn btn-outline-danger w-100 mt-2";
        deleteAllDataButton.innerHTML = "Remove All Data";

        deleteAllDataButton.onclick = () => {
            datasets.length = 0;

            const currentURL = new URL(window.location.href)
            currentURL.searchParams.delete("data");
            window.history.pushState({}, "", currentURL)

            updateChart();
            updateDataTable();
            updateDatasetControls();
        };

        container.appendChild(deleteAllDataButton);

    }

    const rowsPerPage = 25;
    let currentPage = 1;

    function truncateLabel(label: string, maxLength = 30) {
        return label.length > maxLength ? label.slice(0, maxLength) + "…" : label;        // return label;
    }

    function updateDataTable() {
        dataTable.innerHTML = "";

        const timeSet = new Set<number>();
        datasets.forEach(ds => (ds.rawData as DataPoint[]).filter(p => p.x != null).forEach(p => timeSet.add(p.x ?? -1)));
        const times = Array.from(timeSet).sort((a, b) => a - b);

        const totalPages = Math.ceil(times.length / rowsPerPage);
        if (currentPage > totalPages) currentPage = totalPages || 1;

        const start = (currentPage - 1) * rowsPerPage;
        const end = start + rowsPerPage;
        const pageTimes = times.slice(start, end);

        const wrapper = document.createElement("div");
        wrapper.style.overflowX = "auto";
        wrapper.style.width = "100%";

        const table = document.createElement("table");
        table.className = "table table-striped table-hover table-sm";
        table.style.tableLayout = "fixed";
        table.style.width = "max-content";

        // const minPx = 120;
        // const maxPx = 500;

        // const style = document.createElement("style");
        // style.textContent = `
        // #analyticsTable th, #analyticsTable td {
        //     white-space: nowrap;
        //     overflow: hidden;
        //     text-overflow: ellipsis;
        //     min-width: ${minPx}px;
        //     max-width: ${maxPx}px;
        // }
        // `;
        // document.head.appendChild(style);

        const thead = document.createElement("thead");
        const colNames = datasets.map(ds => `<th title="${ds.fullLabel ?? '?'}">${truncateLabel(ds.label ?? '?', 15)}</th>`);

        thead.innerHTML = `<tr><th>Time</th>${colNames.join('')}</tr>`;
        table.appendChild(thead);

        // Seed lastSeen with the last known value per dataset before this page starts
        const lastSeen = new Map<number, string>();
        if (start > 0) {
            const cutoff = times[start - 1];
            datasets.forEach((ds, di) => {
                const allData = ds.rawData as DataPoint[];
                for (let i = allData.length - 1; i >= 0; i--) {
                    if (allData[i].x <= cutoff && allData[i].y != null) {
                        const yv = allData[i].y;
                        const yNum = Number(yv);
                        lastSeen.set(di, truncateLabel(isNaN(yNum) ? String(yv) : parseFloat(yNum.toFixed(6)).toString()));
                        break;
                    }
                }
            });
        }

        const tbody = document.createElement("tbody");
        pageTimes.forEach(t => {
            const tr = document.createElement("tr");
            const tdTime = document.createElement("td");
            tdTime.textContent = t.toString();
            tr.appendChild(tdTime);

            datasets.forEach((ds, di) => {
                const point = (ds.rawData as DataPoint[]).find(p => p.x === t);
                const td = document.createElement("td");
                if (point?.y != null) {
                    const yNum = Number(point.y);
                    const formatted = truncateLabel(isNaN(yNum) ? String(point.y) : parseFloat(yNum.toFixed(6)).toString());
                    lastSeen.set(di, formatted);
                    td.textContent = formatted;
                } else {
                    const carried = lastSeen.get(di);
                    if (carried != null) {
                        td.textContent = carried;
                        td.className = "text-muted fst-italic";
                        td.title = "No value recorded at this step — carried forward from last known value";
                    } else {
                        td.textContent = "-";
                    }
                }
                tr.appendChild(td);
            });

            tbody.appendChild(tr);
        });
        table.appendChild(tbody)
        wrapper.appendChild(table);
        dataTable.appendChild(wrapper);

        // Pagination controls
        const pagination = document.createElement("nav");
        pagination.className = "d-flex justify-content-between align-items-center mt-2";

        const prevBtn = document.createElement("button");
        prevBtn.className = "btn btn-sm btn-outline-primary";
        prevBtn.textContent = "Previous";
        prevBtn.disabled = currentPage === 1;
        prevBtn.onclick = () => { currentPage--; updateDataTable(); };

        const nextBtn = document.createElement("button");
        nextBtn.className = "btn btn-sm btn-outline-primary";
        nextBtn.textContent = "Next";
        nextBtn.disabled = currentPage === totalPages;
        nextBtn.onclick = () => { currentPage++; updateDataTable(); };

        const pageInfo = document.createElement("span");
        pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;

        pagination.appendChild(prevBtn);
        pagination.appendChild(pageInfo);
        pagination.appendChild(nextBtn);

        dataTable.appendChild(pagination);

        if (totalPages > 1) {
            const sliderRow = document.createElement("div");
            sliderRow.className = "d-flex align-items-center gap-2 mt-1";

            const minLabel = document.createElement("span");
            minLabel.className = "small text-muted text-nowrap";
            minLabel.textContent = "1";

            const slider = document.createElement("input");
            slider.type = "range";
            slider.className = "form-range";
            slider.min = "1";
            slider.max = String(totalPages);
            slider.value = String(currentPage);
            slider.addEventListener("input", () => {
                pageInfo.textContent = `Page ${slider.value} of ${totalPages}`;
            });
            slider.addEventListener("change", () => {
                currentPage = parseInt(slider.value, 10);
                updateDataTable();
            });

            const maxLabel = document.createElement("span");
            maxLabel.className = "small text-muted text-nowrap";
            maxLabel.textContent = String(totalPages);

            sliderRow.append(minLabel, slider, maxLabel);
            dataTable.appendChild(sliderRow);
        }

        if (datasets.length === 0) {
            tablePlaceholder.style.display = "block";
            dataTable.style.display = "none";
        } else {
            tablePlaceholder.style.display = "none";
            dataTable.style.display = "block";
        }
    }
});

