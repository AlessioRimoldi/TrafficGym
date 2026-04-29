import type { Chart as ChartType, ChartDataset, Point, DatasetChartOptions} from 'chart.js'
import type {} from 'bootstrap'

declare const Chart: typeof ChartType;

const chartCanvas = document.getElementById("analyticsChart") as HTMLCanvasElement;
const chartPlaceholder = document.getElementById("chartPlaceholder") as HTMLDivElement;
const dataTable = document.getElementById("analyticsTable") as HTMLDivElement;
const tablePlaceholder = document.getElementById("tablePlaceholder") as HTMLDivElement;

interface DataPoint {
    x: number;
    y: number | string;
}

type ExtendedDataset = ChartDataset<"line", Point[]> & {
    fullLabel: string;
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
                runExecsToUse.forEach(runExec => {
                    (currentData.runs[rid] ||= []).push({
                        subscription: dataToLoad.subscription,
                        aggMode: dataToLoad.aggMode,
                        runExecutions: [runExec]
                    });
                });
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
                        chartPlaceholder.style.display = "block";
                        chartCanvas.style.display = "none";
                        return;
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
                            const color = palette[(datasets.length) % palette.length]
                            datasets.push({
                                label: isAggregated
                                    ? `${dataEntry.aggMode}:${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`
                                    : `${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`,
                                fullLabel: isAggregated
                                    ? `${dataEntry.aggMode}:${fp} (${run_execs})`
                                    : `${fp} (${run_execs})`,
                                data: points
                                        .filter(p => !isNaN(Number(p.y)))
                                        .map(p => ({x: p.x, y: Number(p.y)})),
                                runId: rid,
                                dataIndex: dataIndex,
                                rawData: points,
                                borderColor: color,
                                parsing: false
                            });
                        }
                    }
                }

                updateChart();
                updateDataTable();
                updateDatasetControls();
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

            document.getElementById("selectRunForm")?.addEventListener("submit", (e) => {
                e.preventDefault();
            
                const select = document.getElementById("runSelect") as HTMLSelectElement;
                if (!select) throw new Error("Could not find run selector");
                const selected = Array.from(select.selectedOptions).map(opt => opt.value);
            
                if (!selected.length) return;
            
                const url = new URL(window.location.href);
            
                selected.forEach(id => url.searchParams.append("run_request", id));

                window.location.href = url.toString()
            });

            const modal = new window.bootstrap.Modal(modalElem);
            modal.show()
        })
    });

    const urlParams = new URLSearchParams(window.location.search);

    function shortenFingerprint(fingerprint: string) {
        return fingerprint.split(".").map(subPart => subPart.slice(0, 4)).join(".")
    }

    function shortenUUIDs(uuid: string) {
        return uuid.split("+").map(subPart => subPart.slice(0, 4)).join("+")
    }

    function updateChart() {
        const chartData = datasets.filter(ds => ds.data.length > 0)
        if (chart) {
            chart.data.datasets = chartData
            chart.resize();
            chart.update();
        } else {
            chart = new Chart(chartCanvas, {
                type: "line",
                data: {
                    datasets: chartData
                },
                options: {
                    responsive: true,
                    parsing: false,
                    scales: {
                        x: {
                            type: "linear",
                            title: { display: true, text: "Simulation Time" }
                        },
                        y: {
                            title: { display: true, text: "Value" }
                        }
                    }
                }
            });
        }

        if (chart.data.datasets.length === 0) {
            chartPlaceholder.style.display = "block";
            chartCanvas.style.display = "none";
        } else {
            chartPlaceholder.style.display = "none";
            chartCanvas.style.display = "block";
        }
    }

    function updateDatasetControls() {
        if (!chart) throw Error("No chart loaded");
    
        const container = document.getElementById("dataset_controls") as HTMLDivElement;
        container.innerHTML = "";
    
        if (datasets.length === 0) {
            container.innerHTML = '<span class="text-muted">No chart data present</span>';
            return;
        }
    
        datasets.forEach((ds) => {
            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-outline-danger me-1 mb-1 p-0"; // small button, no padding
            btn.style.width = "20px";
            btn.style.height = "20px";
            btn.style.borderRadius = "50%"; // circular swatch
            btn.style.backgroundColor = ds.borderColor as string || "#000";
            btn.title = ds.label ?? "unknown";
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
    
            container.appendChild(btn);
        });

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

        const tbody = document.createElement("tbody");
        pageTimes.forEach(t => {
            const tr = document.createElement("tr");
            const tdTime = document.createElement("td");
            tdTime.textContent = t.toString();
            tr.appendChild(tdTime);

            datasets.forEach(ds => {
                const point = (ds.rawData as DataPoint[]).find(p => p.x === t);
                const td = document.createElement("td");
                td.textContent = point ? truncateLabel(point.y.toString()) : "-";
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

        if (datasets.length === 0) {
            tablePlaceholder.style.display = "block";
            dataTable.style.display = "none";
        } else {
            tablePlaceholder.style.display = "none";
            dataTable.style.display = "block";
        }
    }
});

