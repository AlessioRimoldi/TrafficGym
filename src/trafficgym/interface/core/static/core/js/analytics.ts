import type { Chart as ChartType, ChartDataset, Point} from 'chart.js'

declare const Chart: typeof ChartType;

const form = document.getElementById("analytics_controls") as HTMLFormElement;
const chartCanvas = document.getElementById("analyticsChart") as HTMLCanvasElement;
const chartPlaceholder = document.getElementById("chartPlaceholder") as HTMLDivElement;
const dataTable = document.getElementById("analyticsTable") as HTMLDivElement;
const tablePlaceholder = document.getElementById("tablePlaceholder") as HTMLDivElement;

interface DataPoint {
    x: number;
    y: number | string;
}

interface ChartPoint {
    x: number;
    y: number;
}

type ExtendedDataset = ChartDataset<"line", Point[]> & {
    fullLabel: string;
    rawData: DataPoint[],
};

interface DataItem {
    run_execution: string[];
    fingerprint: string;
    time: number | string;
    value: number | string;

}
let chart: ChartType | null = null;
const datasets: ExtendedDataset[] = [];


document.addEventListener("DOMContentLoaded", () => {
    if (!form) throw Error("form not loaded");

    const forms = document.querySelectorAll('form[id^="analytics_controls"]');

    (forms as NodeListOf<HTMLFormElement>).forEach(form => {
        form.addEventListener("submit", async (e) => { e.preventDefault();

            const formData = new FormData(form)
            const params = new URLSearchParams(formData as any)

            const url = form.action + "?" + params.toString();

            const response = await fetch(url);
            const json = await response.json();

            addData(json.run_execution_data, json.aggregated_data, params.get("agg_mode"));
        });
    });

    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has("load")) {
        forms.forEach(form => {
            // trigger the submit programmatically via dispatchEvent
            const event = new Event("submit", { bubbles: true, cancelable: true });
            form.dispatchEvent(event);
        });
    }

    function shortenFingerprint(fingerprint: string) {
        return fingerprint.split(".").map(subPart => subPart.slice(0, 4)).join(".")
    }

    function shortenUUIDs(uuid: string) {
        return uuid.split("+").map(subPart => subPart.slice(0, 4)).join("+")
    }

    function addData(run_execution_data: DataItem[], aggregated_data: DataItem[], agg_mode: string | null) {
        let isAggregated = null;
        let data: DataItem[] | null = null

        if (run_execution_data && run_execution_data.length !== 0) {
            data = run_execution_data;
            isAggregated = false;
        } else if (aggregated_data && aggregated_data.length !== 0) {
            data = aggregated_data;
            isAggregated = true;
        } else {
            chartPlaceholder.style.display = "block";
            chartCanvas.style.display = "none";
            return;
        }

        chartPlaceholder.style.display = "none";
        tablePlaceholder.style.display = "none";
        chartCanvas.style.display = "block";
        dataTable.style.display = "block";

        const grouped: Record<string, Record<string, DataPoint[]>> = {};
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
            "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
        ];

        for (const [run_execs, fingerprints] of Object.entries(grouped)) {
            for (const [fp, points] of Object.entries(fingerprints)) {
                const color = palette[(datasets.length) % palette.length]
                datasets.push({
                    label: isAggregated
                        ? `${agg_mode}:${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`
                        : `${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`,
                    fullLabel: isAggregated
                        ? `${agg_mode}:${fp} (${run_execs})`
                        : `${fp} (${run_execs})`,
                    data: points
                            .filter(p => !isNaN(Number(p.y)))
                            .map(p => ({x: p.x, y: Number(p.y)})),
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
    
            btn.onclick = () => {
                if (!chart) return;
                const index = datasets.indexOf(ds);
                if (index !== -1) {
                    datasets.splice(index, 1);
    
                    updateChart();
                    updateDataTable();
                    updateDatasetControls();
                }
            };
    
            container.appendChild(btn);
        });

        const deleteAllDataButton = document.createElement("button");
        deleteAllDataButton.className = "btn btn-outline-danger w-100 mt-2";
        deleteAllDataButton.innerHTML = "Remove All Data";

        deleteAllDataButton.onclick = () => {
            datasets.length = 0;

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

