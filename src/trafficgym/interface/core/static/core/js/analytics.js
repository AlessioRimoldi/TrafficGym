var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const form = document.getElementById("analytics_controls");
const chartCanvas = document.getElementById("analyticsChart");
const chartPlaceholder = document.getElementById("chartPlaceholder");
const dataTable = document.getElementById("analyticsTable");
const tablePlaceholder = document.getElementById("tablePlaceholder");
let chart = null;
document.addEventListener("DOMContentLoaded", () => {
    if (!form)
        throw Error("form not loaded");
    form.addEventListener("submit", (e) => __awaiter(void 0, void 0, void 0, function* () {
        e.preventDefault();
        const formData = new FormData(form);
        const params = new URLSearchParams(formData);
        const url = form.action + "?" + params.toString();
        const response = yield fetch(url);
        const json = yield response.json();
        addData(json.run_execution_data, json.aggregated_data, params.get("agg_mode"));
    }));
    function shortenFingerprint(fingerprint) {
        const subParts = fingerprint.split(".").map(subPart => subPart.slice(0, 4));
        return subParts.join(".");
    }
    function shortenUUIDs(uuid) {
        const subParts = uuid.split("+").map(subPart => subPart.slice(0, 4));
        return subParts.join("+");
    }
    function addData(run_execution_data, aggregated_data, agg_mode) {
        var _a;
        let isAggregated = null;
        let data = null;
        if (run_execution_data && run_execution_data.length !== 0) {
            data = run_execution_data;
            isAggregated = false;
        }
        else if (aggregated_data && aggregated_data.length !== 0) {
            data = aggregated_data;
            isAggregated = true;
        }
        else {
            chartPlaceholder.style.display = "block";
            chartCanvas.style.display = "none";
            return;
        }
        chartPlaceholder.style.display = "none";
        tablePlaceholder.style.display = "none";
        chartCanvas.style.display = "block";
        dataTable.style.display = "block";
        const grouped = {};
        data.forEach(d => {
            const combined_executions = d.run_execution.join("+");
            if (!grouped[combined_executions]) {
                grouped[combined_executions] = {};
            }
            if (!grouped[combined_executions][d.fingerprint]) {
                grouped[combined_executions][d.fingerprint] = [];
            }
            grouped[combined_executions][d.fingerprint].push({ x: Number(d.time), y: Number(d.value) });
        });
        const datasets = [];
        const palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
            "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
        ];
        // const execIds = Object.keys(grouped);
        // const colorMap = Object.fromEntries(
        //     execIds.map((id, i) => [id, palette[i % palette.length]])
        // );
        const startIndex = (_a = chart === null || chart === void 0 ? void 0 : chart.data.datasets.length) !== null && _a !== void 0 ? _a : 0;
        for (const [run_execs, fingerprints] of Object.entries(grouped)) {
            for (const [fp, points] of Object.entries(fingerprints)) {
                const color = palette[(startIndex + datasets.length) % palette.length];
                datasets.push({
                    label: isAggregated
                        ? `${agg_mode}:${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`
                        : `${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`,
                    fullLabel: isAggregated
                        ? `${agg_mode}:${fp} (${run_execs})`
                        : `${fp} (${run_execs})`,
                    data: points,
                    borderColor: color,
                    parsing: false
                });
            }
        }
        if (chart) {
            datasets.forEach(ds => chart === null || chart === void 0 ? void 0 : chart.data.datasets.push(ds));
            chart.resize();
            chart.update();
        }
        else {
            chart = new Chart(chartCanvas, {
                type: "line",
                data: {
                    datasets: datasets
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
        updateDatasetControls();
        updateDataTable();
    }
    function updateDatasetControls() {
        if (!chart)
            throw Error("No chart loaded");
        const container = document.getElementById("dataset_controls");
        container.innerHTML = "";
        if (chart.data.datasets.length === 0) {
            container.innerHTML = '<span class="text-muted">No chart data present</span>';
            return;
        }
        chart.data.datasets.forEach((ds) => {
            var _a;
            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-outline-danger me-1 mb-1 p-0"; // small button, no padding
            btn.style.width = "20px";
            btn.style.height = "20px";
            btn.style.borderRadius = "50%"; // circular swatch
            btn.style.backgroundColor = ds.borderColor || "#000";
            btn.title = (_a = ds.label) !== null && _a !== void 0 ? _a : "unknown";
            btn.style.cursor = "pointer";
            btn.onclick = () => {
                if (!chart)
                    return;
                const index = chart.data.datasets.indexOf(ds);
                if (index !== -1) {
                    chart.data.datasets.splice(index, 1);
                    chart.update();
                    if (chart.data.datasets.length === 0) {
                        chartPlaceholder.style.display = "block";
                        tablePlaceholder.style.display = "block";
                        chartCanvas.style.display = "none";
                        dataTable.style.display = "none";
                    }
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
            if (!chart)
                return;
            chartPlaceholder.style.display = "block";
            tablePlaceholder.style.display = "block";
            chartCanvas.style.display = "none";
            dataTable.style.display = "none";
            chart.data.datasets = [];
            chart.update();
            updateDataTable();
            updateDatasetControls();
        };
        container.appendChild(deleteAllDataButton);
    }
    const rowsPerPage = 25;
    let currentPage = 1;
    function truncateLabel(label, maxLength = 30) {
        return label.length > maxLength ? label.slice(0, maxLength) + "…" : label; // return label;
    }
    function updateDataTable() {
        if (!chart)
            return;
        dataTable.innerHTML = "";
        const timeSet = new Set();
        chart.data.datasets.forEach(ds => ds.data.filter(p => p.x != null).forEach(p => { var _a; return timeSet.add((_a = p.x) !== null && _a !== void 0 ? _a : -1); }));
        const times = Array.from(timeSet).sort((a, b) => a - b);
        const totalPages = Math.ceil(times.length / rowsPerPage);
        if (currentPage > totalPages)
            currentPage = totalPages || 1;
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
        const colNames = chart.data.datasets.map(ds => { var _a, _b; return `<th title="${(_a = ds.fullLabel) !== null && _a !== void 0 ? _a : '?'}">${truncateLabel((_b = ds.label) !== null && _b !== void 0 ? _b : '?', 15)}</th>`; });
        thead.innerHTML = `<tr><th>Time</th>${colNames.join('')}</tr>`;
        table.appendChild(thead);
        const tbody = document.createElement("tbody");
        pageTimes.forEach(t => {
            const tr = document.createElement("tr");
            const tdTime = document.createElement("td");
            tdTime.textContent = t.toString();
            tr.appendChild(tdTime);
            chart.data.datasets.forEach(ds => {
                const point = ds.data.find(p => p.x === t);
                const td = document.createElement("td");
                td.textContent = point ? truncateLabel(point.y.toString()) : "-";
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
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
    }
});
export {};
