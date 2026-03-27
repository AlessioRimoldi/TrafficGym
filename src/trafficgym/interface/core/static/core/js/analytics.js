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
        addData(json.run_execution_data, json.aggregated_data);
    }));
    function addData(run_execution_data, aggregated_data) {
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
            if (!grouped[d.run_execution]) {
                grouped[d.run_execution] = {};
            }
            if (!grouped[d.run_execution][d.fingerprint]) {
                grouped[d.run_execution][d.fingerprint] = [];
            }
            grouped[d.run_execution][d.fingerprint].push({ x: Number(d.time), y: Number(d.value) });
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
        for (const [run_exec, fingerprints] of Object.entries(grouped)) {
            const color = palette[datasets.length % palette.length];
            for (const [fp, points] of Object.entries(fingerprints)) {
                datasets.push({
                    label: `${fp} (${run_exec})`,
                    data: points,
                    color: color,
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
    }
    const rowsPerPage = 10;
    let currentPage = 1;
    function truncateLabel(label, maxLength = 10) {
        return label.length > maxLength ? label.slice(0, maxLength) + "…" : label;
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
        const table = document.createElement("table");
        table.className = "table table-striped table-hover table-sm";
        const thead = document.createElement("thead");
        const colNames = chart.data.datasets.map(ds => { var _a, _b; return `<th title=${(_a = ds.label) !== null && _a !== void 0 ? _a : '?'}>${truncateLabel((_b = ds.label) !== null && _b !== void 0 ? _b : '?', 15)}</th>`; });
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
        dataTable.appendChild(table);
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
