var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const chartCanvas = document.getElementById("analyticsChart");
const chartPlaceholder = document.getElementById("chartPlaceholder");
const dataTable = document.getElementById("analyticsTable");
const tablePlaceholder = document.getElementById("tablePlaceholder");
let chart = null;
const datasets = [];
document.addEventListener("DOMContentLoaded", () => {
    var _a, _b;
    const currentURL = new URL(window.location.href);
    let currentData;
    try {
        currentData = JSON.parse((_a = currentURL.searchParams.get("data")) !== null && _a !== void 0 ? _a : '{ "runs": { } }');
    }
    catch (_c) {
        currentData = { runs: {} };
    }
    addData(currentData);
    for (const [rid, entries] of Object.entries(currentData.runs)) {
        if (!entries || entries.length === 0)
            continue;
        const lastEntry = entries[entries.length - 1];
        // Locate the form using the submit button's data-run-id
        const submitBtn = document.querySelector(`button[data-run-id="${rid}"]`);
        if (!submitBtn)
            continue;
        const form = submitBtn.closest("form");
        if (!form)
            continue;
        const subscriptionInput = form.querySelector('[name="fingerprint"]');
        if (subscriptionInput) {
            subscriptionInput.value = lastEntry.subscription;
        }
        const aggModeInput = form.querySelector('[name="agg_mode"]');
        if (aggModeInput && lastEntry.aggMode) {
            aggModeInput.value = lastEntry.aggMode;
        }
        const runExecSelect = form.querySelector('[name="run_execution"]');
        if (runExecSelect) {
            const selectedValues = lastEntry.runExecutions;
            Array.from(runExecSelect.options).forEach(opt => {
                // If the last entry was "all run executions", select the blank option
                if (selectedValues.length === 0) {
                    opt.selected = opt.value === "";
                }
                else {
                    opt.selected = selectedValues.includes(opt.value);
                }
            });
        }
    }
    const forms = document.querySelectorAll('form.analytics_controls');
    forms.forEach((form) => {
        form.addEventListener("submit", (e) => __awaiter(void 0, void 0, void 0, function* () {
            var _a, _b;
            var _c;
            e.preventDefault();
            const formData = new FormData(form);
            const submitBtn = form.querySelector('button[type="submit"]');
            const currentURL = new URL(window.location.href);
            try {
                currentData = JSON.parse((_a = currentURL.searchParams.get("data")) !== null && _a !== void 0 ? _a : "");
            }
            catch (_d) {
                currentData = { runs: {} };
            }
            const rid = submitBtn.dataset.runId;
            if (!rid)
                throw new Error("Run ID missing on button");
            const selectedExecs = formData.getAll("run_execution")
                .map(re => re.toString());
            const allExecs = Array.from(form.querySelectorAll('select[name="run_execution"] option')).map(opt => opt.value).filter(execId => execId !== "");
            const runExecsToUse = selectedExecs.findIndex(exec => exec === "") === -1
                ? selectedExecs
                : allExecs;
            const aggMode = (_b = formData.get("agg_mode")) === null || _b === void 0 ? void 0 : _b.toString();
            const dataToLoad = {
                subscription: formData.get("fingerprint").toString(),
                aggMode: aggMode,
                runExecutions: runExecsToUse,
            };
            if (!aggMode || aggMode === "") {
                runExecsToUse.forEach(runExec => {
                    var _a;
                    ((_a = currentData.runs)[rid] || (_a[rid] = [])).push({
                        subscription: dataToLoad.subscription,
                        aggMode: dataToLoad.aggMode,
                        runExecutions: [runExec]
                    });
                });
            }
            else {
                ((_c = currentData.runs)[rid] || (_c[rid] = [])).push(dataToLoad);
            }
            currentURL.searchParams.set("data", JSON.stringify(currentData));
            window.history.pushState({}, "", currentURL);
            addData({ runs: { [rid]: [dataToLoad] } });
        }));
    });
    function addData(dataToLoad) {
        return __awaiter(this, void 0, void 0, function* () {
            document.body.classList.add("loading");
            const submitBtns = document
                .querySelectorAll('button[type="submit"]');
            submitBtns.forEach(btn => btn.disabled = true);
            try {
                for (const [rid, dataEntries] of Object.entries(dataToLoad.runs)) {
                    for (const [dataIndex, dataEntry] of dataEntries.entries()) {
                        const dataURL = `analytics/subscriptions_data/${rid}?fingerprint=${dataEntry.subscription}&agg_mode=${dataEntry.aggMode}&run_execution=${dataEntry.runExecutions.join("&run_execution=")}`;
                        const response = yield fetch(dataURL);
                        const json = yield response.json();
                        if (json.warnings) {
                            const warningBox = document.getElementById("messageBox");
                            json.warnings.forEach(w => {
                                const div = document.createElement("div");
                                div.classList = "alert alert-warning alert-dismissible fade show";
                                div.role = "alert";
                                const i = document.createElement("i");
                                i.classList = "bi bi-exclamation-triangle-fill me-2";
                                const button = document.createElement("button");
                                button.classList = "btn-close";
                                button.type = "button";
                                button.setAttribute("data-bs-dismiss", "alert");
                                button.ariaLabel = "close";
                                const span = document.createElement("span");
                                span.textContent = w;
                                div.appendChild(i);
                                div.appendChild(span);
                                div.appendChild(button);
                                warningBox === null || warningBox === void 0 ? void 0 : warningBox.appendChild(div);
                            });
                        }
                        // addData(json.run_execution_data, json.aggregated_data, rid, dataIndex, );
                        let isAggregated = null;
                        let data = null;
                        if (json.run_execution_data && json.run_execution_data.length !== 0) {
                            data = json.run_execution_data;
                            isAggregated = false;
                        }
                        else if (json.aggregated_data && json.aggregated_data.length !== 0) {
                            data = json.aggregated_data;
                            isAggregated = true;
                        }
                        else {
                            chartPlaceholder.style.display = "block";
                            chartCanvas.style.display = "none";
                            return;
                        }
                        const grouped = {};
                        data.forEach(d => {
                            const combined_executions = d.run_execution.join("+");
                            if (!grouped[combined_executions]) {
                                grouped[combined_executions] = {};
                            }
                            if (!grouped[combined_executions][d.fingerprint]) {
                                grouped[combined_executions][d.fingerprint] = [];
                            }
                            grouped[combined_executions][d.fingerprint].push({ x: Number(d.time), y: d.value });
                        });
                        const palette = [
                            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                            "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
                        ];
                        for (const [run_execs, fingerprints] of Object.entries(grouped)) {
                            for (const [fp, points] of Object.entries(fingerprints)) {
                                const color = palette[(datasets.length) % palette.length];
                                datasets.push({
                                    label: isAggregated
                                        ? `${dataEntry.aggMode}:${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`
                                        : `${shortenFingerprint(fp)} (${shortenUUIDs(run_execs)})`,
                                    fullLabel: isAggregated
                                        ? `${dataEntry.aggMode}:${fp} (${run_execs})`
                                        : `${fp} (${run_execs})`,
                                    data: points
                                        .filter(p => !isNaN(Number(p.y)))
                                        .map(p => ({ x: p.x, y: Number(p.y) })),
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
            }
            finally {
                document.body.classList.remove("loading");
                submitBtns.forEach(btn => btn.disabled = false);
            }
        });
    }
    (_b = document.getElementById("downloadButton")) === null || _b === void 0 ? void 0 : _b.addEventListener("click", _ => {
        if (datasets.length === 0)
            return;
        // Collect all unique time points
        const timeSet = new Set();
        datasets.forEach(ds => ds.rawData.forEach(p => timeSet.add(p.x)));
        const times = Array.from(timeSet).sort((a, b) => a - b);
        // Prepare CSV header: Time + one column per dataset
        const header = ["Time", ...datasets.map(ds => `"${ds.fullLabel}"`)];
        const rows = [header.join(",")];
        // Build rows for each time
        times.forEach(t => {
            const row = [t.toString()];
            datasets.forEach(ds => {
                const point = ds.rawData.find(p => p.x === t);
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
    const openAddCompareRunModalButton = document.getElementById("openAddCompareRunModal");
    openAddCompareRunModalButton === null || openAddCompareRunModalButton === void 0 ? void 0 : openAddCompareRunModalButton.addEventListener("click", () => {
        const url = openAddCompareRunModalButton.dataset.url;
        if (!url)
            throw new Error("Open Modal URL not found!");
        fetch(url)
            .then(response => response.text())
            .then(html => {
            var _a;
            const modalContent = document.getElementById("addCompareRunModalContent");
            const modalElem = document.getElementById("addCompareRunModal");
            if (!modalContent || !modalElem)
                throw new Error("Cannot find modal!");
            modalContent.innerHTML = html;
            (_a = document.getElementById("selectRunForm")) === null || _a === void 0 ? void 0 : _a.addEventListener("submit", (e) => {
                e.preventDefault();
                const select = document.getElementById("runSelect");
                if (!select)
                    throw new Error("Could not find run selector");
                const selected = Array.from(select.selectedOptions).map(opt => opt.value);
                if (!selected.length)
                    return;
                const url = new URL(window.location.href);
                selected.forEach(id => url.searchParams.append("run_request", id));
                url.searchParams.set("load", "");
                // // update the URL in the browser without refreshing
                // window.history.replaceState(null, "", url);
                // loadDataFromUrl();
                window.location.href = url.toString();
            });
            const modal = new window.bootstrap.Modal(modalElem);
            modal.show();
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
    function shortenFingerprint(fingerprint) {
        return fingerprint.split(".").map(subPart => subPart.slice(0, 4)).join(".");
    }
    function shortenUUIDs(uuid) {
        return uuid.split("+").map(subPart => subPart.slice(0, 4)).join("+");
    }
    function updateChart() {
        const chartData = datasets.filter(ds => ds.data.length > 0);
        if (chart) {
            chart.data.datasets = chartData;
            chart.resize();
            chart.update();
        }
        else {
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
        }
        else {
            chartPlaceholder.style.display = "none";
            chartCanvas.style.display = "block";
        }
    }
    function updateDatasetControls() {
        if (!chart)
            throw Error("No chart loaded");
        const container = document.getElementById("dataset_controls");
        container.innerHTML = "";
        if (datasets.length === 0) {
            container.innerHTML = '<span class="text-muted">No chart data present</span>';
            return;
        }
        datasets.forEach((ds) => {
            var _a;
            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-outline-danger me-1 mb-1 p-0"; // small button, no padding
            btn.style.width = "20px";
            btn.style.height = "20px";
            btn.style.borderRadius = "50%"; // circular swatch
            btn.style.backgroundColor = ds.borderColor || "#000";
            btn.title = (_a = ds.label) !== null && _a !== void 0 ? _a : "unknown";
            btn.style.cursor = "pointer";
            btn.dataset.runId = ds.runId;
            btn.dataset.dataIndex = ds.dataIndex.toString();
            btn.onclick = () => {
                var _a;
                if (!chart)
                    return;
                const runId = btn.dataset.runId;
                const dataIndex = parseInt(btn.dataset.dataIndex);
                const currentURL = new URL(window.location.href);
                let currentData = null;
                try {
                    currentData = JSON.parse((_a = currentURL.searchParams.get("data")) !== null && _a !== void 0 ? _a : "");
                }
                catch (_b) {
                    currentData = { runs: {} };
                }
                const index = datasets.indexOf(ds);
                if (index !== -1) {
                    datasets.splice(index, 1);
                    if (runId === "")
                        throw new Error("Run ID not found during data add");
                    currentData.runs[runId].splice(dataIndex, 1);
                    if (currentData.runs[runId].length === 0)
                        delete currentData.runs[runId];
                }
                if (Object.keys(currentData.runs).length === 0)
                    currentURL.searchParams.delete("data");
                else
                    currentURL.searchParams.set("data", JSON.stringify(currentData));
                window.history.pushState({}, "", currentURL);
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
            const currentURL = new URL(window.location.href);
            currentURL.searchParams.delete("data");
            window.history.pushState({}, "", currentURL);
            updateChart();
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
        dataTable.innerHTML = "";
        const timeSet = new Set();
        datasets.forEach(ds => ds.rawData.filter(p => p.x != null).forEach(p => { var _a; return timeSet.add((_a = p.x) !== null && _a !== void 0 ? _a : -1); }));
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
        const colNames = datasets.map(ds => { var _a, _b; return `<th title="${(_a = ds.fullLabel) !== null && _a !== void 0 ? _a : '?'}">${truncateLabel((_b = ds.label) !== null && _b !== void 0 ? _b : '?', 15)}</th>`; });
        thead.innerHTML = `<tr><th>Time</th>${colNames.join('')}</tr>`;
        table.appendChild(thead);
        const tbody = document.createElement("tbody");
        pageTimes.forEach(t => {
            const tr = document.createElement("tr");
            const tdTime = document.createElement("td");
            tdTime.textContent = t.toString();
            tr.appendChild(tdTime);
            datasets.forEach(ds => {
                const point = ds.rawData.find(p => p.x === t);
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
        if (datasets.length === 0) {
            tablePlaceholder.style.display = "block";
            dataTable.style.display = "none";
        }
        else {
            tablePlaceholder.style.display = "none";
            dataTable.style.display = "block";
        }
    }
});
export {};
//# sourceMappingURL=analytics.js.map