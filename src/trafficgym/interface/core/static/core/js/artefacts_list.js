"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const selectAll = document.getElementById("selectAll");
function getVisibleCheckboxes() {
    return Array.from(document.querySelectorAll("#artefactTable tr"))
        .filter(row => row.offsetParent !== null)
        .map(row => row.querySelector(".row-checkbox"))
        .filter(cb => cb !== null);
}
function updateHeaderCheckbox() {
    const visible = getVisibleCheckboxes();
    const checked = visible.filter(cb => cb.checked);
    if (checked.length === 0) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
    }
    else if (checked.length === visible.length) {
        selectAll.checked = true;
        selectAll.indeterminate = false;
    }
    else {
        selectAll.checked = false;
        selectAll.indeterminate = true;
    }
}
function updateButtonState() {
    const anyChecked = document.querySelectorAll(".row-checkbox:checked").length > 0;
    document.getElementById("transformModalButton").disabled = !anyChecked;
}
document.addEventListener("change", (e) => {
    const checkbox = e.target instanceof Element
        ? e.target.closest(".row-checkbox")
        : null;
    if (checkbox) {
        updateHeaderCheckbox();
        updateButtonState();
    }
});
const previewModal = new bootstrap.Modal(document.getElementById("previewModal"));
const transformModal = new bootstrap.Modal(document.getElementById("transformModal"));
const transformModalButton = document.getElementById("transformModalButton");
const transformModalBody = document.querySelector("#transformModalBody");
const transformButton = document.getElementById("transformButton");
var InputType;
(function (InputType) {
    InputType["FILE"] = "FILE";
    InputType["JSON"] = "JSON";
})(InputType || (InputType = {}));
function validateTransformReady(transformation) {
    if (!transformation) {
        transformButton.disabled = true;
        return;
    }
    const requiredInputs = transformation.inputs.filter(i => i.type === InputType.FILE && i.required);
    const selects = Array.from(document.querySelectorAll("#transformModalBody select"));
    // Map select values using the data attribute
    const mapped = {};
    selects.forEach(select => {
        const inputName = select.dataset.inputName;
        if (inputName) {
            mapped[inputName] = select.value;
        }
    });
    const allRequiredFilled = requiredInputs.every(i => {
        return mapped[i.name] && mapped[i.name] !== "";
    });
    transformButton.disabled = !allRequiredFilled;
}
function renderMappingUI(mappingContainer, selectedArtefacts, transformation) {
    mappingContainer.innerHTML = "";
    const title = document.createElement("h5");
    title.textContent = "Input Mapping";
    mappingContainer.appendChild(title);
    // --- FILE inputs (artefact binding) ---
    const fileInputs = transformation.inputs.filter(i => i.type === InputType.FILE);
    if (fileInputs.length > 0) {
        const fileSection = document.createElement("div");
        fileInputs.forEach(input => {
            const row = document.createElement("div");
            row.className = "d-flex align-items-center justify-content-between mb-2";
            const label = document.createElement("div");
            label.className = "fw-bold";
            label.textContent = `${input.name}${input.required ? " *" : ""}`;
            const select = document.createElement("select");
            select.className = "form-select form-select-sm w-auto";
            select.dataset.inputName = input.name;
            select.addEventListener("change", () => {
                validateTransformReady(transformation);
            });
            const empty = document.createElement("option");
            empty.value = "";
            empty.textContent = "-- select artefact --";
            select.appendChild(empty);
            selectedArtefacts.forEach(a => {
                const opt = document.createElement("option");
                opt.value = a.id;
                opt.textContent = `${a.name} ${a.role ? '(' + a.role + ')' : ""}`;
                select.appendChild(opt);
            });
            row.appendChild(label);
            row.appendChild(select);
            fileSection.appendChild(row);
        });
        mappingContainer.appendChild(fileSection);
    }
    // --- JSON inputs (code box) ---
    const jsonInputs = transformation.inputs.filter(i => i.type === InputType.JSON);
    if (jsonInputs.length > 0) {
        const jsonTitle = document.createElement("h6");
        jsonTitle.className = "mt-3";
        jsonTitle.textContent = "Parameters";
        mappingContainer.appendChild(jsonTitle);
        const wrapper = document.createElement("div");
        wrapper.className = "mb-2";
        const textarea = document.createElement("textarea");
        textarea.className = "form-control font-monospace";
        textarea.rows = 8;
        const defaultParams = {};
        transformation.inputs
            .filter(i => i.type === InputType.JSON)
            .forEach(i => {
            defaultParams[i.name] = {};
        });
        textarea.value = JSON.stringify(defaultParams, null, 2);
        wrapper.appendChild(textarea);
        mappingContainer.appendChild(wrapper);
    }
}
const style = document.createElement("style");
style.textContent = `
.list-group-item:hover {
    border-color: #86b7fe;
}

.list-group-item {
    border: 1px solid #dee2e6;
    box-sizing: border-box;
    cursor: pointer;
    transition: box-shadow 0.15s ease, border-color 0.15s ease;
}

.list-group-item:has(input[type="radio"]:checked) {
    border-color: #0d6efd;
    box-shadow: 0 0 0 2px rgba(13,110,253,.25);
}
`;
document.addEventListener("DOMContentLoaded", () => {
    document.head.appendChild(style);
    updateHeaderCheckbox();
    updateButtonState();
    selectAll.addEventListener("change", () => {
        const visible = getVisibleCheckboxes();
        visible.forEach(cb => {
            cb.checked = selectAll.checked;
        });
        // after bulk change → no intermediate state
        selectAll.indeterminate = false;
        updateButtonState();
    });
    document.querySelectorAll(".preview-btn").forEach(btn => {
        btn.addEventListener("click", () => __awaiter(void 0, void 0, void 0, function* () {
            const filepath = btn.dataset.filepath;
            const res = yield fetch(`/media/${filepath}`);
            const titleEl = document.getElementById("previewTitle");
            const container = document.getElementById("previewContent");
            titleEl.textContent = "Preview";
            container.innerHTML = "";
            const contentType = res.headers.get("content-type") || "";
            if (!res.ok) {
                let msg = "Failed to load preview";
                if (res.status === 404)
                    msg = "Could not find artefact content";
                else if (res.status === 415)
                    msg = "File is not previewable";
                container.textContent = msg;
            }
            else if (contentType.includes("image/png") || contentType.includes("image/jpeg")) {
                const blob = yield res.blob();
                const url = URL.createObjectURL(blob);
                const img = document.createElement("img");
                img.src = url;
                img.className = "img-fluid";
                container.appendChild(img);
            }
            else {
                const text = yield res.text();
                container.textContent = text;
            }
            previewModal.show();
        }));
    });
    transformModalButton.addEventListener("click", () => __awaiter(void 0, void 0, void 0, function* () {
        transformButton.disabled = true;
        const selected = getVisibleCheckboxes().filter(cb => cb.checked);
        transformModalBody.innerHTML = "";
        const subtitleMethod = document.createElement("h5");
        subtitleMethod.innerHTML = "Method";
        transformModalBody.append(subtitleMethod);
        let transformations_list = { transformations: [] };
        try {
            const response = yield fetch("/api/list_transformations");
            if (!response.ok) {
                const reason = yield response.text();
                let parsedReason = reason;
                try {
                    const json = JSON.parse(reason);
                    parsedReason = json.error || reason;
                }
                catch (_a) { }
                const match = parsedReason.match(/details = "(.*?)"/);
                if (match) {
                    parsedReason = match[1];
                }
                throw new Error(`HTTP ${response.status}`, { cause: parsedReason });
            }
            transformations_list = yield response.json();
            const radioGroup = document.createElement("div");
            radioGroup.className = "list-group mb-3";
            const selectedArtefacts = selected.map(cb => ({
                id: cb.value,
                name: cb.dataset.name || cb.value,
                role: cb.dataset.role || ""
            }));
            const mappingContainer = document.createElement("div");
            for (const transformation of transformations_list.transformations) {
                const wrapper = document.createElement("label");
                wrapper.className = "list-group-item";
                const radio = document.createElement("input");
                radio.type = "radio";
                radio.name = "transformation";
                radio.value = transformation.key;
                radio.className = "form-check-input me-2 d-none";
                radio.addEventListener("change", () => {
                    renderMappingUI(mappingContainer, selectedArtefacts, transformation);
                    validateTransformReady(transformation);
                });
                const titleRow = document.createElement("div");
                titleRow.className = "d-flex align-items-center gap-2";
                const title = document.createElement("div");
                title.className = "fw-bold";
                title.textContent = transformation.key;
                // icon
                const infoIcon = document.createElement("i");
                infoIcon.className = "bi bi-question-circle text-muted";
                infoIcon.style.cursor = "pointer";
                infoIcon.setAttribute("title", transformation.docstring || "No description available");
                infoIcon.setAttribute("data-bs-toggle", "tooltip");
                infoIcon.setAttribute("data-bs-placement", "top");
                titleRow.appendChild(title);
                titleRow.appendChild(infoIcon);
                const inputs = document.createElement("div");
                inputs.className = "small text-muted";
                inputs.textContent =
                    "Inputs: " +
                        transformation.inputs
                            .map(i => `${i.name} (${i.type}${i.required ? ", required" : ""})`)
                            .join(", ");
                const outputs = document.createElement("div");
                outputs.className = "small text-muted";
                outputs.textContent =
                    "Outputs: " + transformation.outputs.join(", ");
                const content = document.createElement("div");
                content.appendChild(titleRow);
                content.appendChild(inputs);
                content.appendChild(outputs);
                wrapper.appendChild(radio);
                wrapper.appendChild(content);
                radioGroup.appendChild(wrapper);
            }
            transformModalBody.appendChild(radioGroup);
            transformModalBody.appendChild(mappingContainer);
        }
        catch (err) {
            const alert = document.createElement("div");
            alert.className = "alert alert-danger d-flex align-items-center";
            alert.role = "alert";
            alert.innerHTML = `
                <div>
                    <p><i class="bi bi-exclamation-triangle-fill me-2"></i>Failed to load transformation methods. Please try again.</p>
                    <p>${err}</p>
                    <pre class="mb-0" style="white-space: pre-wrap; word-break: break-word;">${err.cause || ""}</pre>
                </div>
            `;
            transformModalBody.appendChild(alert);
        }
        transformModal.show();
        document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el);
        });
    }));
    transformButton.addEventListener("click", () => __awaiter(void 0, void 0, void 0, function* () {
        transformButton.disabled = true;
        const originalText = transformButton.innerHTML;
        transformButton.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Processing...`;
        const selectedRadio = document.querySelector('input[name="transformation"]:checked');
        const method = selectedRadio === null || selectedRadio === void 0 ? void 0 : selectedRadio.value;
        const inputsMap = {};
        const selects = document.querySelectorAll("#transformModalBody select");
        selects.forEach(select => {
            const inputName = select.dataset.inputName;
            if (inputName && select.value) {
                inputsMap[inputName] = select.value; // The SHA256
            }
        });
        const textarea = document.querySelector("#transformModalBody textarea");
        let parameters = "{}";
        if (textarea) {
            try {
                // Verify it's valid JSON before sending, but send as string
                JSON.parse(textarea.value);
                parameters = textarea.value;
            }
            catch (e) {
                alert("Invalid JSON in Parameters");
                resetButton(transformButton, originalText);
                return;
            }
        }
        const formData = new FormData();
        formData.append("method", method);
        formData.append("inputs", JSON.stringify(inputsMap));
        formData.append("simulation_parameters", parameters);
        try {
            const response = yield fetch("/create_transformation_request", {
                method: "POST",
                headers: {
                    "X-CSRFToken": document.querySelector('[name=csrfmiddlewaretoken]').value
                },
                body: formData
            });
            if (response.redirected) {
                // Django's redirect("transformation_request_detail") will land here
                window.location.href = response.url;
            }
            else if (response.ok) {
                window.location.reload();
            }
            else if (!response.ok) {
                const html = yield response.text();
                // 1. Create a blob from the HTML string
                const blob = new Blob([html], { type: 'text/html' });
                // 2. Create a temporary URL for that blob
                const blobUrl = URL.createObjectURL(blob);
                // 3. Navigate the window to that URL
                window.location.assign(blobUrl);
            }
            else {
                const errorText = yield response.text();
                throw new Error(errorText);
            }
        }
        catch (err) {
            resetButton(transformButton, originalText);
            alert(`Transformation Error: ${err}`);
        }
    }));
    function resetButton(btn, text) {
        btn.disabled = false;
        btn.innerHTML = text;
    }
});
//# sourceMappingURL=artefacts_list.js.map