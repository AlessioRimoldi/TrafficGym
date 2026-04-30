const selectAll = document.getElementById("selectAll") as HTMLInputElement;

function getVisibleCheckboxes() {
    return Array.from(document.querySelectorAll("#artefactTable tr") as NodeListOf<HTMLTableRowElement>)
        .filter(row => row.offsetParent !== null)
        .map(row => row.querySelector(".row-checkbox") as HTMLInputElement)
        .filter(cb => cb !== null);
}

function updateHeaderCheckbox() {
    const visible = getVisibleCheckboxes();
    const checked = visible.filter(cb => cb.checked);

    if (checked.length === 0) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
    } else if (checked.length === visible.length) {
        selectAll.checked = true;
        selectAll.indeterminate = false;
    } else {
        selectAll.checked = false;
        selectAll.indeterminate = true;
    }
}

function updateButtonState() {
    const anyChecked = (document.querySelectorAll(".row-checkbox:checked") as NodeListOf<HTMLInputElement>).length > 0;
    (document.getElementById("transformModalButton") as HTMLButtonElement).disabled = !anyChecked;
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

const transformModal = new bootstrap.Modal(document.getElementById("transformModal") as HTMLDivElement);

const transformModalButton = document.getElementById("transformModalButton") as HTMLButtonElement;
const transformModalBody = document.querySelector("#transformModalBody") as HTMLDivElement;

const transformButton = document.getElementById("transformButton") as HTMLButtonElement;

enum InputType {
  FILE = "FILE",
  JSON = "JSON",
}

interface InputSpec {
  name: string;
  type: InputType;
  required: boolean;
}

interface TransformationSpec {
  key: string;
  inputs: InputSpec[];
  outputs: string[];
  docstring: string;
}

let selectedTransformation: TransformationSpec | null = null;

function validateTransformReady(transformation: TransformationSpec | null) {
    if (!transformation) {
        transformButton.disabled = true;
        return;
    }

    const requiredInputs = transformation.inputs.filter(
        i => i.type === InputType.FILE && i.required
    );

    const selects = Array.from(
        document.querySelectorAll("#transformModalBody select")
    ) as HTMLSelectElement[];

    // Map select values using the data attribute
    const mapped: Record<string, string> = {};
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

function renderMappingUI(mappingContainer: HTMLDivElement, selectedArtefacts: { "id": string, "name": string, "role": string }[], transformation: TransformationSpec) {
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
    
        const defaultParams: Record<string, any> = {};
    
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

    transformModalButton.addEventListener("click", async () => {
        transformButton.disabled = true;

        const selected = getVisibleCheckboxes().filter(cb => cb.checked);

        transformModalBody.innerHTML = "";

        const subtitleMethod = document.createElement("h5");
        subtitleMethod.innerHTML = "Method";
        transformModalBody.append(subtitleMethod)

        let transformations_list: { transformations: TransformationSpec[] } = { transformations: [] }

        try {
            const response = await fetch("/api/list_transformations");
        
            if (!response.ok) {
                const reason = await response.text();

                let parsedReason = reason;

                try {
                    const json = JSON.parse(reason);
                    parsedReason = json.error || reason;
                } catch { }

                const match = parsedReason.match(/details = "(.*?)"/);
                    if (match) {
                        parsedReason = match[1];
                    }

                throw new Error(`HTTP ${response.status}`, { cause: parsedReason } );
            }
        
            transformations_list = await response.json();

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
                    selectedTransformation = transformation;

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

        } catch (err) {
            const alert = document.createElement("div");
            alert.className = "alert alert-danger d-flex align-items-center";
            alert.role = "alert";
        
            alert.innerHTML = `
                <div>
                    <p><i class="bi bi-exclamation-triangle-fill me-2"></i>Failed to load transformation methods. Please try again.</p>
                    <p>${err}</p>
                    <pre class="mb-0" style="white-space: pre-wrap; word-break: break-word;">${(err as Error).cause || ""}</pre>
                </div>
            `;
        
            transformModalBody.appendChild(alert);
        }

        transformModal.show();

        document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el);
        });
    })

    transformButton.addEventListener("click", async () => {
        transformButton.disabled = true;
        const originalText = transformButton.innerHTML;
        transformButton.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Processing...`;
    
        const selectedRadio = document.querySelector('input[name="transformation"]:checked') as HTMLInputElement;
        const method = selectedRadio?.value;

        const schema = selectedTransformation
            ? {
                key: selectedTransformation.key,
                inputs: selectedTransformation.inputs,
                outputs: selectedTransformation.outputs
            }
            : null;

        const inputsMap: Record<string, string> = {};
        const selects = document.querySelectorAll("#transformModalBody select") as NodeListOf<HTMLSelectElement>;
        selects.forEach(select => {
            const inputName = select.dataset.inputName;
            if (inputName && select.value) {
                inputsMap[inputName] = select.value; // The SHA256
            }
        });
    
        const textarea = document.querySelector("#transformModalBody textarea") as HTMLTextAreaElement;
        let parameters = "{}";
        if (textarea) {
            try {
                // Verify it's valid JSON before sending, but send as string
                JSON.parse(textarea.value);
                parameters = textarea.value;
            } catch (e) {
                alert("Invalid JSON in Parameters");
                resetButton(transformButton, originalText);
                return;
            }
        }
    
        const formData = new FormData();
        formData.append("method", method);
        formData.append("schema", JSON.stringify(schema));
        formData.append("inputs", JSON.stringify(inputsMap));
        formData.append("simulation_parameters", parameters);
    
        try {
            const response = await fetch("/create_transformation_request", { // Use your actual URL or {% url %}
                method: "POST",
                headers: {
                    "X-CSRFToken": (document.querySelector('[name=csrfmiddlewaretoken]') as HTMLInputElement).value
                },
                body: formData
            });
    
            if (response.redirected) {
                // Django's redirect("transformation_request_detail") will land here
                window.location.href = response.url;
            } else if (response.ok) {
                window.location.reload();
            } else if (!response.ok) {
                const html = await response.text();
        
                // 1. Create a blob from the HTML string
                const blob = new Blob([html], { type: 'text/html' });
                
                // 2. Create a temporary URL for that blob
                const blobUrl = URL.createObjectURL(blob);
                
                // 3. Navigate the window to that URL
                window.location.assign(blobUrl);
            } else {
                const errorText = await response.text();
                throw new Error(errorText);
            }
        } catch (err) {
            resetButton(transformButton, originalText);
            alert(`Transformation Error: ${err}`);
        }
    });
    
    function resetButton(btn: HTMLButtonElement, text: string) {
        btn.disabled = false;
        btn.innerHTML = text;
    }
})