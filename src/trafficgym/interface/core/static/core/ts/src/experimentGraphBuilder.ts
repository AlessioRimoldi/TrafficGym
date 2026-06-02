interface InspectionData {
    tls_ids: string[];
    edge_ids: string[];
    lane_ids: string[];
    detector_ids: string[];
}

// ─── Domain definitions (populated from /api/domains) ────────────────────────

interface GetterDef { getter: string; type: string; output_key?: string }
interface SetterDef { setter: string; sumo_param: string; type: string; input_key?: string }
interface ObserverDomainEntry { domain: string; getters: GetterDef[] }
interface ActuatorDomainEntry { domain: string; setters: SetterDef[] }

// These are populated by openBuilder before renderPipeline is called.
let OBSERVER_DEFS: Record<string, ObserverDomainEntry> = {};
let ACTUATOR_DEFS: Record<string, ActuatorDomainEntry> = {};

// Derived flat lookups — recomputed after each fetch.
let OBSERVER_GETTERS:      Record<string, string[]>              = {};
let GETTER_TYPE:            Record<string, string>               = {};
let GETTER_DEF:             Record<string, GetterDef>            = {};
let ACTUATOR_SETTERS:       Record<string, string[]>             = {};
let SETTER_INPUT:           Record<string, { sumo_param: string; type: string; input_key?: string }> = {};
let OBSERVER_DOMAIN:        Record<string, string>               = {};
let ACTUATOR_DOMAIN:        Record<string, string>               = {};
let OBSERVER_TYPE_FROM_DOMAIN: Record<string, string>            = {};
let ACTUATOR_TYPE_FROM_DOMAIN: Record<string, string>            = {};

function applyDomainData(data: { observers: Record<string, ObserverDomainEntry>; actuators: Record<string, ActuatorDomainEntry> }): void {
    OBSERVER_DEFS = data.observers;
    ACTUATOR_DEFS = data.actuators;

    OBSERVER_GETTERS = Object.fromEntries(Object.entries(OBSERVER_DEFS).map(([k, v]) => [k, v.getters.map(g => g.getter)]));
    GETTER_TYPE = {};
    GETTER_DEF  = {};
    for (const { getters } of Object.values(OBSERVER_DEFS))
        for (const g of getters) { GETTER_TYPE[g.getter] = g.type; GETTER_DEF[g.getter] = g; }

    ACTUATOR_SETTERS = Object.fromEntries(Object.entries(ACTUATOR_DEFS).map(([k, v]) => [k, v.setters.map(s => s.setter)]));
    SETTER_INPUT = {};
    for (const { setters } of Object.values(ACTUATOR_DEFS))
        for (const s of setters) SETTER_INPUT[s.setter] = { sumo_param: s.sumo_param, type: s.type, input_key: s.input_key };

    OBSERVER_DOMAIN = Object.fromEntries(Object.entries(OBSERVER_DEFS).map(([k, v]) => [k, v.domain]));
    ACTUATOR_DOMAIN = Object.fromEntries(Object.entries(ACTUATOR_DEFS).map(([k, v]) => [k, v.domain]));
    OBSERVER_TYPE_FROM_DOMAIN = Object.fromEntries(Object.entries(OBSERVER_DOMAIN).map(([t, d]) => [d, t]));
    ACTUATOR_TYPE_FROM_DOMAIN = Object.fromEntries(Object.entries(ACTUATOR_DOMAIN).map(([t, d]) => [d, t]));
}

type ParamDef =
    | { type?: "number"; name: string; label: string; default: number }
    | { type: "string";  name: string; label: string; default: string }
    | { type: "phase_list"; name: string; label: string }
    | { type: "select"; name: string; label: string; default: string; choices: string[] };

interface BlockEntry {
    key: string;
    label: string;
    module?: string;
    description?: string;
    input_key?: string;
    input_type?: string;
    output_key?: string;
    output_type?: string;
    params?: ParamDef[];
}

// Populated from /api/blocks and /api/domains when the graph builder opens.
let BLOCKS: BlockEntry[] = [];

// ─── Colours ──────────────────────────────────────────────────────────────────

const BADGE: Record<string, string> = {
    detector:   "bg-success",
    tls:        "bg-danger",
    edge:       "bg-primary",
    lane:       "bg-secondary",
};

const BTN: Record<string, string> = {
    detector: "btn-outline-success",
    tls:      "btn-outline-danger",
    edge:     "btn-outline-primary",
    lane:     "btn-outline-secondary",
};

// ─── Shared types ─────────────────────────────────────────────────────────────

interface OnEnterAction {
    domain: string;
    setter: string;
    object_id: string;
    params: string;
}

interface Phase {
    id: string;
    duration_s: number | null;
    active_pipelines: Set<string>;
    on_enter: OnEnterAction[];
}

interface PipelineReg {
    id: string;
    name: string;
}

interface StaticTLSPhaseRow {
    state: string;
    duration: number;
}

interface BlockReg {
    id: string;
    key: string;
    params: Record<string, unknown>;
    inPort: HTMLElement;
    outPort: HTMLElement;
}

interface ObserverNodeRec {
    id: string;
    type: string;
    objectId: string;
    getterEl: HTMLSelectElement;
    outPort: HTMLElement;
}

interface ActuatorNodeRec {
    id: string;
    type: string;
    objectId: string;
    setterEl: HTMLSelectElement;
    inPort: HTMLElement;
}

interface SinkReg {
    id: string;
    labelEl: HTMLInputElement;
    inPort: HTMLElement;
}

// ─── Serialised graph spec ─────────────────────────────────────────────────────

interface BlockSpec {
    id: string;
    key: string;
    params: Record<string, unknown>;
    inputs_from: string[];
    actuate_to: string | null;
}

interface PipelineSpec {
    id: string;
    name: string;
    observers: { id: string; domain: string; getter: string; object_id: string }[];
    blocks: BlockSpec[];
    actuators: { id: string; domain: string; setter: string; object_id: string }[];
    sinks: { id: string; label: string; input_from: string | null }[];
    stage_order?: { type: "block" | "sink"; index: number }[];
}

interface GraphSpec {
    name: string;
    scenario_id: string;
    pipelines: PipelineSpec[];
    phases: {
        duration_s: number | null;
        active_pipeline_ids: string[];
        on_enter: { domain: string; setter: string; object_id: string; params: Record<string, unknown> }[];
    }[];
}

const ENTER_DOMAIN_SETTERS: Record<string, string[]> = {
    trafficlight: ["setProgram", "setRedYellowGreenState", "setPhase"],
};

interface Conn { from: HTMLElement; to: HTMLElement; line: SVGLineElement; compatible: boolean }

// ─── Custom port tooltip ──────────────────────────────────────────────────────

let _portTooltipEl: HTMLDivElement | null = null;

function getPortTooltipEl(): HTMLDivElement {
    if (!_portTooltipEl) {
        _portTooltipEl = document.createElement("div");
        Object.assign(_portTooltipEl.style, {
            position: "fixed",
            background: "rgba(15,15,15,0.82)",
            color: "#fff",
            padding: "2px 8px",
            borderRadius: "4px",
            fontSize: "12px",
            fontFamily: "monospace",
            pointerEvents: "none",
            zIndex: "9999",
            display: "none",
            whiteSpace: "nowrap",
        });
        document.body.appendChild(_portTooltipEl);
    }
    return _portTooltipEl;
}

// ─── Type-compatibility check ─────────────────────────────────────────────────

function typesCompatible(a: string, b: string): boolean {
    if (a === b) return true;
    const numeric = new Set(["int", "float"]);
    return numeric.has(a) && numeric.has(b);
}

// ─── Top-level utility builders ───────────────────────────────────────────────

function makeSelect(options: string[]): HTMLSelectElement {
    const sel = document.createElement("select");
    sel.className = "form-select form-select-sm ms-2";
    sel.style.maxWidth = "160px";
    for (const o of options) {
        const opt = document.createElement("option");
        opt.value = opt.textContent = o;
        sel.appendChild(opt);
    }
    return sel;
}

function badge(type: string): HTMLSpanElement {
    const b = document.createElement("span");
    b.className = `badge ${BADGE[type] ?? "bg-secondary"} me-2`;
    b.textContent = type;
    return b;
}

function idLabel(text: string): HTMLSpanElement {
    const s = document.createElement("span");
    s.className = "font-monospace small";
    s.textContent = text;
    return s;
}

function makeStaticPhaseRow(row: StaticTLSPhaseRow, rows: StaticTLSPhaseRow[]): HTMLDivElement {
    const div = document.createElement("div");
    div.className = "d-flex align-items-center gap-1 mb-1";

    const stateInput = document.createElement("input");
    stateInput.type = "text";
    stateInput.className = "form-control form-control-sm font-monospace flex-grow-1";
    stateInput.placeholder = "GGGrrr…";
    stateInput.value = row.state;
    stateInput.addEventListener("input", () => { row.state = stateInput.value; });

    const durInput = document.createElement("input");
    durInput.type = "number";
    durInput.className = "form-control form-control-sm";
    durInput.style.width = "58px";
    durInput.min = "1";
    durInput.value = String(row.duration);
    durInput.addEventListener("input", () => { row.duration = Number(durInput.value) || 1; });

    const durUnit = document.createElement("span");
    durUnit.className = "small text-muted";
    durUnit.textContent = "s";

    const rmBtn = document.createElement("button");
    rmBtn.type = "button";
    rmBtn.className = "btn btn-sm btn-link text-danger p-0";
    rmBtn.innerHTML = "&times;";
    rmBtn.addEventListener("click", () => {
        const idx = rows.indexOf(row);
        if (idx !== -1) rows.splice(idx, 1);
        div.remove();
    });

    div.append(stateInput, durInput, durUnit, rmBtn);
    return div;
}

function addDropdown(
    label: string,
    btnClass: string,
    ids: string[],
    onAdd: (id: string, onRemove: () => void) => void,
): HTMLDivElement {
    const wrap = document.createElement("div");
    wrap.className = "dropdown";
    wrap.style.minWidth = "0";

    const btn = document.createElement("button");
    btn.className = `btn btn-sm ${btnClass} dropdown-toggle w-100`;
    btn.setAttribute("data-bs-toggle", "dropdown");
    btn.setAttribute("data-bs-auto-close", "outside");
    btn.textContent = `+ ${label}`;
    btn.title = label;
    Object.assign(btn.style, { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" });

    const menu = document.createElement("ul");
    menu.className = "dropdown-menu";

    const searchLi = document.createElement("li");
    searchLi.className = "px-2 pt-2 pb-1";
    const searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.className = "form-control form-control-sm";
    searchInput.placeholder = "Search…";
    searchInput.addEventListener("click", e => e.stopPropagation());
    searchInput.addEventListener("keydown", e => e.stopPropagation());
    searchLi.appendChild(searchInput);
    menu.appendChild(searchLi);

    const divLi = document.createElement("li");
    divLi.innerHTML = '<hr class="dropdown-divider my-0">';
    menu.appendChild(divLi);

    const scrollLi = document.createElement("li");
    Object.assign(scrollLi.style, { display: "block", maxHeight: "180px", overflowY: "auto" });

    const added = new Set<string>();
    const itemEls: HTMLAnchorElement[] = [];

    for (const id of ids) {
        const a = document.createElement("a");
        a.className = "dropdown-item font-monospace small";
        a.href = "#";
        a.textContent = id;
        a.dataset.id = id;
        a.addEventListener("click", (e) => {
            e.preventDefault();
            if (added.has(id)) return;
            added.add(id);
            onAdd(id, () => added.delete(id));
        });
        scrollLi.appendChild(a);
        itemEls.push(a);
    }

    searchInput.addEventListener("input", () => {
        const q = searchInput.value.toLowerCase();
        itemEls.forEach(a => {
            a.style.display = a.dataset.id!.toLowerCase().includes(q) ? "" : "none";
        });
    });

    wrap.addEventListener("hidden.bs.dropdown", () => {
        searchInput.value = "";
        itemEls.forEach(a => (a.style.display = ""));
    });

    menu.appendChild(scrollLi);
    wrap.append(btn, menu);

    // Must come after wrap.append so nextElementSibling resolves.
    // Fixed strategy escapes overflow-clipping ancestors; z-index 2000 clears the modal (1055).
    wrap.style.setProperty("--bs-dropdown-zindex", "2000");
    if ((window as any).bootstrap?.Dropdown) {
        new (window as any).bootstrap.Dropdown(btn, {
            popperConfig(d: any) { return { ...d, strategy: "fixed" }; },
        });
    }

    return wrap;
}

function renumberPhases(phaseList: HTMLElement): void {
    phaseList.querySelectorAll<HTMLElement>(".phase-label").forEach((el, i) => {
        el.textContent = `Phase ${i + 1}`;
    });
}

function makeActionRow(action: OnEnterAction, phase: Phase, data: InspectionData): HTMLDivElement {
    const row = document.createElement("div");
    row.className = "d-flex align-items-center gap-1 flex-wrap mb-1";

    const domainSel = document.createElement("select");
    domainSel.className = "form-select form-select-sm";
    domainSel.style.width = "130px";

    const setterSel = document.createElement("select");
    setterSel.className = "form-select form-select-sm";
    setterSel.style.width = "170px";

    const objSel = document.createElement("select");
    objSel.className = "form-select form-select-sm";
    objSel.style.width = "110px";

    const paramsInput = document.createElement("input");
    paramsInput.type = "text";
    paramsInput.className = "form-control form-control-sm font-monospace";
    paramsInput.style.width = "160px";
    paramsInput.placeholder = '{"key": "val"}';
    paramsInput.value = action.params;
    paramsInput.addEventListener("input", () => { action.params = paramsInput.value; });

    const domainIds: Record<string, string[]> = { trafficlight: data.tls_ids };

    function refreshSetters() {
        setterSel.innerHTML = "";
        for (const s of (ENTER_DOMAIN_SETTERS[action.domain] ?? [])) {
            const o = document.createElement("option");
            o.value = o.textContent = s;
            if (s === action.setter) o.selected = true;
            setterSel.appendChild(o);
        }
        action.setter = setterSel.value;
    }

    function refreshObjects() {
        objSel.innerHTML = "";
        for (const id of (domainIds[action.domain] ?? [])) {
            const o = document.createElement("option");
            o.value = o.textContent = id;
            if (id === action.object_id) o.selected = true;
            objSel.appendChild(o);
        }
        action.object_id = objSel.value;
    }

    for (const d of Object.keys(ENTER_DOMAIN_SETTERS)) {
        const o = document.createElement("option");
        o.value = o.textContent = d;
        if (d === action.domain) o.selected = true;
        domainSel.appendChild(o);
    }

    domainSel.addEventListener("change", () => { action.domain = domainSel.value; refreshSetters(); refreshObjects(); });
    setterSel.addEventListener("change", () => { action.setter = setterSel.value; });
    objSel.addEventListener("change", () => { action.object_id = objSel.value; });

    refreshSetters();
    refreshObjects();

    const rmBtn = document.createElement("button");
    rmBtn.className = "btn btn-sm btn-link text-danger p-0";
    rmBtn.innerHTML = "&times;";
    rmBtn.addEventListener("click", () => {
        const idx = phase.on_enter.indexOf(action);
        if (idx !== -1) phase.on_enter.splice(idx, 1);
        row.remove();
    });

    row.append(domainSel, setterSel, objSel, paramsInput, rmBtn);
    return row;
}

// ─── Pipeline row builder ─────────────────────────────────────────────────────

function createPipelineRow(
    data: InspectionData,
    reg: PipelineReg,
    onRemove: () => void,
    onErrorsChange: (errors: string[]) => void = () => {},
    initialPipeline: PipelineSpec | null = null,
    missingObjectIds: Set<string> = new Set(),
): { el: HTMLDivElement; serialize: () => PipelineSpec; drawConnections: () => void } {
    let pendingPort: HTMLElement | null = null;
    let svgEl!: SVGSVGElement;
    const conns: Conn[] = [];
    const blockRegs: BlockReg[] = [];
    const obsNodes: ObserverNodeRec[] = [];
    const actNodes: ActuatorNodeRec[] = [];
    const sinkRegs: SinkReg[] = [];
    let blkCounter = 0;
    let obsCounter = 0;
    let actCounter = 0;
    let sinkCounter = 0;
    let stageSeq = 0;

    const markerId      = `arrow_${reg.id}`;
    const errorMarkerId = `arrow_err_${reg.id}`;

    // ── Connection helpers ────────────────────────────────────────────────────

    function portCenter(port: HTMLElement): { x: number; y: number } {
        const pr = port.getBoundingClientRect();
        const sr = svgEl.getBoundingClientRect();
        return { x: pr.left + pr.width / 2 - sr.left, y: pr.top + pr.height / 2 - sr.top };
    }

    function syncLine(conn: Conn): void {
        const c = portCenter(conn.from);
        const d = portCenter(conn.to);
        conn.line.setAttribute("x1", String(c.x));
        conn.line.setAttribute("y1", String(c.y));
        conn.line.setAttribute("x2", String(d.x));
        conn.line.setAttribute("y2", String(d.y));
    }

    function dropConnsFor(ports: HTMLElement[]): void {
        for (let i = conns.length - 1; i >= 0; i--) {
            if (ports.includes(conns[i].from) || ports.includes(conns[i].to)) {
                conns[i].line.remove();
                conns.splice(i, 1);
            }
        }
        // Ports on multi-input blocks shift after a sibling connection is removed.
        // Wait for the DOM to re-layout before repositioning surviving lines.
        requestAnimationFrame(() => { for (const c of conns) syncLine(c); });
        revalidate();
    }

    // portType: raw type for compatibility checks (e.g. "float", "list[str]")
    // portLabel: display string for tooltip (e.g. "occupancy: float")
    // portKey: semantic variable name for strict matching (e.g. "occupancy")
    // All stored in dataset so they can be updated after creation.
    function makePort(side: "input" | "output", portType?: string, portLabel?: string, portKey?: string): HTMLSpanElement {
        const p = document.createElement("span");
        p.dataset.side = side;
        if (portType)  p.dataset.portType  = portType;
        if (portLabel) p.dataset.portLabel = portLabel;
        if (portKey)   p.dataset.portKey   = portKey;
        Object.assign(p.style, {
            position: "absolute",
            top: "50%",
            transform: "translateY(-50%)",
            [side === "output" ? "right" : "left"]: "-8px",
            width: "14px",
            height: "14px",
            borderRadius: "50%",
            background: "#fff",
            border: "2px solid #adb5bd",
            cursor: "pointer",
            zIndex: "20",
            transition: "border-color .15s, background .15s",
        });
        p.addEventListener("click", (e) => { e.stopPropagation(); onPort(p); });
        // Custom tooltip reads dataset dynamically so updates after creation are reflected.
        // Set display:block first so offsetWidth/offsetHeight are accurate (forces reflow).
        p.addEventListener("mousemove", (e) => {
            const text = p.dataset.portLabel ?? p.dataset.portType;
            if (!text) return;
            const el = getPortTooltipEl();
            el.textContent = text;
            el.style.display = "block";
            const w = el.offsetWidth;
            const h = el.offsetHeight;
            const left = Math.max(4, Math.min(e.clientX - w / 2, window.innerWidth - w - 4));
            el.style.left = `${left}px`;
            el.style.top  = `${e.clientY - h - 10}px`;
        });
        p.addEventListener("mouseleave", () => { getPortTooltipEl().style.display = "none"; });
        return p;
    }

    function connIsCompatible(from: HTMLElement, to: HTMLElement): boolean {
        const ft = from.dataset.portType;
        const tt = to.dataset.portType;
        if (!ft || !tt) return true;                  // either side unknown = wildcard
        if (!typesCompatible(ft, tt)) return false;   // type mismatch
        const fk = from.dataset.portKey;
        const tk = to.dataset.portKey;
        if (fk && tk && fk !== tk) return false;      // explicit key mismatch
        if (!fk && tk) return false;                  // destination requires a key but source has none
        return true;
    }

    function applyConnStyle(conn: Conn): void {
        const ok = conn.compatible;
        conn.line.setAttribute("stroke", ok ? "#6c757d" : "#dc3545");
        conn.line.setAttribute("stroke-width", ok ? "2" : "2.5");
        conn.line.setAttribute("marker-end", `url(#${ok ? markerId : errorMarkerId})`);
    }

    function connectPorts(from: HTMLElement, to: HTMLElement): void {
        const compatible = connIsCompatible(from, to);
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        const conn: Conn = { from, to, line, compatible };
        applyConnStyle(conn);
        svgEl.appendChild(line);
        conns.push(conn);
        syncLine(conn);
        revalidate();
    }

    function revalidate(): void {
        // Step 1: infer output types for blocks with no declared output_type.
        for (const br of blockRegs) {
            const bDef = BLOCKS.find(b => b.key === br.key);
            if (bDef?.output_type) continue;  // has a declared type — skip

            const inConns = conns.filter(c => c.to === br.inPort);

            // Constant: output type from params["value_type"], key from params["output_key"].
            // Not input-driven — always set regardless of connections.
            if (br.key === "Constant") {
                const outKey  = (br.params["output_key"] as string) || "";
                const outType = (br.params["value_type"] as string) || "float";
                br.outPort.dataset.portType  = outType;
                br.outPort.dataset.portKey   = outKey;
                br.outPort.dataset.portLabel = outKey ? `${outKey}: ${outType}` : outType;
                continue;
            }

            // Renamer: output key from params["output_key"], type passes through from single input.
            if (br.key === "Renamer") {
                if (inConns.length === 0) {
                    br.outPort.dataset.portType  = "";
                    br.outPort.dataset.portKey   = "";
                    br.outPort.dataset.portLabel = "";
                } else {
                    const outKey  = (br.params["output_key"] as string) || "";
                    const outType = inConns[0].from.dataset.portType || "";
                    br.outPort.dataset.portType  = outType;
                    br.outPort.dataset.portKey   = outKey;
                    br.outPort.dataset.portLabel = outKey && outType ? `${outKey}: ${outType}` : (outType || outKey);
                }
                continue;
            }

            // Aggregator inference: derive output from connected inputs.
            const hasDynOutputKey = Object.prototype.hasOwnProperty.call(br.params, "output_key");
            if (inConns.length === 0) {
                // Show the param value so the port isn't invisibly empty.
                const paramKey = hasDynOutputKey ? String(br.params["output_key"] || "") : "";
                br.outPort.dataset.portType  = "";
                br.outPort.dataset.portKey   = paramKey;
                br.outPort.dataset.portLabel = paramKey;
                continue;
            }
            const keys  = [...new Set(inConns.map(c => c.from.dataset.portKey).filter(Boolean))];
            const types = [...new Set(inConns.map(c => c.from.dataset.portType).filter(Boolean))];
            const allNumeric = types.length > 0 && types.every(t => t === "float" || t === "int");
            const outKey  = keys.length === 1 ? keys[0] : "";
            // Averaging always produces float; if types are incompatible leave blank.
            const outType = allNumeric ? "float" : (types.length === 1 ? types[0] : "");
            br.outPort.dataset.portType  = outType;
            br.outPort.dataset.portKey   = outKey;
            br.outPort.dataset.portLabel = outKey && outType ? `${outKey}: ${outType}` : outType;
            // Keep the output_key param in sync so codegen uses the inferred key.
            if (outKey && hasDynOutputKey) br.params["output_key"] = outKey;
        }

        // Step 2: re-check all connection compatibility (uses updated port data).
        for (const conn of conns) {
            conn.compatible = connIsCompatible(conn.from, conn.to);
            applyConnStyle(conn);
        }

        // Step 3: collect errors.
        const errors: string[] = [];

        // Aggregator input uniformity errors (skip Renamer — single input by design).
        for (const br of blockRegs) {
            const bDef = BLOCKS.find(b => b.key === br.key);
            if (bDef?.output_type || br.key === "Renamer") continue;
            const inConns = conns.filter(c => c.to === br.inPort);
            if (inConns.length < 2) continue;
            const label = bDef?.label ?? br.key;
            const keys  = [...new Set(inConns.map(c => c.from.dataset.portKey).filter(Boolean))];
            const types = [...new Set(inConns.map(c => c.from.dataset.portType).filter(Boolean))];
            if (keys.length > 1)
                errors.push(`${label}: inputs have different variable names (${keys.join(", ")})`);
            if (types.length > 1 && !types.every(t => t === "float" || t === "int"))
                errors.push(`${label}: incompatible input types (${types.join(", ")})`);
        }

        // Connection type/key mismatch errors.
        for (const conn of conns) {
            if (!conn.compatible) {
                const ft = conn.from.dataset.portType ?? "?";
                const tt = conn.to.dataset.portType   ?? "?";
                const fk = conn.from.dataset.portKey;
                const tk = conn.to.dataset.portKey;
                const fl = conn.from.dataset.portLabel ?? ft;
                const tl = conn.to.dataset.portLabel   ?? tt;
                const reason = !typesCompatible(ft, tt)
                    ? `type mismatch: ${ft} ≠ ${tt}`
                    : `variable mismatch: ${fk} ≠ ${tk}`;
                errors.push(`${fl} → ${tl} (${reason})`);
            }
        }
        onErrorsChange(errors);
    }

    function onPort(port: HTMLElement): void {
        if (!pendingPort) {
            if (port.dataset.side !== "output") return;
            pendingPort = port;
            port.style.background = "#0d6efd";
            port.style.borderColor = "#0d6efd";
            return;
        }
        if (port === pendingPort) {
            pendingPort.style.background = "#fff";
            pendingPort.style.borderColor = "#adb5bd";
            pendingPort = null;
            return;
        }
        if (port.dataset.side !== "input") return;

        const existing = conns.findIndex(c => c.from === pendingPort && c.to === port);
        if (existing !== -1) {
            conns[existing].line.remove();
            conns.splice(existing, 1);
            pendingPort.style.background = "#fff";
            pendingPort.style.borderColor = "#adb5bd";
            pendingPort = null;
            revalidate();
            return;
        }

        connectPorts(pendingPort!, port);

        pendingPort.style.background = "#fff";
        pendingPort.style.borderColor = "#adb5bd";
        pendingPort = null;
    }

    function makeCard(
        ports: HTMLElement[],
        onRemoveCard: () => void,
        showRemoveBtn = true,
    ): { card: HTMLDivElement; body: HTMLDivElement } {
        const card = document.createElement("div");
        card.className = "card mb-3";
        Object.assign(card.style, { position: "relative", overflow: "visible" });

        const body = document.createElement("div");
        body.className = "card-body py-2 px-3 d-flex align-items-center";

        if (showRemoveBtn) {
            const rm = document.createElement("button");
            rm.className = "btn btn-sm btn-link text-danger p-0 ms-auto";
            rm.innerHTML = "&times;";
            rm.addEventListener("click", () => { dropConnsFor(ports); onRemoveCard(); card.remove(); });
            body.appendChild(rm);
        }

        card.append(body, ...ports);
        return { card, body };
    }

    // ── Node builders ─────────────────────────────────────────────────────────

    function makeObserverNode(type: string, objectId: string, onRemoveCard: () => void): HTMLDivElement {
        const getterSel = makeSelect(OBSERVER_GETTERS[type] ?? []);
        const obsPortLabel = (getter: string) => {
            const def = GETTER_DEF[getter];
            const t   = def?.type;
            const k   = def?.output_key;
            if (k && t) return `${getter} → ${k}: ${t}`;
            return t ? `${getter} → ${t}` : getter;
        };
        const initialDef  = GETTER_DEF[getterSel.value];
        const initialType = initialDef?.type ?? "";
        const initialKey  = initialDef?.output_key ?? "";
        const out = makePort("output", initialType, obsPortLabel(getterSel.value), initialKey);
        const nodeId = `obs_${obsCounter++}`;
        const rec: ObserverNodeRec = { id: nodeId, type, objectId, getterEl: getterSel, outPort: out };
        obsNodes.push(rec);
        getterSel.addEventListener("change", () => {
            const def = GETTER_DEF[getterSel.value];
            out.dataset.portType  = def?.type  ?? "";
            out.dataset.portKey   = def?.output_key ?? "";
            out.dataset.portLabel = obsPortLabel(getterSel.value);
            revalidate();
        });
        const { card, body } = makeCard([out], () => {
            obsNodes.splice(obsNodes.indexOf(rec), 1);
            onRemoveCard();
        });
        body.classList.remove("d-flex", "align-items-center");
        body.classList.add("d-block");
        const rmBtn = body.lastElementChild as HTMLElement;
        const topRow = document.createElement("div");
        topRow.className = "d-flex align-items-center gap-1 mb-1";
        topRow.append(badge(type), idLabel(objectId), rmBtn);
        body.prepend(topRow);
        getterSel.classList.remove("ms-2");
        getterSel.style.maxWidth = "";
        getterSel.style.width = "100%";
        body.appendChild(getterSel);
        return card;
    }

    function makeBlockNode(
        key: string,
        params: ParamDef[],
        paramValues: Record<string, unknown>,
        onRemoveCard: () => void,
        showRemoveBtn = true,
        inputKey?: string,
        inputType?: string,
        outputKey?: string,
        outputType?: string,
        onParamChange?: () => void,
        showInputPort = true,
    ): { card: HTMLDivElement; inPort: HTMLElement; outPort: HTMLElement } {
        const inpLabel  = inputKey  && inputType  ? `${inputKey}: ${inputType}`   : inputType  ?? inputKey;
        const outLabel  = outputKey && outputType ? `${outputKey}: ${outputType}` : outputType ?? outputKey;
        const inp = makePort("input",  inputType,  inpLabel,  inputKey);
        const out = makePort("output", outputType, outLabel, outputKey);
        const { card, body } = makeCard(showInputPort ? [inp, out] : [out], onRemoveCard, showRemoveBtn);

        if (params.length > 0) {
            body.classList.remove("d-flex", "align-items-center");
            body.classList.add("d-block");

            const topRow = document.createElement("div");
            topRow.className = "d-flex align-items-center mb-2";
            topRow.append(idLabel(key));
            if (body.lastElementChild) topRow.appendChild(body.lastElementChild);
            body.prepend(topRow);

            const grid = document.createElement("div");
            grid.className = "row g-1";

            // Keyed references used for inter-param linking after the loop.
            const inputEls = new Map<string, HTMLInputElement>();
            const selectEls = new Map<string, HTMLSelectElement>();

            for (const p of params) {
                if (p.type === "phase_list") {
                    const rows = (paramValues[p.name] ?? []) as StaticTLSPhaseRow[];
                    paramValues[p.name] = rows;

                    const phaseWrap = document.createElement("div");
                    phaseWrap.className = "col-12 mt-1";
                    const lbl = document.createElement("div");
                    lbl.className = "d-flex gap-1 mb-1";
                    const stateHdr = document.createElement("span");
                    stateHdr.className = "small text-muted flex-grow-1";
                    stateHdr.textContent = "State";
                    const durHdr = document.createElement("span");
                    durHdr.className = "small text-muted";
                    durHdr.style.width = "58px";
                    durHdr.textContent = "Dur";
                    lbl.append(stateHdr, durHdr);
                    const rowsContainer = document.createElement("div");
                    for (const r of rows) {
                        rowsContainer.appendChild(makeStaticPhaseRow(r, rows));
                    }
                    const addRowBtn = document.createElement("button");
                    addRowBtn.type = "button";
                    addRowBtn.className = "btn btn-sm btn-outline-secondary mt-1";
                    addRowBtn.textContent = "+ Add phase";
                    addRowBtn.addEventListener("click", () => {
                        const newRow: StaticTLSPhaseRow = { state: "", duration: 30 };
                        rows.push(newRow);
                        rowsContainer.appendChild(makeStaticPhaseRow(newRow, rows));
                    });
                    phaseWrap.append(lbl, rowsContainer, addRowBtn);
                    grid.appendChild(phaseWrap);
                } else if (p.type === "select") {
                    const col = document.createElement("div");
                    col.className = "col-6";
                    const lbl = document.createElement("label");
                    lbl.className = "form-label mb-0 small text-muted";
                    lbl.textContent = p.label;
                    const sel = document.createElement("select");
                    sel.className = "form-select form-select-sm";
                    const cur = String(paramValues[p.name] ?? p.default);
                    for (const choice of p.choices) {
                        const opt = document.createElement("option");
                        opt.value = opt.textContent = choice;
                        if (choice === cur) opt.selected = true;
                        sel.appendChild(opt);
                    }
                    paramValues[p.name] = cur;
                    sel.addEventListener("change", () => { paramValues[p.name] = sel.value; onParamChange?.(); });
                    selectEls.set(p.name, sel);
                    col.append(lbl, sel);
                    grid.appendChild(col);
                } else if (p.type === "string") {
                    const col = document.createElement("div");
                    col.className = "col-6";
                    const lbl = document.createElement("label");
                    lbl.className = "form-label mb-0 small text-muted";
                    lbl.textContent = p.label;
                    const input = document.createElement("input");
                    input.type = "text";
                    input.className = "form-control form-control-sm";
                    input.value = String(paramValues[p.name] ?? p.default);
                    input.addEventListener("input", () => { paramValues[p.name] = input.value; onParamChange?.(); });
                    inputEls.set(p.name, input);
                    col.append(lbl, input);
                    grid.appendChild(col);
                } else {
                    const col = document.createElement("div");
                    col.className = "col-6";
                    const lbl = document.createElement("label");
                    lbl.className = "form-label mb-0 small text-muted";
                    lbl.textContent = p.label;
                    const input = document.createElement("input");
                    input.type = "number";
                    input.className = "form-control form-control-sm";
                    input.value = String(paramValues[p.name] ?? p.default);
                    input.addEventListener("input", () => {
                        paramValues[p.name] = input.value !== "" ? Number(input.value) : p.default;
                        onParamChange?.();
                    });
                    inputEls.set(p.name, input);
                    col.append(lbl, input);
                    grid.appendChild(col);
                }
            }

            // Link value_type selector → value input type (number vs text).
            const typeSelEl = selectEls.get("value_type");
            const valueInputEl = inputEls.get("value");
            if (typeSelEl && valueInputEl) {
                const syncValueInput = () => {
                    const isNumeric = typeSelEl.value === "float" || typeSelEl.value === "int";
                    valueInputEl.type = isNumeric ? "number" : "text";
                    if (isNumeric) {
                        valueInputEl.step = typeSelEl.value === "int" ? "1" : "any";
                    }
                };
                typeSelEl.addEventListener("change", syncValueInput);
                syncValueInput();
            }

            body.appendChild(grid);
        } else {
            body.prepend(idLabel(key));
        }

        return { card, inPort: inp, outPort: out };
    }

    function makeActuatorNode(type: string, objectId: string, onRemoveCard: () => void): HTMLDivElement {
        const setterSel = makeSelect(ACTUATOR_SETTERS[type] ?? []);
        const actPortLabel = (setter: string) => {
            const si = SETTER_INPUT[setter];
            return si ? `${setter} ← ${si.input_key ?? si.sumo_param}: ${si.type}` : setter;
        };
        const actPortType  = (setter: string) => SETTER_INPUT[setter]?.type;
        const actPortKey   = (setter: string) => SETTER_INPUT[setter]?.input_key;
        const inp = makePort("input", actPortType(setterSel.value), actPortLabel(setterSel.value), actPortKey(setterSel.value));
        const nodeId = `act_${actCounter++}`;
        const rec: ActuatorNodeRec = { id: nodeId, type, objectId, setterEl: setterSel, inPort: inp };
        actNodes.push(rec);
        setterSel.addEventListener("change", () => {
            inp.dataset.portType  = actPortType(setterSel.value) ?? "";
            inp.dataset.portKey   = actPortKey(setterSel.value)  ?? "";
            inp.dataset.portLabel = actPortLabel(setterSel.value);
            revalidate();
        });
        const { card, body } = makeCard([inp], () => {
            actNodes.splice(actNodes.indexOf(rec), 1);
            onRemoveCard();
        });
        body.classList.remove("d-flex", "align-items-center");
        body.classList.add("d-block");
        const rmBtn = body.lastElementChild as HTMLElement;
        const topRow = document.createElement("div");
        topRow.className = "d-flex align-items-center gap-1 mb-1";
        topRow.append(badge(type), idLabel(objectId), rmBtn);
        body.prepend(topRow);
        setterSel.classList.remove("ms-2");
        setterSel.style.maxWidth = "";
        setterSel.style.width = "100%";
        body.appendChild(setterSel);
        return card;
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    function byDomOrder<T>(regs: T[], getPort: (r: T) => HTMLElement): T[] {
        return [...regs].sort((a, b) => {
            const aEl = getPort(a).closest("[data-stage-id]");
            const bEl = getPort(b).closest("[data-stage-id]");
            if (!aEl || !bEl) return 0;
            return aEl.compareDocumentPosition(bEl) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1;
        });
    }

    function serialize(): PipelineSpec {
        const orderedBlocks = byDomOrder(blockRegs, br => br.outPort);
        const orderedSinks  = byDomOrder(sinkRegs,  sr => sr.inPort);

        const outPortToNodeId = new Map<HTMLElement, string>([
            ...obsNodes.map(o => [o.outPort, o.id] as [HTMLElement, string]),
            ...orderedBlocks.map(b => [b.outPort, b.id] as [HTMLElement, string]),
        ]);
        const inPortToActId = new Map<HTMLElement, string>(actNodes.map(a => [a.inPort, a.id]));

        // Build interleaved stage order so hydration can restore the original layout.
        const mixedStages: { type: "block" | "sink"; index: number; el: Element }[] = [
            ...orderedBlocks.map((b, i) => ({ type: "block" as const, index: i, el: b.outPort.closest("[data-stage-id]")! })),
            ...orderedSinks.map((s, i) => ({ type: "sink"  as const, index: i, el: s.inPort.closest("[data-stage-id]")! })),
        ].filter(e => e.el != null);
        mixedStages.sort((a, b) => a.el.compareDocumentPosition(b.el) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1);
        const stage_order = mixedStages.map(({ type, index }) => ({ type, index }));

        return {
            id: reg.id,
            name: reg.name,
            observers: obsNodes.map(o => ({
                id: o.id,
                domain: OBSERVER_DOMAIN[o.type] ?? o.type,
                getter: o.getterEl.value,
                object_id: o.objectId,
            })),
            blocks: orderedBlocks.map(b => {
                const inConns = conns.filter(cn => cn.to   === b.inPort);
                const outConn = conns.find(cn  => cn.from  === b.outPort);
                return {
                    id: b.id,
                    key: b.key,
                    params: { ...b.params },
                    inputs_from: inConns.map(cn => outPortToNodeId.get(cn.from) ?? null).filter((id): id is string => id !== null),
                    actuate_to: outConn ? (inPortToActId.get(outConn.to) ?? null) : null,
                };
            }),
            actuators: actNodes.map(a => ({
                id: a.id,
                domain: ACTUATOR_DOMAIN[a.type] ?? a.type,
                setter: a.setterEl.value,
                object_id: a.objectId,
            })),
            sinks: orderedSinks.map(s => {
                const inConn = conns.find(cn => cn.to === s.inPort);
                return {
                    id: s.id,
                    label: s.labelEl.value,
                    input_from: inConn ? (outPortToNodeId.get(inConn.from) ?? null) : null,
                };
            }),
            stage_order,
        };
    }

    // ── SVG + scrollable stage row ────────────────────────────────────────────

    const wrap = document.createElement("div");
    wrap.style.cssText = "position:relative;";

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.style.cssText = "position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:hidden;z-index:10;";
    svg.innerHTML = `<defs>
        <marker id="${markerId}" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0,8 3,0 6" fill="#6c757d"/>
        </marker>
        <marker id="${errorMarkerId}" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0,8 3,0 6" fill="#dc3545"/>
        </marker>
    </defs>`;
    svgEl = svg;
    wrap.appendChild(svg);

    const row = document.createElement("div");
    row.style.cssText = "display:flex;gap:1rem;overflow-x:auto;padding-bottom:1rem;align-items:flex-start;";
    row.addEventListener("scroll", () => conns.forEach(syncLine));
    wrap.appendChild(row);

    const STAGE_W = "340px";
    const ANCHOR_IDS = new Set(["obs", "act"]);

    function buildStage(
        title: string,
        removable = false,
        onRemoveStage?: () => void,
    ): { stageEl: HTMLDivElement; buttonsRow: HTMLDivElement; body: HTMLDivElement } {
        const stageEl = document.createElement("div");
        stageEl.style.cssText = `flex:0 0 ${STAGE_W};width:${STAGE_W};`;

        const card = document.createElement("div");
        card.className = "card h-100";
        card.style.overflow = "visible";

        const headerEl = document.createElement("div");
        headerEl.className = "card-header fw-semibold d-flex flex-column gap-1 py-2";

        const titleRow = document.createElement("div");
        titleRow.className = "d-flex align-items-center gap-1";

        if (removable) {
            const grip = document.createElement("i");
            grip.className = "bi bi-grip-vertical text-muted";
            grip.style.cursor = "grab";
            grip.addEventListener("mousedown", () => {
                stageEl.draggable = true;
                stageEl.style.opacity = "0.5";
            });
            stageEl.addEventListener("dragstart", e => {
                e.dataTransfer!.setData("stage-id", stageEl.dataset.stageId!);
                e.dataTransfer!.effectAllowed = "move";
            });
            stageEl.addEventListener("dragend", () => {
                stageEl.draggable = false;
                stageEl.style.opacity = "";
                row.querySelectorAll<HTMLElement>("[data-stage-id]").forEach(s => {
                    s.style.borderLeft = s.style.borderRight = "";
                });
            });
            titleRow.appendChild(grip);
        }

        const titleSpan = document.createElement("span");
        titleSpan.textContent = title;
        titleRow.appendChild(titleSpan);

        if (removable) {
            const rmBtn = document.createElement("button");
            rmBtn.className = "btn btn-sm btn-link text-danger p-0 ms-auto";
            rmBtn.innerHTML = "&times;";
            rmBtn.addEventListener("click", () => {
                dropConnsFor(Array.from(stageEl.querySelectorAll<HTMLElement>("[data-side]")));
                onRemoveStage?.();
                stageEl.remove();
                conns.forEach(syncLine);
            });
            titleRow.appendChild(rmBtn);
        }

        headerEl.appendChild(titleRow);

        const buttonsRow = document.createElement("div");
        buttonsRow.style.cssText = "display:grid;grid-template-columns:1fr 1fr;gap:4px;";
        headerEl.appendChild(buttonsRow);

        const body = document.createElement("div");
        body.className = "card-body";

        card.append(headerEl, body);
        stageEl.appendChild(card);

        stageEl.addEventListener("dragover", e => {
            if (!e.dataTransfer?.types.includes("stage-id")) return;
            if (ANCHOR_IDS.has(stageEl.dataset.stageId ?? "")) return;
            e.preventDefault();
            const mid = stageEl.getBoundingClientRect().left + stageEl.getBoundingClientRect().width / 2;
            stageEl.style.borderLeft  = e.clientX <  mid ? "2px solid #0d6efd" : "";
            stageEl.style.borderRight = e.clientX >= mid ? "2px solid #0d6efd" : "";
        });
        stageEl.addEventListener("dragleave", () => {
            stageEl.style.borderLeft = stageEl.style.borderRight = "";
        });
        stageEl.addEventListener("drop", e => {
            e.preventDefault();
            stageEl.style.borderLeft = stageEl.style.borderRight = "";
            if (ANCHOR_IDS.has(stageEl.dataset.stageId ?? "")) return;
            const draggedId = e.dataTransfer!.getData("stage-id");
            const draggedEl = row.querySelector<HTMLElement>(`[data-stage-id="${draggedId}"]`);
            if (!draggedEl || draggedEl === stageEl) return;
            const insertBefore = e.clientX < stageEl.getBoundingClientRect().left + stageEl.getBoundingClientRect().width / 2;
            row.insertBefore(draggedEl, insertBefore ? stageEl : stageEl.nextSibling);
            conns.forEach(syncLine);
        });

        return { stageEl, buttonsRow, body };
    }

    // ── Observer stage ────────────────────────────────────────────────────────

    const obsStage = buildStage("Observers");
    obsStage.stageEl.dataset.stageId = "obs";
    for (const g of [
        { label: "Detectors",      type: "detector", ids: data.detector_ids },
        { label: "Traffic Lights", type: "tls",      ids: data.tls_ids      },
        { label: "Edges",          type: "edge",     ids: data.edge_ids     },
        { label: "Lanes",          type: "lane",     ids: data.lane_ids     },
    ]) {
        if (!g.ids.length) continue;
        obsStage.buttonsRow.appendChild(
            addDropdown(g.label, BTN[g.type] ?? "btn-outline-primary", g.ids, (id, onRemoveCard) => {
                obsStage.body.appendChild(makeObserverNode(g.type, id, onRemoveCard));
                conns.forEach(syncLine);
            })
        );
    }
    row.appendChild(obsStage.stageEl);

    // ── Actuator stage ────────────────────────────────────────────────────────

    const actStage = buildStage("Actuators");
    actStage.stageEl.dataset.stageId = "act";
    for (const g of [{ label: "Traffic Lights", type: "tls", ids: data.tls_ids }]) {
        if (!g.ids.length) continue;
        actStage.buttonsRow.appendChild(
            addDropdown(g.label, BTN[g.type] ?? "btn-outline-primary", g.ids, (id, onRemoveCard) => {
                actStage.body.appendChild(makeActuatorNode(g.type, id, onRemoveCard));
                conns.forEach(syncLine);
            })
        );
    }

    // ── Per-node stage factories ──────────────────────────────────────────────

    function makeBlockStage(key: string, paramValues: Record<string, unknown>): BlockReg {
        const id = `blk_${blkCounter++}`;
        const br: BlockReg = { id, key, params: paramValues, inPort: null!, outPort: null! };
        blockRegs.push(br);

        const bDef = BLOCKS.find(b => b.key === key);
        const label = bDef?.label ?? key;
        const stage = buildStage(label, true, () => {
            blockRegs.splice(blockRegs.indexOf(br), 1);
        });
        stage.stageEl.dataset.stageId = `stage_${stageSeq++}`;

        const { card, inPort, outPort } = makeBlockNode(
            key, bDef?.params ?? [], paramValues, () => {}, false,
            bDef?.input_key, bDef?.input_type, bDef?.output_key, bDef?.output_type,
            revalidate,
            key !== "Constant",
        );
        br.inPort = inPort;
        br.outPort = outPort;
        stage.body.appendChild(card);

        row.insertBefore(stage.stageEl, actStage.stageEl);
        conns.forEach(syncLine);
        return br;
    }

    function makeSinkStage(initialLabel = ""): SinkReg {
        const id = `sink_${sinkCounter++}`;
        const sr: SinkReg = { id, labelEl: null!, inPort: null! };
        sinkRegs.push(sr);

        const stage = buildStage("Sink", true, () => {
            sinkRegs.splice(sinkRegs.indexOf(sr), 1);
        });
        stage.stageEl.dataset.stageId = `stage_${stageSeq++}`;

        const inp = makePort("input", undefined, "recorded value");
        const card = document.createElement("div");
        card.className = "card mb-3";
        Object.assign(card.style, { position: "relative", overflow: "visible" });
        const body = document.createElement("div");
        body.className = "card-body py-2 px-3";
        const labelInput = document.createElement("input");
        labelInput.type = "text";
        labelInput.className = "form-control form-control-sm";
        labelInput.placeholder = "label…";
        labelInput.value = initialLabel;
        body.appendChild(labelInput);
        card.append(body, inp);

        sr.labelEl = labelInput;
        sr.inPort = inp;

        stage.body.appendChild(card);
        row.insertBefore(stage.stageEl, actStage.stageEl);
        conns.forEach(syncLine);
        return sr;
    }

    // ── Add-stage dropdown (moved to pipeline card header below) ─────────────

    const MODULE_LABELS: Record<string, string> = {
        controllers: "Controllers",
        aggregators: "Aggregators",
        utils: "Utilities",
    };

    const stageDropdown = document.createElement("div");
    stageDropdown.className = "dropdown";

    const stageToggle = document.createElement("button");
    stageToggle.type = "button";
    stageToggle.className = "btn btn-sm btn-outline-secondary dropdown-toggle";
    stageToggle.setAttribute("data-bs-toggle", "dropdown");
    stageToggle.innerHTML = `<i class="bi bi-plus-lg me-1"></i>Add Stage`;

    const stageMenu = document.createElement("ul");
    stageMenu.className = "dropdown-menu";
    stageMenu.style.minWidth = "280px";

    // Group blocks by module, preserving registry order within each group.
    const blocksByModule = new Map<string, BlockEntry[]>();
    for (const b of BLOCKS) {
        const mod = b.module ?? "controllers";
        if (!blocksByModule.has(mod)) blocksByModule.set(mod, []);
        blocksByModule.get(mod)!.push(b);
    }

    let firstGroup = true;
    for (const [mod, groupBlocks] of blocksByModule) {
        if (!firstGroup) {
            const divLi = document.createElement("li");
            divLi.innerHTML = '<hr class="dropdown-divider my-1">';
            stageMenu.appendChild(divLi);
        }
        firstGroup = false;

        const hdrLi = document.createElement("li");
        hdrLi.innerHTML = `<h6 class="dropdown-header">${MODULE_LABELS[mod] ?? mod}</h6>`;
        stageMenu.appendChild(hdrLi);

        for (const b of groupBlocks) {
            const li = document.createElement("li");
            const a = document.createElement("a");
            a.className = "dropdown-item py-2";
            a.href = "#";

            const labelEl = document.createElement("div");
            labelEl.className = "fw-semibold small";
            labelEl.textContent = b.label;
            a.appendChild(labelEl);

            if (b.description) {
                const descEl = document.createElement("div");
                descEl.className = "text-muted lh-sm";
                descEl.style.fontSize = "0.72rem";
                const dotIdx = b.description.indexOf(".");
                descEl.textContent = dotIdx !== -1 ? b.description.slice(0, dotIdx + 1) : b.description;
                descEl.title = b.description;
                a.appendChild(descEl);
            }

            a.addEventListener("click", (e) => {
                e.preventDefault();
                const pv: Record<string, unknown> = {};
                for (const p of b.params ?? []) {
                    if (p.type === "phase_list") pv[p.name] = [];
                    else pv[p.name] = p.default;
                }
                makeBlockStage(b.key, pv);
            });
            li.appendChild(a);
            stageMenu.appendChild(li);
        }
    }

    const stageSinkDivLi = document.createElement("li");
    stageSinkDivLi.innerHTML = '<hr class="dropdown-divider">';
    stageMenu.appendChild(stageSinkDivLi);

    const stageSinkHdr = document.createElement("li");
    stageSinkHdr.innerHTML = '<h6 class="dropdown-header">Sinks</h6>';
    stageMenu.appendChild(stageSinkHdr);

    const stageSinkLi = document.createElement("li");
    const stageSinkA = document.createElement("a");
    stageSinkA.className = "dropdown-item";
    stageSinkA.href = "#";
    stageSinkA.textContent = "Sink";
    stageSinkA.addEventListener("click", (e) => { e.preventDefault(); makeSinkStage(); });
    stageSinkLi.appendChild(stageSinkA);
    stageMenu.appendChild(stageSinkLi);

    stageDropdown.append(stageToggle, stageMenu);
    stageDropdown.style.setProperty("--bs-dropdown-zindex", "2000");
    if ((window as any).bootstrap?.Dropdown) {
        new (window as any).bootstrap.Dropdown(stageToggle, {
            popperConfig(d: any) { return { ...d, strategy: "fixed" }; },
        });
    }

    row.append(actStage.stageEl);

    // ── Hydration or empty seed ───────────────────────────────────────────────

    if (initialPipeline) {
        const blocks = initialPipeline.blocks ?? [];
        const sinks  = initialPipeline.sinks  ?? [];
        if (initialPipeline.stage_order) {
            for (const { type, index } of initialPipeline.stage_order) {
                if (type === "block" && blocks[index]) makeBlockStage(blocks[index].key, { ...blocks[index].params });
                else if (type === "sink"  && sinks[index])  makeSinkStage(sinks[index].label);
            }
        } else {
            for (const bs of blocks) makeBlockStage(bs.key, { ...bs.params });
            for (const sk of sinks)  makeSinkStage(sk.label);
        }
        for (const obs of initialPipeline.observers) {
            const type = OBSERVER_TYPE_FROM_DOMAIN[obs.domain] ?? obs.domain;
            const card = makeObserverNode(type, obs.object_id, () => {});
            const rec = obsNodes[obsNodes.length - 1];
            rec.getterEl.value = obs.getter;
            rec.getterEl.dispatchEvent(new Event("change"));
            if (missingObjectIds.has(`${obs.domain}:${obs.object_id}`)) {
                card.classList.add("border-danger");
                card.title = `"${obs.object_id}" not found in the selected scenario`;
                card.querySelector<HTMLElement>(".font-monospace.small")?.classList.add("text-danger");
            }
            obsStage.body.appendChild(card);
        }
        for (const act of initialPipeline.actuators) {
            const type = ACTUATOR_TYPE_FROM_DOMAIN[act.domain] ?? act.domain;
            const card = makeActuatorNode(type, act.object_id, () => {});
            const rec = actNodes[actNodes.length - 1];
            rec.setterEl.value = act.setter;
            rec.setterEl.dispatchEvent(new Event("change"));
            if (missingObjectIds.has(`${act.domain}:${act.object_id}`)) {
                card.classList.add("border-danger");
                card.title = `"${act.object_id}" not found in the selected scenario`;
                card.querySelector<HTMLElement>(".font-monospace.small")?.classList.add("text-danger");
            }
            actStage.body.appendChild(card);
        }
    }

    function drawConnections(): void {
        if (!initialPipeline) return;

        const outPortById = new Map<string, HTMLElement>([
            ...initialPipeline.observers
                .map((spec, i) => [spec.id, obsNodes[i]?.outPort] as [string, HTMLElement | undefined])
                .filter((e): e is [string, HTMLElement] => !!e[1]),
            ...(initialPipeline.blocks ?? [])
                .map((spec, i) => [spec.id, blockRegs[i]?.outPort] as [string, HTMLElement | undefined])
                .filter((e): e is [string, HTMLElement] => !!e[1]),
        ]);
        const actPortById = new Map<string, HTMLElement>(
            initialPipeline.actuators
                .map((spec, i) => [spec.id, actNodes[i]?.inPort] as [string, HTMLElement | undefined])
                .filter((e): e is [string, HTMLElement] => !!e[1])
        );

        (initialPipeline.blocks ?? []).forEach((spec, i) => {
            const br = blockRegs[i];
            if (!br?.inPort || !br?.outPort) return;
            for (const fromId of (spec.inputs_from ?? [])) {
                const from = outPortById.get(fromId);
                if (from) connectPorts(from, br.inPort);
            }
            if (spec.actuate_to) {
                const to = actPortById.get(spec.actuate_to);
                if (to) connectPorts(br.outPort, to);
            }
        });

        (initialPipeline.sinks ?? []).forEach((spec, i) => {
            const sr = sinkRegs[i];
            if (!sr?.inPort) return;
            if (spec.input_from) {
                const from = outPortById.get(spec.input_from);
                if (from) connectPorts(from, sr.inPort);
            }
        });
    }

    // ── Outer pipeline card ───────────────────────────────────────────────────

    const panelEl = document.createElement("div");
    panelEl.className = "card mb-3";
    panelEl.style.overflow = "visible";
    panelEl.dataset.pipelineId = reg.id;

    const panelHeader = document.createElement("div");
    panelHeader.className = "card-header d-flex align-items-center py-2";

    const panelTitle = document.createElement("span");
    panelTitle.className = "fw-semibold";
    panelTitle.textContent = reg.name;

    const rmBtn = document.createElement("button");
    rmBtn.className = "btn btn-sm btn-link text-danger p-0";
    rmBtn.innerHTML = "&times;";
    rmBtn.addEventListener("click", () => { onRemove(); panelEl.remove(); });

    stageDropdown.classList.add("ms-auto", "me-2");
    panelHeader.append(panelTitle, stageDropdown, rmBtn);

    const panelBody = document.createElement("div");
    panelBody.className = "card-body p-2";
    panelBody.appendChild(wrap);

    panelEl.append(panelHeader, panelBody);
    return { el: panelEl, serialize, drawConnections };
}

// ─── Missing ID helpers ───────────────────────────────────────────────────────

function computeMissingIds(graph: GraphSpec, data: InspectionData): Set<string> {
    const domainIds: Record<string, Set<string>> = {
        inductionloop: new Set(data.detector_ids),
        trafficlight:  new Set(data.tls_ids),
        edge:          new Set(data.edge_ids),
        lane:          new Set(data.lane_ids),
    };
    const missing = new Set<string>();
    for (const pl of graph.pipelines) {
        for (const obs of pl.observers) {
            if (!(domainIds[obs.domain] ?? new Set()).has(obs.object_id))
                missing.add(`${obs.domain}:${obs.object_id}`);
        }
        for (const act of pl.actuators) {
            if (!(domainIds[act.domain] ?? new Set()).has(act.object_id))
                missing.add(`${act.domain}:${act.object_id}`);
        }
    }
    return missing;
}

// ─── Main renderer ────────────────────────────────────────────────────────────

function renderPipeline(
    container: HTMLElement,
    data: InspectionData,
    scenarioId: string,
    initialGraph: GraphSpec | null = null,
    missingObjectIds: Set<string> = new Set(),
): { draw: () => void; getSpec: () => GraphSpec | null } {
    container.innerHTML = "";

    // ── Validation banner ─────────────────────────────────────────────────────

    const validationBanner = document.createElement("div");
    validationBanner.className = "alert alert-danger py-2 mb-3 d-none";
    container.appendChild(validationBanner);

    if (missingObjectIds.size > 0) {
        const missingBanner = document.createElement("div");
        missingBanner.className = "alert alert-warning py-2 mb-3";
        missingBanner.textContent =
            `${missingObjectIds.size} observer/actuator ID(s) were not found in the selected scenario and are highlighted in red.`;
        container.appendChild(missingBanner);
    }

    const pipelineErrors = new Map<string, string[]>();

    function updateValidationBanner(): void {
        const all = Array.from(pipelineErrors.values()).flat();
        if (all.length === 0) {
            validationBanner.classList.add("d-none");
        } else {
            validationBanner.classList.remove("d-none");
            validationBanner.innerHTML =
                `<strong>Type mismatches:</strong><ul class="mb-0 mt-1">${
                    all.map(e => `<li class="font-monospace small">${e}</li>`).join("")
                }</ul>`;
        }
    }

    const pipelineRegs: PipelineReg[] = [];
    interface PipelineEntry { reg: PipelineReg; serialize: () => PipelineSpec; }
    const pipelineEntries: PipelineEntry[] = [];
    const allPhases: Phase[] = [];
    let pipelineCounter = 0;
    let phaseList!: HTMLDivElement;
    const pendingDraws: (() => void)[] = [];

    // ── Pipeline section ──────────────────────────────────────────────────────

    const pipelinesSection = document.createElement("div");
    pipelinesSection.className = "mb-4";

    const pipelinesHeaderRow = document.createElement("div");
    pipelinesHeaderRow.className = "d-flex align-items-center gap-2 mb-2";

    const pipelinesTitle = document.createElement("h6");
    pipelinesTitle.className = "mb-0";
    pipelinesTitle.textContent = "Pipelines";

    const addPipelineBtn = document.createElement("button");
    addPipelineBtn.className = "btn btn-sm btn-outline-secondary";
    addPipelineBtn.textContent = "+ Add Pipeline";

    pipelinesHeaderRow.append(pipelinesTitle, addPipelineBtn);

    const pipelinesList = document.createElement("div");

    function addPipeline(spec: PipelineSpec | null = null): void {
        pipelineCounter++;
        const id = spec?.id ?? `pipeline_${pipelineCounter}`;
        const name = spec?.name ?? `Pipeline ${pipelineCounter}`;
        const reg: PipelineReg = { id, name };
        pipelineRegs.push(reg);

        const { el, serialize, drawConnections } = createPipelineRow(data, reg, () => {
            pipelineRegs.splice(pipelineRegs.indexOf(reg), 1);
            pipelineEntries.splice(pipelineEntries.findIndex(e => e.reg === reg), 1);
            allPhases.forEach(p => p.active_pipelines.delete(id));
            phaseList?.querySelectorAll<HTMLElement>(`[data-pipeline-id="${id}"]`).forEach(el => el.remove());
            refreshNoPipelineNotices();
            pipelineErrors.delete(id);
            updateValidationBanner();
        }, (errors) => {
            if (errors.length === 0) pipelineErrors.delete(id);
            else pipelineErrors.set(id, errors.map(e => `${name}: ${e}`));
            updateValidationBanner();
        }, spec, missingObjectIds);
        pipelineEntries.push({ reg, serialize });
        pipelinesList.appendChild(el);
        if (spec) pendingDraws.push(drawConnections);

        phaseList?.querySelectorAll<HTMLElement>("[data-pipeline-list]").forEach(listEl => {
            const phase = allPhases.find(p => p.id === listEl.dataset.pipelineList);
            if (!phase) return;
            const notice = listEl.querySelector<HTMLElement>(".no-pipelines-notice");
            if (notice) notice.style.display = "none";
            listEl.insertBefore(makePipelineCheckbox(phase, reg), listEl.lastElementChild);
        });
    }

    function refreshNoPipelineNotices(): void {
        phaseList?.querySelectorAll<HTMLElement>(".no-pipelines-notice").forEach(el => {
            el.style.display = pipelineRegs.length ? "none" : "";
        });
    }

    addPipelineBtn.addEventListener("click", () => addPipeline());
    pipelinesSection.append(pipelinesHeaderRow, pipelinesList);

    // ── Phase section ─────────────────────────────────────────────────────────

    function makePipelineCheckbox(phase: Phase, pr: PipelineReg): HTMLLabelElement {
        const lbl = document.createElement("label");
        lbl.className = "d-flex align-items-center gap-1 small";
        lbl.dataset.pipelineId = pr.id;

        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.className = "form-check-input mt-0";
        cb.checked = phase.active_pipelines.has(pr.id);
        cb.addEventListener("change", () => {
            if (cb.checked) phase.active_pipelines.add(pr.id);
            else phase.active_pipelines.delete(pr.id);
        });

        lbl.append(cb, document.createTextNode(pr.name));
        return lbl;
    }

    function makePhaseCard(phase: Phase): HTMLDivElement {
        const card = document.createElement("div");
        card.className = "card";
        card.dataset.phaseId = phase.id;

        card.addEventListener("dragover", (e) => {
            e.preventDefault();
            const mid = card.getBoundingClientRect().top + card.getBoundingClientRect().height / 2;
            card.style.borderTop    = e.clientY <  mid ? "2px solid #0d6efd" : "";
            card.style.borderBottom = e.clientY >= mid ? "2px solid #0d6efd" : "";
        });
        card.addEventListener("dragleave", () => { card.style.borderTop = card.style.borderBottom = ""; });
        card.addEventListener("drop", (e) => {
            e.preventDefault();
            card.style.borderTop = card.style.borderBottom = "";
            const draggedId = e.dataTransfer!.getData("text/plain");
            if (draggedId === phase.id) return;
            const draggedCard = phaseList.querySelector<HTMLElement>(`[data-phase-id="${draggedId}"]`);
            const draggedPhase = allPhases.find(p => p.id === draggedId);
            if (!draggedCard || !draggedPhase) return;
            const insertBefore = e.clientY < card.getBoundingClientRect().top + card.getBoundingClientRect().height / 2;
            allPhases.splice(allPhases.indexOf(draggedPhase), 1);
            const targetIdx = allPhases.indexOf(phase);
            allPhases.splice(insertBefore ? targetIdx : targetIdx + 1, 0, draggedPhase);
            phaseList.insertBefore(draggedCard, insertBefore ? card : card.nextSibling);
            renumberPhases(phaseList);
        });
        card.addEventListener("dragend", () => { card.draggable = false; card.style.opacity = ""; });

        const header = document.createElement("div");
        header.className = "card-header d-flex align-items-center py-2 gap-2";

        const grip = document.createElement("i");
        grip.className = "bi bi-grip-vertical text-muted";
        grip.style.cursor = "grab";
        grip.addEventListener("mousedown", () => { card.draggable = true; card.style.opacity = "0.5"; });
        card.addEventListener("dragstart", (e) => {
            e.dataTransfer!.effectAllowed = "move";
            e.dataTransfer!.setData("text/plain", phase.id);
        });

        const label = document.createElement("span");
        label.className = "fw-bold small phase-label";

        const rmBtn = document.createElement("button");
        rmBtn.className = "btn btn-sm btn-link text-danger p-0 ms-auto";
        rmBtn.innerHTML = "&times;";
        rmBtn.addEventListener("click", () => {
            allPhases.splice(allPhases.indexOf(phase), 1);
            card.remove();
            renumberPhases(phaseList);
        });

        header.append(grip, label, rmBtn);

        const body = document.createElement("div");
        body.className = "card-body py-2 px-3";

        // Duration
        const durRow = document.createElement("div");
        durRow.className = "d-flex align-items-center gap-2 mb-3";
        const durLabel = document.createElement("span");
        durLabel.className = "small text-muted";
        durLabel.textContent = "Duration";
        const durInput = document.createElement("input");
        durInput.type = "number";
        durInput.className = "form-control form-control-sm";
        durInput.style.width = "80px";
        durInput.min = "0";
        durInput.placeholder = "0";
        if (phase.duration_s !== null) durInput.value = String(phase.duration_s);
        durInput.addEventListener("input", () => {
            phase.duration_s = durInput.value ? Number(durInput.value) : null;
        });
        const durUnit = document.createElement("span");
        durUnit.className = "small text-muted";
        durUnit.textContent = "s";
        durRow.append(durLabel, durInput, durUnit);

        // Active Pipelines
        const pipelineHeading = document.createElement("div");
        pipelineHeading.className = "small fw-semibold mb-1";
        pipelineHeading.textContent = "Active Pipelines";

        const pipelineListEl = document.createElement("div");
        pipelineListEl.className = "d-flex flex-column gap-1 mb-3";
        pipelineListEl.dataset.pipelineList = phase.id;
        for (const pr of pipelineRegs) {
            pipelineListEl.appendChild(makePipelineCheckbox(phase, pr));
        }
        const noPipelinesNotice = document.createElement("span");
        noPipelinesNotice.className = "small text-muted fst-italic no-pipelines-notice";
        noPipelinesNotice.textContent = "No pipelines added yet";
        noPipelinesNotice.style.display = pipelineRegs.length ? "none" : "";
        pipelineListEl.appendChild(noPipelinesNotice);

        // On Enter
        const enterHeading = document.createElement("div");
        enterHeading.className = "small fw-semibold mb-1";
        enterHeading.textContent = "On Enter";
        const enterList = document.createElement("div");
        for (const action of phase.on_enter) {
            enterList.appendChild(makeActionRow(action, phase, data));
        }
        const addActionBtn = document.createElement("button");
        addActionBtn.className = "btn btn-sm btn-outline-secondary mt-1";
        addActionBtn.textContent = "+ Add action";
        addActionBtn.addEventListener("click", () => {
            const action: OnEnterAction = {
                domain: "trafficlight",
                setter: ENTER_DOMAIN_SETTERS["trafficlight"]?.[0] ?? "",
                object_id: data.tls_ids[0] ?? "",
                params: "{}",
            };
            phase.on_enter.push(action);
            enterList.appendChild(makeActionRow(action, phase, data));
        });

        body.append(durRow, pipelineHeading, pipelineListEl, enterHeading, enterList, addActionBtn);
        card.append(header, body);
        return card;
    }

    function addPhase(): void {
        const phase: Phase = {
            id: `phase_${Date.now()}`,
            duration_s: null,
            active_pipelines: new Set(),
            on_enter: [],
        };
        allPhases.push(phase);
        phaseList.appendChild(makePhaseCard(phase));
        renumberPhases(phaseList);
    }

    const phaseSection = document.createElement("div");
    phaseSection.className = "pt-3 border-top";

    const phaseHeaderRow = document.createElement("div");
    phaseHeaderRow.className = "d-flex align-items-center gap-2 mb-2";
    const phaseTitle = document.createElement("h6");
    phaseTitle.className = "mb-0";
    phaseTitle.textContent = "Phases";
    const addPhaseBtn = document.createElement("button");
    addPhaseBtn.className = "btn btn-sm btn-outline-secondary";
    addPhaseBtn.textContent = "+ Add Phase";
    addPhaseBtn.addEventListener("click", addPhase);
    phaseHeaderRow.append(phaseTitle, addPhaseBtn);

    phaseList = document.createElement("div");
    phaseList.className = "d-flex flex-column gap-2";
    phaseSection.append(phaseHeaderRow, phaseList);

    // ── Save bar ──────────────────────────────────────────────────────────────

    function collectGraph(name: string): { spec: GraphSpec } | { error: string } {
        const parsedPhases: GraphSpec["phases"] = [];
        for (let i = 0; i < allPhases.length; i++) {
            const phase = allPhases[i];
            const actions: GraphSpec["phases"][number]["on_enter"] = [];
            for (const action of phase.on_enter) {
                let params: Record<string, unknown>;
                try {
                    params = JSON.parse(action.params || "{}");
                } catch {
                    return { error: `Phase ${i + 1} on-enter action has invalid JSON: ${action.params}` };
                }
                if (typeof params !== "object" || Array.isArray(params)) {
                    return { error: `Phase ${i + 1} on-enter params must be a JSON object` };
                }
                actions.push({ domain: action.domain, setter: action.setter, object_id: action.object_id, params });
            }
            parsedPhases.push({
                duration_s: phase.duration_s,
                active_pipeline_ids: Array.from(phase.active_pipelines),
                on_enter: actions,
            });
        }
        return {
            spec: {
                name,
                scenario_id: scenarioId,
                pipelines: pipelineEntries.map(e => e.serialize()),
                phases: parsedPhases,
            },
        };
    }

    const saveBar = document.createElement("div");
    saveBar.className = "d-flex align-items-center gap-2 pt-3 border-top mt-4";

    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.className = "form-control form-control-sm";
    nameInput.style.maxWidth = "240px";
    nameInput.placeholder = "Graph name…";

    const saveBtn = document.createElement("button");
    saveBtn.className = "btn btn-primary btn-sm";
    saveBtn.textContent = "Save";

    const saveStatus = document.createElement("span");
    saveStatus.className = "small";

    saveBtn.addEventListener("click", async () => {
        const name = nameInput.value.trim();
        if (!name) {
            saveStatus.className = "text-warning small";
            saveStatus.textContent = "Enter a name first.";
            return;
        }
        const result = collectGraph(name);
        if ("error" in result) {
            saveStatus.className = "text-danger small";
            saveStatus.textContent = result.error;
            return;
        }

        saveBtn.disabled = true;
        saveBtn.innerHTML = `<span class="spinner-border spinner-border-sm"></span>`;
        saveStatus.textContent = "";

        const csrf = document.cookie.split(";")
            .map(c => c.trim())
            .find(c => c.startsWith("csrftoken="))
            ?.split("=")[1] ?? "";

        try {
            const resp = await fetch("/api/experiment_graphs/", {
                method: "POST",
                headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
                body: JSON.stringify(result.spec),
            });
            const body = await resp.json().catch(() => ({}));
            if (resp.ok) {
                saveStatus.className = "text-success small";
                saveStatus.textContent = `Saved — ${body.id}`;
                setTimeout(() => window.location.reload(), 600);
            } else {
                saveStatus.className = "text-danger small";
                saveStatus.textContent = body.error ?? `HTTP ${resp.status}`;
            }
        } catch (err) {
            saveStatus.className = "text-danger small";
            saveStatus.textContent = String(err);
        }

        saveBtn.disabled = false;
        saveBtn.textContent = "Save";
    });

    if (initialGraph) nameInput.value = initialGraph.name;
    saveBar.append(nameInput, saveBtn, saveStatus);

    // ── Assemble & seed ───────────────────────────────────────────────────────

    const outerWrap = document.createElement("div");
    outerWrap.append(pipelinesSection, phaseSection, saveBar);
    container.appendChild(outerWrap);

    if (initialGraph) {
        for (const pipelineSpec of initialGraph.pipelines) {
            addPipeline(pipelineSpec);
        }
        for (let i = 0; i < initialGraph.phases.length; i++) {
            const savedPhase = initialGraph.phases[i];
            const phase: Phase = {
                id: `phase_${Date.now()}_${i}`,
                duration_s: savedPhase.duration_s,
                active_pipelines: new Set(savedPhase.active_pipeline_ids),
                on_enter: savedPhase.on_enter.map(a => ({
                    domain: a.domain,
                    setter: a.setter,
                    object_id: a.object_id,
                    params: JSON.stringify(a.params),
                })),
            };
            allPhases.push(phase);
            phaseList.appendChild(makePhaseCard(phase));
        }
        renumberPhases(phaseList);
    } else {
        addPipeline();
        addPhase();
    }

    return {
        draw: () => pendingDraws.forEach(fn => fn()),
        getSpec: () => {
            const name = nameInput.value.trim() || "__temp__";
            const result = collectGraph(name);
            return "error" in result ? null : result.spec;
        },
    };
}

// ─── Boot ─────────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
    const modalEl = document.getElementById("graphBuilderModal");
    const modalContent = document.getElementById("graphBuilderModalContent");
    if (!modalEl || !modalContent) return;

    const modal = new bootstrap.Modal(modalEl);

    async function openBuilder(url: string): Promise<void> {
        const [html, blocksData, domainsData] = await Promise.all([
            fetch(url).then(r => r.text()),
            fetch("/api/blocks").then(r => r.ok ? r.json() : BLOCKS).catch(() => BLOCKS),
            fetch("/api/domains").then(r => r.ok ? r.json() : null).catch(() => null),
        ]);
        BLOCKS = blocksData as BlockEntry[];
        if (domainsData) applyDomainData(domainsData);
        modalContent!.innerHTML = html;
        modal.show();

        const select = modalContent!.querySelector("#gbScenarioSelect") as HTMLSelectElement | null;
        const builder = modalContent!.querySelector("#gbBuilder") as HTMLElement | null;
        if (!select || !builder) return;
        const b = builder;

        // Holds the serialiser of the currently rendered graph so the scenario-change
        // handler can capture state before re-rendering.
        let getCurrentSpec: (() => GraphSpec | null) | null = null;

        const initScript = modalContent!.querySelector("#gbInitialGraph");
        const initialGraph: GraphSpec | null = initScript
            ? JSON.parse(initScript.textContent ?? "null")
            : null;

        async function loadScenario(scenarioId: string, initial: GraphSpec | null = null): Promise<void> {
            b.innerHTML = `<div class="text-muted py-2">Loading inspection…</div>`;
            const res = await fetch(`/api/scenarios/${scenarioId}/inspection`);
            if (!res.ok) {
                const data = await res.json().catch(() => ({}));
                b.innerHTML = "";
                const alert = document.createElement("div");
                alert.className = "alert alert-warning mb-2";
                alert.textContent = data.error ?? `Error ${res.status}`;
                b.appendChild(alert);
                if (data.net_artefact_sha256 && data.inspect_schema) {
                    const btn = document.createElement("button");
                    btn.className = "btn btn-primary btn-sm";
                    btn.textContent = "Run Inspection";
                    btn.addEventListener("click", async () => {
                        btn.disabled = true;
                        btn.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Creating…`;
                        const csrf = document.cookie.split(";")
                            .map(c => c.trim())
                            .find(c => c.startsWith("csrftoken="))
                            ?.split("=")[1] ?? "";
                        const form = new FormData();
                        form.append("method", "inspect");
                        form.append("schema", JSON.stringify(data.inspect_schema));
                        form.append("inputs", JSON.stringify({ net_xml: data.net_artefact_sha256 }));
                        form.append("simulation_parameters", "{}");
                        const resp = await fetch("/create_transformation_request", {
                            method: "POST",
                            headers: { "X-CSRFToken": csrf },
                            body: form,
                        });
                        if (resp.redirected) {
                            window.location.href = resp.url;
                        } else {
                            btn.disabled = false;
                            btn.textContent = "Run Inspection";
                        }
                    });
                    b.appendChild(btn);
                }
                return;
            }
            const inspectionData = await res.json() as InspectionData;
            const missingIds = initial ? computeMissingIds(initial, inspectionData) : new Set<string>();
            const { draw, getSpec } = renderPipeline(b, inspectionData, scenarioId, initial, missingIds);
            getCurrentSpec = getSpec;
            if (initial) {
                if (modalEl!.classList.contains("show")) {
                    requestAnimationFrame(draw);
                } else {
                    const onShown = () => {
                        modalEl!.removeEventListener("shown.bs.modal", onShown);
                        requestAnimationFrame(draw);
                    };
                    modalEl!.addEventListener("shown.bs.modal", onShown);
                }
            }
        }

        select.addEventListener("change", () => {
            const scenarioId = select.value;
            if (!scenarioId) { b.innerHTML = ""; getCurrentSpec = null; return; }
            loadScenario(scenarioId, getCurrentSpec?.() ?? null);
        });

        if (initialGraph) {
            select.value = initialGraph.scenario_id;
            await loadScenario(initialGraph.scenario_id, initialGraph);
        }
    }

    document.querySelectorAll<HTMLElement>(".open-graph-builder").forEach(trigger => {
        trigger.addEventListener("click", async (e) => {
            e.preventDefault();
            const url = trigger.dataset.url;
            if (!url) return;
            await openBuilder(url);
        });
    });
});
