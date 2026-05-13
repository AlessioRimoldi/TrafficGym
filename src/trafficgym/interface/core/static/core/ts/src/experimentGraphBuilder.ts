interface InspectionData {
    tls_ids: string[];
    edge_ids: string[];
    lane_ids: string[];
    detector_ids: string[];
}

// ─── Domain definitions ───────────────────────────────────────────────────────

const OBSERVER_GETTERS: Record<string, string[]> = {
    detector: ["getLastIntervalOccupancy", "getLastStepOccupancy", "getLastStepVehicleNumber"],
    tls:      ["getRedYellowGreenState", "getSpentDuration", "getPhase"],
    edge:     ["getLastStepVehicleIDs", "getLastStepMeanSpeed", "getLastStepOccupancy"],
    lane:     ["getLastStepOccupancy", "getLastStepVehicleNumber", "getLastStepMeanSpeed"],
};

const ACTUATOR_SETTERS: Record<string, string[]> = {
    tls: ["setProgram", "setRedYellowGreenState", "setPhase"],
};

const OBSERVER_DOMAIN: Record<string, string> = {
    detector: "inductionloop",
    tls:      "trafficlight",
    edge:     "edge",
    lane:     "lane",
};

const ACTUATOR_DOMAIN: Record<string, string> = {
    tls: "trafficlight",
};

const OBSERVER_TYPE_FROM_DOMAIN = Object.fromEntries(Object.entries(OBSERVER_DOMAIN).map(([t, d]) => [d, t]));
const ACTUATOR_TYPE_FROM_DOMAIN = Object.fromEntries(Object.entries(ACTUATOR_DOMAIN).map(([t, d]) => [d, t]));

interface ParamDef {
    name: string;
    label: string;
    default: number;
}

const CONTROLLERS: { key: string; label: string; params?: ParamDef[]; hasPhaseList?: boolean }[] = [
    {
        key: "RampMeterController",
        label: "Ramp Meter",
        params: [
            { name: "off_up",        label: "OFF → QTR",   default: 15 },
            { name: "quarter_up",    label: "QTR → 10TH",  default: 20 },
            { name: "quarter_down",  label: "QTR → OFF",   default: 10 },
            { name: "tenth_up",      label: "10TH → CHK",  default: 30 },
            { name: "tenth_down",    label: "10TH → QTR",  default: 15 },
            { name: "choke_down",    label: "CHK → 10TH",  default: 25 },
        ],
    },
    { key: "StaticTLSController", label: "Static TLS", hasPhaseList: true },
];

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

interface CtrlReg {
    id: string;
    key: string;
    params: Record<string, number>;
    staticPhaseRows?: StaticTLSPhaseRow[];
    inPort?: HTMLElement;
    outPort?: HTMLElement;
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

// ─── Serialized graph spec ─────────────────────────────────────────────────────

interface PipelineSpec {
    id: string;
    name: string;
    observers: { id: string; domain: string; getter: string; object_id: string }[];
    controllers: {
        id: string;
        key: string;
        params: Record<string, number>;
        static_phase_rows: StaticTLSPhaseRow[] | null;
        observe_from: string | null;
        actuate_to: string | null;
    }[];
    actuators: { id: string; domain: string; setter: string; object_id: string }[];
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

interface Conn { from: HTMLElement; to: HTMLElement; line: SVGLineElement }

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

    const btn = document.createElement("button");
    btn.className = `btn btn-sm ${btnClass} dropdown-toggle`;
    btn.setAttribute("data-bs-toggle", "dropdown");
    btn.setAttribute("data-bs-auto-close", "outside");
    btn.textContent = `+ ${label}`;

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
    initialPipeline: PipelineSpec | null = null,
): { el: HTMLDivElement; serialize: () => PipelineSpec; drawConnections: () => void } {
    let pendingPort: HTMLElement | null = null;
    let svgEl!: SVGSVGElement;
    const conns: Conn[] = [];
    const ctrlRegs: CtrlReg[] = [];
    const obsNodes: ObserverNodeRec[] = [];
    const actNodes: ActuatorNodeRec[] = [];
    let ctrlCounter = 0;
    let obsCounter = 0;
    let actCounter = 0;

    const markerId = `arrow_${reg.id}`;

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
    }

    function makePort(side: "input" | "output"): HTMLSpanElement {
        const p = document.createElement("span");
        p.dataset.side = side;
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
        return p;
    }

    function connectPorts(from: HTMLElement, to: HTMLElement): void {
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("stroke", "#6c757d");
        line.setAttribute("stroke-width", "2");
        line.setAttribute("marker-end", `url(#${markerId})`);
        svgEl.appendChild(line);
        const conn: Conn = { from, to, line };
        conns.push(conn);
        syncLine(conn);
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
    ): { card: HTMLDivElement; body: HTMLDivElement } {
        const card = document.createElement("div");
        card.className = "card mb-3";
        Object.assign(card.style, { position: "relative", overflow: "visible" });

        const body = document.createElement("div");
        body.className = "card-body py-2 px-3 d-flex align-items-center";

        const rm = document.createElement("button");
        rm.className = "btn btn-sm btn-link text-danger p-0 ms-auto";
        rm.innerHTML = "&times;";
        rm.addEventListener("click", () => { dropConnsFor(ports); onRemoveCard(); card.remove(); });

        body.appendChild(rm);
        card.append(body, ...ports);
        return { card, body };
    }

    // ── Node builders ─────────────────────────────────────────────────────────

    function makeObserverNode(type: string, objectId: string, onRemoveCard: () => void): HTMLDivElement {
        const out = makePort("output");
        const getterSel = makeSelect(OBSERVER_GETTERS[type] ?? []);
        const nodeId = `obs_${obsCounter++}`;
        const rec: ObserverNodeRec = { id: nodeId, type, objectId, getterEl: getterSel, outPort: out };
        obsNodes.push(rec);
        const { card, body } = makeCard([out], () => {
            obsNodes.splice(obsNodes.indexOf(rec), 1);
            onRemoveCard();
        });
        body.prepend(badge(type), idLabel(objectId), getterSel);
        return card;
    }

    function makeControllerNode(
        key: string,
        params: ParamDef[],
        paramValues: Record<string, number>,
        staticPhaseRows: StaticTLSPhaseRow[] | null,
        onRemoveCard: () => void,
    ): { card: HTMLDivElement; inPort: HTMLElement; outPort: HTMLElement } {
        const inp = makePort("input");
        const out = makePort("output");
        const { card, body } = makeCard([inp, out], onRemoveCard);

        if (params.length > 0 || staticPhaseRows !== null) {
            body.classList.remove("d-flex", "align-items-center");
            body.classList.add("d-block");

            const topRow = document.createElement("div");
            topRow.className = "d-flex align-items-center mb-2";
            topRow.append(idLabel(key));
            topRow.appendChild(body.lastElementChild!);
            body.prepend(topRow);

            if (params.length > 0) {
                const grid = document.createElement("div");
                grid.className = "row g-1";
                for (const p of params) {
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
                    });
                    col.append(lbl, input);
                    grid.appendChild(col);
                }
                body.appendChild(grid);
            }

            if (staticPhaseRows !== null) {
                const colHeaders = document.createElement("div");
                colHeaders.className = "d-flex gap-1 mb-1";
                const stateHdr = document.createElement("span");
                stateHdr.className = "small text-muted flex-grow-1";
                stateHdr.textContent = "State";
                const durHdr = document.createElement("span");
                durHdr.className = "small text-muted";
                durHdr.style.width = "58px";
                durHdr.textContent = "Dur";
                colHeaders.append(stateHdr, durHdr);
                body.appendChild(colHeaders);

                const rowsContainer = document.createElement("div");
                for (const r of staticPhaseRows) {
                    rowsContainer.appendChild(makeStaticPhaseRow(r, staticPhaseRows));
                }
                body.appendChild(rowsContainer);

                const addRowBtn = document.createElement("button");
                addRowBtn.type = "button";
                addRowBtn.className = "btn btn-sm btn-outline-secondary mt-1";
                addRowBtn.textContent = "+ Add phase";
                addRowBtn.addEventListener("click", () => {
                    const newRow: StaticTLSPhaseRow = { state: "", duration: 30 };
                    staticPhaseRows.push(newRow);
                    rowsContainer.appendChild(makeStaticPhaseRow(newRow, staticPhaseRows));
                });
                body.appendChild(addRowBtn);
            }
        } else {
            body.prepend(idLabel(key));
        }

        return { card, inPort: inp, outPort: out };
    }

    function makeActuatorNode(type: string, objectId: string, onRemoveCard: () => void): HTMLDivElement {
        const inp = makePort("input");
        const setterSel = makeSelect(ACTUATOR_SETTERS[type] ?? []);
        const nodeId = `act_${actCounter++}`;
        const rec: ActuatorNodeRec = { id: nodeId, type, objectId, setterEl: setterSel, inPort: inp };
        actNodes.push(rec);
        const { card, body } = makeCard([inp], () => {
            actNodes.splice(actNodes.indexOf(rec), 1);
            onRemoveCard();
        });
        body.prepend(badge(type), idLabel(objectId), setterSel);
        return card;
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    function serialize(): PipelineSpec {
        const outPortToObsId = new Map<HTMLElement, string>(obsNodes.map(o => [o.outPort, o.id]));
        const inPortToActId = new Map<HTMLElement, string>(actNodes.map(a => [a.inPort, a.id]));

        return {
            id: reg.id,
            name: reg.name,
            observers: obsNodes.map(o => ({
                id: o.id,
                domain: OBSERVER_DOMAIN[o.type] ?? o.type,
                getter: o.getterEl.value,
                object_id: o.objectId,
            })),
            controllers: ctrlRegs.map(c => {
                const inConn  = conns.find(cn => cn.to   === c.inPort);
                const outConn = conns.find(cn => cn.from === c.outPort);
                return {
                    id: c.id,
                    key: c.key,
                    params: { ...c.params },
                    static_phase_rows: c.staticPhaseRows ? [...c.staticPhaseRows] : null,
                    observe_from: inConn  ? (outPortToObsId.get(inConn.from)  ?? null) : null,
                    actuate_to:   outConn ? (inPortToActId.get(outConn.to)    ?? null) : null,
                };
            }),
            actuators: actNodes.map(a => ({
                id: a.id,
                domain: ACTUATOR_DOMAIN[a.type] ?? a.type,
                setter: a.setterEl.value,
                object_id: a.objectId,
            })),
        };
    }

    // ── SVG + scrollable stage row ────────────────────────────────────────────

    const wrap = document.createElement("div");
    wrap.style.cssText = "position:relative;";

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.style.cssText = "position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible;z-index:10;";
    svg.innerHTML = `<defs>
        <marker id="${markerId}" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0,8 3,0 6" fill="#6c757d"/>
        </marker>
    </defs>`;
    svgEl = svg;
    wrap.appendChild(svg);

    const row = document.createElement("div");
    row.style.cssText = "display:flex;gap:1rem;overflow-x:auto;padding-bottom:1rem;align-items:flex-start;";
    row.addEventListener("scroll", () => conns.forEach(syncLine));
    wrap.appendChild(row);

    const STAGE_W = "270px";
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
        buttonsRow.className = "d-flex gap-1 flex-wrap";
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

    // ── Controller stage factory ──────────────────────────────────────────────

    function addControllerStage(): { addController: (key: string, paramValues: Record<string, number>, staticPhaseRows: StaticTLSPhaseRow[] | null) => CtrlReg } {
        const stageCtrls: CtrlReg[] = [];

        const stage = buildStage("Controllers", true, () => {
            for (const cr of stageCtrls) {
                ctrlRegs.splice(ctrlRegs.indexOf(cr), 1);
            }
        });
        stage.stageEl.dataset.stageId = `stage_${Date.now()}`;

        function addControllerToStage(key: string, paramValues: Record<string, number>, staticPhaseRows: StaticTLSPhaseRow[] | null): CtrlReg {
            const id = `ctrl_${ctrlCounter++}`;
            const cr: CtrlReg = { id, key, params: paramValues, staticPhaseRows: staticPhaseRows ?? undefined };
            ctrlRegs.push(cr);
            stageCtrls.push(cr);
            const cDef = CONTROLLERS.find(c => c.key === key);
            const { card, inPort, outPort } = makeControllerNode(
                key, cDef?.params ?? [], paramValues, staticPhaseRows,
                () => {
                    ctrlRegs.splice(ctrlRegs.indexOf(cr), 1);
                    stageCtrls.splice(stageCtrls.indexOf(cr), 1);
                },
            );
            cr.inPort = inPort;
            cr.outPort = outPort;
            stage.body.appendChild(card);
            conns.forEach(syncLine);
            return cr;
        }

        for (const c of CONTROLLERS) {
            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-outline-dark";
            btn.textContent = `+ ${c.label}`;
            btn.addEventListener("click", () => {
                const paramValues: Record<string, number> = {};
                for (const p of c.params ?? []) paramValues[p.name] = p.default;
                addControllerToStage(c.key, paramValues, c.hasPhaseList ? [] : null);
            });
            stage.buttonsRow.appendChild(btn);
        }

        row.insertBefore(stage.stageEl, addStageBtnEl);
        conns.forEach(syncLine);
        return { addController: addControllerToStage };
    }

    // ── Add-stage button + actuator at end ────────────────────────────────────

    const addStageBtnEl = document.createElement("div");
    addStageBtnEl.style.cssText = "flex:0 0 auto;display:flex;align-items:flex-start;padding-top:2px;";
    const addStageBtnInner = document.createElement("button");
    addStageBtnInner.className = "btn btn-outline-secondary btn-sm";
    addStageBtnInner.innerHTML = `<i class="bi bi-plus-lg me-1"></i>Stage`;
    addStageBtnInner.addEventListener("click", addControllerStage);
    addStageBtnEl.appendChild(addStageBtnInner);

    row.append(addStageBtnEl, actStage.stageEl);

    // ── Hydration or empty seed ───────────────────────────────────────────────

    const { addController } = addControllerStage();
    if (initialPipeline) {
        for (const cs of initialPipeline.controllers) {
            addController(cs.key, { ...cs.params }, cs.static_phase_rows ? [...cs.static_phase_rows] : null);
        }
        for (const obs of initialPipeline.observers) {
            const type = OBSERVER_TYPE_FROM_DOMAIN[obs.domain] ?? obs.domain;
            const card = makeObserverNode(type, obs.object_id, () => {});
            obsNodes[obsNodes.length - 1].getterEl.value = obs.getter;
            obsStage.body.appendChild(card);
        }
        for (const act of initialPipeline.actuators) {
            const type = ACTUATOR_TYPE_FROM_DOMAIN[act.domain] ?? act.domain;
            const card = makeActuatorNode(type, act.object_id, () => {});
            actNodes[actNodes.length - 1].setterEl.value = act.setter;
            actStage.body.appendChild(card);
        }
    }

    function drawConnections(): void {
        if (!initialPipeline) return;
        const obsPortById = new Map<string, HTMLElement>(
            initialPipeline.observers
                .map((spec, i) => [spec.id, obsNodes[i]?.outPort] as [string, HTMLElement | undefined])
                .filter((e): e is [string, HTMLElement] => !!e[1])
        );
        const actPortById = new Map<string, HTMLElement>(
            initialPipeline.actuators
                .map((spec, i) => [spec.id, actNodes[i]?.inPort] as [string, HTMLElement | undefined])
                .filter((e): e is [string, HTMLElement] => !!e[1])
        );
        initialPipeline.controllers.forEach((spec, i) => {
            const cr = ctrlRegs[i];
            if (!cr?.inPort || !cr?.outPort) return;
            if (spec.observe_from) {
                const from = obsPortById.get(spec.observe_from);
                if (from) connectPorts(from, cr.inPort);
            }
            if (spec.actuate_to) {
                const to = actPortById.get(spec.actuate_to);
                if (to) connectPorts(cr.outPort, to);
            }
        });
    }

    // ── Outer pipeline card ───────────────────────────────────────────────────

    const panelEl = document.createElement("div");
    panelEl.className = "card mb-3";
    panelEl.dataset.pipelineId = reg.id;

    const panelHeader = document.createElement("div");
    panelHeader.className = "card-header d-flex align-items-center py-2";

    const panelTitle = document.createElement("span");
    panelTitle.className = "fw-semibold";
    panelTitle.textContent = reg.name;

    const rmBtn = document.createElement("button");
    rmBtn.className = "btn btn-sm btn-link text-danger p-0 ms-auto";
    rmBtn.innerHTML = "&times;";
    rmBtn.addEventListener("click", () => { onRemove(); panelEl.remove(); });

    panelHeader.append(panelTitle, rmBtn);

    const panelBody = document.createElement("div");
    panelBody.className = "card-body p-2";
    panelBody.appendChild(wrap);

    panelEl.append(panelHeader, panelBody);
    return { el: panelEl, serialize, drawConnections };
}

// ─── Main renderer ────────────────────────────────────────────────────────────

function renderPipeline(container: HTMLElement, data: InspectionData, scenarioId: string, initialGraph: GraphSpec | null = null): () => void {
    container.innerHTML = "";

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
        }, spec);
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
        durInput.placeholder = "∞";
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

    return () => pendingDraws.forEach(fn => fn());
}

// ─── Boot ─────────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
    const modalEl = document.getElementById("graphBuilderModal");
    const modalContent = document.getElementById("graphBuilderModalContent");
    if (!modalEl || !modalContent) return;

    const modal = new bootstrap.Modal(modalEl);

    async function openBuilder(url: string): Promise<void> {
        const html = await fetch(url).then(r => r.text());
        modalContent!.innerHTML = html;
        modal.show();

        const select = modalContent!.querySelector("#gbScenarioSelect") as HTMLSelectElement | null;
        const builder = modalContent!.querySelector("#gbBuilder") as HTMLElement | null;
        if (!select || !builder) return;
        const b = builder;

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
            const drawAll = renderPipeline(b, await res.json(), scenarioId, initial);
            if (initial) {
                if (modalEl!.classList.contains("show")) {
                    requestAnimationFrame(drawAll);
                } else {
                    const onShown = () => {
                        modalEl!.removeEventListener("shown.bs.modal", onShown);
                        requestAnimationFrame(drawAll);
                    };
                    modalEl!.addEventListener("shown.bs.modal", onShown);
                }
            }
        }

        select.addEventListener("change", () => {
            const scenarioId = select.value;
            if (!scenarioId) { b.innerHTML = ""; return; }
            loadScenario(scenarioId);
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
