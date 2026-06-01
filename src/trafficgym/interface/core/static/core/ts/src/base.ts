// base.ts
import { attachPreviewHandlers } from "./attachPreviewHandlers.js";

declare global {
    interface Window {
        __previewHandlersAttached?: boolean;
    }
}

const STATUS_BADGE: Record<string, [string, string]> = {
    COMPLETE:  ["success",   "Success"],
    FAILED:    ["danger",    "Failed"],
    PREPARING: ["primary",   "Preparing"],
    RUNNING:   ["primary",   "Running"],
    PENDING:   ["secondary", "Pending"],
};

const TYPE_PREFIX: Record<string, string> = {
    run_request:            "rr",
    run_execution:          "re",
    transformation_request: "tr",
};

function renderBadge(status: string): string {
    const [cls, label] = STATUS_BADGE[status] ?? ["light", status];
    return `<span class="badge text-bg-${cls}">${label}</span>`;
}

function renderProgress(current: number, total: number): string {
    const pct = total > 0 ? Math.min(100, Math.round((current / total) * 100)) : 100;
    const inner = total > 0 ? `${pct}%` : "";
    const label = total > 0 ? `${current} / ${total}` : `Step ${current}`;
    return `<span class="progress" style="height: 20px; width: 200px; display: inline-flex; vertical-align: middle;">
                <span class="progress-bar progress-bar-striped progress-bar-animated progress-smooth small fw-semibold" style="width: ${pct}%; overflow: hidden; white-space: nowrap;">${inner}</span>
            </span>
            <span class="progress-label text-muted small ms-2">${label}</span>`;
}

function renderProgressDone(status: string): string {
    if (status === "COMPLETE") {
        return `<span class="text-muted small">Simulation complete &mdash; </span>
                <button class="btn btn-sm btn-outline-secondary" onclick="location.reload()">Refresh page</button>`;
    }
    return `<span class="text-muted small">Simulation failed.</span>`;
}

function applyProgressUpdate(el: HTMLElement, current: number, total: number): void {
    const bar = el.querySelector<HTMLElement>(".progress-smooth");
    const label = el.querySelector<HTMLElement>(".progress-label");
    if (bar && label) {
        const pct = total > 0 ? Math.min(100, Math.round((current / total) * 100)) : 100;
        bar.style.width = `${pct}%`;
        bar.textContent = total > 0 ? `${pct}%` : "";
        label.textContent = total > 0 ? `${current} / ${total}` : `Step ${current}`;
    } else {
        el.innerHTML = renderProgress(current, total);
    }
}

function initStatusEvents(): void {
    const es = new EventSource("/api/status-events/");
    window.addEventListener("beforeunload", () => es.close());
    es.onmessage = (e: MessageEvent) => {
        const payload = JSON.parse(e.data) as { type: string; id: string; status?: string; current_step?: number; total_steps?: number };
        const { type, id } = payload;

        if (type === "run_execution_progress") {
            const el = document.querySelector<HTMLElement>(`[data-progress-id="re_${id}"]`);
            if (el) applyProgressUpdate(el, payload.current_step ?? 0, payload.total_steps ?? -1);
            return;
        }

        if (type === "run_request_progress") {
            const el = document.querySelector<HTMLElement>(`[data-progress-id="rr_${id}"]`);
            if (el) applyProgressUpdate(el, payload.current_step ?? 0, payload.total_steps ?? -1);
            return;
        }

        const prefix = TYPE_PREFIX[type];
        if (!prefix) return;
        const el = document.querySelector<HTMLElement>(`[data-status-id="${prefix}_${id}"]`);
        if (el) el.innerHTML = renderBadge(payload.status ?? "");

        if (payload.status === "COMPLETE" || payload.status === "FAILED") {
            const progressPrefix = type === "run_request" ? "rr" : type === "run_execution" ? "re" : null;
            if (progressPrefix) {
                const progressEl = document.querySelector<HTMLElement>(`[data-progress-id="${progressPrefix}_${id}"]`);
                if (progressEl) progressEl.innerHTML = renderProgressDone(payload.status);
            }
        }

        document.dispatchEvent(new CustomEvent("status-update", { detail: payload }));
    };
}

function initCreateRunModal(): void {
    const seedsInput = document.getElementById("seedsInput") as HTMLInputElement | null;
    const seedsError = document.getElementById("seedsError") as HTMLElement | null;
    const rerunCount = document.getElementById("rerunCount") as HTMLInputElement | null;
    const rerunCountHelp = document.getElementById("rerunCountHelp") as HTMLElement | null;
    const submitBtn = document.querySelector<HTMLButtonElement>("#createRunForm button[type='submit']");

    if (!seedsInput || !rerunCount || !rerunCountHelp) return;

    function validateSeeds(): boolean {
        const raw = seedsInput!.value.trim();
        if (!raw) {
            seedsInput!.classList.remove("is-invalid");
            return true;
        }
        const parts = raw.split(",").map(s => s.trim());
        const valid = parts.every(s => /^-?\d+$/.test(s));
        seedsInput!.classList.toggle("is-invalid", !valid);
        if (seedsError) seedsError.textContent = valid ? "" : "Integers only";
        return valid;
    }

    seedsInput.addEventListener("input", () => {
        const valid = validateSeeds();
        if (submitBtn) submitBtn.disabled = !valid;

        const seeds = seedsInput.value.split(",").map(s => s.trim()).filter(s => s !== "");
        if (seeds.length > 0) {
            rerunCount.value = String(seeds.length);
            rerunCount.disabled = true;
            rerunCountHelp.style.display = "";
        } else {
            if (rerunCount.disabled) rerunCount.value = "1";
            rerunCount.disabled = false;
            rerunCountHelp.style.display = "none";
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    if (window.__previewHandlersAttached) return;

    attachPreviewHandlers(document);
    window.__previewHandlersAttached = true;

    initStatusEvents();

    const openBtn = document.getElementById("openCreateRunModal") as HTMLButtonElement | null;
    if (openBtn) {
        openBtn.addEventListener("click", () => {
            fetch(openBtn.dataset.url!)
                .then(r => r.text())
                .then(html => {
                    document.getElementById("createRunModalContent")!.innerHTML = html;
                    const modal = new (window as any).bootstrap.Modal(document.getElementById("createRunModal"));
                    modal.show();
                    initCreateRunModal();
                });
        });
    }
});