export function attachPreviewHandlers(root: Document | HTMLElement = document) {
    const previewModalEl = document.getElementById("previewModal") as HTMLDivElement;
    const previewModal = new window.bootstrap.Modal(previewModalEl);
    const artefactLink = document.getElementById("previewArtefactLink") as HTMLAnchorElement;
    const newTabLink = document.getElementById("previewNewTabLink") as HTMLAnchorElement;
    const previewFooter = document.getElementById("previewFooter") as HTMLElement;
    let currentBlobUrl: string | null = null;

    previewModalEl.addEventListener("hidden.bs.modal", () => {
        if (currentBlobUrl) {
            URL.revokeObjectURL(currentBlobUrl);
            currentBlobUrl = null;
        }
    });

    root.addEventListener("click", async (e) => {

        const target = e.target as HTMLElement;

        // Walk up the DOM to find a matching button
        const btn = target.closest(".preview-btn") as HTMLButtonElement | null;
        if (!btn) return;

        const filepath = btn.dataset.filepath;
        const filename = btn.dataset.filename ?? "Preview";
        const artefactUrl = btn.dataset.artefactUrl ?? null;

        if (!filepath) return;

        const res = await fetch(`/media/${filepath}`);

        const titleEl = document.getElementById("previewTitle") as HTMLHeadingElement;
        const container = document.getElementById("previewContent") as HTMLElement;

        newTabLink.href = `/media/${filepath}`;
        if (artefactUrl) {
            artefactLink.href = artefactUrl;
            artefactLink.style.display = "";
        } else {
            artefactLink.style.display = "none";
        }
        previewFooter.style.display = "";

        titleEl.textContent = filename;
        container.innerHTML = "";

        const contentType = res.headers.get("content-type") || "";

        if (!res.ok) {
            let msg = "Failed to load preview";
            if (res.status === 404) msg = "Could not find artefact content";
            else if (res.status === 415) msg = "File is not previewable";

            container.textContent = msg;
            previewModal.show();
            return;
        }

        if (contentType.startsWith("image/")) {
            const blob = await res.blob();
            currentBlobUrl = URL.createObjectURL(blob);

            const img = document.createElement("img");
            img.src = currentBlobUrl;
            img.className = "img-fluid";

            container.appendChild(img);
        } else if (
            contentType.includes("application/json") ||
            contentType.startsWith("text/")
        ) {
            let text = await res.text();

            if (contentType.includes("application/json")) {
                try {
                    text = JSON.stringify(JSON.parse(text), null, 2);
                } catch { /* leave as-is if parsing fails */ }
            }

            const pre = document.createElement("pre");
            pre.className = "mb-0";
            pre.textContent = text.slice(0, 20000);

            container.appendChild(pre);
        } else {
            container.textContent = "Preview not supported for this file type";
        }

        previewModal.show();
    });
}