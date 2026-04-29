export function attachPreviewHandlers(root: Document | HTMLElement = document) {
    root.addEventListener("click", async (e) => {
        const previewModal = new window.bootstrap.Modal(document.getElementById("previewModal") as HTMLDivElement);

        const target = e.target as HTMLElement;

        // Walk up the DOM to find a matching button
        const btn = target.closest(".preview-btn") as HTMLButtonElement | null;
        if (!btn) return;

        const filepath = btn.dataset.filepath;
        const filename = btn.dataset.filename ?? "Preview";

        if (!filepath) return;

        const res = await fetch(`/media/${filepath}`);

        const titleEl = document.getElementById("previewTitle") as HTMLHeadingElement;
        const container = document.getElementById("previewContent") as HTMLElement;

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
            const url = URL.createObjectURL(blob);

            const img = document.createElement("img");
            img.src = url;
            img.className = "img-fluid";
            img.onload = () => URL.revokeObjectURL(url);

            container.appendChild(img);
        } 
        else if (
            contentType.includes("application/json") ||
            contentType.startsWith("text/")
        ) {
            const text = await res.text();

            const pre = document.createElement("pre");
            pre.className = "mb-0";
            pre.textContent = text.slice(0, 20000);

            container.appendChild(pre);
        } 
        else {
            container.textContent = "Preview not supported for this file type";
        }

        previewModal.show();
    });
}