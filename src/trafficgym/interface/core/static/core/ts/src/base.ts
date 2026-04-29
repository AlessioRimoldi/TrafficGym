// base.ts
import { attachPreviewHandlers } from "./attachPreviewHandlers.js";

declare global {
    interface Window {
        __previewHandlersAttached?: boolean;
    }
}

document.addEventListener("DOMContentLoaded", () => {
    if (window.__previewHandlersAttached) return;

    attachPreviewHandlers(document);
    window.__previewHandlersAttached = true;
});