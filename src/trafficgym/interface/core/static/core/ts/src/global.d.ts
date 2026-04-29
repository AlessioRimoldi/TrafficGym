import * as bootstrap from "bootstrap";

declare global {
    interface Window {
        bootstrap: typeof bootstrap;
    }
}

export {};