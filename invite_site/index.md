---
layout: default
title: Unjam — Traffic Simulation Workbench
---

<!-- ══ HERO ══════════════════════════════════════════════════════════ -->
<section class="hero">
  <div class="container">

    <div>
        <img src="{{ '/assets/images/unjam_white.svg' | relative_url }}" 
             width="300px"
             alt="Unjam Logo"/>
    </div>

    <div class="hero__badge">Invite-only early access</div>

    <p class="hero__tagline">
      A workbench for SUMO traffic simulation.<br>
      Build closed-loop control pipelines, run reproducible multi-seed
      experiments, and compare results in one place.
    </p>

    <div class="hero__actions">
      <a href="mailto:unjam@control.ee.ethz.ch?subject=Unjam%20access%20request"
         class="btn btn-primary">Request Access</a>
    </div>

    <p class="hero__attribution">
      Developed at
      <a href="https://control.ee.ethz.ch" target="_blank" rel="noopener">ETH Zürich, Institute for Automatic Control</a>
      &mdash; Diego Van Overberghe, Carlo Cenedese &amp; Alessio Rimoldi
    </p>

  </div>
</section>

<!-- ══ FEATURES ══════════════════════════════════════════════════════ -->
<section class="features">
  <div class="container">
    <div class="grid">

      <div class="card">
        <div class="card__icon c-green">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <circle cx="6" cy="6" r="2"/><circle cx="18" cy="6" r="2"/>
            <circle cx="12" cy="18" r="2"/>
            <path d="M8 6h8M7.2 7.8l3.6 8.4M16.8 7.8l-3.6 8.4"/>
          </svg>
        </div>
        <h3 class="card__title">Visual Pipeline Builder</h3>
        <p class="card__body">
          Wire observer nodes, controllers, and actuators in a graphical editor.
          The platform generates a Python experiment class automatically — no boilerplate required.
        </p>
      </div>

      <div class="card">
        <div class="card__icon c-blue">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <polyline points="17 1 21 5 17 9"/><path d="M3 11V9a4 4 0 0 1 4-4h14"/>
            <polyline points="7 23 3 19 7 15"/><path d="M21 13v2a4 4 0 0 1-4 4H3"/>
          </svg>
        </div>
        <h3 class="card__title">Reproducible Multi-Seed Runs</h3>
        <p class="card__body">
          Submit any number of seeds in a single run request. Each seed runs
          in parallel as an independent Celery task. Results are content-addressed
          and permanently stored.
        </p>
      </div>

      <div class="card">
        <div class="card__icon c-amber">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/>
            <line x1="6" y1="20" x2="6" y2="14"/>
          </svg>
        </div>
        <h3 class="card__title">Side-by-Side Analytics</h3>
        <p class="card__body">
          Overlay multiple run requests on a single zoomable chart. Aggregate over
          seeds, compare controller strategies, and export data as CSV.
        </p>
      </div>

      <div class="card">
        <div class="card__icon c-teal">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <rect x="4" y="4" width="16" height="16" rx="2"/>
            <path d="M9 9h6M9 12h6M9 15h4"/>
          </svg>
        </div>
        <h3 class="card__title">Built-in Control Blocks</h3>
        <p class="card__body">
          Hysteretic ramp meter, ALINEA-P/PI/PD, rolling average, max aggregator,
          and a static TLS controller ship out of the box. Add your own with a
          single&nbsp;decorator.
        </p>
      </div>

      <div class="card">
        <div class="card__icon c-red">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
            <polyline points="14 2 14 8 20 8"/>
            <rect x="8" y="13" width="8" height="6" rx="1"/>
            <path d="M12 13v-3"/>
          </svg>
        </div>
        <h3 class="card__title">Immutable Artefact Store</h3>
        <p class="card__body">
          Every scenario file and experiment is stored by SHA-256 hash. Runs
          reference exact versions of their inputs, so results remain reproducible&nbsp;indefinitely.
        </p>
      </div>

      <div class="card">
        <div class="card__icon c-purple">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <circle cx="12" cy="12" r="3"/>
            <path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83
                     M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"/>
          </svg>
        </div>
        <h3 class="card__title">Scenario Tooling</h3>
        <p class="card__body">
          Convert OSM exports to SUMO networks, preview the road layout, and run
          any SUMO tool to produce new artefacts — all tracked and accessible from
          the browser.
        </p>
      </div>

    </div>
  </div>
</section>

<!-- ══ AUDIENCE ══════════════════════════════════════════════════════ -->
<section class="audience">
  <div class="container">
    <div class="audience__inner">
      <div>
        <p class="audience__label">Who it's for</p>
        <h2 class="audience__heading">Built for researchers and engineers</h2>
        <p class="audience__body">
          Unjam is designed for anyone who runs SUMO simulations as part of a
          research or engineering workflow — whether you are developing novel
          traffic signal controllers, benchmarking ramp metering strategies, or
          producing reproducible results for a publication.
        </p>
        <div class="audience__tags">
          <span class="tag">Traffic Engineering Researchers</span>
          <span class="tag">Control Systems Engineers</span>
          <span class="tag">PhD &amp; MSc students</span>
          <span class="tag">Transportation Institutes</span>
          <span class="tag">Smart City Labs</span>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ══ CTA ═══════════════════════════════════════════════════════════ -->
<section class="cta">
  <div class="container">
    <p class="cta__eyebrow">Early access</p>
    <h2 class="cta__heading">Request an invitation</h2>
    <p class="cta__body">
      Unjam is currently in a closed early-access phase. Send us a brief
      introduction — your institution, your use case, and what you hope to
      simulate — and we'll be in touch.
    </p>
    <a href="mailto:unjam@control.ee.ethz.ch?subject=Unjam%20access%20request"
       class="btn btn-primary">
      Contact Us
    </a>
    <p class="cta__email">
      or write directly to
      <a href="mailto:unjam@control.ee.ethz.ch">unjam@control.ee.ethz.ch</a>
    </p>
    <p class="cta__note">
      No account required to enquire &mdash; we'll reply within a few working days.
    </p>
  </div>
</section>

<!-- ══ FOOTER ════════════════════════════════════════════════════════ -->
<footer class="footer">
  <div class="container">
    <p>
      &copy; {{ 'now' | date: "%Y" }}
      <a href="https://control.ee.ethz.ch" target="_blank" rel="noopener">
        ETH Zürich, Institute for Automatic Control
      </a>
      &mdash; Unjam is research software, not a commercial product.
    </p>
  </div>
</footer>
