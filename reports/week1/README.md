# Week 1 submission package

Canonical editable sources are `design_report.md`, `task_answers.md` and `benchmark_report.md` in this directory. The Spark runner generates the benchmark source from a successful full-run JSON; the document builder reads these same sources.

1. [Design report](design_report.pdf): architecture, catalog, common model and engineering choices.
2. [Architecture](architecture.pdf): standalone diagram, also available as `architecture.png`.
3. [Benchmark report](benchmark_report.pdf): measurements and method limits.
4. [Task answers](task_answers.pdf): every Week 1 task and discussion question.
5. [Platform README](../../README.md): setup, execution, testing and output semantics.

Rebuild PDFs with `scripts/build_week1_deliverables.py --metrics <successful-full-run.json>` after source changes and visually inspect pagination. The builder regenerates the benchmark source and copies the selected JSON into `evidence/`; the original report there is historical. Runtime storage is ignored by Git, while the selected packaged JSON preserves the reported samples, plans and configuration.
