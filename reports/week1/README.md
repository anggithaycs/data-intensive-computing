# Week 1 submission package

This directory contains the editable Markdown sources for the Week 1 submission, existing PDF exports and supporting evidence. Start with the design report for an explanation of the platform, or use the task answers to follow the assignment question by question.

| Document | What it covers |
|---|---|
| [Design report](design_report.md) | The data catalog, storage architecture, common data model and engineering decisions. |
| [Architecture guide](architecture.md) | The pipeline diagram and how data moves through the platform. |
| [Benchmark report](benchmark_report.md) | Storage and query measurements, the method and the limits of the results. |
| [Task answers](task_answers.md) | Answers to each Week 1 task and its discussion questions. |
| [Platform README](../../README.md) | Setup, execution, testing and output semantics. |

The existing exports are [design_report.pdf](design_report.pdf), [architecture.pdf](architecture.pdf), [benchmark_report.pdf](benchmark_report.pdf) and [task_answers.pdf](task_answers.pdf). The standalone diagram is also available as [architecture.png](architecture.png). Markdown edits do not automatically update these exports.

The Spark runner generates the benchmark Markdown from a successful full-run JSON artifact. To rebuild the PDFs later, run `scripts/package_week1.py --metrics <successful-full-run.json>` from the project root, then visually inspect the resulting pagination. The builder reads the report sources in this directory, but also regenerates the benchmark source, replacing manual edits to that file.

During packaging, the selected metrics JSON is copied into `evidence/`. This preserves the reported samples, physical plans and configuration even though runtime storage is ignored by Git. Treat older reports as historical evidence and use the run identified in `benchmark_report.md` for the current comparison.
