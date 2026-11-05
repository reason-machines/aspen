# Observability assets

| File | What |
|---|---|
| [grafana/aspen.json](grafana/aspen.json) | Grafana dashboard: needs-attention stats, pushes, fetches, background jobs, the express tier. Import it and pick a Prometheus data source |
| [prometheus/aspen-alerts.yml](prometheus/aspen-alerts.yml) | Prometheus alerting rules, labelled `severity: page` or `severity: ticket` |

What each metric means, where the server serves them, and how to act on
each alert: [docs/OBSERVABILITY.md](../../docs/OBSERVABILITY.md).

`cargo test -p aspen-server --test observability` checks that every metric
name these files use is one the server exports, and that every paging
counter has an alert rule. Edit the dashboard in Grafana and export it
("Export for sharing externally"), or edit the JSON directly; keep panel
queries on the `$job` and `$datasource` variables.
