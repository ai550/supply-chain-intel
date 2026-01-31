# Dashboard: Supply Chain Disruption Intelligence

12-tile dashboard providing real-time supply chain health monitoring.

## Tile Layout

### Row 1: Port Strain Overview

| Tile | Type | Dataset |
|------|------|---------|
| 1. Port Strain Index | Time series (line) | mart_port_strain_daily |
| 2. Strain Bucket Distribution (30d) | Pie / Bar | mart_port_strain_daily |
| 3. Components of Strain | Multi-series line | mart_port_strain_daily |

### Row 2: Disruption & Weather Correlation

| Tile | Type | Dataset |
|------|------|---------|
| 4. Disruption Score vs Strain (lag 0-14d) | Dual-axis line | mart_port_strain_daily (virtual) |
| 5. Top Disruption Event Types (90d) | Horizontal bar | fct_disruption_daily + dim_event_type |
| 6. Weather Anomaly vs Strain (7d roll) | Dual-axis line | mart_port_strain_daily |

### Row 3: Trade Impact

| Tile | Type | Dataset |
|------|------|---------|
| 7. Trade Value Trend (by commodity) | Stacked area | mart_trade_impact_monthly |
| 8. Trade MoM and YoY Volatility | Bar + line combo | mart_trade_impact_monthly |
| 9. Impact Windows Leaderboard | Table | mart_event_impact_windows |

### Row 4: Heatmap, Health & Documentation

| Tile | Type | Dataset |
|------|------|---------|
| 10. Port Strain Heatmap (month x port) | Heatmap | mart_port_strain_daily |
| 11. Pipeline Health | Table | mart_pipeline_health |
| 12. Explain the Index | Markdown panel | N/A |

---

## Tile 12: Explain the Index (Markdown Content)

### Strain Index Formula

```
strain_index =
    ln(1 + teu_total) * 0.25
  + terminal_dwell_days * 0.30
  + avg_vessel_wait_hours * 0.25
  + truck_turn_time_min * 0.10
  + disruption_score_14d * 0.07
  + weather_anomaly_7d * 0.03
```

### Buckets
- **Low** (< 2): Normal operations
- **Medium** (2-4): Elevated activity; monitor
- **High** (4-6): Congestion; potential delays
- **Critical** (>= 6): Severe strain; disruptions likely

### Data Sources
- **Port Ops**: Daily TEU throughput, vessel calls, dwell times
- **GDELT**: Geopolitical event disruption scores (14-day rolling)
- **NOAA**: Weather anomaly scores (7-day rolling)
- **UN Comtrade**: Monthly bilateral trade flows

### Limitations
- Port operations data depends on public API availability and may lag 1-2 days
- GDELT country attribution is best-effort (geo-coding accuracy varies)
- Comtrade data is released with ~45-day delay
- Weather anomaly heuristic is simplified; does not account for seasonal norms
