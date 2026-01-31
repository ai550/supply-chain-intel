## Strain Index

**Formula:**
```
strain_index =
    ln(1 + teu_total) * 0.25
  + terminal_dwell_days * 0.30
  + avg_vessel_wait_hours * 0.25
  + truck_turn_time_min * 0.10
  + disruption_score_14d * 0.07
  + weather_anomaly_7d * 0.03
```

**Buckets:** Low (< 2) | Medium (2-4) | High (4-6) | Critical (>= 6)

**Sources:** Port Ops (daily), GDELT (14d rolling), NOAA (7d rolling), UN Comtrade (monthly)

**Limitations:** Port data may lag 1-2d; GDELT geo-coding is best-effort; Comtrade ~45d release delay; weather anomaly heuristic is simplified.
