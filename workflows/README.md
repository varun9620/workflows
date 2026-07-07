# workflows

Automated hourly TomTom traffic data collection for Delhi congestion research.

## What this does
Runs `tomtom_traffic_pull.py` automatically every hour, 6:00 AM - 10:00 PM IST,
via GitHub Actions (see `.github/workflows/tomtom_hourly_pull.yml`). No laptop
or server needs to stay on -- GitHub runs it in the cloud on a schedule.

Each run queries TomTom's Traffic Flow API for the points listed in
`temporal_sample_points.csv` and appends the results (speed, free-flow speed,
travel time, congestion %) to `tomtom_traffic_log.csv`, which is committed
back to this repo automatically.

## One-time setup

1. **Add your TomTom API key as a secret** (never commit it directly):
   - Repo -> Settings -> Secrets and variables -> Actions -> New repository secret
   - Name: `TOMTOM_API_KEY`
   - Value: your actual TomTom API key

2. **Test it manually** before waiting for the schedule:
   - Go to the Actions tab -> "TomTom Hourly Traffic Pull" -> "Run workflow"
   - Check the run succeeds and `tomtom_traffic_log.csv` gets updated

3. That's it -- it now runs automatically on the schedule defined in the
   workflow file (17 times/day, 6 AM-10 PM IST).

## Files
- `tomtom_traffic_pull.py` -- pulls traffic data for all points in the CSV
- `temporal_sample_points.csv` -- 230 stratified sample points across Delhi's
  major road classes (motorway/trunk/primary/secondary/tertiary)
- `tomtom_traffic_log.csv` -- accumulates results over time (created after
  the first run)
- `.github/workflows/tomtom_hourly_pull.yml` -- the schedule definition

## Notes
- GitHub Actions cron runs in UTC and isn't guaranteed to the minute --
  expect runs within ~30 min of the scheduled time, which is fine for
  hourly traffic sampling.
- Free tier: private repos get a monthly free-minutes quota; each run
  takes ~1-2 min, so 17 runs/day comfortably fits within the free quota.
