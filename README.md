Example cmd to run the script:

```bash
PYTHONPATH=. python filter_stats.py --feature='candidate.magpsf,candidate.sigmapsf' --programids=1,2 --plot=True --start=2460355.5 --nb_days=1 --sp_token=<your_sp_token> --sp_groupIDs=41 --sp_filterIDs=1 --nb_bins=1000 --k_token=<your_kowalski_token>
```

This would:
- grab magpsf and sigmapsf values of all alerts from one night starting at jd=2460355.5
- get these values for the subset of alerts that passed the alert filter from group 41 and filter 1 on SkyPortal/Fritz
- get these values for the subset of alerts that passed the alert filter AND were saved as sources (at anytime, not necessarily during that night).
- plot histograms for each feature and alert subset (red: all, blue: passed filter, green: saved as sources)
- plot corner plots for each feature and alert subset (same color palette)
