Example cmd to run the script:

```bash
PYTHONPATH=. python scripts/alert-stats.py --feature='candidate.magpsf,candidate.sigmapsf' --programids=1,2 --plot=True --start=2460355.5 --nb_days=1 --sp_token=<your_sp_token> --sp_groupIDs=41 --sp_filterIDs=1 --nb_bins=1000 --k_token=<your_kowalski_token>
```

This would:

- grab magpsf and sigmapsf values of all alerts from one night starting at jd=2460355.5
- get these values for the subset of alerts that passed the alert filter from group 41 and filter 1 on SkyPortal/Fritz
- get these values for the subset of alerts that passed the alert filter AND were saved as sources (at anytime, not necessarily during that night).
- plot histograms for each feature and alert subset (red: all, blue: passed filter, green: saved as sources)
- plot corner plots for each feature and alert subset (same color palette)

#### V2 (WIP)

- [] Fetch all the features of all alert packets within a given time range with given program ids and store it as a pandas dataframe
- [] Fetch all of the candidates that passed filters in Fritz (with exact candid, not just objectIds). Relies on the new /api/candidates_filter endpoint.
- [] Looking at the subset of alerts that passed the filters, find the obj_id of the sources that were saved in Fritz.
- [] Update the dataframe with a column containing the list of filters passed for each alert, and a column containing the groupIDs for each alert which obj has been saved as a source to the groups associated to the filters passed.
- [] Figure out what visualizations tools and plots we can use to represent the data in a meaningful way and extract insights from it.

You can run frigate with:

```bash
PYTHONPATH=. python frigate -k_token=<your_token>
```

To get the list of all possible arguments, you can run:

```bash
PYTHONPATH=. python frigate --help
```
