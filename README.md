### What's Frigate?

Frigate is a tool to retrieve and analyze alert data from the ZTF alert stream, from the full set of alerts to the subset of alerts that passed filters in Fritz, and the subset that was saved as proper astronomical sources.
The current `frigate/__main__.py` only addresses step 1 (getting the full set of alerts for a specified time period, usually one night). The script found in `scripts/alert-stats.py` is an earlier attempt at step 1,2, and 3 along with visualization. Next, we plan to integrate better versions of step 2 and 3 in the `frigate/__main__.py` script and add more visualization tools.

You can run frigate with:

```bash
PYTHONPATH=. python frigate --k_token=<your_token>
```

To get the list of all possible arguments, you can run:

```bash
PYTHONPATH=. python frigate --help
```

To run the deprecated `scripts/alert-stats.py` script, you can run:

```bash
PYTHONPATH=. python scripts/alert-stats.py --feature='candidate.magpsf,candidate.sigmapsf' --programids=1,2 --plot=True --start=2460355.5 --nb_days=1 --sp_token=<your_sp_token> --sp_groupIDs=41 --sp_filterIDs=1 --nb_bins=1000 --k_token=<your_kowalski_token>
```

- [] Fetch all the features of all alert packets within a given time range with given program ids and store it as a pandas dataframe
- [] Fetch all of the candidates that passed filters in Fritz (with exact candid, not just objectIds). Relies on the new /api/candidates_filter endpoint.
- [] Looking at the subset of alerts that passed the filters, find the obj_id of the sources that were saved in Fritz.
- [] Update the dataframe with a column containing the list of filters passed for each alert, and a column containing the groupIDs for each alert which obj has been saved as a source to the groups associated to the filters passed.
- [] Figure out what visualizations tools and plots we can use to represent the data in a meaningful way and extract insights from it.
