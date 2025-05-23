# Visualizing ZTF alerts

This directory contains tools to visualize the ZTF alert data which can be retrieved with Frigate. An example parquet file, modeling the data retrieved by Frigate for a single night of alerts, is saved in the [example_data directory](./example_data) and can be used to test these plotting options. An introduction to analyzing ZTF alerts can be found in the notebook [intro_to_frigate](./intro_to_frigate.ipynb). Additional analysis tools are demonstrated in [advanced_vis_frigate](./advanced_vis_frigate.ipynb).

Additional development of these visualization tools can be found on this [Miro board](https://miro.com/app/board/uXjVIHR3y_Y=/).

A library of plots is also shown in [these slides](https://docs.google.com/presentation/d/1tLZ_QJ0cQXdJHL7PKWDfVQ4HYYZ_ksNrbPm6PHHAa6w/edit?usp=sharing)

<br><br>

### Guiding questions

Which alerts are we sending that are never looked at and don't need to be sent (filter efficiency)?

Which alerts are we sending that are never looked at but are interesting (filter gap analysis, maybe)?

### Target outcomes

Make suggestions for improvements to filter efficiency

Provide the community with tools to better understand filter performance, and to build filters

Find interesting unfiltered objects / unexplored areas of parameter space

### Plots and tools overview

Basic distributions: histograms, violin plots, scatter plots, corner plots, pair plots

Feature importance: Pearson correlation matrix; Random forest mean difference in impurity and feature permutation; Principle Component Analysis

Visualize filters: chord diagram,

Visualize high dimensional data: t-SNE, UMAP, parallel histogram plots

### t-SNE

t-SNE is an tool to visualize high-dimensional data. An introduction to using t-SNE can be found in the notebook [tsne_example](tsne/tsne_example.ipynb). t-SNE can also be run via command line like so:

```bash
PYTHONPATH=. python tsne/tsne_main.py --alerts_path ./example_data/240319_public_filtered.parquet --custom_columns "candidate.sharpnr" "candidate.sgscore1" "candidate.chinr" --perplexity 50 --max_iter 500 --method barnes_hut --n_jobs 8 --save_path ./example_data/tsne_trained_example.pkl
```

The only argument required to run is an `alerts_path`, for example, the path to a night of alerts that has been saved locally with Frigate.

#### Optional Arguments:

- **`drb_cut`** _(float)_:
  An integer value between 0 and 1, so t-SNE is trained only on alerts above a Real-bogus score threshold.

- **`filtered_only`** _(bool)_:
  If `True`, only train on alerts that were filtered. This can additionally be helpful to try out t-SNE because it will run very quickly given the smaller number of alerts, where a full night of alerts can take over an hour.

- **`remove_instrumental`** _(bool)_:
  If `True`, alert parameters related to image quality, color filter, etc., will be removed.

- **`use_PCA`** _(bool)_:
  If `True`, the data will first be reduced to some number of components using PCA, which is a common method in t-SNE implementations and can help reduce redundancy from linearly correlated parameters.

- **`pca_components`** _(int)_:
  The number of components to output from PCA.

- **`implementation`** _(str)_:
  Select the t-SNE implementation to use, either `"openTSNE"` or `"sklearn"`.

- **`perplexity`** _(float)_:
  A t-SNE hyperparameter.

- **`early_exaggeration`** _(float)_:
  A t-SNE hyperparameter.

- **`learning_rate`** _(float)_:
  A t-SNE hyperparameter.

- **`max_iter`** _(int)_:
  A t-SNE hyperparameter.

- **`method`** _(str)_:
  A t-SNE hyperparameter.

- **`n_jobs`** _(int)_:
  A t-SNE hyperparameter.

- **`custom_columns`** _(list)_:
  Provide a list of custom parameters (as in the example above) to train on. Otherwise, t-SNE will train on all alert parameters (automatically removing irrelevant parameters, e.g., version numbers).

- **`log_path`** _(str)_:
  Path to save the log that will automatically be saved for the t-SNE run.
  **Default**: `../example_data/log_tsne_trained.csv`

- **`log_notes`** _(str)_:
  Add a custom note to the log that will automatically be saved for the t-SNE run.
