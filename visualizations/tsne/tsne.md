### t-SNE for ZTF Alerts

This directory contains code to use t-SNE to visualize ZTF alerts.

We use the scikit-learn implementation of t-SNE, and you can read more about some of the optional arguments there. https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html

We use the [scikit-learn implementation of t-SNE](https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html), and you can read more about some of the optional arguments there.

`tsne_example.ipynb`
This is a good starting place for t-SNE, walking through the implementation.

`tsne_utils.py`
This contains functions to prep the data retrieved by frigate, and do t-SNE.

`alert_classifications.py`
This contains functions to query various sources to get additional classifications for the alerts, to label and understand the t-SNE performance.

`plots_tsne.py`
This contains functions to plot the t-SNE results in various ways.

You can run t-SNE directly with default settings:

```bash
PYTHONPATH=. python tsne_main.py --alerts_path ../example_data/240319_public_filtered.parquet
```

and with customizable arguments:

```bash
PYTHONPATH=. python tsne_main.py --alerts_path ../example_data/240319_public_filtered.parquet --drb_cut 0.4 --filtered_only False --custom_columns candidate.magpsf candidate.drb classifications.acai_h age lastobs --remove_instrumental False --use_PCA True --pca_components 40 --perplexity 60 --max_iter 2000 --method barnes_hut --n_jobs 8 --save_path /path/to/save/results
```
