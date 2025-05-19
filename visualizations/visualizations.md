### Visualizing ZTF alerts

This directory contains tools to visualize the ZTF alert data which can be retrieved with Frigate. An example parquet file, modeling the data retrieved by Frigate for a single night of alerts, is saved in the [example_data directory](./example_data) directory and can be used to test these plotting options. An introduction to analyzing ZTF alerts can be found in the notebook [intro_to_frigate](./intro_to_frigate.ipynb).

Additional development of these visualization tools can be found on this [Miro board](https://miro.com/app/board/uXjVIHR3y_Y=/).

# t-SNE

t-SNE is an tool to visualize high-dimensional data. An introduction to using t-SNE can be found in the notebook [tsne_example](tsne/tsne_example.ipynb).

t-SNE can also be run via command line like so:

```bash
PYTHONPATH=. python tsne/tsne_main.py --alerts_path ./example_data/240319_public_filtered.parquet --custom_columns "candidate.sharpnr" "candidate.sgscore1" "candidate.chinr" --perplexity 60 --max_iter 5000 --method barnes_hut --n_jobs 8 --save_path ../example_data/tsne_trained/tsne_trained_example.pkl
```

The only required argument to run is an "alerts_path". Optional arguments include:

- drb_cut: an integer value between 0 and 1, so t-SNE is trained only on alerts above a Real-bogus score threshold.

- filtered_only: if True, then only train on alerts that were filtered. This can additionaly be helpful to try out the t-SNE because it will run very quickly given the smaller number of alerts.

- remove_instrumental: if True, then alert parameters related to image quality, color filter etc. will be removed.

- use_PCA: if True, then the data will first be reduced to some number of components using PCA, which is a common method in t-SNE implementations and can help reduce redundancy from linearly correlated parameters.

- pca_components: and integer number of components to output from PCA

- implementation: select the t-SNE implementation to use, either "openTSNE" or "sklearn"

- perplexity: a t-SNE hyperparameter

- early_exaggeration: a t-SNE hyperparameter

- learning_rate: a t-SNE hyperparameter

- max_iter: a t-SNE hyperparameter

- method: a t-SNE hyperparameter

- n_jobs: a t-SNE hyperparameter

- custom_colomns: provide a list of custom parameters (as in example above) to train on, otherwise will train on alert parameters (automatically remove absolutely irrelevant parameters ie version numbers).

- log_path: path to save the log that will automatically be saved for the t-SNE run. The default is "../example_data/tsne_trained/log_tsne_trained.csv"

- log_notes: string, add a note to the log that will automatically be saved for the t-SNE run
