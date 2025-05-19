import argparse
import time
from tsne_utils import alert_preprocessor, prep_TSNE, tSNE, logging


def main():
    parser = argparse.ArgumentParser(description="Process t-SNE parameters.")
    parser.add_argument(
        "--alerts_path", type=str, required=True, help="Path to the alerts parquet file"
    )
    parser.add_argument(
        "--drb_cut",
        type=float,
        default=0.4,
        help="drb cut value, set at 0 to keep all alerts",
    )
    parser.add_argument(
        "--filtered_only",
        action="store_true",
        help="To only work with filtered alerts (will speed up if testing)",
    )
    parser.add_argument(
        "--custom_columns",
        nargs="+",
        default=[],
        help="Provide a list of string names of columns to train on (empty list will use default columns)",
    )
    parser.add_argument(
        "--remove_instrumental",
        type=bool,
        default=False,
        help="Don't train on instrumental parameters (not relevant if using custom columns)",
    )
    parser.add_argument(
        "--use_PCA", action="store_true", help="Include argument to use PCA in prep"
    )
    parser.add_argument(
        "--pca_components", type=int, default=None, help="Number of PCA components"
    )
    parser.add_argument(
        "--implementation", type=str, default="openTSNE", help="openTSNE or sklearn"
    )
    parser.add_argument(
        "--perplexity", type=float, default=60, help="Perplexity for t-SNE"
    )
    parser.add_argument(
        "--early_exaggeration",
        type=float,
        default=12.0,
        help="Controls how tight natural clusters are",
    )
    parser.add_argument(
        "--learning_rate",
        type=lambda x: float(x) if x.lower() != "auto" else "auto",
        default="auto",
        help='Learning rate for t-SNE (float or "auto")',
    )
    parser.add_argument(
        "--max_iter",
        type=int,
        default=2000,
        help="Maximum number of iterations for t-SNE",
    )
    parser.add_argument(
        "--method", type=str, default="barnes_hut", help="Method for t-SNE"
    )
    parser.add_argument(
        "--n_jobs", type=int, default=8, help="Number of jobs to run in parallel"
    )
    parser.add_argument(
        "--save_path", type=str, default="default", help="Path to save t-SNE results"
    )
    parser.add_argument(
        "--log_path",
        type=str,
        default="../example_data/tsne_trained/log_tsne_trained.csv",
        help="Path to log t-SNE runs",
    )
    parser.add_argument(
        "--log_notes",
        type=str,
        default="None",
        help="Add any notes to log of the run",
    )

    args = parser.parse_args()

    preprocessor = alert_preprocessor(
        path=args.alerts_path,
        drb_cut=args.drb_cut,
        filtered_only=args.filtered_only,
        custom_columns=args.custom_columns,
        remove_instrumental=args.remove_instrumental,
    )
    df = preprocessor.preprocess_data()
    print("loaded data")

    prep = prep_TSNE(df, use_PCA=args.use_PCA, pca_ncomp=args.pca_components)
    data = prep.prep_data()
    print("prepared data : doing tsne")

    tsne = tSNE(
        data,
        perplexity=args.perplexity,
        early_exaggeration=args.early_exaggeration,
        learning_rate=args.learning_rate,
        max_iter=args.max_iter,
        method=args.method,
        n_jobs=args.n_jobs,
        save_path=args.save_path,
        implementation=args.implementation,
    )
    start_time = time.time()
    tsne, filename = tsne.get_tsne()
    end_time = time.time()
    print("done tsne")

    if args.log_path:
        row = [
            filename,
            args.alerts_path.split("/")[-1].replace(".parquet", ""),
            len(tsne),
            args.drb_cut,
            args.filtered_only,
            args.remove_instrumental,
            args.use_PCA,
            args.pca_components,
            args.implementation,
            args.perplexity,
            args.early_exaggeration,
            args.learning_rate,
            args.max_iter,
            args.method,
            args.n_jobs,
            round((end_time - start_time) / 3600, 2),
            args.custom_columns,
            args.log_notes,
        ]
        logging(args.log_path, row).log_run()
    print("logged run")
    print("t-SNE finished successfully")


if __name__ == "__main__":
    main()
