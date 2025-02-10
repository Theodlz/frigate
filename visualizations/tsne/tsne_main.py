import argparse
from tsne_utils import alert_preprocessor, prep_TSNE, tSNE


def main():
    parser = argparse.ArgumentParser(description="Process t-SNE parameters.")
    parser.add_argument(
        "--alerts_path", type=str, required=True, help="Path to the alerts parquet file"
    )
    parser.add_argument(
        "--drb_cut",
        type=int,
        default=0.4,
        help="drb cut value, set at 0 to keep all alerts",
    )
    parser.add_argument(
        "--filtered_only",
        type=bool,
        default=False,
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
        help="To only work with filtered alerts (will speed up if testing)",
    )
    parser.add_argument(
        "--use_PCA", type=bool, default=False, help="Include argument to not use PCA"
    )
    parser.add_argument(
        "--pca_components", type=int, default=40, help="Number of PCA components"
    )
    parser.add_argument(
        "--perplexity", type=float, default=60, help="Perplexity for t-SNE"
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
        max_iter=args.max_iter,
        method=args.method,
        n_jobs=args.n_jobs,
        save_path=args.save_path,
    )
    tsne.get_tsne()
    print("done tsne")


if __name__ == "__main__":
    main()
