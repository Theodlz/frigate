import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
import astropy.units as u
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.lines import Line2D
from matplotlib.colors import LogNorm
import corner
import seaborn as sns
import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    confusion_matrix,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
)
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import joblib
from astropy.time import Time
import time
from pathlib import Path
import ephem
from datetime import date as DATE


class PrepPlotter:
    def __init__(
        self, path_df, cut_drb=None, ignore_scores=False, multiple_nights=False
    ):
        self.path_df = path_df
        self.cut_drb = cut_drb
        self.ignore_scores = ignore_scores
        self.multiple_nights = multiple_nights

    def remove_filters(self, arr):
        if arr.size == 0:
            return arr
        remove_values = {
            20,
            55,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            74,
            75,
            76,
            79,
            81,
            89,
            90,
            100,
            102,
            103,
            106,
            1159,
            1162,
            1163,
            1164,
            1168,
            1181,
        }
        mask = np.vectorize(lambda x: x not in remove_values)(arr)
        return arr[mask]

    def parameter_modifications(self, df):
        # add age parameter
        df["age"] = df["candidate.jd"] - df["candidate.jdstarthist"]
        df["lastobs"] = df["candidate.jd"] - df["candidate.jdendhist"]
        # use ra and dec to get galactic latitude
        ra = df["candidate.ra"]
        dec = df["candidate.dec"]
        coords = SkyCoord(
            ra=ra.values * u.degree, dec=dec.values * u.degree, frame="icrs"
        )
        galactic_coords = coords.galactic
        galactic_latitudes = galactic_coords.b.deg
        df["galactic_latitude"] = galactic_latitudes
        # add some values related to filtering
        df["filtered_bool"] = df["passed_filters"].apply(
            lambda x: 0 if len(x) == 0 else 1
        )
        df["number_filtered"] = df["passed_filters"].apply(len)
        # make t/f 1/0
        df["candidate.isdiffpos"] = df["candidate.isdiffpos"].map({"t": 1, "f": 0})
        return df

    def ignore_columns(self, df):
        # remove columns that are not useful for analysis
        ignore = [
            "objectId",
            "candid",
            "candidate.jd",
            "candidate.pid",
            "candidate.programid",
            "candidate.tblid",
            "candidate.nid",
            "candidate.rcid",
            "candidate.field",
            "candidate.xpos",
            "candidate.ypos",
            "candidate.rbversion",
            "candidate.drbversion",
            "candidate.ssnamenr",
            "candidate.ranr",
            "candidate.decnr",
            "candidate.tooflag",
            "candidate.objectidps1",
            "candidate.objectidps2",
            "candidate.objectidps3",
            "candidate.rfid",
            "candidate.jdstartref",
            "candidate.jdendref",
            "candidate.nframesref",
            "classifications.braai_version",
            "classifications.acai_h_version",
            "classifications.acai_v_version",
            "classifications.acai_o_version",
            "classifications.acai_n_version",
            "classifications.acai_b_version",
            "classifications.bts_version",
            "candidate.jdstarthist",
            "candidate.jdendhist",
            "candidate.ra",
            "candidate.dec",
        ]
        df = df.drop(columns=[col for col in ignore if col in df.columns])
        if self.ignore_scores:
            scores = [
                "classifications.bts",
                "classifications.braai",
                "classifications.acai_h",
                "classifications.acai_v",
                "classifications.acai_o",
                "classifications.acai_n",
                "classifications.acai_b",
            ]
            df = df.drop(columns=[col for col in scores if col in df.columns])
        return df

    def shift_errors(self, df):
        # reassign -999 values to be just outside the range of the data
        for column in df.columns:
            if pd.api.types.is_numeric_dtype(df[column]):
                valid_values = df[column][df[column] > -900]
                if not valid_values.empty:
                    min_value = valid_values.min()
                    value_range = valid_values.max() - min_value
                    new_value = min_value - 0.1 * value_range
                    df[column] = df[column].apply(
                        lambda x: new_value if x < -900 else x
                    )
        return df

    def prep_data(self):
        if self.multiple_nights:
            data_dir = Path(self.path_df)
            file_names = [
                parquet_file.name for parquet_file in data_dir.glob("*.parquet")
            ]
            nights = [name.split("_")[0] for name in file_names]
            night_names = [
                Time(night, format="jd").to_value("iso", subfmt="date")
                for night in nights
            ]
            df = [
                pd.read_parquet(parquet_file)
                for parquet_file in data_dir.glob("*.parquet")
            ]
        else:
            df = [pd.read_parquet(self.path_df)]

        if self.cut_drb:
            df = [x[x["candidate.drb"] > self.cut_drb] for x in df]

        # Check for 'passed_filters' column and augment data if necessary for testing plots
        if "passed_filters" not in df[0].columns:
            for x in df:
                x["passed_filters"] = [
                    np.array([]) if np.random.rand() < 0.95 else np.array([1])
                    for _ in range(len(x))
                ]

        df = [
            self.shift_errors(
                self.ignore_columns(
                    self.parameter_modifications(
                        x.assign(
                            passed_filters=x["passed_filters"].apply(
                                self.remove_filters
                            )
                        )
                    )
                )
            ).rename(
                columns=lambda col: col.replace("candidate.", "").replace(
                    "classifications.", ""
                )
            )
            for x in df
        ]
        print(f"Loaded {len(df)} nights of alerts")

        if not self.multiple_nights:
            df = df[0]
            night_names = self.path_df.split("/")[-1].split("_")[0]
        return df, night_names

    # def prep_data(self):
    #     if self.multiple_nights:
    #         data_dir = Path(self.path_df)
    #         parquet_files = list(data_dir.glob('*.parquet'))
    #         # Extract night names and load data
    #         file_names = [parquet_file.name for parquet_file in parquet_files]
    #         nights = [name.split('_')[0] for name in file_names]
    #         df = pd.concat(
    #             [pd.read_parquet(parquet_file).assign(night=night) for parquet_file, night in zip(parquet_files, nights)],
    #             ignore_index=True
    #         )
    #     else:
    #         df = pd.read_parquet(self.path_df)
    #         nights = self.path_df.split('/')[-1].split('_')[0]
    #         df['night'] = nights
    #     # prep data
    #     if self.cut_drb:
    #         df = df[df['candidate.drb'] > self.cut_drb]
    #     # Check for 'passed_filters' column and augment data if necessary
    #     if 'passed_filters' not in df.columns:
    #         df['passed_filters'] = np.where(
    #             np.random.rand(len(df)) < 0.95,
    #             np.empty((len(df), 0), dtype=int),  # Empty arrays
    #             np.array([[1]] * len(df), dtype=object)  # Arrays with [1]
    #         )
    #     df['passed_filters'] = df['passed_filters'].apply(self.remove_filters)
    #     df = self.shift_errors(
    #         self.ignore_columns(
    #             self.parameter_modifications(df)
    #         )
    #     ).rename(columns=lambda col: col.replace('candidate.', '').replace('classifications.', ''))

    #     print(f'Loaded {len(parquet_files) if self.multiple_nights else 1} nights of alerts')
    #     # Split back into separate DataFrames by 'night'
    #     if self.multiple_nights:
    #         dfs = [group for _, group in df.groupby('night')]
    #     else:
    #         dfs = df

    #     return dfs, nights


class HistogramPlotter:
    def __init__(self, df):
        self.df = df

    def histogram(self, parameter, log=False):
        grouped_df = [
            self.df[self.df["fid"] == 1],
            self.df[self.df["fid"] == 2],
            self.df[self.df["fid"] == 3],
        ]
        param = [
            np.concatenate(df[[parameter]].values) if len(df) > 0 else []
            for df in grouped_df
        ]

        fig, ax = plt.subplots()
        colors = ["darkseagreen", "rosybrown", "tan"]
        ax.hist(param, histtype="bar", bins=50, stacked=True, color=colors, log=log)
        ax.set_xlabel(parameter, fontsize=15)
        ax.set_ylabel("Number", fontsize=15)
        ax.set_title(f"Histogram of {parameter}", fontsize=30)
        plt.show()


class ViolinPlotter:
    def __init__(self, df):
        self.df = df

    def violin(self, parameter):
        filtered_df = [
            self.df[self.df["filtered_bool"] == 0],
            self.df[self.df["filtered_bool"] == 1],
        ]
        if len(filtered_df[1]) > 0:
            data = [np.concatenate(df[[parameter]].values) for df in filtered_df]
        else:
            data = filtered_df[0][[parameter]].values

        fig, ax = plt.subplots()
        plot = ax.violinplot(data, showmedians=True, points=10)
        for pc in plot["bodies"]:
            pc.set_facecolor("#b7c9e2")
            pc.set_edgecolor("black")
            pc.set_alpha(1)

        x = len(filtered_df[0])
        y = len(filtered_df[1])
        labels = ["%i Objects not filtered" % x, "%i Objects filtered" % y]
        ax.set_xticks(np.arange(1, len(labels) + 1), labels=labels)
        ax.set_xlim(0.25, len(labels) + 0.75)

        ax.set_title(f"Violin plot of {parameter}", fontsize=30)
        plt.show()


class ScatterPlotter:
    def __init__(self, df):
        self.df = df

    def scatter(self, param1, param2):
        fig, axes = plt.subplots(1, 3, figsize=(12, 5))
        # for axes
        x_min, x_max = self.df[param1].min(), self.df[param1].max()
        y_min, y_max = self.df[param2].min(), self.df[param2].max()

        g_filtered = self.df[(self.df["fid"] == 1) & (self.df["filtered_bool"] == 1)]
        g_unfiltered = self.df[(self.df["fid"] == 1) & (self.df["filtered_bool"] == 0)]
        r_filtered = self.df[(self.df["fid"] == 2) & (self.df["filtered_bool"] == 1)]
        r_unfiltered = self.df[(self.df["fid"] == 2) & (self.df["filtered_bool"] == 0)]
        i_filtered = self.df[(self.df["fid"] == 3) & (self.df["filtered_bool"] == 1)]
        i_unfiltered = self.df[(self.df["fid"] == 3) & (self.df["filtered_bool"] == 0)]

        axes[0].set_title("g", fontsize=16)
        hb1 = axes[0].hexbin(
            g_unfiltered[param1], g_unfiltered[param2], cmap="binary", bins="log"
        )
        axes[0].hexbin(
            g_filtered[param1], g_filtered[param2], cmap="viridis", bins="log"
        )
        axes[0].set_xlim([x_min, x_max])
        axes[0].set_ylim([y_min, y_max])
        axes[0].set_aspect("auto")
        axes[0].tick_params(axis="both", which="major", labelsize=17)

        axes[1].set_title("r", fontsize=16)
        hb3 = axes[1].hexbin(
            r_unfiltered[param1], r_unfiltered[param2], cmap="binary", bins="log"
        )
        axes[1].hexbin(
            r_filtered[param1], r_filtered[param2], cmap="viridis", bins="log"
        )
        axes[1].set_xlim([x_min, x_max])
        axes[1].set_ylim([y_min, y_max])
        axes[1].set_aspect("auto")
        axes[1].tick_params(axis="x", which="major", labelsize=17)
        axes[1].set_yticks([])

        axes[2].set_title("i", fontsize=16)
        if len(i_unfiltered) == 0 and len(i_filtered) == 0:
            axes[2].set_title("i: No data", fontsize=16)
        else:
            # hb5 = axes[2].hexbin(
            #     i_unfiltered[param1], i_unfiltered[param2], cmap="binary", bins="log"
            # )
            axes[2].hexbin(
                i_filtered[param1], i_filtered[param2], cmap="viridis", bins="log"
            )
        axes[2].set_xlim([x_min, x_max])
        axes[2].set_ylim([y_min, y_max])
        axes[2].set_aspect("auto")
        axes[2].tick_params(axis="x", which="major", labelsize=17)
        axes[2].set_yticks([])

        cax1 = fig.add_axes([0.13, 0.02, 0.23, 0.02])
        cax2 = fig.add_axes([0.67, 0.02, 0.23, 0.02])
        cax1.tick_params(labelsize=14)
        cax2.tick_params(labelsize=14)

        fig.colorbar(hb1, cax=cax1, orientation="horizontal")
        fig.colorbar(hb3, cax=cax2, orientation="horizontal", ticks=[1])

        axes[1].set_xlabel(param1, fontsize=20)
        axes[0].set_ylabel(param2, fontsize=20)

        fig.suptitle(f"{param2} vs {param1}", fontsize=30)
        fig.subplots_adjust(top=0.85)


class CornerPlotter:
    def __init__(self, df):
        self.df = df

    def cornerplot(self, plot_features, log_features):
        data = self.df[plot_features]

        # reset error values (assuming none of our parameters have negative values)
        negative_counts = (data < 0).sum()
        for column, count in negative_counts.items():
            if count > 0:
                print(f"Column '{column}' has {count} negative values")
        data_cleaned = data.apply(
            lambda x: x.clip(lower=1e-10) if x.name != "filtered_bool" else x
        )

        # take log of some features
        data_cleaned = data_cleaned.apply(
            lambda x: np.log(x) if x.name in log_features else x
        )

        # split filtered and unfiltered
        data1 = data_cleaned[data_cleaned["filtered_bool"] == 1][plot_features[:-1]]
        data0 = data_cleaned[data_cleaned["filtered_bool"] == 0][plot_features[:-1]]

        label = [
            ("log " if feature in log_features else "")
            + feature.replace("candidate.", "").replace("classifications.", "")
            for feature in plot_features
        ]

        figure = corner.corner(
            data0,
            color="orange",
            labels=label,
            title_kwargs={"fontsize": 18},
            label_kwargs={"fontsize": 18},
            hist_kwargs={"density": True},
            plot_density=True,
            log_contours=True,
            fill_contours=True,
            plot_datapoints=False,
        )

        corner.corner(
            data1,
            color="blue",
            labels=label,
            title_kwargs={"fontsize": 18},
            label_kwargs={"fontsize": 18},
            hist_kwargs={"density": True},
            plot_density=True,
            log_contours=True,
            fill_contours=True,
            plot_datapoints=False,
            fig=figure,
        )

        orange_line = mlines.Line2D(
            [], [], color="orange", label=f"{len(data0)} Not Filtered"
        )
        blue_line = mlines.Line2D([], [], color="blue", label=f"{len(data1)} Filtered")
        plt.legend(
            handles=[orange_line, blue_line],
            loc="upper right",
            bbox_to_anchor=(1, 3),
            prop={"size": 14},
        )
        figure.suptitle(
            "Corner plot of filtered and unfiltered data", fontsize=30, y=1.02
        )

        for ax in figure.get_axes():
            ax.tick_params(axis="both", which="major", labelsize=14)

        plt.show()


class PairPlotter:
    def __init__(self, df):
        self.df = df

    def pairplot(self, plot_features):
        data = self.df[plot_features]
        # check error values (assuming none of our parameters should have negative values)
        negative_counts = (data < 0).sum()
        for column, count in negative_counts.items():
            if count > 0:
                print(f"Column {column} has {count} negative (error) values")

        for column, _ in negative_counts.items():
            smallest_non_negative = data[data[column] > 0][column].min()
            shift_value = smallest_non_negative * 0.99
            shift_value = shift_value.astype(data[column].dtype)
            data.loc[data[column] < 0, column] = shift_value

        # plot
        pairplot = sns.pairplot(data, hue="filtered_bool", corner=True, kind="hist")

        # Set title
        pairplot.fig.suptitle(
            "Pairplot of filtered and unfiltered data", fontsize=30, y=1.02
        )

        # Make axis labels bigger
        for ax in pairplot.axes.flatten():
            if ax is not None:
                ax.set_xlabel(ax.get_xlabel(), fontsize=14)
                ax.set_ylabel(ax.get_ylabel(), fontsize=14)

        pairplot._legend.set_title(None)
        legend_labels = ["Unfiltered", "Filtered"]
        for text, label in zip(pairplot._legend.get_texts(), legend_labels):
            text.set_text(label)
            text.set_fontsize(18)

        plt.show()


class PearsonCorrelation:
    def __init__(self, df):
        self.df = df
        self.corr_df = self.pearson_corr()

    def pearson_corr(self):
        df = self.df.drop(
            columns=["filtered_bool", "number_filtered", "passed_filters"]
        )
        df_correlation = df.corr(method="pearson")
        return df_correlation

    def plot_correlation_matrix(self, threshold=0.5):
        filtered_columns = self.corr_df.columns[(self.corr_df.abs() > threshold).any()]
        filtered_correlation = self.corr_df.loc[filtered_columns, filtered_columns]
        plt.figure(figsize=(10, 8))
        sns.heatmap(
            filtered_correlation,
            annot=False,
            cmap="coolwarm",
            square=True,
            cbar_kws={"shrink": 0.8},
        )
        plt.title("Pearson's r Correlation Matrix", fontsize=20)
        plt.show()

    def print_highest_correlations(self, threshold=0.8):
        high_corr = self.corr_df[
            (self.corr_df.abs() > threshold) & (self.corr_df != 1.0)
        ]
        correlation_list = []
        printed_pairs = set()
        for index, row in high_corr.iterrows():
            for column, value in row.items():
                if not pd.isna(value):
                    pair = frozenset([index, column])
                    if pair not in printed_pairs:
                        correlation_list.append((index, column, value))
                        printed_pairs.add(pair)
        correlation_list.sort(key=lambda x: abs(x[2]), reverse=True)
        for index, column, value in correlation_list:
            print(f"{index} + {column} (Correlation: {round(value, 2)})")


class RandomForest:
    def __init__(self, df, target_column="filtered_bool", use_weight=True, save=True):
        self.df = df
        self.target_column = target_column
        self.use_weight = use_weight
        self.save = save
        self.forest = None
        self.X_test = None
        self.y_test = None
        self.feature_names = None

    def train_rf(self):
        # Get train-test split
        columns_to_drop = ["passed_filters", "objectId", "number_filtered", "type"]
        columns_to_drop = [
            col
            for col in columns_to_drop
            if col in self.df.columns and col != self.target_column
        ]
        training_df = self.df.drop(columns=columns_to_drop)
        X = training_df.drop(columns=[self.target_column]).to_numpy()
        y = training_df[self.target_column].to_numpy()
        X_train, self.X_test, y_train, self.y_test = train_test_split(
            X, y, stratify=y, random_state=42
        )
        # Train Random Forest
        self.feature_names = [f"feature {i}" for i in range(X.shape[1])]
        if self.use_weight:
            class_weights = {0: 0.05, 1: 0.95}
            self.forest = RandomForestClassifier(
                random_state=0, class_weight=class_weights
            )
        else:
            self.forest = RandomForestClassifier(random_state=0)
        self.forest.fit(X_train, y_train)
        # Save the model if required
        if self.save:
            filename = f'./example_data/alert_rf_{Time.now().strftime("%Y-%m-%d_%H:%M:%S")}.pkl'
            joblib.dump(self.forest, filename)

    def rf_confusion_matric(self):
        y_pred = self.forest.predict(self.X_test)
        conf_matrix = confusion_matrix(self.y_test, y_pred)
        conf_matrix_normalized = (
            conf_matrix.astype("float") / conf_matrix.sum(axis=1)[:, np.newaxis]
        )
        labels = np.array(
            [
                f"{value:.2%}\n({count})"
                for value, count in zip(
                    conf_matrix_normalized.flatten(), conf_matrix.flatten()
                )
            ]
        ).reshape(conf_matrix.shape)

        plt.figure(figsize=(10, 7))
        sns.heatmap(
            conf_matrix_normalized,
            annot=labels,
            fmt="",
            cmap="Blues",
            xticklabels=["Not Filtered", "Filtered"],
            yticklabels=["Not Filtered", "Filtered"],
        )
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.title("Confusion Matrix for RF Classifier")
        plt.show()

        accuracy = accuracy_score(self.y_test, y_pred)
        precision = precision_score(self.y_test, y_pred, average="weighted")
        recall = recall_score(self.y_test, y_pred, average="weighted")
        f1 = f1_score(self.y_test, y_pred, average="weighted")

        print(f"Accuracy: {accuracy:.2f}")
        print(f"Precision: {precision:.2f}")
        print(f"Recall: {recall:.2f}")
        print(f"F1 Score: {f1:.2f}")

        print("\nClassification Report:")
        print(classification_report(self.y_test, y_pred))

    def get_mdi(self, num_top_features=20):
        start_time = time.time()
        importances = self.forest.feature_importances_
        std = np.std(
            [tree.feature_importances_ for tree in self.forest.estimators_], axis=0
        )
        elapsed_time = time.time() - start_time

        print(f"Elapsed time to compute the importances: {elapsed_time:.3f} seconds")

        columns_to_drop = [
            "passed_filters",
            "objectId",
            "number_filtered",
            "type",
            "filtered_bool",
        ]
        columns_to_drop = [col for col in columns_to_drop if col in self.df.columns]
        feature_names = self.df.drop(columns=columns_to_drop).columns

        std_series = pd.Series(std, index=feature_names)
        forest_importances = pd.Series(importances, index=feature_names)
        top_mdi_importances = forest_importances.nlargest(num_top_features)
        print(top_mdi_importances)

        fig, ax = plt.subplots(figsize=(12, 8))
        top_mdi_importances.plot.bar(yerr=std_series[top_mdi_importances.index], ax=ax)
        ax.set_title(
            f"Top {num_top_features} Feature Importances using MDI", fontsize=25
        )
        ax.set_ylabel("Mean decrease in impurity", fontsize=18)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=18)
        fig.tight_layout()
        plt.show()

    def get_feature_permutation(self, num_top_features=20):
        start_time = time.time()
        result = permutation_importance(
            self.forest,
            self.X_test,
            self.y_test,
            n_repeats=10,
            random_state=42,
            n_jobs=2,
        )
        elapsed_time = time.time() - start_time
        print(f"Elapsed time to compute the importances: {elapsed_time:.3f} seconds")

        columns_to_drop = [
            "passed_filters",
            "objectId",
            "number_filtered",
            "type",
            "filtered_bool",
        ]
        columns_to_drop = [col for col in columns_to_drop if col in self.df.columns]
        feature_names = self.df.drop(columns=columns_to_drop).columns

        forest_importances = pd.Series(result.importances_mean, index=feature_names)

        importances_std_series = pd.Series(
            result.importances_std, index=forest_importances.index
        )
        top_permutation_importances = forest_importances.nlargest(num_top_features)
        print(top_permutation_importances)

        fig, ax = plt.subplots(figsize=(12, 8))
        top_permutation_importances.plot.bar(
            yerr=importances_std_series[top_permutation_importances.index], ax=ax
        )
        ax.set_title(
            f"Top {num_top_features} Feature Importances using permutation on full model",
            fontsize=25,
        )
        ax.set_ylabel("Mean decrease in accuracy", fontsize=18)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=18)
        fig.tight_layout()
        plt.show()


class PrincipleComponentAnalysis:
    def __init__(self, df):
        self.df = df
        self.pca = None
        self.df_normalized = None

    def get_PCA(self):
        # normalize the data - z-score transformation
        scaler = StandardScaler()
        columns_to_drop = [
            "passed_filters",
            "objectId",
            "number_filtered",
            "type",
            "filtered_bool",
        ]
        columns_to_drop = [col for col in columns_to_drop if col in self.df.columns]
        training_df = self.df.drop(columns=columns_to_drop)
        data_scaled = scaler.fit_transform(training_df)
        df_normalized = pd.DataFrame(data_scaled, columns=training_df.columns)
        df_normalized = df_normalized.reset_index(drop=True)
        # do PCA
        pca = PCA()
        pca_result = pca.fit_transform(df_normalized)
        self.pca = pca
        df_normalized["filtered_bool"] = self.df["filtered_bool"]
        df_normalized["pca-one"] = pca_result[:, 0]
        df_normalized["pca-two"] = pca_result[:, 1]
        self.df_normalized = df_normalized

    def scree_plot(self, max_components=50):
        # Get the explained variance ratio for each principal component
        explained_variance = self.pca.explained_variance_ratio_
        # Set the maximum number of principal components to plot
        plt.figure(figsize=(16, 6))
        plt.plot(
            range(1, min(len(explained_variance), max_components) + 1),  # x-axis
            explained_variance[:max_components]
            * 100,  # convert explained variance in percentage
            marker="o",  # add a marker at each value
        )
        plt.title("Scree Plot of Explained Variance for Principal Components")
        plt.xlabel("Principal Component")
        plt.ylabel("Explained Variance (in %)")
        plt.xticks(range(1, min(len(explained_variance), max_components) + 1))
        plt.xlim(1, min(len(explained_variance), max_components))
        plt.show()

    def rank_pca_components(self):
        eigenvectors = abs(self.pca.components_)
        PC1 = eigenvectors[0]
        PC2 = eigenvectors[1]

        def rank_features(features, PC):
            return pd.DataFrame({"feature": features, "PC": PC}).sort_values(
                by="PC", ascending=True
            )

        pc1features = rank_features(self.df_normalized.iloc[:, :-3].columns, PC1)
        print("Component 1:")
        print(pc1features.head(15))
        pc2features = rank_features(self.df_normalized.iloc[:, :-3].columns, PC2)
        print("Component 2:")
        print(pc2features.head(15))

    def plot_pca(self, hue="filtered_bool"):
        df_normalized_sorted = self.df_normalized.sort_values(by=hue, ascending=True)
        plt.figure(figsize=(16, 10))
        sns.scatterplot(
            x="pca-one",
            y="pca-two",
            hue=hue,
            palette=sns.color_palette(["blue", "orange"]),
            data=df_normalized_sorted,
            legend="full",
            alpha=0.9,
        )


class HeatmapNumFiltersPassed:
    def __init__(self, dfs, nightnames):
        self.dfs = dfs
        self.nightnames = nightnames

    def num_filters_passed(self, df, nightname):
        total = len(df)
        num_filtered = df.passed_filters.str.len()
        total_num_filtered = (num_filtered > 0).sum()
        count_num = num_filtered.value_counts()
        df1 = pd.DataFrame(
            {
                "# Filters Passed": ["All"] + ["Filtered"],
                "# Alerts Filtered": [total] + [total_num_filtered],
            }
        )
        df2 = pd.DataFrame(
            {
                "# Filters Passed": count_num.index.tolist(),
                "# Alerts Filtered": count_num.tolist(),
            }
        )
        df3 = df2.sort_values(by="# Filters Passed")
        df4 = pd.concat([df1, df3], ignore_index=True)
        df4["Night"] = nightname
        return df4

    def heatmap(self, table, nightname):
        df = pd.concat(table, axis=0)
        df = df[df["# Filters Passed"] != 0]
        plot = df.pivot(
            index="Night", columns="# Filters Passed", values="# Alerts Filtered"
        )
        cols_to_move = ["All", "Filtered"]
        remaining_cols = [col for col in plot.columns if col not in cols_to_move]
        plot = plot[cols_to_move + remaining_cols]
        plot = plot.reindex(nightname)
        f, ax = plt.subplots(figsize=(16, 15))
        sns.heatmap(
            plot,
            annot=True,
            fmt=".0f",
            linewidths=0.5,
            ax=ax,
            cbar=False,
            norm=LogNorm(),
        )
        cbar = plt.gcf().colorbar(ax.collections[0])
        cbar.ax.set_position([0.8, 0.2, 0.03, 0.6])
        ax.set_title("Heatmap of Number of Alerts Filtered", fontsize=28)
        ax.set_xlabel("Filters Passed", fontsize=22)
        ax.set_ylabel("Night", fontsize=22)
        ax.tick_params(axis="x", labelsize=18)
        ax.tick_params(axis="y", labelsize=18)

    def show_heatmap(self):
        table = [
            self.num_filters_passed(x, night)
            for x, night in zip(self.dfs, self.nightnames)
        ]
        self.heatmap(table, self.nightnames)


class Sankey:
    def __init__(self, df, name):
        self.df = df
        self.name = name

    def get_flow_counts_for_sankey(self):
        rock = self.df[
            (self.df["ssdistnr"] >= 0)
            & (self.df["ssdistnr"] < 12)
            & (abs(self.df["ssmagnr"]) < 20)
        ]
        notrock = self.df[
            (self.df["ssdistnr"] < 0)
            | (self.df["ssdistnr"] > 12)
            | (abs(self.df["ssmagnr"]) > 20)
        ]
        # isdiffpos
        posdiff = self.df[self.df["isdiffpos"] == 1]
        negdiff = self.df[self.df["isdiffpos"] == 0]
        # real
        real = self.df[self.df["drb"] > 0.5]
        notreal = self.df[self.df["drb"] < 0.5]
        # filtered
        filtered = self.df[self.df["filtered_bool"] == 1]
        notfiltered = self.df[self.df["filtered_bool"] == 0]

        realpos = pd.merge(real, posdiff, how="inner")
        realneg = pd.merge(real, negdiff, how="inner")
        realposnotrock = pd.merge(realpos, notrock, how="inner")
        realposrock = pd.merge(realpos, rock, how="inner")
        realposnotrockfilt = pd.merge(realposnotrock, filtered, how="inner")
        realposnotrocknotfilt = pd.merge(realposnotrock, notfiltered, how="inner")
        notrealfilt = pd.merge(notreal, filtered, how="inner")
        notrealnotfilt = pd.merge(notreal, notfiltered, how="inner")
        realnegdifffilt = pd.merge(realneg, filtered, how="inner")
        realnegdiffnotfilt = pd.merge(realneg, notfiltered, how="inner")
        rockfilt = pd.merge(rock, filtered, how="inner")
        rocknotfilt = pd.merge(rock, notfiltered, how="inner")
        values = [
            len(real),
            len(notreal),
            len(realpos),
            len(realneg),
            len(realposnotrock),
            len(realposrock),
            len(realposnotrockfilt),
            len(realposnotrocknotfilt),
            len(notrealfilt),
            len(notrealnotfilt),
            len(realnegdifffilt),
            len(realnegdiffnotfilt),
            len(rockfilt),
            len(rocknotfilt),
        ]
        return values

    def get_dict_for_sankey(self, values):
        sources = [0, 0, 1, 1, 3, 3, 5, 5, 2, 2, 4, 4, 6, 6]
        targets = [1, 2, 3, 4, 5, 6, 7, 8, 7, 8, 7, 8, 7, 8]
        labels = [
            "Full Night",
            "real",
            "not real",
            "Positive subtraction",
            "Negative subtraction",
            "not rock",
            "rock",
            "filtered",
            "not filtered",
        ]

        stream_colors = [
            "rgba(31, 119, 180, 0.2)",
            "rgba(255, 127, 14, 0.2)",
            "rgba(44, 160, 44, 0.2)",
            "rgba(214, 39, 40, 0.2)",
            "rgba(148, 103, 189, 0.2)",
            "rgba(140, 86, 75, 0.2)",
            "rgba(227, 119, 194, 0.2)",
            "rgba(127, 127, 127, 0.2)",
            "rgba(188, 189, 34, 0.2)",
            "rgba(255, 255, 255, 0.0)",
            "rgba(31, 119, 180, 0.2)",
            "rgba(255, 255, 255, 0.0)",
            "rgba(44, 160, 44, 0.2)",
            "rgba(255, 255, 255, 0.0)",
        ]

        node_colors = [
            "rgba(31, 119, 180, 0.8)",  # blue
            "rgba(44, 160, 44, 0.8)",  # green
            "rgba(255, 69, 0, 0.8)",  # orange
            "rgba(44, 160, 44, 0.8)",
            "rgba(255, 69, 0, 0.8)",
            "rgba(44, 160, 44, 0.8)",
            "rgba(255, 69, 0, 0.8)",
            "rgba(44, 160, 44, 0.8)",
            "rgba(255, 69, 0, 0.8)",
            "rgba(255, 255, 255, 0.0)",
            "rgba(31, 119, 180, 0.8)",
            "rgba(255, 255, 255, 0.0)",
            "rgba(44, 160, 44, 0.8)",
            "rgba(255, 255, 255, 0.0)",
        ]

        data = {
            "data": [
                {
                    "type": "sankey",
                    "domain": {"x": [0, 1], "y": [0, 1]},
                    "orientation": "h",
                    "valueformat": ".0f",
                    "valuesuffix": " Alerts",
                    "node": None,
                    "link": None,
                }
            ]
        }

        data["data"][0]["node"] = {
            "pad": 15,
            "thickness": 15,
            "line": {"color": "black", "width": 0.5},
            "label": labels,
            "color": node_colors,
        }

        data["data"][0]["link"] = {
            "source": sources,
            "target": targets,
            "value": values,
            "color": stream_colors,
            "label": None,
        }

        return data

    def plot_sankey(self, data, night):
        fig = go.Figure(
            data=[
                go.Sankey(
                    valueformat=".0f",
                    valuesuffix=" Alerts",
                    # Define nodes
                    node=dict(
                        pad=15,
                        thickness=15,
                        line=dict(color="black", width=0.5),
                        label=data["data"][0]["node"]["label"],
                        color=data["data"][0]["node"]["color"],
                    ),
                    # Add links
                    link=dict(
                        source=data["data"][0]["link"]["source"],
                        target=data["data"][0]["link"]["target"],
                        value=data["data"][0]["link"]["value"],
                        label=data["data"][0]["link"]["label"],
                        color=data["data"][0]["link"]["color"],
                    ),
                )
            ]
        )

        fig.update_layout(
            title_text=f"Filtering of ZTF Data from {night}", font_size=10
        )
        fig.show()

    def show_sankey(self):
        values = self.get_flow_counts_for_sankey()
        data = self.get_dict_for_sankey(values)
        self.plot_sankey(data, self.name)


class ParallelBoxplots:
    def __init__(self, dfs, names, moon_years=[2023, 2024]):
        self.dfs = dfs
        self.names = names
        self.moon_years = moon_years
        self.moons = []
        for year in self.moon_years:
            self.moons += self.get_moons_in_year(year)
        self.stats = [self.compute_stats(df) for df in self.dfs]

    def get_moons_in_year(self, year):
        """Returns a list of the full and new moons in a year. The list contains tuples
        of either the form (DATE,'full') or the form (DATE,'new')"""
        moons = []
        date = ephem.Date(DATE(year, 1, 1))
        while date.datetime().year == year:
            date = ephem.next_full_moon(date)
            moons.append((date, "full"))
        date = ephem.Date(DATE(year, 1, 1))
        while date.datetime().year == year:
            date = ephem.next_new_moon(date)
            moons.append((date, "new"))
        moons.sort(key=lambda x: x[0])
        moons = [
            (round(Time(moon[0].datetime(), scale="utc").jd) - 2400000, moon[1])
            for moon in moons
        ]
        return moons

    def compute_stats(self, df):
        stats = {}
        for column in df.columns:
            if df[column].dtype in [
                np.float64,
                np.int64,
            ]:  # Ensure the column is numeric
                num_alerts = df[column].count()
                col_data = df[column]
                mean = col_data.replace(-999, np.nan).mean()
                std = col_data.replace(-999, np.nan).std()
                Q1 = np.percentile(col_data, 25)
                Q3 = np.percentile(col_data, 75)
                median = col_data.replace(-999, np.nan).median()
                min_val = col_data.replace(-999, np.nan).min()
                max_val = col_data.replace(-999, np.nan).max()
                count_neg_999 = (col_data == -999).sum()
                stats[column] = {
                    "num_alerts": num_alerts,
                    "mean": mean,
                    "std": std,
                    "Q1": Q1,
                    "Q3": Q3,
                    "median": median,
                    "min": min_val,
                    "max": max_val,
                    "error_values": count_neg_999,
                }
        return stats

    def show_boxplots(self, parameter, logarithmic=False):
        dates = [Time(date, format="iso").mjd for date in self.names]
        param_stats = [s[parameter] for s in self.stats]
        means = [stat["mean"] for stat in param_stats]
        medians = [stat["median"] for stat in param_stats]
        miny = [stat["min"] for stat in param_stats]
        maxy = [stat["max"] for stat in param_stats]
        Q1 = [stat["Q1"] for stat in param_stats]
        Q3 = [stat["Q3"] for stat in param_stats]

        full_moons = [
            moon[0]
            for moon in self.moons
            if moon[1] == "full" and moon[0] > min(dates) and moon[0] < max(dates)
        ]
        new_moons = [
            moon[0]
            for moon in self.moons
            if moon[1] == "new" and moon[0] > min(dates) and moon[0] < max(dates)
        ]
        ymoon = np.min(miny) - 0.05 * (np.max(maxy) - np.min(miny))

        minbar = np.array([a - b for a, b in zip(means, miny)])
        maxbar = np.array([a - b for a, b in zip(maxy, means)])

        minIQR = np.array([a - b for a, b in zip(medians, Q1)])
        maxIQR = np.array([a - b for a, b in zip(Q3, medians)])

        plt.figure(figsize=(15, 8))
        plt.errorbar(
            dates,
            means,
            color="black",
            label=None,
            yerr=[minbar, maxbar],
            linestyle="none",
            capsize=4,
            elinewidth=7,
            zorder=0,
        )
        plt.errorbar(
            dates,
            means,
            color="#b7c9e2",
            label="Mean",
            yerr=[minbar, maxbar],
            linestyle="none",
            capsize=3,
            elinewidth=6,
            zorder=1,
        )  # min and max
        plt.errorbar(
            dates,
            medians,
            color="#fdfdfe",
            label="Mean",
            yerr=[minIQR, maxIQR],
            linestyle="none",
            elinewidth=6,
            zorder=1,
        )  # IQR
        plt.scatter(dates, means, color="#3c4142", s=20, marker="o", zorder=2)  # means
        plt.scatter(
            dates, medians, color="#3c4142", s=20, marker="_", zorder=2
        )  # medians
        plt.scatter(
            full_moons,
            [ymoon] * len(full_moons),
            s=85,
            color="#758da3",
            marker="o",
            label="Full Moon",
            zorder=3,
        )
        plt.scatter(
            new_moons,
            [ymoon] * len(new_moons),
            s=85,
            facecolors="white",
            edgecolors="#758da3",
            marker="o",
            label="New Moon",
            zorder=3,
        )
        if logarithmic:
            plt.yscale("log")
        plt.tick_params(axis="both", which="major", labelsize=16)
        plt.xlabel("Date (MJD)", fontsize=20)
        plt.ylabel(parameter, fontsize=20)
        plt.title(f"Distribution of ZTF alert {parameter}", fontsize=22)
        legend_handles = [
            Line2D(
                [0],
                [0],
                marker="o",
                markersize=5,
                color="#3c4142",
                linestyle="None",
                label="Mean",
            ),
            Line2D(
                [0], [0], marker="o", markersize=0.01, color="#3c4142", label="Median"
            ),
            Line2D(
                [0],
                [0],
                color="#fdfdfe",
                linewidth=10,
                label="IQR",
                marker="s",
                markerfacecolor="#fdfdfe",
                markeredgecolor="black",
                markeredgewidth=1,
            ),
            Line2D(
                [0],
                [0],
                color="#758da3",
                marker="o",
                markersize=8,
                linestyle="None",
                label="Full Moon",
            ),
            Line2D(
                [0],
                [0],
                marker="o",
                markersize=8,
                markerfacecolor="white",
                markeredgecolor="#758da3",
                linestyle="None",
                label="New Moon",
            ),
        ]
        plt.legend(handles=legend_handles, fontsize=14, loc="lower right")
        plt.show()
