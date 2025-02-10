import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


class TsnePlotter:
    def __init__(self, df):
        self.df = df

    def plot_filtered(self):
        df = self.df.copy()
        # shift around df to plot filtered on top of unfiltered
        tsne_filtered = df[df["filtered_bool"] == 1]
        tsne_unfiltered = df[df["filtered_bool"] == 0]
        tsne_plotordered = pd.concat([tsne_unfiltered, tsne_filtered])

        custom_palette = ["#d8dcd6", "#3357FF"]
        plt.figure(figsize=(16, 10))
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            hue="filtered_bool",
            palette=custom_palette,
            data=tsne_plotordered,
            legend="full",
            alpha=0.7,
        )
        plt.legend(title="Filtered", title_fontsize="18", fontsize="15")
        plt.xlabel("t-SNE Dimension 1", fontsize=18)
        plt.ylabel("t-SNE Dimension 2", fontsize=18)
        plt.title("t-SNE Plot of ZTF Example Night", fontsize=25)
        plt.show()


class TsnePlotter_featurecolor:
    def __init__(self, df):
        self.df = df

    def multiple_columns_hue(self, df, multiple_columns):
        """
        if we want to handle multiple columns as hue
        ie for multiple crossmatched catalogs
        """

        def get_hue_value(row):
            hue_list = [
                col.replace("_classification", "")
                for col in multiple_columns
                if pd.notna(row[col])
            ]
            return ", ".join(hue_list) if hue_list else None

        df["multiple_columns_hue"] = df.apply(get_hue_value, axis=1)
        return df

    def simplify_fritz_classifications(self, df):
        """
        Condense the fritz classifications into broader categories.
        """

        def group_classes(value):
            if pd.isna(value):
                return value
            if "SN" in value:
                return "SN"
            elif any(
                substring in value
                for substring in [
                    "Stellar variable",
                    "YSO",
                    "FU Ori",
                    "LPV",
                    "Cataclysmic",
                    "AM CVn",
                    "RR Lyrae",
                    "S Doradus",
                    "Eclipsing",
                    "Pulsar",
                    "Polars",
                ]
            ):
                return "Stellar"
            elif any(
                substring in value
                for substring in ["AGN", "BL Lac", "Seyfert", "QSO", "Blazar"]
            ):
                return "Galactic Nuclei"
            else:
                return value

        df["simplified_fritz_classification"] = df[
            "fritz_catalog_classification"
        ].apply(group_classes)
        return df

    def custom_reorder(self, df, reorder_list, column_name):
        unique_values = df[column_name].unique()
        all_categories = list(reorder_list) + [
            val for val in unique_values if val not in reorder_list
        ]
        cat_type = pd.CategoricalDtype(categories=all_categories, ordered=True)
        df[column_name] = df[column_name].astype(cat_type)
        df = df.sort_values(by=column_name)
        return df

    def plot_parameter_analysis(
        self,
        parameter,
        reorder=True,
        ascending=False,
        remove_error_values=True,
        use_colorbar=True,
        simplify_fritz=False,
    ):
        df = self.df.copy()

        if remove_error_values:
            if isinstance(parameter, list):
                for col in parameter:
                    df = df[df[col] > -600]
            else:
                df = df[df[parameter] > -600]

        if simplify_fritz:
            df = self.simplify_fritz_classifications(df)
            parameter = "simplified_fritz_classification"

        # assign the way we will color code the plot
        if isinstance(parameter, list):
            df = self.multiple_columns_hue(df, parameter)
            parameter = "multiple_columns_hue"

        if use_colorbar:
            if reorder:
                df = df.sort_values(by=parameter, ascending=ascending)
            assign_legend = False
            assign_palette = "viridis"

        else:
            if isinstance(reorder, list):
                df = self.custom_reorder(df, reorder, parameter)

            elif reorder:
                plot_top = df[df[parameter].notna()]
                plot_under = df[df[parameter].isna()]
                df = pd.concat([plot_under, plot_top])
            if df[parameter].isna().any():
                df[parameter] = df[parameter].fillna("None")
            assign_legend = "full"
            base_palette = [
                "#d8dcd6",
                "#FF5733",
                "#33FF57",
                "#3357FF",
                "#FF5733",
                "#FF33F6",
                "#FF5733",
                "#FF5733",
                "#33FFF6",
            ]
            unique_values = df[parameter].nunique()
            assign_palette = base_palette[:unique_values]

        plt.figure(figsize=(16, 10))
        ax = plt.gca()  # Get the current axes
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            hue=parameter,
            palette=assign_palette,
            data=df,
            legend=assign_legend,
            alpha=0.7,
            ax=ax,
        )
        if use_colorbar:
            norm = plt.Normalize(df[parameter].min(), df[parameter].max())
            sm = plt.cm.ScalarMappable(cmap="viridis", norm=norm)
            sm.set_array([])
            cbar = plt.colorbar(sm, ax=ax)
            cbar.set_label(f"{parameter}", fontsize=18)
        elif isinstance(parameter, list):
            plt.legend(title="Classification", title_fontsize="18", fontsize="15")
        else:
            plt.legend(
                title=parameter, title_fontsize="18", fontsize="15", loc="lower right"
            )

        plt.xlabel("t-SNE Dimension 1", fontsize=18)
        plt.ylabel("t-SNE Dimension 2", fontsize=18)
        plt.title("t-SNE Plot for Example Night", fontsize=25)
        plt.show()


class TsnePlotter_density:
    def __init__(self, df):
        self.df = df

    def plot_filtered_density(self):
        df = self.df.copy()

        plt.figure(figsize=(16, 10))
        palette = sns.color_palette("hls", 2)
        # Plot the unfiltered alerts as a density
        sns.kdeplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            data=df[df["filtered_bool"] == 0],
            fill=True,
            alpha=0.7,
            label="Density",
            color=palette[0],
        )

        # Plot the filtered alerts as points
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            data=df[df["filtered_bool"] == 1],
            color=palette[1],
            label="Points",
            alpha=0.7,
        )

        plt.legend(title="Filtered", title_fontsize="18", fontsize="15")
        plt.xlabel("t-SNE Dimension 1", fontsize=18)
        plt.ylabel("t-SNE Dimension 2", fontsize=18)
        plt.title("t-SNE Plot from ZTF Example Night", fontsize=25)
        plt.show()


class TsnePlotter_simbad:
    def __init__(self, df):
        self.df = df

    def simplify_simbad_classes(self, df, mapping_dict):
        """
        parse the simbad classifications to a reasonable extent
        """

        def map_value(value):
            for key, mapped_value in mapping_dict.items():
                if key in value:
                    return mapped_value
            return value

        df["simbad_simplified"] = df["simbad_classification"].apply(map_value)
        return df

    def plot_simbad_analysis(
        self,
        parameter,
        reorder=True,
        mapping_dict="Default",
    ):
        df = self.df.copy()

        if reorder:
            plot_top = df[df[parameter].notna()]
            plot_under = df[df[parameter].isna()]
            df = pd.concat([plot_under, plot_top])
        if df[parameter].isna().any():
            df[parameter] = df[parameter].fillna("Not filtered")

        if mapping_dict == "Default":
            mapping_dict = {
                "[]": "filtered no class",
                "Radio": "Radio Source",
                "X-ray": "X-ray Source",
                "Transient Event": "Transient",
                "Binary": "Binary",
                "Variable": "Variable",
                "alaxy": "Galaxy",
                "Star": "Stellar",
                "Mira": "Stellar",
                "BY Dra": "Stellar",
                "White Dwarf": "Stellar",
                "Blazar": "Nuclear",
                "Quasar": "Nuclear",
                "BL Lac": "Nuclear",
                "Active Galaxy Nucleus": "Nuclear",
                "Classical Nova": "Classical Nova",
                "Blue Object": "filtered no class",
                "Object of Unknown Nature": "filtered no class",
                "SuperNova": "SuperNova",
            }

        if mapping_dict:
            df = self.simplify_simbad_classes(df, mapping_dict)
            parameter = "simbad_simplified"

        assign_legend = "full"
        base_palette = [
            "#d8dcd6",
            "#7e1e9c",
            "#15b01a",
            "#0343df",
            "#ff81c0",
            "#653700",
            "#e50000",
            "#95d0fc",
            "#f97306",
            "#96f97b",
            "#ffff14",
            "#00ffff",
            "#fea993",
            "#06470c",
            "#01153e",
        ]
        unique_values = df[parameter].nunique()
        assign_palette = base_palette[:unique_values]

        plt.figure(figsize=(16, 10))
        ax = plt.gca()  # Get the current axes
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            hue=parameter,
            palette=assign_palette,
            data=df,
            legend=assign_legend,
            alpha=0.7,
            ax=ax,
        )

        plt.legend(
            title="Simbad Class",
            title_fontsize="18",
            fontsize="15",
            loc="center left",
            bbox_to_anchor=(1, 0.5),
        )
        plt.xlabel("t-SNE Dimension 1", fontsize=18)
        plt.ylabel("t-SNE Dimension 2", fontsize=18)
        plt.title("t-SNE Plot for Example Night", fontsize=25)
        plt.show()


class Tsne_subset:
    def __init__(self, df):
        self.df = df

    def get_circled_alerts(self, center, radius):
        df = self.df.copy()
        x_values = df["tsne-2d-one"]
        y_values = df["tsne-2d-two"]
        x, y = center[0], center[1]
        distances = np.sqrt((x_values - x) ** 2 + (y_values - y) ** 2)
        indices = np.where(distances < radius)
        subset = df.iloc[indices]
        print(f"Number of alerts selected: {len(subset)}")
        return subset

    def plot_selection(
        self,
        center,
        radius,
        filtered_only=True,
    ):
        df = self.df.copy()

        if filtered_only:
            df = df[df["filtered_bool"] == 1]
            custom_palette = ["#3357FF"]
        else:
            # shift around df to plot filtered on top of unfiltered
            tsne_filtered = df[df["filtered_bool"] == 1]
            tsne_unfiltered = df[df["filtered_bool"] == 0]
            df = pd.concat([tsne_unfiltered, tsne_filtered])
            custom_palette = ["#d8dcd6", "#3357FF"]

        plt.figure(figsize=(16, 10))
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            hue="filtered_bool",
            palette=custom_palette,
            data=df,
            legend="full",
            alpha=0.7,
        )

        # Plot a circle
        circle = plt.Circle(center, radius, color="red", fill=False, linewidth=2)
        plt.gca().add_patch(circle)

        plt.legend(title="Filtered", title_fontsize="18", fontsize="15")
        plt.xlabel("t-SNE Dimension 1", fontsize=18)
        plt.ylabel("t-SNE Dimension 2", fontsize=18)
        plt.title("t-SNE Plot from ZTF Example Night", fontsize=25)
