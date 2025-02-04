import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


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

    def plot_parameter_analysis(
        self, parameter, reorder=True, ascending=False, remove_error_values=True
    ):
        df = self.df.copy()

        # reorder so highest parameter value plots on top
        if reorder:
            df = df.sort_values(by=parameter, ascending=ascending)

        if remove_error_values:
            df = df[df[parameter] > -600]

        plt.figure(figsize=(16, 10))
        ax = plt.gca()  # Get the current axes
        sns.scatterplot(
            x="tsne-2d-one",
            y="tsne-2d-two",
            hue=parameter,
            palette="viridis",
            data=df,
            legend=False,
            alpha=0.7,
            ax=ax,
        )
        # Add a colorbar
        norm = plt.Normalize(df[parameter].min(), df[parameter].max())
        sm = plt.cm.ScalarMappable(cmap="viridis", norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax)
        cbar.set_label(f"{parameter}", fontsize=18)

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

