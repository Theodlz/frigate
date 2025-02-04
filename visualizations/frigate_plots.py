import numpy as np
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import corner
import seaborn as sns


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

        for column in plot_features:
            smallest_non_negative = data[data[column] > 0][column].min()
            shift_value = smallest_non_negative * 0.99
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

