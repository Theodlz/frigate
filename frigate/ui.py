import tkinter as tk
from tkinter import ttk
from datetime import date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import numpy as np
import pandas as pd

def plot_histograms(ax1, ax2):
    """
    Plot histograms on the given axes.

    Parameters:
        ax1 (matplotlib.axes.Axes): The first axes object.
        ax2 (matplotlib.axes.Axes): The second axes object.
    """
    data = np.random.randn(100, 2)
    
    ax1.clear()
    ax1.hist(data[:, 0], color='skyblue', bins=10)
    ax1.set_title('Histogram 1')
    ax1.set_xlabel('Values')
    ax1.set_ylabel('Frequency')
    ax1.grid(True)

    ax2.clear()
    ax2.hist(data[:, 1], color='salmon', bins=10)
    ax2.set_title('Histogram 2')
    ax2.set_xlabel('Values')
    ax2.set_ylabel('Frequency')
    ax2.grid(True)

def plot_corner_plot(ax3):
    """
    Plot a corner plot on the given axes.

    Parameters:
        ax3 (matplotlib.axes.Axes): The axes object.
    """
    data = np.random.randn(100, 2)
    
    ax3.clear()
    pair_plot = sns.pairplot(pd.DataFrame(data))
    pair_plot.fig.suptitle('Corner Plot', y=1.02)
    pair_plot.fig.set_size_inches(6, 6)

def plot_data(ax1, ax2, ax3, canvas):
    """
    Plot data including histograms and corner plot.

    Parameters:
        ax1 (matplotlib.axes.Axes): The first axes object.
        ax2 (matplotlib.axes.Axes): The second axes object.
        ax3 (matplotlib.axes.Axes): The third axes object.
        canvas (FigureCanvasTkAgg): The canvas object.
    """
    plot_histograms(ax1, ax2)
    plot_corner_plot(ax3)
    canvas.draw()

def main():
    """
    Main function to create GUI and run the application.
    """
    root = tk.Tk()
    root.title("Dropdowns and Plots")

    options = ['Option 1', 'Option 2', 'Option 3']
    dropdown1_label = ttk.Label(root, text="Dropdown 1:")
    dropdown1_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    dropdown1 = ttk.Combobox(root, values=options)
    dropdown1.grid(row=0, column=1, padx=5, pady=5)
    dropdown1.current(0)

    dropdown2_label = ttk.Label(root, text="Dropdown 2:")
    dropdown2_label.grid(row=1, column=0, padx=5, pady=5, sticky="w")
    dropdown2 = ttk.Combobox(root, values=options)
    dropdown2.grid(row=1, column=1, padx=5, pady=5)
    dropdown2.current(0)

    fig, ((ax1, ax2), (ax3, _)) = plt.subplots(2, 2, figsize=(10, 6), dpi=100)
    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.get_tk_widget().grid(row=4, column=0, columnspan=2, padx=5, pady=5, sticky="nsew")
    root.grid_rowconfigure(4, weight=1)
    root.grid_columnconfigure(0, weight=1)

    # Add a vertical scrollbar
    scrollbar = ttk.Scrollbar(root, command=canvas.get_tk_widget().yview)
    scrollbar.grid(row=4, column=2, sticky="ns")
    canvas.get_tk_widget().configure(yscrollcommand=scrollbar.set)

    submit_button = ttk.Button(root, text="Submit", command=lambda: plot_data(ax1, ax2, ax3, canvas))
    submit_button.grid(row=2, column=0, columnspan=2, padx=5, pady=10)

    today_date = date.today().strftime("%B %d, %Y")
    date_label = ttk.Label(root, text="Today's Date: " + today_date)
    date_label.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

    # Make the application responsive
    root.grid_rowconfigure(0, weight=0)
    root.grid_rowconfigure(1, weight=0)
    root.grid_rowconfigure(2, weight=0)
    root.grid_rowconfigure(3, weight=0)
    root.grid_rowconfigure(4, weight=1)

    root.grid_columnconfigure(0, weight=1)
    root.grid_columnconfigure(1, weight=1)

    root.mainloop()

if __name__ == "__main__":
    main()
