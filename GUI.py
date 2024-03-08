import tkinter as tk
from tkinter import ttk
from datetime import date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import seaborn as sns
import numpy as np
import pandas as pd

'''This is the basic code that I started for the UI design
It does not have any working feature, I will slowly start adding feature. 
Currently I am trying to build the basic GUI where I will continue adding other
modules as told by Ashish.'''

def plot_data():
    # Generate random data
    data = np.random.randn(100, 2)
    
    # Plotting histograms
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
    
    # Plotting corner plot
    ax3.clear()
    pair_plot = sns.pairplot(pd.DataFrame(data))
    pair_plot.fig.suptitle('Corner Plot', y=1.02)  # Set title
    pair_plot.fig.set_size_inches(6, 6)  # Adjust size

    canvas.draw()

# Creating main window
root = tk.Tk()
root.title("Dropdowns and Plots")

# Dropdowns
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

# Submit button
submit_button = ttk.Button(root, text="Submit", command=plot_data)
submit_button.grid(row=2, column=0, columnspan=2, padx=5, pady=10)

# Display today's date
today_date = date.today().strftime("%B %d, %Y")
date_label = ttk.Label(root, text="Today's Date: " + today_date)
date_label.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

# Matplotlib figure and canvas
fig, ((ax1, ax2), (ax3, _)) = plt.subplots(2, 2, figsize=(10, 6))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().grid(row=4, column=0, columnspan=2, padx=5, pady=5)

# Run the GUI
root.mainloop()
