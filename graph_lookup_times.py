import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Define directories and files
folders = ['hdd_lookup', 'nvme_lookup']
file_range_hdd = range(25, 36)  # For HDD, lookup_times#.csv where # ranges from 25 to 35
file_range_nvme = range(25, 36)  # For NVMe, lookup_times#.csv where # ranges from 25 to 35
file_range_hdd_extra = [40]  # Only for HDD, we need to include file 40
hash_sizes = [3, 4, 5, 6, 7, 8, 16, 32]

# Dictionary to hold all data
data = {folder: {} for folder in folders}

# Read data from CSV files
for folder in folders:
    # Determine correct file range based on the folder
    if folder == 'hdd_lookup':
        file_range = list(file_range_hdd) + file_range_hdd_extra  # Include file 40 for HDD
    else:
        file_range = file_range_nvme  # Use 25-35 for NVMe
    
    for i in file_range:
        file_path = os.path.join(folder, f"lookup_times{i}.csv")
        if not os.path.exists(file_path):  # Skip if file doesn't exist
            print(f"File {file_path} not found. Skipping.")
            continue  # Skip to the next file
        
        try:
            # Read CSV without headers
            df = pd.read_csv(file_path, header=None)
            df.columns = ['hash_size', 'lookup_time']  # Assign column names explicitly
            
            # Convert lookup time from seconds to milliseconds
            df['lookup_time'] = df['lookup_time'] * 1000  # Conversion to ms
            
            # Initialize a dictionary for this file if not already done
            data[folder][i] = {size: [] for size in hash_sizes}
            
            for size in hash_sizes:
                # Filter rows for the current hash size
                size_data = df[df['hash_size'] == size]['lookup_time']
                data[folder][i][size].extend(size_data)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

# Plot the data
for folder in folders:
    plt.figure(figsize=(14, 8))
    
    # Determine correct file range based on the folder
    if folder == 'hdd_lookup':
        file_range = list(file_range_hdd) + file_range_hdd_extra  # Include file 40 for HDD
    else:
        file_range = file_range_nvme  # Use 25-35 for NVMe
    
    # Prepare data for bar chart
    bar_width = 0.1  # Width of each bar
    x_positions = np.arange(len(file_range))  # Base X positions for file sizes
    offsets = np.linspace(-0.35, 0.35, len(hash_sizes))  # Bar offsets for hash sizes
    
    for offset, size in zip(offsets, hash_sizes):
        avg_lookup_times = [
            sum(data[folder][file_id][size]) / len(data[folder][file_id][size]) if data[folder][file_id][size] else 0
            for file_id in file_range
        ]
        plt.bar(x_positions + offset, avg_lookup_times, bar_width, label=f'Hash Size {size}')
    
    # Customize the plot
    plt.title(f"Lookup Times by File Size and Hash Size ({folder})", fontsize=16)
    plt.xlabel("File Size (25-35, 40 for HDD)", fontsize=14)  # Updated file size range on X-axis
    plt.ylabel("Average Lookup Time (ms)", fontsize=14)
    plt.xticks(x_positions, [str(i) for i in file_range], fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(fontsize=10, title="Hash Sizes")
    plt.tight_layout()

    # Save and show the plot
    plt.savefig(f"{folder}_lookup_times_bar_graph.png")
    plt.show()
