import pandas as pd
import matplotlib.pyplot as plt

# List of the CSV filenames for the machines
csv_files = {
    # "epycbox": "lookup_times_epycbox.csv",
    # "epycbox_v2": "lookup_times_epycbox_v2.csv", 
    # "opi5": "lookup_times_opi.csv",
    # "rpi5": "lookup_times_rpi.csv",
    # "epycbox_nvme": "lookup_times_epycbox_nvme.csv",
    # "opi5_nvme": "lookup_times_opi_nvme.csv",
    # "rpi5_nvme": "lookup_times_rpi_nvme.csv",
    # "epycbox_nvme1": "lookup_times_epycbox_nvme1.csv",
    # "epycbox_ssd_v2": "lookup_times_epycbox_ssd_v2.csv",
    # "epycbox_ssd": "lookup_times_epycbox_ssd.csv",
    "epycbox_nvme_v3": "lookup_times_epycbox_nvme_v3.csv",
}

# Define the colors for different hash sizes
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']

# Iterate over each machine and its corresponding CSV file
for machine, file in csv_files.items():
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)
    
    # Debug: Print unique Hash_Size values in the dataset
    print(f"Machine: {machine}, Unique Hash_Size: {sorted(df['Hash_Size'].unique())}")
    
    # Identify drive type based on filename
    if "nvme" in machine:
        drive_type = "NVMe Drives"
    elif "ssd" in machine:
        drive_type = "SSD Drives"
    else:
        drive_type = "HDD Drives"
    
    # Create a figure for each machine and drive type
    plt.figure(figsize=(12, 6))
    
    # Sort the Hash_Size values for consistent ordering
    hash_sizes = sorted(df['Hash_Size'].unique())
    
    # Plot the bar graph for each k-value and hash size
    for i, hash_size in enumerate(hash_sizes):
        data = df[df['Hash_Size'] == hash_size]
        
        # Safeguard against out-of-range color indices
        color_index = i % len(colors)
        
        # Offset bars sequentially
        offset = 0.1 * i
        
        plt.bar(data['K'] + offset, data['Average_Lookup_Time_ms'], width=0.1, 
                label=f'Hash Size {hash_size}', color=colors[color_index])
    
    # Set the title and labels
    plt.title(f"{machine.capitalize()} - {drive_type}", fontsize=14)
    plt.xlabel("K Value", fontsize=12)
    plt.ylabel("Average Lookup Time (ms)", fontsize=12)
    
    # Set x-axis ticks at every K value
    plt.xticks(range(25, 38))  # Ensure ticks are placed at each K value (25 through 40)
    
    # Add a legend
    plt.legend(title="Hash Size", bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout for better spacing
    plt.tight_layout()
    
    # Save the plot for the current machine and drive type
    plt.savefig(f"graph_{machine}_{drive_type}.svg")

