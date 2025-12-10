import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- CONFIGURATION ---
INPUT_FILE = 'docker_stats.csv'
OUTPUT_DIR = 'graphs'  # Folder to save images
sns.set_theme(style="whitegrid") # Academic/Clean style

def parse_elapsed_time(time_str):
    """Parse MM:SS format to total seconds."""
    try:
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes, seconds = int(parts[0]), int(parts[1])
            return minutes * 60 + seconds
        elif len(parts) == 3:
            # Handle HH:MM:SS format if needed
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
            return hours * 3600 + minutes * 60 + seconds
    except:
        pass
    return 0

def format_elapsed_time(seconds):
    """Format seconds as MM:SS for display."""
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}:{secs:02d}"

def generate_graphs():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"Loading data from {INPUT_FILE}...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except pd.errors.EmptyDataError:
        print("CSV is empty. Run the monitor first.")
        return

    # Convert timestamp column (MM:SS format) to elapsed seconds
    df['elapsed_seconds'] = df['timestamp'].apply(parse_elapsed_time)

    # Get unique containers
    containers = df['container'].unique()
    print(f"Found containers: {', '.join(containers)}")

    # Helper function to plot one metric
    def save_plot(metric_col, title, ylabel, filename):
        plt.figure(figsize=(12, 6))
        
        for container in containers:
            subset = df[df['container'] == container]
            
            # Optional: Add .rolling(3).mean() if data is too jittery
            plt.plot(subset['elapsed_seconds'], subset[metric_col], label=container, linewidth=1.5, alpha=0.9)
        
        # Styling
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.ylabel(ylabel, fontsize=12, fontweight='bold')
        plt.xlabel('Elapsed Time (minutes:seconds)', fontsize=12, fontweight='bold')
        
        # X-Axis formatting (elapsed time as M:SS or MM:SS)
        ax = plt.gca()
        
        # Get the range of elapsed seconds
        min_seconds = df['elapsed_seconds'].min()
        max_seconds = df['elapsed_seconds'].max()
        
        # Create tick positions (every 30 seconds or every minute depending on duration)
        duration = max_seconds - min_seconds
        if duration <= 60:
            # Short duration: every 10 seconds
            tick_interval = 10
        elif duration <= 300:  # 5 minutes
            # Medium duration: every 30 seconds
            tick_interval = 30
        else:
            # Long duration: every minute
            tick_interval = 60
        
        tick_positions = list(range(int(min_seconds), int(max_seconds) + 1, tick_interval))
        tick_labels = [format_elapsed_time(sec) for sec in tick_positions]
        
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=45)
        
        # Grid and Limits
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xlim(df['elapsed_seconds'].min(), df['elapsed_seconds'].max())
        
        # Move legend outside to prevent blocking data
        plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, title="Containers")
        
        # Save
        output_path = os.path.join(OUTPUT_DIR, filename)
        plt.tight_layout()
        plt.savefig(output_path, dpi=300)
        print(f"Saved: {output_path}")
        plt.close()

    # --- GENERATE PLOTS ---
    
    # 1. CPU
    save_plot('cpu_percent', 'CPU Usage Over Time', 'CPU (%)', '01_cpu_usage.png')
    
    # 2. Memory
    save_plot('memory_mb', 'Memory Usage Over Time', 'Memory (MB)', '02_memory_usage.png')
    
    # 3. Network Receive (Download)
    save_plot('net_rx_mb_s', 'Network Receive Rate', 'Throughput (MB/s)', '03_network_rx.png')
    
    # 4. Network Transmit (Upload)
    save_plot('net_tx_mb_s', 'Network Transmit Rate', 'Throughput (MB/s)', '04_network_tx.png')
    
    # 5. Disk Read
    save_plot('disk_read_mb_s', 'Disk Read Rate', 'Throughput (MB/s)', '05_disk_read.png')
    
    # 6. Disk Write
    save_plot('disk_write_mb_s', 'Disk Write Rate', 'Throughput (MB/s)', '06_disk_write.png')

    print("\nAll graphs generated successfully in the 'graphs' folder.")

if __name__ == "__main__":
    generate_graphs()