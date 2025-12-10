import subprocess
import json
import csv
import re
import os
import sys
import time
import argparse

# --- CONFIGURATION ---
OUTPUT_FILE = 'docker_stats.csv'

def parse_size(size_str):
    """Parses human-readable sizes (e.g., '10.5MiB', '2.3kB') to Megabytes (MB)."""
    size_str = size_str.strip()
    match = re.match(r"([0-9\.]+)([a-zA-Z]+)", size_str)
    if not match: return 0.0
    value, unit = match.groups()
    value = float(value)
    
    unit = unit.lower()
    if 'k' in unit: return value / 1024
    if 'm' in unit: return value
    if 'g' in unit: return value * 1024
    if 't' in unit: return value * 1024 * 1024
    if 'b' in unit and len(unit) == 1: return value / (1024*1024)
    return value

def parse_pair(pair_str):
    """Splits 'Input / Output' strings."""
    try:
        parts = pair_str.split(' / ')
        if len(parts) == 2:
            return parse_size(parts[0]), parse_size(parts[1])
    except:
        pass
    return 0.0, 0.0

def format_elapsed_time(seconds):
    """Format elapsed seconds as MM:SS."""
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"

def parse_duration(duration_str):
    """Parse duration string (e.g., '30s', '1m', '1h') to seconds.
    
    Args:
        duration_str: String like '30s', '1m', '1h', or a float number (treated as minutes)
    
    Returns:
        Duration in seconds, or None if invalid
    """
    if duration_str is None:
        return None
    
    # If it's already a number, treat as minutes (backward compatibility)
    if isinstance(duration_str, (int, float)):
        return float(duration_str) * 60
    
    # Parse string format
    duration_str = str(duration_str).strip().lower()
    
    # Extract number and unit
    match = re.match(r'^([\d.]+)([smh]?)$', duration_str)
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2) or 'm'  # Default to minutes if no unit
    
    if unit == 's':
        return value
    elif unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    else:
        return None

def format_duration(seconds):
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = seconds / 60
        if minutes == int(minutes):
            return f"{int(minutes)}m"
        else:
            return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        if hours == int(hours):
            return f"{int(hours)}h"
        else:
            return f"{hours:.1f}h"

def monitor_stream(duration_seconds=None):
    print(f"Starting measurement... saving to {OUTPUT_FILE}")
    
    # Check if DOCKER_HOST is set to ensure we are monitoring the right machine
    docker_host = os.environ.get('DOCKER_HOST')
    if docker_host:
        print(f"Target: {docker_host}")
    else:
        print("Target: Localhost (DOCKER_HOST not set)")
    
    if duration_seconds:
        duration_str = format_duration(duration_seconds)
        print(f"Duration: {duration_str} (Press Ctrl+C to stop early)")
    else:
        print("Press Ctrl+C to stop.")
    
    start_time = time.time()

    cmd = ['docker', 'stats', '--format', '{{json .}}']
    
    # Open the process
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1 # Line buffered
    )

    previous_stats = {}

    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = [
            'timestamp', 'absolute_time', 'container', 
            'cpu_percent', 'memory_mb', 
            'net_rx_mb_s', 'net_tx_mb_s',
            'disk_read_mb_s', 'disk_write_mb_s'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        csvfile.flush()

        try:
            for line in process.stdout:
                # Check if duration has elapsed
                if duration_seconds:
                    elapsed = time.time() - start_time
                    if elapsed >= duration_seconds:
                        duration_str = format_duration(duration_seconds)
                        print(f"\nDuration of {duration_str} reached. Stopping...")
                        break
                
                if not line.strip(): continue

                try:
                    # Clean up ANSI codes if any
                    clean_line = re.sub(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])', '', line)
                    c = json.loads(clean_line)
                except json.JSONDecodeError:
                    continue

                current_sys_time = time.time()
                elapsed_seconds = current_sys_time - start_time
                timestamp_str = format_elapsed_time(elapsed_seconds)
                absolute_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_sys_time))
                
                name = c.get('Name', 'unknown')

                # 1. Parse Cumulative Values
                net_rx_cum, net_tx_cum = parse_pair(c.get('NetIO', '0B / 0B'))
                blk_read_cum, blk_write_cum = parse_pair(c.get('BlockIO', '0B / 0B'))
                
                # 2. Calculate Throughput (Delta)
                net_rx_rate = 0.0
                net_tx_rate = 0.0
                disk_read_rate = 0.0
                disk_write_rate = 0.0
                
                if name in previous_stats:
                    prev = previous_stats[name]
                    time_delta = current_sys_time - prev['time']
                    
                    if time_delta > 0.1: 
                        net_rx_rate = (net_rx_cum - prev['net_rx']) / time_delta
                        net_tx_rate = (net_tx_cum - prev['net_tx']) / time_delta
                        disk_read_rate = (blk_read_cum - prev['blk_read']) / time_delta
                        disk_write_rate = (blk_write_cum - prev['blk_write']) / time_delta
                        
                        if net_rx_rate < 0: net_rx_rate = 0

                # 3. Update Previous Stats
                previous_stats[name] = {
                    'time': current_sys_time,
                    'net_rx': net_rx_cum, 'net_tx': net_tx_cum,
                    'blk_read': blk_read_cum, 'blk_write': blk_write_cum
                }

                # 4. Write & Flush
                writer.writerow({
                    'timestamp': timestamp_str,
                    'absolute_time': absolute_time_str,
                    'container': name,
                    'cpu_percent': float(c.get('CPUPerc', '0%').replace('%', '')),
                    'memory_mb': parse_size(c.get('MemUsage', '0B').split(' / ')[0]),
                    'net_rx_mb_s': round(net_rx_rate, 4),
                    'net_tx_mb_s': round(net_tx_rate, 4),
                    'disk_read_mb_s': round(disk_read_rate, 4),
                    'disk_write_mb_s': round(disk_write_rate, 4)
                })
                csvfile.flush()

        except KeyboardInterrupt:
            print("\nStopping...")
            process.terminate()
            process.wait()
            print("Done.")
        finally:
            if duration_seconds:
                elapsed = time.time() - start_time
                print(f"Measurement completed. Total duration: {format_elapsed_time(elapsed)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Monitor Docker container statistics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 monitor.py --duration 30s    # 30 seconds
  python3 monitor.py --duration 5m     # 5 minutes
  python3 monitor.py --duration 1h     # 1 hour
  python3 monitor.py --duration 1.5h   # 1.5 hours
  python3 monitor.py                   # Run until Ctrl+C
        """
    )
    parser.add_argument(
        '--duration',
        type=str,
        help='Duration in seconds (s), minutes (m), or hours (h). Examples: 30s, 5m, 1h. If not specified, runs until Ctrl+C.'
    )
    args = parser.parse_args()
    
    duration_seconds = None
    if args.duration:
        duration_seconds = parse_duration(args.duration)
        if duration_seconds is None:
            print(f"Error: Invalid duration format '{args.duration}'. Use format like '30s', '5m', or '1h'.")
            sys.exit(1)
    
    monitor_stream(duration_seconds=duration_seconds)