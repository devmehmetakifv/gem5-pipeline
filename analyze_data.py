#!/usr/bin/env python3
"""
Data Analysis Example
Quick script to analyze collected simulation data
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_dataset(csv_file: str = 'results/dataset.csv'):
    """Analyze the collected simulation dataset."""
    
    if not Path(csv_file).exists():
        print(f"❌ Dataset not found: {csv_file}")
        print("Run simulations first: python simulation_runner.py --sweep")
        return
    
    # Load data
    df = pd.read_csv(csv_file)
    
    print("\n" + "="*60)
    print("SIMULATION DATASET ANALYSIS")
    print("="*60)
    
    # Basic statistics
    print(f"\n📊 Dataset Overview:")
    print(f"   Total simulations: {len(df)}")
    print(f"   Successful runs: {len(df)}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Benchmarks
    print(f"\n🎯 Benchmarks:")
    benchmark_counts = df['benchmark'].value_counts()
    for bench, count in benchmark_counts.items():
        print(f"   {bench:20s}: {count:4d} configurations")
    
    # Parameters tested
    param_cols = [c for c in df.columns if c.startswith('param_')]
    print(f"\n⚙️  Parameters ({len(param_cols)} total):")
    for col in param_cols:
        unique_vals = df[col].nunique()
        print(f"   {col:40s}: {unique_vals:3d} unique values")
    
    # Metrics collected
    metric_cols = [c for c in df.columns if c.startswith('metric_')]
    print(f"\n📈 Metrics ({len(metric_cols)} total):")
    for col in metric_cols:
        metric_name = col.replace('metric_', '')
        mean_val = df[col].mean()
        std_val = df[col].std()
        print(f"   {metric_name:30s}: mean={mean_val:.4f}, std={std_val:.4f}")
    
    # Performance insights
    print(f"\n🚀 Performance Insights:")
    
    # Best IPC
    if 'metric_ipc' in df.columns:
        best_ipc = df.loc[df['metric_ipc'].idxmax()]
        print(f"   Best IPC: {best_ipc['metric_ipc']:.4f}")
        print(f"     Benchmark: {best_ipc['benchmark']}")
        if 'param_cpu.cpu_type' in df.columns:
            print(f"     CPU Type: {best_ipc['param_cpu.cpu_type']}")
    
    # Lowest cache miss rate
    if 'metric_l1d_miss_rate' in df.columns:
        best_cache = df.loc[df['metric_l1d_miss_rate'].idxmin()]
        print(f"\n   Lowest L1D miss rate: {best_cache['metric_l1d_miss_rate']:.4f}")
        print(f"     Benchmark: {best_cache['benchmark']}")
        if 'param_cache_l1d.size' in df.columns:
            print(f"     L1D Size: {best_cache['param_cache_l1d.size']}")
    
    # Data completeness
    print(f"\n✅ Data Completeness:")
    total_cells = len(df) * len(df.columns)
    missing_cells = df.isnull().sum().sum()
    completeness = 100 * (1 - missing_cells / total_cells)
    print(f"   {completeness:.2f}% complete ({missing_cells} missing values)")
    
    # Simulation time
    if 'duration' in df.columns:
        total_time = df['duration'].sum()
        avg_time = df['duration'].mean()
        print(f"\n⏱️  Simulation Time:")
        print(f"   Total: {total_time/3600:.2f} hours")
        print(f"   Average per run: {avg_time/60:.2f} minutes")
    
    # Storage
    file_size = Path(csv_file).stat().st_size
    print(f"\n💾 Storage:")
    print(f"   CSV file: {file_size / (1024*1024):.2f} MB")
    
    print("\n" + "="*60)
    print(f"✅ Dataset ready for ML training!")
    print(f"   Features (X): {len(param_cols)} parameters")
    print(f"   Targets (y): {len(metric_cols)} metrics")
    print(f"   Samples (n): {len(df)} configurations")
    print("="*60 + "\n")


def export_summary(csv_file: str = 'results/dataset.csv', output: str = 'results/summary.txt'):
    """Export summary statistics to file."""
    
    if not Path(csv_file).exists():
        return
    
    df = pd.read_csv(csv_file)
    
    with open(output, 'w') as f:
        f.write("SIMULATION DATASET SUMMARY\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Total simulations: {len(df)}\n")
        f.write(f"Benchmarks: {df['benchmark'].nunique()}\n")
        f.write(f"Parameters: {len([c for c in df.columns if c.startswith('param_')])}\n")
        f.write(f"Metrics: {len([c for c in df.columns if c.startswith('metric_')])}\n\n")
        
        # Statistical summary
        f.write("METRIC STATISTICS\n")
        f.write("-"*60 + "\n")
        metric_cols = [c for c in df.columns if c.startswith('metric_')]
        summary = df[metric_cols].describe()
        f.write(summary.to_string())
        f.write("\n\n")
        
        # Benchmark breakdown
        f.write("BENCHMARK BREAKDOWN\n")
        f.write("-"*60 + "\n")
        f.write(df['benchmark'].value_counts().to_string())
        f.write("\n")
    
    print(f"✅ Summary exported to: {output}")


if __name__ == "__main__":
    dataset_file = 'results/dataset.csv'
    
    if len(sys.argv) > 1:
        dataset_file = sys.argv[1]
    
    analyze_dataset(dataset_file)
    export_summary(dataset_file)
