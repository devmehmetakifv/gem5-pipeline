# Gem5 Data Collection Pipeline

Automated gem5 simulation pipeline for collecting training data.

## Quick Setup

1. **Install dependencies:**
   ```bash
   bash setup.sh
   ```

2. **Configure paths** in `config.yaml`:
   ```yaml
   gem5:
     installation_path: "/path/to/cloud/gem5"  # Cloud machine gem5
   
   benchmarks:
     cpu2006_path: "./cpu2006"                 # Local benchmarks
   ```

3. **Test:**
   ```bash
   source venv/bin/activate
   python simulation_runner.py --test --benchmark bwaves
   ```

4. **Collect data:**
   ```bash
   # Small test
   python simulation_runner.py --sweep --preset small_test --parallel 4
   
   # Full sweep
   python simulation_runner.py --sweep --strategy grid --parallel 4
   ```

## Output

- `results/dataset.csv` - Your training data (parameters → metrics)
- `results/dataset.json` - JSON format
- `results/<run_dirs>/` - Individual simulation results

## Path Setup

- **Cloud gem5:** `/path/to/cloud/gem5/build/X86/gem5.opt` (binary)
- **Cloud configs:** `/path/to/cloud/gem5/configs/tbtk/*.py` (simulation configs)
- **Local CPU2006:** `./cpu2006/` (benchmarks in this project)
- **Results:** `./results/` (saved locally)

## Benchmark Commands

- Edit `benchmarks.commands` in `config.yaml` to tune the binary path, arguments, and `stdin` requirements for each SPEC workload.
- Options provided as YAML arrays become the exact command-line arguments passed to gem5; relative paths are resolved against `cpu2006/<benchmark>/`.
- Provide a `stdin` entry whenever a benchmark expects redirected input (for example, `gobmk`, `gamess`, `milc`, or `tonto`).
- If a workload writes outputs you want in the run directory, point the option or flag to `results/<run_id>/...` explicitly.

## Commands

```bash
# Test single simulation
python simulation_runner.py --test --benchmark bwaves

# Run parameter sweep
python simulation_runner.py --sweep --parallel 4

# Check status
python simulation_runner.py --status

# Analyze data
python analyze_data.py
```
