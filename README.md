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
   python -m scripts.simulation_runner --test --benchmark bwaves
   ```

4. **Collect data:**
   ```bash
   # Small test
   python -m scripts.simulation_runner --sweep --preset small_test --parallel 4
   
   # Full sweep
   python -m scripts.simulation_runner --sweep --strategy grid --parallel 4
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
- **CPU models:** Current configuration space limits sweeps to `DerivO3CPU`; adjust `config_space.json` if you need other models.

## Benchmark Commands

- Edit `benchmarks.commands` in `config.yaml` to tune the binary path, arguments, and `stdin` requirements for each SPEC workload.
- Options provided as YAML arrays become the exact command-line arguments passed to gem5; relative paths are resolved against `cpu2006/<benchmark>/`.
- Provide a `stdin` entry whenever a benchmark expects redirected input (for example, `gobmk`, `gamess`, `milc`, or `tonto`).
- If a workload writes outputs you want in the run directory, point the option or flag to `results/<run_id>/...` explicitly.
- Each configuration is now executed across *all* benchmarks before moving to the next configuration, so partial sweeps still give you coverage across the full benchmark suite.
- Successful runs append rows directly to `results/dataset.csv`; if Google Drive backup is enabled, the CSV is uploaded (or updated) after every append so the cloud copy stays in sync.

## Commands

```bash
# Test single simulation
python -m scripts.simulation_runner --test --benchmark bwaves

# Run parameter sweep
python -m scripts.simulation_runner --sweep --parallel 4

# Check status
python -m scripts.simulation_runner --status

# Analyze data
python -m scripts.analyze_data
```
