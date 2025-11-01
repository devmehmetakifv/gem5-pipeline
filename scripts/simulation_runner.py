#!/usr/bin/env python3
"""
Gem5 Simulation Runner
Main orchestrator for running gem5 simulations with different configurations
"""

import os
import sys
import json
import yaml
import subprocess
import shutil
import logging
import signal
import argparse
import shlex
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv

import pandas as pd
from tqdm import tqdm
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler

if __package__ is None or __package__ == "":
    import sys as _sys

    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in _sys.path:
        _sys.path.insert(0, str(project_root))

    from scripts.config_manager import ConfigurationManager  # type: ignore
    from scripts.results_parser import Gem5StatsParser  # type: ignore
    from scripts.gdrive_backup import GoogleDriveBackup  # type: ignore
else:
    from .config_manager import ConfigurationManager
    from .results_parser import Gem5StatsParser
    from .gdrive_backup import GoogleDriveBackup

# Default SPEC CPU2006 style commands (can be overridden in config.yaml)
DEFAULT_BENCHMARK_COMMANDS: Dict[str, Dict[str, Any]] = {
    'astar': {
        'binary': 'astar/astar',
        'options': ['rivers.cfg', 'rivers.bin'],
    },
    'bwaves': {
        'binary': 'bwaves/bwaves',
        'options': ['bwaves.in'],
    },
    'bzip2': {
        'binary': 'bzip2/bzip2',
        'options': ['input.source', '280'],
    },
    'calculix': {
        'binary': 'calculix/calculix',
        'options': ['-i', 'hyperviscoplastic'],
    },
    'gamess': {
        'binary': 'gamess/gamess',
        'stdin': 'gamess/cytosine.2.config',
    },
    'gems': {
        'binary': 'gems/gems',
        'stdin': 'gems/ref.in',
    },
    'gobmk': {
        'binary': 'gobmk/gobmk',
        'options': ['--quiet', '--mode', 'gtp'],
        'stdin': 'gobmk/13x13.tst',
    },
    'gromacs': {
        'binary': 'gromacs/gromacs',
        'options': ['-silent', '-deffnm', 'gromacs'],
    },
    'h264': {
        'binary': 'h264/h264',
        'options': ['-d', 'foreman_ref_encoder_baseline.cfg'],
    },
    'hmmer': {
        'binary': 'hmmer/hmmer',
        'options': ['nph3.hmm', 'swiss41'],
    },
    'lbm': {
        'binary': 'lbm/lbm',
        'options': ['3000', 'reference.dat', '0', '0', '100_100_130_ldc.of'],
    },
    'leslie3d': {
        'binary': 'leslie3d/leslie3d',
        'options': ['leslie3d.in'],
    },
    'libquantum': {
        'binary': 'libquantum/libquantum',
        'options': ['1397', '8'],
    },
    'mcf': {
        'binary': 'mcf/mcf',
        'options': ['inp.in'],
    },
    'milc': {
        'binary': 'milc/milc',
        'stdin': 'milc/su3imp.in',
    },
    'namd': {
        'binary': 'namd/namd',
        'options': ['--input', 'namd.input', '--iterations', '38', '--output', 'namd.out'],
    },
    'omnetpp': {
        'binary': 'omnetpp/omnetpp',
        'options': ['omnetpp.ini'],
    },
    'povray': {
        'binary': 'povray/povray',
        'options': ['SPEC-benchmark-ref.ini'],
    },
    'sjeng': {
        'binary': 'sjeng/sjeng',
        'options': ['ref.txt'],
    },
    'soplex': {
        'binary': 'soplex/soplex',
        'options': ['-s1', '-e', '-m45000', 'pds-50.mps'],
    },
    'tonto': {
        'binary': 'tonto/tonto',
        'stdin': 'tonto/stdin',
    },
    'xalanc': {
        'binary': 'xalanc/xalanc',
        'options': ['t5.xml', 'xalanc.xsl'],
    },
    'zeusmp': {
        'binary': 'zeusmp/zeusmp',
    },
}

# Setup logging
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)


class SimulationRunner:
    """Orchestrate gem5 simulation runs with automated data collection."""
    
    def __init__(self, config_file: str = 'config.yaml'):
        """
        Initialize simulation runner.
        
        Args:
            config_file: Path to configuration YAML file
        """
        self.config_file = Path(config_file)
        self.project_root = self.config_file.parent.resolve()
        self.config = self._load_config()
        
        # Setup paths
        self.gem5_root = Path(self.config['gem5']['installation_path']).expanduser().resolve()
        self.gem5_binary = self.gem5_root / self.config['gem5']['binary']
        self.configs_dir = self.gem5_root / self.config['gem5']['configs_dir']
        
        # CPU2006 path - support relative paths (relative to this script's directory)
        cpu2006_path = self.config['benchmarks']['cpu2006_path']
        if not Path(cpu2006_path).is_absolute():
            # Relative to config file or script directory
            self.cpu2006_path = (self.config_file.parent / cpu2006_path).resolve()
        else:
            self.cpu2006_path = Path(cpu2006_path)
        
        self.results_dir = self._resolve_project_path(self.config['output']['results_dir'])
        self.backup_dir = self._resolve_project_path(self.config['output']['backup_dir'])
        
        # Create output directories
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self._resolve_project_path('logs')
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.config_manager = ConfigurationManager(Path('config_space.json'))
        self.stats_parser = Gem5StatsParser()
        
        # Google Drive backup (optional)
        self.gdrive_backup = None
        if self.config['google_drive']['enabled']:
            try:
                self.gdrive_backup = GoogleDriveBackup(
                    credentials_file=self.config['google_drive']['credentials_file'],
                    token_file=self.config['google_drive']['token_file'],
                    folder_id=self.config['google_drive'].get('folder_id')
                )
                logger.info("Google Drive backup enabled")
            except Exception as e:
                logger.warning(f"Google Drive backup disabled: {e}")
        
        # Run tracking
        self.run_log_file = self.results_dir / 'run_log.json'
        self.dataset_file = self.results_dir / 'dataset.csv'
        self.dataset_drive_id_file = self.results_dir / '.dataset_drive_id'
        self.run_log = self._load_run_log()
        self.session_successful_runs = 0
        self.session_failed_runs = 0
        self.dataset_total_rows = self._count_existing_dataset_rows()
        self.dataset_drive_file_id = self._load_dataset_drive_file_id()
        
        # Validate setup
        self._validate_setup()
        self.benchmark_commands = self._load_benchmark_commands()
    
    def _resolve_project_path(self, path_value: str) -> Path:
        """Resolve a path relative to the project root."""
        path_obj = Path(path_value)
        if not path_obj.is_absolute():
            path_obj = (self.project_root / path_obj).resolve()
        else:
            path_obj = path_obj.expanduser().resolve()
        return path_obj
    
    def _resolve_benchmark_path(self, path_value: str) -> Path:
        """Resolve a benchmark-relative path inside cpu2006 directory."""
        path_obj = Path(path_value)
        if not path_obj.is_absolute():
            path_obj = (self.cpu2006_path / path_obj).resolve()
        else:
            path_obj = path_obj.expanduser().resolve()
        return path_obj
    
    def _count_existing_dataset_rows(self) -> int:
        """Count existing rows in dataset.csv (excluding header)."""
        if not self.dataset_file.exists():
            return 0
        try:
            with open(self.dataset_file, 'r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                # Skip header if present
                header = next(reader, None)
                if header is None:
                    return 0
                return sum(1 for _ in reader)
        except Exception as e:
            logger.warning(f"Unable to count existing dataset rows: {e}")
            return 0
    
    def _load_dataset_drive_file_id(self) -> Optional[str]:
        """Load stored Google Drive file ID for dataset."""
        if self.dataset_drive_id_file.exists():
            try:
                return self.dataset_drive_id_file.read_text().strip() or None
            except Exception as e:
                logger.warning(f"Failed to read dataset drive ID: {e}")
        return None
    
    def _persist_dataset_drive_file_id(self, file_id: str):
        """Persist Google Drive dataset file ID to disk."""
        try:
            self.dataset_drive_id_file.write_text(file_id)
        except Exception as e:
            logger.warning(f"Failed to persist dataset drive ID: {e}")
    
    def _flatten_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten result dict into a single row of scalar values."""
        row = {
            'run_id': result['run_id'],
            'benchmark': result['benchmark'],
            'timestamp': result['timestamp'],
            'duration': result.get('duration')
        }
        for key, value in result.get('config', {}).items():
            row[f'param_{key}'] = value
        for key, value in result.get('metrics', {}).items():
            row[f'metric_{key}'] = value
        return row
    
    def _append_to_dataset_row(self, result: Dict[str, Any]):
        """Append a successful run to dataset.csv."""
        if not result.get('metrics'):
            logger.warning(f"No metrics parsed for {result['run_id']}; skipping dataset append.")
            return
        
        row = self._flatten_result(result)
        df = pd.DataFrame([row])
        header = not self.dataset_file.exists()
        try:
            df.to_csv(self.dataset_file, mode='a', header=header, index=False)
            self.dataset_total_rows += 1
            logger.info(f"Appended run #{self.dataset_total_rows} to dataset.csv ({result['run_id']})")
        except Exception as e:
            logger.error(f"Failed to append dataset row for {result['run_id']}: {e}")
    
    def _handle_success_result(self, result: Dict[str, Any]):
        """Process bookkeeping for a successful run."""
        self.session_successful_runs += 1
        self._append_to_dataset_row(result)
        self._sync_dataset_to_drive()
    
    def _sync_dataset_to_drive(self):
        """Upload or update dataset.csv on Google Drive."""
        if not self.gdrive_backup:
            return
        
        if not self.dataset_file.exists():
            return
        
        try:
            new_file_id = self.gdrive_backup.upload_or_update_file(
                self.dataset_file,
                folder_id=self.gdrive_backup.folder_id,
                filename=self.dataset_file.name,
                existing_file_id=self.dataset_drive_file_id
            )
            if new_file_id:
                if new_file_id != self.dataset_drive_file_id:
                    self.dataset_drive_file_id = new_file_id
                    self._persist_dataset_drive_file_id(new_file_id)
                logger.info("✓ dataset.csv synced to Google Drive")
        except Exception as e:
            logger.error(f"Failed to sync dataset.csv to Google Drive: {e}")
    
    def _load_benchmark_commands(self) -> Dict[str, Dict[str, Any]]:
        """Merge default benchmark command metadata with overrides from config."""
        overrides = self.config['benchmarks'].get('commands', {}) or {}
        commands: Dict[str, Dict[str, Any]] = {}
        
        # Start from defaults
        for name, data in DEFAULT_BENCHMARK_COMMANDS.items():
            commands[name] = dict(data)
        
        # Apply overrides (shallow update)
        for name, custom in overrides.items():
            base = commands.get(name, {})
            merged = dict(base)
            merged.update(custom or {})
            commands[name] = merged
        
        return commands
    
    def _format_option_list(self, benchmark: str, options: Any) -> Optional[str]:
        """Convert options into a gem5-friendly string with resolved paths."""
        if options is None:
            return None
        
        if isinstance(options, (list, tuple)):
            resolved: List[str] = []
            for item in options:
                if item is None:
                    continue
                value = str(item)
                if isinstance(item, str):
                    maybe_path = ('/' in value) or bool(Path(value).suffix)
                else:
                    maybe_path = False
                
                if maybe_path:
                    candidate = self._resolve_benchmark_path(f"{benchmark}/{value}") if not Path(value).is_absolute() else Path(value)
                    if candidate.exists():
                        resolved.append(str(candidate))
                        continue
                    candidate_alt = self._resolve_benchmark_path(value) if not Path(value).is_absolute() else Path(value)
                    if candidate_alt.exists():
                        resolved.append(str(candidate_alt))
                        continue
                resolved.append(value)
            if not resolved:
                return None
            return ' '.join(shlex.quote(item) for item in resolved)
        
        if isinstance(options, str):
            return options.strip() or None
        
        return str(options)
    
    def _build_gem5_command(self, benchmark: str, run_dir: Path) -> Tuple[List[str], Optional[Path], Path]:
        """Construct the gem5 command for a benchmark."""
        settings = dict(self.benchmark_commands.get(benchmark, {}))
        
        binary_value = settings.get('binary') or f"{benchmark}/{benchmark}"
        binary_path = self._resolve_benchmark_path(binary_value)
        if not binary_path.exists():
            raise FileNotFoundError(f"Benchmark binary not found for '{benchmark}': {binary_path}")

        working_dir_value = settings.get('working_dir')
        if working_dir_value:
            working_dir = self._resolve_benchmark_path(working_dir_value)
        else:
            working_dir = binary_path.parent

        gem5_config = self.configs_dir / self.config['gem5']['default_config']

        cmd: List[str] = [
            str(self.gem5_binary),
            '-d', str(run_dir),
            str(gem5_config),
            '--cmd', str(binary_path)
        ]
        
        options_value = self._format_option_list(benchmark, settings.get('options'))
        if options_value:
            cmd.extend(['--options', options_value])
        
        stdout_redirect = settings.get('stdout')
        if stdout_redirect:
            stdout_path = Path(stdout_redirect)
            if not stdout_path.is_absolute():
                stdout_path = (run_dir / stdout_path).resolve()
            cmd.extend(['--output', str(stdout_path)])
        
        stderr_redirect = settings.get('stderr')
        if stderr_redirect:
            stderr_path = Path(stderr_redirect)
            if not stderr_path.is_absolute():
                stderr_path = (run_dir / stderr_path).resolve()
            cmd.extend(['--errout', str(stderr_path)])
        
        stdin_value = settings.get('stdin')
        stdin_path: Optional[Path] = None
        if stdin_value:
            stdin_path = self._resolve_benchmark_path(stdin_value)
            if not stdin_path.exists():
                raise FileNotFoundError(f"stdin file not found for '{benchmark}': {stdin_path}")
        
        return cmd, stdin_path, working_dir
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            sys.exit(1)
    
    def _load_run_log(self) -> Dict[str, Any]:
        """Load run log for resume capability."""
        if self.run_log_file.exists():
            try:
                with open(self.run_log_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'completed': [], 'failed': [], 'in_progress': []}
    
    def _save_run_log(self):
        """Save run log."""
        with open(self.run_log_file, 'w') as f:
            json.dump(self.run_log, f, indent=2)
    
    def _validate_setup(self):
        """Validate that all required paths and files exist."""
        errors = []
        
        if not self.gem5_binary.exists():
            errors.append(f"Gem5 binary not found: {self.gem5_binary}")
            errors.append(f"  (from cloud machine gem5 installation)")
        
        if not self.cpu2006_path.exists():
            errors.append(f"CPU2006 path not found: {self.cpu2006_path}")
            errors.append(f"  (expected in local project directory)")
        
        if not self.configs_dir.exists():
            errors.append(f"Gem5 configs directory not found: {self.configs_dir}")
            errors.append(f"  (from cloud machine gem5 installation)")
        
        if errors:
            logger.error("Setup validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            logger.error("\nPlease update config.yaml with correct paths:")
            logger.error(f"  - gem5.installation_path: Path to cloud gem5 installation")
            logger.error(f"  - benchmarks.cpu2006_path: ./cpu2006 (local in this project)")
            sys.exit(1)
        
        logger.info("✓ Setup validation passed")
        logger.info(f"  Using gem5 from: {self.gem5_root}")
        logger.info(f"  Using CPU2006 from: {self.cpu2006_path}")
    
    def run_single_simulation(
        self,
        benchmark: str,
        config: Dict[str, Any],
        run_id: str
    ) -> Dict[str, Any]:
        """
        Run a single simulation.
        
        Args:
            benchmark: Benchmark name
            config: Configuration dictionary
            run_id: Unique run identifier
            
        Returns:
            Dictionary with run results
        """
        # Create output directory
        run_dir = (self.results_dir / run_id).resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
        
        # Save configuration
        config_file = run_dir / 'config.json'
        with open(config_file, 'w') as f:
            json.dump({
                'benchmark': benchmark,
                'config': config,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
        
        # Build gem5 command
        try:
            cmd, stdin_path, working_dir = self._build_gem5_command(benchmark, run_dir)
        except FileNotFoundError as exc:
            logger.error(str(exc))
            return {
                'run_id': run_id,
                'benchmark': benchmark,
                'config': config,
                'success': False,
                'error': str(exc),
                'timestamp': datetime.now().isoformat()
            }
        
        # Add configuration arguments
        cmd.extend(self.config_manager.config_to_gem5_args(config))
        
        logger.info(f"Running: {benchmark} with {run_id}")
        logger.debug(f"Command: {' '.join(cmd)}")
        
        # Run simulation
        stdout_file = run_dir / 'stdout.log'
        stderr_file = run_dir / 'stderr.log'
        
        start_time = datetime.now()
        
        stdin_handle = None
        try:
            if stdin_path:
                stdin_handle = open(stdin_path, 'rb')
            
            env = os.environ.copy()
            env['GEM5_PROCESS_CWD'] = str(working_dir)
            
            with open(stdout_file, 'w') as stdout, open(stderr_file, 'w') as stderr:
                process = subprocess.run(
                    cmd,
                    stdout=stdout,
                    stderr=stderr,
                    stdin=stdin_handle,
                    timeout=self.config['simulation']['timeout_seconds'],
                    cwd=str(self.gem5_root),
                    env=env
                )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            success = process.returncode == 0
            
            if success:
                logger.info(f"✓ Completed: {run_id} ({duration:.1f}s)")
            else:
                logger.warning(f"✗ Failed: {run_id} (return code {process.returncode})")
            
            # Parse results if successful
            metrics = {}
            if success:
                stats_file = run_dir / 'stats.txt'
                if stats_file.exists():
                    metrics = self.stats_parser.parse_and_extract(stats_file)
            
            return {
                'run_id': run_id,
                'benchmark': benchmark,
                'config': config,
                'success': success,
                'returncode': process.returncode,
                'duration': duration,
                'metrics': metrics,
                'timestamp': start_time.isoformat()
            }
        
        except subprocess.TimeoutExpired:
            logger.error(f"✗ Timeout: {run_id}")
            return {
                'run_id': run_id,
                'benchmark': benchmark,
                'config': config,
                'success': False,
                'error': 'timeout',
                'timestamp': start_time.isoformat()
            }
        
        except Exception as e:
            logger.error(f"✗ Error running {run_id}: {e}")
            return {
                'run_id': run_id,
                'benchmark': benchmark,
                'config': config,
                'success': False,
                'error': str(e),
                'timestamp': start_time.isoformat()
            }
        finally:
            if stdin_handle:
                stdin_handle.close()
    
    def _run_configuration_round(
        self,
        config: Dict[str, Any],
        benchmarks: List[str],
        parallel: int,
        round_index: int,
        total_rounds: int
    ) -> List[Dict[str, Any]]:
        """Execute a single configuration across all requested benchmarks."""
        config_id = self.config_manager.get_config_id(config)
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting configuration round {round_index}/{total_rounds}")
        logger.info(f"Configuration ID: {config_id}")
        logger.info(f"Total benchmarks this round: {len(benchmarks)}")
        logger.info(f"{'='*60}\n")
        
        results: List[Dict[str, Any]] = []
        total_benchmarks = len(benchmarks)
        
        if parallel == 1:
            for index, benchmark in enumerate(benchmarks, start=1):
                run_id = f"{benchmark}_{config_id}"
                
                if run_id in self.run_log['completed']:
                    logger.debug(f"Skipping (already completed): {run_id}")
                    continue
                
                logger.info(f"[Round {round_index}/{total_rounds}] Starting {benchmark} "
                            f"({index}/{total_benchmarks}) with {config_id}")
                result = self.run_single_simulation(benchmark, config, run_id)
                results.append(result)
                
                if result['success']:
                    self.run_log['completed'].append(run_id)
                    self._handle_success_result(result)
                else:
                    self.run_log['failed'].append(run_id)
                    self.session_failed_runs += 1
                self._save_run_log()
                logger.info(f"[Round {round_index}/{total_rounds}] Finished {run_id} "
                            f"(success={result['success']})")
        else:
            with ProcessPoolExecutor(max_workers=parallel) as executor:
                futures: Dict[Any, str] = {}
                
                for index, benchmark in enumerate(benchmarks, start=1):
                    run_id = f"{benchmark}_{config_id}"
                    
                    if run_id in self.run_log['completed']:
                        logger.debug(f"Skipping (already completed): {run_id}")
                        continue
                    
                    logger.info(f"[Round {round_index}/{total_rounds}] Queuing {benchmark} "
                                f"({index}/{total_benchmarks}) with {config_id}")
                    future = executor.submit(
                        self.run_single_simulation,
                        benchmark,
                        config,
                        run_id
                    )
                    futures[future] = run_id
                
                if futures:
                    for future in tqdm(
                        as_completed(futures),
                        total=len(futures),
                        desc=f"Config {round_index}/{total_rounds}"
                    ):
                        run_id = futures[future]
                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result['success']:
                                self.run_log['completed'].append(run_id)
                                self._handle_success_result(result)
                            else:
                                self.run_log['failed'].append(run_id)
                                self.session_failed_runs += 1
                            self._save_run_log()
                            logger.info(f"[Round {round_index}/{total_rounds}] Finished {run_id} "
                                        f"(success={result['success']})")
                        except Exception as e:
                            logger.error(f"Exception in {run_id}: {e}")
                            self.run_log['failed'].append(run_id)
                            self.session_failed_runs += 1
                            self._save_run_log()
                else:
                    logger.info("All benchmarks already completed for this configuration.")
        
        successful = sum(1 for r in results if r['success'])
        logger.info(f"\n✓ Completed configuration {config_id}")
        logger.info(f"  Benchmarks run: {len(results)}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {len(results) - successful}")
        
        return results
    
    def run_full_sweep(
        self,
        strategy: str = 'grid',
        preset: Optional[str] = None,
        num_samples: Optional[int] = None,
        parallel: int = 1,
        benchmarks: Optional[List[str]] = None
    ):
        """
        Run parameter sweep across all benchmarks.
        
        Args:
            strategy: Sampling strategy
            preset: Configuration preset
            num_samples: Number of samples
            parallel: Parallel simulations per benchmark
            benchmarks: List of benchmarks (None = all)
        """
        if benchmarks is None:
            benchmarks = self.config['benchmarks']['benchmark_list']
        benchmarks = list(benchmarks)
        
        configurations = list(self.config_manager.generate_configurations(
            strategy=strategy,
            preset=preset,
            num_samples=num_samples
        ))
        
        if not configurations:
            logger.warning("No configurations generated; nothing to run.")
            return
        
        logger.info(f"\n{'='*60}")
        logger.info(f"FULL PARAMETER SWEEP")
        logger.info(f"{'='*60}")
        logger.info(f"Strategy: {strategy}")
        logger.info(f"Preset: {preset or 'None'}")
        logger.info(f"Benchmarks: {len(benchmarks)}")
        logger.info(f"Parallel: {parallel}")
        logger.info(f"Configurations: {len(configurations)}")
        logger.info(f"{'='*60}\n")
        
        for index, config in enumerate(configurations, start=1):
            self._run_configuration_round(
                config=config,
                benchmarks=benchmarks,
                parallel=parallel,
                round_index=index,
                total_rounds=len(configurations)
            )
            
            if self.config['google_drive'].get('backup_frequency') in {'after_each_benchmark', 'after_each_config'}:
                self._backup_results(f"config_{self.config_manager.get_config_id(config)}")
        
        # Finalize dataset artifacts
        total_rows = self._finalize_dataset()
        
        # Final backup
        if self.gdrive_backup:
            self._backup_results('final')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"SWEEP COMPLETE")
        logger.info(f"Configurations processed: {len(configurations)}")
        logger.info(f"New successful runs this session: {self.session_successful_runs}")
        logger.info(f"New failed runs this session: {self.session_failed_runs}")
        logger.info(f"Dataset rows (total): {total_rows}")
        logger.info(f"Dataset CSV: {self.dataset_file}")
        logger.info(f"{'='*60}\n")

    def _finalize_dataset(self) -> int:
        """Create/upsert dataset.json and return total row count."""
        if not self.dataset_file.exists():
            logger.warning("dataset.csv not found; skipping dataset finalization")
            return self.dataset_total_rows
        
        try:
            df = pd.read_csv(self.dataset_file)
        except Exception as e:
            logger.error(f"Failed to read dataset.csv for finalization: {e}")
            return self.dataset_total_rows
        
        total_rows = len(df)
        json_file = self.results_dir / 'dataset.json'
        try:
            df.to_json(json_file, orient='records', indent=2)
            logger.info(f"✓ dataset.json updated with {total_rows} rows")
        except Exception as e:
            logger.error(f"Failed to write dataset.json: {e}")
        
        self.dataset_total_rows = total_rows
        # Ensure dataset.csv sync is up to date after finalization
        self._sync_dataset_to_drive()
        return total_rows
    
    def _backup_results(self, label: str):
        """Backup results to Google Drive."""
        if not self.gdrive_backup:
            return
        
        try:
            logger.info(f"Backing up results to Google Drive...")
            self.gdrive_backup.backup_results(
                self.results_dir,
                compress=self.config['google_drive']['compress_before_upload']
            )
            logger.info("✓ Backup successful")
        except Exception as e:
            logger.error(f"✗ Backup failed: {e}")
    
    def show_status(self):
        """Display current status and statistics."""
        table = Table(title="Simulation Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Completed Runs", str(len(self.run_log['completed'])))
        table.add_row("Failed Runs", str(len(self.run_log['failed'])))
        table.add_row("In Progress", str(len(self.run_log['in_progress'])))
        
        if self.dataset_file.exists():
            df = pd.read_csv(self.dataset_file)
            table.add_row("Dataset Rows", str(len(df)))
            table.add_row("Unique Benchmarks", str(df['benchmark'].nunique()))
        
        console.print(table)


def main():
    parser = argparse.ArgumentParser(description='Gem5 Simulation Pipeline')
    parser.add_argument('--config', default='config.yaml', help='Configuration file')
    parser.add_argument('--test', action='store_true', help='Run single test simulation')
    parser.add_argument('--sweep', action='store_true', help='Run full parameter sweep')
    parser.add_argument('--benchmark', help='Specific benchmark to run')
    parser.add_argument('--strategy', default='grid', choices=['grid', 'random', 'latin_hypercube', 'custom'])
    parser.add_argument('--preset', help='Configuration preset')
    parser.add_argument('--samples', type=int, help='Number of samples (for random strategies)')
    parser.add_argument('--parallel', type=int, default=1, help='Number of parallel simulations')
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted run')
    
    args = parser.parse_args()
    
    runner = SimulationRunner(args.config)
    
    if args.status:
        runner.show_status()
    
    elif args.test:
        benchmark = args.benchmark or 'bwaves'
        preset_name = args.preset or 'small_test'
        sample_count = args.samples
        if args.strategy in ('random', 'latin_hypercube') and (not sample_count or sample_count < 1):
            sample_count = 1
        
        config_iter = runner.config_manager.generate_configurations(
            strategy=args.strategy,
            preset=preset_name,
            num_samples=sample_count
        )
        
        try:
            test_config = next(config_iter)
        except StopIteration:
            logger.error(f"No configurations available for preset '{preset_name}'")
            return
        
        logger.info(f"Running test simulation: {benchmark}")
        result = runner.run_single_simulation(
            benchmark=benchmark,
            config=test_config,
            run_id=f"test_{benchmark}"
        )
        console.print(result)
    
    elif args.sweep:
        runner.run_full_sweep(
            strategy=args.strategy,
            preset=args.preset,
            num_samples=args.samples,
            parallel=args.parallel,
            benchmarks=[args.benchmark] if args.benchmark else None
        )
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
