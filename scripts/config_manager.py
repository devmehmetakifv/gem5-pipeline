#!/usr/bin/env python3
"""
Configuration Manager
Handles parameter space generation and iteration
"""

import json
import itertools
from pathlib import Path
from typing import Dict, List, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Manage simulation configuration space and generate parameter combinations."""
    
    def __init__(self, config_space_file: Path):
        """
        Initialize configuration manager.
        
        Args:
            config_space_file: Path to config_space.json
        """
        self.config_space_file = config_space_file
        self.config_space = self._load_config_space()
        self.presets = self.config_space.get('presets', {})
    
    def _load_config_space(self) -> Dict[str, Any]:
        """Load configuration space from JSON file."""
        try:
            with open(self.config_space_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config space: {e}")
            return {}
    
    def get_parameter_space(self, preset: Optional[str] = None) -> Dict[str, List[Any]]:
        """
        Get the parameter space, optionally filtered by preset.
        
        Args:
            preset: Name of preset configuration to use
            
        Returns:
            Dictionary mapping parameter names to possible values
        """
        param_space = {}
        
        # Extract base parameters
        for category, params in self.config_space.items():
            if category in ['description', 'version', 'sampling_strategy', 'presets']:
                continue
            
            for param_name, param_config in params.items():
                if isinstance(param_config, dict) and 'values' in param_config:
                    full_name = f"{category}.{param_name}"
                    param_space[full_name] = param_config['values']
        
        # Apply preset overrides
        if preset and preset in self.presets:
            overrides = self.presets[preset].get('overrides', {})
            param_space.update(overrides)
            logger.info(f"Applied preset '{preset}'")
        
        return param_space
    
    def generate_configurations(
        self,
        strategy: str = 'grid',
        preset: Optional[str] = None,
        num_samples: Optional[int] = None,
        seed: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate configurations based on sampling strategy.
        
        Args:
            strategy: Sampling strategy ('grid', 'random', 'latin_hypercube', 'custom')
            preset: Preset configuration name
            num_samples: Number of samples for random/LHS strategies
            seed: Random seed for reproducibility
            
        Yields:
            Configuration dictionaries
        """
        param_space = self.get_parameter_space(preset)
        
        if strategy == 'grid':
            yield from self._grid_sampling(param_space)
        elif strategy == 'random':
            yield from self._random_sampling(param_space, num_samples, seed)
        elif strategy == 'latin_hypercube':
            yield from self._lhs_sampling(param_space, num_samples, seed)
        elif strategy == 'custom':
            yield from self._custom_sampling()
        else:
            logger.error(f"Unknown strategy: {strategy}")
            return
    
    def _grid_sampling(self, param_space: Dict[str, List[Any]]) -> Iterator[Dict[str, Any]]:
        """
        Full factorial grid sampling - generates all combinations.
        
        Args:
            param_space: Parameter space dictionary
            
        Yields:
            Configuration dictionaries
        """
        # Get parameter names and values
        param_names = sorted(param_space.keys())
        param_values = [param_space[name] for name in param_names]
        
        # Generate all combinations
        total_configs = 1
        for values in param_values:
            total_configs *= len(values)
        
        logger.info(f"Generating {total_configs} configurations (grid sampling)")
        
        for combo in itertools.product(*param_values):
            config = dict(zip(param_names, combo))
            yield config
    
    def _random_sampling(
        self,
        param_space: Dict[str, List[Any]],
        num_samples: int,
        seed: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Random sampling from parameter space.
        
        Args:
            param_space: Parameter space dictionary
            num_samples: Number of samples to generate
            seed: Random seed
            
        Yields:
            Configuration dictionaries
        """
        import random
        
        if seed is not None:
            random.seed(seed)
        
        logger.info(f"Generating {num_samples} configurations (random sampling)")
        
        param_names = sorted(param_space.keys())
        
        for _ in range(num_samples):
            config = {}
            for name in param_names:
                config[name] = random.choice(param_space[name])
            yield config
    
    def _lhs_sampling(
        self,
        param_space: Dict[str, List[Any]],
        num_samples: int,
        seed: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Latin Hypercube Sampling for better parameter space coverage.
        
        Args:
            param_space: Parameter space dictionary
            num_samples: Number of samples to generate
            seed: Random seed
            
        Yields:
            Configuration dictionaries
        """
        try:
            from scipy.stats import qmc
            import numpy as np
            
            if seed is not None:
                np.random.seed(seed)
            
            logger.info(f"Generating {num_samples} configurations (LHS)")
            
            param_names = sorted(param_space.keys())
            n_params = len(param_names)
            
            # Generate LHS samples in [0, 1]^n
            sampler = qmc.LatinHypercube(d=n_params, seed=seed)
            samples = sampler.random(n=num_samples)
            
            # Map samples to parameter values
            for sample in samples:
                config = {}
                for i, name in enumerate(param_names):
                    values = param_space[name]
                    # Map [0, 1] to index in values list
                    idx = int(sample[i] * len(values))
                    idx = min(idx, len(values) - 1)  # Clamp to valid range
                    config[name] = values[idx]
                yield config
        
        except ImportError:
            logger.warning("scipy not available, falling back to random sampling")
            yield from self._random_sampling(param_space, num_samples, seed)
    
    def _custom_sampling(self) -> Iterator[Dict[str, Any]]:
        """
        Load custom configurations from file.
        
        Yields:
            Configuration dictionaries
        """
        custom_file = Path("custom_configs.json")
        
        if not custom_file.exists():
            logger.error(f"Custom config file not found: {custom_file}")
            return
        
        try:
            with open(custom_file, 'r') as f:
                configs = json.load(f)
            
            logger.info(f"Loaded {len(configs)} custom configurations")
            
            for config in configs:
                yield config
        
        except Exception as e:
            logger.error(f"Error loading custom configs: {e}")
    
    def config_to_gem5_args(self, config: Dict[str, Any]) -> List[str]:
        """
        Convert configuration dictionary to gem5 command-line arguments.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            List of command-line argument strings
        """
        args = []
        
        # Map config keys to gem5 argument names
        arg_mapping = {
            'cpu.cpu_type': '--cpu-type',
            'cpu.cpu_clock': '--cpu-clock',
            'memory.mem_size': '--mem-size',
            'memory.mem_type': '--mem-type',
            'cache_l1d.size': '--l1d_size',
            'cache_l1d.assoc': '--l1d_assoc',
            'cache_l1i.size': '--l1i_size',
            'cache_l1i.assoc': '--l1i_assoc',
            'cache_l2.size': '--l2_size',
            'cache_l2.assoc': '--l2_assoc',
            'cache_l3.size': '--l3_size',
            'cache_l3.assoc': '--l3_assoc',
            'system.sys_clock': '--sys-clock',
            'simulation.fast_forward': '--fast-forward',
            'simulation.max_insts': '--maxinsts',
        }
        
        for key, value in config.items():
            if key in arg_mapping:
                args.append(arg_mapping[key])
                args.append(str(value))
            
            # Special handling for cache enables
            elif key == 'cache_l2.enabled' and value:
                args.append('--caches')
                args.append('--l2cache')
            elif key == 'cache_l3.enabled' and value:
                args.append('--l3cache')
        
        return args
    
    def get_config_id(self, config: Dict[str, Any]) -> str:
        """
        Generate unique identifier for a configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configuration ID string
        """
        # Create deterministic hash from config
        import hashlib
        
        config_str = json.dumps(config, sort_keys=True)
        config_hash = hashlib.md5(config_str.encode()).hexdigest()[:8]
        
        return f"config_{config_hash}"
    
    def save_configuration(self, config: Dict[str, Any], output_file: Path):
        """
        Save configuration to JSON file.
        
        Args:
            config: Configuration dictionary
            output_file: Output file path
        """
        try:
            with open(output_file, 'w') as f:
                json.dump(config, f, indent=2)
            logger.debug(f"Saved configuration to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")


if __name__ == "__main__":
    # Test the configuration manager
    import sys
    
    config_file = Path("config_space.json")
    
    if not config_file.exists():
        print(f"Config file not found: {config_file}")
        sys.exit(1)
    
    manager = ConfigurationManager(config_file)
    
    # Generate a few sample configurations
    print("\n=== Grid Sampling (first 5) ===")
    for i, config in enumerate(manager.generate_configurations(strategy='grid', preset='small_test')):
        if i >= 5:
            break
        print(f"\nConfig {i+1}:")
        print(json.dumps(config, indent=2))
        print("\nGem5 args:", ' '.join(manager.config_to_gem5_args(config)))
