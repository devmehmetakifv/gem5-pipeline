#!/usr/bin/env python3
"""
Gem5 Stats Parser
Extracts metrics from gem5 stats.txt output files
"""

import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class Gem5StatsParser:
    """Parse gem5 statistics files and extract relevant metrics."""
    
    def __init__(self, metrics_config: Optional[List[str]] = None):
        """
        Initialize the parser.
        
        Args:
            metrics_config: List of metric names to extract (None = extract all)
        """
        self.metrics_config = metrics_config
        self.stats = {}
    
    def parse_file(self, stats_file: Path) -> Dict[str, Any]:
        """
        Parse a gem5 stats.txt file.
        
        Args:
            stats_file: Path to stats.txt file
            
        Returns:
            Dictionary of metric_name: value pairs
        """
        if not stats_file.exists():
            logger.error(f"Stats file not found: {stats_file}")
            return {}
        
        stats = {}
        
        try:
            with open(stats_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#') or line.startswith('-'):
                        continue
                    
                    # Parse stat line: stat_name value # description
                    match = re.match(r'^([^\s]+)\s+([^\s#]+)(?:\s+#\s*(.*))?$', line)
                    if match:
                        stat_name = match.group(1)
                        stat_value = match.group(2)
                        stat_desc = match.group(3) if match.group(3) else ""
                        
                        # Filter by metrics_config if provided
                        if self.metrics_config is None or stat_name in self.metrics_config:
                            # Try to convert to numeric
                            try:
                                # Handle scientific notation
                                if 'e' in stat_value.lower() or '.' in stat_value:
                                    stat_value = float(stat_value)
                                else:
                                    stat_value = int(stat_value)
                            except ValueError:
                                # Keep as string if not numeric
                                pass
                            
                            stats[stat_name] = {
                                'value': stat_value,
                                'description': stat_desc
                            }
        
        except Exception as e:
            logger.error(f"Error parsing stats file {stats_file}: {e}")
            return {}
        
        logger.info(f"Parsed {len(stats)} metrics from {stats_file}")
        return stats
    
    def extract_key_metrics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and compute key performance metrics.
        
        Args:
            stats: Dictionary of raw statistics
            
        Returns:
            Dictionary of computed metrics
        """
        metrics = {}
        
        # Helper to safely get stat value
        def get_value(key: str, default=0):
            return stats.get(key, {}).get('value', default)
        
        # Basic performance metrics
        metrics['sim_seconds'] = get_value('sim_seconds')
        metrics['sim_ticks'] = get_value('sim_ticks')
        metrics['sim_freq'] = get_value('sim_freq')
        metrics['sim_insts'] = get_value('sim_insts')
        metrics['sim_ops'] = get_value('sim_ops')
        
        # Host performance
        metrics['host_inst_rate'] = get_value('host_inst_rate')
        metrics['host_op_rate'] = get_value('host_op_rate')
        metrics['host_seconds'] = get_value('host_seconds')
        
        # Compute IPC (instructions per cycle)
        num_cycles = get_value('system.switch_cpus.numCycles') or get_value('system.cpu.numCycles')
        if num_cycles > 0 and metrics['sim_insts'] > 0:
            metrics['ipc'] = metrics['sim_insts'] / num_cycles
            metrics['cpi'] = num_cycles / metrics['sim_insts']
        else:
            metrics['ipc'] = 0
            metrics['cpi'] = 0
        
        # Cache statistics (L1 Data)
        l1d_hits = get_value('system.cpu.dcache.overall_hits::total')
        l1d_misses = get_value('system.cpu.dcache.overall_misses::total')
        l1d_accesses = l1d_hits + l1d_misses
        metrics['l1d_miss_rate'] = (l1d_misses / l1d_accesses) if l1d_accesses > 0 else 0
        metrics['l1d_hits'] = l1d_hits
        metrics['l1d_misses'] = l1d_misses
        
        # Cache statistics (L1 Instruction)
        l1i_hits = get_value('system.cpu.icache.overall_hits::total')
        l1i_misses = get_value('system.cpu.icache.overall_misses::total')
        l1i_accesses = l1i_hits + l1i_misses
        metrics['l1i_miss_rate'] = (l1i_misses / l1i_accesses) if l1i_accesses > 0 else 0
        metrics['l1i_hits'] = l1i_hits
        metrics['l1i_misses'] = l1i_misses
        
        # Cache statistics (L2)
        l2_hits = get_value('system.l2.overall_hits::total')
        l2_misses = get_value('system.l2.overall_misses::total')
        l2_accesses = l2_hits + l2_misses
        metrics['l2_miss_rate'] = (l2_misses / l2_accesses) if l2_accesses > 0 else 0
        metrics['l2_hits'] = l2_hits
        metrics['l2_misses'] = l2_misses
        
        # Branch prediction
        branches = get_value('system.cpu.branchPred.condPredicted') or get_value('system.switch_cpus.branchPred.condPredicted')
        branch_misses = get_value('system.cpu.branchPred.condIncorrect') or get_value('system.switch_cpus.branchPred.condIncorrect')
        metrics['branch_mispred_rate'] = (branch_misses / branches) if branches > 0 else 0
        metrics['branches'] = branches
        metrics['branch_mispredicts'] = branch_misses
        
        # Memory bandwidth
        bytes_read = get_value('system.mem_ctrls.bytesReadSys')
        bytes_written = get_value('system.mem_ctrls.bytesWrittenSys')
        if metrics['sim_seconds'] > 0:
            metrics['memory_read_bw'] = bytes_read / metrics['sim_seconds']
            metrics['memory_write_bw'] = bytes_written / metrics['sim_seconds']
            metrics['memory_total_bw'] = (bytes_read + bytes_written) / metrics['sim_seconds']
        else:
            metrics['memory_read_bw'] = 0
            metrics['memory_write_bw'] = 0
            metrics['memory_total_bw'] = 0
        
        # Memory requests
        metrics['mem_read_reqs'] = get_value('system.mem_ctrls.readReqs')
        metrics['mem_write_reqs'] = get_value('system.mem_ctrls.writeReqs')
        
        return metrics
    
    def parse_and_extract(self, stats_file: Path) -> Dict[str, Any]:
        """
        Parse stats file and extract key metrics in one call.
        
        Args:
            stats_file: Path to stats.txt
            
        Returns:
            Dictionary of key metrics
        """
        raw_stats = self.parse_file(stats_file)
        return self.extract_key_metrics(raw_stats)


def parse_config_ini(config_file: Path) -> Dict[str, Any]:
    """
    Parse gem5 config.ini file to extract configuration parameters.
    
    Args:
        config_file: Path to config.ini
        
    Returns:
        Dictionary of configuration parameters
    """
    config = {}
    current_section = None
    
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                
                # Section header
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1]
                    config[current_section] = {}
                
                # Parameter line
                elif '=' in line and current_section:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    config[current_section][key] = value
    
    except Exception as e:
        logger.error(f"Error parsing config file {config_file}: {e}")
    
    return config


if __name__ == "__main__":
    # Test the parser
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python results_parser.py <stats.txt>")
        sys.exit(1)
    
    stats_file = Path(sys.argv[1])
    
    parser = Gem5StatsParser()
    metrics = parser.parse_and_extract(stats_file)
    
    print("\n=== Extracted Metrics ===")
    for key, value in sorted(metrics.items()):
        print(f"{key:30s} = {value}")
