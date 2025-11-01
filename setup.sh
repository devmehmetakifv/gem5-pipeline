#!/bin/bash
#
# Setup script for Gem5 Simulation Pipeline
# This script helps configure the pipeline for first-time use
#

set -e

echo "=================================="
echo "Gem5 Simulation Pipeline Setup"
echo "=================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python version
echo -n "Checking Python version... "
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    echo -e "${GREEN}Found Python $PYTHON_VERSION${NC}"
else
    echo -e "${RED}Python 3 not found!${NC}"
    exit 1
fi

# Create virtual environment
echo -n "Creating virtual environment... "
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}Done${NC}"
else
    echo -e "${YELLOW}Already exists${NC}"
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo -e "${GREEN}Dependencies installed${NC}"

# Create necessary directories
echo -n "Creating directories... "
mkdir -p results
mkdir -p backups
mkdir -p logs
mkdir -p custom_configs
echo -e "${GREEN}Done${NC}"

# Check for config.yaml
echo ""
echo "=================================="
echo "Configuration"
echo "=================================="
echo ""

if [ ! -f "config.yaml" ]; then
    echo -e "${RED}config.yaml not found!${NC}"
    echo "Please create config.yaml from the template"
    exit 1
fi

# Prompt for gem5 path
echo -e "${YELLOW}Please verify your paths in config.yaml:${NC}"
echo ""

read -p "Path to gem5 installation on cloud machine: " GEM5_PATH

if [ -z "$GEM5_PATH" ]; then
    echo -e "${YELLOW}Skipping path update. You can manually edit config.yaml${NC}"
    echo "NOTE: cpu2006 benchmarks will be used from ./cpu2006 (local)"
else
    # Update config.yaml with paths
    sed -i.bak "s|installation_path:.*|installation_path: \"$GEM5_PATH\"|" config.yaml
    echo -e "${GREEN}Updated config.yaml${NC}"
    echo "NOTE: cpu2006 benchmarks will be used from ./cpu2006 (local)"
fi

# Verify gem5
echo ""
echo "Verifying gem5 installation..."
GEM5_BINARY="${GEM5_PATH}/build/X86/gem5.opt"

if [ -f "$GEM5_BINARY" ]; then
    echo -e "${GREEN}✓ Found gem5 binary: $GEM5_BINARY${NC}"
else
    echo -e "${YELLOW}✗ gem5 binary not found: $GEM5_BINARY${NC}"
    echo "  Please compile gem5 first:"
    echo "  cd $GEM5_PATH && scons build/X86/gem5.opt -j\$(nproc)"
fi

# Verify CPU2006
echo ""
CPU2006_PATH="./cpu2006"
if [ -d "$CPU2006_PATH" ]; then
    echo -e "${GREEN}✓ Found CPU2006 directory (local): $CPU2006_PATH${NC}"
    NUM_BENCHMARKS=$(ls -1 "$CPU2006_PATH" 2>/dev/null | wc -l)
    echo "  Found $NUM_BENCHMARKS files/directories"
else
    echo -e "${YELLOW}✗ CPU2006 directory not found: $CPU2006_PATH${NC}"
    echo "  Expected to be in this project directory"
fi

# Google Drive setup
echo ""
echo "=================================="
echo "Google Drive Setup (Optional)"
echo "=================================="
echo ""
echo "To enable Google Drive backup:"
echo "1. Go to https://console.cloud.google.com/"
echo "2. Create project and enable Google Drive API"
echo "3. Create OAuth credentials (Desktop app)"
echo "4. Download as credentials.json"
echo "5. Place in this directory"
echo ""

if [ -f "credentials.json" ]; then
    echo -e "${GREEN}✓ Found credentials.json${NC}"
else
    echo -e "${YELLOW}✗ credentials.json not found${NC}"
    echo "  Google Drive backup will be disabled"
fi

# Test installation
echo ""
echo "=================================="
echo "Testing Installation"
echo "=================================="
echo ""

echo "Running import test..."
python3 -c "
import yaml
import pandas
import tqdm
import rich
print('✓ All imports successful')
" && echo -e "${GREEN}✓ Python dependencies OK${NC}" || echo -e "${RED}✗ Import failed${NC}"

# Summary
echo ""
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""
echo "Next steps:"
echo "1. Review and edit config.yaml with your paths"
echo "2. Review config_space.json for parameter space"
echo "3. (Optional) Setup Google Drive credentials"
echo "4. Run a test: python simulation_runner.py --test"
echo ""
echo "For detailed instructions, see:"
echo "  - README.md"
echo "  - INTEGRATION_GUIDE.md"
echo ""
echo -e "${GREEN}Happy simulating!${NC}"
