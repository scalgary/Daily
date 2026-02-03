#!/bin/bash
# save as check_limits.sh

echo "=== CAPACITÉS MAXIMALES DE LA MACHINE ==="
echo "CPU Cores: $(nproc)"
echo "RAM Total: $(free -g | grep Mem | awk '{print $2}') GB"
echo "RAM Disponible: $(free -g | grep Mem | awk '{print $7}') GB"
echo "Swap: $(free -g | grep Swap | awk '{print $2}') GB"
echo "File Descriptors: $(ulimit -n)"
echo "Network: $(ip link show | grep "state UP" -A 1 | grep "link/ether")"

# Test Spark
echo "spark-shell: $(spark-shell --version)"

# Test Jupyter
echo "Jupyter: $(jupyter --version)"



# Test linting

echo "ruff: $(ruff --version)"
echo "pylint: $(pylint --version)"



# Test Python kernel
echo "kernel available: $(jupyter kernelspec list)"

echo "🚀 Starting devcontainer setup..."
echo "📍 Current directory: $(pwd)"
echo "👤 Current user: $(whoami)"