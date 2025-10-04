#!/bin/bash
# save as check_limits.sh

echo "=== CAPACITÉS MAXIMALES DE LA MACHINE ==="
echo "CPU Cores: $(nproc)"
echo "RAM Total: $(free -g | grep Mem | awk '{print $2}') GB"
echo "RAM Disponible: $(free -g | grep Mem | awk '{print $7}') GB"
echo "Swap: $(free -g | grep Swap | awk '{print $2}') GB"
echo "Disk I/O: $(hdparm -tT /dev/sda | grep "Timing" 2>/dev/null)"
echo "File Descriptors: $(ulimit -n)"
echo "Max Processes: $(ulimit -u)"
echo "Network: $(ip link show | grep "state UP" -A 1 | grep "link/ether")"