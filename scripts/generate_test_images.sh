#!/bin/bash

# Script to generate test images using different approaches
# Usage: ./scripts/generate_test_images.sh [approach]
# Approaches: "patch" (default), "grid", or "both"

APPROACH=${1:-"patch"}

echo "Generating test images using approach: $APPROACH"
echo "=============================================="

# Check if PIL is available
python3 -c "from PIL import Image" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: PIL (Pillow) is required. Install with: pip install Pillow"
    exit 1
fi

# Check if base image exists
if [ ! -f "data/test.png" ]; then
    echo "Error: Base image data/test.png not found"
    exit 1
fi

case $APPROACH in
    "patch")
        echo "Using patched approach (estimates target file sizes)..."
        python3 scripts/create_test_images.py --sizes 5 10 20 50 --padding 50
        ;;
    "grid")
        echo "Using grid approach (predictable dimensions)..."
        python3 scripts/create_grid_images.py --padding 20
        ;;
    "both")
        echo "Running both approaches..."
        echo ""
        echo "=== PATCHED APPROACH ==="
        python3 scripts/create_test_images.py --sizes 5 10 20 50 --padding 50
        echo ""
        echo "=== GRID APPROACH ==="
        python3 scripts/create_grid_images.py --padding 20
        ;;
    *)
        echo "Error: Unknown approach '$APPROACH'"
        echo "Valid approaches: patch, grid, both"
        exit 1
        ;;
esac

echo ""
echo "Generated images:"
ls -lh data/test_*mb*.png 2>/dev/null | awk '{print $9, $5}' || echo "No test images found"
