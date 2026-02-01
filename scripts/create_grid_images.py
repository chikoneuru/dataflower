#!/usr/bin/env python3
"""
Alternative approach: Create test images by arranging the base image in a grid pattern
with specific dimensions to achieve target file sizes more predictably.
"""

import os
import sys
from PIL import Image
import argparse

def create_grid_image(input_path, output_path, rows, cols, padding=20):
    """
    Create a grid image with specified dimensions.
    
    Args:
        input_path: Path to the base test image
        output_path: Path where the new image will be saved
        rows: Number of rows in the grid
        cols: Number of columns in the grid
        padding: Padding in pixels between images
    """
    try:
        # Load the base image
        base_img = Image.open(input_path)
        base_width, base_height = base_img.size
        
        # Calculate output dimensions
        output_width = cols * base_width + (cols - 1) * padding
        output_height = rows * base_height + (rows - 1) * padding
        
        print(f"Creating {rows}x{cols} grid")
        print(f"Base image: {base_width}x{base_height}")
        print(f"Output dimensions: {output_width}x{output_height}")
        
        # Create new image with white background
        output_img = Image.new('RGB', (output_width, output_height), 'white')
        
        # Fill the grid
        for row in range(rows):
            for col in range(cols):
                x = col * (base_width + padding)
                y = row * (base_height + padding)
                output_img.paste(base_img, (x, y))
        
        # Save the output image
        output_img.save(output_path, 'PNG', optimize=True)
        
        # Check final size
        final_size_bytes = os.path.getsize(output_path)
        final_size_mb = final_size_bytes / (1024 * 1024)
        
        print(f"Created {output_path}")
        print(f"Final size: {final_size_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"Error creating grid image: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Create test images in grid pattern')
    parser.add_argument('--base-image', default='data/test.png', 
                       help='Path to base test image (default: data/test.png)')
    parser.add_argument('--padding', type=int, default=20,
                       help='Padding between images in pixels (default: 20)')
    
    args = parser.parse_args()
    
    # Check if base image exists
    if not os.path.exists(args.base_image):
        print(f"Error: Base image {args.base_image} not found")
        return 1
    
    print(f"Using base image: {args.base_image}")
    print(f"Padding: {args.padding} pixels")
    print("-" * 50)
    
    # Define grid configurations for different target sizes
    # These are estimates - actual file sizes will vary
    grid_configs = [
        (2, 2, "5mb"),      # 2x2 grid for ~5MB
        (3, 3, "10mb"),     # 3x3 grid for ~10MB
        (4, 4, "20mb"),     # 4x4 grid for ~20MB
        (6, 6, "50mb"),     # 6x6 grid for ~50MB
    ]
    
    success_count = 0
    for rows, cols, size_name in grid_configs:
        output_path = f"data/test_{size_name}_grid.png"
        print(f"\nCreating {size_name} image ({rows}x{cols} grid)...")
        
        if create_grid_image(args.base_image, output_path, rows, cols, args.padding):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"Successfully created {success_count}/{len(grid_configs)} images")
    
    return 0 if success_count == len(grid_configs) else 1

if __name__ == '__main__':
    sys.exit(main())
