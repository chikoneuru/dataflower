#!/usr/bin/env python3
"""
Script to create larger test images by patching together multiple copies of test.png
with padding between them. This is much faster than creating entirely new images.
"""

import os
import sys
from PIL import Image
import argparse

def create_patched_image(input_path, output_path, target_size_mb, padding=50):
    """
    Create a larger image by patching together multiple copies of the input image.
    
    Args:
        input_path: Path to the base test image
        output_path: Path where the new image will be saved
        target_size_mb: Target size in MB for the output image
        padding: Padding in pixels between patched images
    """
    try:
        # Load the base image
        base_img = Image.open(input_path)
        base_width, base_height = base_img.size
        base_size_bytes = os.path.getsize(input_path)
        target_size_bytes = target_size_mb * 1024 * 1024
        
        print(f"Base image size: {base_size_bytes / 1024:.1f} KB")
        print(f"Target size: {target_size_mb} MB")
        
        # For very large targets, use a more aggressive approach
        if target_size_mb >= 50:
            # For large images, use a more conservative compression factor
            compression_factor = 6  # More aggressive for large files
        else:
            compression_factor = 4  # Standard compression factor
        
        estimated_copies = max(1, int((target_size_bytes * compression_factor) / base_size_bytes))
        print(f"Estimated copies needed: {estimated_copies}")
        
        # Calculate grid dimensions to create a roughly square layout
        import math
        grid_size = int(math.ceil(math.sqrt(estimated_copies)))
        
        # Calculate output dimensions
        output_width = grid_size * base_width + (grid_size - 1) * padding
        output_height = grid_size * base_height + (grid_size - 1) * padding
        
        print(f"Creating {grid_size}x{grid_size} grid")
        print(f"Output dimensions: {output_width}x{output_height}")
        
        # Create new image with white background
        output_img = Image.new('RGB', (output_width, output_height), 'white')
        
        # Patch images onto the output
        for i in range(estimated_copies):
            row = i // grid_size
            col = i % grid_size
            
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
        
        # If still too small, try adding more copies
        if final_size_mb < target_size_mb * 0.8 and target_size_mb >= 50:
            print(f"Size too small, trying with more copies...")
            # Try with even more copies
            additional_copies = int((target_size_bytes - final_size_bytes) * compression_factor / base_size_bytes)
            if additional_copies > 0:
                new_total_copies = estimated_copies + additional_copies
                new_grid_size = int(math.ceil(math.sqrt(new_total_copies)))
                
                # Only recreate if we're not too close to memory limits
                if new_total_copies < 100:  # Reasonable limit
                    print(f"Adding {additional_copies} more copies (total: {new_total_copies})")
                    
                    new_output_width = new_grid_size * base_width + (new_grid_size - 1) * padding
                    new_output_height = new_grid_size * base_height + (new_grid_size - 1) * padding
                    
                    output_img = Image.new('RGB', (new_output_width, new_output_height), 'white')
                    
                    for i in range(new_total_copies):
                        row = i // new_grid_size
                        col = i % new_grid_size
                        
                        x = col * (base_width + padding)
                        y = row * (base_height + padding)
                        
                        output_img.paste(base_img, (x, y))
                    
                    output_img.save(output_path, 'PNG', optimize=True)
                    
                    final_size_bytes = os.path.getsize(output_path)
                    final_size_mb = final_size_bytes / (1024 * 1024)
                    print(f"Updated final size: {final_size_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"Error creating patched image: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Create larger test images by patching')
    parser.add_argument('--base-image', default='data/test.png', 
                       help='Path to base test image (default: data/test.png)')
    parser.add_argument('--sizes', nargs='+', type=int, default=[ 1, 5, 10, 20, 50],
                       help='Target sizes in MB (default: 1 5 10 20 50)')
    parser.add_argument('--padding', type=int, default=50,
                       help='Padding between images in pixels (default: 50)')
    
    args = parser.parse_args()
    
    # Check if base image exists
    if not os.path.exists(args.base_image):
        print(f"Error: Base image {args.base_image} not found")
        return 1
    
    print(f"Using base image: {args.base_image}")
    print(f"Target sizes: {args.sizes} MB")
    print(f"Padding: {args.padding} pixels")
    print("-" * 50)
    
    success_count = 0
    for size_mb in args.sizes:
        output_path = f"data/test_{size_mb}mb_patched.png"
        print(f"\nCreating {size_mb}MB image...")
        
        if create_patched_image(args.base_image, output_path, size_mb, args.padding):
            success_count += 1
    
    print(f"\n{'='*50}")
    print(f"Successfully created {success_count}/{len(args.sizes)} images")
    
    return 0 if success_count == len(args.sizes) else 1

if __name__ == '__main__':
    sys.exit(main())
