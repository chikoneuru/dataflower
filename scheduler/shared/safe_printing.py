"""
Safe printing utilities for scheduler components.

Provides functions to safely print data structures without flooding the terminal
with large binary data like base64 images or hex strings.
"""

from typing import Any


def is_binary_field(value: str) -> bool:
    """
    Check if a string field contains binary data that should be truncated.
    
    Args:
        value: String to check
        
    Returns:
        True if the string appears to contain binary data
    """
    # Check for common binary field patterns
    binary_patterns = [
        '_b64', '_img_b64', '_image_b64', '_data_b64',  # Base64 image fields
        '_hex', '_binary', '_data', '_payload',          # Binary data fields
        'img', 'image', 'photo', 'picture',              # Image fields
        'file', 'content', 'body'                        # File/content fields
    ]
    
    # Check if the string looks like base64 (alphanumeric + / + = padding)
    if len(value) > 100:  # Only check long strings
        base64_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=')
        if all(c in base64_chars for c in value):
            return True
    
    return False


def safe_print_value(value: Any, max_length: int = 50) -> Any:
    """
    Safely format a value for printing by truncating binary fields longer than max_length.
    
    Args:
        value: Value to format for printing
        max_length: Maximum length before considering truncation
        
    Returns:
        Safely formatted value for printing
    """
    if isinstance(value, str) and len(value) > max_length:
        # Check if this looks like binary data (base64, hex, etc.)
        if is_binary_field(value):
            return f"<binary data length: {len(value)}>"
        else:
            # Keep non-binary strings intact
            return value
    elif isinstance(value, dict):
        # Recursively apply to dict values
        safe_dict = {}
        for k, v in value.items():
            safe_dict[k] = safe_print_value(v, max_length)
        return safe_dict
    elif isinstance(value, list):
        # Recursively apply to list values
        safe_list = []
        for item in value:
            safe_list.append(safe_print_value(item, max_length))
        return safe_list
    else:
        return value


def safe_print_dict(data: dict, max_length: int = 50) -> dict:
    """
    Convenience function to safely print a dictionary.
    
    Args:
        data: Dictionary to format
        max_length: Maximum length before considering truncation
        
    Returns:
        Safely formatted dictionary
    """
    return safe_print_value(data, max_length)


def safe_print_list(data: list, max_length: int = 50) -> list:
    """
    Convenience function to safely print a list.
    
    Args:
        data: List to format
        max_length: Maximum length before considering truncation
        
    Returns:
        Safely formatted list
    """
    return safe_print_value(data, max_length)
