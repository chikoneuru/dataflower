from dataclasses import dataclass
import os
import sys


@dataclass
class DittoConfig:
    alpha: float = 1.0   # base compute cost factor
    beta: float = 0.1    # overhead term
    dop_min: int = 1
    dop_max: int = 8
    objective: str = "jct"  # jct or-cost
    debug_mode: bool = False  # enable debug logging and metrics
    debug_log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR
    debug_save_plans: bool = False  # save execution plans to files
    debug_metrics_file: str = "ditto_debug_metrics.json"  # file to save debug metrics
    request_timeout: int = 60  # HTTP request timeout in seconds

def _get_cli_log_level(argv: list[str]) -> str | None:
    """Get LOG_LEVEL from command line arguments (same as experiment_runner)."""
    try:
        for i, arg in enumerate(argv[1:]):
            if isinstance(arg, str):
                if arg.startswith('LOG_LEVEL='):
                    return arg.split('=', 1)[1].upper()
                if arg in ('--log-level', '-l') and i + 2 <= len(argv) - 1:
                    return argv[i + 2].upper()
                if arg.startswith('--log-level='):
                    return arg.split('=', 1)[1].upper()
    except Exception:
        pass
    return None

def get_ditto_config() -> DittoConfig:
    """Get Ditto configuration with simple LOG_LEVEL integration."""
    config = DittoConfig()
    
    # Check CLI first, then environment variable (same as experiment_runner)
    cli_level = _get_cli_log_level(sys.argv)
    log_level = (cli_level or os.getenv('LOG_LEVEL', 'WARNING')).upper()
    
    # Simple integration: if LOG_LEVEL=DEBUG, enable Ditto debug mode
    if log_level == 'DEBUG':
        config.debug_mode = True
        config.debug_log_level = 'DEBUG'
        config.debug_save_plans = True  # Enable plan saving in debug mode
    
    return config
