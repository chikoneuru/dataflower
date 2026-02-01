# Serverless Function Orchestration - Experiment Guide

This guide provides comprehensive instructions for running experiments with the serverless function orchestration system.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Experiment Types](#experiment-types)
3. [Running Experiments](#running-experiments)
4. [Scale Testing](#scale-testing)
5. [Understanding Results](#understanding-results)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Configuration](#advanced-configuration)

## Quick Start

### Prerequisites

- Python 3.8+
- Docker containers running for the recognizer functions
- Required dependencies installed

### Basic Experiment

Run a standard experiment with default settings:

```bash
python -m integration.experiment_runner
```

This will run:
- Input sizes: 0.1MB, 1.0MB, 2.0MB
- Concurrency levels: 1, 5, 10
- Scheduler: "ours"
- Total experiments: 9

## Experiment Types

### 1. Standard Experiments

**Purpose**: Baseline performance testing with typical workloads

**Configuration**:
- Input sizes: 0.1MB, 1.0MB, 2.0MB
- Concurrency: 1, 5, 10
- Requests per experiment: 10

**Command**:
```bash
python -m integration.experiment_runner
# or
python -m integration.experiment_runner standard
```

### 2. Scale Testing

**Purpose**: Performance testing under extreme conditions

**Configuration**:
- Large input sizes: 5MB, 10MB
- High concurrency: 10, 20, 50
- Extreme combinations
- Total experiments: 16

**Command**:
```bash
python -m integration.experiment_runner scale
```

## Running Experiments

### Standard Experiment Runner

The main experiment runner supports different modes:

```bash
# Standard experiments (default)
python -m integration.experiment_runner

# Scale testing
python -m integration.experiment_runner scale
```

### Scale Testing Mode

For comprehensive scale testing, use the scale mode:

```bash
python -m integration.experiment_runner scale
```

This mode:
- Creates large test images (5MB, 10MB) if they don't exist
- Runs comprehensive scale testing
- Provides detailed analysis
- Saves results with scale testing prefix

## Scale Testing

### What is Scale Testing?

Scale testing evaluates system performance under extreme conditions:

1. **Large Input Sizes**: Tests with 5MB and 10MB input data
2. **High Concurrency**: Tests with 10, 20, and 50 concurrent requests
3. **Extreme Combinations**: Large inputs + high concurrency
4. **Baseline Comparison**: Standard tests for comparison

### Scale Testing Categories

#### Standard Performance (Baseline)
- Input: 0.1MB, 1.0MB, 2.0MB
- Concurrency: 1, 5, 10
- Purpose: Establish baseline performance

#### Large Input Size Impact
- Input: 5MB, 10MB
- Concurrency: 1, 5, 10
- Purpose: Test data processing scalability

#### High Concurrency Impact
- Input: 0.1MB, 1.0MB, 2.0MB
- Concurrency: 10, 20, 50
- Purpose: Test concurrent request handling

#### Extreme Scale Performance
- Input: 5MB, 10MB
- Concurrency: 10, 20, 50
- Purpose: Test system limits

### Expected Scale Testing Results

**Success Indicators**:
- High success rates (>90%) across all categories
- Reasonable throughput degradation with larger inputs
- Graceful handling of high concurrency
- No system crashes or timeouts

**Performance Patterns**:
- Throughput typically decreases with larger inputs
- Concurrency may improve or degrade throughput depending on system capacity
- Extreme combinations test system limits

## Understanding Results

### Output Format

Each experiment produces:

```
🧪 Running experiment: exp_input1.0mb_conc5_ours
   📊 Input size: 1.0MB
   🔄 Concurrency: 5
   📈 Total requests: 7
   ⚙️  Scheduler: ours
✅ Loaded test image: 2285881 bytes from data/test.png
   🎯 Functions: recognizer__upload, recognizer__adult, recognizer__violence, recognizer__extract, recognizer__translate, recognizer__censor, recognizer__mosaic
   ✅ Success: 100.0%, Avg time: 0.0ms, Throughput: 33505.2 req/s
```

### Key Metrics

1. **Success Rate**: Percentage of successful function executions
2. **Average Execution Time**: Mean time per function execution (ms)
3. **Throughput**: Requests processed per second (req/s)

### Result Files

Results are saved to `results/` directory:
- Standard: `experiment_results_[timestamp].json`
- Scale testing: `experiment_results_[timestamp].json` (with scale testing prefix)

### Analysis Categories

#### Standard Performance
- Baseline metrics for comparison
- Typical workload performance
- System stability indicators

#### Large Input Impact
- Data processing scalability
- Memory usage patterns
- I/O performance characteristics

#### High Concurrency Impact
- Concurrent request handling
- Resource contention effects
- Load balancing effectiveness

#### Extreme Scale
- System limits and bottlenecks
- Failure modes and recovery
- Performance degradation patterns

## Troubleshooting

### Common Issues

#### 1. Low Success Rates (<50%)

**Symptoms**: Many failed function executions
**Causes**:
- Container connectivity issues
- Resource constraints
- Payload size limits

**Solutions**:
```bash
# Check container status
docker ps

# Verify function availability
python -m integration.test_function_execution

# Test with smaller payloads
python -m integration.test_working_functions
```

#### 2. High Execution Times

**Symptoms**: Execution times >1000ms
**Causes**:
- Resource contention
- Network latency
- Large payload processing

**Solutions**:
- Reduce concurrency levels
- Use smaller input sizes
- Check system resources

#### 3. Zero Throughput

**Symptoms**: 0.0 req/s throughput
**Causes**:
- All requests failing
- Measurement errors
- System overload

**Solutions**:
- Check success rates
- Verify container health
- Reduce load

#### 4. Scale Testing Failures

**Symptoms**: Failures in extreme conditions
**Causes**:
- System resource limits
- Memory constraints
- Network timeouts

**Solutions**:
- Increase system resources
- Reduce extreme test parameters
- Check container limits

### Debug Commands

```bash
# Test individual functions
python -m integration.test_function_execution

# Test working functions only
python -m integration.test_working_functions

# Check container connectivity
docker ps
docker logs [container_id]

# Monitor system resources
htop
docker stats
```

## Advanced Configuration

### Custom Experiment Configurations

Modify `integration/experiment_runner.py` to create custom configurations:

```python
def create_custom_configs():
    configs = []
    
    # Custom input sizes
    input_sizes = [0.5, 2.0, 5.0]
    
    # Custom concurrency levels
    concurrency_levels = [2, 8, 16]
    
    # Custom schedulers
    schedulers = ["ours", "faaspr"]
    
    for scheduler in schedulers:
        for input_size in input_sizes:
            for concurrency in concurrency_levels:
                config = ExperimentConfig(
                    experiment_id=f"custom_input{input_size}mb_conc{concurrency}_{scheduler}",
                    input_size_mb=input_size,
                    concurrency_level=concurrency,
                    total_requests=10,  # More requests
                    scheduler_type=scheduler,
                    workflow_name="recognizer"
                )
                configs.append(config)
    
    return configs
```

### Environment Variables

Set environment variables for configuration:

```bash
# Set log level
export LOG_LEVEL=DEBUG

# Set experiment parameters
export DEFAULT_INPUT_SIZE=1.0
export DEFAULT_CONCURRENCY=5
export DEFAULT_REQUESTS=10
```

### Custom Test Images

Create custom test images for specific scenarios:

```python
# Create custom test image
def create_custom_test_image(size_mb, filename):
    data = b"X" * (size_mb * 1024 * 1024)
    with open(f"data/{filename}", "wb") as f:
        f.write(data)
    print(f"Created {size_mb}MB test image: data/{filename}")

# Usage
create_custom_test_image(2.5, "test_2.5mb.png")
```

## Best Practices

### 1. Experiment Design

- Start with standard experiments for baseline
- Gradually increase scale parameters
- Test one variable at a time
- Document all configuration changes

### 2. Resource Management

- Monitor system resources during experiments
- Ensure adequate memory and CPU
- Use appropriate concurrency levels
- Clean up resources after experiments

### 3. Data Collection

- Save all experiment results
- Include timestamps and configurations
- Document environmental conditions
- Track system resource usage

### 4. Analysis

- Compare results across different configurations
- Look for performance patterns and trends
- Identify bottlenecks and limitations
- Document findings and recommendations

## Support

For issues or questions:

1. Check the troubleshooting section
2. Review experiment logs
3. Verify system requirements
4. Check container health
5. Consult the main documentation

---

**Happy Experimenting!** 🚀
