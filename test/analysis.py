import couchdb
import numpy as np
import matplotlib.pyplot as plt
import os
from collections import defaultdict
import re

# CouchDB connection
COUCHDB_URL = 'http://openwhisk:openwhisk@127.0.0.1:5984/'
DB_LATENCY = 'workflow_latency'
DB_RESULTS = 'results'

fig_dir = 'test/figs'
os.makedirs(fig_dir, exist_ok=True)

def get_db(db_name):
    server = couchdb.Server(COUCHDB_URL)
    return server[db_name]

def fetch_docs(db):
    return [doc for doc in db]

def extract_rpm_from_request_id(request_id):
    """Extract RPM from request_id like 'rate_test_005rpm_request_0001'"""
    match = re.search(r'(\d+)rpm', request_id)
    if match:
        return int(match.group(1))
    return None

def plot_latency(latency_by_rpm):
    """Plot latency vs request number with multiple RPM lines per block type"""
    
    # Get all unique block types
    all_blocks = set()
    for rpm_data in latency_by_rpm.values():
        all_blocks.update(rpm_data.keys())
    
    # Plot each block type separately
    for block_key in sorted(all_blocks):
        plt.figure(figsize=(14, 8))
        
        colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray']
        rpm_list = sorted(latency_by_rpm.keys())
        
        for i, rpm in enumerate(rpm_list):
            latencies = latency_by_rpm[rpm].get(block_key, [])
            
            if not latencies:
                continue
                
            color = colors[i % len(colors)]
            
            # Create request numbers (1, 2, 3, ...)
            request_numbers = list(range(1, len(latencies) + 1))
            
            # Plot line graph
            plt.plot(request_numbers, latencies, 
                    color=color, 
                    marker='o', 
                    markersize=3,
                    linewidth=1.5,
                    alpha=0.8,
                    label=f'{rpm} RPM (n={len(latencies)})')
        
        plt.title(f'Latency vs Request Number: {block_key}', fontsize=14, fontweight='bold')
        plt.xlabel('Request Number', fontsize=12)
        plt.ylabel('Execution Time (seconds)', fontsize=12)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save figure
        filename = f'{fig_dir}/latency_vs_request_{block_key}.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"💾 Saved: {filename}")
        plt.show()
    
    # Create combined overview plot - one subplot per block
    if len(all_blocks) > 1:
        plt.figure(figsize=(15, 5 * len(all_blocks)))
        
        for idx, block_key in enumerate(sorted(all_blocks)):
            plt.subplot(len(all_blocks), 1, idx + 1)
            
            rpm_list = sorted(latency_by_rpm.keys())
            colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray']
            
            for i, rpm in enumerate(rpm_list):
                latencies = latency_by_rpm[rpm].get(block_key, [])
                
                if not latencies:
                    continue
                    
                color = colors[i % len(colors)]
                request_numbers = list(range(1, len(latencies) + 1))
                
                plt.plot(request_numbers, latencies,
                        color=color,
                        marker='o',
                        markersize=2,
                        linewidth=1,
                        alpha=0.8,
                        label=f'{rpm} RPM')
            
            plt.title(f'{block_key}', fontsize=12, fontweight='bold')
            plt.xlabel('Request Number')
            plt.ylabel('Latency (s)')
            plt.legend()
            plt.grid(True, alpha=0.3)
        
        plt.suptitle('Latency vs Request Number - All Blocks', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        # Save combined figure
        filename = f'{fig_dir}/latency_vs_request_overview.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"💾 Saved: {filename}")
        plt.show()

def analyze_latency(results_docs):
    """Analyze latency by RPM and template blocks from results database"""
    latency_by_rpm = defaultdict(lambda: defaultdict(list))
    
    print("📊 Analyzing execution latencies from results database...")
    
    # Group documents by RPM first to maintain request order
    docs_by_rpm = defaultdict(list)
    
    for doc in results_docs:
        request_id = doc.get('_id', '')
        rpm = extract_rpm_from_request_id(request_id)
        
        if rpm is None:
            continue
            
        docs_by_rpm[rpm].append(doc)
    
    # Sort documents within each RPM group by request ID to maintain order
    for rpm in docs_by_rpm:
        docs_by_rpm[rpm].sort(key=lambda x: x.get('_id', ''))
    
    # Extract latencies in order
    for rpm, docs in docs_by_rpm.items():
        for doc in docs:
            executions = doc.get('executions', [])
            
            for execution in executions:
                template_name = execution.get('template_name', '')
                block_name = execution.get('block_name', '')
                execution_time = execution.get('execution_time', 0)
                status = execution.get('status', '')
                
                if status == 'success' and execution_time > 0:
                    block_key = f"{template_name}_{block_name}"
                    latency_by_rpm[rpm][block_key].append(execution_time)
    
    # Print summary statistics
    print("\n📈 Latency Summary by RPM and Block:")
    for rpm in sorted(latency_by_rpm.keys()):
        print(f"\n🎯 {rpm} RPM:")
        for block_key, latencies in latency_by_rpm[rpm].items():
            if latencies:
                mean_lat = np.mean(latencies)
                std_lat = np.std(latencies)
                min_lat = np.min(latencies)
                max_lat = np.max(latencies)
                count = len(latencies)
                print(f"   {block_key}: {mean_lat:.4f}s ± {std_lat:.4f}s (min={min_lat:.4f}, max={max_lat:.4f}, n={count})")
    
    return latency_by_rpm

def plot_word_counts(total_words, unique_words):
    """Plot word count statistics"""
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.hist(total_words, bins=20, color='lightgreen', edgecolor='black')
    plt.title('Total Words Distribution')
    plt.xlabel('Total Words')
    plt.ylabel('Frequency')
    
    plt.subplot(1, 2, 2)
    plt.hist(unique_words, bins=20, color='lightcoral', edgecolor='black')
    plt.title('Unique Words Distribution')
    plt.xlabel('Unique Words')
    plt.ylabel('Frequency')
    
    plt.tight_layout()
    
    # Save figure
    filename = f'{fig_dir}/word_count_distribution.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"💾 Saved: {filename}")
    plt.show()

def analyze_word_counts(results_docs):
    """Analyze word count results"""
    total_words = []
    unique_words = []
    for doc in results_docs:
        res = doc.get('workflow_results', {})
        if 'total_words' in res and 'unique_words' in res:
            total_words.append(res['total_words'])
            unique_words.append(res['unique_words'])
    return total_words, unique_words

def main():
    print("🔍 Starting latency analysis...")
    
    db_latency = get_db(DB_LATENCY)
    db_results = get_db(DB_RESULTS)

    latency_docs = [db_latency[doc_id] for doc_id in db_latency]
    results_docs = [db_results[doc_id] for doc_id in db_results]

    print(f"📄 Found {len(results_docs)} result documents")

    # Latency analysis from results database (executions)
    latency_by_rpm = analyze_latency(results_docs)
    
    if latency_by_rpm:
        plot_latency(latency_by_rpm)
    else:
        print("❌ No execution latency data found in results database")

    # # Word count analysis
    # total_words, unique_words = analyze_word_counts(results_docs)
    # if total_words and unique_words:
    #     print(f"\n📊 Word count stats: mean_total={np.mean(total_words):.2f}, mean_unique={np.mean(unique_words):.2f}")
    #     plot_word_counts(total_words, unique_words)
    # else:
    #     print("❌ No word count data found")

if __name__ == "__main__":
    main()