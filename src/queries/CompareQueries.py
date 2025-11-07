import yaml
import argparse
from difflib import unified_diff
from datetime import datetime

def load_yaml_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None

def normalize_query(query):
    if query is None:
        return ""
    # Remove leading/trailing whitespace and normalize internal whitespace
    return ' '.join(query.strip().split()).lower()

def compare_queries(ground_truth_file, llm_output_file, output_file='query_differences.txt'):
    # Load both YAML files
    ground_truth = load_yaml_file(ground_truth_file)
    llm_output = load_yaml_file(llm_output_file)
    
    if ground_truth is None or llm_output is None:
        print("Failed to load one or both files. Exiting.")
        return
    
    # Extract query results from both files
    gt_queries = {}
    if 'responses_results' in ground_truth:
        for item in ground_truth['responses_results']:
            gt_queries[item['id']] = item['query']
    
    llm_queries = {}
    if 'responses' in llm_output:
        for item in llm_output['responses']:
            llm_queries[item['id']] = item['query']
    
    # Open output file for writing differences
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write("=" * 80 + "\n")
        out_file.write("QUERY COMPARISON REPORT\n")
        out_file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out_file.write(f"Ground Truth File: {ground_truth_file}\n")
        out_file.write(f"LLM Output File: {llm_output_file}\n")
        out_file.write("=" * 80 + "\n")
        
        # Get all unique query IDs
        all_ids = sorted(set(list(gt_queries.keys()) + list(llm_queries.keys())))
        
        differences_found = False
        
        for query_id in all_ids:
            gt_query = gt_queries.get(query_id, "")
            llm_query = llm_queries.get(query_id, "")
            
            # Normalize queries for comparison
            gt_normalized = normalize_query(gt_query)
            llm_normalized = normalize_query(llm_query)
            
            # Check if queries are different
            if gt_normalized != llm_normalized:
                differences_found = True
                out_file.write(f"\n{'=' * 80}\n")
                out_file.write(f"DIFFERENCE FOUND - Query ID: {query_id}\n")
                out_file.write(f"{'=' * 80}\n\n")
                
                # Check for missing queries
                if not gt_query.strip():
                    out_file.write("WARNING: Query missing in ground truth file\n\n")
                if not llm_query.strip():
                    out_file.write("WARNING: Query missing in LLM output file\n\n")
                
                # Write ground truth query
                out_file.write("GROUND TRUTH QUERY:\n")
                out_file.write("-" * 80 + "\n")
                out_file.write(gt_query.strip() + "\n\n")
                
                # Write LLM generated query
                out_file.write("LLM GENERATED QUERY:\n")
                out_file.write("-" * 80 + "\n")
                out_file.write(llm_query.strip() + "\n\n")
                
                # Generate unified diff
                out_file.write("DETAILED DIFF:\n")
                out_file.write("-" * 80 + "\n")
                
                gt_lines = gt_query.strip().split('\n')
                llm_lines = llm_query.strip().split('\n')
                
                diff = unified_diff(
                    gt_lines,
                    llm_lines,
                    fromfile='Ground Truth',
                    tofile='LLM Output',
                    lineterm=''
                )
                
                diff_output = '\n'.join(diff)
                if diff_output:
                    out_file.write(diff_output + "\n")
                else:
                    out_file.write("(Whitespace differences only)\n")
                
                out_file.write("\n")
            else:
                out_file.write(f"\n{'=' * 80}\n")
                out_file.write(f"NO DIFFERENCE FOUND - Query ID: {query_id}\n")
                out_file.write(f"{'=' * 80}\n")
                
        
        # Summary
        out_file.write("\n" + "=" * 80 + "\n")
        out_file.write("SUMMARY\n")
        out_file.write("=" * 80 + "\n")
        out_file.write(f"Total queries compared: {len(all_ids)}\n")
        out_file.write(f"Queries in ground truth: {len(gt_queries)}\n")
        out_file.write(f"Queries in LLM output: {len(llm_queries)}\n")
        
        if differences_found:
            out_file.write(f"\nDifferences detected - see details above\n")
        else:
            out_file.write(f"\nAll queries match!\n")
    
    print(f"Comparison complete. Results written to '{output_file}'")
    
    if differences_found:
        print("Differences found between ground truth and LLM output.")
    else:
        print("All queries match!")

def main():
    parser = argparse.ArgumentParser(
        description='Compare LLM generated queries with ground truth queries',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'llm_file',
        type=str,
        help='Path to the LLM generated YAML file'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='../../output/queries/query_differences.txt',
        help='Path for the output difference report (default: query_differences.txt)'
    )
    
    args = parser.parse_args()
    
    # Ground truth file is fixed
    ground_truth_file = '../../prompts/queries/responses_groundTrue.yaml'
    
    # Display configuration
    print("=" * 80)
    print("QUERY COMPARISON TOOL")
    print("=" * 80)
    print(f"Ground Truth File: {ground_truth_file}")
    print(f"LLM Output File:   {args.llm_file}")
    print(f"Output Report:     {args.output}")
    print("=" * 80 + "\n")
    
    # Run comparison
    compare_queries(ground_truth_file, '../../output/responses/'+args.llm_file, args.output)

if __name__ == "__main__":
    main()