import csv
import os

def split_csv_by_chunks(dataset,ending, output_folder, num_files):
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Open the input file
    with open(os.path.join(output_folder,dataset+ending+'.csv'), 'r') as infile:
        reader = csv.reader(infile)
        
        # Get the header and determine total rows in the file
        header = next(reader)
        total_rows = sum(1 for _ in reader)
        infile.seek(0)
        next(reader)  # Skip header again
        
        # Calculate rows per file
        rows_per_file = total_rows // num_files
        remainder = total_rows % num_files
        
        # Reset file pointer
        infile.seek(0)
        next(reader)  # Skip header again
        
        # Split into chunks
        for i in range(num_files):
            output_file = os.path.join(output_folder, f'{dataset}-{i + 1}{ending}.csv')
            
            # Determine rows for this chunk
            rows_to_write = rows_per_file + (1 if i < remainder else 0)
            
            # Write the chunk
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                
                # Write the header
                writer.writerow(header)
                
                # Write the rows
                for _ in range(rows_to_write):
                    try:
                        writer.writerow(next(reader))
                    except StopIteration:
                        break
        
        print(f"CSV successfully split into {num_files} files in the folder '{output_folder}'.")


output_folder = '/p/projects/eubucco/data/1-intermediary-outputs-v0_1/france'    
num_files = 20
dataset = 'france-gov'

for ending in ('-3035_geoms','_attrib','_extra_attrib'):  
                      
    split_csv_by_chunks(dataset, ending, output_folder, num_files)