import re
from datetime import datetime
import pandas as pd
from collections import defaultdict

def parse_file(filename, output_csv):
    with open(filename, 'r') as f:
        lines = f.readlines()

    data = []
    current_time = None
    entries = []
    buffer_line = ""
    
    for line in lines:
        line = line.strip()
        
        #timestamp 
        if line.startswith("Saturday"):
            current_time = datetime.strptime(line, "%A %d %B %Y %H:%M")
        
        
        elif line.startswith("Int"):
            # If there's a leftover from the previous line, process it
            if buffer_line:
                entries.append(buffer_line.strip())
            buffer_line = line  # Start new block

        #continue of previous intersection block
        elif line and not line.startswith("end of file"):
            buffer_line += " " + line

        elif line == "end of file":
            if buffer_line:
                entries.append(buffer_line.strip())
                buffer_line = ""  # Clear after appending

    #add buffer if not added 
    if buffer_line:
        entries.append(buffer_line.strip())

   
    parsed_data = defaultdict(list)

    for entry in entries:
        match = re.match(r"Int\s*(\d+)", entry)
        if not match:
            continue
        
        inter_id = match.group(1)
        values = re.findall(r"=\s*(\d+|NA)", entry)
        
        clean_vals = []
        for val in values:
            if val == "NA":
                continue
            clean_vals.append(int(val))

        parsed_data[inter_id].append((current_time, clean_vals))

    cleaned_rows = []

    for inter_id, time_series in parsed_data.items():
        #get all valid entries (between 1 and 500) for stats
        all_vals = [v for _, val_list in time_series for v in val_list if 0 < v <= 500]
        if not all_vals:
            continue  # Skip if all values are invalid

        avg_val = round(sum(all_vals) / len(all_vals), 2)
        median_val = round(pd.Series(all_vals).median(), 2)

        for dt, vals in time_series:
            fixed_vals = []
            for v in vals:
                if v == 0:
                    fixed_vals.append(avg_val)
                elif v == 2047 or v > 500:
                    fixed_vals.append(median_val)
                else:
                    fixed_vals.append(v)
            total = round(sum(fixed_vals), 2)
            cleaned_rows.append([inter_id, dt.strftime("%Y-%m-%d %H:%M:%S"), total])

    df = pd.DataFrame(cleaned_rows, columns=["Intersection_ID", "Datetime", "Total_Cars"])
    df.to_csv(output_csv, index=False)
    print(f"Cleaned data saved to: {output_csv}")


parse_file("2018-December.txt", "cleaned_data.csv")
