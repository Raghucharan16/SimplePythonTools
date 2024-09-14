import csv

# Define seat mappings
seat_mappings = {
    'FRONT': [9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32],
    'MIDDLE': [1,2,3,4,5,6,7,8],
    'FIFTH': [33,34,35,36,37,39],
    'DOUBLE': [41,42,44,45],
    'BACK': [43,38]
}

# Input and output file names
input_file = 'fares.csv'
output_file = 'output.csv'

# Read input CSV and write to output CSV
with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Write header
    header = next(reader)
    writer.writerow(header)
    
    # Process rows
    for row in reader:
        route, classification, base_fare, seat_bucket = row
        if seat_bucket in seat_mappings:
            for seat_number in seat_mappings[seat_bucket]:
                writer.writerow([route, classification, base_fare, seat_number])
        else:
            writer.writerow(row)

print(f"Processing complete. Output written to {output_file}")
