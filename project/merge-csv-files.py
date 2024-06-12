import csv

file1 = r'C:\Users\panos\Documents\PhD\courses\semester-4\Crime_Data_from_2010_to_2019.csv'
file2 = r'C:\Users\panos\Documents\PhD\courses\semester-4\Crime_Data_from_2020_to_Present.csv'
output_file = r'C:\Users\panos\Documents\PhD\courses\semester-4\Crime_Data_all.csv'


def merge_csv(file1, file2, output_file):
    with open(file1, 'r', newline='') as csv_file1, \
            open(file2, 'r', newline='') as csv_file2, \
            open(output_file, 'w', newline='') as output_csv:

        reader1 = csv.reader(csv_file1)
        reader2 = csv.reader(csv_file2)
        writer = csv.writer(output_csv)

        # Copy headers from the first file
        headers = next(reader1)
        writer.writerow(headers)

        # Write rows from the first file
        for row in reader1:
            writer.writerow(row)

        # Skip the first row from the second file
        next(reader2)

        # Write rows from the second file
        for row in reader2:
            writer.writerow(row)


merge_csv(file1, file2, output_file)
