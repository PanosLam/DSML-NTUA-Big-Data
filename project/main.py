import sys

from timeit import default_timer as timer
from questions import question_a, question_b, question_c, question_d, question_a_sql, question_b_rdd, question_d_rdd
from transformations import from_csv_to_parquet, merge_files_into_one


FILENAME = 'Crime_Data_all.csv'


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    option = None
    if len(sys.argv) > 1:
        option = str(sys.argv[1])

    # # uncomment only for windows
    # import os
    # os.environ["HADOOP_HOME"] = r"C:/Users/panos/winutils"

    print(f"Execution option: '{option}'.")
    start = timer()
    if (option is None) or (option == 'A'):
        question_a()
    elif option == 'A-SQL':
        question_a_sql()
    elif option == 'B':
        question_b()
    elif option == 'B-RDD':
        question_b_rdd()
    elif option == 'C':
        question_c()
    elif option == 'D':
        question_d()
    elif option == 'D-RDD':
        question_d_rdd()
    elif option == 'transform':
        from_csv_to_parquet(FILENAME)
    elif option == 'merge':
        merge_files_into_one('/Crime_Data_from_2010_to_2019.csv',
                             '/Crime_Data_from_2020_to_Present.csv',
                             '/Crime_Data_all.csv')
    else:
        print(f'Option: {option} does not exist')
    end = timer()
    print(f'Total time elapsed: {round(end-start, 3)} seconds.')

