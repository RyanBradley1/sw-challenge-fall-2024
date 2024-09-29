'''
Cardinal Trading Group F24 Software Engineer Task
Ryan Bradley
9.28.24
'''

from CTG_Deliverable import DataLoader, DataCleaner, DataInterface

def main():
    ## User inputs
    directory = input("Enter the directory location: ")
    interval = input("Enter the aggregation interval (ex 15m, 1h, 1d): ")
    start_time = input("Enter start time (YYYY-MM-DD HH:MM:SS.SSS): ")
    end_time = input("Enter end time (YYYY-MM-DD HH:MM:SS.SSS): ")
    output_name = input("Enter the filename for the ouput CSV: ")

    ## Use methods
    loader = DataLoader(directory)
    data = loader.load_all_data()

    cleaner = DataCleaner(data)
    cleaned_data = cleaner.clean_data()

    interface = DataInterface(cleaned_data)
    ohlcv = interface.aggregate_data(interval, start_time, end_time)

    interface.save_to_csv(ohlcv, output_name)
    print("Data processing complete. Output saved to: " + output_name)


if __name__ == "__main__":
    main()

## Example 
## C:\\Users\\ryanp\\sw-challenge-fall-2024\\data
## 15m
## 2024-09-16 09:30:00.076
## 2024-09-16 12:54:00.035
## ctg-example-output.csv