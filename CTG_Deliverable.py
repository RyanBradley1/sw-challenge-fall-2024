'''
Cardinal Trading Group F24 Software Engineer Task
Ryan Bradley
9.25.24

1. Data Loading
    Load & combine CSV files containing ticker data 
2. Data Cleaning
    Identify 4 issues in data
    Create pipeline to address issues
3. Data Transformation
    Interface that accepts time intervals 
    Generates flat files with OHLCV bars

'''
import os
import csv
import datetime

## 1 - Data Loading
#
# Basic csv reader method is called by method that parses designated directory
# 
# Current limitations:
#   Only considers that Price may be missing data, does not currently handle potential Timestamp, Size columns

class DataLoader: 

    def __init__(self, directory) :
        self.directory = directory 

    ## reads individual csv file 
    def read_csv(self, filepath) :
        data = []
        with open (filepath, 'r', newline='') as file: 
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    ## if price row is empty sets price to 0.0 for time being
                    price = float(row['Price'].strip()) if row['Price'].strip() else 0.0
                    dataline = {
                        'Timestamp': datetime.datetime.strptime(row['Timestamp'], '%Y-%m-%d %H:%M:%S.%f'),
                        'Price': price,
                        'Size': int(row['Size'])
                    }
                    data.append(dataline)

                ## handles instances where data does not fit assigned value type   
                except ValueError as e:
                    print(f"data type error in file {filepath}: {e}")
                ## handles missing data 
                except IndexError as e:
                    print(f"index error in file {filepath}: {e}")
        return data

    
    ## combines all csv files by calling read_csv and returns entire dataset 
    def load_all_data(self):

        ## data from all csv files
        all_data = []
        for filename in os.listdir(self.directory):
            if (filename.endswith('.csv')):
                filepath = os.path.join(self.directory, filename)
                try: 
                    file_data = self.read_csv(filepath)
                    all_data.extend(file_data)

                ## general error handling 
                except Exception as e:
                    print(f"error reading {filename}: {e}")
        return all_data




## 2 - Data Cleaning
#
# Errors found:
# 1: Missing values (price)
# 2: Negative values (price)
# 3: Price value errors (1off decimal point)
# 4: ?
#
# Current limitations:
#   Assumes any negative values just need to be swapped positive 
#   Assumes first 'Price' value will never be empty (0.0 via DataLoader)
#   For all 'Price' values missing data (turned to 0.0 by DataLoader),
#       the value is simply set to the price from the previous timestamp. 
#       This is not a comprehensive solution, but without knowing the end usage of the
#       OHLCV data it is difficult to come up with a more appropriate solution. 
#       Other possible ways to handle this would be to get rid of this error-filled dataline
#       completely, use the avereage of surrounding prices assuming a return-to-mean type of idea
#       or a more complex calculation of where price action moved at said time. 
#   Finally, I found multiple instances of decimal places being 1 slot off, so I simply multiplied 
#       these values by 10 and checked if this value seemed plausable. If it was not, similarily to
#       the missing values I just set the price to previous price. Same limitations apply. 

class DataCleaner: 
    def __init__(self, data):
        self.data = data

    def clean_data(self):
        self.correct_negatives()
        self.correct_outliers()
        return self.data
        
    ## Swaps negative values into positive values
    def correct_negatives(self):
        for dataline in self.data:
            if dataline['Price'] < 0.0:
                dataline['Price'] = (abs(dataline['Price']))

    ## empty price values and outlying price values
    def correct_outliers(self):
        prev_price = 0.0
        for dataline in self.data:
            current_price = dataline['Price']
            ## set empty price vals to previous price 
            if current_price == 0.0:
                dataline['Price'] = prev_price
            else:
                ## price outliers: try *10, else set to prev
                if (abs(current_price - prev_price) > 10 and prev_price != 0):
                    if (abs(current_price*10.0 - prev_price) < 1):
                        dataline['Price'] = current_price*10
                    else:
                        dataline['Price'] = prev_price

            prev_price = dataline['Price']




## 3 - Data Interface      
#   
# Current Limitations:
#   No error handling for parse interval and improper time interval inputs
#   

class DataInterface:
    def __init__(self, data):
        self.data = data

    def parse_interval(self, interval_str):
        seconds = 0
        number = ''
        units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400} #seconds in each unit

        for char in interval_str:
            if char.isdigit():
                number += char 
            else:
                if number:
                    seconds += int(number) * units[char]
                number = ''

        return datetime.timedelta(seconds=seconds)
    

    def aggregate_data(self, interval, start_time, end_time):
        interval_delta = self.parse_interval(interval)
        start_dt = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
        end_dt = datetime.datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S.%f')

        current_start = start_dt
        ag_data = []

        while current_start < end_dt:
            current_end = current_start + interval_delta
            filtered_data = [
                d for d in self.data if current_start <= d['Timestamp'] < current_end
            ]

            if filtered_data:
                ## OHLCV logic
                open = filtered_data[0]['Price']
                high = max(d['Price'] for d in filtered_data)
                low = min(d['Price'] for d in filtered_data)
                close = filtered_data[-1]['Price']
                volume = sum(int(d['Size']) for d in filtered_data)

                ## OHLCV bars 
                ag_data.append({
                    'Timestamp': current_start.strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'Open': open,
                    'High': high,
                    'Low': low,
                    'Close': close,
                    'Volume': volume,
                })
            current_start = current_end
        return ag_data
    
    ## basic save to csv
    def save_to_csv(self, data, filename):
        with open(filename, 'w') as file:
            headers = data[0].keys()
            file.write(','.join(headers) + '\n')
            for row in data:
                file.write(','.join(str(row[h]) for h in headers) + '\n')