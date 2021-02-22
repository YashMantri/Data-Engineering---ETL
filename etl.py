import urllib.request as urllib
import json
from datetime import date
from datetime import datetime
from time import time
from queries import Database_Queries

class Health_Data_NY:
    def __init__(self, source_data_url, db_name, columns_required, number_of_threads):
        self.source_data_url = source_data_url
        self.db_name = db_name
        self.columns_required = columns_required
        self.number_of_threads = number_of_threads
        self.column_list = []
        self.extracted_data = None
        self.transformed_data = None

    def etl(self):
        start_time = time()
        # 1 - Extraction
        extracted_data = self.extraction()
        if extracted_data["status"] == "error":
            print("Error in Extraction: "+extracted_data["message"])
            return
        
        self.extracted_data = extracted_data["file_data"]
        if self.extracted_data is None or len(self.extracted_data) == 0:
            print("No Records present.")
            return
        
        # 2 - Transformation
        self.transformed_data = self.transformation()
        if self.transformed_data is None or len(self.transformed_data) == 0:
            print("Error in Transforming Data. Please Check!")
            return
        print("Length of transformed data (Number of counties) = "+str(len(self.transformed_data)))

        # 3 - Loading
        self.loading()
        end_time = time()
        print("Time taken to perform ETL = "+str(end_time - start_time))
    
    # 1 - Extract Data from given URL
    def extraction(self):
        result = {}
        try:
            # Read data from URL
            f = urllib.urlopen(self.source_data_url)
            # print(f)
            file_data = json.loads((f.read()).decode('utf8').replace("'", '"'))
            result["status"] = "success"
            result["file_data"] = file_data
        except Exception as e:
            print(e)
            # Exception Handling
            result["status"] = "error"
            result["message"] = str(e)
        return result    

    # 2 - Transform Data to the required format
    def transformation(self):
        # Retrieve column names
        column_names = self.extracted_data["meta"]['view']['columns']
        for column in column_names:
            self.column_list.append(column["name"])
        
        # Match row-data with column names
        json_data = self.extracted_data["data"]
        result = {}
        for json_row in json_data:
            current_row_data = {}
            for index in range(len(json_row)):
                current_row_data[self.column_list[index]] = json_row[index]
            
            # Row-level data for each county
            if current_row_data["County"] not in result:
                result[current_row_data["County"]] = []
            test_date = (datetime.strptime(current_row_data["Test Date"], '%Y-%m-%dT%H:%M:%S')).date()
            result[current_row_data["County"]].append([test_date, current_row_data["New Positives"], current_row_data["Cumulative Number of Positives"], current_row_data["Total Number of Tests Performed"], current_row_data["Cumulative Number of Tests Performed"], date.today()])
            
            # Data for each key in data for a given county
            # if current_row_data["County"] not in result:
            #     result[current_row_data["County"]] = {}
            #     result[current_row_data["County"]]["test_date"] = []
            #     result[current_row_data["County"]]["new_positives"] = []
            #     result[current_row_data["County"]]["cum_new_positives"] = []
            #     result[current_row_data["County"]]["total_positives"] = []
            #     result[current_row_data["County"]]["cum_total_positives"] = []
            #     result[current_row_data["County"]]["load_date"] = []
            
            # test_date = str(datetime.strptime(current_row_data["Test Date"], '%Y-%m-%dT%H:%M:%S').date())
            # result[current_row_data["County"]]["test_date"].append(test_date)
            # result[current_row_data["County"]]["new_positives"].append(current_row_data["New Positives"])
            # result[current_row_data["County"]]["cum_new_positives"].append(current_row_data["Cumulative Number of Positives"])
            # result[current_row_data["County"]]["total_positives"].append(current_row_data["Total Number of Tests Performed"])
            # result[current_row_data["County"]]["cum_total_positives"].append(current_row_data["Cumulative Number of Tests Performed"])
            # result[current_row_data["County"]]["load_date"].append(str(date.today()))
        return result
    
    # 3 - Load the data to SQLite DB
    def loading(self):
        # Create DB tables
        db = Database_Queries(self.db_name, self.transformed_data)
        print("Transformed data length = "+str(len(self.transformed_data)))

        # Implementing Multi-threading to Load and Fetch Data
        print("Number of threads = "+str(self.number_of_threads))
        result = db.loadAndFetchData(self.number_of_threads)
        print("Length of fetch result = "+str(len(result)))
        # # Check Length of Data in each County
        # for county in result:
        #     print("county = "+county+", Length = "+str(len(result[county])))

if __name__ == '__main__':
    # Base Data - config
    source_data_url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    db_name = "ETL_Health_data_NY.db"
    number_of_threads = 31
    columns_required = ["Test_Date", "New_Positives", "Cumulative_New_Positives", "Total_Tests_Performed", "Cumulative_Tests_Performed", "Load_Date"]
    # Create object and perform ETL
    etl_obj = Health_Data_NY(source_data_url, db_name, columns_required, number_of_threads)
    etl_obj.etl()
