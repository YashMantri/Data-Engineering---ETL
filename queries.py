import sqlite3
from datetime import date
from datetime import datetime
from multiprocessing.pool import ThreadPool
import concurrent.futures
from threading import Lock
lock = Lock()

class Database_Queries:
    def __init__(self, database_name, data):
        self.database_name = database_name
        self.data = data
        self.counties_list = self.data.keys()
        self.connectDatabase()
    
    # Connecting to sqlite
    def connectDatabase(self):
        self.conn = sqlite3.connect(self.database_name, check_same_thread = False)
    
    # Closing sqlite connection
    def closeDatabase(self):
        self.conn.close()
    
    # Create Table if not present for a given county
    def createTable(self, county):
        for county in self.counties_list:
            try:
                sql = "CREATE table if not exists '"+county+"' (id INTEGER PRIMARY KEY AUTOINCREMENT, test_date date, new_positives INTEGER, cum_new_positives INTEGER, total_positives INTEGER, cum_total_positives INTEGER, load_date date);"
                self.conn.execute(sql)
                self.conn.commit()
            except Exception as e:
                print("Error in Creating Table '"+county+"': "+str(e))

    # Sample Method to display all the tables of the DB
    def displayTables(self):
        self.connectDatabase()
        try:
            sql = "SELECT name from sqlite_master where type = 'table' and name not like 'sqlite_%';"
            table_list = self.conn.execute(sql).fetchall()
            self.conn.commit()
            table_list = [item for t in table_list for item in t]
            print("Displaying Tables List:")
            print(table_list)
        except Exception as e:
            print(str(e))
        self.closeDatabase()
    
    # Filter the data according to the given last date - Return all the data after the last date
    def getFilteredData(self, last_date, county_data):
        result = []
        for row in county_data:
            if row[0] > last_date:
                result.append(row)
        return result
    
    # Load data for the given county
    def LoadData(self, county):
        # Implementing lock on DB for the current thread
        lock.acquire(True)
        # print("Insert into '"+county+"', Length = "+str(len(self.data[county])))
        sql = "INSERT into '"+county+"' (test_date, new_positives, cum_new_positives, total_positives, cum_total_positives, load_date) values (?, ?, ?, ?, ?, ?);"
        try:
            # Create Table if not present
            self.createTable(county)
            # Get the last date for current county
            last_date_sql = "Select max(test_date) from '"+county+"';"
            last_date = self.conn.execute(last_date_sql).fetchall()[0][0]
            self.conn.commit()
            # Filter data from the last date for the current county
            if last_date is not None:
                filtered_data = self.getFilteredData(datetime.strptime(last_date, '%Y-%m-%d').date(), self.data[county])
            else:
                filtered_data = self.data[county]
            # If data is valid and not emtpy, insert into DB
            if len(filtered_data) > 0:
                self.conn.executemany(sql, self.data[county])
                self.conn.commit()
                sql = "Select * from '"+county+"';"
                county_result = self.conn.execute(sql).fetchall()
                self.conn.commit()
                # print("sql = "+sql+", county = "+county+"1, Length = "+str(len(county_result)))
                total_county_result = []
                for row in county_result:
                    total_county_result.append(row)
                # print("sql = "+sql+", county = "+county+", Length = "+str(len(total_county_result)))
            lock.release()
        except Exception as e:
            print(str(e))
        return county
    
    # Load Data using Multiple threads
    def MultipleLoad(self, number_of_threads):
        county_list = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=number_of_threads) as executor:
            # Load the data for each county in the current thread
            load_tables = {executor.submit(self.LoadData, county): county for county in self.counties_list}
            for table in concurrent.futures.as_completed(load_tables):
                county = load_tables[table]
                try:
                    data = table.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (county, exc))
                else:
                    county_list.append(county)
        print("Number of counties loaded: ", len(county_list))
    
    # Fetch data for the given county
    def FetchData(self, county):
        # Implementing lock on DB for the current thread
        lock.acquire(True)
        sql = "Select * from '"+county+"';"
        try:
            county_result = self.conn.execute(sql).fetchall()
            self.conn.commit()
            total_county_result = []
            # Iterate all the rows of the result
            for row in county_result:
                row_cols = []
                # Append each column for the row
                for inner_row in row:
                    row_cols.append(inner_row)
                total_county_result.append(row_cols)
            # print("sql = "+sql+", county = "+county+", Length = "+str(len(total_county_result)))
        finally:
            lock.release()
        return total_county_result

    # Fetch Data using Multiple threads
    def MultipleFetch(self, number_of_threads):
        result = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=number_of_threads) as executor:
            # Fetch the data for each county in the current thread
            load_tables = {executor.submit(self.FetchData, county): county for county in self.counties_list}
            for table in concurrent.futures.as_completed(load_tables):
                county = load_tables[table]
                try:
                    data = table.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (county, exc))
                else:
                    result[county] = data
        return result
    
    # Implementing Multi-threading to Load and Fetch Data
    def loadAndFetchData(self, number_of_threads):
        result = []
        try:
            # Load Data using Multiple threads
            self.MultipleLoad(number_of_threads)
        except Exception as exc:
            print('Error occured in Loading Data.')
        finally:
            # Fetch Data using Multiple threads
            result = self.MultipleFetch(number_of_threads)
        self.closeDatabase()
        return result