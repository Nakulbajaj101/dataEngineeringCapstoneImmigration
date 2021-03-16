from datetime import datetime


def print_unique_records(df):
    """
    Takes in a dataframe and prints out the total length of the
    dataframe and number of unique rows for each column
    """
    
    data = df.copy()
    print("\nLength of dataframe is {}".format(data.shape[0]))
    for col in data:
        print("The number of unique rows for column {} are {}".format(col, len(data[col].unique())))
        

def clean_latitude_longitude(value):
    """
    Takes in the latitude longitude value with direction symbol 
    and converts to float and returns the south latitude and 
    west longitude as a negative number.
    """
    
    if any([val for val in value if val in ["N","E"]]):
        return float(value.replace("N","0").replace("E","0"))
    elif any([val for val in value if val in ["S","W"]]):
        return -float(value.replace("S","0").replace("W","0"))
    else:
        return float(0)
    

def split_extract(value, split_character="-",extract_index=0,output_type="str"):
    """
    Function is used to split the value and return a particular split
    and outputs it to a string, float or int
    """
    
    splits = [eval(output_type)(val) for val in value.split(split_character)]
    return splits[extract_index]
    

def clean_date(date_val="", date_format="%Y-%m-%d",ignore_val="D/S"):
    """
    Take in the unformatted date value, string format and any value 
    that needs to be ignored, and returns the datetime object if value
    doesn't contain the ignored value.
    """
    
    if type(date_val) in ['int','float']:
        date_val = str(date_val)
    if date_val == ignore_val:
        return ignore_val
    else:
        return datetime.strptime(date_val,date_format).date()
    
    
    
def explore_dataframe(df,n=10):
    """
    The utility function to provide basic information about the data
    such as data types, missing rows, uniqueness of column and the 
    dimenions of of the dataframe, and finally printing the n first rows
    of the data.
    
    """
    
    data = df.copy()
    
    #Printing the shape of the data
    print("The data has {} rows and {} columns".format(data.shape[0],data.shape[1]))
    
    #Printing the data types of columns
    print("\nThe data types are : {}".format(data.dtypes))
    
    #Printing the missing records for each column
    print("\nShowing number of missing records per column")
    print(data.isna().sum())
    
    #Printing the number of unique records compared to the total records
    print_unique_records(data)
    
    #Printing the top 10 rows of the dataframe
    
    print("\nTop {} rows of the data are: \n".format(n))
    return data.head(n)