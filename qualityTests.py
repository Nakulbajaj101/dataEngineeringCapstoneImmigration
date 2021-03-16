####File for quality checks functions for spark####
def check_unique_keys(df,table_name="", primary_key=""):
    """
    Function to check table primary key is unique in nature by comparing
    distinct number of primary key values with total number of rows, a
    value error is raised 
    """
    
    num_rows = df.count()
    num_unique_identifier_rows = df.dropDuplicates(subset=[primary_key]).count()
    if num_rows == num_unique_identifier_rows:
        print("{} has unique rows, and primary key constraint is not violated".format(table_name))
    else:
        raise ValueError("{} uniqueness violated , duplicated data detected".format(table_name)) 


def ensure_no_nulls(df, column=""):
    """
    Function to check for null counts on spark dataframe columns, 
    a value error is raised if column has null values
    """
    
    null_counts = df.filter("{} is NULL".format(column)).count()
    if null_counts == 0:
        print("{} doesnt have any nulls".format(column))
    else:
        raise ValueError("{} violated the null constraint, \
                        cannot contain nulls".format(column))
        

def check_data_type(df, column="", datatype=""):
    """
    Function to check datatype matches for spark dataframe.
    If mismatch is detected then value error is raised, and if column
    is not found then key error is raised
    """
    
    for _ in df.schema:
        if _.name == column:
            if str(_.dataType) == datatype:
                print("datatype match for column {} having {} values".format(column, datatype))
                break
            else:
                raise ValueError("""datatype mismatch detected for column {} .
                                Expected {} but got {}""".format(column, datatype, _.dataType))
                

def check_greater_that_zero(df):
    """
    Function to perform greater than 0 rows check for spark df.
    If check fails a value error is raised
    """
    
    if df.count() > 0:
        print("Greater than 0 test passed for the table")
    else:
        raise ValueError("Table has 0 rows, data may not have loaded correctly")
        

def match_source_input(df_input, df_output):
    """
    Function to check if data pushed to a location matches 
    data before pushing, to ensure data completeness, else
    value error is raised
    """
    
    if df_input.count() == df_output.count():
        print("Data pushed has complete data")
    else:
        raise ValueError("Data at source doesnt match data at destination")