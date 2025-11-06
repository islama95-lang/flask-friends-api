import pandas as pd
from flask import Flask, jsonify, request
import os
import logging
import math #need in pagination

#to record the error(INFO, WARNING, ERROR)
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

DATA = 'friends_data.csv'
app = Flask(__name__) #making the Flask 

ALL_COLUMNS = ['id', 'first_name', 'last_name', 'gender', 'primary_role', 'occupation', 'relationship_status', 'city', 'notable_trait', 'catchphrase', 'first_appearance_season', 'related_to','notes', 'episode_count', 'nationality', 'screen_time_minutes' ]

## Read data safely
def load_data():

#read data from csv and return to DataFrame
    if not os.path.exists(DATA):
        logging.error(f"FATAL ERROR: Data file '{DATA}' not found. Creating empty DataFrame.")

        return pd.DataFrame(columns=ALL_COLUMNS)

#errorhandling for reading csv safely
    try:

        df_loaded = pd.read_csv(DATA, dtype={'id':'Int64'})
        #Int64(64bit/8bytes) is nullable integer type in pandas(Uses for Nan)
        logging.info(f"Data loaded successfully. Total rows: {len(df_loaded)}")
        return df_loaded 

    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return pd.DataFrame(columns=ALL_COLUMNS) 
        #Return an empty DataFrame with the correct structure even in case of failure



##Write data
def save_data(data_frame):
#save the dataframe in csv file safely

#errorhandling for writing in csv safely
    try:
        data_frame.to_csv(DATA, index = False) #Doesn't save the index column in csv (index= False)
        logging.info("Data successfully persisted to CSV.")
        return True #Succesfully saved the file
    
    except Exception as e:
        logging.error(f"ERROR: Failed to write data to CSV: {e}")
        return False #Failed saving the file
 
  
    # Read the csv file once and store as global variable while starting the Flask
df = load_data()


##Common error handling


@app.errorhandler(400)
def bad_request_error(error):
    """400 Bad Request(Client input miss)"""
    return jsonify({"error": "Bad Request", "message": error.description or "Invalid request."}), 400
# error.description: The browser will sent a request that this server could not understand. If there is no description, it will show "Invalid request."


@app.errorhandler(404)
def not_found_error(error):
    """404 Not Found(Resource Not Found)"""
    return jsonify({"error": "Not Found", "message": "The requested resource could not be found"}), 404

@app.errorhandler(500)
def internal_error(error):
    """500 Internal Server Error(Unexpected error from server)"""
    logging.error(f"International Server Error:{error}")
    return jsonify({"error": "International Server Error", "message": "An unexpected server error occurred"}), 500


#status check route
@app.route('/', methods=['GET']) # get the 'GET request'(contains the requested resource) while accessing the URL
def home():
    if df.empty:
        return jsonify({"message":"API is running. Data loading failed or file is empty."}), 500 # Internal server error
    return jsonify({"message": f"API is running with {len(df)} charecters."}), 200 # HTTP status code 200 = OK


## Add pagination and search

# Define an endpoint to handle GET requests for character data
@app.route('/characters', methods=['GET'])
def list_characters():
    global df #global Dataframe loaded from CSV
    if df.empty:
        return jsonify({"data": [], "meta": {"total_items": 0}}),200 # HTTP status code 200 = OK
      #meta shows the details like Number of pages/items
      
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page',10))
    
    except ValueError:
         # Handle invalid (non-integer) pagination parameters
        logging.warning("Pagination query recieved non-integer value.")
        return jsonify({"error": "Bad Request", "message": "Page and per_page must be integers."}), 400
    
    # Calculate total items and pages
    total_items = len(df)
    total_pages = math.ceil(total_items / per_page)  
    
    #validate the requested page number
    if page < 1 or (page > total_pages and total_items > 0):
        logging.warning(f"Requested page {page} is out of range.")
        return jsonify({"error": "Not Found", "message": f"Page{page} is out of range. Max page is {total_pages}."}), 404
    
    # Compute the slice indices for pagination
    start_index = (page - 1)* per_page #page-1 because python starts the index from 0
    end_index = start_index + per_page
    
    # Select only the requested portion of the DataFrame
    paginated_df = df.iloc[start_index:end_index] #Extract a specific range of rows from the DataFrame

    
    # Return paginated data and metadata
    return jsonify({
        "data": paginated_df.to_dict(orient='records'), # Convert each row to a dictionary and create a list for JSON serialization

        "meta":{
             "page": page,
             "per_page": per_page,
             "total_items": total_items,
             "total_pages": total_pages
            
        }
    }), 200 
    
    

    
    ## GET /characters/search (Search by name)
@app.route('/characters/search', methods=['GET'])
def search_characters():
        global df
        
        
        # Extract query parameters for searching
        first_name = request.args.get('first_name')
        last_name = request.args.get('last_name')
        
            # If no search parameter is provided, return an error
        if not first_name and not last_name:
            logging.warning("Search failed: No first_name or last_name provided. ")
            return jsonify({"error": "Bad Request", "message": "Must provide 'first_name' or 'last_name' for search."}),400 #Bad request
        
        # Create a Boolean Series (same length as the DataFrame)
        # Initially set all values to False — meaning no rows match yet
        # 'index=df.index' ensures it aligns with the DataFrame’s row indices
        filter_condition = pd.Series([False] * len(df), index=df.index)
        
        if first_name:
             # Case-insensitive partial match on 'first_name'
            filter_condition |= df['first_name'].astype(str).str.contains(first_name, case= False, na = False)
            # case=False: ignore letter casing during comparison
        if last_name:
           
            # Case-insensitive partial match on 'last_name'
            filter_condition |= df['last_name'].astype(str).str.contains(last_name, case= False, na = False)
        
        # Filter the DataFrame based on the search condition 
        search_results = df[filter_condition]
    
        
        # Return the search query, number of results, and data
        return jsonify({
            "search_query": request.args.to_dict(),# Include the original query parameters sent by the client (e.g., first_name, last_name)
            "result_count": len(search_results),# Count how many records matched the search criteria
            "data": search_results.to_dict(orient='records')  ## Convert each row to a dictionary and return as a list of records
 
        }), 200
        
        
##Put/ Delete   

#PUT    
@app.route('/characters/<int:id>',methods=['PUT'])
def update_character(id):
    global df
    data = request.request.get_json(silent = True) # Get JSON data from request body
    
    # Validate the JSON body
    if not data:
        logging.warning(f"PUT failed for ID {id}: Invalid or missing JSON body.")
        return jsonify({"error": "Bad Request", "message": "Request body must be valid JSON."}), 400
    
    #Find the index with given character with the given ID
    index = df[df['id'] == id].index
    
    
    # If no record found, return 404 Not Found
    if index.empty:
        logging.warning(f"PUT failed: Character ID {id} not found.")
        return jsonify({"error": "Not Found", "message": f"Character with ID {id} not found."}), 404
    
     # Allow updating of all columns except 'id'
    updatable_keys = [col for col in ALL_COLUMNS if col != 'id']
    
    
      # Filter only valid keys that exist in the dataframe
    update_data = {k: v for k, v in data.items() if k in updatable_keys}
    
    # If no valid fields provided, return 400 Bad Request
    if not update_data:
        return jsonify({"error": "Bad Request", "message": "No valid fields provided for update."}), 400
    
    
    # Update each column value
    try:
        for key,  value in update_data.items():
            df.loc[index, key] = value
            
        # Persist the updated dataframe back to the CSV file  
        if not save_data(df):
            logging.error(f"PUT operation failed to persist data for ID {id}.")
            return jsonify({"error": "Internal Server Error", "meassage": "Server failed to save data to CSV."}), 500
        
        
        # Return the updated character data
        updated_char = df[df['id'] == id].iloc[0].to_dict() # Convert the first matching row into dictionary format
        logging.info(f"Character ID {id} updated successfully.")
        
        return jsonify(updated_char), 200
    
    except Exception as e:
        logging.error(f"Unhandled error during PUT for ID {id}: {e}")
        return jsonify({"error": "Internal Server Error", "message": "An unexpected server error occurred during update."}), 500
    
   #DELETE
   
@app.route('/characters/<int:id>', methods=['DELETE'])
def delete_character(id):
       global df
       
         # Find the record by ID
       index = df[df['id'] == id].index
       
        # If no record found, return 404
       if index.empty:
           logging.warning(f"DELETE failed: Charater ID {id} not found.")
           return jsonify({"error": "Not Found", "message": f"Character withh ID {id} not found."}), 404
       
       try:
           
             # Delete the record from the dataframe
           df.drop(index, inplace= True) #inplace= True can make the change
           
              # Save changes to the CSV file 
           if not save_data(df):
               logging.error(f"DELETE operation failed to persist data for ID {id}.")
               return jsonify({"error": "Internal Server Error", "message": "Server failed to save data to CSV after deletion."}), 500
           
           logging.info(f"Character ID {id} deleted successfully.")
             # Return HTTP 204 (No Content) on success
           return '', 204
       
       except Exception as e:
              # Handle unexpected deletion errors
        logging.error(f"Unhandled error during DELETE for ID {id}: {e}")
        return jsonify({"error": "Internal Server Error", "message": "An unexpected server error occurred during deletion."}), 500
    
    
#server startup code
if __name__ == '__main__':
    # Log a warning if the app starts with an empty dataset
      if df.empty and os.path.exists(DATA):
        logging.critical("App starting with empty data; check CSV format.")
        
       # Run Flask in debug mode   
      app.run(debug=True)