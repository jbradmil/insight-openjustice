
# coding: utf-8

import requests, pandas, os, glob

def build_output_directories(years):
    print '  Building directory structure ...',
    print('\bWill write output to: ')
    for year in years:
        base_dir = "data/census/CA/acs-1yr/" + str(year)
        # Create base directory if it doesn't exist
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        print base_dir
    return base_dir

def pull_variable_lists(years):
    print '  Pulling JSON variable list...',
    jsons = []
    for year in years:
        # Build the API URL
        variables_url = 'https://api.census.gov/data/'+str(year)+'/acs1/variables.json'
        # Read in the data
        data = requests.get(url=variables_url)
        # Check to make sure we could pull variables
        if data.status_code == 404:
            print('\bFailed')
            import sys
            sys.exit('You entered an invalid ACS year.  Please try again.')
        else:
            data = data.json()
            print('Pulled '+variables_url)
            jsons.append(data)

    return dict(zip(years, jsons))

def build_variable_dict(variable_lists):
    var_dict = dict()
    for year in variable_lists:
        var_json=variable_lists[year]
        #print '  Building table list...',
        table_list = list() # This will hold all the tables.
        acs_dict = dict() # This will hold the variables by table.
        # Iterate through the variables
        ivar = 0
        for variable in var_json['variables']:
            ivar+=1
            s = variable.split('_') # Break the string apart by the underscore.
            table = s[0] # This is the table name.
            if (table[0]!='C') or (table[1] not in [str(i) for i in range(10)]) or (table[-1] not in [str(i) for i in range(10)]):
                # for now, saving the slightly smaller tables (C*),
                # not quite so finely segmented
                continue
            if not table in table_list:
                table_list.append(table) # Add it to the table list
                var_list = list() # Create an empty list for the acs_dict
                var_list.append(variable) # Put the variable name in the list
                acs_dict[table] = var_list # Add the variable list to the dictionary
            else:
                var_list = acs_dict[table] # Pull the existing variable list
                var_list.append(variable) # Add in the new variable
                var_list.sort() # Sort it (so the estimates are followed by the MOE)
                acs_dict[table] = var_list # Replace the list with the updated one
        #print('\bDone')
        table_list.sort()
        var_dict[year]={'tables': table_list, 'acs_dict':acs_dict}
    return var_dict

def download_table(acs_dict, table, api_key, api_url_base, base_dir):
    # Since there is a 50 variable maximum we need to see how many calls
    # to the API we need to make to get all the variables.
    api_calls_needed = (len(acs_dict[table])/49)+1
    api_calls_done = 0
    variable_range = 49
    while api_calls_done < api_calls_needed:
        get_string = ''
        print('        API Call Set '+str(api_calls_done+1)+' of '+str(api_calls_needed))
        variable_range_start = variable_range * api_calls_done
        variable_range_end = variable_range_start + variable_range
        for variable in acs_dict[table][variable_range_start:variable_range_end]:
            get_string = get_string + ','+variable
        # Get the state level data -- note: 6 corresponds to CA only
        api_url = api_url_base + get_string + '&for=state:6&key=' + api_key
        ## to debug, uncomment the following line to see the exact API call
        # print 'State URL: ' + api_url
        state_data = pandas.io.json.read_json(api_url)
        state_data.columns = state_data[:1].values.tolist() # Rename columns based on first row
        state_data['Geocode'] = state_data['state']
        state_data = state_data[1:] # Drop first row
        # Pull all of the counties in the state
        api_url = api_url_base + get_string + '&for=county:*&in=state:6&key=' + api_key
        ## to debug, uncomment the following line to see the exact API call
        #print 'Counties URL: ' + api_url
        county_data = pandas.io.json.read_json(api_url)
        county_data.columns = county_data[:1].values.tolist() # Rename columns based on first row
        county_data['Geocode'] = county_data['state'] + county_data['county']
        county_data = county_data[1:] # Drop first row
        ## could also add subdivisions, cities, places later if desired
        # Build long table by append rows
        temp = state_data.append(county_data)
        # Add columns if the final data frame is created
        if api_calls_done == 0:
            data = temp
        else:
            data = pandas.concat([data, temp], axis=1)
        api_calls_done = api_calls_done + 1
        
    csv_path = base_dir + '/' + table + '.csv'
    # Pull out the Geocode and Name        
    geocode = data['Geocode']
    series = type(pandas.Series())
    #if type(geocode) == 'pandas.core.series.Series':
    if isinstance(geocode, series):
        geocode = pandas.DataFrame(geocode, columns=['Geocode'])
    else:
        geocode = geocode[[1]]
    name = data['NAME']
    if isinstance(name, series):
        name = pandas.DataFrame(name, columns=['NAME'])
    else:
        name = name[[1]]
    # Drop unneeded columns in they exist
    data = data.drop(['state'], axis=1) # Drop the state column
    data = data.drop(['county'], axis=1) # Drop the county column
    # Drop the duplicated location information
    data = data.drop(['Geocode'], axis=1)
    data = data.drop(['NAME'], axis=1)
    # Build data frame with columns in the desired order
    data = pandas.concat([geocode, name, data], axis=1)
    #print data
    data.to_csv(csv_path, index=False)
    print('      Table '+table+' Downloaded and Saved')
    
def download_and_save_data(var_dict, api_key):
    print('  Downloading tables for')
    for year in var_dict:
        j=0
        print year
        for table in var_dict[year]['tables']:
            j=j+1
            ## if j>1: break
            print('      Table: '+table)
            api_url_base = 'https://api.census.gov/data/'+str(year)+'/acs1?get=NAME'
            download_table(var_dict[year]['acs_dict'], table, api_key, api_url_base, "data/census/CA/acs-1yr/"+str(year))

def make_description_file(infile, var_json):
    dfin = pandas.DataFrame.from_csv(infile)
    descriptions=[var_json['variables'][dfin.columns[1]]['concept']]
    indices = ['Table Description']
    for var in dfin.columns[1:]:
        indices.append(var)
        descriptions.append(var_json['variables'][var]['label'])
    df_out = pandas.DataFrame({'Variable': indices, 'Description': descriptions})
    df_out.set_index('Variable', inplace=True)
    df_out.to_csv(infile[:-4]+'-vardesc.csv') 

def make_all_description_files(year, variable_lists):
    indir = 'data/census/CA/acs-1yr/'+str(year)
    for infile in glob.glob(indir+'/*.csv'):
        if infile.find('-vardesc.csv')>=0:
            continue
        make_description_file(infile, variable_lists[year])


