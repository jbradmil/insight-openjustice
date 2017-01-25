# after you've downloaded census data and stored it in a directory,
# combine the files into one table and replace the headers with variable descriptions from
# http://api.census.gov/data/2015/acs5/variables.html

import pandas as pd
import os
import glob


def clean_census(indir='data/census/acs/2011-15/CA/', outtag='acs'):
    # first load labels table
    labels = pd.read_json('http://api.census.gov/data/2015/acs5/variables.json')
    # now parse the json to get columns
    varnames = pd.read_json( (labels['variables']).to_json(), orient='index') 
    outdir = indir+'cleaned/'
    if not os.path.exists(outdir):
            os.makedirs(outdir)
            
    # loop over downloaded frames
    df_list = []
    for input_csv in glob.glob(indir+'*csv'):
        # load one of the downloaded data frames
        df = pd.DataFrame.from_csv(input_csv)
        outfile = input_csv.replace(indir, '')
        print 'Loading data in ' + input_csv    
        # load the downloaded data frame
        df = pd.DataFrame.from_csv(input_csv)
        #select only the full counties
        county_level = (df.NAME.str.contains('County')) & (df.NAME.str.find('CCD')<0) & (df.NAME.str.find('CDP')<0)
        df_cleaned = df[county_level]
        column_names = []
        # replace column names
        for code in df_cleaned.columns.values:
            column_names.append(varnames.loc[[code]].label.max())
            column_names[-1] = column_names[-1].replace('$','\$')
        df_cleaned.columns=column_names
        df_cleaned['Geographic Area Name']=df_cleaned['Geographic Area Name'].str.replace(' County, California', '')
        # write individual files
        df_cleaned.to_csv(outdir+outfile)
        df_list.append(df_cleaned)
        print 'Wrote to %s' % (outdir+outfile)
   
    # now write the combined file
    combined_df = pd.concat(df_list, axis=1)
    combined_df.to_csv(outdir+outtag+'.csv')
    print 'Wrote all to %s' % (outdir+outtag+'.csv')
    
if __name__ == "__main__":
    relabel_census()

