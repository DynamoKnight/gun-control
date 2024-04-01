import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt


# Read the files and clean 'em up
def main():
    data_gdp = pd.read_csv('raw_data/gdp-by-state.csv')
    data_laws = pd.read_csv('raw_data/gun_laws.csv')
    data_violence = pd.read_csv('raw_data/gun-violence-data.csv', index_col='date', parse_dates=True)
    data_behaviors = pd.read_csv('raw_data/States_Behavior.csv')
    data_states = gpd.read_file('raw_data/States_shapefile.shp')

    # The clean up
    data_gdp = clean_gdp(data_gdp)
    data_laws = clean_laws(data_laws)
    data_violence = clean_violence(data_violence)
    data_behaviors = clean_behaviors(data_behaviors)
    
    # Merges based on state and year, and data_laws is complete
    data_all = data_laws.merge(data_violence, on=['state','year'], how='left')
    # Merge conflict of wrong type so fix
    data_all['year'] = data_all['year'].astype(int)
    data_gdp['year'] = data_gdp['year'].astype(int)
    data_all = data_all.merge(data_gdp, on=['state','year'], how='left')

    # Merges on index because it is alphabetically sorted. 
    # Since they are not related to a specific year,
    # they wont be merged with the big dataset
    data_states = data_states.merge(data_behaviors, on=data_states.index, how='left')
    data_states = data_states[data_states['State'] != 'District of Columbia'].reset_index()
    data_states = data_states.drop('key_0', axis='columns')
    data_states = data_states.drop('index', axis='columns')

    # Saves files
    data_gdp.to_csv('data_organized/gdp_by_state.csv')
    data_laws.to_csv('data_organized/gun_laws.csv')
    data_violence.to_csv('data_organized/gun_violence_data.csv')
    data_states.to_file('data_organized/shapes/states_behaviors.shp')
    data_all.to_csv('data_organized/state_data_2013-2017.csv')

    # Just to confirm the columns
    #sta = gpd.read_file('data_organized/states_behaviors.shp')
    #print(sta)

def clean_gdp(df):
    '''
    Only stores the desired columns
    Rearranges the data to have a year column

    df = a dataframe of the gdp per state
    '''
    df = df.drop('Fips', axis='columns')
    # 50 states and D.C.
    df = df[0:52]
    # Calls the column_to_row method to make the years into 1 column
    df = column_to_row(df,'year', 'GDP', 'Area')
    df = df.rename(columns={'Area': 'state'})
    return df

def clean_laws(df):
    '''
    Only stores the desired columns
    Gets data from 2013 to 2017

    df = a dataframe of gun laws per state
    '''
    df = df.loc[(df['year'] >= 2013) & (df['year'] <= 2017), :].reset_index()
    df = df.drop('index', axis='columns')
    return df

def clean_violence(df):
    '''
    Only stores the desired columns
    Combines the time based on the year

    df = a dataframe of gun violence,
    must be a time series
    '''
    df['n_victims'] = df['n_killed'] + df['n_injured']
    df = df[[
        'state',
        'n_victims'
    ]]
    # Solves SettingWithCopyWarning, even though nothings wrong?
    df = df.copy()
    # Modifies all the times to only store the year
    df.loc[:, 'year'] = df.index.year
    # Index is reset so that it is no longer a time series
    # Victims are summmed based on year and state
    df = df.groupby(['year', 'state']).agg({'n_victims': 'sum'}).reset_index()
    # Only looking at 2013 to 2017
    df = df.loc[(df['year'] >= 2013) & (df['year'] <= 2017), :]
    return df

def clean_behaviors(df):
    '''
    Only stores the desired columns
    which are then shortened by name

    df = a dataframe of state behaviors
    '''
    df = df[[
        'State',
        'Total.Popilation',
        'LawEnforcementPer100k',
        'Gun.Ownership',
        'Thoughts.of.Suicide.Past.Year.18+',
        'Major.Depressive.Past.Year.18+',
        'Received.Mental.Health.Services.Past.Year',
        'Illicit.Drug.Use.Other.Than.Marijuana.12+',
        'Alcohol.Use.Disorder.Past.Year',
        'Substance.Use.Disorder.Past.Year'
    ]]
    # Since this will be combined with a gdf
    # it can't have more than 10 character column names
    df = df.rename(
        columns={
            'Total.Popilation': 'POP', 
            'LawEnforcementPer100k':'LEper100k', 
            'Gun.Ownership':'gun_own', 
            'Thoughts.of.Suicide.Past.Year.18+':'scd_thts',
            'Major.Depressive.Past.Year.18+':'dprsv',
            'Received.Mental.Health.Services.Past.Year':'rcv_med',
            'Illicit.Drug.Use.Other.Than.Marijuana.12+':'drug_use',
            'Alcohol.Use.Disorder.Past.Year':'alcl_use',
            'Substance.Use.Disorder.Past.Year':'sbs_use'
    })
    return df


def column_to_row(df, group_name, new_col, main_col):
    '''
    Returns a new dataframe where selected columns
    will be stacked into one column, and new rows will
    be created for each old column.

    df = the dataframe that will be modified
    group_name = the name of the column that 
    contains all old columns
    new_col = the name of the column that was 
    the value of the old columns
    main_col = the column that will be unchanged,
    usually this is the name
    '''
    # A new dataframe will be made to lengthen the years
    dict_df = {main_col:[], group_name:[], new_col:[]}
    # Gets every old column
    things = [thing for thing in df.columns if thing != main_col]
    for thing in things:
        # Iterates through every name
        for name in list(df[main_col]):
            dict_df[main_col].append(name)
            dict_df[group_name].append(thing)
            # Gets the row index of the 1st occurence of the name
            row = df[df[main_col] == name].index[0]
            # Gets the value from the thing at the row of the name
            dict_df[new_col].append(df.loc[row, thing])
    return pd.DataFrame(dict_df)

def test_column_to_row():
    '''
    Tests column_to_row
    '''
    test_data = pd.read_csv('raw_data/test_col_to_row.csv')
    new_data = column_to_row(test_data,'class','grade','name')
    #print(new_data)

def test_clean_gdp():
    '''
    Tests clean_gdp
    '''
    test_data = pd.read_csv('raw_data/test_gdp.csv')
    new_data = clean_gdp(test_data)
    #print(new_data)


def test_clean_laws():
    '''
    Tests clean_laws
    '''
    test_data = pd.read_csv('raw_data/test_laws.csv')
    new_data = clean_laws(test_data)
    #print(new_data)

def test_clean_violence():
    '''
    Tests clean_violence
    '''
    test_data = pd.read_csv('raw_data/test_violence.csv',index_col='date',parse_dates=True)
    new_data = clean_violence(test_data)
    #print(new_data)

def test_clean_behaviors():
    '''
    Tests clean_behaviors
    '''
    test_data = pd.read_csv('raw_data/test_behaviors.csv')
    new_data = clean_behaviors(test_data)
    #print(new_data)


if __name__ == '__main__':
    test_column_to_row()
    main()
