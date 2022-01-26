from sqlalchemy import create_engine
#Reciving gereric data 
def load_dados_in_database(df,endpoint):
    if endpoint == 'vehicles' or endpoint == 'people' or endpoint == 'films' or endpoint == 'planets' or endpoint == 'species' or endpoint == 'starships':
        engine = create_engine('sqlite://', echo=False)
        df.to_sql('%s'%endpoint, con=engine)
        print("\n\n######## Selecting %s data from database  #########\n"%endpoint )
        print(engine.execute('SELECT * FROM %s'%endpoint).fetchall())
    else :
        print("Desenvolvimento dos outros endpoints")

def load_dados_in_parquet_file(df,endpoint):
    if endpoint == 'vehicles' or endpoint == 'people' or endpoint == 'films' or endpoint == 'planets' or endpoint == 'species' or endpoint == 'starships':
        df.to_parquet('df_%s_parquet.gzip'%endpoint,compression='gzip')
    else :
        print("Desenvolvimento dos outros endpoints em andamento")
