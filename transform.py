import pandas as pd

from load import load_dados_in_database,load_dados_in_parquet_file

def transform_dados(retorno,endpoint):
    if endpoint=='vehicles':
        
        repetitive = ['name','model','manufacturer','cost_in_credits','length','max_atmosphering_speed','crew','passengers','cargo_capacity','consumables','vehicle_class','created','edited','url']
        pilots = pd.json_normalize(retorno['results'],'pilots',repetitive)
        films = pd.json_normalize(retorno['results'],'films',repetitive)

        c = pd.DataFrame(pilots.merge(films,how='inner',left_on=repetitive,right_on=repetitive))
        load_dados_in_database(c,endpoint)
        load_dados_in_parquet_file(c,endpoint)

    elif endpoint=='planets':
     
        repetitive = ['name','rotation_period','orbital_period','diameter','climate','gravity','terrain','surface_water','population','created','edited','url']
        films = pd.json_normalize(retorno['results'],'films',repetitive)
        residents = pd.json_normalize(retorno['results'],'residents',repetitive)
       
        result_set =  pd.DataFrame(films.merge(residents,how='inner',left_on=repetitive,right_on=repetitive))
        load_dados_in_database(result_set,endpoint)
        load_dados_in_parquet_file(result_set,endpoint)

    elif endpoint=='species':
        
        repetitive = ['name','classification','designation','average_height','skin_colors','hair_colors','eye_colors','average_lifespan','homeworld','language','created','edited','url']
        people = pd.json_normalize(retorno['results'],'people',repetitive)
        films = pd.json_normalize(retorno['results'],'films',repetitive)
       
        result_set =  pd.DataFrame(people.merge(films,how='inner',left_on=repetitive,right_on=repetitive))
        load_dados_in_database(result_set,endpoint)
        load_dados_in_parquet_file(result_set,endpoint)

    else :
        print('Endpoint desenvolvidos: Vehicles,Planets,Species')
    



    