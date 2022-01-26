import requests
import json
import pandas as pd
from transform import transform_dados

#Extracting data on start and sending objects to store in a database. 
def buscar_dados(endpoint):
    if endpoint == 'vehicles' or endpoint == 'people' or endpoint == 'films' or endpoint == 'planets' or endpoint == 'species' or endpoint == 'starships':
        request = requests.get(f"https://swapi.dev/api/{endpoint}/")
        retorno = json.loads(request.content)
        print("\n\n######## Getting Data of %s #########\n"%endpoint )
        print(json.dumps(retorno, indent=4))
        transform_dados(retorno,endpoint)
    else :
        print("Desenvolvimento dos outros endpoints")
        

if __name__ == '__main__':
    buscar_dados("vehicles")
    buscar_dados("planets")
    buscar_dados("species")
    

    