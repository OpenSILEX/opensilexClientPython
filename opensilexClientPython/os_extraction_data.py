import opensilexClientToolsPython
import pandas as pd

def recup_infos_os(uri_experience, identifiant, password, host): 
    """
    Fonction pour récupérer les infos sur les objets scientifiques existants dans l'xp.
    """
    pythonClient = opensilexClientToolsPython.ApiClient()
    pythonClient.connect_to_opensilex_ws(identifier=identifiant,password=password,host=host)
    
    api_instance = opensilexClientToolsPython.ScientificObjectsApi(pythonClient)
    try:
        api_response = api_instance.search_scientific_objects(uri_experience, page_size=0,)
    except opensilexClientToolsPython.rest.ApiException as e:
        print("Exception: %s\n" % e)

    objetsscient = api_response["result"]
    os_par_type = {}

    for obj in objetsscient:
        type_os = obj.rdf_type_name
        if type_os not in os_par_type:
            os_par_type[type_os] = {"noms" : [], "uris"  : []}
        os_par_type[type_os]["noms"].append(obj.name)
        os_par_type[type_os]["uris"].append(obj.uri) 
    return os_par_type

def os_extraction_data(uri_expe, identifiant, mdp, host):
    """
    Extracts data from an OpenSilex experiment.
    """
    pythonClient = opensilexClientToolsPython.ApiClient()
    pythonClient.connect_to_opensilex_ws(identifier=identifiant, password=mdp, host=host)

    os_par_type = recup_infos_os(uri_expe, identifiant, mdp, host)
    os_data_frames = {}

    for type_os, infos in os_par_type.items():
        pythonClient = opensilexClientToolsPython.ApiClient()
        pythonClient.connect_to_opensilex_ws(identifier=identifiant, password=mdp, host=host)
        
        noms = infos["noms"]
        uris = infos["uris"]
        dico_uri_nom = dict(zip(uris, noms))
        os_uri_df = []

        for uri in uris:
            try:
                api_instance = opensilexClientToolsPython.DataApi(pythonClient)
                api_response = api_instance.get_data_list_by_targets(
                    experiments=[uri_expe], targets=[uri], page_size=1000
                )
                results = api_response["result"]

                for r in results:
                    ligne = {
                        "code_os": dico_uri_nom.get(uri, uri),
                        "variable": r.variable,
                        "date": r._date,
                        "value": r.value
                    }
                    os_uri_df.append(ligne)

            except Exception as e:
                print(f"Error: {e}")

        os_data_frames[type_os] = pd.DataFrame(os_uri_df)

    return os_data_frames
