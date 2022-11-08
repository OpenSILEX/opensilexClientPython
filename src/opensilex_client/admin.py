



def create_users_from_google_sheet(
    python_client: opensilexClientToolsPython.ApiClient, 
    sheet_id: str = "1hcWI9BoMlJLMi0RGfl1C2pDdnxEVDaUvbCY14VxUZ4g",
    sheet_name: str = "DiaPhen_users",
    maping: dict = {
        "uri": "orcid",
        "first_name": "first_name",
        "last_name": "family_name",
        "email": "email",
        "admin": "isadmin"
    }) -> None:
    
    
    # Fetching data from google sheet
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    users_df = pd.read_csv(url)

    # Maping columns
    users_mod = pd.DataFrame(columns=[
        "uri", "first_name", "last_name", "email", "language", "password", "admin"
    ])
    for col in maping.keys():
        users_mod[col] = users_df[maping[col]]
    
    # List of attributes that have no default
    no_default = ["first_name", "last_name", "email"]

    
    # Setting missing values to none or default
    users_mod["uri"] = users_mod["uri"].apply(lambda x: x if pd.notnull(x)  else None)
    users_mod["language"] = users_mod["language"].apply(lambda x: x if pd.notnull(x)  else "fr")
    users_mod["password"] = users_mod.apply(
        (lambda row: row["password"] if pd.notnull(row["password"])
         else users_mod.loc[row.name,"first_name"].lower()),
        axis=1
    )
    users_mod["admin"] = users_mod["admin"].apply(lambda x: False if not x==True else x)

    # Api client to create users
    sec_api = opensilexClientToolsPython.SecurityApi(python_client)

    # Creating the users
    for index,row in users_mod.iterrows():
        no_missing = True
        for col in no_default:
            if (not col in users_mod.columns or not pd.notnull(row[col])):
                logging.error(
                    "Exception : On row {} value for '{}' is missing"\
                        .format(index, col)
                )
                no_missing = False

        if no_missing:
            new_user = opensilexClientToolsPython.UserCreationDTO(**row)
            
            try :
                sec_api.create_user(body=new_user)
            except Exception as e:
                logging.error(
                    "Exception : {}"\
                        .format(e)
                )