#!/usr/bin/env python

# # Example script for importing variables into OpenSILEX.

# In[ ]:


import json
import os

from opensilex_python_client.auth import connect
from opensilex_python_client.file_management.read_yaml import read_yaml
from opensilex_python_client.variables import download_config_example, import_from_csv
from opensilex_python_client.variables.groups import manage

# In[ ]:


# 1. Authentication
with open("credentials.json") as f:
    credentials_dict = json.load(f)

client = connect.connect_to_opensilex(credentials_dict)


# ## Generate files to upload
#

# In[ ]:


# 2. File paths (modify these paths)
save_file_path = os.path.join(os.path.abspath(""), "output_files")
os.makedirs(save_file_path, exist_ok=True)
data_examples_variables_dir_path = os.path.realpath(os.path.join(os.path.abspath(""), "../data_examples/variables"))
os.makedirs(data_examples_variables_dir_path, exist_ok=True)
print(f"Config files: {data_examples_variables_dir_path}")

download_config_example.download_variables_config(data_examples_variables_dir_path)


# In[ ]:


## 3. load files  - MODIFY FILES IF Need


# without uris
# input_csv_path = os.path.realpath(os.path.join(os.path.abspath(""), "../data_examples/variables", "test_variables.csv"))
# with uris
input_csv_path = os.path.realpath(
    os.path.join(os.path.abspath(""), "../data_examples/variables", "test_variables_with_uris.csv")
)

input_config_path = os.path.realpath(
    os.path.join(os.path.abspath(""), "../data_examples/variables", "test_config.yaml")
)
print(f"Input CSV path: {os.path.realpath(input_csv_path)}")
print(f"Input Config path: {os.path.realpath(input_config_path)}")


# In[ ]:


# 3. Import variables (creates components + variables automatically)
grouped_vars = import_from_csv.run(client, input_csv_path, input_config_path, False)


# In[ ]:


# 4. Attach to groups
config = read_yaml(input_config_path)
group_config = config.get("groups", {})
available_groups = group_config.get("available_groups", {})

for group_name, group_uri in available_groups.items():
    group = manage.find_or_create_group(client, group_uri if group_uri else None, group_name, debug=False)
    print(f"Existing {group}")


# In[ ]:


# 5. Attach to groups
manage.attach_variables(client, grouped_vars, input_config_path)


# In[ ]:


print(" Import completed!")
