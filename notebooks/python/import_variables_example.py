"""Example script for importing variables into OpenSILEX."""
import os

from opensilexClientPython.auth import connect
from opensilexClientPython.variables import import_from_csv
from opensilexClientPython.variables.groups import update

script_dir = os.path.dirname(os.path.realpath(__file__))
# 1. Authentication
client = connect.connect_to_opensilex({
    "host": "http://localhost:8666/rest",
    "identifier": "admin@opensilex.org",
    "password": "admin"
})

print(script_dir)
# 2. File paths (modify these paths)
csv_path = os.path.realpath(os.path.join(script_dir, "../data_examples/variables_csv_examples/test_variables.csv"))
yaml_config_path = os.path.realpath(os.path.join(script_dir, "../data_examples/variables_csv_examples/test_config.yaml"))

# 3. Import variables (creates components + variables automatically)
grouped_vars = import_from_csv.run(client, csv_path, yaml_config_path)

# 4. Attach to groups
update.attach_to_groups(client, grouped_vars, yaml_config_path)

print("✅ Import completed!")
