"""Example script for importing variables into OpenSILEX."""

from opensilexClientPython.auth import connect
from opensilexClientPython.variables import import_from_csv
from opensilexClientPython.variables.groups import update

# 1. Authentication
client = connect.connect_to_opensilex({
    "host": "http://local:8081/rest",
    "identifier": "guest@opensilex.org",
    "password": "guest"
})

# 2. File paths (modify these paths)
csv_path = "path/to/your/variables.csv"
config_path = "path/to/your/config.yaml"

# 3. Import variables (creates components + variables automatically)
grouped_vars = import_from_csv.run(client, csv_path, config_path)

# 4. Attach to groups
update.attach_to_groups(client, grouped_vars, config_path)

print("✅ Import completed!")
