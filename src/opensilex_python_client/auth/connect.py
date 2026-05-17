import sys

import opensilexClientToolsPython
from rich.console import Console

_console = Console()


def connect_to_opensilex(connection_info: dict, verbose: bool = False) -> opensilexClientToolsPython.ApiClient:
    """
    This function connects to Opensilex using the provided connection information and returns an
    instance of the opensilexClientToolsPython.ApiClient.

    :param connection_info: The `connection_info` parameter is a dictionary that should contain the
    necessary information to establish a connection to the Opensilex API. This information may include
    details such as the API endpoint URL, authentication credentials, and any other required connection
    settings. The function `connect_to_opensilex` likely uses
    :type connection_info: dict
    :param verbose: The `verbose` parameter in the `connect_to_opensilex` function is a boolean flag
    that determines whether additional information or messages should be displayed during the connection
    process. When `verbose` is set to `True`, it indicates that the function should provide more
    detailed output or logging information, while, defaults to False
    :type verbose: bool (optional)
    """

    try:
        if not connection_info["host"] or not connection_info["identifier"] or not connection_info["password"]:
            _console.print("[red]✗ Missing connection parameter(s)[/red]")
            sys.exit(1)

        # Se connecter à OpenSILEX
        with _console.status("[bold cyan]Connection to OpenSILEX...[/bold cyan]"):
            client = opensilexClientToolsPython.ApiClient(verbose=verbose)
            client.connect_to_opensilex_ws(
                identifier=connection_info["identifier"],
                password=connection_info["password"],
                host=connection_info["host"],
            )
            if "Authorization"  not in client.default_headers:
                return None
        _console.print("[bold green]✓ Successful connection[/bold green]")
        return client
    except Exception as e:
        print(connection_info)
        print(connection_info["host"])
        _console.print(f"[bold red]✗ Connection error: {e}[/bold red]")
        return None
