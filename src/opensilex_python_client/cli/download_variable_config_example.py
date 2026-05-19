from opensilex_python_client.variables.download_config_example import _parse_args, download_variables_config


def main():
    args = _parse_args()
    download_variables_config(args.dest)


if __name__ == "__main__":
    main()
