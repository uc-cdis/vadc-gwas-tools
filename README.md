# vadc-gwas-tools

Python CLI with various subcommands to support the argo GWAS workflows.

## Installation

1. Install poetry if you don't already have it (https://python-poetry.org/)
2. Install package `poetry install` (Note: poetry can manage environments but you can also generate a virtual environment yourself; regardless always build in a venv).

## Basic usage

To see the available subcommands:

```
vadc-gwas-tools
```

To access the help documentation for a particular subcommand:

```
vadc-gwas-tools <subcommand> -h
```

## Adding a new subcommand

1. Create a python file in `vadc_gwas_tools/subcommands/`
2. Inherit from the `vadc_gwas_tools.subcommands.Subcommand` abstract base class.
3. Follow the subcommand API to implement your subcommand:
```python
 class MySubcommand(Subcommand): # This class name will be the subcommand to call on the command line
     @classmethod
     def __add_arguments__(cls, parser: ArgumentParser) -> None: # Define your subcommand-specific arguments
         """Add the subcommand params"""
         parser.add_argument(
             "inputs",
             help="Input to process.",
         )

     @classmethod
     def main(cls, options: Namespace) -> None: # All the main logic of your subcommand goes here
         """
         Entrypoint for MySubcommand
         """
         logger = Logger.get_logger(cls.__tool_name__()) # create a logger by importing this class: from vadc_gwas_tools.common.logger import Logger
         logger.info(cls.__get_description__())

         # do stuff...

     @classmethod
     def __get_description__(cls) -> str: # Define the subcommand description which will appear in CLI
         """
         Description of tool.
         """
         return (
             "This subcommand does really cool stuff. "
             "It outputs some things and handles some inputs."
         )
```
4. Import your subcommand class in `vadc_gwas_tools/subcommands/__init__.py`
5. Import your subcommand in `vadc_gwas_tools/__main__.py`
6. Add your subcommand class to the `vadc_gwas_tools.__main__.main` function where the other ones are called: `MySubcommand.add(subparsers=subparsers)`
7. Create unit tests.

If you need to add dependencies you must use the `poetry add ...` functionality.
