"""Console script for {{cookiecutter.repo_name}}."""
import {{cookiecutter.repo_name}}

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for {{cookiecutter.repo_name}}."""
    console.print("Replace this message by putting your code into "
               "{{cookiecutter.repo_name}}.cli.main")
    console.print("See Typer documentation at https://typer.tiangolo.com/")
    


if __name__ == "__main__":
    app()
