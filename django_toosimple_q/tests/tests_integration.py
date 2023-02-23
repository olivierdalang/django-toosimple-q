import re

from django.core.management import call_command
from django.test import TestCase


class TestIntegration(TestCase):
    def test_readme(self):
        # Test code in the readme

        readme = open("README.md", "r").read()

        # This finds all ```python``` blocks
        python_blocks = re.findall(r"```python\n([\s\S]*?)```", readme, re.MULTILINE)

        # Run each block
        full_python_code = ""
        for python_block in python_blocks:
            # We concatenate all blocks with the previous ones (so we keep imports)
            full_python_code += python_block
            try:
                exec(full_python_code)
            except Exception as e:
                hr = "~" * 80
                raise Exception(
                    f"Invalid readme block:\n{hr}\n{python_block}{hr}"
                ) from e

    def test_makemigrations(self):
        # Ensure migrations are up to date with model changes
        try:
            call_command("makemigrations", "--check", "--dry-run")
        except SystemExit:
            raise AssertionError(
                "Migrations are not up to date. You need to run `makemigrations`."
            )
