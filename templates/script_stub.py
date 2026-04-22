"""
Starting point for a new NetBox custom script (copy to repo root, rename file + class).

Do not deploy this stub to NetBox as-is—replace Meta and run().
"""

from extras.scripts import Script


class ExampleScript(Script):
    """TODO: Replace class name with something unique in this repo."""

    class Meta(Script.Meta):
        name = "Example script"
        description = "TODO: Short description shown in NetBox Scripts list."
        commit_default = False

    def run(self, data, commit):
        self.log_info("TODO: Implement your logic.")
        self.log_success("Done.")
