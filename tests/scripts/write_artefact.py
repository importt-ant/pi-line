"""Write a file into the artefact directory."""
import os
from pathlib import Path

artefact_dir = Path(os.environ["PILINE_ARTEFACT_DIR"])
(artefact_dir / "output.txt").write_text("artefact content")
