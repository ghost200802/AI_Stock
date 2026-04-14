import subprocess
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

app_path = project_root / "modules" / "visualizer" / "app.py"

if __name__ == "__main__":
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path),
         "--server.address", "localhost",
         "--server.port", "8501"],
        cwd=str(project_root),
    )
