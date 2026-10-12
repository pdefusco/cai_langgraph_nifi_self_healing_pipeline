"""
CAI Application entry point.

    Script:  app.py
    Runtime: PBJ Workbench, Python 3.12
    Env:     SELFHEAL_DF_CLIENT=mock|live, SELFHEAL_ARMED, LLM_* (see README)

Binds 127.0.0.1:$CDSW_APP_PORT, which is what a CAI Application proxies.
"""

import os
import sys

# A CAI Application runs this in an IPython kernel, where __file__ is
# undefined and the working directory is the project root.
sys.path.insert(0, os.path.dirname(os.path.abspath(globals().get("__file__") or "app.py")))

from nifi_healer.ui import gradio_app  # noqa: E402

if __name__ == "__main__":
    gradio_app.main()
