import os

from src.web_app import *  # noqa: F401,F403


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    app.run(
        debug=debug_mode,
        use_reloader=False,
        host="127.0.0.1",
        port=int(os.environ.get("PORT", "5000")),
    )
