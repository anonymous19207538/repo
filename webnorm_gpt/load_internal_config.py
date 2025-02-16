import os

import yaml

_params = {
    "api_key": "",
    "api_host": "https://api.openai.com",
    "api_model": "gpt-3.5-turbo",
    "max_tokens": 4096,
    "temperature": 0.0,
    "top_p": 1.0,
    "turns": 3,
}

__all__ = list(_params.keys())


def load_internal_config():
    config = None
    try:
        with open(
            os.path.join(os.path.dirname(__file__), "openai_config.yml"), "r"
        ) as f:
            config = yaml.load(f, yaml.FullLoader)
    except Exception:
        pass
    if config is None:
        config = {}

    for k, v in _params.items():
        if k in config:
            globals()[k] = config[k]
        else:
            globals()[k] = v


load_internal_config()
