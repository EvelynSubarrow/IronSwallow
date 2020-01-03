#!/usr/bin/env python3

import json

config = {}

for config_path in ("config.json", "secret.json"):
    try:
        with open(config_path) as f:
            config.update(json.load(f))
    except e:
        pass

def get(key, default=None):
    return config.get(key, default)
