import requests
import pandas as pd

def parse_dict(d):
    result = {}

    for k, v in d.items():
        if isinstance(v, dict):
            result.update(parse_dict(v))

        elif isinstance(v, list):
            for elem in v:
                if isinstance(elem, dict): result.update(parse_dict(elem))

        elif isinstance(v, int) or isinstance(v, float) or isinstance(v, str):
            result[k] = v
        else:
            continue

    return result

def list_to_dataframe(lst):
    data = []
    for d in lst:
        if isinstance(d, dict): 
            data.append(parse_dict(d))
        else: 
            continue
    return pd.DataFrame(data)

OPENAI_API_KEY = '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'

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}"
}

response = requests.get("https://api.openai.com/v1/models", headers=headers)

rtext = response.json()

pd = list_to_dataframe(rtext['data'])

print(rtext)
