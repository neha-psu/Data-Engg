import json

data = []

for n in range(1000):
    data.append({"count":n+1})

with open("wcsample.json", "w+") as file:
    json.dump(data, file, indent=2)

