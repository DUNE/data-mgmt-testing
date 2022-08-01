import sys,os,time,json,jsonlines

filename = sys.argv[1]

shortname = filename.replace(".jsonl","").replace(".json","")

newfilename = shortname+".json"

if "jsonl" in filename:
    thedata = {}
    with jsonlines.open(filename, mode='r') as reader:
    with jsonlines.open(inputfilename, mode='r') as reader:
      for obj in reader:
        thedata.append(obj)
else:
    with open(filename, 'r') as myfile:
        data=myfile.read()
    thedata = json.loads(data)
if "jsonl" in filename:
    with jsonlines.open(newfilename, mode='w') as writer:
    
 

print (json.dumps(thedata, indent = 2, separators=(',', ': ')))
