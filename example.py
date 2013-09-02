from dpack import *

# FILL THESE IN
APIKEY = "" 
APPCX = ""

d = dpack(APIKEY, APPCX) # create an instance
d.setQuery("aljazeera") # set the query
d.createDpack() # automatically construct the package
d.sprayPack("library") # spray into MongoDB
d.writePack("/tmp/") # write to /tmp
