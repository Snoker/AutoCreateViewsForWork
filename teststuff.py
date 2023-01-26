import re
def cleanString(inputString):
    return re.sub('[\W_]+',"",inputString)

variable = 'hejsan 9090__--..,$$$'

print( cleanString(variable) )
