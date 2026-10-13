
import math
import re
import numpy as np
from .xcel import row_col_to_cell, xlvalue

FALSE = False
TRUE = True

xfunctions={}
_xworkbook_ =None

    
def CDiv(v1, v2):
    if round(v2, 4) == 0:
        if round(v1, 4) == 0:
            return 1
        return 0
    return v1 / v2

def floatv(v):
    if v == None:
        return 0
    if v == "":
        return 0
    return float(v)

def flatten_(lst):
    out = list()
    for obj in lst:
        if isinstance(obj, list):
            for index in range(len(obj)):
                obj[index] = floatv(obj[index])
            out += obj
    if len(out) == 0:
        return lst
    else:
        return out
    
def flatten(lst):
    while True:
        if lst==flatten_(lst):
            return lst
        else:
            lst =flatten_(lst)
            
def cells_reset():
    _xworkbook_.reset()

def IF(a,b,c = 0):
    if a:
        return b
    else:
        return c

def AND(*a):
    for ea in a:
        if ea == False:
            return False
    return True

def OR(*a):
    for ea in a:
        if ea:
            return True
    return False


def TAN(a):
    return math.tan(a)

def RADIANS(a):
    # return math.pi/360*a
    # return a/(math.pi*18)
    return math.radians(a)

def DEGREES(a):
    # return a/math.pi*180
    # return math.pi/360*a
    return math.degrees(a)

def SIN(a):
    return math.sin(a)

def COS(a):
    return math.cos(a)

def ACOS(a):
    return math.acos(a)

def ATAN(a):
    return math.atan(a)

def ISNUMBER(a):
    return isinstance(a, (int, float)) or (isinstance(a, str) and a.isdigit())

def ABS(a):
    return abs(a)


# def TAN(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.tan(a))

# def RADIANS(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.pi/360*a)

# def DEGREES(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(a/math.pi*180)

# def SIN(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.sin(a))

# def COS(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.cos(a))

# def ACOS(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.acos(a))

# def ATAN(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(math.atan(a))

# def ISNUMBER(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return isinstance(a, (int, float)) or (isinstance(a, str) and a.isdigit())

# def ABS(a):
#     if isinstance(a, xlvalue):
#         a = a.v
#     return xlvalue(abs(a))

def AVERAGE(*a):
    a= flatten(list(a))
    return sum(a)/len(a)

def SUM(*a):
    a= flatten(list(a))
    return sum(a)

def MIN(*a):
    a= flatten(list(a))
    return min(a)

def MAX(*a):
    a= flatten(list(a))
    return max(a)

def SQRT(a):
    return math.sqrt(a)

def CHOOSE(a,*b):
    a = int(a)
    if a > len(b) - 2:
        return 1
    return b[a-1]
    
def LOOKUP(t,k,v):
    for kk,vv in zip(k,v):
        if t == kk:
            return vv
    return None
    
def INDIRECT(a,b,ws):
    chs="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    a = int(a)
    c= chs[a-1]+str(b)
    return vcell(c,ws)


def init_workbook(wb):
    for i,f in xfunctions.items():
        c = wb.cell(i)
        c.f = f
    global _xworkbook_
    _xworkbook_=wb



def sheet4_A3():#for a simple test1
    return MAX(vcell("A1","sheet4"),vcell("A2","sheet4"))
xfunctions["sheet4!A3"]= sheet4_A3

def sheet4_A4():#for a simple test2
    return SUM(vcell("A1:A3","sheet4"))
xfunctions["sheet4!A4"]= sheet4_A4


def vcell(c,ws=None):
    v = None
    if(ws):
        ws = ws.replace("_", " ")
    if(c.find("!")!=-1):
        c = c.replace("_", " ")
        v = _xworkbook_.cell(c).value
    else:
        v = _xworkbook_.worksheet(ws).cell(c).value
    if isinstance(v, float) or isinstance(v, int):
        return v
    if isinstance(v, list):
        return v
    if v == None or v.strip() == "":
        return 0
    if v == "Own Data":
        return 17
    return float(v)
        
def cell(c,ws=None):
    if(c.find("!")!=-1):
        return _xworkbook_.cell(c)
    else:
        return _xworkbook_.worksheet(ws).cell(c)
          

def icell(r, c, ws = None):
    return _xworkbook_.worksheet(ws).icell(r, c)

def set_cell(c, v, ws=None):
    _xworkbook_.worksheet(ws).set_cell(c, v)
    
def set_icell(r, c, v, ws=None):
    name = row_col_to_cell(r, c)
    _xworkbook_.worksheet(ws).set_cell(name, v)
          
#E115-H35
def xcf_Input_Geometry_J35(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J35')
    return None
xfunctions['Input Geometry!J35']=xcf_Input_Geometry_J35

#J35-H37
def xcf_Input_Geometry_J37(): 
    try:      
        return vcell("J35","Input Geometry")-vcell("H37","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J37')
    return None
xfunctions['Input Geometry!J37']=xcf_Input_Geometry_J37

#J37-H39
def xcf_Input_Geometry_J39(): 
    try:      
        return vcell("J37","Input Geometry")-vcell("H39","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J39')
    return None
xfunctions['Input Geometry!J39']=xcf_Input_Geometry_J39

#J39-H41
def xcf_Input_Geometry_J41(): 
    try:      
        return vcell("J39","Input Geometry")-vcell("H41","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J41')
    return None
xfunctions['Input Geometry!J41']=xcf_Input_Geometry_J41

#J41-H43
def xcf_Input_Geometry_J43(): 
    try:      
        return vcell("J41","Input Geometry")-vcell("H43","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J43')
    return None
xfunctions['Input Geometry!J43']=xcf_Input_Geometry_J43

#IF(OR(VertSliceCalcs!Q32==-99999,VertSliceCalcs!Q33==-99999),0,AVERAGE(VertSliceCalcs!P32:P33))
def xcf_Input_Geometry_C72(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q32","Input Geometry")==-99999,vcell("VertSliceCalcs!Q33","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P32:P33","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C72')
    return None
xfunctions['Input Geometry!C72']=xcf_Input_Geometry_C72

#IF(OR(VertSliceCalcs!Q32==-99999,VertSliceCalcs!Q33==-99999),0,AVERAGE(VertSliceCalcs!Q32:Q33))
def xcf_Input_Geometry_E72(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q32","Input Geometry")==-99999,vcell("VertSliceCalcs!Q33","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q32:Q33","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E72')
    return None
xfunctions['Input Geometry!E72']=xcf_Input_Geometry_E72

#IF(E72!=0,IF($B$96==1,H72,IF(E72>$F$93,$G$93+($G$92-$G$93)*(E72-$F$93)/($F$92-$F$93),IF(AND(E72<=$F$93,E72>$F$94),$G$94+($G$93-$G$94)*(E72-$F$94)/($F$93-$F$94),IF(AND(E72<=$F$94,E72>$F$95),$G$95+($G$94-$G$95)*(E72-$F$95)/($F$94-$F$95),IF(E72<=$F$95,$G$96+($G$95-$G$96)*(E72-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G72(): 
    try:      
        return IF(vcell("E72","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H72","Input Geometry"),IF(vcell("E72","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E72","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E72","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E72","Input Geometry")>vcell("$F$94","Input Geometry")),0+(vcell("$G$93","Input Geometry")-0)*(vcell("E72","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E72","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E72","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(0-vcell("$G$95","Input Geometry"))*(vcell("E72","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E72","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E72","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G72')
    return None
xfunctions['Input Geometry!G72']=xcf_Input_Geometry_G72

#IF(E72!=0,IF(AVERAGE(VertSliceCalcs!Q5:Q6)>=$B$92,(($B$92-E72)*-9.807),((AVERAGE(VertSliceCalcs!Q5:Q6)-E72)*-9.807)),0)
def xcf_Input_Geometry_H72(): 
    try:      
        return IF(vcell("E72","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q5:Q6","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E72","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q5:Q6","Input Geometry"))-vcell("E72","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H72')
    return None
xfunctions['Input Geometry!H72']=xcf_Input_Geometry_H72

#IF(OR(VertSliceCalcs!Q33==-99999,VertSliceCalcs!Q34==-99999),0,AVERAGE(VertSliceCalcs!P33:P34))
def xcf_Input_Geometry_C73(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q33","Input Geometry")==-99999,vcell("VertSliceCalcs!Q34","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P33:P34","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C73')
    return None
xfunctions['Input Geometry!C73']=xcf_Input_Geometry_C73

#IF(OR(VertSliceCalcs!Q33==-99999,VertSliceCalcs!Q34==-99999),0,AVERAGE(VertSliceCalcs!Q33:Q34))
def xcf_Input_Geometry_E73(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q33","Input Geometry")==-99999,vcell("VertSliceCalcs!Q34","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q33:Q34","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E73')
    return None
xfunctions['Input Geometry!E73']=xcf_Input_Geometry_E73

#IF(E73!=0,IF($B$96==1,H73,IF(E73>$F$93,$G$93+($G$92-$G$93)*(E73-$F$93)/($F$92-$F$93),IF(AND(E73<=$F$93,E73>$F$94),$G$94+($G$93-$G$94)*(E73-$F$94)/($F$93-$F$94),IF(AND(E73<=$F$94,E73>$F$95),$G$95+($G$94-$G$95)*(E73-$F$95)/($F$94-$F$95),IF(E73<=$F$95,$G$96+($G$95-$G$96)*(E73-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G73(): 
    try:      
        return IF(vcell("E73","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H73","Input Geometry"),IF(vcell("E73","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E73","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E73","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E73","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E73","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E73","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E73","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E73","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E73","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E73","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G73')
    return None
xfunctions['Input Geometry!G73']=xcf_Input_Geometry_G73

#IF(E73!=0,IF(AVERAGE(VertSliceCalcs!Q6:Q7)>=$B$92,(($B$92-E73)*-9.807),((AVERAGE(VertSliceCalcs!Q6:Q7)-E73)*-9.807)),0)
def xcf_Input_Geometry_H73(): 
    try:      
        return IF(vcell("E73","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q6:Q7","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E73","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q6:Q7","Input Geometry"))-vcell("E73","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H73')
    return None
xfunctions['Input Geometry!H73']=xcf_Input_Geometry_H73

#IF(OR(VertSliceCalcs!Q34==-99999,VertSliceCalcs!Q35==-99999),0,AVERAGE(VertSliceCalcs!P34:P35))
def xcf_Input_Geometry_C74(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q34","Input Geometry")==-99999,vcell("VertSliceCalcs!Q35","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P34:P35","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C74')
    return None
xfunctions['Input Geometry!C74']=xcf_Input_Geometry_C74

#IF(OR(VertSliceCalcs!Q34==-99999,VertSliceCalcs!Q35==-99999),0,AVERAGE(VertSliceCalcs!Q34:Q35))
def xcf_Input_Geometry_E74(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q34","Input Geometry")==-99999,vcell("VertSliceCalcs!Q35","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q34:Q35","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E74')
    return None
xfunctions['Input Geometry!E74']=xcf_Input_Geometry_E74

#IF(E74!=0,IF($B$96==1,H74,IF(E74>$F$93,$G$93+($G$92-$G$93)*(E74-$F$93)/($F$92-$F$93),IF(AND(E74<=$F$93,E74>$F$94),$G$94+($G$93-$G$94)*(E74-$F$94)/($F$93-$F$94),IF(AND(E74<=$F$94,E74>$F$95),$G$95+($G$94-$G$95)*(E74-$F$95)/($F$94-$F$95),IF(E74<=$F$95,$G$96+($G$95-$G$96)*(E74-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G74(): 
    try:      
        return IF(vcell("E74","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H74","Input Geometry"),IF(vcell("E74","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E74","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E74","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E74","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E74","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E74","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E74","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E74","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E74","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E74","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G74')
    return None
xfunctions['Input Geometry!G74']=xcf_Input_Geometry_G74

#IF(E74!=0,IF(AVERAGE(VertSliceCalcs!Q7:Q8)>=$B$92,(($B$92-E74)*-9.807),((AVERAGE(VertSliceCalcs!Q7:Q8)-E74)*-9.807)),0)
def xcf_Input_Geometry_H74(): 
    try:      
        return IF(vcell("E74","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q7:Q8","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E74","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q7:Q8","Input Geometry"))-vcell("E74","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H74')
    return None
xfunctions['Input Geometry!H74']=xcf_Input_Geometry_H74

#IF(OR(VertSliceCalcs!Q35==-99999,VertSliceCalcs!Q36==-99999),0,AVERAGE(VertSliceCalcs!P35:P36))
def xcf_Input_Geometry_C75(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q35","Input Geometry")==-99999,vcell("VertSliceCalcs!Q36","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P35:P36","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C75')
    return None
xfunctions['Input Geometry!C75']=xcf_Input_Geometry_C75

#IF(OR(VertSliceCalcs!Q35==-99999,VertSliceCalcs!Q36==-99999),0,AVERAGE(VertSliceCalcs!Q35:Q36))
def xcf_Input_Geometry_E75(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q35","Input Geometry")==-99999,vcell("VertSliceCalcs!Q36","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q35:Q36","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E75')
    return None
xfunctions['Input Geometry!E75']=xcf_Input_Geometry_E75

#IF(E75!=0,IF($B$96==1,H75,IF(E75>$F$93,$G$93+($G$92-$G$93)*(E75-$F$93)/($F$92-$F$93),IF(AND(E75<=$F$93,E75>$F$94),$G$94+($G$93-$G$94)*(E75-$F$94)/($F$93-$F$94),IF(AND(E75<=$F$94,E75>$F$95),$G$95+($G$94-$G$95)*(E75-$F$95)/($F$94-$F$95),IF(E75<=$F$95,$G$96+($G$95-$G$96)*(E75-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G75(): 
    try:      
        return IF(vcell("E75","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H75","Input Geometry"),IF(vcell("E75","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E75","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E75","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E75","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E75","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E75","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E75","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E75","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E75","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E75","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G75')
    return None
xfunctions['Input Geometry!G75']=xcf_Input_Geometry_G75

#IF(E75!=0,IF(AVERAGE(VertSliceCalcs!Q8:Q9)>=$B$92,(($B$92-E75)*-9.807),((AVERAGE(VertSliceCalcs!Q8:Q9)-E75)*-9.807)),0)
def xcf_Input_Geometry_H75(): 
    try:      
        return IF(vcell("E75","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q8:Q9","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E75","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q8:Q9","Input Geometry"))-vcell("E75","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H75')
    return None
xfunctions['Input Geometry!H75']=xcf_Input_Geometry_H75

#IF(OR(VertSliceCalcs!Q36==-99999,VertSliceCalcs!Q37==-99999),0,AVERAGE(VertSliceCalcs!P36:P37))
def xcf_Input_Geometry_C76(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q36","Input Geometry")==-99999,vcell("VertSliceCalcs!Q37","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P36:P37","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C76')
    return None
xfunctions['Input Geometry!C76']=xcf_Input_Geometry_C76

#IF(OR(VertSliceCalcs!Q36==-99999,VertSliceCalcs!Q37==-99999),0,AVERAGE(VertSliceCalcs!Q36:Q37))
def xcf_Input_Geometry_E76(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q36","Input Geometry")==-99999,vcell("VertSliceCalcs!Q37","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q36:Q37","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E76')
    return None
xfunctions['Input Geometry!E76']=xcf_Input_Geometry_E76

#IF(E76!=0,IF($B$96==1,H76,IF(E76>$F$93,$G$93+($G$92-$G$93)*(E76-$F$93)/($F$92-$F$93),IF(AND(E76<=$F$93,E76>$F$94),$G$94+($G$93-$G$94)*(E76-$F$94)/($F$93-$F$94),IF(AND(E76<=$F$94,E76>$F$95),$G$95+($G$94-$G$95)*(E76-$F$95)/($F$94-$F$95),IF(E76<=$F$95,$G$96+($G$95-$G$96)*(E76-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G76(): 
    try:      
        return IF(vcell("E76","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H76","Input Geometry"),IF(vcell("E76","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E76","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E76","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E76","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E76","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E76","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E76","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E76","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E76","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E76","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G76')
    return None
xfunctions['Input Geometry!G76']=xcf_Input_Geometry_G76

#IF(E76!=0,IF(AVERAGE(VertSliceCalcs!Q9:Q10)>=$B$92,(($B$92-E76)*-9.807),((AVERAGE(VertSliceCalcs!Q9:Q10)-E76)*-9.807)),0)
def xcf_Input_Geometry_H76(): 
    try:      
        return IF(vcell("E76","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q9:Q10","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E76","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q9:Q10","Input Geometry"))-vcell("E76","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H76')
    return None
xfunctions['Input Geometry!H76']=xcf_Input_Geometry_H76

#IF(OR(VertSliceCalcs!Q37==-99999,VertSliceCalcs!Q38==-99999),0,AVERAGE(VertSliceCalcs!P37:P38))
def xcf_Input_Geometry_C77(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q37","Input Geometry")==-99999,vcell("VertSliceCalcs!Q38","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P37:P38","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C77')
    return None
xfunctions['Input Geometry!C77']=xcf_Input_Geometry_C77

#IF(OR(VertSliceCalcs!Q37==-99999,VertSliceCalcs!Q38==-99999),0,AVERAGE(VertSliceCalcs!Q37:Q38))
def xcf_Input_Geometry_E77(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q37","Input Geometry")==-99999,vcell("VertSliceCalcs!Q38","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q37:Q38","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E77')
    return None
xfunctions['Input Geometry!E77']=xcf_Input_Geometry_E77

#IF(E77!=0,IF($B$96==1,H77,IF(E77>$F$93,$G$93+($G$92-$G$93)*(E77-$F$93)/($F$92-$F$93),IF(AND(E77<=$F$93,E77>$F$94),$G$94+($G$93-$G$94)*(E77-$F$94)/($F$93-$F$94),IF(AND(E77<=$F$94,E77>$F$95),$G$95+($G$94-$G$95)*(E77-$F$95)/($F$94-$F$95),IF(E77<=$F$95,$G$96+($G$95-$G$96)*(E77-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G77(): 
    try:      
        return IF(vcell("E77","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H77","Input Geometry"),IF(vcell("E77","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E77","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E77","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E77","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E77","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E77","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E77","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E77","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E77","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E77","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G77')
    return None
xfunctions['Input Geometry!G77']=xcf_Input_Geometry_G77

#IF(E77!=0,IF(AVERAGE(VertSliceCalcs!Q10:Q11)>=$B$92,(($B$92-E77)*-9.807),((AVERAGE(VertSliceCalcs!Q10:Q11)-E77)*-9.807)),0)
def xcf_Input_Geometry_H77(): 
    try:      
        return IF(vcell("E77","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q10:Q11","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E77","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q10:Q11","Input Geometry"))-vcell("E77","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H77')
    return None
xfunctions['Input Geometry!H77']=xcf_Input_Geometry_H77

#IF(OR(VertSliceCalcs!Q38==-99999,VertSliceCalcs!Q39==-99999),0,AVERAGE(VertSliceCalcs!P38:P39))
def xcf_Input_Geometry_C78(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q38","Input Geometry")==-99999,vcell("VertSliceCalcs!Q39","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P38:P39","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C78')
    return None
xfunctions['Input Geometry!C78']=xcf_Input_Geometry_C78

#IF(OR(VertSliceCalcs!Q38==-99999,VertSliceCalcs!Q39==-99999),0,AVERAGE(VertSliceCalcs!Q38:Q39))
def xcf_Input_Geometry_E78(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q38","Input Geometry")==-99999,vcell("VertSliceCalcs!Q39","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q38:Q39","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E78')
    return None
xfunctions['Input Geometry!E78']=xcf_Input_Geometry_E78

#IF(E78!=0,IF($B$96==1,H78,IF(E78>$F$93,$G$93+($G$92-$G$93)*(E78-$F$93)/($F$92-$F$93),IF(AND(E78<=$F$93,E78>$F$94),$G$94+($G$93-$G$94)*(E78-$F$94)/($F$93-$F$94),IF(AND(E78<=$F$94,E78>$F$95),$G$95+($G$94-$G$95)*(E78-$F$95)/($F$94-$F$95),IF(E78<=$F$95,$G$96+($G$95-$G$96)*(E78-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G78(): 
    try:      
        return IF(vcell("E78","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H78","Input Geometry"),IF(vcell("E78","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E78","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E78","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E78","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E78","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E78","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E78","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E78","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E78","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E78","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G78')
    return None
xfunctions['Input Geometry!G78']=xcf_Input_Geometry_G78

#IF(E78!=0,IF(AVERAGE(VertSliceCalcs!Q11:Q12)>=$B$92,(($B$92-E78)*-9.807),((AVERAGE(VertSliceCalcs!Q11:Q12)-E78)*-9.807)),0)
def xcf_Input_Geometry_H78(): 
    try:      
        return IF(vcell("E78","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q11:Q12","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E78","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q11:Q12","Input Geometry"))-vcell("E78","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H78')
    return None
xfunctions['Input Geometry!H78']=xcf_Input_Geometry_H78

#IF(OR(VertSliceCalcs!Q39==-99999,VertSliceCalcs!Q40==-99999),0,AVERAGE(VertSliceCalcs!P39:P40))
def xcf_Input_Geometry_C79(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q39","Input Geometry")==-99999,vcell("VertSliceCalcs!Q40","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P39:P40","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C79')
    return None
xfunctions['Input Geometry!C79']=xcf_Input_Geometry_C79

#IF(OR(VertSliceCalcs!Q39==-99999,VertSliceCalcs!Q40==-99999),0,AVERAGE(VertSliceCalcs!Q39:Q40))
def xcf_Input_Geometry_E79(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q39","Input Geometry")==-99999,vcell("VertSliceCalcs!Q40","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q39:Q40","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E79')
    return None
xfunctions['Input Geometry!E79']=xcf_Input_Geometry_E79

#IF(E79!=0,IF($B$96==1,H79,IF(E79>$F$93,$G$93+($G$92-$G$93)*(E79-$F$93)/($F$92-$F$93),IF(AND(E79<=$F$93,E79>$F$94),$G$94+($G$93-$G$94)*(E79-$F$94)/($F$93-$F$94),IF(AND(E79<=$F$94,E79>$F$95),$G$95+($G$94-$G$95)*(E79-$F$95)/($F$94-$F$95),IF(E79<=$F$95,$G$96+($G$95-$G$96)*(E79-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G79(): 
    try:      
        return IF(vcell("E79","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H79","Input Geometry"),IF(vcell("E79","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E79","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E79","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E79","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E79","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E79","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E79","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E79","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E79","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E79","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G79')
    return None
xfunctions['Input Geometry!G79']=xcf_Input_Geometry_G79

#IF(E79!=0,IF(AVERAGE(VertSliceCalcs!Q12:Q13)>=$B$92,(($B$92-E79)*-9.807),((AVERAGE(VertSliceCalcs!Q12:Q13)-E79)*-9.807)),0)
def xcf_Input_Geometry_H79(): 
    try:      
        return IF(vcell("E79","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q12:Q13","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E79","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q12:Q13","Input Geometry"))-vcell("E79","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H79')
    return None
xfunctions['Input Geometry!H79']=xcf_Input_Geometry_H79

#IF(OR(VertSliceCalcs!Q40==-99999,VertSliceCalcs!Q41==-99999),0,AVERAGE(VertSliceCalcs!P40:P41))
def xcf_Input_Geometry_C80(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q40","Input Geometry")==-99999,vcell("VertSliceCalcs!Q41","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P40:P41","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C80')
    return None
xfunctions['Input Geometry!C80']=xcf_Input_Geometry_C80

#IF(OR(VertSliceCalcs!Q40==-99999,VertSliceCalcs!Q41==-99999),0,AVERAGE(VertSliceCalcs!Q40:Q41))
def xcf_Input_Geometry_E80(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q40","Input Geometry")==-99999,vcell("VertSliceCalcs!Q41","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q40:Q41","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E80')
    return None
xfunctions['Input Geometry!E80']=xcf_Input_Geometry_E80

#IF(E80!=0,IF($B$96==1,H80,IF(E80>$F$93,$G$93+($G$92-$G$93)*(E80-$F$93)/($F$92-$F$93),IF(AND(E80<=$F$93,E80>$F$94),$G$94+($G$93-$G$94)*(E80-$F$94)/($F$93-$F$94),IF(AND(E80<=$F$94,E80>$F$95),$G$95+($G$94-$G$95)*(E80-$F$95)/($F$94-$F$95),IF(E80<=$F$95,$G$96+($G$95-$G$96)*(E80-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G80(): 
    try:      
        return IF(vcell("E80","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H80","Input Geometry"),IF(vcell("E80","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E80","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E80","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E80","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E80","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E80","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E80","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E80","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E80","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E80","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G80')
    return None
xfunctions['Input Geometry!G80']=xcf_Input_Geometry_G80

#IF(E80!=0,IF(AVERAGE(VertSliceCalcs!Q13:Q14)>=$B$92,(($B$92-E80)*-9.807),((AVERAGE(VertSliceCalcs!Q13:Q14)-E80)*-9.807)),0)
def xcf_Input_Geometry_H80(): 
    try:      
        return IF(vcell("E80","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q13:Q14","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E80","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q13:Q14","Input Geometry"))-vcell("E80","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H80')
    return None
xfunctions['Input Geometry!H80']=xcf_Input_Geometry_H80

#IF(OR(VertSliceCalcs!Q41==-99999,VertSliceCalcs!Q42==-99999),0,AVERAGE(VertSliceCalcs!P41:P42))
def xcf_Input_Geometry_C81(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q41","Input Geometry")==-99999,vcell("VertSliceCalcs!Q42","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P41:P42","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C81')
    return None
xfunctions['Input Geometry!C81']=xcf_Input_Geometry_C81

#IF(OR(VertSliceCalcs!Q41==-99999,VertSliceCalcs!Q42==-99999),0,AVERAGE(VertSliceCalcs!Q41:Q42))
def xcf_Input_Geometry_E81(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q41","Input Geometry")==-99999,vcell("VertSliceCalcs!Q42","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q41:Q42","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E81')
    return None
xfunctions['Input Geometry!E81']=xcf_Input_Geometry_E81

#IF(E81!=0,IF($B$96==1,H81,IF(E81>$F$93,$G$93+($G$92-$G$93)*(E81-$F$93)/($F$92-$F$93),IF(AND(E81<=$F$93,E81>$F$94),$G$94+($G$93-$G$94)*(E81-$F$94)/($F$93-$F$94),IF(AND(E81<=$F$94,E81>$F$95),$G$95+($G$94-$G$95)*(E81-$F$95)/($F$94-$F$95),IF(E81<=$F$95,$G$96+($G$95-$G$96)*(E81-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G81(): 
    try:      
        return IF(vcell("E81","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H81","Input Geometry"),IF(vcell("E81","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E81","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E81","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E81","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E81","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E81","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E81","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E81","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E81","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E81","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G81')
    return None
xfunctions['Input Geometry!G81']=xcf_Input_Geometry_G81

#IF(E81!=0,IF(AVERAGE(VertSliceCalcs!Q14:Q15)>=$B$92,(($B$92-E81)*-9.807),((AVERAGE(VertSliceCalcs!Q14:Q15)-E81)*-9.807)),0)
def xcf_Input_Geometry_H81(): 
    try:      
        return IF(vcell("E81","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q14:Q15","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E81","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q14:Q15","Input Geometry"))-vcell("E81","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H81')
    return None
xfunctions['Input Geometry!H81']=xcf_Input_Geometry_H81

#IF(OR(VertSliceCalcs!Q42==-99999,VertSliceCalcs!Q43==-99999),0,AVERAGE(VertSliceCalcs!P42:P43))
def xcf_Input_Geometry_C82(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q42","Input Geometry")==-99999,vcell("VertSliceCalcs!Q43","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P42:P43","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C82')
    return None
xfunctions['Input Geometry!C82']=xcf_Input_Geometry_C82

#IF(OR(VertSliceCalcs!Q42==-99999,VertSliceCalcs!Q43==-99999),0,AVERAGE(VertSliceCalcs!Q42:Q43))
def xcf_Input_Geometry_E82(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q42","Input Geometry")==-99999,vcell("VertSliceCalcs!Q43","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q42:Q43","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E82')
    return None
xfunctions['Input Geometry!E82']=xcf_Input_Geometry_E82

#IF(E82!=0,IF($B$96==1,H82,IF(E82>$F$93,$G$93+($G$92-$G$93)*(E82-$F$93)/($F$92-$F$93),IF(AND(E82<=$F$93,E82>$F$94),$G$94+($G$93-$G$94)*(E82-$F$94)/($F$93-$F$94),IF(AND(E82<=$F$94,E82>$F$95),$G$95+($G$94-$G$95)*(E82-$F$95)/($F$94-$F$95),IF(E82<=$F$95,$G$96+($G$95-$G$96)*(E82-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G82(): 
    try:      
        return IF(vcell("E82","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H82","Input Geometry"),IF(vcell("E82","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E82","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E82","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E82","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E82","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E82","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E82","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E82","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E82","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E82","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G82')
    return None
xfunctions['Input Geometry!G82']=xcf_Input_Geometry_G82

#IF(E82!=0,IF(AVERAGE(VertSliceCalcs!Q15:Q16)>=$B$92,(($B$92-E82)*-9.807),((AVERAGE(VertSliceCalcs!Q15:Q16)-E82)*-9.807)),0)
def xcf_Input_Geometry_H82(): 
    try:      
        return IF(vcell("E82","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q15:Q16","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E82","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q15:Q16","Input Geometry"))-vcell("E82","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H82')
    return None
xfunctions['Input Geometry!H82']=xcf_Input_Geometry_H82

#IF(OR(VertSliceCalcs!Q43==-99999,VertSliceCalcs!Q44==-99999),0,AVERAGE(VertSliceCalcs!P43:P44))
def xcf_Input_Geometry_C83(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q43","Input Geometry")==-99999,vcell("VertSliceCalcs!Q44","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P43:P44","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C83')
    return None
xfunctions['Input Geometry!C83']=xcf_Input_Geometry_C83

#IF(OR(VertSliceCalcs!Q43==-99999,VertSliceCalcs!Q44==-99999),0,AVERAGE(VertSliceCalcs!Q43:Q44))
def xcf_Input_Geometry_E83(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q43","Input Geometry")==-99999,vcell("VertSliceCalcs!Q44","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q43:Q44","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E83')
    return None
xfunctions['Input Geometry!E83']=xcf_Input_Geometry_E83

#IF(E83!=0,IF($B$96==1,H83,IF(E83>$F$93,$G$93+($G$92-$G$93)*(E83-$F$93)/($F$92-$F$93),IF(AND(E83<=$F$93,E83>$F$94),$G$94+($G$93-$G$94)*(E83-$F$94)/($F$93-$F$94),IF(AND(E83<=$F$94,E83>$F$95),$G$95+($G$94-$G$95)*(E83-$F$95)/($F$94-$F$95),IF(E83<=$F$95,$G$96+($G$95-$G$96)*(E83-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G83(): 
    try:      
        return IF(vcell("E83","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H83","Input Geometry"),IF(vcell("E83","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E83","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E83","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E83","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E83","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E83","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E83","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E83","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E83","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E83","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G83')
    return None
xfunctions['Input Geometry!G83']=xcf_Input_Geometry_G83

#IF(E83!=0,IF(AVERAGE(VertSliceCalcs!Q16:Q17)>=$B$92,(($B$92-E83)*-9.807),((AVERAGE(VertSliceCalcs!Q16:Q17)-E83)*-9.807)),0)
def xcf_Input_Geometry_H83(): 
    try:      
        return IF(vcell("E83","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q16:Q17","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E83","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q16:Q17","Input Geometry"))-vcell("E83","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H83')
    return None
xfunctions['Input Geometry!H83']=xcf_Input_Geometry_H83

#IF(OR(VertSliceCalcs!Q44==-99999,VertSliceCalcs!Q45==-99999),0,AVERAGE(VertSliceCalcs!P44:P45))
def xcf_Input_Geometry_C84(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q44","Input Geometry")==-99999,vcell("VertSliceCalcs!Q45","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P44:P45","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C84')
    return None
xfunctions['Input Geometry!C84']=xcf_Input_Geometry_C84

#IF(OR(VertSliceCalcs!Q44==-99999,VertSliceCalcs!Q45==-99999),0,AVERAGE(VertSliceCalcs!Q44:Q45))
def xcf_Input_Geometry_E84(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q44","Input Geometry")==-99999,vcell("VertSliceCalcs!Q45","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q44:Q45","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E84')
    return None
xfunctions['Input Geometry!E84']=xcf_Input_Geometry_E84

#IF(E84!=0,IF($B$96==1,H84,IF(E84>$F$93,$G$93+($G$92-$G$93)*(E84-$F$93)/($F$92-$F$93),IF(AND(E84<=$F$93,E84>$F$94),$G$94+($G$93-$G$94)*(E84-$F$94)/($F$93-$F$94),IF(AND(E84<=$F$94,E84>$F$95),$G$95+($G$94-$G$95)*(E84-$F$95)/($F$94-$F$95),IF(E84<=$F$95,$G$96+($G$95-$G$96)*(E84-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G84(): 
    try:      
        return IF(vcell("E84","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H84","Input Geometry"),IF(vcell("E84","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E84","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E84","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E84","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E84","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E84","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E84","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E84","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E84","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E84","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G84')
    return None
xfunctions['Input Geometry!G84']=xcf_Input_Geometry_G84

#IF(E84!=0,IF(AVERAGE(VertSliceCalcs!Q17:Q18)>=$B$92,(($B$92-E84)*-9.807),((AVERAGE(VertSliceCalcs!Q17:Q18)-E84)*-9.807)),0)
def xcf_Input_Geometry_H84(): 
    try:      
        return IF(vcell("E84","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q17:Q18","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E84","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q17:Q18","Input Geometry"))-vcell("E84","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H84')
    return None
xfunctions['Input Geometry!H84']=xcf_Input_Geometry_H84

#IF(OR(VertSliceCalcs!Q45==-99999,VertSliceCalcs!Q46==-99999),0,AVERAGE(VertSliceCalcs!P45:P46))
def xcf_Input_Geometry_C85(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q45","Input Geometry")==-99999,vcell("VertSliceCalcs!Q46","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P45:P46","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C85')
    return None
xfunctions['Input Geometry!C85']=xcf_Input_Geometry_C85

#IF(OR(VertSliceCalcs!Q45==-99999,VertSliceCalcs!Q46==-99999),0,AVERAGE(VertSliceCalcs!Q45:Q46))
def xcf_Input_Geometry_E85(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q45","Input Geometry")==-99999,vcell("VertSliceCalcs!Q46","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q45:Q46","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E85')
    return None
xfunctions['Input Geometry!E85']=xcf_Input_Geometry_E85

#IF(E85!=0,IF($B$96==1,H85,IF(E85>$F$93,$G$93+($G$92-$G$93)*(E85-$F$93)/($F$92-$F$93),IF(AND(E85<=$F$93,E85>$F$94),$G$94+($G$93-$G$94)*(E85-$F$94)/($F$93-$F$94),IF(AND(E85<=$F$94,E85>$F$95),$G$95+($G$94-$G$95)*(E85-$F$95)/($F$94-$F$95),IF(E85<=$F$95,$G$96+($G$95-$G$96)*(E85-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G85(): 
    try:      
        return IF(vcell("E85","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H85","Input Geometry"),IF(vcell("E85","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E85","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E85","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E85","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E85","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E85","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E85","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E85","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E85","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E85","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G85')
    return None
xfunctions['Input Geometry!G85']=xcf_Input_Geometry_G85

#IF(E85!=0,IF(AVERAGE(VertSliceCalcs!Q18:Q19)>=$B$92,(($B$92-E85)*-9.807),((AVERAGE(VertSliceCalcs!Q18:Q19)-E85)*-9.807)),0)
def xcf_Input_Geometry_H85(): 
    try:      
        return IF(vcell("E85","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q18:Q19","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E85","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q18:Q19","Input Geometry"))-vcell("E85","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H85')
    return None
xfunctions['Input Geometry!H85']=xcf_Input_Geometry_H85

#IF(OR(VertSliceCalcs!Q46==-99999,VertSliceCalcs!Q47==-99999),0,AVERAGE(VertSliceCalcs!P46:P47))
def xcf_Input_Geometry_C86(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q46","Input Geometry")==-99999,vcell("VertSliceCalcs!Q47","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!P46:P47","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C86')
    return None
xfunctions['Input Geometry!C86']=xcf_Input_Geometry_C86

#IF(OR(VertSliceCalcs!Q46==-99999,VertSliceCalcs!Q47==-99999),0,AVERAGE(VertSliceCalcs!Q46:Q47))
def xcf_Input_Geometry_E86(): 
    try:      
        return IF(OR(vcell("VertSliceCalcs!Q46","Input Geometry")==-99999,vcell("VertSliceCalcs!Q47","Input Geometry")==-99999),0,AVERAGE(vcell("VertSliceCalcs!Q46:Q47","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E86')
    return None
xfunctions['Input Geometry!E86']=xcf_Input_Geometry_E86

#IF(E86!=0,IF($B$96==1,H86,IF(E86>$F$93,$G$93+($G$92-$G$93)*(E86-$F$93)/($F$92-$F$93),IF(AND(E86<=$F$93,E86>$F$94),$G$94+($G$93-$G$94)*(E86-$F$94)/($F$93-$F$94),IF(AND(E86<=$F$94,E86>$F$95),$G$95+($G$94-$G$95)*(E86-$F$95)/($F$94-$F$95),IF(E86<=$F$95,$G$96+($G$95-$G$96)*(E86-$F$96)/($F$95-$F$96)))))),0)
def xcf_Input_Geometry_G86(): 
    try:      
        return IF(vcell("E86","Input Geometry")!=0,IF(vcell("$B$96","Input Geometry")==1,vcell("H86","Input Geometry"),IF(vcell("E86","Input Geometry")>vcell("$F$93","Input Geometry"),vcell("$G$93","Input Geometry")+(vcell("$G$92","Input Geometry")-vcell("$G$93","Input Geometry"))*(vcell("E86","Input Geometry")-vcell("$F$93","Input Geometry"))/(vcell("$F$92","Input Geometry")-vcell("$F$93","Input Geometry")),IF(AND(vcell("E86","Input Geometry")<=vcell("$F$93","Input Geometry"),vcell("E86","Input Geometry")>vcell("$F$94","Input Geometry")),vcell("$G$94","Input Geometry")+(vcell("$G$93","Input Geometry")-vcell("$G$94","Input Geometry"))*(vcell("E86","Input Geometry")-vcell("$F$94","Input Geometry"))/(vcell("$F$93","Input Geometry")-vcell("$F$94","Input Geometry")),IF(AND(vcell("E86","Input Geometry")<=vcell("$F$94","Input Geometry"),vcell("E86","Input Geometry")>vcell("$F$95","Input Geometry")),vcell("$G$95","Input Geometry")+(vcell("$G$94","Input Geometry")-vcell("$G$95","Input Geometry"))*(vcell("E86","Input Geometry")-vcell("$F$95","Input Geometry"))/(vcell("$F$94","Input Geometry")-vcell("$F$95","Input Geometry")),IF(vcell("E86","Input Geometry")<=vcell("$F$95","Input Geometry"),vcell("$G$96","Input Geometry")+(vcell("$G$95","Input Geometry")-vcell("$G$96","Input Geometry"))*(vcell("E86","Input Geometry")-vcell("$F$96","Input Geometry"))/(vcell("$F$95","Input Geometry")-vcell("$F$96","Input Geometry"))))))),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G86')
    return None
xfunctions['Input Geometry!G86']=xcf_Input_Geometry_G86

#IF(E86!=0,IF(AVERAGE(VertSliceCalcs!Q19:Q20)>=$B$92,(($B$92-E86)*-9.807),((AVERAGE(VertSliceCalcs!Q19:Q20)-E86)*-9.807)),0)
def xcf_Input_Geometry_H86(): 
    try:      
        return IF(vcell("E86","Input Geometry")!=0,IF(AVERAGE(vcell("VertSliceCalcs!Q19:Q20","Input Geometry"))>=vcell("$B$92","Input Geometry"),((vcell("$B$92","Input Geometry")-vcell("E86","Input Geometry"))*-9.807),((AVERAGE(vcell("VertSliceCalcs!Q19:Q20","Input Geometry"))-vcell("E86","Input Geometry"))*-9.807)),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H86')
    return None
xfunctions['Input Geometry!H86']=xcf_Input_Geometry_H86

#IF(B96==1,E115-Bank_Model_Output!G11,IF(0>=$G$93,MIN($F92-$G92*($F92-$F93)/($G92-$G93),E115),IF(AND(0<$G$93,0>=$G$94),$F93-$G93*($F93-$F94)/($G93-$G94),IF(AND(0<$G$94,0>=$G$95),$F94-$G94*($F94-$F95)/($G94-$G95),$F95-$G95*($F95-$F96)/($G95-$G96)))))
def xcf_Input_Geometry_B92(): 
    try:      
        # return IF(vcell("B96","Input Geometry")==1,vcell("E115","Input Geometry")-vcell("Bank_Model_Output!G11","Input Geometry"),IF(0>=vcell("$G$93","Input Geometry"),MIN(vcell("$F92","Input Geometry")-vcell("$G92","Input Geometry")*(vcell("$F92","Input Geometry")-vcell("$F93","Input Geometry"))/(vcell("$G92","Input Geometry")-vcell("$G93","Input Geometry")),vcell("E115","Input Geometry")),IF(AND(0<vcell("$G$93","Input Geometry"),0>=vcell("$G$94","Input Geometry")),vcell("$F93","Input Geometry")-vcell("$G93","Input Geometry")*(vcell("$F93","Input Geometry")-vcell("$F94","Input Geometry"))/(vcell("$G93","Input Geometry")-vcell("$G94","Input Geometry")),IF(AND(0<vcell("$G$94","Input Geometry"),0>=vcell("$G$95","Input Geometry")),vcell("$F94","Input Geometry")-vcell("$G94","Input Geometry")*(vcell("$F94","Input Geometry")-vcell("$F95","Input Geometry"))/(vcell("$G94","Input Geometry")-vcell("$G95","Input Geometry")),vcell("$F95","Input Geometry")-vcell("$G95","Input Geometry")*(vcell("$F95","Input Geometry")-vcell("$F96","Input Geometry"))/(vcell("$G95","Input Geometry")-vcell("$G96","Input Geometry"))))))
        return IF(vcell("B96","Input Geometry")==1,vcell("E115","Input Geometry")-vcell("Bank_Model_Output!G11","Input Geometry"),IF(0>=0,MIN(vcell("$F92","Input Geometry")-1*(vcell("$F92","Input Geometry")-vcell("$F93","Input Geometry"))/1,vcell("E115","Input Geometry")),IF(AND(0<0,0>=0),vcell("$F93","Input Geometry")-1*(vcell("$F93","Input Geometry")-vcell("$F94","Input Geometry"))/1,IF(AND(0<0,0>=0),vcell("$F94","Input Geometry")-1*(vcell("$F94","Input Geometry")-vcell("$F95","Input Geometry"))/1,vcell("$F95","Input Geometry")-1*(vcell("$F95","Input Geometry")-vcell("$F96","Input Geometry"))/1))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_B92')
    return None
xfunctions['Input Geometry!B92']=xcf_Input_Geometry_B92

#MAX(E114:E136)
def xcf_Input_Geometry_E92(): 
    try:      
        return MAX(vcell("E114:E136","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E92')
    return None
xfunctions['Input Geometry!E92']=xcf_Input_Geometry_E92

#E115-0.5*H35
def xcf_Input_Geometry_F92(): 
    try:      
        return vcell("E115","Input Geometry")-0.5*vcell("H35","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_F92')
    return None
xfunctions['Input Geometry!F92']=xcf_Input_Geometry_F92

#IF($B$96==1,(($B$92-F92)*-9.807),Bank_Model_Output!G15*-1)
def xcf_Input_Geometry_G92(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("F92","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G15","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G92')
    return None
xfunctions['Input Geometry!G92']=xcf_Input_Geometry_G92

#(($B$92-F92)*-9.807)
def xcf_Input_Geometry_H92(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("F92","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H92')
    return None
xfunctions['Input Geometry!H92']=xcf_Input_Geometry_H92

#IF(E92>$E$138,IF(E93>$E$138,E92-E93,E92-$E$138),0)
def xcf_Input_Geometry_J92(): 
    try:      
        return IF(vcell("E92","Input Geometry")>vcell("$E$138","Input Geometry"),IF(vcell("E93","Input Geometry")>vcell("$E$138","Input Geometry"),vcell("E92","Input Geometry")-vcell("E93","Input Geometry"),vcell("E92","Input Geometry")-vcell("$E$138","Input Geometry")),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J92')
    return None
xfunctions['Input Geometry!J92']=xcf_Input_Geometry_J92

#IF(J92!=0,$E$115-0.5*J92,0)
def xcf_Input_Geometry_L92(): 
    try:      
        return IF(vcell("J92","Input Geometry")!=0,vcell("$E$115","Input Geometry")-0.5*vcell("J92","Input Geometry"),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_L92')
    return None
xfunctions['Input Geometry!L92']=xcf_Input_Geometry_L92

#IF($B$96==1,(($B$92-L92)*-9.807),Bank_Model_Output!G15*-1)
def xcf_Input_Geometry_M92(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("L92","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G15","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_M92')
    return None
xfunctions['Input Geometry!M92']=xcf_Input_Geometry_M92

#(($B$92-L92)*-9.807)
def xcf_Input_Geometry_N92(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("L92","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_N92')
    return None
xfunctions['Input Geometry!N92']=xcf_Input_Geometry_N92

#E115-H35
def xcf_Input_Geometry_E93(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E93')
    return None
xfunctions['Input Geometry!E93']=xcf_Input_Geometry_E93

#E115-H35-0.5*H37
def xcf_Input_Geometry_F93(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")-0.5*vcell("H37","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_F93')
    return None
xfunctions['Input Geometry!F93']=xcf_Input_Geometry_F93

#IF($B$96==1,(($B$92-F93)*-9.807),Bank_Model_Output!G17*-1)
def xcf_Input_Geometry_G93(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("F93","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G17","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G93')
    return None
xfunctions['Input Geometry!G93']=xcf_Input_Geometry_G93

#(($B$92-F93)*-9.807)
def xcf_Input_Geometry_H93(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("F93","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H93')
    return None
xfunctions['Input Geometry!H93']=xcf_Input_Geometry_H93

#IF(E93>$E$138,IF(E94>$E$138,E93-E94,E93-$E$138),0)
def xcf_Input_Geometry_J93(): 
    try:      
        return IF(vcell("E93","Input Geometry")>vcell("$E$138","Input Geometry"),IF(vcell("E94","Input Geometry")>vcell("$E$138","Input Geometry"),vcell("E93","Input Geometry")-vcell("E94","Input Geometry"),vcell("E93","Input Geometry")-vcell("$E$138","Input Geometry")),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J93')
    return None
xfunctions['Input Geometry!J93']=xcf_Input_Geometry_J93

#IF(J93!=0,E93-0.5*J93,0)
def xcf_Input_Geometry_L93(): 
    try:      
        return IF(vcell("J93","Input Geometry")!=0,vcell("E93","Input Geometry")-0.5*vcell("J93","Input Geometry"),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_L93')
    return None
xfunctions['Input Geometry!L93']=xcf_Input_Geometry_L93

#IF($B$96==1,(($B$92-L93)*-9.807),Bank_Model_Output!G17*-1)
def xcf_Input_Geometry_M93(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("L93","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G17","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_M93')
    return None
xfunctions['Input Geometry!M93']=xcf_Input_Geometry_M93

#(($B$92-L93)*-9.807)
def xcf_Input_Geometry_N93(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("L93","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_N93')
    return None
xfunctions['Input Geometry!N93']=xcf_Input_Geometry_N93

#E93-H37
def xcf_Input_Geometry_E94(): 
    try:      
        return vcell("E93","Input Geometry")-vcell("H37","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E94')
    return None
xfunctions['Input Geometry!E94']=xcf_Input_Geometry_E94

#E115-H35-H37-0.5*H39
def xcf_Input_Geometry_F94(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")-vcell("H37","Input Geometry")-0.5*vcell("H39","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_F94')
    return None
xfunctions['Input Geometry!F94']=xcf_Input_Geometry_F94

#IF($B$96==1,(($B$92-F94)*-9.807),Bank_Model_Output!G19*-1)
def xcf_Input_Geometry_G94(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("F94","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G19","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G94')
    return None
xfunctions['Input Geometry!G94']=xcf_Input_Geometry_G94

#(($B$92-F94)*-9.807)
def xcf_Input_Geometry_H94(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("F94","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H94')
    return None
xfunctions['Input Geometry!H94']=xcf_Input_Geometry_H94

#IF(E94>$E$138,IF(E95>$E$138,E94-E95,E94-$E$138),0)
def xcf_Input_Geometry_J94(): 
    try:      
        return IF(vcell("E94","Input Geometry")>vcell("$E$138","Input Geometry"),IF(vcell("E95","Input Geometry")>vcell("$E$138","Input Geometry"),vcell("E94","Input Geometry")-vcell("E95","Input Geometry"),vcell("E94","Input Geometry")-vcell("$E$138","Input Geometry")),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J94')
    return None
xfunctions['Input Geometry!J94']=xcf_Input_Geometry_J94

#IF(J94!=0,E94-0.5*J94,0)
def xcf_Input_Geometry_L94(): 
    try:      
        return IF(vcell("J94","Input Geometry")!=0,vcell("E94","Input Geometry")-0.5*vcell("J94","Input Geometry"),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_L94')
    return None
xfunctions['Input Geometry!L94']=xcf_Input_Geometry_L94

#IF($B$96==1,(($B$92-L94)*-9.807),Bank_Model_Output!G19*-1)
def xcf_Input_Geometry_M94(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("L94","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G19","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_M94')
    return None
xfunctions['Input Geometry!M94']=xcf_Input_Geometry_M94

#(($B$92-L94)*-9.807)
def xcf_Input_Geometry_N94(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("L94","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_N94')
    return None
xfunctions['Input Geometry!N94']=xcf_Input_Geometry_N94

#E94-H39
def xcf_Input_Geometry_E95(): 
    try:      
        return vcell("E94","Input Geometry")-vcell("H39","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E95')
    return None
xfunctions['Input Geometry!E95']=xcf_Input_Geometry_E95

#E115-H35-H37-H39-0.5*H41
def xcf_Input_Geometry_F95(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")-vcell("H37","Input Geometry")-vcell("H39","Input Geometry")-0.5*vcell("H41","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_F95')
    return None
xfunctions['Input Geometry!F95']=xcf_Input_Geometry_F95

#IF($B$96==1,(($B$92-F95)*-9.807),Bank_Model_Output!G21*-1)
def xcf_Input_Geometry_G95(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("F95","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G21","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G95')
    return None
xfunctions['Input Geometry!G95']=xcf_Input_Geometry_G95

#(($B$92-F95)*-9.807)
def xcf_Input_Geometry_H95(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("F95","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H95')
    return None
xfunctions['Input Geometry!H95']=xcf_Input_Geometry_H95

#IF(E95>$E$138,IF(E96>$E$138,E95-E96,E95-$E$138),0)
def xcf_Input_Geometry_J95(): 
    try:      
        return IF(vcell("E95","Input Geometry")>vcell("$E$138","Input Geometry"),IF(vcell("E96","Input Geometry")>vcell("$E$138","Input Geometry"),vcell("E95","Input Geometry")-vcell("E96","Input Geometry"),vcell("E95","Input Geometry")-vcell("$E$138","Input Geometry")),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J95')
    return None
xfunctions['Input Geometry!J95']=xcf_Input_Geometry_J95

#IF(J95!=0,E95-0.5*J95,0)
def xcf_Input_Geometry_L95(): 
    try:      
        return IF(vcell("J95","Input Geometry")!=0,vcell("E95","Input Geometry")-0.5*vcell("J95","Input Geometry"),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_L95')
    return None
xfunctions['Input Geometry!L95']=xcf_Input_Geometry_L95

#IF($B$96==1,(($B$92-L95)*-9.807),Bank_Model_Output!G21*-1)
def xcf_Input_Geometry_M95(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("L95","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G21","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_M95')
    return None
xfunctions['Input Geometry!M95']=xcf_Input_Geometry_M95

#(($B$92-L95)*-9.807)
def xcf_Input_Geometry_N95(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("L95","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_N95')
    return None
xfunctions['Input Geometry!N95']=xcf_Input_Geometry_N95

#E95-H41
def xcf_Input_Geometry_E96(): 
    try:      
        return vcell("E95","Input Geometry")-vcell("H41","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E96')
    return None
xfunctions['Input Geometry!E96']=xcf_Input_Geometry_E96

#E115-H35-H37-H39-H41-0.5*H43
def xcf_Input_Geometry_F96(): 
    try:      
        return vcell("E115","Input Geometry")-vcell("H35","Input Geometry")-vcell("H37","Input Geometry")-vcell("H39","Input Geometry")-vcell("H41","Input Geometry")-0.5*vcell("H43","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_F96')
    return None
xfunctions['Input Geometry!F96']=xcf_Input_Geometry_F96

#IF($B$96==1,(($B$92-F96)*-9.807),Bank_Model_Output!G23*-1)
def xcf_Input_Geometry_G96(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("F96","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G23","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_G96')
    return None
xfunctions['Input Geometry!G96']=xcf_Input_Geometry_G96

#(($B$92-F96)*-9.807)
def xcf_Input_Geometry_H96(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("F96","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_H96')
    return None
xfunctions['Input Geometry!H96']=xcf_Input_Geometry_H96

#IF(E96>$E$138,IF(E97>$E$138,E96-E97,E96-$E$138),0)
def xcf_Input_Geometry_J96(): 
    try:      
        return IF(vcell("E96","Input Geometry")>vcell("$E$138","Input Geometry"),IF(vcell("E97","Input Geometry")>vcell("$E$138","Input Geometry"),vcell("E96","Input Geometry")-vcell("E97","Input Geometry"),vcell("E96","Input Geometry")-vcell("$E$138","Input Geometry")),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_J96')
    return None
xfunctions['Input Geometry!J96']=xcf_Input_Geometry_J96

#IF(J96!=0,E96-0.5*J96,0)
def xcf_Input_Geometry_L96(): 
    try:      
        return IF(vcell("J96","Input Geometry")!=0,vcell("E96","Input Geometry")-0.5*vcell("J96","Input Geometry"),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_L96')
    return None
xfunctions['Input Geometry!L96']=xcf_Input_Geometry_L96

#IF($B$96==1,(($B$92-L96)*-9.807),Bank_Model_Output!G23*-1)
def xcf_Input_Geometry_M96(): 
    try:      
        return IF(vcell("$B$96","Input Geometry")==1,((vcell("$B$92","Input Geometry")-vcell("L96","Input Geometry"))*-9.807),vcell("Bank_Model_Output!G23","Input Geometry")*-1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_M96')
    return None
xfunctions['Input Geometry!M96']=xcf_Input_Geometry_M96

#(($B$92-L96)*-9.807)
def xcf_Input_Geometry_N96(): 
    try:      
        return ((vcell("$B$92","Input Geometry")-vcell("L96","Input Geometry"))*-9.807)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_N96')
    return None
xfunctions['Input Geometry!N96']=xcf_Input_Geometry_N96

#E96-H43
def xcf_Input_Geometry_E97(): 
    try:      
        return vcell("E96","Input Geometry")-vcell("H43","Input Geometry")
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E97')
    return None
xfunctions['Input Geometry!E97']=xcf_Input_Geometry_E97

#IF($C$109==1,IF(C20!="",C20,""),0)
def xcf_Input_Geometry_C114(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C20","Input Geometry")!="",vcell("C20","Input Geometry"),""),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C114')
    return None
xfunctions['Input Geometry!C114']=xcf_Input_Geometry_C114

#IF($C$109==1,IF(E20!="",E20,""),G$18)
def xcf_Input_Geometry_E114(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E20","Input Geometry")!="",vcell("E20","Input Geometry"),""),vcell("G$18","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E114')
    return None
xfunctions['Input Geometry!E114']=xcf_Input_Geometry_E114

#IF($C$109==1,IF(C21!="",C21,""),E$115)
def xcf_Input_Geometry_C115(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C21","Input Geometry")!="",vcell("C21","Input Geometry"),""),vcell("E$115","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C115')
    return None
xfunctions['Input Geometry!C115']=xcf_Input_Geometry_C115

#IF($C$109==1,IF(E21!="",E21,""),G$18)
def xcf_Input_Geometry_E115(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E21","Input Geometry")!="",vcell("E21","Input Geometry"),""),vcell("G$18","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E115')
    return None
xfunctions['Input Geometry!E115']=xcf_Input_Geometry_E115

#IF($C$109==1,IF(C22!="",C22,""),C$115+0.0666666666666667*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C116(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C22","Input Geometry")!="",vcell("C22","Input Geometry"),""),vcell("C$115","Input Geometry")+0.0666666666666667*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C116')
    return None
xfunctions['Input Geometry!C116']=xcf_Input_Geometry_C116

#IF($C$109==1,IF(E22!="",E22,""),E$130+(0.933333333333333*(E$115-E$130)))
def xcf_Input_Geometry_E116(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E22","Input Geometry")!="",vcell("E22","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.933333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E116')
    return None
xfunctions['Input Geometry!E116']=xcf_Input_Geometry_E116

#IF($C$109==1,IF(C23!="",C23,""),C$115+0.133333333333333*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C117(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C23","Input Geometry")!="",vcell("C23","Input Geometry"),""),vcell("C$115","Input Geometry")+0.133333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C117')
    return None
xfunctions['Input Geometry!C117']=xcf_Input_Geometry_C117

#IF($C$109==1,IF(E23!="",E23,""),E$130+(0.866666666666666*(E$115-E$130)))
def xcf_Input_Geometry_E117(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E23","Input Geometry")!="",vcell("E23","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.866666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E117')
    return None
xfunctions['Input Geometry!E117']=xcf_Input_Geometry_E117

#IF($C$109==1,IF(C24!="",C24,""),C$115+0.2*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C118(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C24","Input Geometry")!="",vcell("C24","Input Geometry"),""),vcell("C$115","Input Geometry")+0.2*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C118')
    return None
xfunctions['Input Geometry!C118']=xcf_Input_Geometry_C118

#IF($C$109==1,IF(E24!="",E24,""),E$130+(0.8*(E$115-E$130)))
def xcf_Input_Geometry_E118(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E24","Input Geometry")!="",vcell("E24","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.8*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E118')
    return None
xfunctions['Input Geometry!E118']=xcf_Input_Geometry_E118

#IF($C$109==1,IF(C25!="",C25,""),C$115+0.266666666666666*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C119(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C25","Input Geometry")!="",vcell("C25","Input Geometry"),""),vcell("C$115","Input Geometry")+0.266666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C119')
    return None
xfunctions['Input Geometry!C119']=xcf_Input_Geometry_C119

#IF($C$109==1,IF(E25!="",E25,""),E$130+(0.733333333333333*(E$115-E$130)))
def xcf_Input_Geometry_E119(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E25","Input Geometry")!="",vcell("E25","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.733333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E119')
    return None
xfunctions['Input Geometry!E119']=xcf_Input_Geometry_E119

#IF($C$109==1,IF(C26!="",C26,""),C$115+0.333333333333333*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C120(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C26","Input Geometry")!="",vcell("C26","Input Geometry"),""),vcell("C$115","Input Geometry")+0.333333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C120')
    return None
xfunctions['Input Geometry!C120']=xcf_Input_Geometry_C120

#IF($C$109==1,IF(E26!="",E26,""),E$130+(0.666666666666666*(E$115-E$130)))
def xcf_Input_Geometry_E120(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E26","Input Geometry")!="",vcell("E26","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.666666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E120')
    return None
xfunctions['Input Geometry!E120']=xcf_Input_Geometry_E120

#IF($C$109==1,IF(C27!="",C27,""),C$115+0.4*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C121(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C27","Input Geometry")!="",vcell("C27","Input Geometry"),""),vcell("C$115","Input Geometry")+0.4*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C121')
    return None
xfunctions['Input Geometry!C121']=xcf_Input_Geometry_C121

#IF($C$109==1,IF(E27!="",E27,""),E$130+(0.6*(E$115-E$130)))
def xcf_Input_Geometry_E121(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E27","Input Geometry")!="",vcell("E27","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.6*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E121')
    return None
xfunctions['Input Geometry!E121']=xcf_Input_Geometry_E121

#IF($C$109==1,IF(C28!="",C28,""),C$115+0.466666666666666*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C122(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C28","Input Geometry")!="",vcell("C28","Input Geometry"),""),vcell("C$115","Input Geometry")+0.466666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C122')
    return None
xfunctions['Input Geometry!C122']=xcf_Input_Geometry_C122

#IF($C$109==1,IF(E28!="",E28,""),E$130+(0.533333333333333*(E$115-E$130)))
def xcf_Input_Geometry_E122(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E28","Input Geometry")!="",vcell("E28","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.533333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E122')
    return None
xfunctions['Input Geometry!E122']=xcf_Input_Geometry_E122

#IF($C$109==1,IF(C29!="",C29,""),C$115+0.533333333333333*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C123(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C29","Input Geometry")!="",vcell("C29","Input Geometry"),""),vcell("C$115","Input Geometry")+0.533333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C123')
    return None
xfunctions['Input Geometry!C123']=xcf_Input_Geometry_C123

#IF($C$109==1,IF(E29!="",E29,""),E$130+(0.466666666666666*(E$115-E$130)))
def xcf_Input_Geometry_E123(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E29","Input Geometry")!="",vcell("E29","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.466666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E123')
    return None
xfunctions['Input Geometry!E123']=xcf_Input_Geometry_E123

#IF($C$109==1,IF(C30!="",C30,""),C$115+0.6*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C124(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C30","Input Geometry")!="",vcell("C30","Input Geometry"),""),vcell("C$115","Input Geometry")+0.6*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C124')
    return None
xfunctions['Input Geometry!C124']=xcf_Input_Geometry_C124

#IF($C$109==1,IF(E30!="",E30,""),E$130+(0.4*(E$115-E$130)))
def xcf_Input_Geometry_E124(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E30","Input Geometry")!="",vcell("E30","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.4*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E124')
    return None
xfunctions['Input Geometry!E124']=xcf_Input_Geometry_E124

#IF($C$109==1,IF(C31!="",C31,""),C$115+0.666666666666666*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C125(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C31","Input Geometry")!="",vcell("C31","Input Geometry"),""),vcell("C$115","Input Geometry")+0.666666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C125')
    return None
xfunctions['Input Geometry!C125']=xcf_Input_Geometry_C125

#IF($C$109==1,IF(E31!="",E31,""),E$130+(0.333333333333333*(E$115-E$130)))
def xcf_Input_Geometry_E125(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E31","Input Geometry")!="",vcell("E31","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.333333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E125')
    return None
xfunctions['Input Geometry!E125']=xcf_Input_Geometry_E125

#IF($C$109==1,IF(C32!="",C32,""),C$115+0.733333333333333*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C126(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C32","Input Geometry")!="",vcell("C32","Input Geometry"),""),vcell("C$115","Input Geometry")+0.733333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C126')
    return None
xfunctions['Input Geometry!C126']=xcf_Input_Geometry_C126

#IF($C$109==1,IF(E32!="",E32,""),E$130+(0.266666666666666*(E$115-E$130)))
def xcf_Input_Geometry_E126(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E32","Input Geometry")!="",vcell("E32","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.266666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E126')
    return None
xfunctions['Input Geometry!E126']=xcf_Input_Geometry_E126

#IF($C$109==1,IF(C33!="",C33,""),C$115+0.8*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C127(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C33","Input Geometry")!="",vcell("C33","Input Geometry"),""),vcell("C$115","Input Geometry")+0.8*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C127')
    return None
xfunctions['Input Geometry!C127']=xcf_Input_Geometry_C127

#IF($C$109==1,IF(E33!="",E33,""),E$130+(0.2*(E$115-E$130)))
def xcf_Input_Geometry_E127(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E33","Input Geometry")!="",vcell("E33","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.2*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E127')
    return None
xfunctions['Input Geometry!E127']=xcf_Input_Geometry_E127

#IF($C$109==1,IF(C34!="",C34,""),C$115+0.866666666666666*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C128(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C34","Input Geometry")!="",vcell("C34","Input Geometry"),""),vcell("C$115","Input Geometry")+0.866666666666666*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C128')
    return None
xfunctions['Input Geometry!C128']=xcf_Input_Geometry_C128

#IF($C$109==1,IF(E34!="",E34,""),E$130+(0.133333333333333*(E$115-E$130)))
def xcf_Input_Geometry_E128(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E34","Input Geometry")!="",vcell("E34","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.133333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E128')
    return None
xfunctions['Input Geometry!E128']=xcf_Input_Geometry_E128

#IF($C$109==1,IF(C35!="",C35,""),C$115+0.933333333333333*(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C129(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C35","Input Geometry")!="",vcell("C35","Input Geometry"),""),vcell("C$115","Input Geometry")+0.933333333333333*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C129')
    return None
xfunctions['Input Geometry!C129']=xcf_Input_Geometry_C129

#IF($C$109==1,IF(E35!="",E35,""),E$130+(0.0666666666666667*(E$115-E$130)))
def xcf_Input_Geometry_E129(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E35","Input Geometry")!="",vcell("E35","Input Geometry"),""),vcell("E$130","Input Geometry")+(0.0666666666666667*(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E129')
    return None
xfunctions['Input Geometry!E129']=xcf_Input_Geometry_E129

#IF($C$109==1,IF(C36!="",C36,""),C$115+(E$115-E$130)/TAN(RADIANS(G$20)))
def xcf_Input_Geometry_C130(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C36","Input Geometry")!="",vcell("C36","Input Geometry"),""),vcell("C$115","Input Geometry")+(vcell("E$115","Input Geometry")-vcell("E$130","Input Geometry"))/TAN(RADIANS(vcell("G$20","Input Geometry"))))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C130')
    return None
xfunctions['Input Geometry!C130']=xcf_Input_Geometry_C130

#IF($C$109==1,IF(E36!="",E36,""),SIN(RADIANS(G$24))*G$22)
def xcf_Input_Geometry_E130(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E36","Input Geometry")!="",vcell("E36","Input Geometry"),""),SIN(RADIANS(vcell("G$24","Input Geometry")))*vcell("G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E130')
    return None
xfunctions['Input Geometry!E130']=xcf_Input_Geometry_E130

#IF($C$109==1,IF(C37!="",C37,""),C$130+0.2*COS(RADIANS($G$24))*$G$22)
def xcf_Input_Geometry_C131(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C37","Input Geometry")!="",vcell("C37","Input Geometry"),""),vcell("C$130","Input Geometry")+0.2*COS(RADIANS(vcell("$G$24","Input Geometry")))*vcell("$G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C131')
    return None
xfunctions['Input Geometry!C131']=xcf_Input_Geometry_C131

#IF($C$109==1,IF(E37!="",E37,""),0.8*SIN(RADIANS(G$24))*G$22)
def xcf_Input_Geometry_E131(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E37","Input Geometry")!="",vcell("E37","Input Geometry"),""),0.8*SIN(RADIANS(vcell("G$24","Input Geometry")))*vcell("G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E131')
    return None
xfunctions['Input Geometry!E131']=xcf_Input_Geometry_E131

#IF($C$109==1,IF(C38!="",C38,""),C$130+0.4*COS(RADIANS($G$24))*$G$22)
def xcf_Input_Geometry_C132(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C38","Input Geometry")!="",vcell("C38","Input Geometry"),""),vcell("C$130","Input Geometry")+0.4*COS(RADIANS(vcell("$G$24","Input Geometry")))*vcell("$G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C132')
    return None
xfunctions['Input Geometry!C132']=xcf_Input_Geometry_C132

#IF($C$109==1,IF(E38!="",E38,""),0.6*SIN(RADIANS(G$24))*G$22)
def xcf_Input_Geometry_E132(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E38","Input Geometry")!="",vcell("E38","Input Geometry"),""),0.6*SIN(RADIANS(vcell("G$24","Input Geometry")))*vcell("G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E132')
    return None
xfunctions['Input Geometry!E132']=xcf_Input_Geometry_E132

#IF($C$109==1,IF(C39!="",C39,""),C$130+0.6*COS(RADIANS($G$24))*$G$22)
def xcf_Input_Geometry_C133(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C39","Input Geometry")!="",vcell("C39","Input Geometry"),""),vcell("C$130","Input Geometry")+0.6*COS(RADIANS(vcell("$G$24","Input Geometry")))*vcell("$G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C133')
    return None
xfunctions['Input Geometry!C133']=xcf_Input_Geometry_C133

#IF($C$109==1,IF(E39!="",E39,""),0.4*SIN(RADIANS(G$24))*G$22)
def xcf_Input_Geometry_E133(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E39","Input Geometry")!="",vcell("E39","Input Geometry"),""),0.4*SIN(RADIANS(vcell("G$24","Input Geometry")))*vcell("G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E133')
    return None
xfunctions['Input Geometry!E133']=xcf_Input_Geometry_E133

#IF($C$109==1,IF(C40!="",C40,""),C$130+0.8*COS(RADIANS($G$24))*$G$22)
def xcf_Input_Geometry_C134(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C40","Input Geometry")!="",vcell("C40","Input Geometry"),""),vcell("C$130","Input Geometry")+0.8*COS(RADIANS(vcell("$G$24","Input Geometry")))*vcell("$G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C134')
    return None
xfunctions['Input Geometry!C134']=xcf_Input_Geometry_C134

#IF($C$109==1,IF(E40!="",E40,""),0.2*SIN(RADIANS(G$24))*G$22)
def xcf_Input_Geometry_E134(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E40","Input Geometry")!="",vcell("E40","Input Geometry"),""),0.2*SIN(RADIANS(vcell("G$24","Input Geometry")))*vcell("G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E134')
    return None
xfunctions['Input Geometry!E134']=xcf_Input_Geometry_E134

#IF($C$109==1,IF(C41!="",C41,""),C$130+COS(RADIANS($G$24))*$G$22)
def xcf_Input_Geometry_C135(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C41","Input Geometry")!="",vcell("C41","Input Geometry"),""),vcell("C$130","Input Geometry")+COS(RADIANS(vcell("$G$24","Input Geometry")))*vcell("$G$22","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C135')
    return None
xfunctions['Input Geometry!C135']=xcf_Input_Geometry_C135

#IF($C$109==1,IF(E41!="",E41,""),0)
def xcf_Input_Geometry_E135(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E41","Input Geometry")!="",vcell("E41","Input Geometry"),""),0)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E135')
    return None
xfunctions['Input Geometry!E135']=xcf_Input_Geometry_E135

#IF($C$109==1,IF(C42!="",C42,""),C135+1)
def xcf_Input_Geometry_C136(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("C42","Input Geometry")!="",vcell("C42","Input Geometry"),""),vcell("C135","Input Geometry")+1)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C136')
    return None
xfunctions['Input Geometry!C136']=xcf_Input_Geometry_C136

#IF($C$109==1,IF(E42!="",E42,""),-0.000001)
def xcf_Input_Geometry_E136(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,IF(vcell("E42","Input Geometry")!="",vcell("E42","Input Geometry"),""),-0.000001)
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E136')
    return None
xfunctions['Input Geometry!E136']=xcf_Input_Geometry_E136

#IF($C$109==1,E44+0.000001,MAX(E130+0.000001,G26))
def xcf_Input_Geometry_E138(): 
    try:      
        return IF(vcell("$C$109","Input Geometry")==1,vcell("E44","Input Geometry")+0.000001,MAX(vcell("E130","Input Geometry")+0.000001,vcell("G26","Input Geometry")))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E138')
    return None
xfunctions['Input Geometry!E138']=xcf_Input_Geometry_E138

#SUM(H35*Calculations!D9,H37*Calculations!D10,H39*Calculations!D11,H41*Calculations!D12,H43*Calculations!D13)/SUM(H35,H37,H39,H41,H43)
def xcf_Input_Geometry_C140(): 
    try:      
        return SUM(vcell("H35","Input Geometry")*vcell("Calculations!D9","Input Geometry"),vcell("H37","Input Geometry")*vcell("Calculations!D10","Input Geometry"),vcell("H39","Input Geometry")*vcell("Calculations!D11","Input Geometry"),vcell("H41","Input Geometry")*vcell("Calculations!D12","Input Geometry"),vcell("H43","Input Geometry")*vcell("Calculations!D13","Input Geometry"))/SUM(vcell("H35","Input Geometry"),vcell("H37","Input Geometry"),vcell("H39","Input Geometry"),vcell("H41","Input Geometry"),vcell("H43","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_C140')
    return None
xfunctions['Input Geometry!C140']=xcf_Input_Geometry_C140

#IF(C109==1,E46,G28)
def xcf_Input_Geometry_E140(): 
    try:      
        return IF(vcell("C109","Input Geometry")==1,vcell("E46","Input Geometry"),vcell("G28","Input Geometry"))
    except Exception as ex:
        print(ex,'on xcf_Input_Geometry_E140')
    return None
xfunctions['Input Geometry!E140']=xcf_Input_Geometry_E140

#IF(F14>0.002,0.06*16200*F14,0.044*16200*F14)
def xcf_Bank_Material_AF14(): 
    try:      
        return IF(vcell("F14","Bank Material")>0.002,0.06*16200*vcell("F14","Bank Material"),0.044*16200*vcell("F14","Bank Material"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AF14')
    return None
xfunctions['Bank Material!AF14']=xcf_Bank_Material_AF14

#0.1*AF14^-0.5
def xcf_Bank_Material_AH14(): 
    try:      
        return 0.1*vcell("AF14","Bank Material")**-0.5
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AH14')
    return None
xfunctions['Bank Material!AH14']=xcf_Bank_Material_AH14

#IF(F15>0.002,0.06*16200*F15,0.044*16200*F15)
def xcf_Bank_Material_AF15(): 
    try:      
        return IF(vcell("F15","Bank Material")>0.002,0.06*16200*vcell("F15","Bank Material"),0.044*16200*vcell("F15","Bank Material"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AF15')
    return None
xfunctions['Bank Material!AF15']=xcf_Bank_Material_AF15

#0.1*AF15^-0.5
def xcf_Bank_Material_AH15(): 
    try:      
        return 0.1*vcell("AF15","Bank Material")**-0.5
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AH15')
    return None
xfunctions['Bank Material!AH15']=xcf_Bank_Material_AH15

#IF(F16>0.002,0.06*16200*F16,0.044*16200*F16)
def xcf_Bank_Material_AF16(): 
    try:      
        return IF(vcell("F16","Bank Material")>0.002,0.06*16200*vcell("F16","Bank Material"),0.044*16200*vcell("F16","Bank Material"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AF16')
    return None
xfunctions['Bank Material!AF16']=xcf_Bank_Material_AF16

#0.1*AF16^-0.5
def xcf_Bank_Material_AH16(): 
    try:      
        return 0.1*vcell("AF16","Bank Material")**-0.5
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_AH16')
    return None
xfunctions['Bank Material!AH16']=xcf_Bank_Material_AH16

#IF(H41>0,IF(H41>2,0.06*16.2*H41,0.044*16.2*H41),"")
def xcf_Bank_Material_H43(): 
    try:      
        return IF(vcell("H41","Bank Material")>0,IF(vcell("H41","Bank Material")>2,0.06*16.2*vcell("H41","Bank Material"),0.044*16.2*vcell("H41","Bank Material")),"")
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_H43')
    return None
xfunctions['Bank Material!H43']=xcf_Bank_Material_H43

#IF(P41>0,0.1*P41^-0.5,"")
def xcf_Bank_Material_P43(): 
    try:      
        return IF(vcell("P41","Bank Material")>0,0.1*vcell("P41","Bank Material")**-0.5,"")
    except Exception as ex:
        print(ex,'on xcf_Bank_Material_P43')
    return None
xfunctions['Bank Material!P43']=xcf_Bank_Material_P43

#E84
def xcf_Bank_Model_Output_B8(): 
    try:      
        return vcell("E84","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_B8')
    return None
xfunctions['Bank Model Output!B8']=xcf_Bank_Model_Output_B8

#E85
def xcf_Bank_Model_Output_C8(): 
    try:      
        return vcell("E85","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_C8')
    return None
xfunctions['Bank Model Output!C8']=xcf_Bank_Model_Output_C8

#E86
def xcf_Bank_Model_Output_D8(): 
    try:      
        return vcell("E86","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D8')
    return None
xfunctions['Bank Model Output!D8']=xcf_Bank_Model_Output_D8

#E87
def xcf_Bank_Model_Output_E8(): 
    try:      
        return vcell("E87","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E8')
    return None
xfunctions['Bank Model Output!E8']=xcf_Bank_Model_Output_E8

#E88
def xcf_Bank_Model_Output_F8(): 
    try:      
        return vcell("E88","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F8')
    return None
xfunctions['Bank Model Output!F8']=xcf_Bank_Model_Output_F8

#Input_Geometry!N92*-1
def xcf_Bank_Model_Output_I15(): 
    try:      
        return vcell("Input_Geometry!N92","Bank Model Output")*-1
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I15')
    return None
xfunctions['Bank Model Output!I15']=xcf_Bank_Model_Output_I15

#Input_Geometry!N93*-1
def xcf_Bank_Model_Output_I17(): 
    try:      
        return vcell("Input_Geometry!N93","Bank Model Output")*-1
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I17')
    return None
xfunctions['Bank Model Output!I17']=xcf_Bank_Model_Output_I17

#Input_Geometry!N94*-1
def xcf_Bank_Model_Output_I19(): 
    try:      
        return vcell("Input_Geometry!N94","Bank Model Output")*-1
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I19')
    return None
xfunctions['Bank Model Output!I19']=xcf_Bank_Model_Output_I19

#Input_Geometry!N95*-1
def xcf_Bank_Model_Output_I21(): 
    try:      
        return vcell("Input_Geometry!N95","Bank Model Output")*-1
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I21')
    return None
xfunctions['Bank Model Output!I21']=xcf_Bank_Model_Output_I21

#Input_Geometry!N96*-1
def xcf_Bank_Model_Output_I23(): 
    try:      
        return vcell("Input_Geometry!N96","Bank Model Output")*-1
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I23')
    return None
xfunctions['Bank Model Output!I23']=xcf_Bank_Model_Output_I23

#IF(VertSliceCalcs!$Q$27=="",IF(Calculations!L77<0,99999999,Calculations!L77),VertSliceCalcs!$Q$67)
def xcf_Bank_Model_Output_H27(): 
    try:      
        return IF(vcell("VertSliceCalcs!$Q$27","Bank Model Output")=="",IF(vcell("Calculations!L77","Bank Model Output")<0,99999999,vcell("Calculations!L77","Bank Model Output")),vcell("VertSliceCalcs!$Q$67","Bank Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H27')
    return None
xfunctions['Bank Model Output!H27']=xcf_Bank_Model_Output_H27

#IF(H27<1,"Unstable",IF(H27<1.3,"Conditionally stable","Stable"))
def xcf_Bank_Model_Output_I27(): 
    try:      
        return IF(vcell("H27","Bank Model Output")<1,"Unstable",IF(vcell("H27","Bank Model Output")<1.3,"Conditionally stable","Stable"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I27')
    return None
xfunctions['Bank Model Output!I27']=xcf_Bank_Model_Output_I27

#Input_Geometry!E138
def xcf_Bank_Model_Output_B41(): 
    try:      
        return vcell("Input_Geometry!E138","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_B41')
    return None
xfunctions['Bank Model Output!B41']=xcf_Bank_Model_Output_B41

#IF(H27>1.3,"-",Input_Geometry!C115-VertSliceCalcs!P26)
def xcf_Bank_Model_Output_I41(): 
    try:      
        return IF(vcell("H27","Bank Model Output")>1.3,"-",vcell("Input_Geometry!C115","Bank Model Output")-vcell("VertSliceCalcs!P26","Bank Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I41')
    return None
xfunctions['Bank Model Output!I41']=xcf_Bank_Model_Output_I41

#IF(H27>1.3,"-",IF(VertSliceCalcs!$Q$27=="",Calculations!D41*Input_Geometry!C51,VertSliceCalcs!J20*Input_Geometry!C51))
def xcf_Bank_Model_Output_I42(): 
    try:      
        return IF(vcell("H27","Bank Model Output")>1.3,"-",IF(vcell("VertSliceCalcs!$Q$27","Bank Model Output")=="",vcell("Calculations!D41","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output"),vcell("VertSliceCalcs!J20","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I42')
    return None
xfunctions['Bank Model Output!I42']=xcf_Bank_Model_Output_I42

#Input_Geometry!E140
def xcf_Bank_Model_Output_B43(): 
    try:      
        return vcell("Input_Geometry!E140","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_B43')
    return None
xfunctions['Bank Model Output!B43']=xcf_Bank_Model_Output_B43

#IF(H27>1.3,"-",IF(VertSliceCalcs!$Q$27=="",Calculations!E41*Input_Geometry!C51,VertSliceCalcs!J21*Input_Geometry!C51))
def xcf_Bank_Model_Output_I43(): 
    try:      
        return IF(vcell("H27","Bank Model Output")>1.3,"-",IF(vcell("VertSliceCalcs!$Q$27","Bank Model Output")=="",vcell("Calculations!E41","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output"),vcell("VertSliceCalcs!J21","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I43')
    return None
xfunctions['Bank Model Output!I43']=xcf_Bank_Model_Output_I43

#IF(H27>1.3,"-",IF(VertSliceCalcs!$Q$27=="",Calculations!F41*Input_Geometry!C51,VertSliceCalcs!J22*Input_Geometry!C51))
def xcf_Bank_Model_Output_I44(): 
    try:      
        return IF(vcell("H27","Bank Model Output")>1.3,"-",IF(vcell("VertSliceCalcs!$Q$27","Bank Model Output")=="",vcell("Calculations!F41","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output"),vcell("VertSliceCalcs!J22","Bank Model Output")*vcell("Input_Geometry!C51","Bank Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I44')
    return None
xfunctions['Bank Model Output!I44']=xcf_Bank_Model_Output_I44

#IF(OR(C84==4,C84==5),4,IF(OR(C84==6,C84==7),5,IF(OR(C84==8,C84==9,C84==10),6,IF(OR(C84==11,C84==12,C84==13),7,IF(OR(C84==14,C84==15,C84==16),8,IF(C84==17,9,C84))))))
def xcf_Bank_Model_Output_D84(): 
    try:      
        return IF(OR(vcell("C84","Bank Model Output")==4,vcell("C84","Bank Model Output")==5),4,IF(OR(vcell("C84","Bank Model Output")==6,vcell("C84","Bank Model Output")==7),5,IF(OR(vcell("C84","Bank Model Output")==8,vcell("C84","Bank Model Output")==9,vcell("C84","Bank Model Output")==10),6,IF(OR(vcell("C84","Bank Model Output")==11,vcell("C84","Bank Model Output")==12,vcell("C84","Bank Model Output")==13),7,IF(OR(vcell("C84","Bank Model Output")==14,vcell("C84","Bank Model Output")==15,vcell("C84","Bank Model Output")==16),8,IF(vcell("C84","Bank Model Output")==17,9,vcell("C84","Bank Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D84')
    return None
xfunctions['Bank Model Output!D84']=xcf_Bank_Model_Output_D84

#IF(D84==1,"Boulders",IF(D84==2,"Cobbles",IF(D84==3,"Gravel",IF(D84==4,"Angular Sand",IF(D84==5,"Rounded Sand",IF(D84==6,"Silt",IF(D84==7,"Soft Clay",IF(D84==8,"Stiff Clay","Own Data"))))))))
def xcf_Bank_Model_Output_E84(): 
    try:      
        return IF(vcell("D84","Bank Model Output")==1,"Boulders",IF(vcell("D84","Bank Model Output")==2,"Cobbles",IF(vcell("D84","Bank Model Output")==3,"Gravel",IF(vcell("D84","Bank Model Output")==4,"Angular Sand",IF(vcell("D84","Bank Model Output")==5,"Rounded Sand",IF(vcell("D84","Bank Model Output")==6,"Silt",IF(vcell("D84","Bank Model Output")==7,"Soft Clay",IF(vcell("D84","Bank Model Output")==8,"Stiff Clay","Own Data"))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E84')
    return None
xfunctions['Bank Model Output!E84']=xcf_Bank_Model_Output_E84

#IF($D84==1,Bank_Material!L$14,IF($D84==2,Bank_Material!L$15,IF($D84==3,Bank_Material!L$16,IF($D84==4,Bank_Material!L$18,IF($D84==5,Bank_Material!L$19,IF($D84==6,Bank_Material!L$21,IF($D84==7,Bank_Material!L$22,IF($D84==8,Bank_Material!L$23,Bank_Material!L25))))))))
def xcf_Bank_Model_Output_F84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!L$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!L$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!L$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!L$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!L$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!L$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!L$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!L$23","Bank Model Output"),vcell("Bank_Material!L25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F84')
    return None
xfunctions['Bank Model Output!F84']=xcf_Bank_Model_Output_F84

#IF($D84==1,Bank_Material!H$14,IF($D84==2,Bank_Material!H$15,IF($D84==3,Bank_Material!H$16,IF($D84==4,Bank_Material!H$18,IF($D84==5,Bank_Material!H$19,IF($D84==6,Bank_Material!H$21,IF($D84==7,Bank_Material!H$22,IF($D84==8,Bank_Material!H$23,Bank_Material!H25))))))))
def xcf_Bank_Model_Output_G84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!H$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!H$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!H$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!H$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!H$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!H$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!H$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!H$23","Bank Model Output"),vcell("Bank_Material!H25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_G84')
    return None
xfunctions['Bank Model Output!G84']=xcf_Bank_Model_Output_G84

#IF($D84==1,Bank_Material!J$14,IF($D84==2,Bank_Material!J$15,IF($D84==3,Bank_Material!J$16,IF($D84==4,Bank_Material!J$18,IF($D84==5,Bank_Material!J$19,IF($D84==6,Bank_Material!J$21,IF($D84==7,Bank_Material!J$22,IF($D84==8,Bank_Material!J$23,Bank_Material!J25))))))))
def xcf_Bank_Model_Output_H84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!J$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!J$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!J$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!J$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!J$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!J$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!J$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!J$23","Bank Model Output"),vcell("Bank_Material!J25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H84')
    return None
xfunctions['Bank Model Output!H84']=xcf_Bank_Model_Output_H84

#IF($D84==1,Bank_Material!N$14,IF($D84==2,Bank_Material!N$15,IF($D84==3,Bank_Material!N$16,IF($D84==4,Bank_Material!N$18,IF($D84==5,Bank_Material!N$19,IF($D84==6,Bank_Material!N$21,IF($D84==7,Bank_Material!N$22,IF($D84==8,Bank_Material!N$23,Bank_Material!N25))))))))
def xcf_Bank_Model_Output_I84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!N$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!N$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!N$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!N$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!N$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!N$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!N$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!N$23","Bank Model Output"),vcell("Bank_Material!N25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I84')
    return None
xfunctions['Bank Model Output!I84']=xcf_Bank_Model_Output_I84

#IF($D84==9,Bank_Material!P$25,0)
def xcf_Bank_Model_Output_J84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==9,vcell("Bank_Material!P$25","Bank Model Output"),0)
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_J84')
    return None
xfunctions['Bank Model Output!J84']=xcf_Bank_Model_Output_J84

#IF($D84==1,Bank_Material!S$14,IF($D84==2,Bank_Material!S$15,IF($D84==3,Bank_Material!S$16,IF($D84==4,Bank_Material!S$18,IF($D84==5,Bank_Material!S$19,IF($D84==6,Bank_Material!S$21,IF($D84==7,Bank_Material!S$22,IF($D84==8,Bank_Material!S$23,Bank_Material!S25))))))))
def xcf_Bank_Model_Output_L84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!S$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!S$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!S$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!S$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!S$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!S$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!S$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!S$23","Bank Model Output"),vcell("Bank_Material!S25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_L84')
    return None
xfunctions['Bank Model Output!L84']=xcf_Bank_Model_Output_L84

#IF($D84==1,Bank_Material!U$14,IF($D84==2,Bank_Material!U$15,IF($D84==3,Bank_Material!U$16,IF($D84==4,Bank_Material!U$18,IF($D84==5,Bank_Material!U$19,IF($D84==6,Bank_Material!U$21,IF($D84==7,Bank_Material!U$22,IF($D84==8,Bank_Material!U$23,Bank_Material!U25))))))))
def xcf_Bank_Model_Output_M84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!U$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!U$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!U$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!U$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!U$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!U$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!U$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!U$23","Bank Model Output"),vcell("Bank_Material!U25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_M84')
    return None
xfunctions['Bank Model Output!M84']=xcf_Bank_Model_Output_M84

#IF($D84==1,Bank_Material!W$14,IF($D84==2,Bank_Material!W$15,IF($D84==3,Bank_Material!W$16,IF($D84==4,Bank_Material!W$18,IF($D84==5,Bank_Material!W$19,IF($D84==6,Bank_Material!W$21,IF($D84==7,Bank_Material!W$22,IF($D84==8,Bank_Material!W$23,Bank_Material!W25))))))))
def xcf_Bank_Model_Output_N84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!W$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!W$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!W$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!W$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!W$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!W$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!W$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!W$23","Bank Model Output"),vcell("Bank_Material!W25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_N84')
    return None
xfunctions['Bank Model Output!N84']=xcf_Bank_Model_Output_N84

#IF($D84==1,Bank_Material!Y$14,IF($D84==2,Bank_Material!Y$15,IF($D84==3,Bank_Material!Y$16,IF($D84==4,Bank_Material!Y$18,IF($D84==5,Bank_Material!Y$19,IF($D84==6,Bank_Material!Y$21,IF($D84==7,Bank_Material!Y$22,IF($D84==8,Bank_Material!Y$23,Bank_Material!Y25))))))))
def xcf_Bank_Model_Output_O84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!Y$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!Y$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!Y$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!Y$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!Y$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!Y$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!Y$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!Y$23","Bank Model Output"),vcell("Bank_Material!Y25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_O84')
    return None
xfunctions['Bank Model Output!O84']=xcf_Bank_Model_Output_O84

#IF($D84==1,Bank_Material!AA$14,IF($D84==2,Bank_Material!AA$15,IF($D84==3,Bank_Material!AA$16,IF($D84==4,Bank_Material!AA$18,IF($D84==5,Bank_Material!AA$19,IF($D84==6,Bank_Material!AA$21,IF($D84==7,Bank_Material!AA$22,IF($D84==8,Bank_Material!AA$23,Bank_Material!AA25))))))))
def xcf_Bank_Model_Output_P84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!AA$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!AA$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!AA$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!AA$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!AA$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!AA$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!AA$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!AA$23","Bank Model Output"),vcell("Bank_Material!AA25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_P84')
    return None
xfunctions['Bank Model Output!P84']=xcf_Bank_Model_Output_P84

#IF($D84==1,Bank_Material!AC$14,IF($D84==2,Bank_Material!AC$15,IF($D84==3,Bank_Material!AC$16,IF($D84==4,Bank_Material!AC$18,IF($D84==5,Bank_Material!AC$19,IF($D84==6,Bank_Material!AC$21,IF($D84==7,Bank_Material!AC$22,IF($D84==8,Bank_Material!AC$23,Bank_Material!AC25))))))))
def xcf_Bank_Model_Output_Q84(): 
    try:      
        return IF(vcell("$D84","Bank Model Output")==1,vcell("Bank_Material!AC$14","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==2,vcell("Bank_Material!AC$15","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==3,vcell("Bank_Material!AC$16","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==4,vcell("Bank_Material!AC$18","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==5,vcell("Bank_Material!AC$19","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==6,vcell("Bank_Material!AC$21","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==7,vcell("Bank_Material!AC$22","Bank Model Output"),IF(vcell("$D84","Bank Model Output")==8,vcell("Bank_Material!AC$23","Bank Model Output"),vcell("Bank_Material!AC25","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_Q84')
    return None
xfunctions['Bank Model Output!Q84']=xcf_Bank_Model_Output_Q84

#IF(OR(C85==4,C85==5),4,IF(OR(C85==6,C85==7),5,IF(OR(C85==8,C85==9,C85==10),6,IF(OR(C85==11,C85==12,C85==13),7,IF(OR(C85==14,C85==15,C85==16),8,IF(C85==17,9,C85))))))
def xcf_Bank_Model_Output_D85(): 
    try:      
        return IF(OR(vcell("C85","Bank Model Output")==4,vcell("C85","Bank Model Output")==5),4,IF(OR(vcell("C85","Bank Model Output")==6,vcell("C85","Bank Model Output")==7),5,IF(OR(vcell("C85","Bank Model Output")==8,vcell("C85","Bank Model Output")==9,vcell("C85","Bank Model Output")==10),6,IF(OR(vcell("C85","Bank Model Output")==11,vcell("C85","Bank Model Output")==12,vcell("C85","Bank Model Output")==13),7,IF(OR(vcell("C85","Bank Model Output")==14,vcell("C85","Bank Model Output")==15,vcell("C85","Bank Model Output")==16),8,IF(vcell("C85","Bank Model Output")==17,9,vcell("C85","Bank Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D85')
    return None
xfunctions['Bank Model Output!D85']=xcf_Bank_Model_Output_D85

#IF(D85==1,"Boulders",IF(D85==2,"Cobbles",IF(D85==3,"Gravel",IF(D85==4,"Angular Sand",IF(D85==5,"Rounded Sand",IF(D85==6,"Silt",IF(D85==7,"Soft Clay",IF(D85==8,"Stiff Clay","Own Data"))))))))
def xcf_Bank_Model_Output_E85(): 
    try:      
        return IF(vcell("D85","Bank Model Output")==1,"Boulders",IF(vcell("D85","Bank Model Output")==2,"Cobbles",IF(vcell("D85","Bank Model Output")==3,"Gravel",IF(vcell("D85","Bank Model Output")==4,"Angular Sand",IF(vcell("D85","Bank Model Output")==5,"Rounded Sand",IF(vcell("D85","Bank Model Output")==6,"Silt",IF(vcell("D85","Bank Model Output")==7,"Soft Clay",IF(vcell("D85","Bank Model Output")==8,"Stiff Clay","Own Data"))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E85')
    return None
xfunctions['Bank Model Output!E85']=xcf_Bank_Model_Output_E85

#IF($D85==1,Bank_Material!L$14,IF($D85==2,Bank_Material!L$15,IF($D85==3,Bank_Material!L$16,IF($D85==4,Bank_Material!L$18,IF($D85==5,Bank_Material!L$19,IF($D85==6,Bank_Material!L$21,IF($D85==7,Bank_Material!L$22,IF($D85==8,Bank_Material!L$23,Bank_Material!L27))))))))
def xcf_Bank_Model_Output_F85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!L$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!L$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!L$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!L$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!L$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!L$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!L$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!L$23","Bank Model Output"),vcell("Bank_Material!L27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F85')
    return None
xfunctions['Bank Model Output!F85']=xcf_Bank_Model_Output_F85

#IF($D85==1,Bank_Material!H$14,IF($D85==2,Bank_Material!H$15,IF($D85==3,Bank_Material!H$16,IF($D85==4,Bank_Material!H$18,IF($D85==5,Bank_Material!H$19,IF($D85==6,Bank_Material!H$21,IF($D85==7,Bank_Material!H$22,IF($D85==8,Bank_Material!H$23,Bank_Material!H27))))))))
def xcf_Bank_Model_Output_G85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!H$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!H$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!H$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!H$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!H$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!H$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!H$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!H$23","Bank Model Output"),vcell("Bank_Material!H27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_G85')
    return None
xfunctions['Bank Model Output!G85']=xcf_Bank_Model_Output_G85

#IF($D85==1,Bank_Material!J$14,IF($D85==2,Bank_Material!J$15,IF($D85==3,Bank_Material!J$16,IF($D85==4,Bank_Material!J$18,IF($D85==5,Bank_Material!J$19,IF($D85==6,Bank_Material!J$21,IF($D85==7,Bank_Material!J$22,IF($D85==8,Bank_Material!J$23,Bank_Material!J27))))))))
def xcf_Bank_Model_Output_H85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!J$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!J$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!J$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!J$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!J$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!J$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!J$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!J$23","Bank Model Output"),vcell("Bank_Material!J27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H85')
    return None
xfunctions['Bank Model Output!H85']=xcf_Bank_Model_Output_H85

#IF($D85==1,Bank_Material!N$14,IF($D85==2,Bank_Material!N$15,IF($D85==3,Bank_Material!N$16,IF($D85==4,Bank_Material!N$18,IF($D85==5,Bank_Material!N$19,IF($D85==6,Bank_Material!N$21,IF($D85==7,Bank_Material!N$22,IF($D85==8,Bank_Material!N$23,Bank_Material!N27))))))))
def xcf_Bank_Model_Output_I85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!N$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!N$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!N$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!N$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!N$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!N$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!N$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!N$23","Bank Model Output"),vcell("Bank_Material!N27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I85')
    return None
xfunctions['Bank Model Output!I85']=xcf_Bank_Model_Output_I85

#IF($D85==9,Bank_Material!P$27,0)
def xcf_Bank_Model_Output_J85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==9,vcell("Bank_Material!P$27","Bank Model Output"),0)
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_J85')
    return None
xfunctions['Bank Model Output!J85']=xcf_Bank_Model_Output_J85

#IF($D85==1,Bank_Material!S$14,IF($D85==2,Bank_Material!S$15,IF($D85==3,Bank_Material!S$16,IF($D85==4,Bank_Material!S$18,IF($D85==5,Bank_Material!S$19,IF($D85==6,Bank_Material!S$21,IF($D85==7,Bank_Material!S$22,IF($D85==8,Bank_Material!S$23,Bank_Material!S27))))))))
def xcf_Bank_Model_Output_L85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!S$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!S$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!S$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!S$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!S$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!S$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!S$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!S$23","Bank Model Output"),vcell("Bank_Material!S27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_L85')
    return None
xfunctions['Bank Model Output!L85']=xcf_Bank_Model_Output_L85

#IF($D85==1,Bank_Material!U$14,IF($D85==2,Bank_Material!U$15,IF($D85==3,Bank_Material!U$16,IF($D85==4,Bank_Material!U$18,IF($D85==5,Bank_Material!U$19,IF($D85==6,Bank_Material!U$21,IF($D85==7,Bank_Material!U$22,IF($D85==8,Bank_Material!U$23,Bank_Material!U27))))))))
def xcf_Bank_Model_Output_M85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!U$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!U$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!U$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!U$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!U$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!U$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!U$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!U$23","Bank Model Output"),vcell("Bank_Material!U27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_M85')
    return None
xfunctions['Bank Model Output!M85']=xcf_Bank_Model_Output_M85

#IF($D85==1,Bank_Material!W$14,IF($D85==2,Bank_Material!W$15,IF($D85==3,Bank_Material!W$16,IF($D85==4,Bank_Material!W$18,IF($D85==5,Bank_Material!W$19,IF($D85==6,Bank_Material!W$21,IF($D85==7,Bank_Material!W$22,IF($D85==8,Bank_Material!W$23,Bank_Material!W27))))))))
def xcf_Bank_Model_Output_N85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!W$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!W$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!W$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!W$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!W$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!W$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!W$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!W$23","Bank Model Output"),vcell("Bank_Material!W27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_N85')
    return None
xfunctions['Bank Model Output!N85']=xcf_Bank_Model_Output_N85

#IF($D85==1,Bank_Material!Y$14,IF($D85==2,Bank_Material!Y$15,IF($D85==3,Bank_Material!Y$16,IF($D85==4,Bank_Material!Y$18,IF($D85==5,Bank_Material!Y$19,IF($D85==6,Bank_Material!Y$21,IF($D85==7,Bank_Material!Y$22,IF($D85==8,Bank_Material!Y$23,Bank_Material!Y27))))))))
def xcf_Bank_Model_Output_O85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!Y$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!Y$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!Y$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!Y$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!Y$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!Y$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!Y$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!Y$23","Bank Model Output"),vcell("Bank_Material!Y27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_O85')
    return None
xfunctions['Bank Model Output!O85']=xcf_Bank_Model_Output_O85

#IF($D85==1,Bank_Material!AA$14,IF($D85==2,Bank_Material!AA$15,IF($D85==3,Bank_Material!AA$16,IF($D85==4,Bank_Material!AA$18,IF($D85==5,Bank_Material!AA$19,IF($D85==6,Bank_Material!AA$21,IF($D85==7,Bank_Material!AA$22,IF($D85==8,Bank_Material!AA$23,Bank_Material!AA27))))))))
def xcf_Bank_Model_Output_P85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!AA$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!AA$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!AA$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!AA$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!AA$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!AA$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!AA$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!AA$23","Bank Model Output"),vcell("Bank_Material!AA27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_P85')
    return None
xfunctions['Bank Model Output!P85']=xcf_Bank_Model_Output_P85

#IF($D85==1,Bank_Material!AC$14,IF($D85==2,Bank_Material!AC$15,IF($D85==3,Bank_Material!AC$16,IF($D85==4,Bank_Material!AC$18,IF($D85==5,Bank_Material!AC$19,IF($D85==6,Bank_Material!AC$21,IF($D85==7,Bank_Material!AC$22,IF($D85==8,Bank_Material!AC$23,Bank_Material!AC27))))))))
def xcf_Bank_Model_Output_Q85(): 
    try:      
        return IF(vcell("$D85","Bank Model Output")==1,vcell("Bank_Material!AC$14","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==2,vcell("Bank_Material!AC$15","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==3,vcell("Bank_Material!AC$16","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==4,vcell("Bank_Material!AC$18","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==5,vcell("Bank_Material!AC$19","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==6,vcell("Bank_Material!AC$21","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==7,vcell("Bank_Material!AC$22","Bank Model Output"),IF(vcell("$D85","Bank Model Output")==8,vcell("Bank_Material!AC$23","Bank Model Output"),vcell("Bank_Material!AC27","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_Q85')
    return None
xfunctions['Bank Model Output!Q85']=xcf_Bank_Model_Output_Q85

#IF(OR(C86==4,C86==5),4,IF(OR(C86==6,C86==7),5,IF(OR(C86==8,C86==9,C86==10),6,IF(OR(C86==11,C86==12,C86==13),7,IF(OR(C86==14,C86==15,C86==16),8,IF(C86==17,9,C86))))))
def xcf_Bank_Model_Output_D86(): 
    try:      
        return IF(OR(vcell("C86","Bank Model Output")==4,vcell("C86","Bank Model Output")==5),4,IF(OR(vcell("C86","Bank Model Output")==6,vcell("C86","Bank Model Output")==7),5,IF(OR(vcell("C86","Bank Model Output")==8,vcell("C86","Bank Model Output")==9,vcell("C86","Bank Model Output")==10),6,IF(OR(vcell("C86","Bank Model Output")==11,vcell("C86","Bank Model Output")==12,vcell("C86","Bank Model Output")==13),7,IF(OR(vcell("C86","Bank Model Output")==14,vcell("C86","Bank Model Output")==15,vcell("C86","Bank Model Output")==16),8,IF(vcell("C86","Bank Model Output")==17,9,vcell("C86","Bank Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D86')
    return None
xfunctions['Bank Model Output!D86']=xcf_Bank_Model_Output_D86

#IF(D86==1,"Boulders",IF(D86==2,"Cobbles",IF(D86==3,"Gravel",IF(D86==4,"Angular Sand",IF(D86==5,"Rounded Sand",IF(D86==6,"Silt",IF(D86==7,"Soft Clay",IF(D86==8,"Stiff Clay","Own Data"))))))))
def xcf_Bank_Model_Output_E86(): 
    try:      
        return IF(vcell("D86","Bank Model Output")==1,"Boulders",IF(vcell("D86","Bank Model Output")==2,"Cobbles",IF(vcell("D86","Bank Model Output")==3,"Gravel",IF(vcell("D86","Bank Model Output")==4,"Angular Sand",IF(vcell("D86","Bank Model Output")==5,"Rounded Sand",IF(vcell("D86","Bank Model Output")==6,"Silt",IF(vcell("D86","Bank Model Output")==7,"Soft Clay",IF(vcell("D86","Bank Model Output")==8,"Stiff Clay","Own Data"))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E86')
    return None
xfunctions['Bank Model Output!E86']=xcf_Bank_Model_Output_E86

#IF($D86==1,Bank_Material!L$14,IF($D86==2,Bank_Material!L$15,IF($D86==3,Bank_Material!L$16,IF($D86==4,Bank_Material!L$18,IF($D86==5,Bank_Material!L$19,IF($D86==6,Bank_Material!L$21,IF($D86==7,Bank_Material!L$22,IF($D86==8,Bank_Material!L$23,Bank_Material!L29))))))))
def xcf_Bank_Model_Output_F86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!L$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!L$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!L$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!L$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!L$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!L$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!L$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!L$23","Bank Model Output"),vcell("Bank_Material!L29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F86')
    return None
xfunctions['Bank Model Output!F86']=xcf_Bank_Model_Output_F86

#IF($D86==1,Bank_Material!H$14,IF($D86==2,Bank_Material!H$15,IF($D86==3,Bank_Material!H$16,IF($D86==4,Bank_Material!H$18,IF($D86==5,Bank_Material!H$19,IF($D86==6,Bank_Material!H$21,IF($D86==7,Bank_Material!H$22,IF($D86==8,Bank_Material!H$23,Bank_Material!H29))))))))
def xcf_Bank_Model_Output_G86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!H$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!H$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!H$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!H$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!H$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!H$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!H$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!H$23","Bank Model Output"),vcell("Bank_Material!H29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_G86')
    return None
xfunctions['Bank Model Output!G86']=xcf_Bank_Model_Output_G86

#IF($D86==1,Bank_Material!J$14,IF($D86==2,Bank_Material!J$15,IF($D86==3,Bank_Material!J$16,IF($D86==4,Bank_Material!J$18,IF($D86==5,Bank_Material!J$19,IF($D86==6,Bank_Material!J$21,IF($D86==7,Bank_Material!J$22,IF($D86==8,Bank_Material!J$23,Bank_Material!J29))))))))
def xcf_Bank_Model_Output_H86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!J$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!J$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!J$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!J$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!J$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!J$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!J$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!J$23","Bank Model Output"),vcell("Bank_Material!J29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H86')
    return None
xfunctions['Bank Model Output!H86']=xcf_Bank_Model_Output_H86

#IF($D86==1,Bank_Material!N$14,IF($D86==2,Bank_Material!N$15,IF($D86==3,Bank_Material!N$16,IF($D86==4,Bank_Material!N$18,IF($D86==5,Bank_Material!N$19,IF($D86==6,Bank_Material!N$21,IF($D86==7,Bank_Material!N$22,IF($D86==8,Bank_Material!N$23,Bank_Material!N29))))))))
def xcf_Bank_Model_Output_I86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!N$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!N$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!N$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!N$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!N$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!N$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!N$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!N$23","Bank Model Output"),vcell("Bank_Material!N29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I86')
    return None
xfunctions['Bank Model Output!I86']=xcf_Bank_Model_Output_I86

#IF($D86==9,Bank_Material!P$29,0)
def xcf_Bank_Model_Output_J86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==9,vcell("Bank_Material!P$29","Bank Model Output"),0)
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_J86')
    return None
xfunctions['Bank Model Output!J86']=xcf_Bank_Model_Output_J86

#IF($D86==1,Bank_Material!S$14,IF($D86==2,Bank_Material!S$15,IF($D86==3,Bank_Material!S$16,IF($D86==4,Bank_Material!S$18,IF($D86==5,Bank_Material!S$19,IF($D86==6,Bank_Material!S$21,IF($D86==7,Bank_Material!S$22,IF($D86==8,Bank_Material!S$23,Bank_Material!S29))))))))
def xcf_Bank_Model_Output_L86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!S$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!S$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!S$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!S$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!S$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!S$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!S$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!S$23","Bank Model Output"),vcell("Bank_Material!S29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_L86')
    return None
xfunctions['Bank Model Output!L86']=xcf_Bank_Model_Output_L86

#IF($D86==1,Bank_Material!U$14,IF($D86==2,Bank_Material!U$15,IF($D86==3,Bank_Material!U$16,IF($D86==4,Bank_Material!U$18,IF($D86==5,Bank_Material!U$19,IF($D86==6,Bank_Material!U$21,IF($D86==7,Bank_Material!U$22,IF($D86==8,Bank_Material!U$23,Bank_Material!U29))))))))
def xcf_Bank_Model_Output_M86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!U$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!U$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!U$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!U$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!U$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!U$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!U$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!U$23","Bank Model Output"),vcell("Bank_Material!U29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_M86')
    return None
xfunctions['Bank Model Output!M86']=xcf_Bank_Model_Output_M86

#IF($D86==1,Bank_Material!W$14,IF($D86==2,Bank_Material!W$15,IF($D86==3,Bank_Material!W$16,IF($D86==4,Bank_Material!W$18,IF($D86==5,Bank_Material!W$19,IF($D86==6,Bank_Material!W$21,IF($D86==7,Bank_Material!W$22,IF($D86==8,Bank_Material!W$23,Bank_Material!W29))))))))
def xcf_Bank_Model_Output_N86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!W$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!W$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!W$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!W$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!W$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!W$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!W$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!W$23","Bank Model Output"),vcell("Bank_Material!W29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_N86')
    return None
xfunctions['Bank Model Output!N86']=xcf_Bank_Model_Output_N86

#IF($D86==1,Bank_Material!Y$14,IF($D86==2,Bank_Material!Y$15,IF($D86==3,Bank_Material!Y$16,IF($D86==4,Bank_Material!Y$18,IF($D86==5,Bank_Material!Y$19,IF($D86==6,Bank_Material!Y$21,IF($D86==7,Bank_Material!Y$22,IF($D86==8,Bank_Material!Y$23,Bank_Material!Y29))))))))
def xcf_Bank_Model_Output_O86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!Y$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!Y$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!Y$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!Y$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!Y$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!Y$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!Y$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!Y$23","Bank Model Output"),vcell("Bank_Material!Y29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_O86')
    return None
xfunctions['Bank Model Output!O86']=xcf_Bank_Model_Output_O86

#IF($D86==1,Bank_Material!AA$14,IF($D86==2,Bank_Material!AA$15,IF($D86==3,Bank_Material!AA$16,IF($D86==4,Bank_Material!AA$18,IF($D86==5,Bank_Material!AA$19,IF($D86==6,Bank_Material!AA$21,IF($D86==7,Bank_Material!AA$22,IF($D86==8,Bank_Material!AA$23,Bank_Material!AA29))))))))
def xcf_Bank_Model_Output_P86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!AA$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!AA$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!AA$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!AA$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!AA$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!AA$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!AA$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!AA$23","Bank Model Output"),vcell("Bank_Material!AA29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_P86')
    return None
xfunctions['Bank Model Output!P86']=xcf_Bank_Model_Output_P86

#IF($D86==1,Bank_Material!AC$14,IF($D86==2,Bank_Material!AC$15,IF($D86==3,Bank_Material!AC$16,IF($D86==4,Bank_Material!AC$18,IF($D86==5,Bank_Material!AC$19,IF($D86==6,Bank_Material!AC$21,IF($D86==7,Bank_Material!AC$22,IF($D86==8,Bank_Material!AC$23,Bank_Material!AC29))))))))
def xcf_Bank_Model_Output_Q86(): 
    try:      
        return IF(vcell("$D86","Bank Model Output")==1,vcell("Bank_Material!AC$14","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==2,vcell("Bank_Material!AC$15","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==3,vcell("Bank_Material!AC$16","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==4,vcell("Bank_Material!AC$18","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==5,vcell("Bank_Material!AC$19","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==6,vcell("Bank_Material!AC$21","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==7,vcell("Bank_Material!AC$22","Bank Model Output"),IF(vcell("$D86","Bank Model Output")==8,vcell("Bank_Material!AC$23","Bank Model Output"),vcell("Bank_Material!AC29","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_Q86')
    return None
xfunctions['Bank Model Output!Q86']=xcf_Bank_Model_Output_Q86

#IF(OR(C87==4,C87==5),4,IF(OR(C87==6,C87==7),5,IF(OR(C87==8,C87==9,C87==10),6,IF(OR(C87==11,C87==12,C87==13),7,IF(OR(C87==14,C87==15,C87==16),8,IF(C87==17,9,C87))))))
def xcf_Bank_Model_Output_D87(): 
    try:      
        return IF(OR(vcell("C87","Bank Model Output")==4,vcell("C87","Bank Model Output")==5),4,IF(OR(vcell("C87","Bank Model Output")==6,vcell("C87","Bank Model Output")==7),5,IF(OR(vcell("C87","Bank Model Output")==8,vcell("C87","Bank Model Output")==9,vcell("C87","Bank Model Output")==10),6,IF(OR(vcell("C87","Bank Model Output")==11,vcell("C87","Bank Model Output")==12,vcell("C87","Bank Model Output")==13),7,IF(OR(vcell("C87","Bank Model Output")==14,vcell("C87","Bank Model Output")==15,vcell("C87","Bank Model Output")==16),8,IF(vcell("C87","Bank Model Output")==17,9,vcell("C87","Bank Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D87')
    return None
xfunctions['Bank Model Output!D87']=xcf_Bank_Model_Output_D87

#IF(D87==1,"Boulders",IF(D87==2,"Cobbles",IF(D87==3,"Gravel",IF(D87==4,"Angular Sand",IF(D87==5,"Rounded Sand",IF(D87==6,"Silt",IF(D87==7,"Soft Clay",IF(D87==8,"Stiff Clay","Own Data"))))))))
def xcf_Bank_Model_Output_E87(): 
    try:      
        return IF(vcell("D87","Bank Model Output")==1,"Boulders",IF(vcell("D87","Bank Model Output")==2,"Cobbles",IF(vcell("D87","Bank Model Output")==3,"Gravel",IF(vcell("D87","Bank Model Output")==4,"Angular Sand",IF(vcell("D87","Bank Model Output")==5,"Rounded Sand",IF(vcell("D87","Bank Model Output")==6,"Silt",IF(vcell("D87","Bank Model Output")==7,"Soft Clay",IF(vcell("D87","Bank Model Output")==8,"Stiff Clay","Own Data"))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E87')
    return None
xfunctions['Bank Model Output!E87']=xcf_Bank_Model_Output_E87

#IF($D87==1,Bank_Material!L$14,IF($D87==2,Bank_Material!L$15,IF($D87==3,Bank_Material!L$16,IF($D87==4,Bank_Material!L$18,IF($D87==5,Bank_Material!L$19,IF($D87==6,Bank_Material!L$21,IF($D87==7,Bank_Material!L$22,IF($D87==8,Bank_Material!L$23,Bank_Material!L31))))))))
def xcf_Bank_Model_Output_F87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!L$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!L$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!L$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!L$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!L$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!L$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!L$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!L$23","Bank Model Output"),vcell("Bank_Material!L31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F87')
    return None
xfunctions['Bank Model Output!F87']=xcf_Bank_Model_Output_F87

#IF($D87==1,Bank_Material!H$14,IF($D87==2,Bank_Material!H$15,IF($D87==3,Bank_Material!H$16,IF($D87==4,Bank_Material!H$18,IF($D87==5,Bank_Material!H$19,IF($D87==6,Bank_Material!H$21,IF($D87==7,Bank_Material!H$22,IF($D87==8,Bank_Material!H$23,Bank_Material!H31))))))))
def xcf_Bank_Model_Output_G87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!H$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!H$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!H$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!H$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!H$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!H$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!H$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!H$23","Bank Model Output"),vcell("Bank_Material!H31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_G87')
    return None
xfunctions['Bank Model Output!G87']=xcf_Bank_Model_Output_G87

#IF($D87==1,Bank_Material!J$14,IF($D87==2,Bank_Material!J$15,IF($D87==3,Bank_Material!J$16,IF($D87==4,Bank_Material!J$18,IF($D87==5,Bank_Material!J$19,IF($D87==6,Bank_Material!J$21,IF($D87==7,Bank_Material!J$22,IF($D87==8,Bank_Material!J$23,Bank_Material!J31))))))))
def xcf_Bank_Model_Output_H87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!J$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!J$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!J$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!J$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!J$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!J$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!J$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!J$23","Bank Model Output"),vcell("Bank_Material!J31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H87')
    return None
xfunctions['Bank Model Output!H87']=xcf_Bank_Model_Output_H87

#IF($D87==1,Bank_Material!N$14,IF($D87==2,Bank_Material!N$15,IF($D87==3,Bank_Material!N$16,IF($D87==4,Bank_Material!N$18,IF($D87==5,Bank_Material!N$19,IF($D87==6,Bank_Material!N$21,IF($D87==7,Bank_Material!N$22,IF($D87==8,Bank_Material!N$23,Bank_Material!N31))))))))
def xcf_Bank_Model_Output_I87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!N$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!N$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!N$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!N$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!N$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!N$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!N$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!N$23","Bank Model Output"),vcell("Bank_Material!N31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I87')
    return None
xfunctions['Bank Model Output!I87']=xcf_Bank_Model_Output_I87

#IF($D87==9,Bank_Material!P$31,0)
def xcf_Bank_Model_Output_J87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==9,vcell("Bank_Material!P$31","Bank Model Output"),0)
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_J87')
    return None
xfunctions['Bank Model Output!J87']=xcf_Bank_Model_Output_J87

#IF($D87==1,Bank_Material!S$14,IF($D87==2,Bank_Material!S$15,IF($D87==3,Bank_Material!S$16,IF($D87==4,Bank_Material!S$18,IF($D87==5,Bank_Material!S$19,IF($D87==6,Bank_Material!S$21,IF($D87==7,Bank_Material!S$22,IF($D87==8,Bank_Material!S$23,Bank_Material!S31))))))))
def xcf_Bank_Model_Output_L87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!S$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!S$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!S$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!S$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!S$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!S$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!S$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!S$23","Bank Model Output"),vcell("Bank_Material!S31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_L87')
    return None
xfunctions['Bank Model Output!L87']=xcf_Bank_Model_Output_L87

#IF($D87==1,Bank_Material!U$14,IF($D87==2,Bank_Material!U$15,IF($D87==3,Bank_Material!U$16,IF($D87==4,Bank_Material!U$18,IF($D87==5,Bank_Material!U$19,IF($D87==6,Bank_Material!U$21,IF($D87==7,Bank_Material!U$22,IF($D87==8,Bank_Material!U$23,Bank_Material!U31))))))))
def xcf_Bank_Model_Output_M87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!U$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!U$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!U$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!U$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!U$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!U$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!U$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!U$23","Bank Model Output"),vcell("Bank_Material!U31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_M87')
    return None
xfunctions['Bank Model Output!M87']=xcf_Bank_Model_Output_M87

#IF($D87==1,Bank_Material!W$14,IF($D87==2,Bank_Material!W$15,IF($D87==3,Bank_Material!W$16,IF($D87==4,Bank_Material!W$18,IF($D87==5,Bank_Material!W$19,IF($D87==6,Bank_Material!W$21,IF($D87==7,Bank_Material!W$22,IF($D87==8,Bank_Material!W$23,Bank_Material!W31))))))))
def xcf_Bank_Model_Output_N87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!W$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!W$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!W$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!W$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!W$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!W$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!W$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!W$23","Bank Model Output"),vcell("Bank_Material!W31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_N87')
    return None
xfunctions['Bank Model Output!N87']=xcf_Bank_Model_Output_N87

#IF($D87==1,Bank_Material!Y$14,IF($D87==2,Bank_Material!Y$15,IF($D87==3,Bank_Material!Y$16,IF($D87==4,Bank_Material!Y$18,IF($D87==5,Bank_Material!Y$19,IF($D87==6,Bank_Material!Y$21,IF($D87==7,Bank_Material!Y$22,IF($D87==8,Bank_Material!Y$23,Bank_Material!Y31))))))))
def xcf_Bank_Model_Output_O87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!Y$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!Y$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!Y$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!Y$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!Y$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!Y$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!Y$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!Y$23","Bank Model Output"),vcell("Bank_Material!Y31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_O87')
    return None
xfunctions['Bank Model Output!O87']=xcf_Bank_Model_Output_O87

#IF($D87==1,Bank_Material!AA$14,IF($D87==2,Bank_Material!AA$15,IF($D87==3,Bank_Material!AA$16,IF($D87==4,Bank_Material!AA$18,IF($D87==5,Bank_Material!AA$19,IF($D87==6,Bank_Material!AA$21,IF($D87==7,Bank_Material!AA$22,IF($D87==8,Bank_Material!AA$23,Bank_Material!AA31))))))))
def xcf_Bank_Model_Output_P87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!AA$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!AA$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!AA$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!AA$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!AA$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!AA$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!AA$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!AA$23","Bank Model Output"),vcell("Bank_Material!AA31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_P87')
    return None
xfunctions['Bank Model Output!P87']=xcf_Bank_Model_Output_P87

#IF($D87==1,Bank_Material!AC$14,IF($D87==2,Bank_Material!AC$15,IF($D87==3,Bank_Material!AC$16,IF($D87==4,Bank_Material!AC$18,IF($D87==5,Bank_Material!AC$19,IF($D87==6,Bank_Material!AC$21,IF($D87==7,Bank_Material!AC$22,IF($D87==8,Bank_Material!AC$23,Bank_Material!AC31))))))))
def xcf_Bank_Model_Output_Q87(): 
    try:      
        return IF(vcell("$D87","Bank Model Output")==1,vcell("Bank_Material!AC$14","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==2,vcell("Bank_Material!AC$15","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==3,vcell("Bank_Material!AC$16","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==4,vcell("Bank_Material!AC$18","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==5,vcell("Bank_Material!AC$19","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==6,vcell("Bank_Material!AC$21","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==7,vcell("Bank_Material!AC$22","Bank Model Output"),IF(vcell("$D87","Bank Model Output")==8,vcell("Bank_Material!AC$23","Bank Model Output"),vcell("Bank_Material!AC31","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_Q87')
    return None
xfunctions['Bank Model Output!Q87']=xcf_Bank_Model_Output_Q87

#IF(OR(C88==4,C88==5),4,IF(OR(C88==6,C88==7),5,IF(OR(C88==8,C88==9,C88==10),6,IF(OR(C88==11,C88==12,C88==13),7,IF(OR(C88==14,C88==15,C88==16),8,IF(C88==17,9,C88))))))
def xcf_Bank_Model_Output_D88(): 
    try:      
        return IF(OR(vcell("C88","Bank Model Output")==4,vcell("C88","Bank Model Output")==5),4,IF(OR(vcell("C88","Bank Model Output")==6,vcell("C88","Bank Model Output")==7),5,IF(OR(vcell("C88","Bank Model Output")==8,vcell("C88","Bank Model Output")==9,vcell("C88","Bank Model Output")==10),6,IF(OR(vcell("C88","Bank Model Output")==11,vcell("C88","Bank Model Output")==12,vcell("C88","Bank Model Output")==13),7,IF(OR(vcell("C88","Bank Model Output")==14,vcell("C88","Bank Model Output")==15,vcell("C88","Bank Model Output")==16),8,IF(vcell("C88","Bank Model Output")==17,9,vcell("C88","Bank Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_D88')
    return None
xfunctions['Bank Model Output!D88']=xcf_Bank_Model_Output_D88

#IF(D88==1,"Boulders",IF(D88==2,"Cobbles",IF(D88==3,"Gravel",IF(D88==4,"Angular Sand",IF(D88==5,"Rounded Sand",IF(D88==6,"Silt",IF(D88==7,"Soft Clay",IF(D88==8,"Stiff Clay","Own Data"))))))))
def xcf_Bank_Model_Output_E88(): 
    try:      
        return IF(vcell("D88","Bank Model Output")==1,"Boulders",IF(vcell("D88","Bank Model Output")==2,"Cobbles",IF(vcell("D88","Bank Model Output")==3,"Gravel",IF(vcell("D88","Bank Model Output")==4,"Angular Sand",IF(vcell("D88","Bank Model Output")==5,"Rounded Sand",IF(vcell("D88","Bank Model Output")==6,"Silt",IF(vcell("D88","Bank Model Output")==7,"Soft Clay",IF(vcell("D88","Bank Model Output")==8,"Stiff Clay","Own Data"))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_E88')
    return None
xfunctions['Bank Model Output!E88']=xcf_Bank_Model_Output_E88

#IF($D88==1,Bank_Material!L$14,IF($D88==2,Bank_Material!L$15,IF($D88==3,Bank_Material!L$16,IF($D88==4,Bank_Material!L$18,IF($D88==5,Bank_Material!L$19,IF($D88==6,Bank_Material!L$21,IF($D88==7,Bank_Material!L$22,IF($D88==8,Bank_Material!L$23,Bank_Material!L33))))))))
def xcf_Bank_Model_Output_F88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!L$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!L$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!L$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!L$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!L$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!L$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!L$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!L$23","Bank Model Output"),vcell("Bank_Material!L33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F88')
    return None
xfunctions['Bank Model Output!F88']=xcf_Bank_Model_Output_F88

#IF($D88==1,Bank_Material!H$14,IF($D88==2,Bank_Material!H$15,IF($D88==3,Bank_Material!H$16,IF($D88==4,Bank_Material!H$18,IF($D88==5,Bank_Material!H$19,IF($D88==6,Bank_Material!H$21,IF($D88==7,Bank_Material!H$22,IF($D88==8,Bank_Material!H$23,Bank_Material!H33))))))))
def xcf_Bank_Model_Output_G88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!H$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!H$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!H$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!H$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!H$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!H$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!H$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!H$23","Bank Model Output"),vcell("Bank_Material!H33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_G88')
    return None
xfunctions['Bank Model Output!G88']=xcf_Bank_Model_Output_G88

#IF($D88==1,Bank_Material!J$14,IF($D88==2,Bank_Material!J$15,IF($D88==3,Bank_Material!J$16,IF($D88==4,Bank_Material!J$18,IF($D88==5,Bank_Material!J$19,IF($D88==6,Bank_Material!J$21,IF($D88==7,Bank_Material!J$22,IF($D88==8,Bank_Material!J$23,Bank_Material!J33))))))))
def xcf_Bank_Model_Output_H88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!J$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!J$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!J$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!J$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!J$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!J$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!J$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!J$23","Bank Model Output"),vcell("Bank_Material!J33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H88')
    return None
xfunctions['Bank Model Output!H88']=xcf_Bank_Model_Output_H88

#IF($D88==1,Bank_Material!N$14,IF($D88==2,Bank_Material!N$15,IF($D88==3,Bank_Material!N$16,IF($D88==4,Bank_Material!N$18,IF($D88==5,Bank_Material!N$19,IF($D88==6,Bank_Material!N$21,IF($D88==7,Bank_Material!N$22,IF($D88==8,Bank_Material!N$23,Bank_Material!N33))))))))
def xcf_Bank_Model_Output_I88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!N$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!N$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!N$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!N$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!N$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!N$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!N$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!N$23","Bank Model Output"),vcell("Bank_Material!N33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_I88')
    return None
xfunctions['Bank Model Output!I88']=xcf_Bank_Model_Output_I88

#IF($D88==9,Bank_Material!P$33,0)
def xcf_Bank_Model_Output_J88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==9,vcell("Bank_Material!P$33","Bank Model Output"),0)
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_J88')
    return None
xfunctions['Bank Model Output!J88']=xcf_Bank_Model_Output_J88

#IF($D88==1,Bank_Material!S$14,IF($D88==2,Bank_Material!S$15,IF($D88==3,Bank_Material!S$16,IF($D88==4,Bank_Material!S$18,IF($D88==5,Bank_Material!S$19,IF($D88==6,Bank_Material!S$21,IF($D88==7,Bank_Material!S$22,IF($D88==8,Bank_Material!S$23,Bank_Material!S33))))))))
def xcf_Bank_Model_Output_L88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!S$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!S$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!S$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!S$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!S$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!S$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!S$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!S$23","Bank Model Output"),vcell("Bank_Material!S33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_L88')
    return None
xfunctions['Bank Model Output!L88']=xcf_Bank_Model_Output_L88

#IF($D88==1,Bank_Material!U$14,IF($D88==2,Bank_Material!U$15,IF($D88==3,Bank_Material!U$16,IF($D88==4,Bank_Material!U$18,IF($D88==5,Bank_Material!U$19,IF($D88==6,Bank_Material!U$21,IF($D88==7,Bank_Material!U$22,IF($D88==8,Bank_Material!U$23,Bank_Material!U33))))))))
def xcf_Bank_Model_Output_M88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!U$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!U$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!U$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!U$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!U$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!U$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!U$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!U$23","Bank Model Output"),vcell("Bank_Material!U33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_M88')
    return None
xfunctions['Bank Model Output!M88']=xcf_Bank_Model_Output_M88

#IF($D88==1,Bank_Material!W$14,IF($D88==2,Bank_Material!W$15,IF($D88==3,Bank_Material!W$16,IF($D88==4,Bank_Material!W$18,IF($D88==5,Bank_Material!W$19,IF($D88==6,Bank_Material!W$21,IF($D88==7,Bank_Material!W$22,IF($D88==8,Bank_Material!W$23,Bank_Material!W33))))))))
def xcf_Bank_Model_Output_N88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!W$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!W$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!W$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!W$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!W$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!W$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!W$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!W$23","Bank Model Output"),vcell("Bank_Material!W33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_N88')
    return None
xfunctions['Bank Model Output!N88']=xcf_Bank_Model_Output_N88

#IF($D88==1,Bank_Material!Y$14,IF($D88==2,Bank_Material!Y$15,IF($D88==3,Bank_Material!Y$16,IF($D88==4,Bank_Material!Y$18,IF($D88==5,Bank_Material!Y$19,IF($D88==6,Bank_Material!Y$21,IF($D88==7,Bank_Material!Y$22,IF($D88==8,Bank_Material!Y$23,Bank_Material!Y33))))))))
def xcf_Bank_Model_Output_O88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!Y$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!Y$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!Y$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!Y$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!Y$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!Y$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!Y$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!Y$23","Bank Model Output"),vcell("Bank_Material!Y33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_O88')
    return None
xfunctions['Bank Model Output!O88']=xcf_Bank_Model_Output_O88

#IF($D88==1,Bank_Material!AA$14,IF($D88==2,Bank_Material!AA$15,IF($D88==3,Bank_Material!AA$16,IF($D88==4,Bank_Material!AA$18,IF($D88==5,Bank_Material!AA$19,IF($D88==6,Bank_Material!AA$21,IF($D88==7,Bank_Material!AA$22,IF($D88==8,Bank_Material!AA$23,Bank_Material!AA33))))))))
def xcf_Bank_Model_Output_P88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!AA$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!AA$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!AA$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!AA$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!AA$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!AA$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!AA$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!AA$23","Bank Model Output"),vcell("Bank_Material!AA33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_P88')
    return None
xfunctions['Bank Model Output!P88']=xcf_Bank_Model_Output_P88

#IF($D88==1,Bank_Material!AC$14,IF($D88==2,Bank_Material!AC$15,IF($D88==3,Bank_Material!AC$16,IF($D88==4,Bank_Material!AC$18,IF($D88==5,Bank_Material!AC$19,IF($D88==6,Bank_Material!AC$21,IF($D88==7,Bank_Material!AC$22,IF($D88==8,Bank_Material!AC$23,Bank_Material!AC33))))))))
def xcf_Bank_Model_Output_Q88(): 
    try:      
        return IF(vcell("$D88","Bank Model Output")==1,vcell("Bank_Material!AC$14","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==2,vcell("Bank_Material!AC$15","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==3,vcell("Bank_Material!AC$16","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==4,vcell("Bank_Material!AC$18","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==5,vcell("Bank_Material!AC$19","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==6,vcell("Bank_Material!AC$21","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==7,vcell("Bank_Material!AC$22","Bank Model Output"),IF(vcell("$D88","Bank Model Output")==8,vcell("Bank_Material!AC$23","Bank Model Output"),vcell("Bank_Material!AC33","Bank Model Output")))))))))
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_Q88')
    return None
xfunctions['Bank Model Output!Q88']=xcf_Bank_Model_Output_Q88

#Calculations!D24-Bank_Model_Output!F84
def xcf_Bank_Model_Output_F89(): 
    try:      
        return vcell("Calculations!D24","Bank Model Output")-vcell("Bank_Model_Output!F84","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_F89')
    return None
xfunctions['Bank Model Output!F89']=xcf_Bank_Model_Output_F89

#Bank_Vegetation_and_Protection!I29
def xcf_Bank_Model_Output_H89(): 
    try:      
        return vcell("Bank_Vegetation_and_Protection!I29","Bank Model Output")
    except Exception as ex:
        print(ex,'on xcf_Bank_Model_Output_H89')
    return None
xfunctions['Bank Model Output!H89']=xcf_Bank_Model_Output_H89

#I69
def xcf_Toe_Model_Output_C8(): 
    try:      
        return vcell("I69","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_C8')
    return None
xfunctions['Toe Model Output!C8']=xcf_Toe_Model_Output_C8

#I70
def xcf_Toe_Model_Output_E8(): 
    try:      
        return vcell("I70","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E8')
    return None
xfunctions['Toe Model Output!E8']=xcf_Toe_Model_Output_E8

#I71
def xcf_Toe_Model_Output_G8(): 
    try:      
        return vcell("I71","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G8')
    return None
xfunctions['Toe Model Output!G8']=xcf_Toe_Model_Output_G8

#I72
def xcf_Toe_Model_Output_I8(): 
    try:      
        return vcell("I72","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I8')
    return None
xfunctions['Toe Model Output!I8']=xcf_Toe_Model_Output_I8

#I73
def xcf_Toe_Model_Output_K8(): 
    try:      
        return vcell("I73","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K8')
    return None
xfunctions['Toe Model Output!K8']=xcf_Toe_Model_Output_K8

#I74
def xcf_Toe_Model_Output_M8(): 
    try:      
        return vcell("I74","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M8')
    return None
xfunctions['Toe Model Output!M8']=xcf_Toe_Model_Output_M8

#IF(K69==0,I69,K69)
def xcf_Toe_Model_Output_C10(): 
    try:      
        return IF(vcell("K69","Toe Model Output")==0,vcell("I69","Toe Model Output"),vcell("K69","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_C10')
    return None
xfunctions['Toe Model Output!C10']=xcf_Toe_Model_Output_C10

#IF(K70==0,I70,K70)
def xcf_Toe_Model_Output_E10(): 
    try:      
        return IF(vcell("K70","Toe Model Output")==0,vcell("I70","Toe Model Output"),vcell("K70","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E10')
    return None
xfunctions['Toe Model Output!E10']=xcf_Toe_Model_Output_E10

#IF(K71==0,I71,K71)
def xcf_Toe_Model_Output_G10(): 
    try:      
        return IF(vcell("K71","Toe Model Output")==0,vcell("I71","Toe Model Output"),vcell("K71","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G10')
    return None
xfunctions['Toe Model Output!G10']=xcf_Toe_Model_Output_G10

#IF(K72==0,I72,K72)
def xcf_Toe_Model_Output_I10(): 
    try:      
        return IF(vcell("K72","Toe Model Output")==0,vcell("I72","Toe Model Output"),vcell("K72","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I10')
    return None
xfunctions['Toe Model Output!I10']=xcf_Toe_Model_Output_I10

#IF(K73==0,I73,K73)
def xcf_Toe_Model_Output_K10(): 
    try:      
        return IF(vcell("K73","Toe Model Output")==0,vcell("I73","Toe Model Output"),vcell("K73","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K10')
    return None
xfunctions['Toe Model Output!K10']=xcf_Toe_Model_Output_K10

#IF(K74==0,I74,K74)
def xcf_Toe_Model_Output_M10(): 
    try:      
        return IF(vcell("K74","Toe Model Output")==0,vcell("I74","Toe Model Output"),vcell("K74","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M10')
    return None
xfunctions['Toe Model Output!M10']=xcf_Toe_Model_Output_M10

#IF(M69==0,I69,M69)
def xcf_Toe_Model_Output_C12(): 
    try:      
        return IF(vcell("M69","Toe Model Output")==0,vcell("I69","Toe Model Output"),vcell("M69","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_C12')
    return None
xfunctions['Toe Model Output!C12']=xcf_Toe_Model_Output_C12

#IF(M70==0,I70,M70)
def xcf_Toe_Model_Output_E12(): 
    try:      
        return IF(vcell("M70","Toe Model Output")==0,vcell("I70","Toe Model Output"),vcell("M70","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E12')
    return None
xfunctions['Toe Model Output!E12']=xcf_Toe_Model_Output_E12

#IF(M71==0,I71,M71)
def xcf_Toe_Model_Output_G12(): 
    try:      
        return IF(vcell("M71","Toe Model Output")==0,vcell("I71","Toe Model Output"),vcell("M71","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G12')
    return None
xfunctions['Toe Model Output!G12']=xcf_Toe_Model_Output_G12

#IF(M72==0,I72,M72)
def xcf_Toe_Model_Output_I12(): 
    try:      
        return IF(vcell("M72","Toe Model Output")==0,vcell("I72","Toe Model Output"),vcell("M72","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I12')
    return None
xfunctions['Toe Model Output!I12']=xcf_Toe_Model_Output_I12

#IF(M73==0,I73,M73)
def xcf_Toe_Model_Output_K12(): 
    try:      
        return IF(vcell("M73","Toe Model Output")==0,vcell("I73","Toe Model Output"),vcell("M73","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K12')
    return None
xfunctions['Toe Model Output!K12']=xcf_Toe_Model_Output_K12

#IF(M74==0,I74,M74)
def xcf_Toe_Model_Output_M12(): 
    try:      
        return IF(vcell("M74","Toe Model Output")==0,vcell("I74","Toe Model Output"),vcell("M74","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M12')
    return None
xfunctions['Toe Model Output!M12']=xcf_Toe_Model_Output_M12

#Toe_Model!C153*100
def xcf_Toe_Model_Output_O26(): 
    try:      
        return vcell("Toe_Model!C153","Toe Model Output")*100
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_O26')
    return None
xfunctions['Toe Model Output!O26']=xcf_Toe_Model_Output_O26

#Toe_Model!C154
def xcf_Toe_Model_Output_O28(): 
    try:      
        return vcell("Toe_Model!C154","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_O28')
    return None
xfunctions['Toe Model Output!O28']=xcf_Toe_Model_Output_O28

#Toe_Model!C155
def xcf_Toe_Model_Output_O30(): 
    try:      
        return vcell("Toe_Model!C155","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_O30')
    return None
xfunctions['Toe Model Output!O30']=xcf_Toe_Model_Output_O30

#Toe_Model!C156
def xcf_Toe_Model_Output_O32(): 
    try:      
        return vcell("Toe_Model!C156","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_O32')
    return None
xfunctions['Toe Model Output!O32']=xcf_Toe_Model_Output_O32

#Toe_Model!C157
def xcf_Toe_Model_Output_O34(): 
    try:      
        return vcell("Toe_Model!C157","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_O34')
    return None
xfunctions['Toe Model Output!O34']=xcf_Toe_Model_Output_O34

#Bank_Material!AF14
def xcf_Toe_Model_Output_I48(): 
    try:      
        return vcell("Bank_Material!AF14","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I48')
    return None
xfunctions['Toe Model Output!I48']=xcf_Toe_Model_Output_I48

#Bank_Material!AF15
def xcf_Toe_Model_Output_I49(): 
    try:      
        return vcell("Bank_Material!AF15","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I49')
    return None
xfunctions['Toe Model Output!I49']=xcf_Toe_Model_Output_I49

#Bank_Material!AF16
def xcf_Toe_Model_Output_I50(): 
    try:      
        return vcell("Bank_Material!AF16","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I50')
    return None
xfunctions['Toe Model Output!I50']=xcf_Toe_Model_Output_I50

#0.044*16200*0.000707106781186548
def xcf_Toe_Model_Output_I51(): 
    try:      
        return 0.044*16200*0.000707106781186548
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I51')
    return None
xfunctions['Toe Model Output!I51']=xcf_Toe_Model_Output_I51

#0.044*16200*0.000176776695296637
def xcf_Toe_Model_Output_I52(): 
    try:      
        return 0.044*16200*0.000176776695296637
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I52')
    return None
xfunctions['Toe Model Output!I52']=xcf_Toe_Model_Output_I52

#Bank_Model_Output!C84
def xcf_Toe_Model_Output_E69(): 
    try:      
        return vcell("Bank_Model_Output!C84","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E69')
    return None
xfunctions['Toe Model Output!E69']=xcf_Toe_Model_Output_E69

#IF(OR(E69==4,E69==6),4,IF(OR(E69==5,E69==7),5,IF(OR(E69==8,E69==11,E69==14),6,IF(OR(E69==9,E69==12,E69==15),7,IF(OR(E69==10,E69==13,E69==16),8,IF(E69==17,9,E69))))))
def xcf_Toe_Model_Output_G69(): 
    try:      
        return IF(OR(vcell("E69","Toe Model Output")==4,vcell("E69","Toe Model Output")==6),4,IF(OR(vcell("E69","Toe Model Output")==5,vcell("E69","Toe Model Output")==7),5,IF(OR(vcell("E69","Toe Model Output")==8,vcell("E69","Toe Model Output")==11,vcell("E69","Toe Model Output")==14),6,IF(OR(vcell("E69","Toe Model Output")==9,vcell("E69","Toe Model Output")==12,vcell("E69","Toe Model Output")==15),7,IF(OR(vcell("E69","Toe Model Output")==10,vcell("E69","Toe Model Output")==13,vcell("E69","Toe Model Output")==16),8,IF(vcell("E69","Toe Model Output")==17,9,vcell("E69","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G69')
    return None
xfunctions['Toe Model Output!G69']=xcf_Toe_Model_Output_G69

#IF(E$77>1,G$77,LOOKUP(G69,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I69(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("G$77","Toe Model Output"),LOOKUP(vcell("G69","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I69')
    return None
xfunctions['Toe Model Output!I69']=xcf_Toe_Model_Output_I69

#IF(E$77>1,I$77,IF(G69==9,Bank_Material!AF25,LOOKUP(G69,F$48:F$55,I$48:I$53)))
def xcf_Toe_Model_Output_K69(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("I$77","Toe Model Output"),IF(vcell("G69","Toe Model Output")==9,vcell("Bank_Material!AF25","Toe Model Output"),LOOKUP(vcell("G69","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$53","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K69')
    return None
xfunctions['Toe Model Output!K69']=xcf_Toe_Model_Output_K69

#IF(OR(E$77>1,G69<9),0.1*K69^-0.5,Bank_Material!AH25)
def xcf_Toe_Model_Output_M69(): 
    try:      
        return IF(OR(vcell("E$77","Toe Model Output")>1,vcell("G69","Toe Model Output")<9),0.1*vcell("K69","Toe Model Output")**-0.5,vcell("Bank_Material!AH25","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M69')
    return None
xfunctions['Toe Model Output!M69']=xcf_Toe_Model_Output_M69

#Bank_Model_Output!C85
def xcf_Toe_Model_Output_E70(): 
    try:      
        return vcell("Bank_Model_Output!C85","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E70')
    return None
xfunctions['Toe Model Output!E70']=xcf_Toe_Model_Output_E70

#IF(OR(E70==4,E70==6),4,IF(OR(E70==5,E70==7),5,IF(OR(E70==8,E70==11,E70==14),6,IF(OR(E70==9,E70==12,E70==15),7,IF(OR(E70==10,E70==13,E70==16),8,IF(E70==17,9,E70))))))
def xcf_Toe_Model_Output_G70(): 
    try:      
        return IF(OR(vcell("E70","Toe Model Output")==4,vcell("E70","Toe Model Output")==6),4,IF(OR(vcell("E70","Toe Model Output")==5,vcell("E70","Toe Model Output")==7),5,IF(OR(vcell("E70","Toe Model Output")==8,vcell("E70","Toe Model Output")==11,vcell("E70","Toe Model Output")==14),6,IF(OR(vcell("E70","Toe Model Output")==9,vcell("E70","Toe Model Output")==12,vcell("E70","Toe Model Output")==15),7,IF(OR(vcell("E70","Toe Model Output")==10,vcell("E70","Toe Model Output")==13,vcell("E70","Toe Model Output")==16),8,IF(vcell("E70","Toe Model Output")==17,9,vcell("E70","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G70')
    return None
xfunctions['Toe Model Output!G70']=xcf_Toe_Model_Output_G70

#IF(E$77>1,G$77,LOOKUP(G70,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I70(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("G$77","Toe Model Output"),LOOKUP(vcell("G70","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I70')
    return None
xfunctions['Toe Model Output!I70']=xcf_Toe_Model_Output_I70

#IF(E$77>1,I$77,IF(G70==9,Bank_Material!AF27,LOOKUP(G70,F$48:F$55,I$48:I$55)))
def xcf_Toe_Model_Output_K70(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("I$77","Toe Model Output"),IF(vcell("G70","Toe Model Output")==9,vcell("Bank_Material!AF27","Toe Model Output"),LOOKUP(vcell("G70","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$55","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K70')
    return None
xfunctions['Toe Model Output!K70']=xcf_Toe_Model_Output_K70

#IF(OR(E$77>1,G70<9),0.1*K70^-0.5,Bank_Material!AH27)
def xcf_Toe_Model_Output_M70(): 
    try:      
        return IF(OR(vcell("E$77","Toe Model Output")>1,vcell("G70","Toe Model Output")<9),0.1*vcell("K70","Toe Model Output")**-0.5,vcell("Bank_Material!AH27","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M70')
    return None
xfunctions['Toe Model Output!M70']=xcf_Toe_Model_Output_M70

#Bank_Model_Output!C86
def xcf_Toe_Model_Output_E71(): 
    try:      
        return vcell("Bank_Model_Output!C86","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E71')
    return None
xfunctions['Toe Model Output!E71']=xcf_Toe_Model_Output_E71

#IF(OR(E71==4,E71==6),4,IF(OR(E71==5,E71==7),5,IF(OR(E71==8,E71==11,E71==14),6,IF(OR(E71==9,E71==12,E71==15),7,IF(OR(E71==10,E71==13,E71==16),8,IF(E71==17,9,E71))))))
def xcf_Toe_Model_Output_G71(): 
    try:      
        return IF(OR(vcell("E71","Toe Model Output")==4,vcell("E71","Toe Model Output")==6),4,IF(OR(vcell("E71","Toe Model Output")==5,vcell("E71","Toe Model Output")==7),5,IF(OR(vcell("E71","Toe Model Output")==8,vcell("E71","Toe Model Output")==11,vcell("E71","Toe Model Output")==14),6,IF(OR(vcell("E71","Toe Model Output")==9,vcell("E71","Toe Model Output")==12,vcell("E71","Toe Model Output")==15),7,IF(OR(vcell("E71","Toe Model Output")==10,vcell("E71","Toe Model Output")==13,vcell("E71","Toe Model Output")==16),8,IF(vcell("E71","Toe Model Output")==17,9,vcell("E71","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G71')
    return None
xfunctions['Toe Model Output!G71']=xcf_Toe_Model_Output_G71

#IF(E$77>1,G$77,LOOKUP(G71,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I71(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("G$77","Toe Model Output"),LOOKUP(vcell("G71","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I71')
    return None
xfunctions['Toe Model Output!I71']=xcf_Toe_Model_Output_I71

#IF(E$77>1,I$77,IF(G71==9,Bank_Material!AF29,LOOKUP(G71,F$48:F$55,I$48:I$55)))
def xcf_Toe_Model_Output_K71(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("I$77","Toe Model Output"),IF(vcell("G71","Toe Model Output")==9,vcell("Bank_Material!AF29","Toe Model Output"),LOOKUP(vcell("G71","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$55","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K71')
    return None
xfunctions['Toe Model Output!K71']=xcf_Toe_Model_Output_K71

#IF(OR(E$77>1,G71<9),0.1*K71^-0.5,Bank_Material!AH29)
def xcf_Toe_Model_Output_M71(): 
    try:      
        return IF(OR(vcell("E$77","Toe Model Output")>1,vcell("G71","Toe Model Output")<9),0.1*vcell("K71","Toe Model Output")**-0.5,vcell("Bank_Material!AH29","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M71')
    return None
xfunctions['Toe Model Output!M71']=xcf_Toe_Model_Output_M71

#Bank_Model_Output!C87
def xcf_Toe_Model_Output_E72(): 
    try:      
        return vcell("Bank_Model_Output!C87","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E72')
    return None
xfunctions['Toe Model Output!E72']=xcf_Toe_Model_Output_E72

#IF(OR(E72==4,E72==6),4,IF(OR(E72==5,E72==7),5,IF(OR(E72==8,E72==11,E72==14),6,IF(OR(E72==9,E72==12,E72==15),7,IF(OR(E72==10,E72==13,E72==16),8,IF(E72==17,9,E72))))))
def xcf_Toe_Model_Output_G72(): 
    try:      
        return IF(OR(vcell("E72","Toe Model Output")==4,vcell("E72","Toe Model Output")==6),4,IF(OR(vcell("E72","Toe Model Output")==5,vcell("E72","Toe Model Output")==7),5,IF(OR(vcell("E72","Toe Model Output")==8,vcell("E72","Toe Model Output")==11,vcell("E72","Toe Model Output")==14),6,IF(OR(vcell("E72","Toe Model Output")==9,vcell("E72","Toe Model Output")==12,vcell("E72","Toe Model Output")==15),7,IF(OR(vcell("E72","Toe Model Output")==10,vcell("E72","Toe Model Output")==13,vcell("E72","Toe Model Output")==16),8,IF(vcell("E72","Toe Model Output")==17,9,vcell("E72","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G72')
    return None
xfunctions['Toe Model Output!G72']=xcf_Toe_Model_Output_G72

#IF(E$77>1,G$77,LOOKUP(G72,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I72(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("G$77","Toe Model Output"),LOOKUP(vcell("G72","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I72')
    return None
xfunctions['Toe Model Output!I72']=xcf_Toe_Model_Output_I72

#IF(E$77>1,I$77,IF(G72==9,Bank_Material!AF31,LOOKUP(G72,F$48:F$55,I$48:I$55)))
def xcf_Toe_Model_Output_K72(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("I$77","Toe Model Output"),IF(vcell("G72","Toe Model Output")==9,vcell("Bank_Material!AF31","Toe Model Output"),LOOKUP(vcell("G72","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$55","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K72')
    return None
xfunctions['Toe Model Output!K72']=xcf_Toe_Model_Output_K72

#IF(OR(E$77>1,G72<9),0.1*K72^-0.5,Bank_Material!AH31)
def xcf_Toe_Model_Output_M72(): 
    try:      
        return IF(OR(vcell("E$77","Toe Model Output")>1,vcell("G72","Toe Model Output")<9),0.1*vcell("K72","Toe Model Output")**-0.5,vcell("Bank_Material!AH31","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M72')
    return None
xfunctions['Toe Model Output!M72']=xcf_Toe_Model_Output_M72

#Bank_Model_Output!C88
def xcf_Toe_Model_Output_E73(): 
    try:      
        return vcell("Bank_Model_Output!C88","Toe Model Output")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E73')
    return None
xfunctions['Toe Model Output!E73']=xcf_Toe_Model_Output_E73

#IF(OR(E73==4,E73==6),4,IF(OR(E73==5,E73==7),5,IF(OR(E73==8,E73==11,E73==14),6,IF(OR(E73==9,E73==12,E73==15),7,IF(OR(E73==10,E73==13,E73==16),8,IF(E73==17,9,E73))))))
def xcf_Toe_Model_Output_G73(): 
    try:      
        return IF(OR(vcell("E73","Toe Model Output")==4,vcell("E73","Toe Model Output")==6),4,IF(OR(vcell("E73","Toe Model Output")==5,vcell("E73","Toe Model Output")==7),5,IF(OR(vcell("E73","Toe Model Output")==8,vcell("E73","Toe Model Output")==11,vcell("E73","Toe Model Output")==14),6,IF(OR(vcell("E73","Toe Model Output")==9,vcell("E73","Toe Model Output")==12,vcell("E73","Toe Model Output")==15),7,IF(OR(vcell("E73","Toe Model Output")==10,vcell("E73","Toe Model Output")==13,vcell("E73","Toe Model Output")==16),8,IF(vcell("E73","Toe Model Output")==17,9,vcell("E73","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G73')
    return None
xfunctions['Toe Model Output!G73']=xcf_Toe_Model_Output_G73

#IF(E$77>1,G$77,LOOKUP(G73,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I73(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("G$77","Toe Model Output"),LOOKUP(vcell("G73","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I73')
    return None
xfunctions['Toe Model Output!I73']=xcf_Toe_Model_Output_I73

#IF(E$77>1,I$77,IF(G73==9,Bank_Material!AF33,LOOKUP(G73,F$48:F$55,I$48:I$55)))
def xcf_Toe_Model_Output_K73(): 
    try:      
        return IF(vcell("E$77","Toe Model Output")>1,vcell("I$77","Toe Model Output"),IF(vcell("G73","Toe Model Output")==9,vcell("Bank_Material!AF33","Toe Model Output"),LOOKUP(vcell("G73","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$55","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K73')
    return None
xfunctions['Toe Model Output!K73']=xcf_Toe_Model_Output_K73

#IF(OR(E$77>1,G73<9),0.1*K73^-0.5,Bank_Material!AH33)
def xcf_Toe_Model_Output_M73(): 
    try:      
        return IF(OR(vcell("E$77","Toe Model Output")>1,vcell("G73","Toe Model Output")<9),0.1*vcell("K73","Toe Model Output")**-0.5,vcell("Bank_Material!AH33","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M73')
    return None
xfunctions['Toe Model Output!M73']=xcf_Toe_Model_Output_M73

#IF(OR(E74==4,E74==6),4,IF(OR(E74==5,E74==7),5,IF(OR(E74==8,E74==11,E74==14),6,IF(OR(E74==9,E74==12,E74==15),7,IF(OR(E74==10,E74==13,E74==16),8,IF(E74==17,9,E74))))))
def xcf_Toe_Model_Output_G74(): 
    try:      
        return IF(OR(vcell("E74","Toe Model Output")==4,vcell("E74","Toe Model Output")==6),4,IF(OR(vcell("E74","Toe Model Output")==5,vcell("E74","Toe Model Output")==7),5,IF(OR(vcell("E74","Toe Model Output")==8,vcell("E74","Toe Model Output")==11,vcell("E74","Toe Model Output")==14),6,IF(OR(vcell("E74","Toe Model Output")==9,vcell("E74","Toe Model Output")==12,vcell("E74","Toe Model Output")==15),7,IF(OR(vcell("E74","Toe Model Output")==10,vcell("E74","Toe Model Output")==13,vcell("E74","Toe Model Output")==16),8,IF(vcell("E74","Toe Model Output")==17,9,vcell("E74","Toe Model Output")))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G74')
    return None
xfunctions['Toe Model Output!G74']=xcf_Toe_Model_Output_G74

#IF(E$78>1,G$78,LOOKUP(G74,F$48:F$56,G$48:G$56))
def xcf_Toe_Model_Output_I74(): 
    try:      
        return IF(vcell("E$78","Toe Model Output")>1,vcell("G$78","Toe Model Output"),LOOKUP(vcell("G74","Toe Model Output"),vcell("F$48:F$56","Toe Model Output"),vcell("G$48:G$56","Toe Model Output")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I74')
    return None
xfunctions['Toe Model Output!I74']=xcf_Toe_Model_Output_I74

#IF(E$78>1,I$78,IF(G74==9,Bank_Material!AF35,LOOKUP(G74,F$48:F$55,I$48:I$55)))
def xcf_Toe_Model_Output_K74(): 
    try:      
        return IF(vcell("E$78","Toe Model Output")>1,vcell("I$78","Toe Model Output"),IF(vcell("G74","Toe Model Output")==9,vcell("Bank_Material!AF35","Toe Model Output"),LOOKUP(vcell("G74","Toe Model Output"),vcell("F$48:F$55","Toe Model Output"),vcell("I$48:I$55","Toe Model Output"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_K74')
    return None
xfunctions['Toe Model Output!K74']=xcf_Toe_Model_Output_K74

#IF(OR(E$78>1,G74<9),0.1*K74^-0.5,Bank_Material!AH35)
def xcf_Toe_Model_Output_M74(): 
    try:      
        return IF(OR(vcell("E$78","Toe Model Output")>1,vcell("G74","Toe Model Output")<9),0.1*vcell("K74","Toe Model Output")**-0.5,vcell("Bank_Material!AH35","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_M74')
    return None
xfunctions['Toe Model Output!M74']=xcf_Toe_Model_Output_M74

#LOOKUP($E77,Bank_Vegetation_and_Protection!$P$17:$P$29,Bank_Vegetation_and_Protection!S$17:S$29)
def xcf_Toe_Model_Output_G77(): 
    try:      
        return LOOKUP(vcell("$E77","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!$P$17:$P$29","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!S$17:S$29","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G77')
    return None
xfunctions['Toe Model Output!G77']=xcf_Toe_Model_Output_G77

#LOOKUP($E77,Bank_Vegetation_and_Protection!$P$17:$P$29,Bank_Vegetation_and_Protection!V$17:V$29)
def xcf_Toe_Model_Output_I77(): 
    try:      
        return LOOKUP(vcell("$E77","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!$P$17:$P$29","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!V$17:V$29","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I77')
    return None
xfunctions['Toe Model Output!I77']=xcf_Toe_Model_Output_I77

#LOOKUP($E78,Bank_Vegetation_and_Protection!$P$17:$P$29,Bank_Vegetation_and_Protection!S$17:S$29)
def xcf_Toe_Model_Output_G78(): 
    try:      
        return LOOKUP(vcell("$E78","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!$P$17:$P$29","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!S$17:S$29","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_G78')
    return None
xfunctions['Toe Model Output!G78']=xcf_Toe_Model_Output_G78

#LOOKUP($E78,Bank_Vegetation_and_Protection!$P$17:$P$29,Bank_Vegetation_and_Protection!V$17:V$29)
def xcf_Toe_Model_Output_I78(): 
    try:      
        return LOOKUP(vcell("$E78","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!$P$17:$P$29","Toe Model Output"),vcell("Bank_Vegetation_and_Protection!V$17:V$29","Toe Model Output"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_I78')
    return None
xfunctions['Toe Model Output!I78']=xcf_Toe_Model_Output_I78

#IF(E82==TRUE,1,0)+IF(E83==TRUE,2,0)
def xcf_Toe_Model_Output_E84(): 
    try:      
        return IF(vcell("E82","Toe Model Output")==TRUE,1,0)+IF(vcell("E83","Toe Model Output")==TRUE,2,0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Output_E84')
    return None
xfunctions['Toe Model Output!E84']=xcf_Toe_Model_Output_E84

#B6*F9
def xcf_Unit_Converter_F6(): 
    try:      
        return vcell("B6","Unit Converter")*vcell("F9","Unit Converter")
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_F6')
    return None
xfunctions['Unit Converter!F6']=xcf_Unit_Converter_F6

#INDIRECT("R"&8+D16&"C"&2+J16,FALSE)
def xcf_Unit_Converter_F9(): 
    try:      
        return  INDIRECT(8+vcell("D16","Unit Converter"),2+vcell("J16","Unit Converter"),"Unit Converter") #�ֶ��滻
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_F9')
    return None
xfunctions['Unit Converter!F9']=xcf_Unit_Converter_F9

#IF(D$16<3,"Pa",IF(D$16<5,"N/m3",IF(D$16<6,"m","1/m")))
def xcf_Unit_Converter_H9(): 
    try:      
        return IF(vcell("D$16","Unit Converter")<3,"Pa",IF(vcell("D$16","Unit Converter")<5,"N/m3",IF(vcell("D$16","Unit Converter")<6,"m","1/m")))
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_H9')
    return None
xfunctions['Unit Converter!H9']=xcf_Unit_Converter_H9

#IF(D$16<3,"kPa",IF(D$16<5,"kN/m3",IF(D$16<6,"mm","1/mm")))
def xcf_Unit_Converter_H10(): 
    try:      
        return IF(vcell("D$16","Unit Converter")<3,"kPa",IF(vcell("D$16","Unit Converter")<5,"kN/m3",IF(vcell("D$16","Unit Converter")<6,"mm","1/mm")))
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_H10')
    return None
xfunctions['Unit Converter!H10']=xcf_Unit_Converter_H10

#1/0.3048
def xcf_Unit_Converter_C14(): 
    try:      
        return 1/0.3048
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_C14')
    return None
xfunctions['Unit Converter!C14']=xcf_Unit_Converter_C14

#1000/0.3048
def xcf_Unit_Converter_D14(): 
    try:      
        return 1000/0.3048
    except Exception as ex:
        print(ex,'on xcf_Unit_Converter_D14')
    return None
xfunctions['Unit Converter!D14']=xcf_Unit_Converter_D14

#IF(Input_Geometry!C55!="",Input_Geometry!C55,"")
def xcf_Toe_Model_G3(): 
    try:      
        return IF(vcell("Input_Geometry!C55","Toe Model")!="",vcell("Input_Geometry!C55","Toe Model"),"")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G3')
    return None
xfunctions['Toe Model!G3']=xcf_Toe_Model_G3

#Input_Geometry!E92
def xcf_Toe_Model_B4(): 
    try:      
        return vcell("Input_Geometry!E92","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B4')
    return None
xfunctions['Toe Model!B4']=xcf_Toe_Model_B4

#Input_Geometry!E93
def xcf_Toe_Model_B5(): 
    try:      
        return vcell("Input_Geometry!E93","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B5')
    return None
xfunctions['Toe Model!B5']=xcf_Toe_Model_B5

#IF(Toe_Model_Output!C10=="Enter own data",0,Toe_Model_Output!C10)
def xcf_Toe_Model_C5(): 
    try:      
        return IF(vcell("Toe_Model_Output!C10","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!C10","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C5')
    return None
xfunctions['Toe Model!C5']=xcf_Toe_Model_C5

#IF(Toe_Model_Output!C12=="Enter own data",0,Toe_Model_Output!C12/1000000)
def xcf_Toe_Model_D5(): 
    try:      
        return IF(vcell("Toe_Model_Output!C12","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!C12","Toe Model")/1000000)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D5')
    return None
xfunctions['Toe Model!D5']=xcf_Toe_Model_D5

#Input_Geometry!C57*60*60
def xcf_Toe_Model_G5(): 
    try:      
        return vcell("Input_Geometry!C57","Toe Model")*60*60
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G5')
    return None
xfunctions['Toe Model!G5']=xcf_Toe_Model_G5

#Input_Geometry!E94
def xcf_Toe_Model_B6(): 
    try:      
        return vcell("Input_Geometry!E94","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B6')
    return None
xfunctions['Toe Model!B6']=xcf_Toe_Model_B6

#IF(Toe_Model_Output!E10=="Enter own data",0,Toe_Model_Output!E10)
def xcf_Toe_Model_C6(): 
    try:      
        return IF(vcell("Toe_Model_Output!E10","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!E10","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C6')
    return None
xfunctions['Toe Model!C6']=xcf_Toe_Model_C6

#IF(Toe_Model_Output!E12=="Enter own data",0,Toe_Model_Output!E12/1000000)
def xcf_Toe_Model_D6(): 
    try:      
        return IF(vcell("Toe_Model_Output!E12","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!E12","Toe Model")/1000000)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D6')
    return None
xfunctions['Toe Model!D6']=xcf_Toe_Model_D6

#Input_Geometry!E95
def xcf_Toe_Model_B7(): 
    try:      
        return vcell("Input_Geometry!E95","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B7')
    return None
xfunctions['Toe Model!B7']=xcf_Toe_Model_B7

#IF(Toe_Model_Output!G10=="Enter own data",0,Toe_Model_Output!G10)
def xcf_Toe_Model_C7(): 
    try:      
        return IF(vcell("Toe_Model_Output!G10","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!G10","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C7')
    return None
xfunctions['Toe Model!C7']=xcf_Toe_Model_C7

#IF(Toe_Model_Output!G12=="Enter own data",0,Toe_Model_Output!G12/1000000)
def xcf_Toe_Model_D7(): 
    try:      
        return IF(vcell("Toe_Model_Output!G12","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!G12","Toe Model")/1000000)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D7')
    return None
xfunctions['Toe Model!D7']=xcf_Toe_Model_D7

#C19-C40
def xcf_Toe_Model_G7(): 
    try:      
        return vcell("C19","Toe Model")-vcell("C40","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G7')
    return None
xfunctions['Toe Model!G7']=xcf_Toe_Model_G7

#Input_Geometry!E96
def xcf_Toe_Model_B8(): 
    try:      
        return vcell("Input_Geometry!E96","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B8')
    return None
xfunctions['Toe Model!B8']=xcf_Toe_Model_B8

#IF(Toe_Model_Output!I10=="Enter own data",0,Toe_Model_Output!I10)
def xcf_Toe_Model_C8(): 
    try:      
        return IF(vcell("Toe_Model_Output!I10","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!I10","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C8')
    return None
xfunctions['Toe Model!C8']=xcf_Toe_Model_C8

#IF(Toe_Model_Output!I12=="Enter own data",0,Toe_Model_Output!I12/1000000)
def xcf_Toe_Model_D8(): 
    try:      
        return IF(vcell("Toe_Model_Output!I12","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!I12","Toe Model")/1000000)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D8')
    return None
xfunctions['Toe Model!D8']=xcf_Toe_Model_D8

#Input_Geometry!E97
def xcf_Toe_Model_B9(): 
    try:      
        return vcell("Input_Geometry!E97","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B9')
    return None
xfunctions['Toe Model!B9']=xcf_Toe_Model_B9

#IF(Toe_Model_Output!K10=="Enter own data",0,Toe_Model_Output!K10)
def xcf_Toe_Model_C9(): 
    try:      
        return IF(vcell("Toe_Model_Output!K10","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!K10","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C9')
    return None
xfunctions['Toe Model!C9']=xcf_Toe_Model_C9

#IF(Toe_Model_Output!K12=="Enter own data",0,Toe_Model_Output!K12/1000000)
def xcf_Toe_Model_D9(): 
    try:      
        return IF(vcell("Toe_Model_Output!K12","Toe Model")=="Enter own data",0,vcell("Toe_Model_Output!K12","Toe Model")/1000000)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D9')
    return None
xfunctions['Toe Model!D9']=xcf_Toe_Model_D9

#IF(Input_Geometry!C109==1,MIN(DEGREES(ATAN((C19-C34)/(B34-B19))),90),Input_Geometry!G20)
def xcf_Toe_Model_G9(): 
    try:      
        return IF(vcell("Input_Geometry!C109","Toe Model")==1,MIN(DEGREES(ATAN((vcell("C19","Toe Model")-vcell("C34","Toe Model"))/(vcell("B34","Toe Model")-vcell("B19","Toe Model")))),90),vcell("Input_Geometry!G20","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G9')
    return None
xfunctions['Toe Model!G9']=xcf_Toe_Model_G9

#Toe_Model_Output!M10
def xcf_Toe_Model_C10(): 
    try:      
        return vcell("Toe_Model_Output!M10","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C10')
    return None
xfunctions['Toe Model!C10']=xcf_Toe_Model_C10

#Toe_Model_Output!M12/1000000
def xcf_Toe_Model_D10(): 
    try:      
        return vcell("Toe_Model_Output!M12","Toe Model")/1000000
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D10')
    return None
xfunctions['Toe Model!D10']=xcf_Toe_Model_D10

#MIN(C18:C40)
def xcf_Toe_Model_B11(): 
    try:      
        return MIN(vcell("C18:C40","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B11')
    return None
xfunctions['Toe Model!B11']=xcf_Toe_Model_B11

#IF(Toe_Model_Output!E78>1,C10,IF(AND($B4>$B$11,$B$11>=$B5),C5,IF(AND($B5>$B$11,$B$11>=$B6),C6,IF(AND($B6>$B$11,$B$11>=$B7),C7,IF(AND($B7>$B$11,$B$11>=$B8),C8,C9)))))
def xcf_Toe_Model_C11(): 
    try:      
        return IF(vcell("Toe_Model_Output!E78","Toe Model")>1,vcell("C10","Toe Model"),IF(AND(vcell("$B4","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B5","Toe Model")),vcell("C5","Toe Model"),IF(AND(vcell("$B5","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B6","Toe Model")),vcell("C6","Toe Model"),IF(AND(vcell("$B6","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B7","Toe Model")),vcell("C7","Toe Model"),IF(AND(vcell("$B7","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B8","Toe Model")),vcell("C8","Toe Model"),vcell("C9","Toe Model"))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C11')
    return None
xfunctions['Toe Model!C11']=xcf_Toe_Model_C11

#IF(Toe_Model_Output!E78>1,D10,IF(AND($B4>$B$11,$B$11>=$B5),D5,IF(AND($B5>$B$11,$B$11>=$B6),D6,IF(AND($B6>$B$11,$B$11>=$B7),D7,IF(AND($B7>$B$11,$B$11>=$B8),D8,D9)))))
def xcf_Toe_Model_D11(): 
    try:      
        return IF(vcell("Toe_Model_Output!E78","Toe Model")>1,vcell("D10","Toe Model"),IF(AND(vcell("$B4","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B5","Toe Model")),vcell("D5","Toe Model"),IF(AND(vcell("$B5","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B6","Toe Model")),vcell("D6","Toe Model"),IF(AND(vcell("$B6","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B7","Toe Model")),vcell("D7","Toe Model"),IF(AND(vcell("$B7","Toe Model")>vcell("$B$11","Toe Model"),vcell("$B$11","Toe Model")>=vcell("$B8","Toe Model")),vcell("D8","Toe Model"),vcell("D9","Toe Model"))))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D11')
    return None
xfunctions['Toe Model!D11']=xcf_Toe_Model_D11

#IF(Input_Geometry!C109==1,((B39-B34)^2+(C34-C39)^2)^0.5,Input_Geometry!G22)
def xcf_Toe_Model_G11(): 
    try:      
        return IF(vcell("Input_Geometry!C109","Toe Model")==1,((vcell("B39","Toe Model")-vcell("B34","Toe Model"))**2+(vcell("C34","Toe Model")-vcell("C39","Toe Model"))**2)**0.5,vcell("Input_Geometry!G22","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G11')
    return None
xfunctions['Toe Model!G11']=xcf_Toe_Model_G11

#MIN(90,IF(Input_Geometry!C109==1,DEGREES(ACOS((B39-B34)/G11)),Input_Geometry!G24))
def xcf_Toe_Model_G13(): 
    try:      
        return MIN(90,IF(vcell("Input_Geometry!C109","Toe Model")==1,DEGREES(ACOS((vcell("B39","Toe Model")-vcell("B34","Toe Model"))/vcell("G11","Toe Model"))),vcell("Input_Geometry!G24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G13')
    return None
xfunctions['Toe Model!G13']=xcf_Toe_Model_G13

#Calculations!F5
def xcf_Toe_Model_B18(): 
    try:      
        return vcell("Calculations!F5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B18')
    return None
xfunctions['Toe Model!B18']=xcf_Toe_Model_B18

#Calculations!G5
def xcf_Toe_Model_C18(): 
    try:      
        return vcell("Calculations!G5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C18')
    return None
xfunctions['Toe Model!C18']=xcf_Toe_Model_C18

#IF(AND((B18>B19),(C19>C18)),360,DEGREES(ATAN(MAX((C18-C19),1E-20)/MAX((B19-B18),(1E-20)))))
def xcf_Toe_Model_D18(): 
    try:      
        return IF(AND((vcell("B18","Toe Model")>vcell("B19","Toe Model")),(vcell("C19","Toe Model")>vcell("C18","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C18","Toe Model")-vcell("C19","Toe Model")),1E-20)/MAX((vcell("B19","Toe Model")-vcell("B18","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D18')
    return None
xfunctions['Toe Model!D18']=xcf_Toe_Model_D18

#IF($C18>$B$5,1,IF(AND($C18<=$B$5,$C18>$B$6),2,IF(AND($C18<=$B$6,$C18>$B$7),3,IF(AND($C18<=$B$7,$C18>$B$8),4,IF(AND($C18<=$B$8,$C18>$B$9),5,6)))))
def xcf_Toe_Model_E18(): 
    try:      
        return IF(vcell("$C18","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C18","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C18","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C18","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C18","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C18","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C18","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C18","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C18","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E18')
    return None
xfunctions['Toe Model!E18']=xcf_Toe_Model_E18

#(C72-$B$5)/COS(RADIANS($D18))
def xcf_Toe_Model_K18(): 
    try:      
        return (vcell("C72","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K18')
    return None
xfunctions['Toe Model!K18']=xcf_Toe_Model_K18

#(C72-$B$6)/COS(RADIANS($D18))
def xcf_Toe_Model_L18(): 
    try:      
        return (vcell("C72","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L18')
    return None
xfunctions['Toe Model!L18']=xcf_Toe_Model_L18

#(C72-$B$7)/COS(RADIANS($D18))
def xcf_Toe_Model_M18(): 
    try:      
        return (vcell("C72","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M18')
    return None
xfunctions['Toe Model!M18']=xcf_Toe_Model_M18

#(C72-$B$8)/COS(RADIANS($D18))
def xcf_Toe_Model_N18(): 
    try:      
        return (vcell("C72","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N18')
    return None
xfunctions['Toe Model!N18']=xcf_Toe_Model_N18

#(C72-$B$9)/COS(RADIANS($D18))
def xcf_Toe_Model_O18(): 
    try:      
        return (vcell("C72","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O18')
    return None
xfunctions['Toe Model!O18']=xcf_Toe_Model_O18

#(C72+999999)/COS(RADIANS($D18))
def xcf_Toe_Model_P18(): 
    try:      
        return (vcell("C72","Toe Model")+999999)/COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P18')
    return None
xfunctions['Toe Model!P18']=xcf_Toe_Model_P18

#Calculations!F6
def xcf_Toe_Model_B19(): 
    try:      
        return vcell("Calculations!F6","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B19')
    return None
xfunctions['Toe Model!B19']=xcf_Toe_Model_B19

#Calculations!G6
def xcf_Toe_Model_C19(): 
    try:      
        return vcell("Calculations!G6","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C19')
    return None
xfunctions['Toe Model!C19']=xcf_Toe_Model_C19

#IF(AND((B18>B20),(C20>C18)),360,DEGREES(ATAN(MAX((C18-C20),1E-20)/MAX((B20-B18),(1E-20)))))
def xcf_Toe_Model_D19(): 
    try:      
        return IF(AND((vcell("B18","Toe Model")>vcell("B20","Toe Model")),(vcell("C20","Toe Model")>vcell("C18","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C18","Toe Model")-vcell("C20","Toe Model")),1E-20)/MAX((vcell("B20","Toe Model")-vcell("B18","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D19')
    return None
xfunctions['Toe Model!D19']=xcf_Toe_Model_D19

#IF($C19>$B$5,1,IF(AND($C19<=$B$5,$C19>$B$6),2,IF(AND($C19<=$B$6,$C19>$B$7),3,IF(AND($C19<=$B$7,$C19>$B$8),4,IF(AND($C19<=$B$8,$C19>$B$9),5,6)))))
def xcf_Toe_Model_E19(): 
    try:      
        return IF(vcell("$C19","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C19","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C19","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C19","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C19","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C19","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C19","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C19","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C19","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E19')
    return None
xfunctions['Toe Model!E19']=xcf_Toe_Model_E19

#B34
def xcf_Toe_Model_G19(): 
    try:      
        return vcell("B34","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G19')
    return None
xfunctions['Toe Model!G19']=xcf_Toe_Model_G19

#C34
def xcf_Toe_Model_H19(): 
    try:      
        return vcell("C34","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H19')
    return None
xfunctions['Toe Model!H19']=xcf_Toe_Model_H19

#(C73-$B$5)/COS(RADIANS($D19))
def xcf_Toe_Model_K19(): 
    try:      
        return (vcell("C73","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K19')
    return None
xfunctions['Toe Model!K19']=xcf_Toe_Model_K19

#(C73-$B$6)/COS(RADIANS($D19))
def xcf_Toe_Model_L19(): 
    try:      
        return (vcell("C73","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L19')
    return None
xfunctions['Toe Model!L19']=xcf_Toe_Model_L19

#(C73-$B$7)/COS(RADIANS($D19))
def xcf_Toe_Model_M19(): 
    try:      
        return (vcell("C73","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M19')
    return None
xfunctions['Toe Model!M19']=xcf_Toe_Model_M19

#(C73-$B$8)/COS(RADIANS($D19))
def xcf_Toe_Model_N19(): 
    try:      
        return (vcell("C73","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N19')
    return None
xfunctions['Toe Model!N19']=xcf_Toe_Model_N19

#(C73-$B$9)/COS(RADIANS($D19))
def xcf_Toe_Model_O19(): 
    try:      
        return (vcell("C73","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O19')
    return None
xfunctions['Toe Model!O19']=xcf_Toe_Model_O19

#(C73+999999)/COS(RADIANS($D19))
def xcf_Toe_Model_P19(): 
    try:      
        return (vcell("C73","Toe Model")+999999)/COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P19')
    return None
xfunctions['Toe Model!P19']=xcf_Toe_Model_P19

#Calculations!F7
def xcf_Toe_Model_B20(): 
    try:      
        return vcell("Calculations!F7","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B20')
    return None
xfunctions['Toe Model!B20']=xcf_Toe_Model_B20

#Calculations!G7
def xcf_Toe_Model_C20(): 
    try:      
        return vcell("Calculations!G7","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C20')
    return None
xfunctions['Toe Model!C20']=xcf_Toe_Model_C20

#IF(AND((B19>B21),(C21>C19)),360,DEGREES(ATAN(MAX((C19-C21),1E-20)/MAX((B21-B19),(1E-20)))))
def xcf_Toe_Model_D20(): 
    try:      
        return IF(AND((vcell("B19","Toe Model")>vcell("B21","Toe Model")),(vcell("C21","Toe Model")>vcell("C19","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C19","Toe Model")-vcell("C21","Toe Model")),1E-20)/MAX((vcell("B21","Toe Model")-vcell("B19","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D20')
    return None
xfunctions['Toe Model!D20']=xcf_Toe_Model_D20

#IF($C20>$B$5,1,IF(AND($C20<=$B$5,$C20>$B$6),2,IF(AND($C20<=$B$6,$C20>$B$7),3,IF(AND($C20<=$B$7,$C20>$B$8),4,IF(AND($C20<=$B$8,$C20>$B$9),5,6)))))
def xcf_Toe_Model_E20(): 
    try:      
        return IF(vcell("$C20","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C20","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C20","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C20","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C20","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C20","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C20","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C20","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C20","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E20')
    return None
xfunctions['Toe Model!E20']=xcf_Toe_Model_E20

#MAX(((B39-B40)*(B34*(C33-C34)+C40*(B33-B34)-C34*(B33-B34))-B40*(C39-C40)*(B33-B34))/((C33-C34)*(B39-B40)-(B33-B34)*(C39-C40)),B34)
def xcf_Toe_Model_G20(): 
    try:      
        return MAX(((vcell("B39","Toe Model")-vcell("B40","Toe Model"))*(vcell("B34","Toe Model")*(vcell("C33","Toe Model")-vcell("C34","Toe Model"))+vcell("C40","Toe Model")*(vcell("B33","Toe Model")-vcell("B34","Toe Model"))-vcell("C34","Toe Model")*(vcell("B33","Toe Model")-vcell("B34","Toe Model")))-vcell("B40","Toe Model")*(vcell("C39","Toe Model")-vcell("C40","Toe Model"))*(vcell("B33","Toe Model")-vcell("B34","Toe Model")))/((vcell("C33","Toe Model")-vcell("C34","Toe Model"))*(vcell("B39","Toe Model")-vcell("B40","Toe Model"))-(vcell("B33","Toe Model")-vcell("B34","Toe Model"))*(vcell("C39","Toe Model")-vcell("C40","Toe Model"))),vcell("B34","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G20')
    return None
xfunctions['Toe Model!G20']=xcf_Toe_Model_G20

#C39+(G20-B39)*(C40-C39)/(B40-B39)
def xcf_Toe_Model_H20(): 
    try:      
        return vcell("C39","Toe Model")+(vcell("G20","Toe Model")-vcell("B39","Toe Model"))*(vcell("C40","Toe Model")-vcell("C39","Toe Model"))/(vcell("B40","Toe Model")-vcell("B39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H20')
    return None
xfunctions['Toe Model!H20']=xcf_Toe_Model_H20

#(C74-$B$5)/COS(RADIANS($D20))
def xcf_Toe_Model_K20(): 
    try:      
        return (vcell("C74","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K20')
    return None
xfunctions['Toe Model!K20']=xcf_Toe_Model_K20

#(C74-$B$6)/COS(RADIANS($D20))
def xcf_Toe_Model_L20(): 
    try:      
        return (vcell("C74","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L20')
    return None
xfunctions['Toe Model!L20']=xcf_Toe_Model_L20

#(C74-$B$7)/COS(RADIANS($D20))
def xcf_Toe_Model_M20(): 
    try:      
        return (vcell("C74","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M20')
    return None
xfunctions['Toe Model!M20']=xcf_Toe_Model_M20

#(C74-$B$8)/COS(RADIANS($D20))
def xcf_Toe_Model_N20(): 
    try:      
        return (vcell("C74","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N20')
    return None
xfunctions['Toe Model!N20']=xcf_Toe_Model_N20

#(C74-$B$9)/COS(RADIANS($D20))
def xcf_Toe_Model_O20(): 
    try:      
        return (vcell("C74","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O20')
    return None
xfunctions['Toe Model!O20']=xcf_Toe_Model_O20

#(C74+999999)/COS(RADIANS($D20))
def xcf_Toe_Model_P20(): 
    try:      
        return (vcell("C74","Toe Model")+999999)/COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P20')
    return None
xfunctions['Toe Model!P20']=xcf_Toe_Model_P20

#Calculations!F8
def xcf_Toe_Model_B21(): 
    try:      
        return vcell("Calculations!F8","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B21')
    return None
xfunctions['Toe Model!B21']=xcf_Toe_Model_B21

#Calculations!G8
def xcf_Toe_Model_C21(): 
    try:      
        return vcell("Calculations!G8","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C21')
    return None
xfunctions['Toe Model!C21']=xcf_Toe_Model_C21

#IF(AND((B20>B22),(C22>C20)),360,DEGREES(ATAN(MAX((C20-C22),1E-20)/MAX((B22-B20),(1E-20)))))
def xcf_Toe_Model_D21(): 
    try:      
        return IF(AND((vcell("B20","Toe Model")>vcell("B22","Toe Model")),(vcell("C22","Toe Model")>vcell("C20","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C20","Toe Model")-vcell("C22","Toe Model")),1E-20)/MAX((vcell("B22","Toe Model")-vcell("B20","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D21')
    return None
xfunctions['Toe Model!D21']=xcf_Toe_Model_D21

#IF($C21>$B$5,1,IF(AND($C21<=$B$5,$C21>$B$6),2,IF(AND($C21<=$B$6,$C21>$B$7),3,IF(AND($C21<=$B$7,$C21>$B$8),4,IF(AND($C21<=$B$8,$C21>$B$9),5,6)))))
def xcf_Toe_Model_E21(): 
    try:      
        return IF(vcell("$C21","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C21","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C21","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C21","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C21","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C21","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C21","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C21","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C21","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E21')
    return None
xfunctions['Toe Model!E21']=xcf_Toe_Model_E21

#B39
def xcf_Toe_Model_G21(): 
    try:      
        return vcell("B39","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G21')
    return None
xfunctions['Toe Model!G21']=xcf_Toe_Model_G21

#C39
def xcf_Toe_Model_H21(): 
    try:      
        return vcell("C39","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H21')
    return None
xfunctions['Toe Model!H21']=xcf_Toe_Model_H21

#(C75-$B$5)/COS(RADIANS($D21))
def xcf_Toe_Model_K21(): 
    try:      
        return (vcell("C75","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K21')
    return None
xfunctions['Toe Model!K21']=xcf_Toe_Model_K21

#(C75-$B$6)/COS(RADIANS($D21))
def xcf_Toe_Model_L21(): 
    try:      
        return (vcell("C75","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L21')
    return None
xfunctions['Toe Model!L21']=xcf_Toe_Model_L21

#(C75-$B$7)/COS(RADIANS($D21))
def xcf_Toe_Model_M21(): 
    try:      
        return (vcell("C75","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M21')
    return None
xfunctions['Toe Model!M21']=xcf_Toe_Model_M21

#(C75-$B$8)/COS(RADIANS($D21))
def xcf_Toe_Model_N21(): 
    try:      
        return (vcell("C75","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N21')
    return None
xfunctions['Toe Model!N21']=xcf_Toe_Model_N21

#(C75-$B$9)/COS(RADIANS($D21))
def xcf_Toe_Model_O21(): 
    try:      
        return (vcell("C75","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O21')
    return None
xfunctions['Toe Model!O21']=xcf_Toe_Model_O21

#(C75+999999)/COS(RADIANS($D21))
def xcf_Toe_Model_P21(): 
    try:      
        return (vcell("C75","Toe Model")+999999)/COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P21')
    return None
xfunctions['Toe Model!P21']=xcf_Toe_Model_P21

#Calculations!F9
def xcf_Toe_Model_B22(): 
    try:      
        return vcell("Calculations!F9","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B22')
    return None
xfunctions['Toe Model!B22']=xcf_Toe_Model_B22

#Calculations!G9
def xcf_Toe_Model_C22(): 
    try:      
        return vcell("Calculations!G9","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C22')
    return None
xfunctions['Toe Model!C22']=xcf_Toe_Model_C22

#IF(AND((B21>B23),(C23>C21)),360,DEGREES(ATAN(MAX((C21-C23),1E-20)/MAX((B23-B21),(1E-20)))))
def xcf_Toe_Model_D22(): 
    try:      
        return IF(AND((vcell("B21","Toe Model")>vcell("B23","Toe Model")),(vcell("C23","Toe Model")>vcell("C21","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C21","Toe Model")-vcell("C23","Toe Model")),1E-20)/MAX((vcell("B23","Toe Model")-vcell("B21","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D22')
    return None
xfunctions['Toe Model!D22']=xcf_Toe_Model_D22

#IF($C22>$B$5,1,IF(AND($C22<=$B$5,$C22>$B$6),2,IF(AND($C22<=$B$6,$C22>$B$7),3,IF(AND($C22<=$B$7,$C22>$B$8),4,IF(AND($C22<=$B$8,$C22>$B$9),5,6)))))
def xcf_Toe_Model_E22(): 
    try:      
        return IF(vcell("$C22","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C22","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C22","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C22","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C22","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C22","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C22","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C22","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C22","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E22')
    return None
xfunctions['Toe Model!E22']=xcf_Toe_Model_E22

#(C76-$B$5)/COS(RADIANS($D22))
def xcf_Toe_Model_K22(): 
    try:      
        return (vcell("C76","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K22')
    return None
xfunctions['Toe Model!K22']=xcf_Toe_Model_K22

#(C76-$B$6)/COS(RADIANS($D22))
def xcf_Toe_Model_L22(): 
    try:      
        return (vcell("C76","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L22')
    return None
xfunctions['Toe Model!L22']=xcf_Toe_Model_L22

#(C76-$B$7)/COS(RADIANS($D22))
def xcf_Toe_Model_M22(): 
    try:      
        return (vcell("C76","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M22')
    return None
xfunctions['Toe Model!M22']=xcf_Toe_Model_M22

#(C76-$B$8)/COS(RADIANS($D22))
def xcf_Toe_Model_N22(): 
    try:      
        return (vcell("C76","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N22')
    return None
xfunctions['Toe Model!N22']=xcf_Toe_Model_N22

#(C76-$B$9)/COS(RADIANS($D22))
def xcf_Toe_Model_O22(): 
    try:      
        return (vcell("C76","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O22')
    return None
xfunctions['Toe Model!O22']=xcf_Toe_Model_O22

#(C76+999999)/COS(RADIANS($D22))
def xcf_Toe_Model_P22(): 
    try:      
        return (vcell("C76","Toe Model")+999999)/COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P22')
    return None
xfunctions['Toe Model!P22']=xcf_Toe_Model_P22

#Calculations!F10
def xcf_Toe_Model_B23(): 
    try:      
        return vcell("Calculations!F10","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B23')
    return None
xfunctions['Toe Model!B23']=xcf_Toe_Model_B23

#Calculations!G10
def xcf_Toe_Model_C23(): 
    try:      
        return vcell("Calculations!G10","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C23')
    return None
xfunctions['Toe Model!C23']=xcf_Toe_Model_C23

#IF(AND((B22>B24),(C24>C22)),360,DEGREES(ATAN(MAX((C22-C24),1E-20)/MAX((B24-B22),(1E-20)))))
def xcf_Toe_Model_D23(): 
    try:      
        return IF(AND((vcell("B22","Toe Model")>vcell("B24","Toe Model")),(vcell("C24","Toe Model")>vcell("C22","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C22","Toe Model")-vcell("C24","Toe Model")),1E-20)/MAX((vcell("B24","Toe Model")-vcell("B22","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D23')
    return None
xfunctions['Toe Model!D23']=xcf_Toe_Model_D23

#IF($C23>$B$5,1,IF(AND($C23<=$B$5,$C23>$B$6),2,IF(AND($C23<=$B$6,$C23>$B$7),3,IF(AND($C23<=$B$7,$C23>$B$8),4,IF(AND($C23<=$B$8,$C23>$B$9),5,6)))))
def xcf_Toe_Model_E23(): 
    try:      
        return IF(vcell("$C23","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C23","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C23","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C23","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C23","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C23","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C23","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C23","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C23","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E23')
    return None
xfunctions['Toe Model!E23']=xcf_Toe_Model_E23

#(C77-$B$5)/COS(RADIANS($D23))
def xcf_Toe_Model_K23(): 
    try:      
        return (vcell("C77","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K23')
    return None
xfunctions['Toe Model!K23']=xcf_Toe_Model_K23

#(C77-$B$6)/COS(RADIANS($D23))
def xcf_Toe_Model_L23(): 
    try:      
        return (vcell("C77","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L23')
    return None
xfunctions['Toe Model!L23']=xcf_Toe_Model_L23

#(C77-$B$7)/COS(RADIANS($D23))
def xcf_Toe_Model_M23(): 
    try:      
        return (vcell("C77","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M23')
    return None
xfunctions['Toe Model!M23']=xcf_Toe_Model_M23

#(C77-$B$8)/COS(RADIANS($D23))
def xcf_Toe_Model_N23(): 
    try:      
        return (vcell("C77","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N23')
    return None
xfunctions['Toe Model!N23']=xcf_Toe_Model_N23

#(C77-$B$9)/COS(RADIANS($D23))
def xcf_Toe_Model_O23(): 
    try:      
        return (vcell("C77","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O23')
    return None
xfunctions['Toe Model!O23']=xcf_Toe_Model_O23

#(C77+999999)/COS(RADIANS($D23))
def xcf_Toe_Model_P23(): 
    try:      
        return (vcell("C77","Toe Model")+999999)/COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P23')
    return None
xfunctions['Toe Model!P23']=xcf_Toe_Model_P23

#Calculations!F11
def xcf_Toe_Model_B24(): 
    try:      
        return vcell("Calculations!F11","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B24')
    return None
xfunctions['Toe Model!B24']=xcf_Toe_Model_B24

#Calculations!G11
def xcf_Toe_Model_C24(): 
    try:      
        return vcell("Calculations!G11","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C24')
    return None
xfunctions['Toe Model!C24']=xcf_Toe_Model_C24

#IF(AND((B23>B25),(C25>C23)),360,DEGREES(ATAN(MAX((C23-C25),1E-20)/MAX((B25-B23),(1E-20)))))
def xcf_Toe_Model_D24(): 
    try:      
        return IF(AND((vcell("B23","Toe Model")>vcell("B25","Toe Model")),(vcell("C25","Toe Model")>vcell("C23","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C23","Toe Model")-vcell("C25","Toe Model")),1E-20)/MAX((vcell("B25","Toe Model")-vcell("B23","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D24')
    return None
xfunctions['Toe Model!D24']=xcf_Toe_Model_D24

#IF($C24>$B$5,1,IF(AND($C24<=$B$5,$C24>$B$6),2,IF(AND($C24<=$B$6,$C24>$B$7),3,IF(AND($C24<=$B$7,$C24>$B$8),4,IF(AND($C24<=$B$8,$C24>$B$9),5,6)))))
def xcf_Toe_Model_E24(): 
    try:      
        return IF(vcell("$C24","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C24","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C24","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C24","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C24","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C24","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C24","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C24","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C24","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E24')
    return None
xfunctions['Toe Model!E24']=xcf_Toe_Model_E24

#(C78-$B$5)/COS(RADIANS($D24))
def xcf_Toe_Model_K24(): 
    try:      
        return (vcell("C78","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K24')
    return None
xfunctions['Toe Model!K24']=xcf_Toe_Model_K24

#(C78-$B$6)/COS(RADIANS($D24))
def xcf_Toe_Model_L24(): 
    try:      
        return (vcell("C78","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L24')
    return None
xfunctions['Toe Model!L24']=xcf_Toe_Model_L24

#(C78-$B$7)/COS(RADIANS($D24))
def xcf_Toe_Model_M24(): 
    try:      
        return (vcell("C78","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M24')
    return None
xfunctions['Toe Model!M24']=xcf_Toe_Model_M24

#(C78-$B$8)/COS(RADIANS($D24))
def xcf_Toe_Model_N24(): 
    try:      
        return (vcell("C78","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N24')
    return None
xfunctions['Toe Model!N24']=xcf_Toe_Model_N24

#(C78-$B$9)/COS(RADIANS($D24))
def xcf_Toe_Model_O24(): 
    try:      
        return (vcell("C78","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O24')
    return None
xfunctions['Toe Model!O24']=xcf_Toe_Model_O24

#(C78+999999)/COS(RADIANS($D24))
def xcf_Toe_Model_P24(): 
    try:      
        return (vcell("C78","Toe Model")+999999)/COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P24')
    return None
xfunctions['Toe Model!P24']=xcf_Toe_Model_P24

#Calculations!F12
def xcf_Toe_Model_B25(): 
    try:      
        return vcell("Calculations!F12","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B25')
    return None
xfunctions['Toe Model!B25']=xcf_Toe_Model_B25

#Calculations!G12
def xcf_Toe_Model_C25(): 
    try:      
        return vcell("Calculations!G12","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C25')
    return None
xfunctions['Toe Model!C25']=xcf_Toe_Model_C25

#IF(AND((B24>B26),(C26>C24)),360,DEGREES(ATAN(MAX((C24-C26),1E-20)/MAX((B26-B24),(1E-20)))))
def xcf_Toe_Model_D25(): 
    try:      
        return IF(AND((vcell("B24","Toe Model")>vcell("B26","Toe Model")),(vcell("C26","Toe Model")>vcell("C24","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C24","Toe Model")-vcell("C26","Toe Model")),1E-20)/MAX((vcell("B26","Toe Model")-vcell("B24","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D25')
    return None
xfunctions['Toe Model!D25']=xcf_Toe_Model_D25

#IF($C25>$B$5,1,IF(AND($C25<=$B$5,$C25>$B$6),2,IF(AND($C25<=$B$6,$C25>$B$7),3,IF(AND($C25<=$B$7,$C25>$B$8),4,IF(AND($C25<=$B$8,$C25>$B$9),5,6)))))
def xcf_Toe_Model_E25(): 
    try:      
        return IF(vcell("$C25","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C25","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C25","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C25","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C25","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C25","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C25","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C25","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C25","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E25')
    return None
xfunctions['Toe Model!E25']=xcf_Toe_Model_E25

#MAX(((C35-C$34)*(B$33-B$34)*(C$34-C$39)+B$34*(C$33-C$34)*(C$34-C$39)-B35*(B$39-B$34)*(B$33-B$34))/((C$33-C$34)*(C$34-C$39)-(B$39-B$34)*(B$33-B$34)),((C35-C$40)*(B$39-B$40)*(C$34-C$39)+B$40*(C$39-C$40)*(C$34-C$39)-B35*(B$39-B$34)*(B$39-B$40))/((C$39-C$40)*(C$34-C$39)-(B$39-B$34)*(B$39-B$40)))
def xcf_Toe_Model_G25(): 
    try:      
        return MAX(((vcell("C35","Toe Model")-vcell("C$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$34","Toe Model")*(vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B35","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model")))/((vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))),((vcell("C35","Toe Model")-vcell("C$40","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$40","Toe Model")*(vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B35","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model")))/((vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G25')
    return None
xfunctions['Toe Model!G25']=xcf_Toe_Model_G25

#C35+(B$39-B$34)*(G25-B35)/(C$34-C$39)
def xcf_Toe_Model_H25(): 
    try:      
        return vcell("C35","Toe Model")+(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("G25","Toe Model")-vcell("B35","Toe Model"))/(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H25')
    return None
xfunctions['Toe Model!H25']=xcf_Toe_Model_H25

#(C79-$B$5)/COS(RADIANS($D25))
def xcf_Toe_Model_K25(): 
    try:      
        return (vcell("C79","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K25')
    return None
xfunctions['Toe Model!K25']=xcf_Toe_Model_K25

#(C79-$B$6)/COS(RADIANS($D25))
def xcf_Toe_Model_L25(): 
    try:      
        return (vcell("C79","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L25')
    return None
xfunctions['Toe Model!L25']=xcf_Toe_Model_L25

#(C79-$B$7)/COS(RADIANS($D25))
def xcf_Toe_Model_M25(): 
    try:      
        return (vcell("C79","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M25')
    return None
xfunctions['Toe Model!M25']=xcf_Toe_Model_M25

#(C79-$B$8)/COS(RADIANS($D25))
def xcf_Toe_Model_N25(): 
    try:      
        return (vcell("C79","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N25')
    return None
xfunctions['Toe Model!N25']=xcf_Toe_Model_N25

#(C79-$B$9)/COS(RADIANS($D25))
def xcf_Toe_Model_O25(): 
    try:      
        return (vcell("C79","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O25')
    return None
xfunctions['Toe Model!O25']=xcf_Toe_Model_O25

#(C79+999999)/COS(RADIANS($D25))
def xcf_Toe_Model_P25(): 
    try:      
        return (vcell("C79","Toe Model")+999999)/COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P25')
    return None
xfunctions['Toe Model!P25']=xcf_Toe_Model_P25

#Calculations!F13
def xcf_Toe_Model_B26(): 
    try:      
        return vcell("Calculations!F13","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B26')
    return None
xfunctions['Toe Model!B26']=xcf_Toe_Model_B26

#Calculations!G13
def xcf_Toe_Model_C26(): 
    try:      
        return vcell("Calculations!G13","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C26')
    return None
xfunctions['Toe Model!C26']=xcf_Toe_Model_C26

#IF(AND((B25>B27),(C27>C25)),360,DEGREES(ATAN(MAX((C25-C27),1E-20)/MAX((B27-B25),(1E-20)))))
def xcf_Toe_Model_D26(): 
    try:      
        return IF(AND((vcell("B25","Toe Model")>vcell("B27","Toe Model")),(vcell("C27","Toe Model")>vcell("C25","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C25","Toe Model")-vcell("C27","Toe Model")),1E-20)/MAX((vcell("B27","Toe Model")-vcell("B25","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D26')
    return None
xfunctions['Toe Model!D26']=xcf_Toe_Model_D26

#IF($C26>$B$5,1,IF(AND($C26<=$B$5,$C26>$B$6),2,IF(AND($C26<=$B$6,$C26>$B$7),3,IF(AND($C26<=$B$7,$C26>$B$8),4,IF(AND($C26<=$B$8,$C26>$B$9),5,6)))))
def xcf_Toe_Model_E26(): 
    try:      
        return IF(vcell("$C26","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C26","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C26","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C26","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C26","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C26","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C26","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C26","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C26","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E26')
    return None
xfunctions['Toe Model!E26']=xcf_Toe_Model_E26

#MAX(((C36-C$34)*(B$33-B$34)*(C$34-C$39)+B$34*(C$33-C$34)*(C$34-C$39)-B36*(B$39-B$34)*(B$33-B$34))/((C$33-C$34)*(C$34-C$39)-(B$39-B$34)*(B$33-B$34)),((C36-C$40)*(B$39-B$40)*(C$34-C$39)+B$40*(C$39-C$40)*(C$34-C$39)-B36*(B$39-B$34)*(B$39-B$40))/((C$39-C$40)*(C$34-C$39)-(B$39-B$34)*(B$39-B$40)))
def xcf_Toe_Model_G26(): 
    try:      
        return MAX(((vcell("C36","Toe Model")-vcell("C$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$34","Toe Model")*(vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B36","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model")))/((vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))),((vcell("C36","Toe Model")-vcell("C$40","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$40","Toe Model")*(vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B36","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model")))/((vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G26')
    return None
xfunctions['Toe Model!G26']=xcf_Toe_Model_G26

#C36+(B$39-B$34)*(G26-B36)/(C$34-C$39)
def xcf_Toe_Model_H26(): 
    try:      
        return vcell("C36","Toe Model")+(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("G26","Toe Model")-vcell("B36","Toe Model"))/(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H26')
    return None
xfunctions['Toe Model!H26']=xcf_Toe_Model_H26

#(C80-$B$5)/COS(RADIANS($D26))
def xcf_Toe_Model_K26(): 
    try:      
        return (vcell("C80","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K26')
    return None
xfunctions['Toe Model!K26']=xcf_Toe_Model_K26

#(C80-$B$6)/COS(RADIANS($D26))
def xcf_Toe_Model_L26(): 
    try:      
        return (vcell("C80","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L26')
    return None
xfunctions['Toe Model!L26']=xcf_Toe_Model_L26

#(C80-$B$7)/COS(RADIANS($D26))
def xcf_Toe_Model_M26(): 
    try:      
        return (vcell("C80","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M26')
    return None
xfunctions['Toe Model!M26']=xcf_Toe_Model_M26

#(C80-$B$8)/COS(RADIANS($D26))
def xcf_Toe_Model_N26(): 
    try:      
        return (vcell("C80","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N26')
    return None
xfunctions['Toe Model!N26']=xcf_Toe_Model_N26

#(C80-$B$9)/COS(RADIANS($D26))
def xcf_Toe_Model_O26(): 
    try:      
        return (vcell("C80","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O26')
    return None
xfunctions['Toe Model!O26']=xcf_Toe_Model_O26

#(C80+999999)/COS(RADIANS($D26))
def xcf_Toe_Model_P26(): 
    try:      
        return (vcell("C80","Toe Model")+999999)/COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P26')
    return None
xfunctions['Toe Model!P26']=xcf_Toe_Model_P26

#Calculations!F14
def xcf_Toe_Model_B27(): 
    try:      
        return vcell("Calculations!F14","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B27')
    return None
xfunctions['Toe Model!B27']=xcf_Toe_Model_B27

#Calculations!G14
def xcf_Toe_Model_C27(): 
    try:      
        return vcell("Calculations!G14","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C27')
    return None
xfunctions['Toe Model!C27']=xcf_Toe_Model_C27

#IF(AND((B26>B28),(C28>C26)),360,DEGREES(ATAN(MAX((C26-C28),1E-20)/MAX((B28-B26),(1E-20)))))
def xcf_Toe_Model_D27(): 
    try:      
        return IF(AND((vcell("B26","Toe Model")>vcell("B28","Toe Model")),(vcell("C28","Toe Model")>vcell("C26","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C26","Toe Model")-vcell("C28","Toe Model")),1E-20)/MAX((vcell("B28","Toe Model")-vcell("B26","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D27')
    return None
xfunctions['Toe Model!D27']=xcf_Toe_Model_D27

#IF($C27>$B$5,1,IF(AND($C27<=$B$5,$C27>$B$6),2,IF(AND($C27<=$B$6,$C27>$B$7),3,IF(AND($C27<=$B$7,$C27>$B$8),4,IF(AND($C27<=$B$8,$C27>$B$9),5,6)))))
def xcf_Toe_Model_E27(): 
    try:      
        return IF(vcell("$C27","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C27","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C27","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C27","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C27","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C27","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C27","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C27","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C27","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E27')
    return None
xfunctions['Toe Model!E27']=xcf_Toe_Model_E27

#MAX(((C37-C$34)*(B$33-B$34)*(C$34-C$39)+B$34*(C$33-C$34)*(C$34-C$39)-B37*(B$39-B$34)*(B$33-B$34))/((C$33-C$34)*(C$34-C$39)-(B$39-B$34)*(B$33-B$34)),((C37-C$40)*(B$39-B$40)*(C$34-C$39)+B$40*(C$39-C$40)*(C$34-C$39)-B37*(B$39-B$34)*(B$39-B$40))/((C$39-C$40)*(C$34-C$39)-(B$39-B$34)*(B$39-B$40)))
def xcf_Toe_Model_G27(): 
    try:      
        return MAX(((vcell("C37","Toe Model")-vcell("C$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$34","Toe Model")*(vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B37","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model")))/((vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))),((vcell("C37","Toe Model")-vcell("C$40","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$40","Toe Model")*(vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B37","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model")))/((vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G27')
    return None
xfunctions['Toe Model!G27']=xcf_Toe_Model_G27

#C37+(B$39-B$34)*(G27-B37)/(C$34-C$39)
def xcf_Toe_Model_H27(): 
    try:      
        return vcell("C37","Toe Model")+(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("G27","Toe Model")-vcell("B37","Toe Model"))/(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H27')
    return None
xfunctions['Toe Model!H27']=xcf_Toe_Model_H27

#(C81-$B$5)/COS(RADIANS($D27))
def xcf_Toe_Model_K27(): 
    try:      
        return (vcell("C81","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K27')
    return None
xfunctions['Toe Model!K27']=xcf_Toe_Model_K27

#(C81-$B$6)/COS(RADIANS($D27))
def xcf_Toe_Model_L27(): 
    try:      
        return (vcell("C81","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L27')
    return None
xfunctions['Toe Model!L27']=xcf_Toe_Model_L27

#(C81-$B$7)/COS(RADIANS($D27))
def xcf_Toe_Model_M27(): 
    try:      
        return (vcell("C81","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M27')
    return None
xfunctions['Toe Model!M27']=xcf_Toe_Model_M27

#(C81-$B$8)/COS(RADIANS($D27))
def xcf_Toe_Model_N27(): 
    try:      
        return (vcell("C81","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N27')
    return None
xfunctions['Toe Model!N27']=xcf_Toe_Model_N27

#(C81-$B$9)/COS(RADIANS($D27))
def xcf_Toe_Model_O27(): 
    try:      
        return (vcell("C81","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O27')
    return None
xfunctions['Toe Model!O27']=xcf_Toe_Model_O27

#(C81+999999)/COS(RADIANS($D27))
def xcf_Toe_Model_P27(): 
    try:      
        return (vcell("C81","Toe Model")+999999)/COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P27')
    return None
xfunctions['Toe Model!P27']=xcf_Toe_Model_P27

#Calculations!F15
def xcf_Toe_Model_B28(): 
    try:      
        return vcell("Calculations!F15","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B28')
    return None
xfunctions['Toe Model!B28']=xcf_Toe_Model_B28

#Calculations!G15
def xcf_Toe_Model_C28(): 
    try:      
        return vcell("Calculations!G15","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C28')
    return None
xfunctions['Toe Model!C28']=xcf_Toe_Model_C28

#IF(AND((B27>B29),(C29>C27)),360,DEGREES(ATAN(MAX((C27-C29),1E-20)/MAX((B29-B27),(1E-20)))))
def xcf_Toe_Model_D28(): 
    try:      
        return IF(AND((vcell("B27","Toe Model")>vcell("B29","Toe Model")),(vcell("C29","Toe Model")>vcell("C27","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C27","Toe Model")-vcell("C29","Toe Model")),1E-20)/MAX((vcell("B29","Toe Model")-vcell("B27","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D28')
    return None
xfunctions['Toe Model!D28']=xcf_Toe_Model_D28

#IF($C28>$B$5,1,IF(AND($C28<=$B$5,$C28>$B$6),2,IF(AND($C28<=$B$6,$C28>$B$7),3,IF(AND($C28<=$B$7,$C28>$B$8),4,IF(AND($C28<=$B$8,$C28>$B$9),5,6)))))
def xcf_Toe_Model_E28(): 
    try:      
        return IF(vcell("$C28","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C28","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C28","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C28","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C28","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C28","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C28","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C28","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C28","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E28')
    return None
xfunctions['Toe Model!E28']=xcf_Toe_Model_E28

#MAX(((C38-C$34)*(B$33-B$34)*(C$34-C$39)+B$34*(C$33-C$34)*(C$34-C$39)-B38*(B$39-B$34)*(B$33-B$34))/((C$33-C$34)*(C$34-C$39)-(B$39-B$34)*(B$33-B$34)),((C38-C$40)*(B$39-B$40)*(C$34-C$39)+B$40*(C$39-C$40)*(C$34-C$39)-B38*(B$39-B$34)*(B$39-B$40))/((C$39-C$40)*(C$34-C$39)-(B$39-B$34)*(B$39-B$40)))
def xcf_Toe_Model_G28(): 
    try:      
        return MAX(((vcell("C38","Toe Model")-vcell("C$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$34","Toe Model")*(vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B38","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model")))/((vcell("C$33","Toe Model")-vcell("C$34","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$33","Toe Model")-vcell("B$34","Toe Model"))),((vcell("C38","Toe Model")-vcell("C$40","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))+vcell("B$40","Toe Model")*(vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-vcell("B38","Toe Model")*(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model")))/((vcell("C$39","Toe Model")-vcell("C$40","Toe Model"))*(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))-(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("B$39","Toe Model")-vcell("B$40","Toe Model"))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G28')
    return None
xfunctions['Toe Model!G28']=xcf_Toe_Model_G28

#C38+(B$39-B$34)*(G28-B38)/(C$34-C$39)
def xcf_Toe_Model_H28(): 
    try:      
        return vcell("C38","Toe Model")+(vcell("B$39","Toe Model")-vcell("B$34","Toe Model"))*(vcell("G28","Toe Model")-vcell("B38","Toe Model"))/(vcell("C$34","Toe Model")-vcell("C$39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H28')
    return None
xfunctions['Toe Model!H28']=xcf_Toe_Model_H28

#(C82-$B$5)/COS(RADIANS($D28))
def xcf_Toe_Model_K28(): 
    try:      
        return (vcell("C82","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K28')
    return None
xfunctions['Toe Model!K28']=xcf_Toe_Model_K28

#(C82-$B$6)/COS(RADIANS($D28))
def xcf_Toe_Model_L28(): 
    try:      
        return (vcell("C82","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L28')
    return None
xfunctions['Toe Model!L28']=xcf_Toe_Model_L28

#(C82-$B$7)/COS(RADIANS($D28))
def xcf_Toe_Model_M28(): 
    try:      
        return (vcell("C82","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M28')
    return None
xfunctions['Toe Model!M28']=xcf_Toe_Model_M28

#(C82-$B$8)/COS(RADIANS($D28))
def xcf_Toe_Model_N28(): 
    try:      
        return (vcell("C82","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N28')
    return None
xfunctions['Toe Model!N28']=xcf_Toe_Model_N28

#(C82-$B$9)/COS(RADIANS($D28))
def xcf_Toe_Model_O28(): 
    try:      
        return (vcell("C82","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O28')
    return None
xfunctions['Toe Model!O28']=xcf_Toe_Model_O28

#(C82+999999)/COS(RADIANS($D28))
def xcf_Toe_Model_P28(): 
    try:      
        return (vcell("C82","Toe Model")+999999)/COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P28')
    return None
xfunctions['Toe Model!P28']=xcf_Toe_Model_P28

#Calculations!F16
def xcf_Toe_Model_B29(): 
    try:      
        return vcell("Calculations!F16","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B29')
    return None
xfunctions['Toe Model!B29']=xcf_Toe_Model_B29

#Calculations!G16
def xcf_Toe_Model_C29(): 
    try:      
        return vcell("Calculations!G16","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C29')
    return None
xfunctions['Toe Model!C29']=xcf_Toe_Model_C29

#IF(AND((B28>B30),(C30>C28)),360,DEGREES(ATAN(MAX((C28-C30),1E-20)/MAX((B30-B28),(1E-20)))))
def xcf_Toe_Model_D29(): 
    try:      
        return IF(AND((vcell("B28","Toe Model")>vcell("B30","Toe Model")),(vcell("C30","Toe Model")>vcell("C28","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C28","Toe Model")-vcell("C30","Toe Model")),1E-20)/MAX((vcell("B30","Toe Model")-vcell("B28","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D29')
    return None
xfunctions['Toe Model!D29']=xcf_Toe_Model_D29

#IF($C29>$B$5,1,IF(AND($C29<=$B$5,$C29>$B$6),2,IF(AND($C29<=$B$6,$C29>$B$7),3,IF(AND($C29<=$B$7,$C29>$B$8),4,IF(AND($C29<=$B$8,$C29>$B$9),5,6)))))
def xcf_Toe_Model_E29(): 
    try:      
        return IF(vcell("$C29","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C29","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C29","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C29","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C29","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C29","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C29","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C29","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C29","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E29')
    return None
xfunctions['Toe Model!E29']=xcf_Toe_Model_E29

#(C83-$B$5)/COS(RADIANS($D29))
def xcf_Toe_Model_K29(): 
    try:      
        return (vcell("C83","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K29')
    return None
xfunctions['Toe Model!K29']=xcf_Toe_Model_K29

#(C83-$B$6)/COS(RADIANS($D29))
def xcf_Toe_Model_L29(): 
    try:      
        return (vcell("C83","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L29')
    return None
xfunctions['Toe Model!L29']=xcf_Toe_Model_L29

#(C83-$B$7)/COS(RADIANS($D29))
def xcf_Toe_Model_M29(): 
    try:      
        return (vcell("C83","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M29')
    return None
xfunctions['Toe Model!M29']=xcf_Toe_Model_M29

#(C83-$B$8)/COS(RADIANS($D29))
def xcf_Toe_Model_N29(): 
    try:      
        return (vcell("C83","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N29')
    return None
xfunctions['Toe Model!N29']=xcf_Toe_Model_N29

#(C83-$B$9)/COS(RADIANS($D29))
def xcf_Toe_Model_O29(): 
    try:      
        return (vcell("C83","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O29')
    return None
xfunctions['Toe Model!O29']=xcf_Toe_Model_O29

#(C83+999999)/COS(RADIANS($D29))
def xcf_Toe_Model_P29(): 
    try:      
        return (vcell("C83","Toe Model")+999999)/COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P29')
    return None
xfunctions['Toe Model!P29']=xcf_Toe_Model_P29

#Calculations!F17
def xcf_Toe_Model_B30(): 
    try:      
        return vcell("Calculations!F17","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B30')
    return None
xfunctions['Toe Model!B30']=xcf_Toe_Model_B30

#Calculations!G17
def xcf_Toe_Model_C30(): 
    try:      
        return vcell("Calculations!G17","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C30')
    return None
xfunctions['Toe Model!C30']=xcf_Toe_Model_C30

#IF(AND((B29>B31),(C31>C29)),360,DEGREES(ATAN(MAX((C29-C31),1E-20)/MAX((B31-B29),(1E-20)))))
def xcf_Toe_Model_D30(): 
    try:      
        return IF(AND((vcell("B29","Toe Model")>vcell("B31","Toe Model")),(vcell("C31","Toe Model")>vcell("C29","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C29","Toe Model")-vcell("C31","Toe Model")),1E-20)/MAX((vcell("B31","Toe Model")-vcell("B29","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D30')
    return None
xfunctions['Toe Model!D30']=xcf_Toe_Model_D30

#IF($C30>$B$5,1,IF(AND($C30<=$B$5,$C30>$B$6),2,IF(AND($C30<=$B$6,$C30>$B$7),3,IF(AND($C30<=$B$7,$C30>$B$8),4,IF(AND($C30<=$B$8,$C30>$B$9),5,6)))))
def xcf_Toe_Model_E30(): 
    try:      
        return IF(vcell("$C30","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C30","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C30","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C30","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C30","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C30","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C30","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C30","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C30","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E30')
    return None
xfunctions['Toe Model!E30']=xcf_Toe_Model_E30

#(C84-$B$5)/COS(RADIANS($D30))
def xcf_Toe_Model_K30(): 
    try:      
        return (vcell("C84","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K30')
    return None
xfunctions['Toe Model!K30']=xcf_Toe_Model_K30

#(C84-$B$6)/COS(RADIANS($D30))
def xcf_Toe_Model_L30(): 
    try:      
        return (vcell("C84","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L30')
    return None
xfunctions['Toe Model!L30']=xcf_Toe_Model_L30

#(C84-$B$7)/COS(RADIANS($D30))
def xcf_Toe_Model_M30(): 
    try:      
        return (vcell("C84","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M30')
    return None
xfunctions['Toe Model!M30']=xcf_Toe_Model_M30

#(C84-$B$8)/COS(RADIANS($D30))
def xcf_Toe_Model_N30(): 
    try:      
        return (vcell("C84","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N30')
    return None
xfunctions['Toe Model!N30']=xcf_Toe_Model_N30

#(C84-$B$9)/COS(RADIANS($D30))
def xcf_Toe_Model_O30(): 
    try:      
        return (vcell("C84","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O30')
    return None
xfunctions['Toe Model!O30']=xcf_Toe_Model_O30

#(C84+999999)/COS(RADIANS($D30))
def xcf_Toe_Model_P30(): 
    try:      
        return (vcell("C84","Toe Model")+999999)/COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P30')
    return None
xfunctions['Toe Model!P30']=xcf_Toe_Model_P30

#Calculations!F18
def xcf_Toe_Model_B31(): 
    try:      
        return vcell("Calculations!F18","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B31')
    return None
xfunctions['Toe Model!B31']=xcf_Toe_Model_B31

#Calculations!G18
def xcf_Toe_Model_C31(): 
    try:      
        return vcell("Calculations!G18","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C31')
    return None
xfunctions['Toe Model!C31']=xcf_Toe_Model_C31

#IF(AND((B30>B32),(C32>C30)),360,DEGREES(ATAN(MAX((C30-C32),1E-20)/MAX((B32-B30),(1E-20)))))
def xcf_Toe_Model_D31(): 
    try:      
        return IF(AND((vcell("B30","Toe Model")>vcell("B32","Toe Model")),(vcell("C32","Toe Model")>vcell("C30","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C30","Toe Model")-vcell("C32","Toe Model")),1E-20)/MAX((vcell("B32","Toe Model")-vcell("B30","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D31')
    return None
xfunctions['Toe Model!D31']=xcf_Toe_Model_D31

#IF($C31>$B$5,1,IF(AND($C31<=$B$5,$C31>$B$6),2,IF(AND($C31<=$B$6,$C31>$B$7),3,IF(AND($C31<=$B$7,$C31>$B$8),4,IF(AND($C31<=$B$8,$C31>$B$9),5,6)))))
def xcf_Toe_Model_E31(): 
    try:      
        return IF(vcell("$C31","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C31","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C31","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C31","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C31","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C31","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C31","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C31","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C31","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E31')
    return None
xfunctions['Toe Model!E31']=xcf_Toe_Model_E31

#(C85-$B$5)/COS(RADIANS($D31))
def xcf_Toe_Model_K31(): 
    try:      
        return (vcell("C85","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K31')
    return None
xfunctions['Toe Model!K31']=xcf_Toe_Model_K31

#(C85-$B$6)/COS(RADIANS($D31))
def xcf_Toe_Model_L31(): 
    try:      
        return (vcell("C85","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L31')
    return None
xfunctions['Toe Model!L31']=xcf_Toe_Model_L31

#(C85-$B$7)/COS(RADIANS($D31))
def xcf_Toe_Model_M31(): 
    try:      
        return (vcell("C85","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M31')
    return None
xfunctions['Toe Model!M31']=xcf_Toe_Model_M31

#(C85-$B$8)/COS(RADIANS($D31))
def xcf_Toe_Model_N31(): 
    try:      
        return (vcell("C85","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N31')
    return None
xfunctions['Toe Model!N31']=xcf_Toe_Model_N31

#(C85-$B$9)/COS(RADIANS($D31))
def xcf_Toe_Model_O31(): 
    try:      
        return (vcell("C85","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O31')
    return None
xfunctions['Toe Model!O31']=xcf_Toe_Model_O31

#(C85+999999)/COS(RADIANS($D31))
def xcf_Toe_Model_P31(): 
    try:      
        return (vcell("C85","Toe Model")+999999)/COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P31')
    return None
xfunctions['Toe Model!P31']=xcf_Toe_Model_P31

#Calculations!F19
def xcf_Toe_Model_B32(): 
    try:      
        return vcell("Calculations!F19","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B32')
    return None
xfunctions['Toe Model!B32']=xcf_Toe_Model_B32

#Calculations!G19
def xcf_Toe_Model_C32(): 
    try:      
        return vcell("Calculations!G19","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C32')
    return None
xfunctions['Toe Model!C32']=xcf_Toe_Model_C32

#IF(AND((B31>B33),(C33>C31)),360,DEGREES(ATAN(MAX((C31-C33),1E-20)/MAX((B33-B31),(1E-20)))))
def xcf_Toe_Model_D32(): 
    try:      
        return IF(AND((vcell("B31","Toe Model")>vcell("B33","Toe Model")),(vcell("C33","Toe Model")>vcell("C31","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C31","Toe Model")-vcell("C33","Toe Model")),1E-20)/MAX((vcell("B33","Toe Model")-vcell("B31","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D32')
    return None
xfunctions['Toe Model!D32']=xcf_Toe_Model_D32

#IF($C32>$B$5,1,IF(AND($C32<=$B$5,$C32>$B$6),2,IF(AND($C32<=$B$6,$C32>$B$7),3,IF(AND($C32<=$B$7,$C32>$B$8),4,IF(AND($C32<=$B$8,$C32>$B$9),5,6)))))
def xcf_Toe_Model_E32(): 
    try:      
        return IF(vcell("$C32","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C32","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C32","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C32","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C32","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C32","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C32","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C32","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C32","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E32')
    return None
xfunctions['Toe Model!E32']=xcf_Toe_Model_E32

#(C86-$B$5)/COS(RADIANS($D32))
def xcf_Toe_Model_K32(): 
    try:      
        return (vcell("C86","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K32')
    return None
xfunctions['Toe Model!K32']=xcf_Toe_Model_K32

#(C86-$B$6)/COS(RADIANS($D32))
def xcf_Toe_Model_L32(): 
    try:      
        return (vcell("C86","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L32')
    return None
xfunctions['Toe Model!L32']=xcf_Toe_Model_L32

#(C86-$B$7)/COS(RADIANS($D32))
def xcf_Toe_Model_M32(): 
    try:      
        return (vcell("C86","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M32')
    return None
xfunctions['Toe Model!M32']=xcf_Toe_Model_M32

#(C86-$B$8)/COS(RADIANS($D32))
def xcf_Toe_Model_N32(): 
    try:      
        return (vcell("C86","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N32')
    return None
xfunctions['Toe Model!N32']=xcf_Toe_Model_N32

#(C86-$B$9)/COS(RADIANS($D32))
def xcf_Toe_Model_O32(): 
    try:      
        return (vcell("C86","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O32')
    return None
xfunctions['Toe Model!O32']=xcf_Toe_Model_O32

#(C86+999999)/COS(RADIANS($D32))
def xcf_Toe_Model_P32(): 
    try:      
        return (vcell("C86","Toe Model")+999999)/COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P32')
    return None
xfunctions['Toe Model!P32']=xcf_Toe_Model_P32

#Calculations!F20
def xcf_Toe_Model_B33(): 
    try:      
        return vcell("Calculations!F20","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B33')
    return None
xfunctions['Toe Model!B33']=xcf_Toe_Model_B33

#Calculations!G20
def xcf_Toe_Model_C33(): 
    try:      
        return vcell("Calculations!G20","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C33')
    return None
xfunctions['Toe Model!C33']=xcf_Toe_Model_C33

#IF(AND((B32>B34),(C34>C32)),360,DEGREES(ATAN(MAX((C32-C34),1E-20)/MAX((B34-B32),(1E-20)))))
def xcf_Toe_Model_D33(): 
    try:      
        return IF(AND((vcell("B32","Toe Model")>vcell("B34","Toe Model")),(vcell("C34","Toe Model")>vcell("C32","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C32","Toe Model")-vcell("C34","Toe Model")),1E-20)/MAX((vcell("B34","Toe Model")-vcell("B32","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D33')
    return None
xfunctions['Toe Model!D33']=xcf_Toe_Model_D33

#IF($C33>$B$5,1,IF(AND($C33<=$B$5,$C33>$B$6),2,IF(AND($C33<=$B$6,$C33>$B$7),3,IF(AND($C33<=$B$7,$C33>$B$8),4,IF(AND($C33<=$B$8,$C33>$B$9),5,6)))))
def xcf_Toe_Model_E33(): 
    try:      
        return IF(vcell("$C33","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C33","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C33","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C33","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C33","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C33","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C33","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C33","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C33","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E33')
    return None
xfunctions['Toe Model!E33']=xcf_Toe_Model_E33

#(C87-$B$5)/COS(RADIANS($D33))
def xcf_Toe_Model_K33(): 
    try:      
        return (vcell("C87","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K33')
    return None
xfunctions['Toe Model!K33']=xcf_Toe_Model_K33

#(C87-$B$6)/COS(RADIANS($D33))
def xcf_Toe_Model_L33(): 
    try:      
        return (vcell("C87","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L33')
    return None
xfunctions['Toe Model!L33']=xcf_Toe_Model_L33

#(C87-$B$7)/COS(RADIANS($D33))
def xcf_Toe_Model_M33(): 
    try:      
        return (vcell("C87","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M33')
    return None
xfunctions['Toe Model!M33']=xcf_Toe_Model_M33

#(C87-$B$8)/COS(RADIANS($D33))
def xcf_Toe_Model_N33(): 
    try:      
        return (vcell("C87","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N33')
    return None
xfunctions['Toe Model!N33']=xcf_Toe_Model_N33

#(C87-$B$9)/COS(RADIANS($D33))
def xcf_Toe_Model_O33(): 
    try:      
        return (vcell("C87","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O33')
    return None
xfunctions['Toe Model!O33']=xcf_Toe_Model_O33

#(C87+999999)/COS(RADIANS($D33))
def xcf_Toe_Model_P33(): 
    try:      
        return (vcell("C87","Toe Model")+999999)/COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P33')
    return None
xfunctions['Toe Model!P33']=xcf_Toe_Model_P33

#Calculations!F21
def xcf_Toe_Model_B34(): 
    try:      
        return vcell("Calculations!F21","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B34')
    return None
xfunctions['Toe Model!B34']=xcf_Toe_Model_B34

#Calculations!G21
def xcf_Toe_Model_C34(): 
    try:      
        return vcell("Calculations!G21","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C34')
    return None
xfunctions['Toe Model!C34']=xcf_Toe_Model_C34

#IF(AND((B33>B35),(C35>C33)),360,DEGREES(ATAN(MAX((C33-C35),1E-20)/MAX((B35-B33),(1E-20)))))
def xcf_Toe_Model_D34(): 
    try:      
        return IF(AND((vcell("B33","Toe Model")>vcell("B35","Toe Model")),(vcell("C35","Toe Model")>vcell("C33","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C33","Toe Model")-vcell("C35","Toe Model")),1E-20)/MAX((vcell("B35","Toe Model")-vcell("B33","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D34')
    return None
xfunctions['Toe Model!D34']=xcf_Toe_Model_D34

#IF($C34>$B$5,1,IF(AND($C34<=$B$5,$C34>$B$6),2,IF(AND($C34<=$B$6,$C34>$B$7),3,IF(AND($C34<=$B$7,$C34>$B$8),4,IF(AND($C34<=$B$8,$C34>$B$9),5,6)))))
def xcf_Toe_Model_E34(): 
    try:      
        return IF(vcell("$C34","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C34","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C34","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C34","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C34","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C34","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C34","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C34","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C34","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E34')
    return None
xfunctions['Toe Model!E34']=xcf_Toe_Model_E34

#(C88-$B$5)/COS(RADIANS($D34))
def xcf_Toe_Model_K34(): 
    try:      
        return (vcell("C88","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K34')
    return None
xfunctions['Toe Model!K34']=xcf_Toe_Model_K34

#(C88-$B$6)/COS(RADIANS($D34))
def xcf_Toe_Model_L34(): 
    try:      
        return (vcell("C88","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L34')
    return None
xfunctions['Toe Model!L34']=xcf_Toe_Model_L34

#(C88-$B$7)/COS(RADIANS($D34))
def xcf_Toe_Model_M34(): 
    try:      
        return (vcell("C88","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M34')
    return None
xfunctions['Toe Model!M34']=xcf_Toe_Model_M34

#(C88-$B$8)/COS(RADIANS($D34))
def xcf_Toe_Model_N34(): 
    try:      
        return (vcell("C88","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N34')
    return None
xfunctions['Toe Model!N34']=xcf_Toe_Model_N34

#(C88-$B$9)/COS(RADIANS($D34))
def xcf_Toe_Model_O34(): 
    try:      
        return (vcell("C88","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O34')
    return None
xfunctions['Toe Model!O34']=xcf_Toe_Model_O34

#(C88+999999)/COS(RADIANS($D34))
def xcf_Toe_Model_P34(): 
    try:      
        return (vcell("C88","Toe Model")+999999)/COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P34')
    return None
xfunctions['Toe Model!P34']=xcf_Toe_Model_P34

#Calculations!F22
def xcf_Toe_Model_B35(): 
    try:      
        return vcell("Calculations!F22","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B35')
    return None
xfunctions['Toe Model!B35']=xcf_Toe_Model_B35

#Calculations!G22
def xcf_Toe_Model_C35(): 
    try:      
        return vcell("Calculations!G22","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C35')
    return None
xfunctions['Toe Model!C35']=xcf_Toe_Model_C35

#IF(AND((B34>B36),(C36>C34)),360,DEGREES(ATAN(MAX((C34-C36),1E-20)/MAX((B36-B34),(1E-20)))))
def xcf_Toe_Model_D35(): 
    try:      
        return IF(AND((vcell("B34","Toe Model")>vcell("B36","Toe Model")),(vcell("C36","Toe Model")>vcell("C34","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C34","Toe Model")-vcell("C36","Toe Model")),1E-20)/MAX((vcell("B36","Toe Model")-vcell("B34","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D35')
    return None
xfunctions['Toe Model!D35']=xcf_Toe_Model_D35

#IF($C35>$B$5,1,IF(AND($C35<=$B$5,$C35>$B$6),2,IF(AND($C35<=$B$6,$C35>$B$7),3,IF(AND($C35<=$B$7,$C35>$B$8),4,IF(AND($C35<=$B$8,$C35>$B$9),5,6)))))
def xcf_Toe_Model_E35(): 
    try:      
        return IF(vcell("$C35","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C35","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C35","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C35","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C35","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C35","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C35","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C35","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C35","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E35')
    return None
xfunctions['Toe Model!E35']=xcf_Toe_Model_E35

#(C89-$B$5)/COS(RADIANS($D35))
def xcf_Toe_Model_K35(): 
    try:      
        return (vcell("C89","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K35')
    return None
xfunctions['Toe Model!K35']=xcf_Toe_Model_K35

#(C89-$B$6)/COS(RADIANS($D35))
def xcf_Toe_Model_L35(): 
    try:      
        return (vcell("C89","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L35')
    return None
xfunctions['Toe Model!L35']=xcf_Toe_Model_L35

#(C89-$B$7)/COS(RADIANS($D35))
def xcf_Toe_Model_M35(): 
    try:      
        return (vcell("C89","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M35')
    return None
xfunctions['Toe Model!M35']=xcf_Toe_Model_M35

#(C89-$B$8)/COS(RADIANS($D35))
def xcf_Toe_Model_N35(): 
    try:      
        return (vcell("C89","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N35')
    return None
xfunctions['Toe Model!N35']=xcf_Toe_Model_N35

#(C89-$B$9)/COS(RADIANS($D35))
def xcf_Toe_Model_O35(): 
    try:      
        return (vcell("C89","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O35')
    return None
xfunctions['Toe Model!O35']=xcf_Toe_Model_O35

#(C89+999999)/COS(RADIANS($D35))
def xcf_Toe_Model_P35(): 
    try:      
        return (vcell("C89","Toe Model")+999999)/COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P35')
    return None
xfunctions['Toe Model!P35']=xcf_Toe_Model_P35

#Calculations!F23
def xcf_Toe_Model_B36(): 
    try:      
        return vcell("Calculations!F23","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B36')
    return None
xfunctions['Toe Model!B36']=xcf_Toe_Model_B36

#Calculations!G23
def xcf_Toe_Model_C36(): 
    try:      
        return vcell("Calculations!G23","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C36')
    return None
xfunctions['Toe Model!C36']=xcf_Toe_Model_C36

#IF(AND((B35>B37),(C37>C35)),360,DEGREES(ATAN(MAX((C35-C37),1E-20)/MAX((B37-B35),(1E-20)))))
def xcf_Toe_Model_D36(): 
    try:      
        return IF(AND((vcell("B35","Toe Model")>vcell("B37","Toe Model")),(vcell("C37","Toe Model")>vcell("C35","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C35","Toe Model")-vcell("C37","Toe Model")),1E-20)/MAX((vcell("B37","Toe Model")-vcell("B35","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D36')
    return None
xfunctions['Toe Model!D36']=xcf_Toe_Model_D36

#IF($C36>$B$5,1,IF(AND($C36<=$B$5,$C36>$B$6),2,IF(AND($C36<=$B$6,$C36>$B$7),3,IF(AND($C36<=$B$7,$C36>$B$8),4,IF(AND($C36<=$B$8,$C36>$B$9),5,6)))))
def xcf_Toe_Model_E36(): 
    try:      
        return IF(vcell("$C36","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C36","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C36","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C36","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C36","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C36","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C36","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C36","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C36","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E36')
    return None
xfunctions['Toe Model!E36']=xcf_Toe_Model_E36

#(C90-$B$5)/COS(RADIANS($D36))
def xcf_Toe_Model_K36(): 
    try:      
        return (vcell("C90","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K36')
    return None
xfunctions['Toe Model!K36']=xcf_Toe_Model_K36

#(C90-$B$6)/COS(RADIANS($D36))
def xcf_Toe_Model_L36(): 
    try:      
        return (vcell("C90","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L36')
    return None
xfunctions['Toe Model!L36']=xcf_Toe_Model_L36

#(C90-$B$7)/COS(RADIANS($D36))
def xcf_Toe_Model_M36(): 
    try:      
        return (vcell("C90","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M36')
    return None
xfunctions['Toe Model!M36']=xcf_Toe_Model_M36

#(C90-$B$8)/COS(RADIANS($D36))
def xcf_Toe_Model_N36(): 
    try:      
        return (vcell("C90","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N36')
    return None
xfunctions['Toe Model!N36']=xcf_Toe_Model_N36

#(C90-$B$9)/COS(RADIANS($D36))
def xcf_Toe_Model_O36(): 
    try:      
        return (vcell("C90","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O36')
    return None
xfunctions['Toe Model!O36']=xcf_Toe_Model_O36

#(C90+999999)/COS(RADIANS($D36))
def xcf_Toe_Model_P36(): 
    try:      
        return (vcell("C90","Toe Model")+999999)/COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P36')
    return None
xfunctions['Toe Model!P36']=xcf_Toe_Model_P36

#Calculations!F24
def xcf_Toe_Model_B37(): 
    try:      
        return vcell("Calculations!F24","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B37')
    return None
xfunctions['Toe Model!B37']=xcf_Toe_Model_B37

#Calculations!G24
def xcf_Toe_Model_C37(): 
    try:      
        return vcell("Calculations!G24","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C37')
    return None
xfunctions['Toe Model!C37']=xcf_Toe_Model_C37

#IF(AND((B36>B38),(C38>C36)),360,DEGREES(ATAN(MAX((C36-C38),1E-20)/MAX((B38-B36),(1E-20)))))
def xcf_Toe_Model_D37(): 
    try:      
        return IF(AND((vcell("B36","Toe Model")>vcell("B38","Toe Model")),(vcell("C38","Toe Model")>vcell("C36","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C36","Toe Model")-vcell("C38","Toe Model")),1E-20)/MAX((vcell("B38","Toe Model")-vcell("B36","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D37')
    return None
xfunctions['Toe Model!D37']=xcf_Toe_Model_D37

#IF($C37>$B$5,1,IF(AND($C37<=$B$5,$C37>$B$6),2,IF(AND($C37<=$B$6,$C37>$B$7),3,IF(AND($C37<=$B$7,$C37>$B$8),4,IF(AND($C37<=$B$8,$C37>$B$9),5,6)))))
def xcf_Toe_Model_E37(): 
    try:      
        return IF(vcell("$C37","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C37","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C37","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C37","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C37","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C37","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C37","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C37","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C37","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E37')
    return None
xfunctions['Toe Model!E37']=xcf_Toe_Model_E37

#(C91-$B$5)/COS(RADIANS($D37))
def xcf_Toe_Model_K37(): 
    try:      
        return (vcell("C91","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K37')
    return None
xfunctions['Toe Model!K37']=xcf_Toe_Model_K37

#(C91-$B$6)/COS(RADIANS($D37))
def xcf_Toe_Model_L37(): 
    try:      
        return (vcell("C91","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L37')
    return None
xfunctions['Toe Model!L37']=xcf_Toe_Model_L37

#(C91-$B$7)/COS(RADIANS($D37))
def xcf_Toe_Model_M37(): 
    try:      
        return (vcell("C91","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M37')
    return None
xfunctions['Toe Model!M37']=xcf_Toe_Model_M37

#(C91-$B$8)/COS(RADIANS($D37))
def xcf_Toe_Model_N37(): 
    try:      
        return (vcell("C91","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N37')
    return None
xfunctions['Toe Model!N37']=xcf_Toe_Model_N37

#(C91-$B$9)/COS(RADIANS($D37))
def xcf_Toe_Model_O37(): 
    try:      
        return (vcell("C91","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O37')
    return None
xfunctions['Toe Model!O37']=xcf_Toe_Model_O37

#(C91+999999)/COS(RADIANS($D37))
def xcf_Toe_Model_P37(): 
    try:      
        return (vcell("C91","Toe Model")+999999)/COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P37')
    return None
xfunctions['Toe Model!P37']=xcf_Toe_Model_P37

#Calculations!F25
def xcf_Toe_Model_B38(): 
    try:      
        return vcell("Calculations!F25","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B38')
    return None
xfunctions['Toe Model!B38']=xcf_Toe_Model_B38

#Calculations!G25
def xcf_Toe_Model_C38(): 
    try:      
        return vcell("Calculations!G25","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C38')
    return None
xfunctions['Toe Model!C38']=xcf_Toe_Model_C38

#IF(AND((B37>B39),(C39>C37)),360,DEGREES(ATAN(MAX((C37-C39),1E-20)/MAX((B39-B37),(1E-20)))))
def xcf_Toe_Model_D38(): 
    try:      
        return IF(AND((vcell("B37","Toe Model")>vcell("B39","Toe Model")),(vcell("C39","Toe Model")>vcell("C37","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C37","Toe Model")-vcell("C39","Toe Model")),1E-20)/MAX((vcell("B39","Toe Model")-vcell("B37","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D38')
    return None
xfunctions['Toe Model!D38']=xcf_Toe_Model_D38

#IF($C38>$B$5,1,IF(AND($C38<=$B$5,$C38>$B$6),2,IF(AND($C38<=$B$6,$C38>$B$7),3,IF(AND($C38<=$B$7,$C38>$B$8),4,IF(AND($C38<=$B$8,$C38>$B$9),5,6)))))
def xcf_Toe_Model_E38(): 
    try:      
        return IF(vcell("$C38","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C38","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C38","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C38","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C38","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C38","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C38","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C38","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C38","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E38')
    return None
xfunctions['Toe Model!E38']=xcf_Toe_Model_E38

#(C92-$B$5)/COS(RADIANS($D38))
def xcf_Toe_Model_K38(): 
    try:      
        return (vcell("C92","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K38')
    return None
xfunctions['Toe Model!K38']=xcf_Toe_Model_K38

#(C92-$B$6)/COS(RADIANS($D38))
def xcf_Toe_Model_L38(): 
    try:      
        return (vcell("C92","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L38')
    return None
xfunctions['Toe Model!L38']=xcf_Toe_Model_L38

#(C92-$B$7)/COS(RADIANS($D38))
def xcf_Toe_Model_M38(): 
    try:      
        return (vcell("C92","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M38')
    return None
xfunctions['Toe Model!M38']=xcf_Toe_Model_M38

#(C92-$B$8)/COS(RADIANS($D38))
def xcf_Toe_Model_N38(): 
    try:      
        return (vcell("C92","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N38')
    return None
xfunctions['Toe Model!N38']=xcf_Toe_Model_N38

#(C92-$B$9)/COS(RADIANS($D38))
def xcf_Toe_Model_O38(): 
    try:      
        return (vcell("C92","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O38')
    return None
xfunctions['Toe Model!O38']=xcf_Toe_Model_O38

#(C92+999999)/COS(RADIANS($D38))
def xcf_Toe_Model_P38(): 
    try:      
        return (vcell("C92","Toe Model")+999999)/COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P38')
    return None
xfunctions['Toe Model!P38']=xcf_Toe_Model_P38

#Calculations!F26
def xcf_Toe_Model_B39(): 
    try:      
        return vcell("Calculations!F26","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B39')
    return None
xfunctions['Toe Model!B39']=xcf_Toe_Model_B39

#Calculations!G26
def xcf_Toe_Model_C39(): 
    try:      
        return vcell("Calculations!G26","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C39')
    return None
xfunctions['Toe Model!C39']=xcf_Toe_Model_C39

#IF(AND((B38>B40),(C40>C38)),360,DEGREES(ATAN(MAX((C38-C40),1E-20)/MAX((B40-B38),(1E-20)))))
def xcf_Toe_Model_D39(): 
    try:      
        return IF(AND((vcell("B38","Toe Model")>vcell("B40","Toe Model")),(vcell("C40","Toe Model")>vcell("C38","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C38","Toe Model")-vcell("C40","Toe Model")),1E-20)/MAX((vcell("B40","Toe Model")-vcell("B38","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D39')
    return None
xfunctions['Toe Model!D39']=xcf_Toe_Model_D39

#IF($C39>$B$5,1,IF(AND($C39<=$B$5,$C39>$B$6),2,IF(AND($C39<=$B$6,$C39>$B$7),3,IF(AND($C39<=$B$7,$C39>$B$8),4,IF(AND($C39<=$B$8,$C39>$B$9),5,6)))))
def xcf_Toe_Model_E39(): 
    try:      
        return IF(vcell("$C39","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C39","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C39","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C39","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C39","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C39","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C39","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C39","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C39","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E39')
    return None
xfunctions['Toe Model!E39']=xcf_Toe_Model_E39

#(C93-$B$5)/COS(RADIANS($D39))
def xcf_Toe_Model_K39(): 
    try:      
        return (vcell("C93","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K39')
    return None
xfunctions['Toe Model!K39']=xcf_Toe_Model_K39

#(C93-$B$6)/COS(RADIANS($D39))
def xcf_Toe_Model_L39(): 
    try:      
        return (vcell("C93","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L39')
    return None
xfunctions['Toe Model!L39']=xcf_Toe_Model_L39

#(C93-$B$7)/COS(RADIANS($D39))
def xcf_Toe_Model_M39(): 
    try:      
        return (vcell("C93","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M39')
    return None
xfunctions['Toe Model!M39']=xcf_Toe_Model_M39

#(C93-$B$8)/COS(RADIANS($D39))
def xcf_Toe_Model_N39(): 
    try:      
        return (vcell("C93","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N39')
    return None
xfunctions['Toe Model!N39']=xcf_Toe_Model_N39

#(C93-$B$9)/COS(RADIANS($D39))
def xcf_Toe_Model_O39(): 
    try:      
        return (vcell("C93","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O39')
    return None
xfunctions['Toe Model!O39']=xcf_Toe_Model_O39

#(C93+999999)/COS(RADIANS($D39))
def xcf_Toe_Model_P39(): 
    try:      
        return (vcell("C93","Toe Model")+999999)/COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P39')
    return None
xfunctions['Toe Model!P39']=xcf_Toe_Model_P39

#Calculations!F27
def xcf_Toe_Model_B40(): 
    try:      
        return vcell("Calculations!F27","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B40')
    return None
xfunctions['Toe Model!B40']=xcf_Toe_Model_B40

#Calculations!G27
def xcf_Toe_Model_C40(): 
    try:      
        return vcell("Calculations!G27","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C40')
    return None
xfunctions['Toe Model!C40']=xcf_Toe_Model_C40

#IF(AND((B39>B40),(C40>C39)),360,DEGREES(ATAN(MAX((C39-C40),1E-20)/MAX((B40-B39),(1E-20)))))
def xcf_Toe_Model_D40(): 
    try:      
        return IF(AND((vcell("B39","Toe Model")>vcell("B40","Toe Model")),(vcell("C40","Toe Model")>vcell("C39","Toe Model"))),360,DEGREES(ATAN(MAX((vcell("C39","Toe Model")-vcell("C40","Toe Model")),1E-20)/MAX((vcell("B40","Toe Model")-vcell("B39","Toe Model")),(1E-20)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D40')
    return None
xfunctions['Toe Model!D40']=xcf_Toe_Model_D40

#IF($C40>$B$5,1,IF(AND($C40<=$B$5,$C40>$B$6),2,IF(AND($C40<=$B$6,$C40>$B$7),3,IF(AND($C40<=$B$7,$C40>$B$8),4,IF(AND($C40<=$B$8,$C40>$B$9),5,6)))))
def xcf_Toe_Model_E40(): 
    try:      
        return IF(vcell("$C40","Toe Model")>vcell("$B$5","Toe Model"),1,IF(AND(vcell("$C40","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C40","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C40","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C40","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C40","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C40","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C40","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C40","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E40')
    return None
xfunctions['Toe Model!E40']=xcf_Toe_Model_E40

#(C94-$B$5)/COS(RADIANS($D40))
def xcf_Toe_Model_K40(): 
    try:      
        return (vcell("C94","Toe Model")-vcell("$B$5","Toe Model"))/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K40')
    return None
xfunctions['Toe Model!K40']=xcf_Toe_Model_K40

#(C94-$B$6)/COS(RADIANS($D40))
def xcf_Toe_Model_L40(): 
    try:      
        return (vcell("C94","Toe Model")-vcell("$B$6","Toe Model"))/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L40')
    return None
xfunctions['Toe Model!L40']=xcf_Toe_Model_L40

#(C94-$B$7)/COS(RADIANS($D40))
def xcf_Toe_Model_M40(): 
    try:      
        return (vcell("C94","Toe Model")-vcell("$B$7","Toe Model"))/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M40')
    return None
xfunctions['Toe Model!M40']=xcf_Toe_Model_M40

#(C94-$B$8)/COS(RADIANS($D40))
def xcf_Toe_Model_N40(): 
    try:      
        return (vcell("C94","Toe Model")-vcell("$B$8","Toe Model"))/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N40')
    return None
xfunctions['Toe Model!N40']=xcf_Toe_Model_N40

#(C94-$B$9)/COS(RADIANS($D40))
def xcf_Toe_Model_O40(): 
    try:      
        return (vcell("C94","Toe Model")-vcell("$B$9","Toe Model"))/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O40')
    return None
xfunctions['Toe Model!O40']=xcf_Toe_Model_O40

#(C94+999999)/COS(RADIANS($D40))
def xcf_Toe_Model_P40(): 
    try:      
        return (vcell("C94","Toe Model")+999999)/COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P40')
    return None
xfunctions['Toe Model!P40']=xcf_Toe_Model_P40

#IF($B45>$C$5,$D$5*($B45-$C$5),1E-20)
def xcf_Toe_Model_C45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C45')
    return None
xfunctions['Toe Model!C45']=xcf_Toe_Model_C45

#IF($B45>$C$6,$D$6*($B45-$C$6),1E-20)
def xcf_Toe_Model_D45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D45')
    return None
xfunctions['Toe Model!D45']=xcf_Toe_Model_D45

#IF($B45>$C$7,$D$7*($B45-$C$7),1E-20)
def xcf_Toe_Model_E45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E45')
    return None
xfunctions['Toe Model!E45']=xcf_Toe_Model_E45

#IF($B45>$C$8,$D$8*($B45-$C$8),1E-20)
def xcf_Toe_Model_F45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F45')
    return None
xfunctions['Toe Model!F45']=xcf_Toe_Model_F45

#IF($B45>$C$9,$D$9*($B45-$C$9),1E-20)
def xcf_Toe_Model_G45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G45')
    return None
xfunctions['Toe Model!G45']=xcf_Toe_Model_G45

#IF($B45>$C$11,$D$11*($B45-$C$11),1E-20)
def xcf_Toe_Model_H45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H45')
    return None
xfunctions['Toe Model!H45']=xcf_Toe_Model_H45

#IF($B45>$C$10,$D$10*($B45-$C$10),1E-20)
def xcf_Toe_Model_I45(): 
    try:      
        return IF(vcell("$B45","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B45","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I45')
    return None
xfunctions['Toe Model!I45']=xcf_Toe_Model_I45

#IF($B46>$C$5,$D$5*($B46-$C$5),1E-20)
def xcf_Toe_Model_C46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C46')
    return None
xfunctions['Toe Model!C46']=xcf_Toe_Model_C46

#IF($B46>$C$6,$D$6*($B46-$C$6),1E-20)
def xcf_Toe_Model_D46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D46')
    return None
xfunctions['Toe Model!D46']=xcf_Toe_Model_D46

#IF($B46>$C$7,$D$7*($B46-$C$7),1E-20)
def xcf_Toe_Model_E46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E46')
    return None
xfunctions['Toe Model!E46']=xcf_Toe_Model_E46

#IF($B46>$C$8,$D$8*($B46-$C$8),1E-20)
def xcf_Toe_Model_F46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F46')
    return None
xfunctions['Toe Model!F46']=xcf_Toe_Model_F46

#IF($B46>$C$9,$D$9*($B46-$C$9),1E-20)
def xcf_Toe_Model_G46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G46')
    return None
xfunctions['Toe Model!G46']=xcf_Toe_Model_G46

#IF($B46>$C$11,$D$11*($B46-$C$11),1E-20)
def xcf_Toe_Model_H46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H46')
    return None
xfunctions['Toe Model!H46']=xcf_Toe_Model_H46

#IF($B46>$C$10,$D$10*($B46-$C$10),1E-20)
def xcf_Toe_Model_I46(): 
    try:      
        return IF(vcell("$B46","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B46","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I46')
    return None
xfunctions['Toe Model!I46']=xcf_Toe_Model_I46

#IF($B47>$C$5,$D$5*($B47-$C$5),1E-20)
def xcf_Toe_Model_C47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C47')
    return None
xfunctions['Toe Model!C47']=xcf_Toe_Model_C47

#IF($B47>$C$6,$D$6*($B47-$C$6),1E-20)
def xcf_Toe_Model_D47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D47')
    return None
xfunctions['Toe Model!D47']=xcf_Toe_Model_D47

#IF($B47>$C$7,$D$7*($B47-$C$7),1E-20)
def xcf_Toe_Model_E47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E47')
    return None
xfunctions['Toe Model!E47']=xcf_Toe_Model_E47

#IF($B47>$C$8,$D$8*($B47-$C$8),1E-20)
def xcf_Toe_Model_F47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F47')
    return None
xfunctions['Toe Model!F47']=xcf_Toe_Model_F47

#IF($B47>$C$9,$D$9*($B47-$C$9),1E-20)
def xcf_Toe_Model_G47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G47')
    return None
xfunctions['Toe Model!G47']=xcf_Toe_Model_G47

#IF($B47>$C$11,$D$11*($B47-$C$11),1E-20)
def xcf_Toe_Model_H47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H47')
    return None
xfunctions['Toe Model!H47']=xcf_Toe_Model_H47

#IF($B47>$C$10,$D$10*($B47-$C$10),1E-20)
def xcf_Toe_Model_I47(): 
    try:      
        return IF(vcell("$B47","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B47","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I47')
    return None
xfunctions['Toe Model!I47']=xcf_Toe_Model_I47

#IF($B48>$C$5,$D$5*($B48-$C$5),1E-20)
def xcf_Toe_Model_C48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C48')
    return None
xfunctions['Toe Model!C48']=xcf_Toe_Model_C48

#IF($B48>$C$6,$D$6*($B48-$C$6),1E-20)
def xcf_Toe_Model_D48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D48')
    return None
xfunctions['Toe Model!D48']=xcf_Toe_Model_D48

#IF($B48>$C$7,$D$7*($B48-$C$7),1E-20)
def xcf_Toe_Model_E48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E48')
    return None
xfunctions['Toe Model!E48']=xcf_Toe_Model_E48

#IF($B48>$C$8,$D$8*($B48-$C$8),1E-20)
def xcf_Toe_Model_F48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F48')
    return None
xfunctions['Toe Model!F48']=xcf_Toe_Model_F48

#IF($B48>$C$9,$D$9*($B48-$C$9),1E-20)
def xcf_Toe_Model_G48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G48')
    return None
xfunctions['Toe Model!G48']=xcf_Toe_Model_G48

#IF($B48>$C$11,$D$11*($B48-$C$11),1E-20)
def xcf_Toe_Model_H48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H48')
    return None
xfunctions['Toe Model!H48']=xcf_Toe_Model_H48

#IF($B48>$C$10,$D$10*($B48-$C$10),1E-20)
def xcf_Toe_Model_I48(): 
    try:      
        return IF(vcell("$B48","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B48","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I48')
    return None
xfunctions['Toe Model!I48']=xcf_Toe_Model_I48

#IF($B49>$C$5,$D$5*($B49-$C$5),1E-20)
def xcf_Toe_Model_C49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C49')
    return None
xfunctions['Toe Model!C49']=xcf_Toe_Model_C49

#IF($B49>$C$6,$D$6*($B49-$C$6),1E-20)
def xcf_Toe_Model_D49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D49')
    return None
xfunctions['Toe Model!D49']=xcf_Toe_Model_D49

#IF($B49>$C$7,$D$7*($B49-$C$7),1E-20)
def xcf_Toe_Model_E49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E49')
    return None
xfunctions['Toe Model!E49']=xcf_Toe_Model_E49

#IF($B49>$C$8,$D$8*($B49-$C$8),1E-20)
def xcf_Toe_Model_F49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F49')
    return None
xfunctions['Toe Model!F49']=xcf_Toe_Model_F49

#IF($B49>$C$9,$D$9*($B49-$C$9),1E-20)
def xcf_Toe_Model_G49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G49')
    return None
xfunctions['Toe Model!G49']=xcf_Toe_Model_G49

#IF($B49>$C$11,$D$11*($B49-$C$11),1E-20)
def xcf_Toe_Model_H49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H49')
    return None
xfunctions['Toe Model!H49']=xcf_Toe_Model_H49

#IF($B49>$C$10,$D$10*($B49-$C$10),1E-20)
def xcf_Toe_Model_I49(): 
    try:      
        return IF(vcell("$B49","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B49","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I49')
    return None
xfunctions['Toe Model!I49']=xcf_Toe_Model_I49

#IF($B50>$C$5,$D$5*($B50-$C$5),1E-20)
def xcf_Toe_Model_C50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C50')
    return None
xfunctions['Toe Model!C50']=xcf_Toe_Model_C50

#IF($B50>$C$6,$D$6*($B50-$C$6),1E-20)
def xcf_Toe_Model_D50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D50')
    return None
xfunctions['Toe Model!D50']=xcf_Toe_Model_D50

#IF($B50>$C$7,$D$7*($B50-$C$7),1E-20)
def xcf_Toe_Model_E50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E50')
    return None
xfunctions['Toe Model!E50']=xcf_Toe_Model_E50

#IF($B50>$C$8,$D$8*($B50-$C$8),1E-20)
def xcf_Toe_Model_F50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F50')
    return None
xfunctions['Toe Model!F50']=xcf_Toe_Model_F50

#IF($B50>$C$9,$D$9*($B50-$C$9),1E-20)
def xcf_Toe_Model_G50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G50')
    return None
xfunctions['Toe Model!G50']=xcf_Toe_Model_G50

#IF($B50>$C$11,$D$11*($B50-$C$11),1E-20)
def xcf_Toe_Model_H50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H50')
    return None
xfunctions['Toe Model!H50']=xcf_Toe_Model_H50

#IF($B50>$C$10,$D$10*($B50-$C$10),1E-20)
def xcf_Toe_Model_I50(): 
    try:      
        return IF(vcell("$B50","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B50","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I50')
    return None
xfunctions['Toe Model!I50']=xcf_Toe_Model_I50

#IF($B51>$C$5,$D$5*($B51-$C$5),1E-20)
def xcf_Toe_Model_C51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C51')
    return None
xfunctions['Toe Model!C51']=xcf_Toe_Model_C51

#IF($B51>$C$6,$D$6*($B51-$C$6),1E-20)
def xcf_Toe_Model_D51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D51')
    return None
xfunctions['Toe Model!D51']=xcf_Toe_Model_D51

#IF($B51>$C$7,$D$7*($B51-$C$7),1E-20)
def xcf_Toe_Model_E51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E51')
    return None
xfunctions['Toe Model!E51']=xcf_Toe_Model_E51

#IF($B51>$C$8,$D$8*($B51-$C$8),1E-20)
def xcf_Toe_Model_F51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F51')
    return None
xfunctions['Toe Model!F51']=xcf_Toe_Model_F51

#IF($B51>$C$9,$D$9*($B51-$C$9),1E-20)
def xcf_Toe_Model_G51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G51')
    return None
xfunctions['Toe Model!G51']=xcf_Toe_Model_G51

#IF($B51>$C$11,$D$11*($B51-$C$11),1E-20)
def xcf_Toe_Model_H51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H51')
    return None
xfunctions['Toe Model!H51']=xcf_Toe_Model_H51

#IF($B51>$C$10,$D$10*($B51-$C$10),1E-20)
def xcf_Toe_Model_I51(): 
    try:      
        return IF(vcell("$B51","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B51","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I51')
    return None
xfunctions['Toe Model!I51']=xcf_Toe_Model_I51

#IF($B52>$C$5,$D$5*($B52-$C$5),1E-20)
def xcf_Toe_Model_C52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C52')
    return None
xfunctions['Toe Model!C52']=xcf_Toe_Model_C52

#IF($B52>$C$6,$D$6*($B52-$C$6),1E-20)
def xcf_Toe_Model_D52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D52')
    return None
xfunctions['Toe Model!D52']=xcf_Toe_Model_D52

#IF($B52>$C$7,$D$7*($B52-$C$7),1E-20)
def xcf_Toe_Model_E52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E52')
    return None
xfunctions['Toe Model!E52']=xcf_Toe_Model_E52

#IF($B52>$C$8,$D$8*($B52-$C$8),1E-20)
def xcf_Toe_Model_F52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F52')
    return None
xfunctions['Toe Model!F52']=xcf_Toe_Model_F52

#IF($B52>$C$9,$D$9*($B52-$C$9),1E-20)
def xcf_Toe_Model_G52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G52')
    return None
xfunctions['Toe Model!G52']=xcf_Toe_Model_G52

#IF($B52>$C$11,$D$11*($B52-$C$11),1E-20)
def xcf_Toe_Model_H52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H52')
    return None
xfunctions['Toe Model!H52']=xcf_Toe_Model_H52

#IF($B52>$C$10,$D$10*($B52-$C$10),1E-20)
def xcf_Toe_Model_I52(): 
    try:      
        return IF(vcell("$B52","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B52","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I52')
    return None
xfunctions['Toe Model!I52']=xcf_Toe_Model_I52

#IF($B53>$C$5,$D$5*($B53-$C$5),1E-20)
def xcf_Toe_Model_C53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C53')
    return None
xfunctions['Toe Model!C53']=xcf_Toe_Model_C53

#IF($B53>$C$6,$D$6*($B53-$C$6),1E-20)
def xcf_Toe_Model_D53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D53')
    return None
xfunctions['Toe Model!D53']=xcf_Toe_Model_D53

#IF($B53>$C$7,$D$7*($B53-$C$7),1E-20)
def xcf_Toe_Model_E53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E53')
    return None
xfunctions['Toe Model!E53']=xcf_Toe_Model_E53

#IF($B53>$C$8,$D$8*($B53-$C$8),1E-20)
def xcf_Toe_Model_F53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F53')
    return None
xfunctions['Toe Model!F53']=xcf_Toe_Model_F53

#IF($B53>$C$9,$D$9*($B53-$C$9),1E-20)
def xcf_Toe_Model_G53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G53')
    return None
xfunctions['Toe Model!G53']=xcf_Toe_Model_G53

#IF($B53>$C$11,$D$11*($B53-$C$11),1E-20)
def xcf_Toe_Model_H53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H53')
    return None
xfunctions['Toe Model!H53']=xcf_Toe_Model_H53

#IF($B53>$C$10,$D$10*($B53-$C$10),1E-20)
def xcf_Toe_Model_I53(): 
    try:      
        return IF(vcell("$B53","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B53","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I53')
    return None
xfunctions['Toe Model!I53']=xcf_Toe_Model_I53

#IF($B54>$C$5,$D$5*($B54-$C$5),1E-20)
def xcf_Toe_Model_C54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C54')
    return None
xfunctions['Toe Model!C54']=xcf_Toe_Model_C54

#IF($B54>$C$6,$D$6*($B54-$C$6),1E-20)
def xcf_Toe_Model_D54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D54')
    return None
xfunctions['Toe Model!D54']=xcf_Toe_Model_D54

#IF($B54>$C$7,$D$7*($B54-$C$7),1E-20)
def xcf_Toe_Model_E54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E54')
    return None
xfunctions['Toe Model!E54']=xcf_Toe_Model_E54

#IF($B54>$C$8,$D$8*($B54-$C$8),1E-20)
def xcf_Toe_Model_F54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F54')
    return None
xfunctions['Toe Model!F54']=xcf_Toe_Model_F54

#IF($B54>$C$9,$D$9*($B54-$C$9),1E-20)
def xcf_Toe_Model_G54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G54')
    return None
xfunctions['Toe Model!G54']=xcf_Toe_Model_G54

#IF($B54>$C$11,$D$11*($B54-$C$11),1E-20)
def xcf_Toe_Model_H54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H54')
    return None
xfunctions['Toe Model!H54']=xcf_Toe_Model_H54

#IF($B54>$C$10,$D$10*($B54-$C$10),1E-20)
def xcf_Toe_Model_I54(): 
    try:      
        return IF(vcell("$B54","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B54","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I54')
    return None
xfunctions['Toe Model!I54']=xcf_Toe_Model_I54

#IF($B55>$C$5,$D$5*($B55-$C$5),1E-20)
def xcf_Toe_Model_C55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C55')
    return None
xfunctions['Toe Model!C55']=xcf_Toe_Model_C55

#IF($B55>$C$6,$D$6*($B55-$C$6),1E-20)
def xcf_Toe_Model_D55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D55')
    return None
xfunctions['Toe Model!D55']=xcf_Toe_Model_D55

#IF($B55>$C$7,$D$7*($B55-$C$7),1E-20)
def xcf_Toe_Model_E55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E55')
    return None
xfunctions['Toe Model!E55']=xcf_Toe_Model_E55

#IF($B55>$C$8,$D$8*($B55-$C$8),1E-20)
def xcf_Toe_Model_F55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F55')
    return None
xfunctions['Toe Model!F55']=xcf_Toe_Model_F55

#IF($B55>$C$9,$D$9*($B55-$C$9),1E-20)
def xcf_Toe_Model_G55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G55')
    return None
xfunctions['Toe Model!G55']=xcf_Toe_Model_G55

#IF($B55>$C$11,$D$11*($B55-$C$11),1E-20)
def xcf_Toe_Model_H55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H55')
    return None
xfunctions['Toe Model!H55']=xcf_Toe_Model_H55

#IF($B55>$C$10,$D$10*($B55-$C$10),1E-20)
def xcf_Toe_Model_I55(): 
    try:      
        return IF(vcell("$B55","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B55","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I55')
    return None
xfunctions['Toe Model!I55']=xcf_Toe_Model_I55

#IF($B56>$C$5,$D$5*($B56-$C$5),1E-20)
def xcf_Toe_Model_C56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C56')
    return None
xfunctions['Toe Model!C56']=xcf_Toe_Model_C56

#IF($B56>$C$6,$D$6*($B56-$C$6),1E-20)
def xcf_Toe_Model_D56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D56')
    return None
xfunctions['Toe Model!D56']=xcf_Toe_Model_D56

#IF($B56>$C$7,$D$7*($B56-$C$7),1E-20)
def xcf_Toe_Model_E56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E56')
    return None
xfunctions['Toe Model!E56']=xcf_Toe_Model_E56

#IF($B56>$C$8,$D$8*($B56-$C$8),1E-20)
def xcf_Toe_Model_F56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F56')
    return None
xfunctions['Toe Model!F56']=xcf_Toe_Model_F56

#IF($B56>$C$9,$D$9*($B56-$C$9),1E-20)
def xcf_Toe_Model_G56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G56')
    return None
xfunctions['Toe Model!G56']=xcf_Toe_Model_G56

#IF($B56>$C$11,$D$11*($B56-$C$11),1E-20)
def xcf_Toe_Model_H56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H56')
    return None
xfunctions['Toe Model!H56']=xcf_Toe_Model_H56

#IF($B56>$C$10,$D$10*($B56-$C$10),1E-20)
def xcf_Toe_Model_I56(): 
    try:      
        return IF(vcell("$B56","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B56","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I56')
    return None
xfunctions['Toe Model!I56']=xcf_Toe_Model_I56

#IF($B57>$C$5,$D$5*($B57-$C$5),1E-20)
def xcf_Toe_Model_C57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C57')
    return None
xfunctions['Toe Model!C57']=xcf_Toe_Model_C57

#IF($B57>$C$6,$D$6*($B57-$C$6),1E-20)
def xcf_Toe_Model_D57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D57')
    return None
xfunctions['Toe Model!D57']=xcf_Toe_Model_D57

#IF($B57>$C$7,$D$7*($B57-$C$7),1E-20)
def xcf_Toe_Model_E57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E57')
    return None
xfunctions['Toe Model!E57']=xcf_Toe_Model_E57

#IF($B57>$C$8,$D$8*($B57-$C$8),1E-20)
def xcf_Toe_Model_F57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F57')
    return None
xfunctions['Toe Model!F57']=xcf_Toe_Model_F57

#IF($B57>$C$9,$D$9*($B57-$C$9),1E-20)
def xcf_Toe_Model_G57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G57')
    return None
xfunctions['Toe Model!G57']=xcf_Toe_Model_G57

#IF($B57>$C$11,$D$11*($B57-$C$11),1E-20)
def xcf_Toe_Model_H57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H57')
    return None
xfunctions['Toe Model!H57']=xcf_Toe_Model_H57

#IF($B57>$C$10,$D$10*($B57-$C$10),1E-20)
def xcf_Toe_Model_I57(): 
    try:      
        return IF(vcell("$B57","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B57","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I57')
    return None
xfunctions['Toe Model!I57']=xcf_Toe_Model_I57

#IF($B58>$C$5,$D$5*($B58-$C$5),1E-20)
def xcf_Toe_Model_C58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C58')
    return None
xfunctions['Toe Model!C58']=xcf_Toe_Model_C58

#IF($B58>$C$6,$D$6*($B58-$C$6),1E-20)
def xcf_Toe_Model_D58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D58')
    return None
xfunctions['Toe Model!D58']=xcf_Toe_Model_D58

#IF($B58>$C$7,$D$7*($B58-$C$7),1E-20)
def xcf_Toe_Model_E58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E58')
    return None
xfunctions['Toe Model!E58']=xcf_Toe_Model_E58

#IF($B58>$C$8,$D$8*($B58-$C$8),1E-20)
def xcf_Toe_Model_F58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F58')
    return None
xfunctions['Toe Model!F58']=xcf_Toe_Model_F58

#IF($B58>$C$9,$D$9*($B58-$C$9),1E-20)
def xcf_Toe_Model_G58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G58')
    return None
xfunctions['Toe Model!G58']=xcf_Toe_Model_G58

#IF($B58>$C$11,$D$11*($B58-$C$11),1E-20)
def xcf_Toe_Model_H58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H58')
    return None
xfunctions['Toe Model!H58']=xcf_Toe_Model_H58

#IF($B58>$C$10,$D$10*($B58-$C$10),1E-20)
def xcf_Toe_Model_I58(): 
    try:      
        return IF(vcell("$B58","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B58","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I58')
    return None
xfunctions['Toe Model!I58']=xcf_Toe_Model_I58

#IF($B59>$C$5,$D$5*($B59-$C$5),1E-20)
def xcf_Toe_Model_C59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C59')
    return None
xfunctions['Toe Model!C59']=xcf_Toe_Model_C59

#IF($B59>$C$6,$D$6*($B59-$C$6),1E-20)
def xcf_Toe_Model_D59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D59')
    return None
xfunctions['Toe Model!D59']=xcf_Toe_Model_D59

#IF($B59>$C$7,$D$7*($B59-$C$7),1E-20)
def xcf_Toe_Model_E59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E59')
    return None
xfunctions['Toe Model!E59']=xcf_Toe_Model_E59

#IF($B59>$C$8,$D$8*($B59-$C$8),1E-20)
def xcf_Toe_Model_F59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F59')
    return None
xfunctions['Toe Model!F59']=xcf_Toe_Model_F59

#IF($B59>$C$9,$D$9*($B59-$C$9),1E-20)
def xcf_Toe_Model_G59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G59')
    return None
xfunctions['Toe Model!G59']=xcf_Toe_Model_G59

#IF($B59>$C$11,$D$11*($B59-$C$11),1E-20)
def xcf_Toe_Model_H59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H59')
    return None
xfunctions['Toe Model!H59']=xcf_Toe_Model_H59

#IF($B59>$C$10,$D$10*($B59-$C$10),1E-20)
def xcf_Toe_Model_I59(): 
    try:      
        return IF(vcell("$B59","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B59","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I59')
    return None
xfunctions['Toe Model!I59']=xcf_Toe_Model_I59

#IF($B60>$C$5,$D$5*($B60-$C$5),1E-20)
def xcf_Toe_Model_C60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C60')
    return None
xfunctions['Toe Model!C60']=xcf_Toe_Model_C60

#IF($B60>$C$6,$D$6*($B60-$C$6),1E-20)
def xcf_Toe_Model_D60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D60')
    return None
xfunctions['Toe Model!D60']=xcf_Toe_Model_D60

#IF($B60>$C$7,$D$7*($B60-$C$7),1E-20)
def xcf_Toe_Model_E60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E60')
    return None
xfunctions['Toe Model!E60']=xcf_Toe_Model_E60

#IF($B60>$C$8,$D$8*($B60-$C$8),1E-20)
def xcf_Toe_Model_F60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F60')
    return None
xfunctions['Toe Model!F60']=xcf_Toe_Model_F60

#IF($B60>$C$9,$D$9*($B60-$C$9),1E-20)
def xcf_Toe_Model_G60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G60')
    return None
xfunctions['Toe Model!G60']=xcf_Toe_Model_G60

#IF($B60>$C$11,$D$11*($B60-$C$11),1E-20)
def xcf_Toe_Model_H60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H60')
    return None
xfunctions['Toe Model!H60']=xcf_Toe_Model_H60

#IF($B60>$C$10,$D$10*($B60-$C$10),1E-20)
def xcf_Toe_Model_I60(): 
    try:      
        return IF(vcell("$B60","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B60","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I60')
    return None
xfunctions['Toe Model!I60']=xcf_Toe_Model_I60

#IF($B61>$C$5,$D$5*($B61-$C$5),1E-20)
def xcf_Toe_Model_C61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C61')
    return None
xfunctions['Toe Model!C61']=xcf_Toe_Model_C61

#IF($B61>$C$6,$D$6*($B61-$C$6),1E-20)
def xcf_Toe_Model_D61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D61')
    return None
xfunctions['Toe Model!D61']=xcf_Toe_Model_D61

#IF($B61>$C$7,$D$7*($B61-$C$7),1E-20)
def xcf_Toe_Model_E61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E61')
    return None
xfunctions['Toe Model!E61']=xcf_Toe_Model_E61

#IF($B61>$C$8,$D$8*($B61-$C$8),1E-20)
def xcf_Toe_Model_F61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F61')
    return None
xfunctions['Toe Model!F61']=xcf_Toe_Model_F61

#IF($B61>$C$9,$D$9*($B61-$C$9),1E-20)
def xcf_Toe_Model_G61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G61')
    return None
xfunctions['Toe Model!G61']=xcf_Toe_Model_G61

#IF($B61>$C$11,$D$11*($B61-$C$11),1E-20)
def xcf_Toe_Model_H61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H61')
    return None
xfunctions['Toe Model!H61']=xcf_Toe_Model_H61

#IF($B61>$C$10,$D$10*($B61-$C$10),1E-20)
def xcf_Toe_Model_I61(): 
    try:      
        return IF(vcell("$B61","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B61","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I61')
    return None
xfunctions['Toe Model!I61']=xcf_Toe_Model_I61

#IF($B62>$C$5,$D$5*($B62-$C$5),1E-20)
def xcf_Toe_Model_C62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C62')
    return None
xfunctions['Toe Model!C62']=xcf_Toe_Model_C62

#IF($B62>$C$6,$D$6*($B62-$C$6),1E-20)
def xcf_Toe_Model_D62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D62')
    return None
xfunctions['Toe Model!D62']=xcf_Toe_Model_D62

#IF($B62>$C$7,$D$7*($B62-$C$7),1E-20)
def xcf_Toe_Model_E62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E62')
    return None
xfunctions['Toe Model!E62']=xcf_Toe_Model_E62

#IF($B62>$C$8,$D$8*($B62-$C$8),1E-20)
def xcf_Toe_Model_F62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F62')
    return None
xfunctions['Toe Model!F62']=xcf_Toe_Model_F62

#IF($B62>$C$9,$D$9*($B62-$C$9),1E-20)
def xcf_Toe_Model_G62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G62')
    return None
xfunctions['Toe Model!G62']=xcf_Toe_Model_G62

#IF($B62>$C$11,$D$11*($B62-$C$11),1E-20)
def xcf_Toe_Model_H62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H62')
    return None
xfunctions['Toe Model!H62']=xcf_Toe_Model_H62

#IF($B62>$C$10,$D$10*($B62-$C$10),1E-20)
def xcf_Toe_Model_I62(): 
    try:      
        return IF(vcell("$B62","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B62","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I62')
    return None
xfunctions['Toe Model!I62']=xcf_Toe_Model_I62

#IF($B63>$C$5,$D$5*($B63-$C$5),1E-20)
def xcf_Toe_Model_C63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C63')
    return None
xfunctions['Toe Model!C63']=xcf_Toe_Model_C63

#IF($B63>$C$6,$D$6*($B63-$C$6),1E-20)
def xcf_Toe_Model_D63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D63')
    return None
xfunctions['Toe Model!D63']=xcf_Toe_Model_D63

#IF($B63>$C$7,$D$7*($B63-$C$7),1E-20)
def xcf_Toe_Model_E63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E63')
    return None
xfunctions['Toe Model!E63']=xcf_Toe_Model_E63

#IF($B63>$C$8,$D$8*($B63-$C$8),1E-20)
def xcf_Toe_Model_F63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F63')
    return None
xfunctions['Toe Model!F63']=xcf_Toe_Model_F63

#IF($B63>$C$9,$D$9*($B63-$C$9),1E-20)
def xcf_Toe_Model_G63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G63')
    return None
xfunctions['Toe Model!G63']=xcf_Toe_Model_G63

#IF($B63>$C$11,$D$11*($B63-$C$11),1E-20)
def xcf_Toe_Model_H63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H63')
    return None
xfunctions['Toe Model!H63']=xcf_Toe_Model_H63

#IF($B63>$C$10,$D$10*($B63-$C$10),1E-20)
def xcf_Toe_Model_I63(): 
    try:      
        return IF(vcell("$B63","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B63","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I63')
    return None
xfunctions['Toe Model!I63']=xcf_Toe_Model_I63

#IF($B64>$C$5,$D$5*($B64-$C$5),1E-20)
def xcf_Toe_Model_C64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C64')
    return None
xfunctions['Toe Model!C64']=xcf_Toe_Model_C64

#IF($B64>$C$6,$D$6*($B64-$C$6),1E-20)
def xcf_Toe_Model_D64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D64')
    return None
xfunctions['Toe Model!D64']=xcf_Toe_Model_D64

#IF($B64>$C$7,$D$7*($B64-$C$7),1E-20)
def xcf_Toe_Model_E64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E64')
    return None
xfunctions['Toe Model!E64']=xcf_Toe_Model_E64

#IF($B64>$C$8,$D$8*($B64-$C$8),1E-20)
def xcf_Toe_Model_F64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F64')
    return None
xfunctions['Toe Model!F64']=xcf_Toe_Model_F64

#IF($B64>$C$9,$D$9*($B64-$C$9),1E-20)
def xcf_Toe_Model_G64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G64')
    return None
xfunctions['Toe Model!G64']=xcf_Toe_Model_G64

#IF($B64>$C$11,$D$11*($B64-$C$11),1E-20)
def xcf_Toe_Model_H64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H64')
    return None
xfunctions['Toe Model!H64']=xcf_Toe_Model_H64

#IF($B64>$C$10,$D$10*($B64-$C$10),1E-20)
def xcf_Toe_Model_I64(): 
    try:      
        return IF(vcell("$B64","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B64","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I64')
    return None
xfunctions['Toe Model!I64']=xcf_Toe_Model_I64

#IF($B65>$C$5,$D$5*($B65-$C$5),1E-20)
def xcf_Toe_Model_C65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C65')
    return None
xfunctions['Toe Model!C65']=xcf_Toe_Model_C65

#IF($B65>$C$6,$D$6*($B65-$C$6),1E-20)
def xcf_Toe_Model_D65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D65')
    return None
xfunctions['Toe Model!D65']=xcf_Toe_Model_D65

#IF($B65>$C$7,$D$7*($B65-$C$7),1E-20)
def xcf_Toe_Model_E65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E65')
    return None
xfunctions['Toe Model!E65']=xcf_Toe_Model_E65

#IF($B65>$C$8,$D$8*($B65-$C$8),1E-20)
def xcf_Toe_Model_F65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F65')
    return None
xfunctions['Toe Model!F65']=xcf_Toe_Model_F65

#IF($B65>$C$9,$D$9*($B65-$C$9),1E-20)
def xcf_Toe_Model_G65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G65')
    return None
xfunctions['Toe Model!G65']=xcf_Toe_Model_G65

#IF($B65>$C$11,$D$11*($B65-$C$11),1E-20)
def xcf_Toe_Model_H65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H65')
    return None
xfunctions['Toe Model!H65']=xcf_Toe_Model_H65

#IF($B65>$C$10,$D$10*($B65-$C$10),1E-20)
def xcf_Toe_Model_I65(): 
    try:      
        return IF(vcell("$B65","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B65","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I65')
    return None
xfunctions['Toe Model!I65']=xcf_Toe_Model_I65

#IF($B66>$C$5,$D$5*($B66-$C$5),1E-20)
def xcf_Toe_Model_C66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C66')
    return None
xfunctions['Toe Model!C66']=xcf_Toe_Model_C66

#IF($B66>$C$6,$D$6*($B66-$C$6),1E-20)
def xcf_Toe_Model_D66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D66')
    return None
xfunctions['Toe Model!D66']=xcf_Toe_Model_D66

#IF($B66>$C$7,$D$7*($B66-$C$7),1E-20)
def xcf_Toe_Model_E66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E66')
    return None
xfunctions['Toe Model!E66']=xcf_Toe_Model_E66

#IF($B66>$C$8,$D$8*($B66-$C$8),1E-20)
def xcf_Toe_Model_F66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F66')
    return None
xfunctions['Toe Model!F66']=xcf_Toe_Model_F66

#IF($B66>$C$9,$D$9*($B66-$C$9),1E-20)
def xcf_Toe_Model_G66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G66')
    return None
xfunctions['Toe Model!G66']=xcf_Toe_Model_G66

#IF($B66>$C$11,$D$11*($B66-$C$11),1E-20)
def xcf_Toe_Model_H66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H66')
    return None
xfunctions['Toe Model!H66']=xcf_Toe_Model_H66

#IF($B66>$C$10,$D$10*($B66-$C$10),1E-20)
def xcf_Toe_Model_I66(): 
    try:      
        return IF(vcell("$B66","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B66","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I66')
    return None
xfunctions['Toe Model!I66']=xcf_Toe_Model_I66

#IF($B67>$C$5,$D$5*($B67-$C$5),1E-20)
def xcf_Toe_Model_C67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$5","Toe Model"),vcell("$D$5","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$5","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C67')
    return None
xfunctions['Toe Model!C67']=xcf_Toe_Model_C67

#IF($B67>$C$6,$D$6*($B67-$C$6),1E-20)
def xcf_Toe_Model_D67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$6","Toe Model"),vcell("$D$6","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$6","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D67')
    return None
xfunctions['Toe Model!D67']=xcf_Toe_Model_D67

#IF($B67>$C$7,$D$7*($B67-$C$7),1E-20)
def xcf_Toe_Model_E67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$7","Toe Model"),vcell("$D$7","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$7","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E67')
    return None
xfunctions['Toe Model!E67']=xcf_Toe_Model_E67

#IF($B67>$C$8,$D$8*($B67-$C$8),1E-20)
def xcf_Toe_Model_F67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$8","Toe Model"),vcell("$D$8","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$8","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F67')
    return None
xfunctions['Toe Model!F67']=xcf_Toe_Model_F67

#IF($B67>$C$9,$D$9*($B67-$C$9),1E-20)
def xcf_Toe_Model_G67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$9","Toe Model"),vcell("$D$9","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$9","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G67')
    return None
xfunctions['Toe Model!G67']=xcf_Toe_Model_G67

#IF($B67>$C$11,$D$11*($B67-$C$11),1E-20)
def xcf_Toe_Model_H67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$11","Toe Model"),vcell("$D$11","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$11","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H67')
    return None
xfunctions['Toe Model!H67']=xcf_Toe_Model_H67

#IF($B67>$C$10,$D$10*($B67-$C$10),1E-20)
def xcf_Toe_Model_I67(): 
    try:      
        return IF(vcell("$B67","Toe Model")>vcell("$C$10","Toe Model"),vcell("$D$10","Toe Model")*(vcell("$B67","Toe Model")-vcell("$C$10","Toe Model")),1E-20)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I67')
    return None
xfunctions['Toe Model!I67']=xcf_Toe_Model_I67

#B18
def xcf_Toe_Model_B72(): 
    try:      
        return vcell("B18","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B72')
    return None
xfunctions['Toe Model!B72']=xcf_Toe_Model_B72

#C18
def xcf_Toe_Model_C72(): 
    try:      
        return vcell("C18","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C72')
    return None
xfunctions['Toe Model!C72']=xcf_Toe_Model_C72

#G$5
def xcf_Toe_Model_D72(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D72')
    return None
xfunctions['Toe Model!D72']=xcf_Toe_Model_D72

#B19
def xcf_Toe_Model_B73(): 
    try:      
        return vcell("B19","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B73')
    return None
xfunctions['Toe Model!B73']=xcf_Toe_Model_B73

#C19
def xcf_Toe_Model_C73(): 
    try:      
        return vcell("C19","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C73')
    return None
xfunctions['Toe Model!C73']=xcf_Toe_Model_C73

#G$5
def xcf_Toe_Model_D73(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D73')
    return None
xfunctions['Toe Model!D73']=xcf_Toe_Model_D73

#B20
def xcf_Toe_Model_B74(): 
    try:      
        return vcell("B20","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B74')
    return None
xfunctions['Toe Model!B74']=xcf_Toe_Model_B74

#C20
def xcf_Toe_Model_C74(): 
    try:      
        return vcell("C20","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C74')
    return None
xfunctions['Toe Model!C74']=xcf_Toe_Model_C74

#G$5
def xcf_Toe_Model_D74(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D74')
    return None
xfunctions['Toe Model!D74']=xcf_Toe_Model_D74

#B21
def xcf_Toe_Model_B75(): 
    try:      
        return vcell("B21","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B75')
    return None
xfunctions['Toe Model!B75']=xcf_Toe_Model_B75

#C21
def xcf_Toe_Model_C75(): 
    try:      
        return vcell("C21","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C75')
    return None
xfunctions['Toe Model!C75']=xcf_Toe_Model_C75

#G$5
def xcf_Toe_Model_D75(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D75')
    return None
xfunctions['Toe Model!D75']=xcf_Toe_Model_D75

#B22
def xcf_Toe_Model_B76(): 
    try:      
        return vcell("B22","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B76')
    return None
xfunctions['Toe Model!B76']=xcf_Toe_Model_B76

#C22
def xcf_Toe_Model_C76(): 
    try:      
        return vcell("C22","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C76')
    return None
xfunctions['Toe Model!C76']=xcf_Toe_Model_C76

#G$5
def xcf_Toe_Model_D76(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D76')
    return None
xfunctions['Toe Model!D76']=xcf_Toe_Model_D76

#B23
def xcf_Toe_Model_B77(): 
    try:      
        return vcell("B23","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B77')
    return None
xfunctions['Toe Model!B77']=xcf_Toe_Model_B77

#C23
def xcf_Toe_Model_C77(): 
    try:      
        return vcell("C23","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C77')
    return None
xfunctions['Toe Model!C77']=xcf_Toe_Model_C77

#G$5
def xcf_Toe_Model_D77(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D77')
    return None
xfunctions['Toe Model!D77']=xcf_Toe_Model_D77

#B24
def xcf_Toe_Model_B78(): 
    try:      
        return vcell("B24","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B78')
    return None
xfunctions['Toe Model!B78']=xcf_Toe_Model_B78

#C24
def xcf_Toe_Model_C78(): 
    try:      
        return vcell("C24","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C78')
    return None
xfunctions['Toe Model!C78']=xcf_Toe_Model_C78

#G$5
def xcf_Toe_Model_D78(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D78')
    return None
xfunctions['Toe Model!D78']=xcf_Toe_Model_D78

#B25
def xcf_Toe_Model_B79(): 
    try:      
        return vcell("B25","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B79')
    return None
xfunctions['Toe Model!B79']=xcf_Toe_Model_B79

#C25
def xcf_Toe_Model_C79(): 
    try:      
        return vcell("C25","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C79')
    return None
xfunctions['Toe Model!C79']=xcf_Toe_Model_C79

#G$5
def xcf_Toe_Model_D79(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D79')
    return None
xfunctions['Toe Model!D79']=xcf_Toe_Model_D79

#B26
def xcf_Toe_Model_B80(): 
    try:      
        return vcell("B26","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B80')
    return None
xfunctions['Toe Model!B80']=xcf_Toe_Model_B80

#C26
def xcf_Toe_Model_C80(): 
    try:      
        return vcell("C26","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C80')
    return None
xfunctions['Toe Model!C80']=xcf_Toe_Model_C80

#G$5
def xcf_Toe_Model_D80(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D80')
    return None
xfunctions['Toe Model!D80']=xcf_Toe_Model_D80

#B27
def xcf_Toe_Model_B81(): 
    try:      
        return vcell("B27","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B81')
    return None
xfunctions['Toe Model!B81']=xcf_Toe_Model_B81

#C27
def xcf_Toe_Model_C81(): 
    try:      
        return vcell("C27","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C81')
    return None
xfunctions['Toe Model!C81']=xcf_Toe_Model_C81

#G$5
def xcf_Toe_Model_D81(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D81')
    return None
xfunctions['Toe Model!D81']=xcf_Toe_Model_D81

#B28
def xcf_Toe_Model_B82(): 
    try:      
        return vcell("B28","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B82')
    return None
xfunctions['Toe Model!B82']=xcf_Toe_Model_B82

#C28
def xcf_Toe_Model_C82(): 
    try:      
        return vcell("C28","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C82')
    return None
xfunctions['Toe Model!C82']=xcf_Toe_Model_C82

#G$5
def xcf_Toe_Model_D82(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D82')
    return None
xfunctions['Toe Model!D82']=xcf_Toe_Model_D82

#B29
def xcf_Toe_Model_B83(): 
    try:      
        return vcell("B29","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B83')
    return None
xfunctions['Toe Model!B83']=xcf_Toe_Model_B83

#C29
def xcf_Toe_Model_C83(): 
    try:      
        return vcell("C29","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C83')
    return None
xfunctions['Toe Model!C83']=xcf_Toe_Model_C83

#G$5
def xcf_Toe_Model_D83(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D83')
    return None
xfunctions['Toe Model!D83']=xcf_Toe_Model_D83

#B30
def xcf_Toe_Model_B84(): 
    try:      
        return vcell("B30","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B84')
    return None
xfunctions['Toe Model!B84']=xcf_Toe_Model_B84

#C30
def xcf_Toe_Model_C84(): 
    try:      
        return vcell("C30","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C84')
    return None
xfunctions['Toe Model!C84']=xcf_Toe_Model_C84

#G$5
def xcf_Toe_Model_D84(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D84')
    return None
xfunctions['Toe Model!D84']=xcf_Toe_Model_D84

#B31
def xcf_Toe_Model_B85(): 
    try:      
        return vcell("B31","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B85')
    return None
xfunctions['Toe Model!B85']=xcf_Toe_Model_B85

#C31
def xcf_Toe_Model_C85(): 
    try:      
        return vcell("C31","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C85')
    return None
xfunctions['Toe Model!C85']=xcf_Toe_Model_C85

#G$5
def xcf_Toe_Model_D85(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D85')
    return None
xfunctions['Toe Model!D85']=xcf_Toe_Model_D85

#B32
def xcf_Toe_Model_B86(): 
    try:      
        return vcell("B32","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B86')
    return None
xfunctions['Toe Model!B86']=xcf_Toe_Model_B86

#C32
def xcf_Toe_Model_C86(): 
    try:      
        return vcell("C32","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C86')
    return None
xfunctions['Toe Model!C86']=xcf_Toe_Model_C86

#G$5
def xcf_Toe_Model_D86(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D86')
    return None
xfunctions['Toe Model!D86']=xcf_Toe_Model_D86

#B33
def xcf_Toe_Model_B87(): 
    try:      
        return vcell("B33","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B87')
    return None
xfunctions['Toe Model!B87']=xcf_Toe_Model_B87

#C33
def xcf_Toe_Model_C87(): 
    try:      
        return vcell("C33","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C87')
    return None
xfunctions['Toe Model!C87']=xcf_Toe_Model_C87

#G$5
def xcf_Toe_Model_D87(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D87')
    return None
xfunctions['Toe Model!D87']=xcf_Toe_Model_D87

#B34
def xcf_Toe_Model_B88(): 
    try:      
        return vcell("B34","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B88')
    return None
xfunctions['Toe Model!B88']=xcf_Toe_Model_B88

#C34
def xcf_Toe_Model_C88(): 
    try:      
        return vcell("C34","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C88')
    return None
xfunctions['Toe Model!C88']=xcf_Toe_Model_C88

#G$5
def xcf_Toe_Model_D88(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D88')
    return None
xfunctions['Toe Model!D88']=xcf_Toe_Model_D88

#B35-MIN(MAX(SQRT((B35-G25)^2+(C35-H25)^2),0.000001953125),$I62*$G$5)*SIN(RADIANS($D35))
def xcf_Toe_Model_B89(): 
    try:      
        return vcell("B35","Toe Model")-MIN(MAX(SQRT((vcell("B35","Toe Model")-vcell("G25","Toe Model"))**2+(vcell("C35","Toe Model")-vcell("H25","Toe Model"))**2),0.000001953125),vcell("$I62","Toe Model")*vcell("$G$5","Toe Model"))*SIN(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B89')
    return None
xfunctions['Toe Model!B89']=xcf_Toe_Model_B89

#C35-MIN(MAX(SQRT((B35-G25)^2+(C35-H25)^2),0.000001953125),$I62*$G$5)*COS(RADIANS($D35))
def xcf_Toe_Model_C89(): 
    try:      
        return vcell("C35","Toe Model")-MIN(MAX(SQRT((vcell("B35","Toe Model")-vcell("G25","Toe Model"))**2+(vcell("C35","Toe Model")-vcell("H25","Toe Model"))**2),0.000001953125),vcell("$I62","Toe Model")*vcell("$G$5","Toe Model"))*COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C89')
    return None
xfunctions['Toe Model!C89']=xcf_Toe_Model_C89

#$G$5-MIN(MAX(SQRT((B35-G25)^2+(C35-H25)^2),0.000001953125),$I62*$G$5)/I62
def xcf_Toe_Model_D89(): 
    try:      
        return vcell("$G$5","Toe Model")-MIN(MAX(SQRT((vcell("B35","Toe Model")-vcell("G25","Toe Model"))**2+(vcell("C35","Toe Model")-vcell("H25","Toe Model"))**2),0.000001953125),vcell("$I62","Toe Model")*vcell("$G$5","Toe Model"))/vcell("I62","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D89')
    return None
xfunctions['Toe Model!D89']=xcf_Toe_Model_D89

#B36-MIN(MAX(SQRT((B36-G26)^2+(C36-H26)^2),0.000001953125),$I63*$G$5)*SIN(RADIANS($D36))
def xcf_Toe_Model_B90(): 
    try:      
        return vcell("B36","Toe Model")-MIN(MAX(SQRT((vcell("B36","Toe Model")-vcell("G26","Toe Model"))**2+(vcell("C36","Toe Model")-vcell("H26","Toe Model"))**2),0.000001953125),vcell("$I63","Toe Model")*vcell("$G$5","Toe Model"))*SIN(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B90')
    return None
xfunctions['Toe Model!B90']=xcf_Toe_Model_B90

#C36-MIN(MAX(SQRT((B36-G26)^2+(C36-H26)^2),0.000001953125),$I63*$G$5)*COS(RADIANS($D36))
def xcf_Toe_Model_C90(): 
    try:      
        return vcell("C36","Toe Model")-MIN(MAX(SQRT((vcell("B36","Toe Model")-vcell("G26","Toe Model"))**2+(vcell("C36","Toe Model")-vcell("H26","Toe Model"))**2),0.000001953125),vcell("$I63","Toe Model")*vcell("$G$5","Toe Model"))*COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C90')
    return None
xfunctions['Toe Model!C90']=xcf_Toe_Model_C90

#$G$5-MIN(MAX(SQRT((B36-G26)^2+(C36-H26)^2),0.000001953125),$I63*$G$5)/I63
def xcf_Toe_Model_D90(): 
    try:      
        return vcell("$G$5","Toe Model")-MIN(MAX(SQRT((vcell("B36","Toe Model")-vcell("G26","Toe Model"))**2+(vcell("C36","Toe Model")-vcell("H26","Toe Model"))**2),0.000001953125),vcell("$I63","Toe Model")*vcell("$G$5","Toe Model"))/vcell("I63","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D90')
    return None
xfunctions['Toe Model!D90']=xcf_Toe_Model_D90

#B37-MIN(MAX(SQRT((B37-G27)^2+(C37-H27)^2),0.000001953125),$I64*$G$5)*SIN(RADIANS($D37))
def xcf_Toe_Model_B91(): 
    try:      
        return vcell("B37","Toe Model")-MIN(MAX(SQRT((vcell("B37","Toe Model")-vcell("G27","Toe Model"))**2+(vcell("C37","Toe Model")-vcell("H27","Toe Model"))**2),0.000001953125),vcell("$I64","Toe Model")*vcell("$G$5","Toe Model"))*SIN(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B91')
    return None
xfunctions['Toe Model!B91']=xcf_Toe_Model_B91

#C37-MIN(MAX(SQRT((B37-G27)^2+(C37-H27)^2),0.000001953125),$I64*$G$5)*COS(RADIANS($D37))
def xcf_Toe_Model_C91(): 
    try:      
        return vcell("C37","Toe Model")-MIN(MAX(SQRT((vcell("B37","Toe Model")-vcell("G27","Toe Model"))**2+(vcell("C37","Toe Model")-vcell("H27","Toe Model"))**2),0.000001953125),vcell("$I64","Toe Model")*vcell("$G$5","Toe Model"))*COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C91')
    return None
xfunctions['Toe Model!C91']=xcf_Toe_Model_C91

#$G$5-MIN(MAX(SQRT((B37-G27)^2+(C37-H27)^2),0.000001953125),$I64*$G$5)/I64
def xcf_Toe_Model_D91(): 
    try:      
        return vcell("$G$5","Toe Model")-MIN(MAX(SQRT((vcell("B37","Toe Model")-vcell("G27","Toe Model"))**2+(vcell("C37","Toe Model")-vcell("H27","Toe Model"))**2),0.000001953125),vcell("$I64","Toe Model")*vcell("$G$5","Toe Model"))/vcell("I64","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D91')
    return None
xfunctions['Toe Model!D91']=xcf_Toe_Model_D91

#B38-MIN(MAX(SQRT((B38-G28)^2+(C38-H28)^2),0.000001953125),$I65*$G$5)*SIN(RADIANS($D38))
def xcf_Toe_Model_B92(): 
    try:      
        return vcell("B38","Toe Model")-MIN(MAX(SQRT((vcell("B38","Toe Model")-vcell("G28","Toe Model"))**2+(vcell("C38","Toe Model")-vcell("H28","Toe Model"))**2),0.000001953125),vcell("$I65","Toe Model")*vcell("$G$5","Toe Model"))*SIN(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B92')
    return None
xfunctions['Toe Model!B92']=xcf_Toe_Model_B92

#C38-MIN(MAX(SQRT((B38-G28)^2+(C38-H28)^2),0.000001953125),$I65*$G$5)*COS(RADIANS($D38))
def xcf_Toe_Model_C92(): 
    try:      
        return vcell("C38","Toe Model")-MIN(MAX(SQRT((vcell("B38","Toe Model")-vcell("G28","Toe Model"))**2+(vcell("C38","Toe Model")-vcell("H28","Toe Model"))**2),0.000001953125),vcell("$I65","Toe Model")*vcell("$G$5","Toe Model"))*COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C92')
    return None
xfunctions['Toe Model!C92']=xcf_Toe_Model_C92

#$G$5-MIN(MAX(SQRT((B38-G28)^2+(C38-H28)^2),0.000001953125),$I65*$G$5)/I65
def xcf_Toe_Model_D92(): 
    try:      
        return vcell("$G$5","Toe Model")-MIN(MAX(SQRT((vcell("B38","Toe Model")-vcell("G28","Toe Model"))**2+(vcell("C38","Toe Model")-vcell("H28","Toe Model"))**2),0.000001953125),vcell("$I65","Toe Model")*vcell("$G$5","Toe Model"))/vcell("I65","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D92')
    return None
xfunctions['Toe Model!D92']=xcf_Toe_Model_D92

#B39-MIN(0.000001953125,$I66*$G$5)*SIN(RADIANS($D39))
def xcf_Toe_Model_B93(): 
    try:      
        return vcell("B39","Toe Model")-MIN(0.000001953125,vcell("$I66","Toe Model")*vcell("$G$5","Toe Model"))*SIN(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B93')
    return None
xfunctions['Toe Model!B93']=xcf_Toe_Model_B93

#C39-MIN(0.000001953125,$I66*$G$5)*COS(RADIANS($D39))
def xcf_Toe_Model_C93(): 
    try:      
        return vcell("C39","Toe Model")-MIN(0.000001953125,vcell("$I66","Toe Model")*vcell("$G$5","Toe Model"))*COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C93')
    return None
xfunctions['Toe Model!C93']=xcf_Toe_Model_C93

#$G$5-MIN(0.000001953125,$I66*$G$5)/I66
def xcf_Toe_Model_D93(): 
    try:      
        return vcell("$G$5","Toe Model")-MIN(0.000001953125,vcell("$I66","Toe Model")*vcell("$G$5","Toe Model"))/vcell("I66","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D93')
    return None
xfunctions['Toe Model!D93']=xcf_Toe_Model_D93

#B40
def xcf_Toe_Model_B94(): 
    try:      
        return vcell("B40","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B94')
    return None
xfunctions['Toe Model!B94']=xcf_Toe_Model_B94

#C40
def xcf_Toe_Model_C94(): 
    try:      
        return vcell("C40","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C94')
    return None
xfunctions['Toe Model!C94']=xcf_Toe_Model_C94

#G$5
def xcf_Toe_Model_D94(): 
    try:      
        return vcell("G$5","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D94')
    return None
xfunctions['Toe Model!D94']=xcf_Toe_Model_D94

#IF(AND($C72<=$B$4,$C72>$B$5),1,IF(AND($C72<=$B$5,$C72>$B$6),2,IF(AND($C72<=$B$6,$C72>$B$7),3,IF(AND($C72<=$B$7,$C72>$B$8),4,IF(AND($C72<=$B$8,$C72>$B$9),5,6)))))
def xcf_Toe_Model_B101(): 
    try:      
        return IF(AND(vcell("$C72","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C72","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C72","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C72","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C72","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C72","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C72","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C72","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C72","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C72","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B101')
    return None
xfunctions['Toe Model!B101']=xcf_Toe_Model_B101

#IF(ISNUMBER($B101),MIN(CHOOSE($B101,$K18,$L18,$M18,$N18,$O18,$P18),CHOOSE($B101,$C45,$D45,$E45,$F45,$G45,$H45)*$D72),0)
def xcf_Toe_Model_C101(): 
    try:      
        return IF(ISNUMBER(vcell("$B101","Toe Model")),MIN(CHOOSE(vcell("$B101","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model"),vcell("$P18","Toe Model")),CHOOSE(vcell("$B101","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model"),vcell("$H45","Toe Model"))*vcell("$D72","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C101')
    return None
xfunctions['Toe Model!C101']=xcf_Toe_Model_C101

#$D72-$C101/CHOOSE($B101,$C45,$D45,$E45,$F45,$G45,$H45)
def xcf_Toe_Model_D101(): 
    try:      
        return vcell("$D72","Toe Model")-vcell("$C101","Toe Model")/CHOOSE(vcell("$B101","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model"),vcell("$H45","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D101')
    return None
xfunctions['Toe Model!D101']=xcf_Toe_Model_D101

#IF(D101>0,MIN(IF($B101+E$98>=6,$H45,CHOOSE($B101+E$98,$C45,$D45,$E45,$F45,$G45))*D101,IF($B101+E$98>=6,$P18,CHOOSE($B101+E$98,$K18,$L18,$M18,$N18,$O18))-IF($B101>=6,$P18,CHOOSE($B101,$K18,$L18,$M18,$N18,$O18))),0)
def xcf_Toe_Model_E101(): 
    try:      
        return IF(vcell("D101","Toe Model")>0,MIN(IF(vcell("$B101","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("E$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))*vcell("D101","Toe Model"),IF(vcell("$B101","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("E$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))-IF(vcell("$B101","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E101')
    return None
xfunctions['Toe Model!E101']=xcf_Toe_Model_E101

#D101-E101/IF($B101+$E$98>=6,$H45,CHOOSE($B101+$E$98,$C45,$D45,$E45,$F45,$G45))
def xcf_Toe_Model_F101(): 
    try:      
        return vcell("D101","Toe Model")-vcell("E101","Toe Model")/IF(vcell("$B101","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F101')
    return None
xfunctions['Toe Model!F101']=xcf_Toe_Model_F101

#IF(F101>0,MIN(IF($B101+G$98>=6,$H45,CHOOSE($B101+G$98,$C45,$D45,$E45,$F45,$G45))*F101,IF($B101+G$98>=6,$P18,CHOOSE($B101+G$98,$K18,$L18,$M18,$N18,$O18))-IF($B101+E$98>=6,$P18,CHOOSE($B101+E$98,$K18,$L18,$M18,$N18,$O18))),0)
def xcf_Toe_Model_G101(): 
    try:      
        return IF(vcell("F101","Toe Model")>0,MIN(IF(vcell("$B101","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("G$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))*vcell("F101","Toe Model"),IF(vcell("$B101","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("G$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))-IF(vcell("$B101","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("E$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G101')
    return None
xfunctions['Toe Model!G101']=xcf_Toe_Model_G101

#F101-G101/IF($B101+$G$98>=6,$H45,CHOOSE($B101+$G$98,$C45,$D45,$E45,$F45,$G45))
def xcf_Toe_Model_H101(): 
    try:      
        return vcell("F101","Toe Model")-vcell("G101","Toe Model")/IF(vcell("$B101","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H101')
    return None
xfunctions['Toe Model!H101']=xcf_Toe_Model_H101

#IF(H101>0,MIN(IF($B101+I$98>=6,$H45,CHOOSE($B101+I$98,$C45,$D45,$E45,$F45,$G45))*H101,IF($B101+I$98>=6,$P18,CHOOSE($B101+I$98,$K18,$L18,$M18,$N18,$O18))-IF($B101+G$98>=6,$P18,CHOOSE($B101+G$98,$K18,$L18,$M18,$N18,$O18))),0)
def xcf_Toe_Model_I101(): 
    try:      
        return IF(vcell("H101","Toe Model")>0,MIN(IF(vcell("$B101","Toe Model")+vcell("I$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("I$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))*vcell("H101","Toe Model"),IF(vcell("$B101","Toe Model")+vcell("I$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("I$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))-IF(vcell("$B101","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("G$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I101')
    return None
xfunctions['Toe Model!I101']=xcf_Toe_Model_I101

#H101-I101/IF($B101+$I$98>=6,$H45,CHOOSE($B101+$I$98,$C45,$D45,$E45,$F45,$G45))
def xcf_Toe_Model_J101(): 
    try:      
        return vcell("H101","Toe Model")-vcell("I101","Toe Model")/IF(vcell("$B101","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J101')
    return None
xfunctions['Toe Model!J101']=xcf_Toe_Model_J101

#IF(J101>0,MIN(IF($B101+K$98>=6,$H45,CHOOSE($B101+K$98,$C45,$D45,$E45,$F45,$G45))*J101,IF($B101+K$98>=6,$P18,CHOOSE($B101+K$98,$K18,$L18,$M18,$N18,$O18))-IF($B101+I$98>=6,$P18,CHOOSE($B101+I$98,$K18,$L18,$M18,$N18,$O18))),0)
def xcf_Toe_Model_K101(): 
    try:      
        return IF(vcell("J101","Toe Model")>0,MIN(IF(vcell("$B101","Toe Model")+vcell("K$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("K$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))*vcell("J101","Toe Model"),IF(vcell("$B101","Toe Model")+vcell("K$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("K$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))-IF(vcell("$B101","Toe Model")+vcell("I$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("I$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K101')
    return None
xfunctions['Toe Model!K101']=xcf_Toe_Model_K101

#J101-K101/IF($B101+$K$98>=6,$H45,CHOOSE($B101+$K$98,$C45,$D45,$E45,$F45,$G45))
def xcf_Toe_Model_L101(): 
    try:      
        return vcell("J101","Toe Model")-vcell("K101","Toe Model")/IF(vcell("$B101","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L101')
    return None
xfunctions['Toe Model!L101']=xcf_Toe_Model_L101

#IF(L101>0,MIN(IF($B101+M$98>=6,$H45,CHOOSE($B101+M$98,$C45,$D45,$E45,$F45,$G45))*L101,IF($B101+M$98>=6,$P18,CHOOSE($B101+M$98,$K18,$L18,$M18,$N18,$O18))-IF($B101+K$98>=6,$P18,CHOOSE($B101+K$98,$K18,$L18,$M18,$N18,$O18))),0)
def xcf_Toe_Model_M101(): 
    try:      
        return IF(vcell("L101","Toe Model")>0,MIN(IF(vcell("$B101","Toe Model")+vcell("M$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("M$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))*vcell("L101","Toe Model"),IF(vcell("$B101","Toe Model")+vcell("M$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("M$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))-IF(vcell("$B101","Toe Model")+vcell("K$98","Toe Model")>=6,vcell("$P18","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("K$98","Toe Model"),vcell("$K18","Toe Model"),vcell("$L18","Toe Model"),vcell("$M18","Toe Model"),vcell("$N18","Toe Model"),vcell("$O18","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M101')
    return None
xfunctions['Toe Model!M101']=xcf_Toe_Model_M101

#L101-M101/IF($B101+$M$98>=6,$H45,CHOOSE($B101+$M$98,$C45,$D45,$E45,$F45,$G45))
def xcf_Toe_Model_N101(): 
    try:      
        return vcell("L101","Toe Model")-vcell("M101","Toe Model")/IF(vcell("$B101","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H45","Toe Model"),CHOOSE(vcell("$B101","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C45","Toe Model"),vcell("$D45","Toe Model"),vcell("$E45","Toe Model"),vcell("$F45","Toe Model"),vcell("$G45","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N101')
    return None
xfunctions['Toe Model!N101']=xcf_Toe_Model_N101

#B72-SUM($C101,$E101,$G101,$I101,$K101,$M101)*SIN(RADIANS($D18))
def xcf_Toe_Model_O101(): 
    try:      
        return vcell("B72","Toe Model")-SUM(vcell("$C101","Toe Model"),vcell("$E101","Toe Model"),vcell("$G101","Toe Model"),vcell("$I101","Toe Model"),vcell("$K101","Toe Model"),vcell("$M101","Toe Model"))*SIN(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O101')
    return None
xfunctions['Toe Model!O101']=xcf_Toe_Model_O101

#C72+IF($D18==360,1,-1)*SUM($C101,$E101,$G101,$I101,$K101,$M101)*COS(RADIANS($D18))
def xcf_Toe_Model_P101(): 
    try:      
        return vcell("C72","Toe Model")+IF(vcell("$D18","Toe Model")==360,1,-1)*SUM(vcell("$C101","Toe Model"),vcell("$E101","Toe Model"),vcell("$G101","Toe Model"),vcell("$I101","Toe Model"),vcell("$K101","Toe Model"),vcell("$M101","Toe Model"))*COS(RADIANS(vcell("$D18","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P101')
    return None
xfunctions['Toe Model!P101']=xcf_Toe_Model_P101

#O101
def xcf_Toe_Model_Q101(): 
    try:      
        return vcell("O101","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q101')
    return None
xfunctions['Toe Model!Q101']=xcf_Toe_Model_Q101

#MAX(C72+IF($D18==360,1,-1)*SUM($C101,$E101,$G101,$I101,$K101,$M101)*COS(RADIANS($D18)),MIN(C$18:C$40))
def xcf_Toe_Model_R101(): 
    try:      
        return MAX(vcell("C72","Toe Model")+IF(vcell("$D18","Toe Model")==360,1,-1)*SUM(vcell("$C101","Toe Model"),vcell("$E101","Toe Model"),vcell("$G101","Toe Model"),vcell("$I101","Toe Model"),vcell("$K101","Toe Model"),vcell("$M101","Toe Model"))*COS(RADIANS(vcell("$D18","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R101')
    return None
xfunctions['Toe Model!R101']=xcf_Toe_Model_R101

#IF(AND($C73<=$B$4,$C73>$B$5),1,IF(AND($C73<=$B$5,$C73>$B$6),2,IF(AND($C73<=$B$6,$C73>$B$7),3,IF(AND($C73<=$B$7,$C73>$B$8),4,IF(AND($C73<=$B$8,$C73>$B$9),5,6)))))
def xcf_Toe_Model_B102(): 
    try:      
        return IF(AND(vcell("$C73","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C73","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C73","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C73","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C73","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C73","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C73","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C73","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C73","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C73","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B102')
    return None
xfunctions['Toe Model!B102']=xcf_Toe_Model_B102

#IF(ISNUMBER($B102),MIN(CHOOSE($B102,$K19,$L19,$M19,$N19,$O19,$P19),CHOOSE($B102,$C46,$D46,$E46,$F46,$G46,$H46)*$D73),0)
def xcf_Toe_Model_C102(): 
    try:      
        return IF(ISNUMBER(vcell("$B102","Toe Model")),MIN(CHOOSE(vcell("$B102","Toe Model"),vcell("$K19","Toe Model"),vcell("$L19","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model"),vcell("$P19","Toe Model")),CHOOSE(vcell("$B102","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model"),vcell("$H46","Toe Model"))*vcell("$D73","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C102')
    return None
xfunctions['Toe Model!C102']=xcf_Toe_Model_C102

#$D73-$C102/CHOOSE($B102,$C46,$D46,$E46,$F46,$G46,$H46)
def xcf_Toe_Model_D102(): 
    try:      
        return vcell("$D73","Toe Model")-vcell("$C102","Toe Model")/CHOOSE(vcell("$B102","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model"),vcell("$H46","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D102')
    return None
xfunctions['Toe Model!D102']=xcf_Toe_Model_D102

#IF(D102>0,MIN(IF($B102+E$98>=6,$H46,CHOOSE($B102+E$98,$C46,$D46,$E46,$F46,$G46))*D102,IF($B102+E$98>=6,$P19,CHOOSE($B102+E$98,$K19,$L19,$M19,$N19,$O19))-IF($B102>=6,$P19,CHOOSE($B102,$K19,$L19,$M19,$N19,$O19))),0)
def xcf_Toe_Model_E102(): 
    try:      
        return IF(vcell("D102","Toe Model")>0,MIN(IF(vcell("$B102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("E$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))*vcell("D102","Toe Model"),IF(vcell("$B102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P19","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("E$98","Toe Model"),vcell("$K19","Toe Model"),vcell("$L19","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model")))-IF(vcell("$B102","Toe Model")>=6,vcell("$P19","Toe Model"),CHOOSE(vcell("$B102","Toe Model"),vcell("$K19","Toe Model"),vcell("$L19","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E102')
    return None
xfunctions['Toe Model!E102']=xcf_Toe_Model_E102

#D102-E102/IF($B102+$E$98>=6,$H46,CHOOSE($B102+$E$98,$C46,$D46,$E46,$F46,$G46))
def xcf_Toe_Model_F102(): 
    try:      
        return vcell("D102","Toe Model")-vcell("E102","Toe Model")/IF(vcell("$B102","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F102')
    return None
xfunctions['Toe Model!F102']=xcf_Toe_Model_F102

#IF(F102>0,MIN(IF($B102+G$98>=6,$H46,CHOOSE($B102+G$98,$C46,$D46,$E46,$F46,$G46))*F102,IF($B102+G$98>=6,$P19,CHOOSE($B102+G$98,$K19,$L19,$M19,$N19,$O19))-IF($B102+E$98>=6,$P19,CHOOSE($B102+E$98,$K19,$L19,$M19,$N19,$O19))),0)
def xcf_Toe_Model_G102(): 
    try:      
        return IF(vcell("F102","Toe Model")>0,MIN(IF(vcell("$B102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("G$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))*vcell("F102","Toe Model"),IF(vcell("$B102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P19","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("G$98","Toe Model"),vcell("$K19","Toe Model"),vcell("$L19","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model")))-IF(vcell("$B102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P19","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("E$98","Toe Model"),vcell("$K19","Toe Model"),vcell("$L19","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G102')
    return None
xfunctions['Toe Model!G102']=xcf_Toe_Model_G102

#F102-G102/IF($B102+$G$98>=6,$H46,CHOOSE($B102+$G$98,$C46,$D46,$E46,$F46,$G46))
def xcf_Toe_Model_H102(): 
    try:      
        return vcell("F102","Toe Model")-vcell("G102","Toe Model")/IF(vcell("$B102","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H102')
    return None
xfunctions['Toe Model!H102']=xcf_Toe_Model_H102

#IF(H102>0,MIN(IF($D102+G$98>=6,$J46,CHOOSE($D102+G$98,$E46,$F46,$G46,$H46,$I46))*H102,IF($D102+G$98>=6,$R19,CHOOSE($D102+G$98,$M19,$N19,$O19,$P19,$Q19))-IF($D102+E$98>=6,$R19,CHOOSE($D102+E$98,$M19,$N19,$O19,$P19,$Q19))),0)
def xcf_Toe_Model_I102(): 
    try:      
        return IF(vcell("H102","Toe Model")>0,MIN(IF(vcell("$D102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J46","Toe Model"),CHOOSE(vcell("$D102","Toe Model")+vcell("G$98","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model"),vcell("$H46","Toe Model"),vcell("$I46","Toe Model")))*vcell("H102","Toe Model"),IF(vcell("$D102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R19","Toe Model"),CHOOSE(vcell("$D102","Toe Model")+vcell("G$98","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model"),vcell("$P19","Toe Model"),vcell("$Q19","Toe Model")))-IF(vcell("$D102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R19","Toe Model"),CHOOSE(vcell("$D102","Toe Model")+vcell("E$98","Toe Model"),vcell("$M19","Toe Model"),vcell("$N19","Toe Model"),vcell("$O19","Toe Model"),vcell("$P19","Toe Model"),vcell("$Q19","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I102')
    return None
xfunctions['Toe Model!I102']=xcf_Toe_Model_I102

#H102-I102/IF($B102+$I$98>=6,$H46,CHOOSE($B102+$I$98,$C46,$D46,$E46,$F46,$G46))
def xcf_Toe_Model_J102(): 
    try:      
        return vcell("H102","Toe Model")-vcell("I102","Toe Model")/IF(vcell("$B102","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J102')
    return None
xfunctions['Toe Model!J102']=xcf_Toe_Model_J102

#IF(J102>0,MIN(IF($F102+G$98>=6,$L46,CHOOSE($F102+G$98,$G46,$H46,$I46,$J46,$K46))*J102,IF($F102+G$98>=6,$T19,CHOOSE($F102+G$98,$O19,$P19,$Q19,$R19,$S19))-IF($F102+E$98>=6,$T19,CHOOSE($F102+E$98,$O19,$P19,$Q19,$R19,$S19))),0)
def xcf_Toe_Model_K102(): 
    try:      
        return IF(vcell("J102","Toe Model")>0,MIN(IF(vcell("$F102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L46","Toe Model"),CHOOSE(vcell("$F102","Toe Model")+vcell("G$98","Toe Model"),vcell("$G46","Toe Model"),vcell("$H46","Toe Model"),vcell("$I46","Toe Model"),vcell("$J46","Toe Model"),vcell("$K46","Toe Model")))*vcell("J102","Toe Model"),IF(vcell("$F102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T19","Toe Model"),CHOOSE(vcell("$F102","Toe Model")+vcell("G$98","Toe Model"),vcell("$O19","Toe Model"),vcell("$P19","Toe Model"),vcell("$Q19","Toe Model"),vcell("$R19","Toe Model"),vcell("$S19","Toe Model")))-IF(vcell("$F102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T19","Toe Model"),CHOOSE(vcell("$F102","Toe Model")+vcell("E$98","Toe Model"),vcell("$O19","Toe Model"),vcell("$P19","Toe Model"),vcell("$Q19","Toe Model"),vcell("$R19","Toe Model"),vcell("$S19","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K102')
    return None
xfunctions['Toe Model!K102']=xcf_Toe_Model_K102

#J102-K102/IF($B102+$K$98>=6,$H46,CHOOSE($B102+$K$98,$C46,$D46,$E46,$F46,$G46))
def xcf_Toe_Model_L102(): 
    try:      
        return vcell("J102","Toe Model")-vcell("K102","Toe Model")/IF(vcell("$B102","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L102')
    return None
xfunctions['Toe Model!L102']=xcf_Toe_Model_L102

#IF(L102>0,MIN(IF($H102+G$98>=6,$N46,CHOOSE($H102+G$98,$I46,$J46,$K46,$L46,$M46))*L102,IF($H102+G$98>=6,$V19,CHOOSE($H102+G$98,$Q19,$R19,$S19,$T19,$U19))-IF($H102+E$98>=6,$V19,CHOOSE($H102+E$98,$Q19,$R19,$S19,$T19,$U19))),0)
def xcf_Toe_Model_M102(): 
    try:      
        return IF(vcell("L102","Toe Model")>0,MIN(IF(vcell("$H102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N46","Toe Model"),CHOOSE(vcell("$H102","Toe Model")+vcell("G$98","Toe Model"),vcell("$I46","Toe Model"),vcell("$J46","Toe Model"),vcell("$K46","Toe Model"),vcell("$L46","Toe Model"),vcell("$M46","Toe Model")))*vcell("L102","Toe Model"),IF(vcell("$H102","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V19","Toe Model"),CHOOSE(vcell("$H102","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q19","Toe Model"),vcell("$R19","Toe Model"),vcell("$S19","Toe Model"),vcell("$T19","Toe Model"),vcell("$U19","Toe Model")))-IF(vcell("$H102","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V19","Toe Model"),CHOOSE(vcell("$H102","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q19","Toe Model"),vcell("$R19","Toe Model"),vcell("$S19","Toe Model"),vcell("$T19","Toe Model"),vcell("$U19","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M102')
    return None
xfunctions['Toe Model!M102']=xcf_Toe_Model_M102

#L102-M102/IF($B102+$M$98>=6,$H46,CHOOSE($B102+$M$98,$C46,$D46,$E46,$F46,$G46))
def xcf_Toe_Model_N102(): 
    try:      
        return vcell("L102","Toe Model")-vcell("M102","Toe Model")/IF(vcell("$B102","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H46","Toe Model"),CHOOSE(vcell("$B102","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C46","Toe Model"),vcell("$D46","Toe Model"),vcell("$E46","Toe Model"),vcell("$F46","Toe Model"),vcell("$G46","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N102')
    return None
xfunctions['Toe Model!N102']=xcf_Toe_Model_N102

#B73-SUM($C102,$E102,$G102,$I102,$K102,$M102)*SIN(RADIANS($D19))
def xcf_Toe_Model_O102(): 
    try:      
        return vcell("B73","Toe Model")-SUM(vcell("$C102","Toe Model"),vcell("$E102","Toe Model"),vcell("$G102","Toe Model"),vcell("$I102","Toe Model"),vcell("$K102","Toe Model"),vcell("$M102","Toe Model"))*SIN(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O102')
    return None
xfunctions['Toe Model!O102']=xcf_Toe_Model_O102

#C73+IF($D19==360,1,-1)*SUM($C102,$E102,$G102,$I102,$K102,$M102)*COS(RADIANS($D19))
def xcf_Toe_Model_P102(): 
    try:      
        return vcell("C73","Toe Model")+IF(vcell("$D19","Toe Model")==360,1,-1)*SUM(vcell("$C102","Toe Model"),vcell("$E102","Toe Model"),vcell("$G102","Toe Model"),vcell("$I102","Toe Model"),vcell("$K102","Toe Model"),vcell("$M102","Toe Model"))*COS(RADIANS(vcell("$D19","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P102')
    return None
xfunctions['Toe Model!P102']=xcf_Toe_Model_P102

#IF(MIN(C$18:C$40)>P102,IF(P101>=MIN(C$18:C$40),O101+(O102-O101)*(MIN(C$18:C$40)-P101)/(P102-P101),Q101+ABS(O102-O101)),O102)
def xcf_Toe_Model_Q102(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P102","Toe Model"),IF(vcell("P101","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O101","Toe Model")+(vcell("O102","Toe Model")-vcell("O101","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P101","Toe Model"))/(vcell("P102","Toe Model")-vcell("P101","Toe Model")),vcell("Q101","Toe Model")+ABS(vcell("O102","Toe Model")-vcell("O101","Toe Model"))),vcell("O102","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q102')
    return None
xfunctions['Toe Model!Q102']=xcf_Toe_Model_Q102

#MAX(C73+IF($D19==360,1,-1)*SUM($C102,$E102,$G102,$I102,$K102,$M102)*COS(RADIANS($D19)),MIN(C$18:C$40))
def xcf_Toe_Model_R102(): 
    try:      
        return MAX(vcell("C73","Toe Model")+IF(vcell("$D19","Toe Model")==360,1,-1)*SUM(vcell("$C102","Toe Model"),vcell("$E102","Toe Model"),vcell("$G102","Toe Model"),vcell("$I102","Toe Model"),vcell("$K102","Toe Model"),vcell("$M102","Toe Model"))*COS(RADIANS(vcell("$D19","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R102')
    return None
xfunctions['Toe Model!R102']=xcf_Toe_Model_R102

#IF(AND($C74<=$B$4,$C74>$B$5),1,IF(AND($C74<=$B$5,$C74>$B$6),2,IF(AND($C74<=$B$6,$C74>$B$7),3,IF(AND($C74<=$B$7,$C74>$B$8),4,IF(AND($C74<=$B$8,$C74>$B$9),5,6)))))
def xcf_Toe_Model_B103(): 
    try:      
        return IF(AND(vcell("$C74","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C74","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C74","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C74","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C74","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C74","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C74","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C74","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C74","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C74","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B103')
    return None
xfunctions['Toe Model!B103']=xcf_Toe_Model_B103

#IF(ISNUMBER($B103),MIN(CHOOSE($B103,$K20,$L20,$M20,$N20,$O20,$P20),CHOOSE($B103,$C47,$D47,$E47,$F47,$G47,$H47)*$D74),0)
def xcf_Toe_Model_C103(): 
    try:      
        return IF(ISNUMBER(vcell("$B103","Toe Model")),MIN(CHOOSE(vcell("$B103","Toe Model"),vcell("$K20","Toe Model"),vcell("$L20","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model"),vcell("$P20","Toe Model")),CHOOSE(vcell("$B103","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model"),vcell("$H47","Toe Model"))*vcell("$D74","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C103')
    return None
xfunctions['Toe Model!C103']=xcf_Toe_Model_C103

#$D74-$C103/CHOOSE($B103,$C47,$D47,$E47,$F47,$G47,$H47)
def xcf_Toe_Model_D103(): 
    try:      
        return vcell("$D74","Toe Model")-vcell("$C103","Toe Model")/CHOOSE(vcell("$B103","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model"),vcell("$H47","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D103')
    return None
xfunctions['Toe Model!D103']=xcf_Toe_Model_D103

#IF(D103>0,MIN(IF($B103+E$98>=6,$H47,CHOOSE($B103+E$98,$C47,$D47,$E47,$F47,$G47))*D103,IF($B103+E$98>=6,$P20,CHOOSE($B103+E$98,$K20,$L20,$M20,$N20,$O20))-IF($B103>=6,$P20,CHOOSE($B103,$K20,$L20,$M20,$N20,$O20))),0)
def xcf_Toe_Model_E103(): 
    try:      
        return IF(vcell("D103","Toe Model")>0,MIN(IF(vcell("$B103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("E$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))*vcell("D103","Toe Model"),IF(vcell("$B103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P20","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("E$98","Toe Model"),vcell("$K20","Toe Model"),vcell("$L20","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model")))-IF(vcell("$B103","Toe Model")>=6,vcell("$P20","Toe Model"),CHOOSE(vcell("$B103","Toe Model"),vcell("$K20","Toe Model"),vcell("$L20","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E103')
    return None
xfunctions['Toe Model!E103']=xcf_Toe_Model_E103

#D103-E103/IF($B103+$E$98>=6,$H47,CHOOSE($B103+$E$98,$C47,$D47,$E47,$F47,$G47))
def xcf_Toe_Model_F103(): 
    try:      
        return vcell("D103","Toe Model")-vcell("E103","Toe Model")/IF(vcell("$B103","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F103')
    return None
xfunctions['Toe Model!F103']=xcf_Toe_Model_F103

#IF(F103>0,MIN(IF($B103+G$98>=6,$H47,CHOOSE($B103+G$98,$C47,$D47,$E47,$F47,$G47))*F103,IF($B103+G$98>=6,$P20,CHOOSE($B103+G$98,$K20,$L20,$M20,$N20,$O20))-IF($B103+E$98>=6,$P20,CHOOSE($B103+E$98,$K20,$L20,$M20,$N20,$O20))),0)
def xcf_Toe_Model_G103(): 
    try:      
        return IF(vcell("F103","Toe Model")>0,MIN(IF(vcell("$B103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("G$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))*vcell("F103","Toe Model"),IF(vcell("$B103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P20","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("G$98","Toe Model"),vcell("$K20","Toe Model"),vcell("$L20","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model")))-IF(vcell("$B103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P20","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("E$98","Toe Model"),vcell("$K20","Toe Model"),vcell("$L20","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G103')
    return None
xfunctions['Toe Model!G103']=xcf_Toe_Model_G103

#F103-G103/IF($B103+$G$98>=6,$H47,CHOOSE($B103+$G$98,$C47,$D47,$E47,$F47,$G47))
def xcf_Toe_Model_H103(): 
    try:      
        return vcell("F103","Toe Model")-vcell("G103","Toe Model")/IF(vcell("$B103","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H103')
    return None
xfunctions['Toe Model!H103']=xcf_Toe_Model_H103

#IF(H103>0,MIN(IF($D103+G$98>=6,$J47,CHOOSE($D103+G$98,$E47,$F47,$G47,$H47,$I47))*H103,IF($D103+G$98>=6,$R20,CHOOSE($D103+G$98,$M20,$N20,$O20,$P20,$Q20))-IF($D103+E$98>=6,$R20,CHOOSE($D103+E$98,$M20,$N20,$O20,$P20,$Q20))),0)
def xcf_Toe_Model_I103(): 
    try:      
        return IF(vcell("H103","Toe Model")>0,MIN(IF(vcell("$D103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J47","Toe Model"),CHOOSE(vcell("$D103","Toe Model")+vcell("G$98","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model"),vcell("$H47","Toe Model"),vcell("$I47","Toe Model")))*vcell("H103","Toe Model"),IF(vcell("$D103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R20","Toe Model"),CHOOSE(vcell("$D103","Toe Model")+vcell("G$98","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model"),vcell("$P20","Toe Model"),vcell("$Q20","Toe Model")))-IF(vcell("$D103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R20","Toe Model"),CHOOSE(vcell("$D103","Toe Model")+vcell("E$98","Toe Model"),vcell("$M20","Toe Model"),vcell("$N20","Toe Model"),vcell("$O20","Toe Model"),vcell("$P20","Toe Model"),vcell("$Q20","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I103')
    return None
xfunctions['Toe Model!I103']=xcf_Toe_Model_I103

#H103-I103/IF($B103+$I$98>=6,$H47,CHOOSE($B103+$I$98,$C47,$D47,$E47,$F47,$G47))
def xcf_Toe_Model_J103(): 
    try:      
        return vcell("H103","Toe Model")-vcell("I103","Toe Model")/IF(vcell("$B103","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J103')
    return None
xfunctions['Toe Model!J103']=xcf_Toe_Model_J103

#IF(J103>0,MIN(IF($F103+G$98>=6,$L47,CHOOSE($F103+G$98,$G47,$H47,$I47,$J47,$K47))*J103,IF($F103+G$98>=6,$T20,CHOOSE($F103+G$98,$O20,$P20,$Q20,$R20,$S20))-IF($F103+E$98>=6,$T20,CHOOSE($F103+E$98,$O20,$P20,$Q20,$R20,$S20))),0)
def xcf_Toe_Model_K103(): 
    try:      
        return IF(vcell("J103","Toe Model")>0,MIN(IF(vcell("$F103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L47","Toe Model"),CHOOSE(vcell("$F103","Toe Model")+vcell("G$98","Toe Model"),vcell("$G47","Toe Model"),vcell("$H47","Toe Model"),vcell("$I47","Toe Model"),vcell("$J47","Toe Model"),vcell("$K47","Toe Model")))*vcell("J103","Toe Model"),IF(vcell("$F103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T20","Toe Model"),CHOOSE(vcell("$F103","Toe Model")+vcell("G$98","Toe Model"),vcell("$O20","Toe Model"),vcell("$P20","Toe Model"),vcell("$Q20","Toe Model"),vcell("$R20","Toe Model"),vcell("$S20","Toe Model")))-IF(vcell("$F103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T20","Toe Model"),CHOOSE(vcell("$F103","Toe Model")+vcell("E$98","Toe Model"),vcell("$O20","Toe Model"),vcell("$P20","Toe Model"),vcell("$Q20","Toe Model"),vcell("$R20","Toe Model"),vcell("$S20","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K103')
    return None
xfunctions['Toe Model!K103']=xcf_Toe_Model_K103

#J103-K103/IF($B103+$K$98>=6,$H47,CHOOSE($B103+$K$98,$C47,$D47,$E47,$F47,$G47))
def xcf_Toe_Model_L103(): 
    try:      
        return vcell("J103","Toe Model")-vcell("K103","Toe Model")/IF(vcell("$B103","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L103')
    return None
xfunctions['Toe Model!L103']=xcf_Toe_Model_L103

#IF(L103>0,MIN(IF($H103+G$98>=6,$N47,CHOOSE($H103+G$98,$I47,$J47,$K47,$L47,$M47))*L103,IF($H103+G$98>=6,$V20,CHOOSE($H103+G$98,$Q20,$R20,$S20,$T20,$U20))-IF($H103+E$98>=6,$V20,CHOOSE($H103+E$98,$Q20,$R20,$S20,$T20,$U20))),0)
def xcf_Toe_Model_M103(): 
    try:      
        return IF(vcell("L103","Toe Model")>0,MIN(IF(vcell("$H103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N47","Toe Model"),CHOOSE(vcell("$H103","Toe Model")+vcell("G$98","Toe Model"),vcell("$I47","Toe Model"),vcell("$J47","Toe Model"),vcell("$K47","Toe Model"),vcell("$L47","Toe Model"),vcell("$M47","Toe Model")))*vcell("L103","Toe Model"),IF(vcell("$H103","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V20","Toe Model"),CHOOSE(vcell("$H103","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q20","Toe Model"),vcell("$R20","Toe Model"),vcell("$S20","Toe Model"),vcell("$T20","Toe Model"),vcell("$U20","Toe Model")))-IF(vcell("$H103","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V20","Toe Model"),CHOOSE(vcell("$H103","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q20","Toe Model"),vcell("$R20","Toe Model"),vcell("$S20","Toe Model"),vcell("$T20","Toe Model"),vcell("$U20","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M103')
    return None
xfunctions['Toe Model!M103']=xcf_Toe_Model_M103

#L103-M103/IF($B103+$M$98>=6,$H47,CHOOSE($B103+$M$98,$C47,$D47,$E47,$F47,$G47))
def xcf_Toe_Model_N103(): 
    try:      
        return vcell("L103","Toe Model")-vcell("M103","Toe Model")/IF(vcell("$B103","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H47","Toe Model"),CHOOSE(vcell("$B103","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C47","Toe Model"),vcell("$D47","Toe Model"),vcell("$E47","Toe Model"),vcell("$F47","Toe Model"),vcell("$G47","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N103')
    return None
xfunctions['Toe Model!N103']=xcf_Toe_Model_N103

#B74-SUM($C103,$E103,$G103,$I103,$K103,$M103)*SIN(RADIANS($D20))
def xcf_Toe_Model_O103(): 
    try:      
        return vcell("B74","Toe Model")-SUM(vcell("$C103","Toe Model"),vcell("$E103","Toe Model"),vcell("$G103","Toe Model"),vcell("$I103","Toe Model"),vcell("$K103","Toe Model"),vcell("$M103","Toe Model"))*SIN(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O103')
    return None
xfunctions['Toe Model!O103']=xcf_Toe_Model_O103

#C74+IF($D20==360,1,-1)*SUM($C103,$E103,$G103,$I103,$K103,$M103)*COS(RADIANS($D20))
def xcf_Toe_Model_P103(): 
    try:      
        return vcell("C74","Toe Model")+IF(vcell("$D20","Toe Model")==360,1,-1)*SUM(vcell("$C103","Toe Model"),vcell("$E103","Toe Model"),vcell("$G103","Toe Model"),vcell("$I103","Toe Model"),vcell("$K103","Toe Model"),vcell("$M103","Toe Model"))*COS(RADIANS(vcell("$D20","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P103')
    return None
xfunctions['Toe Model!P103']=xcf_Toe_Model_P103

#IF(MIN(C$18:C$40)>P103,IF(P102>=MIN(C$18:C$40),O102+(O103-O102)*(MIN(C$18:C$40)-P102)/(P103-P102),Q102+ABS(O103-O102)),O103)
def xcf_Toe_Model_Q103(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P103","Toe Model"),IF(vcell("P102","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O102","Toe Model")+(vcell("O103","Toe Model")-vcell("O102","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P102","Toe Model"))/(vcell("P103","Toe Model")-vcell("P102","Toe Model")),vcell("Q102","Toe Model")+ABS(vcell("O103","Toe Model")-vcell("O102","Toe Model"))),vcell("O103","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q103')
    return None
xfunctions['Toe Model!Q103']=xcf_Toe_Model_Q103

#MAX(C74+IF($D20==360,1,-1)*SUM($C103,$E103,$G103,$I103,$K103,$M103)*COS(RADIANS($D20)),MIN(C$18:C$40))
def xcf_Toe_Model_R103(): 
    try:      
        return MAX(vcell("C74","Toe Model")+IF(vcell("$D20","Toe Model")==360,1,-1)*SUM(vcell("$C103","Toe Model"),vcell("$E103","Toe Model"),vcell("$G103","Toe Model"),vcell("$I103","Toe Model"),vcell("$K103","Toe Model"),vcell("$M103","Toe Model"))*COS(RADIANS(vcell("$D20","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R103')
    return None
xfunctions['Toe Model!R103']=xcf_Toe_Model_R103

#IF(AND($C75<=$B$4,$C75>$B$5),1,IF(AND($C75<=$B$5,$C75>$B$6),2,IF(AND($C75<=$B$6,$C75>$B$7),3,IF(AND($C75<=$B$7,$C75>$B$8),4,IF(AND($C75<=$B$8,$C75>$B$9),5,6)))))
def xcf_Toe_Model_B104(): 
    try:      
        return IF(AND(vcell("$C75","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C75","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C75","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C75","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C75","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C75","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C75","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C75","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C75","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C75","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B104')
    return None
xfunctions['Toe Model!B104']=xcf_Toe_Model_B104

#IF(ISNUMBER($B104),MIN(CHOOSE($B104,$K21,$L21,$M21,$N21,$O21,$P21),CHOOSE($B104,$C48,$D48,$E48,$F48,$G48,$H48)*$D75),0)
def xcf_Toe_Model_C104(): 
    try:      
        return IF(ISNUMBER(vcell("$B104","Toe Model")),MIN(CHOOSE(vcell("$B104","Toe Model"),vcell("$K21","Toe Model"),vcell("$L21","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model"),vcell("$P21","Toe Model")),CHOOSE(vcell("$B104","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model"),vcell("$H48","Toe Model"))*vcell("$D75","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C104')
    return None
xfunctions['Toe Model!C104']=xcf_Toe_Model_C104

#$D75-$C104/CHOOSE($B104,$C48,$D48,$E48,$F48,$G48,$H48)
def xcf_Toe_Model_D104(): 
    try:      
        return vcell("$D75","Toe Model")-vcell("$C104","Toe Model")/CHOOSE(vcell("$B104","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model"),vcell("$H48","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D104')
    return None
xfunctions['Toe Model!D104']=xcf_Toe_Model_D104

#IF(D104>0,MIN(IF($B104+E$98>=6,$H48,CHOOSE($B104+E$98,$C48,$D48,$E48,$F48,$G48))*D104,IF($B104+E$98>=6,$P21,CHOOSE($B104+E$98,$K21,$L21,$M21,$N21,$O21))-IF($B104>=6,$P21,CHOOSE($B104,$K21,$L21,$M21,$N21,$O21))),0)
def xcf_Toe_Model_E104(): 
    try:      
        return IF(vcell("D104","Toe Model")>0,MIN(IF(vcell("$B104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("E$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))*vcell("D104","Toe Model"),IF(vcell("$B104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P21","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("E$98","Toe Model"),vcell("$K21","Toe Model"),vcell("$L21","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model")))-IF(vcell("$B104","Toe Model")>=6,vcell("$P21","Toe Model"),CHOOSE(vcell("$B104","Toe Model"),vcell("$K21","Toe Model"),vcell("$L21","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E104')
    return None
xfunctions['Toe Model!E104']=xcf_Toe_Model_E104

#D104-E104/IF($B104+$E$98>=6,$H48,CHOOSE($B104+$E$98,$C48,$D48,$E48,$F48,$G48))
def xcf_Toe_Model_F104(): 
    try:      
        return vcell("D104","Toe Model")-vcell("E104","Toe Model")/IF(vcell("$B104","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F104')
    return None
xfunctions['Toe Model!F104']=xcf_Toe_Model_F104

#IF(F104>0,MIN(IF($B104+G$98>=6,$H48,CHOOSE($B104+G$98,$C48,$D48,$E48,$F48,$G48))*F104,IF($B104+G$98>=6,$P21,CHOOSE($B104+G$98,$K21,$L21,$M21,$N21,$O21))-IF($B104+E$98>=6,$P21,CHOOSE($B104+E$98,$K21,$L21,$M21,$N21,$O21))),0)
def xcf_Toe_Model_G104(): 
    try:      
        return IF(vcell("F104","Toe Model")>0,MIN(IF(vcell("$B104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("G$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))*vcell("F104","Toe Model"),IF(vcell("$B104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P21","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("G$98","Toe Model"),vcell("$K21","Toe Model"),vcell("$L21","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model")))-IF(vcell("$B104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P21","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("E$98","Toe Model"),vcell("$K21","Toe Model"),vcell("$L21","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G104')
    return None
xfunctions['Toe Model!G104']=xcf_Toe_Model_G104

#F104-G104/IF($B104+$G$98>=6,$H48,CHOOSE($B104+$G$98,$C48,$D48,$E48,$F48,$G48))
def xcf_Toe_Model_H104(): 
    try:      
        return vcell("F104","Toe Model")-vcell("G104","Toe Model")/IF(vcell("$B104","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H104')
    return None
xfunctions['Toe Model!H104']=xcf_Toe_Model_H104

#IF(H104>0,MIN(IF($D104+G$98>=6,$J48,CHOOSE($D104+G$98,$E48,$F48,$G48,$H48,$I48))*H104,IF($D104+G$98>=6,$R21,CHOOSE($D104+G$98,$M21,$N21,$O21,$P21,$Q21))-IF($D104+E$98>=6,$R21,CHOOSE($D104+E$98,$M21,$N21,$O21,$P21,$Q21))),0)
def xcf_Toe_Model_I104(): 
    try:      
        return IF(vcell("H104","Toe Model")>0,MIN(IF(vcell("$D104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J48","Toe Model"),CHOOSE(vcell("$D104","Toe Model")+vcell("G$98","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model"),vcell("$H48","Toe Model"),vcell("$I48","Toe Model")))*vcell("H104","Toe Model"),IF(vcell("$D104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R21","Toe Model"),CHOOSE(vcell("$D104","Toe Model")+vcell("G$98","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model"),vcell("$P21","Toe Model"),vcell("$Q21","Toe Model")))-IF(vcell("$D104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R21","Toe Model"),CHOOSE(vcell("$D104","Toe Model")+vcell("E$98","Toe Model"),vcell("$M21","Toe Model"),vcell("$N21","Toe Model"),vcell("$O21","Toe Model"),vcell("$P21","Toe Model"),vcell("$Q21","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I104')
    return None
xfunctions['Toe Model!I104']=xcf_Toe_Model_I104

#H104-I104/IF($B104+$I$98>=6,$H48,CHOOSE($B104+$I$98,$C48,$D48,$E48,$F48,$G48))
def xcf_Toe_Model_J104(): 
    try:      
        return vcell("H104","Toe Model")-vcell("I104","Toe Model")/IF(vcell("$B104","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J104')
    return None
xfunctions['Toe Model!J104']=xcf_Toe_Model_J104

#IF(J104>0,MIN(IF($F104+G$98>=6,$L48,CHOOSE($F104+G$98,$G48,$H48,$I48,$J48,$K48))*J104,IF($F104+G$98>=6,$T21,CHOOSE($F104+G$98,$O21,$P21,$Q21,$R21,$S21))-IF($F104+E$98>=6,$T21,CHOOSE($F104+E$98,$O21,$P21,$Q21,$R21,$S21))),0)
def xcf_Toe_Model_K104(): 
    try:      
        return IF(vcell("J104","Toe Model")>0,MIN(IF(vcell("$F104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L48","Toe Model"),CHOOSE(vcell("$F104","Toe Model")+vcell("G$98","Toe Model"),vcell("$G48","Toe Model"),vcell("$H48","Toe Model"),vcell("$I48","Toe Model"),vcell("$J48","Toe Model"),vcell("$K48","Toe Model")))*vcell("J104","Toe Model"),IF(vcell("$F104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T21","Toe Model"),CHOOSE(vcell("$F104","Toe Model")+vcell("G$98","Toe Model"),vcell("$O21","Toe Model"),vcell("$P21","Toe Model"),vcell("$Q21","Toe Model"),vcell("$R21","Toe Model"),vcell("$S21","Toe Model")))-IF(vcell("$F104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T21","Toe Model"),CHOOSE(vcell("$F104","Toe Model")+vcell("E$98","Toe Model"),vcell("$O21","Toe Model"),vcell("$P21","Toe Model"),vcell("$Q21","Toe Model"),vcell("$R21","Toe Model"),vcell("$S21","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K104')
    return None
xfunctions['Toe Model!K104']=xcf_Toe_Model_K104

#J104-K104/IF($B104+$K$98>=6,$H48,CHOOSE($B104+$K$98,$C48,$D48,$E48,$F48,$G48))
def xcf_Toe_Model_L104(): 
    try:      
        return vcell("J104","Toe Model")-vcell("K104","Toe Model")/IF(vcell("$B104","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L104')
    return None
xfunctions['Toe Model!L104']=xcf_Toe_Model_L104

#IF(L104>0,MIN(IF($H104+G$98>=6,$N48,CHOOSE($H104+G$98,$I48,$J48,$K48,$L48,$M48))*L104,IF($H104+G$98>=6,$V21,CHOOSE($H104+G$98,$Q21,$R21,$S21,$T21,$U21))-IF($H104+E$98>=6,$V21,CHOOSE($H104+E$98,$Q21,$R21,$S21,$T21,$U21))),0)
def xcf_Toe_Model_M104(): 
    try:      
        return IF(vcell("L104","Toe Model")>0,MIN(IF(vcell("$H104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N48","Toe Model"),CHOOSE(vcell("$H104","Toe Model")+vcell("G$98","Toe Model"),vcell("$I48","Toe Model"),vcell("$J48","Toe Model"),vcell("$K48","Toe Model"),vcell("$L48","Toe Model"),vcell("$M48","Toe Model")))*vcell("L104","Toe Model"),IF(vcell("$H104","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V21","Toe Model"),CHOOSE(vcell("$H104","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q21","Toe Model"),vcell("$R21","Toe Model"),vcell("$S21","Toe Model"),vcell("$T21","Toe Model"),vcell("$U21","Toe Model")))-IF(vcell("$H104","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V21","Toe Model"),CHOOSE(vcell("$H104","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q21","Toe Model"),vcell("$R21","Toe Model"),vcell("$S21","Toe Model"),vcell("$T21","Toe Model"),vcell("$U21","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M104')
    return None
xfunctions['Toe Model!M104']=xcf_Toe_Model_M104

#L104-M104/IF($B104+$M$98>=6,$H48,CHOOSE($B104+$M$98,$C48,$D48,$E48,$F48,$G48))
def xcf_Toe_Model_N104(): 
    try:      
        return vcell("L104","Toe Model")-vcell("M104","Toe Model")/IF(vcell("$B104","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H48","Toe Model"),CHOOSE(vcell("$B104","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C48","Toe Model"),vcell("$D48","Toe Model"),vcell("$E48","Toe Model"),vcell("$F48","Toe Model"),vcell("$G48","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N104')
    return None
xfunctions['Toe Model!N104']=xcf_Toe_Model_N104

#B75-SUM($C104,$E104,$G104,$I104,$K104,$M104)*SIN(RADIANS($D21))
def xcf_Toe_Model_O104(): 
    try:      
        return vcell("B75","Toe Model")-SUM(vcell("$C104","Toe Model"),vcell("$E104","Toe Model"),vcell("$G104","Toe Model"),vcell("$I104","Toe Model"),vcell("$K104","Toe Model"),vcell("$M104","Toe Model"))*SIN(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O104')
    return None
xfunctions['Toe Model!O104']=xcf_Toe_Model_O104

#C75+IF($D21==360,1,-1)*SUM($C104,$E104,$G104,$I104,$K104,$M104)*COS(RADIANS($D21))
def xcf_Toe_Model_P104(): 
    try:      
        return vcell("C75","Toe Model")+IF(vcell("$D21","Toe Model")==360,1,-1)*SUM(vcell("$C104","Toe Model"),vcell("$E104","Toe Model"),vcell("$G104","Toe Model"),vcell("$I104","Toe Model"),vcell("$K104","Toe Model"),vcell("$M104","Toe Model"))*COS(RADIANS(vcell("$D21","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P104')
    return None
xfunctions['Toe Model!P104']=xcf_Toe_Model_P104

#IF(MIN(C$18:C$40)>P104,IF(P103>=MIN(C$18:C$40),O103+(O104-O103)*(MIN(C$18:C$40)-P103)/(P104-P103),Q103+ABS(O104-O103)),O104)
def xcf_Toe_Model_Q104(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P104","Toe Model"),IF(vcell("P103","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O103","Toe Model")+(vcell("O104","Toe Model")-vcell("O103","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P103","Toe Model"))/(vcell("P104","Toe Model")-vcell("P103","Toe Model")),vcell("Q103","Toe Model")+ABS(vcell("O104","Toe Model")-vcell("O103","Toe Model"))),vcell("O104","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q104')
    return None
xfunctions['Toe Model!Q104']=xcf_Toe_Model_Q104

#MAX(C75+IF($D21==360,1,-1)*SUM($C104,$E104,$G104,$I104,$K104,$M104)*COS(RADIANS($D21)),MIN(C$18:C$40))
def xcf_Toe_Model_R104(): 
    try:      
        return MAX(vcell("C75","Toe Model")+IF(vcell("$D21","Toe Model")==360,1,-1)*SUM(vcell("$C104","Toe Model"),vcell("$E104","Toe Model"),vcell("$G104","Toe Model"),vcell("$I104","Toe Model"),vcell("$K104","Toe Model"),vcell("$M104","Toe Model"))*COS(RADIANS(vcell("$D21","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R104')
    return None
xfunctions['Toe Model!R104']=xcf_Toe_Model_R104

#IF(AND($C76<=$B$4,$C76>$B$5),1,IF(AND($C76<=$B$5,$C76>$B$6),2,IF(AND($C76<=$B$6,$C76>$B$7),3,IF(AND($C76<=$B$7,$C76>$B$8),4,IF(AND($C76<=$B$8,$C76>$B$9),5,6)))))
def xcf_Toe_Model_B105(): 
    try:      
        return IF(AND(vcell("$C76","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C76","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C76","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C76","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C76","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C76","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C76","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C76","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C76","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C76","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B105')
    return None
xfunctions['Toe Model!B105']=xcf_Toe_Model_B105

#IF(ISNUMBER($B105),MIN(CHOOSE($B105,$K22,$L22,$M22,$N22,$O22,$P22),CHOOSE($B105,$C49,$D49,$E49,$F49,$G49,$H49)*$D76),0)
def xcf_Toe_Model_C105(): 
    try:      
        return IF(ISNUMBER(vcell("$B105","Toe Model")),MIN(CHOOSE(vcell("$B105","Toe Model"),vcell("$K22","Toe Model"),vcell("$L22","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model"),vcell("$P22","Toe Model")),CHOOSE(vcell("$B105","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model"),vcell("$H49","Toe Model"))*vcell("$D76","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C105')
    return None
xfunctions['Toe Model!C105']=xcf_Toe_Model_C105

#$D76-$C105/CHOOSE($B105,$C49,$D49,$E49,$F49,$G49,$H49)
def xcf_Toe_Model_D105(): 
    try:      
        return vcell("$D76","Toe Model")-vcell("$C105","Toe Model")/CHOOSE(vcell("$B105","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model"),vcell("$H49","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D105')
    return None
xfunctions['Toe Model!D105']=xcf_Toe_Model_D105

#IF(D105>0,MIN(IF($B105+E$98>=6,$H49,CHOOSE($B105+E$98,$C49,$D49,$E49,$F49,$G49))*D105,IF($B105+E$98>=6,$P22,CHOOSE($B105+E$98,$K22,$L22,$M22,$N22,$O22))-IF($B105>=6,$P22,CHOOSE($B105,$K22,$L22,$M22,$N22,$O22))),0)
def xcf_Toe_Model_E105(): 
    try:      
        return IF(vcell("D105","Toe Model")>0,MIN(IF(vcell("$B105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("E$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))*vcell("D105","Toe Model"),IF(vcell("$B105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P22","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("E$98","Toe Model"),vcell("$K22","Toe Model"),vcell("$L22","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model")))-IF(vcell("$B105","Toe Model")>=6,vcell("$P22","Toe Model"),CHOOSE(vcell("$B105","Toe Model"),vcell("$K22","Toe Model"),vcell("$L22","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E105')
    return None
xfunctions['Toe Model!E105']=xcf_Toe_Model_E105

#D105-E105/IF($B105+$E$98>=6,$H49,CHOOSE($B105+$E$98,$C49,$D49,$E49,$F49,$G49))
def xcf_Toe_Model_F105(): 
    try:      
        return vcell("D105","Toe Model")-vcell("E105","Toe Model")/IF(vcell("$B105","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F105')
    return None
xfunctions['Toe Model!F105']=xcf_Toe_Model_F105

#IF(F105>0,MIN(IF($B105+G$98>=6,$H49,CHOOSE($B105+G$98,$C49,$D49,$E49,$F49,$G49))*F105,IF($B105+G$98>=6,$P22,CHOOSE($B105+G$98,$K22,$L22,$M22,$N22,$O22))-IF($B105+E$98>=6,$P22,CHOOSE($B105+E$98,$K22,$L22,$M22,$N22,$O22))),0)
def xcf_Toe_Model_G105(): 
    try:      
        return IF(vcell("F105","Toe Model")>0,MIN(IF(vcell("$B105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("G$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))*vcell("F105","Toe Model"),IF(vcell("$B105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P22","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("G$98","Toe Model"),vcell("$K22","Toe Model"),vcell("$L22","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model")))-IF(vcell("$B105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P22","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("E$98","Toe Model"),vcell("$K22","Toe Model"),vcell("$L22","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G105')
    return None
xfunctions['Toe Model!G105']=xcf_Toe_Model_G105

#F105-G105/IF($B105+$G$98>=6,$H49,CHOOSE($B105+$G$98,$C49,$D49,$E49,$F49,$G49))
def xcf_Toe_Model_H105(): 
    try:      
        return vcell("F105","Toe Model")-vcell("G105","Toe Model")/IF(vcell("$B105","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H105')
    return None
xfunctions['Toe Model!H105']=xcf_Toe_Model_H105

#IF(H105>0,MIN(IF($D105+G$98>=6,$J49,CHOOSE($D105+G$98,$E49,$F49,$G49,$H49,$I49))*H105,IF($D105+G$98>=6,$R22,CHOOSE($D105+G$98,$M22,$N22,$O22,$P22,$Q22))-IF($D105+E$98>=6,$R22,CHOOSE($D105+E$98,$M22,$N22,$O22,$P22,$Q22))),0)
def xcf_Toe_Model_I105(): 
    try:      
        return IF(vcell("H105","Toe Model")>0,MIN(IF(vcell("$D105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J49","Toe Model"),CHOOSE(vcell("$D105","Toe Model")+vcell("G$98","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model"),vcell("$H49","Toe Model"),vcell("$I49","Toe Model")))*vcell("H105","Toe Model"),IF(vcell("$D105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R22","Toe Model"),CHOOSE(vcell("$D105","Toe Model")+vcell("G$98","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model"),vcell("$P22","Toe Model"),vcell("$Q22","Toe Model")))-IF(vcell("$D105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R22","Toe Model"),CHOOSE(vcell("$D105","Toe Model")+vcell("E$98","Toe Model"),vcell("$M22","Toe Model"),vcell("$N22","Toe Model"),vcell("$O22","Toe Model"),vcell("$P22","Toe Model"),vcell("$Q22","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I105')
    return None
xfunctions['Toe Model!I105']=xcf_Toe_Model_I105

#H105-I105/IF($B105+$I$98>=6,$H49,CHOOSE($B105+$I$98,$C49,$D49,$E49,$F49,$G49))
def xcf_Toe_Model_J105(): 
    try:      
        return vcell("H105","Toe Model")-vcell("I105","Toe Model")/IF(vcell("$B105","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J105')
    return None
xfunctions['Toe Model!J105']=xcf_Toe_Model_J105

#IF(J105>0,MIN(IF($F105+G$98>=6,$L49,CHOOSE($F105+G$98,$G49,$H49,$I49,$J49,$K49))*J105,IF($F105+G$98>=6,$T22,CHOOSE($F105+G$98,$O22,$P22,$Q22,$R22,$S22))-IF($F105+E$98>=6,$T22,CHOOSE($F105+E$98,$O22,$P22,$Q22,$R22,$S22))),0)
def xcf_Toe_Model_K105(): 
    try:      
        return IF(vcell("J105","Toe Model")>0,MIN(IF(vcell("$F105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L49","Toe Model"),CHOOSE(vcell("$F105","Toe Model")+vcell("G$98","Toe Model"),vcell("$G49","Toe Model"),vcell("$H49","Toe Model"),vcell("$I49","Toe Model"),vcell("$J49","Toe Model"),vcell("$K49","Toe Model")))*vcell("J105","Toe Model"),IF(vcell("$F105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T22","Toe Model"),CHOOSE(vcell("$F105","Toe Model")+vcell("G$98","Toe Model"),vcell("$O22","Toe Model"),vcell("$P22","Toe Model"),vcell("$Q22","Toe Model"),vcell("$R22","Toe Model"),vcell("$S22","Toe Model")))-IF(vcell("$F105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T22","Toe Model"),CHOOSE(vcell("$F105","Toe Model")+vcell("E$98","Toe Model"),vcell("$O22","Toe Model"),vcell("$P22","Toe Model"),vcell("$Q22","Toe Model"),vcell("$R22","Toe Model"),vcell("$S22","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K105')
    return None
xfunctions['Toe Model!K105']=xcf_Toe_Model_K105

#J105-K105/IF($B105+$K$98>=6,$H49,CHOOSE($B105+$K$98,$C49,$D49,$E49,$F49,$G49))
def xcf_Toe_Model_L105(): 
    try:      
        return vcell("J105","Toe Model")-vcell("K105","Toe Model")/IF(vcell("$B105","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L105')
    return None
xfunctions['Toe Model!L105']=xcf_Toe_Model_L105

#IF(L105>0,MIN(IF($H105+G$98>=6,$N49,CHOOSE($H105+G$98,$I49,$J49,$K49,$L49,$M49))*L105,IF($H105+G$98>=6,$V22,CHOOSE($H105+G$98,$Q22,$R22,$S22,$T22,$U22))-IF($H105+E$98>=6,$V22,CHOOSE($H105+E$98,$Q22,$R22,$S22,$T22,$U22))),0)
def xcf_Toe_Model_M105(): 
    try:      
        return IF(vcell("L105","Toe Model")>0,MIN(IF(vcell("$H105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N49","Toe Model"),CHOOSE(vcell("$H105","Toe Model")+vcell("G$98","Toe Model"),vcell("$I49","Toe Model"),vcell("$J49","Toe Model"),vcell("$K49","Toe Model"),vcell("$L49","Toe Model"),vcell("$M49","Toe Model")))*vcell("L105","Toe Model"),IF(vcell("$H105","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V22","Toe Model"),CHOOSE(vcell("$H105","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q22","Toe Model"),vcell("$R22","Toe Model"),vcell("$S22","Toe Model"),vcell("$T22","Toe Model"),vcell("$U22","Toe Model")))-IF(vcell("$H105","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V22","Toe Model"),CHOOSE(vcell("$H105","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q22","Toe Model"),vcell("$R22","Toe Model"),vcell("$S22","Toe Model"),vcell("$T22","Toe Model"),vcell("$U22","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M105')
    return None
xfunctions['Toe Model!M105']=xcf_Toe_Model_M105

#L105-M105/IF($B105+$M$98>=6,$H49,CHOOSE($B105+$M$98,$C49,$D49,$E49,$F49,$G49))
def xcf_Toe_Model_N105(): 
    try:      
        return vcell("L105","Toe Model")-vcell("M105","Toe Model")/IF(vcell("$B105","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H49","Toe Model"),CHOOSE(vcell("$B105","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C49","Toe Model"),vcell("$D49","Toe Model"),vcell("$E49","Toe Model"),vcell("$F49","Toe Model"),vcell("$G49","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N105')
    return None
xfunctions['Toe Model!N105']=xcf_Toe_Model_N105

#B76-SUM($C105,$E105,$G105,$I105,$K105,$M105)*SIN(RADIANS($D22))
def xcf_Toe_Model_O105(): 
    try:      
        return vcell("B76","Toe Model")-SUM(vcell("$C105","Toe Model"),vcell("$E105","Toe Model"),vcell("$G105","Toe Model"),vcell("$I105","Toe Model"),vcell("$K105","Toe Model"),vcell("$M105","Toe Model"))*SIN(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O105')
    return None
xfunctions['Toe Model!O105']=xcf_Toe_Model_O105

#C76+IF($D22==360,1,-1)*SUM($C105,$E105,$G105,$I105,$K105,$M105)*COS(RADIANS($D22))
def xcf_Toe_Model_P105(): 
    try:      
        return vcell("C76","Toe Model")+IF(vcell("$D22","Toe Model")==360,1,-1)*SUM(vcell("$C105","Toe Model"),vcell("$E105","Toe Model"),vcell("$G105","Toe Model"),vcell("$I105","Toe Model"),vcell("$K105","Toe Model"),vcell("$M105","Toe Model"))*COS(RADIANS(vcell("$D22","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P105')
    return None
xfunctions['Toe Model!P105']=xcf_Toe_Model_P105

#IF(MIN(C$18:C$40)>P105,IF(P104>=MIN(C$18:C$40),O104+(O105-O104)*(MIN(C$18:C$40)-P104)/(P105-P104),Q104+ABS(O105-O104)),O105)
def xcf_Toe_Model_Q105(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P105","Toe Model"),IF(vcell("P104","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O104","Toe Model")+(vcell("O105","Toe Model")-vcell("O104","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P104","Toe Model"))/(vcell("P105","Toe Model")-vcell("P104","Toe Model")),vcell("Q104","Toe Model")+ABS(vcell("O105","Toe Model")-vcell("O104","Toe Model"))),vcell("O105","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q105')
    return None
xfunctions['Toe Model!Q105']=xcf_Toe_Model_Q105

#MAX(C76+IF($D22==360,1,-1)*SUM($C105,$E105,$G105,$I105,$K105,$M105)*COS(RADIANS($D22)),MIN(C$18:C$40))
def xcf_Toe_Model_R105(): 
    try:      
        return MAX(vcell("C76","Toe Model")+IF(vcell("$D22","Toe Model")==360,1,-1)*SUM(vcell("$C105","Toe Model"),vcell("$E105","Toe Model"),vcell("$G105","Toe Model"),vcell("$I105","Toe Model"),vcell("$K105","Toe Model"),vcell("$M105","Toe Model"))*COS(RADIANS(vcell("$D22","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R105')
    return None
xfunctions['Toe Model!R105']=xcf_Toe_Model_R105

#IF(AND($C77<=$B$4,$C77>$B$5),1,IF(AND($C77<=$B$5,$C77>$B$6),2,IF(AND($C77<=$B$6,$C77>$B$7),3,IF(AND($C77<=$B$7,$C77>$B$8),4,IF(AND($C77<=$B$8,$C77>$B$9),5,6)))))
def xcf_Toe_Model_B106(): 
    try:      
        return IF(AND(vcell("$C77","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C77","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C77","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C77","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C77","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C77","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C77","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C77","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C77","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C77","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B106')
    return None
xfunctions['Toe Model!B106']=xcf_Toe_Model_B106

#IF(ISNUMBER($B106),MIN(CHOOSE($B106,$K23,$L23,$M23,$N23,$O23,$P23),CHOOSE($B106,$C50,$D50,$E50,$F50,$G50,$H50)*$D77),0)
def xcf_Toe_Model_C106(): 
    try:      
        return IF(ISNUMBER(vcell("$B106","Toe Model")),MIN(CHOOSE(vcell("$B106","Toe Model"),vcell("$K23","Toe Model"),vcell("$L23","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model"),vcell("$P23","Toe Model")),CHOOSE(vcell("$B106","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model"),vcell("$H50","Toe Model"))*vcell("$D77","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C106')
    return None
xfunctions['Toe Model!C106']=xcf_Toe_Model_C106

#$D77-$C106/CHOOSE($B106,$C50,$D50,$E50,$F50,$G50,$H50)
def xcf_Toe_Model_D106(): 
    try:      
        return vcell("$D77","Toe Model")-vcell("$C106","Toe Model")/CHOOSE(vcell("$B106","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model"),vcell("$H50","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D106')
    return None
xfunctions['Toe Model!D106']=xcf_Toe_Model_D106

#IF(D106>0,MIN(IF($B106+E$98>=6,$H50,CHOOSE($B106+E$98,$C50,$D50,$E50,$F50,$G50))*D106,IF($B106+E$98>=6,$P23,CHOOSE($B106+E$98,$K23,$L23,$M23,$N23,$O23))-IF($B106>=6,$P23,CHOOSE($B106,$K23,$L23,$M23,$N23,$O23))),0)
def xcf_Toe_Model_E106(): 
    try:      
        return IF(vcell("D106","Toe Model")>0,MIN(IF(vcell("$B106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("E$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))*vcell("D106","Toe Model"),IF(vcell("$B106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P23","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("E$98","Toe Model"),vcell("$K23","Toe Model"),vcell("$L23","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model")))-IF(vcell("$B106","Toe Model")>=6,vcell("$P23","Toe Model"),CHOOSE(vcell("$B106","Toe Model"),vcell("$K23","Toe Model"),vcell("$L23","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E106')
    return None
xfunctions['Toe Model!E106']=xcf_Toe_Model_E106

#D106-E106/IF($B106+$E$98>=6,$H50,CHOOSE($B106+$E$98,$C50,$D50,$E50,$F50,$G50))
def xcf_Toe_Model_F106(): 
    try:      
        return vcell("D106","Toe Model")-vcell("E106","Toe Model")/IF(vcell("$B106","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F106')
    return None
xfunctions['Toe Model!F106']=xcf_Toe_Model_F106

#IF(F106>0,MIN(IF($B106+G$98>=6,$H50,CHOOSE($B106+G$98,$C50,$D50,$E50,$F50,$G50))*F106,IF($B106+G$98>=6,$P23,CHOOSE($B106+G$98,$K23,$L23,$M23,$N23,$O23))-IF($B106+E$98>=6,$P23,CHOOSE($B106+E$98,$K23,$L23,$M23,$N23,$O23))),0)
def xcf_Toe_Model_G106(): 
    try:      
        return IF(vcell("F106","Toe Model")>0,MIN(IF(vcell("$B106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("G$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))*vcell("F106","Toe Model"),IF(vcell("$B106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P23","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("G$98","Toe Model"),vcell("$K23","Toe Model"),vcell("$L23","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model")))-IF(vcell("$B106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P23","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("E$98","Toe Model"),vcell("$K23","Toe Model"),vcell("$L23","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G106')
    return None
xfunctions['Toe Model!G106']=xcf_Toe_Model_G106

#F106-G106/IF($B106+$G$98>=6,$H50,CHOOSE($B106+$G$98,$C50,$D50,$E50,$F50,$G50))
def xcf_Toe_Model_H106(): 
    try:      
        return vcell("F106","Toe Model")-vcell("G106","Toe Model")/IF(vcell("$B106","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H106')
    return None
xfunctions['Toe Model!H106']=xcf_Toe_Model_H106

#IF(H106>0,MIN(IF($D106+G$98>=6,$J50,CHOOSE($D106+G$98,$E50,$F50,$G50,$H50,$I50))*H106,IF($D106+G$98>=6,$R23,CHOOSE($D106+G$98,$M23,$N23,$O23,$P23,$Q23))-IF($D106+E$98>=6,$R23,CHOOSE($D106+E$98,$M23,$N23,$O23,$P23,$Q23))),0)
def xcf_Toe_Model_I106(): 
    try:      
        return IF(vcell("H106","Toe Model")>0,MIN(IF(vcell("$D106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J50","Toe Model"),CHOOSE(vcell("$D106","Toe Model")+vcell("G$98","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model"),vcell("$H50","Toe Model"),vcell("$I50","Toe Model")))*vcell("H106","Toe Model"),IF(vcell("$D106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R23","Toe Model"),CHOOSE(vcell("$D106","Toe Model")+vcell("G$98","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model"),vcell("$P23","Toe Model"),vcell("$Q23","Toe Model")))-IF(vcell("$D106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R23","Toe Model"),CHOOSE(vcell("$D106","Toe Model")+vcell("E$98","Toe Model"),vcell("$M23","Toe Model"),vcell("$N23","Toe Model"),vcell("$O23","Toe Model"),vcell("$P23","Toe Model"),vcell("$Q23","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I106')
    return None
xfunctions['Toe Model!I106']=xcf_Toe_Model_I106

#H106-I106/IF($B106+$I$98>=6,$H50,CHOOSE($B106+$I$98,$C50,$D50,$E50,$F50,$G50))
def xcf_Toe_Model_J106(): 
    try:      
        return vcell("H106","Toe Model")-vcell("I106","Toe Model")/IF(vcell("$B106","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J106')
    return None
xfunctions['Toe Model!J106']=xcf_Toe_Model_J106

#IF(J106>0,MIN(IF($F106+G$98>=6,$L50,CHOOSE($F106+G$98,$G50,$H50,$I50,$J50,$K50))*J106,IF($F106+G$98>=6,$T23,CHOOSE($F106+G$98,$O23,$P23,$Q23,$R23,$S23))-IF($F106+E$98>=6,$T23,CHOOSE($F106+E$98,$O23,$P23,$Q23,$R23,$S23))),0)
def xcf_Toe_Model_K106(): 
    try:      
        return IF(vcell("J106","Toe Model")>0,MIN(IF(vcell("$F106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L50","Toe Model"),CHOOSE(vcell("$F106","Toe Model")+vcell("G$98","Toe Model"),vcell("$G50","Toe Model"),vcell("$H50","Toe Model"),vcell("$I50","Toe Model"),vcell("$J50","Toe Model"),vcell("$K50","Toe Model")))*vcell("J106","Toe Model"),IF(vcell("$F106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T23","Toe Model"),CHOOSE(vcell("$F106","Toe Model")+vcell("G$98","Toe Model"),vcell("$O23","Toe Model"),vcell("$P23","Toe Model"),vcell("$Q23","Toe Model"),vcell("$R23","Toe Model"),vcell("$S23","Toe Model")))-IF(vcell("$F106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T23","Toe Model"),CHOOSE(vcell("$F106","Toe Model")+vcell("E$98","Toe Model"),vcell("$O23","Toe Model"),vcell("$P23","Toe Model"),vcell("$Q23","Toe Model"),vcell("$R23","Toe Model"),vcell("$S23","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K106')
    return None
xfunctions['Toe Model!K106']=xcf_Toe_Model_K106

#J106-K106/IF($B106+$K$98>=6,$H50,CHOOSE($B106+$K$98,$C50,$D50,$E50,$F50,$G50))
def xcf_Toe_Model_L106(): 
    try:      
        return vcell("J106","Toe Model")-vcell("K106","Toe Model")/IF(vcell("$B106","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L106')
    return None
xfunctions['Toe Model!L106']=xcf_Toe_Model_L106

#IF(L106>0,MIN(IF($H106+G$98>=6,$N50,CHOOSE($H106+G$98,$I50,$J50,$K50,$L50,$M50))*L106,IF($H106+G$98>=6,$V23,CHOOSE($H106+G$98,$Q23,$R23,$S23,$T23,$U23))-IF($H106+E$98>=6,$V23,CHOOSE($H106+E$98,$Q23,$R23,$S23,$T23,$U23))),0)
def xcf_Toe_Model_M106(): 
    try:      
        return IF(vcell("L106","Toe Model")>0,MIN(IF(vcell("$H106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N50","Toe Model"),CHOOSE(vcell("$H106","Toe Model")+vcell("G$98","Toe Model"),vcell("$I50","Toe Model"),vcell("$J50","Toe Model"),vcell("$K50","Toe Model"),vcell("$L50","Toe Model"),vcell("$M50","Toe Model")))*vcell("L106","Toe Model"),IF(vcell("$H106","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V23","Toe Model"),CHOOSE(vcell("$H106","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q23","Toe Model"),vcell("$R23","Toe Model"),vcell("$S23","Toe Model"),vcell("$T23","Toe Model"),vcell("$U23","Toe Model")))-IF(vcell("$H106","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V23","Toe Model"),CHOOSE(vcell("$H106","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q23","Toe Model"),vcell("$R23","Toe Model"),vcell("$S23","Toe Model"),vcell("$T23","Toe Model"),vcell("$U23","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M106')
    return None
xfunctions['Toe Model!M106']=xcf_Toe_Model_M106

#L106-M106/IF($B106+$M$98>=6,$H50,CHOOSE($B106+$M$98,$C50,$D50,$E50,$F50,$G50))
def xcf_Toe_Model_N106(): 
    try:      
        return vcell("L106","Toe Model")-vcell("M106","Toe Model")/IF(vcell("$B106","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H50","Toe Model"),CHOOSE(vcell("$B106","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C50","Toe Model"),vcell("$D50","Toe Model"),vcell("$E50","Toe Model"),vcell("$F50","Toe Model"),vcell("$G50","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N106')
    return None
xfunctions['Toe Model!N106']=xcf_Toe_Model_N106

#B77-SUM($C106,$E106,$G106,$I106,$K106,$M106)*SIN(RADIANS($D23))
def xcf_Toe_Model_O106(): 
    try:      
        return vcell("B77","Toe Model")-SUM(vcell("$C106","Toe Model"),vcell("$E106","Toe Model"),vcell("$G106","Toe Model"),vcell("$I106","Toe Model"),vcell("$K106","Toe Model"),vcell("$M106","Toe Model"))*SIN(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O106')
    return None
xfunctions['Toe Model!O106']=xcf_Toe_Model_O106

#C77+IF($D23==360,1,-1)*SUM($C106,$E106,$G106,$I106,$K106,$M106)*COS(RADIANS($D23))
def xcf_Toe_Model_P106(): 
    try:      
        return vcell("C77","Toe Model")+IF(vcell("$D23","Toe Model")==360,1,-1)*SUM(vcell("$C106","Toe Model"),vcell("$E106","Toe Model"),vcell("$G106","Toe Model"),vcell("$I106","Toe Model"),vcell("$K106","Toe Model"),vcell("$M106","Toe Model"))*COS(RADIANS(vcell("$D23","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P106')
    return None
xfunctions['Toe Model!P106']=xcf_Toe_Model_P106

#IF(MIN(C$18:C$40)>P106,IF(P105>=MIN(C$18:C$40),O105+(O106-O105)*(MIN(C$18:C$40)-P105)/(P106-P105),Q105+ABS(O106-O105)),O106)
def xcf_Toe_Model_Q106(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P106","Toe Model"),IF(vcell("P105","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O105","Toe Model")+(vcell("O106","Toe Model")-vcell("O105","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P105","Toe Model"))/(vcell("P106","Toe Model")-vcell("P105","Toe Model")),vcell("Q105","Toe Model")+ABS(vcell("O106","Toe Model")-vcell("O105","Toe Model"))),vcell("O106","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q106')
    return None
xfunctions['Toe Model!Q106']=xcf_Toe_Model_Q106

#MAX(C77+IF($D23==360,1,-1)*SUM($C106,$E106,$G106,$I106,$K106,$M106)*COS(RADIANS($D23)),MIN(C$18:C$40))
def xcf_Toe_Model_R106(): 
    try:      
        return MAX(vcell("C77","Toe Model")+IF(vcell("$D23","Toe Model")==360,1,-1)*SUM(vcell("$C106","Toe Model"),vcell("$E106","Toe Model"),vcell("$G106","Toe Model"),vcell("$I106","Toe Model"),vcell("$K106","Toe Model"),vcell("$M106","Toe Model"))*COS(RADIANS(vcell("$D23","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R106')
    return None
xfunctions['Toe Model!R106']=xcf_Toe_Model_R106

#IF(AND($C78<=$B$4,$C78>$B$5),1,IF(AND($C78<=$B$5,$C78>$B$6),2,IF(AND($C78<=$B$6,$C78>$B$7),3,IF(AND($C78<=$B$7,$C78>$B$8),4,IF(AND($C78<=$B$8,$C78>$B$9),5,6)))))
def xcf_Toe_Model_B107(): 
    try:      
        return IF(AND(vcell("$C78","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C78","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C78","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C78","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C78","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C78","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C78","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C78","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C78","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C78","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B107')
    return None
xfunctions['Toe Model!B107']=xcf_Toe_Model_B107

#IF(ISNUMBER($B107),MIN(CHOOSE($B107,$K24,$L24,$M24,$N24,$O24,$P24),CHOOSE($B107,$C51,$D51,$E51,$F51,$G51,$H51)*$D78),0)
def xcf_Toe_Model_C107(): 
    try:      
        return IF(ISNUMBER(vcell("$B107","Toe Model")),MIN(CHOOSE(vcell("$B107","Toe Model"),vcell("$K24","Toe Model"),vcell("$L24","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model"),vcell("$P24","Toe Model")),CHOOSE(vcell("$B107","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model"),vcell("$H51","Toe Model"))*vcell("$D78","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C107')
    return None
xfunctions['Toe Model!C107']=xcf_Toe_Model_C107

#$D78-$C107/CHOOSE($B107,$C51,$D51,$E51,$F51,$G51,$H51)
def xcf_Toe_Model_D107(): 
    try:      
        return vcell("$D78","Toe Model")-vcell("$C107","Toe Model")/CHOOSE(vcell("$B107","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model"),vcell("$H51","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D107')
    return None
xfunctions['Toe Model!D107']=xcf_Toe_Model_D107

#IF(D107>0,MIN(IF($B107+E$98>=6,$H51,CHOOSE($B107+E$98,$C51,$D51,$E51,$F51,$G51))*D107,IF($B107+E$98>=6,$P24,CHOOSE($B107+E$98,$K24,$L24,$M24,$N24,$O24))-IF($B107>=6,$P24,CHOOSE($B107,$K24,$L24,$M24,$N24,$O24))),0)
def xcf_Toe_Model_E107(): 
    try:      
        return IF(vcell("D107","Toe Model")>0,MIN(IF(vcell("$B107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("E$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))*vcell("D107","Toe Model"),IF(vcell("$B107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P24","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("E$98","Toe Model"),vcell("$K24","Toe Model"),vcell("$L24","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model")))-IF(vcell("$B107","Toe Model")>=6,vcell("$P24","Toe Model"),CHOOSE(vcell("$B107","Toe Model"),vcell("$K24","Toe Model"),vcell("$L24","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E107')
    return None
xfunctions['Toe Model!E107']=xcf_Toe_Model_E107

#D107-E107/IF($B107+$E$98>=6,$H51,CHOOSE($B107+$E$98,$C51,$D51,$E51,$F51,$G51))
def xcf_Toe_Model_F107(): 
    try:      
        return vcell("D107","Toe Model")-vcell("E107","Toe Model")/IF(vcell("$B107","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F107')
    return None
xfunctions['Toe Model!F107']=xcf_Toe_Model_F107

#IF(F107>0,MIN(IF($B107+G$98>=6,$H51,CHOOSE($B107+G$98,$C51,$D51,$E51,$F51,$G51))*F107,IF($B107+G$98>=6,$P24,CHOOSE($B107+G$98,$K24,$L24,$M24,$N24,$O24))-IF($B107+E$98>=6,$P24,CHOOSE($B107+E$98,$K24,$L24,$M24,$N24,$O24))),0)
def xcf_Toe_Model_G107(): 
    try:      
        return IF(vcell("F107","Toe Model")>0,MIN(IF(vcell("$B107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("G$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))*vcell("F107","Toe Model"),IF(vcell("$B107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P24","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("G$98","Toe Model"),vcell("$K24","Toe Model"),vcell("$L24","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model")))-IF(vcell("$B107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P24","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("E$98","Toe Model"),vcell("$K24","Toe Model"),vcell("$L24","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G107')
    return None
xfunctions['Toe Model!G107']=xcf_Toe_Model_G107

#F107-G107/IF($B107+$G$98>=6,$H51,CHOOSE($B107+$G$98,$C51,$D51,$E51,$F51,$G51))
def xcf_Toe_Model_H107(): 
    try:      
        return vcell("F107","Toe Model")-vcell("G107","Toe Model")/IF(vcell("$B107","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H107')
    return None
xfunctions['Toe Model!H107']=xcf_Toe_Model_H107

#IF(H107>0,MIN(IF($D107+G$98>=6,$J51,CHOOSE($D107+G$98,$E51,$F51,$G51,$H51,$I51))*H107,IF($D107+G$98>=6,$R24,CHOOSE($D107+G$98,$M24,$N24,$O24,$P24,$Q24))-IF($D107+E$98>=6,$R24,CHOOSE($D107+E$98,$M24,$N24,$O24,$P24,$Q24))),0)
def xcf_Toe_Model_I107(): 
    try:      
        return IF(vcell("H107","Toe Model")>0,MIN(IF(vcell("$D107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J51","Toe Model"),CHOOSE(vcell("$D107","Toe Model")+vcell("G$98","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model"),vcell("$H51","Toe Model"),vcell("$I51","Toe Model")))*vcell("H107","Toe Model"),IF(vcell("$D107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R24","Toe Model"),CHOOSE(vcell("$D107","Toe Model")+vcell("G$98","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model"),vcell("$P24","Toe Model"),vcell("$Q24","Toe Model")))-IF(vcell("$D107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R24","Toe Model"),CHOOSE(vcell("$D107","Toe Model")+vcell("E$98","Toe Model"),vcell("$M24","Toe Model"),vcell("$N24","Toe Model"),vcell("$O24","Toe Model"),vcell("$P24","Toe Model"),vcell("$Q24","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I107')
    return None
xfunctions['Toe Model!I107']=xcf_Toe_Model_I107

#H107-I107/IF($B107+$I$98>=6,$H51,CHOOSE($B107+$I$98,$C51,$D51,$E51,$F51,$G51))
def xcf_Toe_Model_J107(): 
    try:      
        return vcell("H107","Toe Model")-vcell("I107","Toe Model")/IF(vcell("$B107","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J107')
    return None
xfunctions['Toe Model!J107']=xcf_Toe_Model_J107

#IF(J107>0,MIN(IF($F107+G$98>=6,$L51,CHOOSE($F107+G$98,$G51,$H51,$I51,$J51,$K51))*J107,IF($F107+G$98>=6,$T24,CHOOSE($F107+G$98,$O24,$P24,$Q24,$R24,$S24))-IF($F107+E$98>=6,$T24,CHOOSE($F107+E$98,$O24,$P24,$Q24,$R24,$S24))),0)
def xcf_Toe_Model_K107(): 
    try:      
        return IF(vcell("J107","Toe Model")>0,MIN(IF(vcell("$F107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L51","Toe Model"),CHOOSE(vcell("$F107","Toe Model")+vcell("G$98","Toe Model"),vcell("$G51","Toe Model"),vcell("$H51","Toe Model"),vcell("$I51","Toe Model"),vcell("$J51","Toe Model"),vcell("$K51","Toe Model")))*vcell("J107","Toe Model"),IF(vcell("$F107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T24","Toe Model"),CHOOSE(vcell("$F107","Toe Model")+vcell("G$98","Toe Model"),vcell("$O24","Toe Model"),vcell("$P24","Toe Model"),vcell("$Q24","Toe Model"),vcell("$R24","Toe Model"),vcell("$S24","Toe Model")))-IF(vcell("$F107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T24","Toe Model"),CHOOSE(vcell("$F107","Toe Model")+vcell("E$98","Toe Model"),vcell("$O24","Toe Model"),vcell("$P24","Toe Model"),vcell("$Q24","Toe Model"),vcell("$R24","Toe Model"),vcell("$S24","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K107')
    return None
xfunctions['Toe Model!K107']=xcf_Toe_Model_K107

#J107-K107/IF($B107+$K$98>=6,$H51,CHOOSE($B107+$K$98,$C51,$D51,$E51,$F51,$G51))
def xcf_Toe_Model_L107(): 
    try:      
        return vcell("J107","Toe Model")-vcell("K107","Toe Model")/IF(vcell("$B107","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L107')
    return None
xfunctions['Toe Model!L107']=xcf_Toe_Model_L107

#IF(L107>0,MIN(IF($H107+G$98>=6,$N51,CHOOSE($H107+G$98,$I51,$J51,$K51,$L51,$M51))*L107,IF($H107+G$98>=6,$V24,CHOOSE($H107+G$98,$Q24,$R24,$S24,$T24,$U24))-IF($H107+E$98>=6,$V24,CHOOSE($H107+E$98,$Q24,$R24,$S24,$T24,$U24))),0)
def xcf_Toe_Model_M107(): 
    try:      
        return IF(vcell("L107","Toe Model")>0,MIN(IF(vcell("$H107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N51","Toe Model"),CHOOSE(vcell("$H107","Toe Model")+vcell("G$98","Toe Model"),vcell("$I51","Toe Model"),vcell("$J51","Toe Model"),vcell("$K51","Toe Model"),vcell("$L51","Toe Model"),vcell("$M51","Toe Model")))*vcell("L107","Toe Model"),IF(vcell("$H107","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V24","Toe Model"),CHOOSE(vcell("$H107","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q24","Toe Model"),vcell("$R24","Toe Model"),vcell("$S24","Toe Model"),vcell("$T24","Toe Model"),vcell("$U24","Toe Model")))-IF(vcell("$H107","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V24","Toe Model"),CHOOSE(vcell("$H107","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q24","Toe Model"),vcell("$R24","Toe Model"),vcell("$S24","Toe Model"),vcell("$T24","Toe Model"),vcell("$U24","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M107')
    return None
xfunctions['Toe Model!M107']=xcf_Toe_Model_M107

#L107-M107/IF($B107+$M$98>=6,$H51,CHOOSE($B107+$M$98,$C51,$D51,$E51,$F51,$G51))
def xcf_Toe_Model_N107(): 
    try:      
        return vcell("L107","Toe Model")-vcell("M107","Toe Model")/IF(vcell("$B107","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H51","Toe Model"),CHOOSE(vcell("$B107","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C51","Toe Model"),vcell("$D51","Toe Model"),vcell("$E51","Toe Model"),vcell("$F51","Toe Model"),vcell("$G51","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N107')
    return None
xfunctions['Toe Model!N107']=xcf_Toe_Model_N107

#B78-SUM($C107,$E107,$G107,$I107,$K107,$M107)*SIN(RADIANS($D24))
def xcf_Toe_Model_O107(): 
    try:      
        return vcell("B78","Toe Model")-SUM(vcell("$C107","Toe Model"),vcell("$E107","Toe Model"),vcell("$G107","Toe Model"),vcell("$I107","Toe Model"),vcell("$K107","Toe Model"),vcell("$M107","Toe Model"))*SIN(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O107')
    return None
xfunctions['Toe Model!O107']=xcf_Toe_Model_O107

#C78+IF($D24==360,1,-1)*SUM($C107,$E107,$G107,$I107,$K107,$M107)*COS(RADIANS($D24))
def xcf_Toe_Model_P107(): 
    try:      
        return vcell("C78","Toe Model")+IF(vcell("$D24","Toe Model")==360,1,-1)*SUM(vcell("$C107","Toe Model"),vcell("$E107","Toe Model"),vcell("$G107","Toe Model"),vcell("$I107","Toe Model"),vcell("$K107","Toe Model"),vcell("$M107","Toe Model"))*COS(RADIANS(vcell("$D24","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P107')
    return None
xfunctions['Toe Model!P107']=xcf_Toe_Model_P107

#IF(MIN(C$18:C$40)>P107,IF(P106>=MIN(C$18:C$40),O106+(O107-O106)*(MIN(C$18:C$40)-P106)/(P107-P106),Q106+ABS(O107-O106)),O107)
def xcf_Toe_Model_Q107(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P107","Toe Model"),IF(vcell("P106","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O106","Toe Model")+(vcell("O107","Toe Model")-vcell("O106","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P106","Toe Model"))/(vcell("P107","Toe Model")-vcell("P106","Toe Model")),vcell("Q106","Toe Model")+ABS(vcell("O107","Toe Model")-vcell("O106","Toe Model"))),vcell("O107","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q107')
    return None
xfunctions['Toe Model!Q107']=xcf_Toe_Model_Q107

#MAX(C78+IF($D24==360,1,-1)*SUM($C107,$E107,$G107,$I107,$K107,$M107)*COS(RADIANS($D24)),MIN(C$18:C$40))
def xcf_Toe_Model_R107(): 
    try:      
        return MAX(vcell("C78","Toe Model")+IF(vcell("$D24","Toe Model")==360,1,-1)*SUM(vcell("$C107","Toe Model"),vcell("$E107","Toe Model"),vcell("$G107","Toe Model"),vcell("$I107","Toe Model"),vcell("$K107","Toe Model"),vcell("$M107","Toe Model"))*COS(RADIANS(vcell("$D24","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R107')
    return None
xfunctions['Toe Model!R107']=xcf_Toe_Model_R107

#IF(AND($C79<=$B$4,$C79>$B$5),1,IF(AND($C79<=$B$5,$C79>$B$6),2,IF(AND($C79<=$B$6,$C79>$B$7),3,IF(AND($C79<=$B$7,$C79>$B$8),4,IF(AND($C79<=$B$8,$C79>$B$9),5,6)))))
def xcf_Toe_Model_B108(): 
    try:      
        return IF(AND(vcell("$C79","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C79","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C79","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C79","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C79","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C79","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C79","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C79","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C79","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C79","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B108')
    return None
xfunctions['Toe Model!B108']=xcf_Toe_Model_B108

#IF(ISNUMBER($B108),MIN(CHOOSE($B108,$K25,$L25,$M25,$N25,$O25,$P25),CHOOSE($B108,$C52,$D52,$E52,$F52,$G52,$H52)*$D79),0)
def xcf_Toe_Model_C108(): 
    try:      
        return IF(ISNUMBER(vcell("$B108","Toe Model")),MIN(CHOOSE(vcell("$B108","Toe Model"),vcell("$K25","Toe Model"),vcell("$L25","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model"),vcell("$P25","Toe Model")),CHOOSE(vcell("$B108","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model"),vcell("$H52","Toe Model"))*vcell("$D79","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C108')
    return None
xfunctions['Toe Model!C108']=xcf_Toe_Model_C108

#$D79-$C108/CHOOSE($B108,$C52,$D52,$E52,$F52,$G52,$H52)
def xcf_Toe_Model_D108(): 
    try:      
        return vcell("$D79","Toe Model")-vcell("$C108","Toe Model")/CHOOSE(vcell("$B108","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model"),vcell("$H52","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D108')
    return None
xfunctions['Toe Model!D108']=xcf_Toe_Model_D108

#IF(D108>0,MIN(IF($B108+E$98>=6,$H52,CHOOSE($B108+E$98,$C52,$D52,$E52,$F52,$G52))*D108,IF($B108+E$98>=6,$P25,CHOOSE($B108+E$98,$K25,$L25,$M25,$N25,$O25))-IF($B108>=6,$P25,CHOOSE($B108,$K25,$L25,$M25,$N25,$O25))),0)
def xcf_Toe_Model_E108(): 
    try:      
        return IF(vcell("D108","Toe Model")>0,MIN(IF(vcell("$B108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("E$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))*vcell("D108","Toe Model"),IF(vcell("$B108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P25","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("E$98","Toe Model"),vcell("$K25","Toe Model"),vcell("$L25","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model")))-IF(vcell("$B108","Toe Model")>=6,vcell("$P25","Toe Model"),CHOOSE(vcell("$B108","Toe Model"),vcell("$K25","Toe Model"),vcell("$L25","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E108')
    return None
xfunctions['Toe Model!E108']=xcf_Toe_Model_E108

#D108-E108/IF($B108+$E$98>=6,$H52,CHOOSE($B108+$E$98,$C52,$D52,$E52,$F52,$G52))
def xcf_Toe_Model_F108(): 
    try:      
        return vcell("D108","Toe Model")-vcell("E108","Toe Model")/IF(vcell("$B108","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F108')
    return None
xfunctions['Toe Model!F108']=xcf_Toe_Model_F108

#IF(F108>0,MIN(IF($B108+G$98>=6,$H52,CHOOSE($B108+G$98,$C52,$D52,$E52,$F52,$G52))*F108,IF($B108+G$98>=6,$P25,CHOOSE($B108+G$98,$K25,$L25,$M25,$N25,$O25))-IF($B108+E$98>=6,$P25,CHOOSE($B108+E$98,$K25,$L25,$M25,$N25,$O25))),0)
def xcf_Toe_Model_G108(): 
    try:      
        return IF(vcell("F108","Toe Model")>0,MIN(IF(vcell("$B108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("G$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))*vcell("F108","Toe Model"),IF(vcell("$B108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P25","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("G$98","Toe Model"),vcell("$K25","Toe Model"),vcell("$L25","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model")))-IF(vcell("$B108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P25","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("E$98","Toe Model"),vcell("$K25","Toe Model"),vcell("$L25","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G108')
    return None
xfunctions['Toe Model!G108']=xcf_Toe_Model_G108

#F108-G108/IF($B108+$G$98>=6,$H52,CHOOSE($B108+$G$98,$C52,$D52,$E52,$F52,$G52))
def xcf_Toe_Model_H108(): 
    try:      
        return vcell("F108","Toe Model")-vcell("G108","Toe Model")/IF(vcell("$B108","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H108')
    return None
xfunctions['Toe Model!H108']=xcf_Toe_Model_H108

#IF(H108>0,MIN(IF($D108+G$98>=6,$J52,CHOOSE($D108+G$98,$E52,$F52,$G52,$H52,$I52))*H108,IF($D108+G$98>=6,$R25,CHOOSE($D108+G$98,$M25,$N25,$O25,$P25,$Q25))-IF($D108+E$98>=6,$R25,CHOOSE($D108+E$98,$M25,$N25,$O25,$P25,$Q25))),0)
def xcf_Toe_Model_I108(): 
    try:      
        return IF(vcell("H108","Toe Model")>0,MIN(IF(vcell("$D108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J52","Toe Model"),CHOOSE(vcell("$D108","Toe Model")+vcell("G$98","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model"),vcell("$H52","Toe Model"),vcell("$I52","Toe Model")))*vcell("H108","Toe Model"),IF(vcell("$D108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R25","Toe Model"),CHOOSE(vcell("$D108","Toe Model")+vcell("G$98","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model"),vcell("$P25","Toe Model"),vcell("$Q25","Toe Model")))-IF(vcell("$D108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R25","Toe Model"),CHOOSE(vcell("$D108","Toe Model")+vcell("E$98","Toe Model"),vcell("$M25","Toe Model"),vcell("$N25","Toe Model"),vcell("$O25","Toe Model"),vcell("$P25","Toe Model"),vcell("$Q25","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I108')
    return None
xfunctions['Toe Model!I108']=xcf_Toe_Model_I108

#H108-I108/IF($B108+$I$98>=6,$H52,CHOOSE($B108+$I$98,$C52,$D52,$E52,$F52,$G52))
def xcf_Toe_Model_J108(): 
    try:      
        return vcell("H108","Toe Model")-vcell("I108","Toe Model")/IF(vcell("$B108","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J108')
    return None
xfunctions['Toe Model!J108']=xcf_Toe_Model_J108

#IF(J108>0,MIN(IF($F108+G$98>=6,$L52,CHOOSE($F108+G$98,$G52,$H52,$I52,$J52,$K52))*J108,IF($F108+G$98>=6,$T25,CHOOSE($F108+G$98,$O25,$P25,$Q25,$R25,$S25))-IF($F108+E$98>=6,$T25,CHOOSE($F108+E$98,$O25,$P25,$Q25,$R25,$S25))),0)
def xcf_Toe_Model_K108(): 
    try:      
        return IF(vcell("J108","Toe Model")>0,MIN(IF(vcell("$F108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L52","Toe Model"),CHOOSE(vcell("$F108","Toe Model")+vcell("G$98","Toe Model"),vcell("$G52","Toe Model"),vcell("$H52","Toe Model"),vcell("$I52","Toe Model"),vcell("$J52","Toe Model"),vcell("$K52","Toe Model")))*vcell("J108","Toe Model"),IF(vcell("$F108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T25","Toe Model"),CHOOSE(vcell("$F108","Toe Model")+vcell("G$98","Toe Model"),vcell("$O25","Toe Model"),vcell("$P25","Toe Model"),vcell("$Q25","Toe Model"),vcell("$R25","Toe Model"),vcell("$S25","Toe Model")))-IF(vcell("$F108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T25","Toe Model"),CHOOSE(vcell("$F108","Toe Model")+vcell("E$98","Toe Model"),vcell("$O25","Toe Model"),vcell("$P25","Toe Model"),vcell("$Q25","Toe Model"),vcell("$R25","Toe Model"),vcell("$S25","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K108')
    return None
xfunctions['Toe Model!K108']=xcf_Toe_Model_K108

#J108-K108/IF($B108+$K$98>=6,$H52,CHOOSE($B108+$K$98,$C52,$D52,$E52,$F52,$G52))
def xcf_Toe_Model_L108(): 
    try:      
        return vcell("J108","Toe Model")-vcell("K108","Toe Model")/IF(vcell("$B108","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L108')
    return None
xfunctions['Toe Model!L108']=xcf_Toe_Model_L108

#IF(L108>0,MIN(IF($H108+G$98>=6,$N52,CHOOSE($H108+G$98,$I52,$J52,$K52,$L52,$M52))*L108,IF($H108+G$98>=6,$V25,CHOOSE($H108+G$98,$Q25,$R25,$S25,$T25,$U25))-IF($H108+E$98>=6,$V25,CHOOSE($H108+E$98,$Q25,$R25,$S25,$T25,$U25))),0)
def xcf_Toe_Model_M108(): 
    try:      
        return IF(vcell("L108","Toe Model")>0,MIN(IF(vcell("$H108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N52","Toe Model"),CHOOSE(vcell("$H108","Toe Model")+vcell("G$98","Toe Model"),vcell("$I52","Toe Model"),vcell("$J52","Toe Model"),vcell("$K52","Toe Model"),vcell("$L52","Toe Model"),vcell("$M52","Toe Model")))*vcell("L108","Toe Model"),IF(vcell("$H108","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V25","Toe Model"),CHOOSE(vcell("$H108","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q25","Toe Model"),vcell("$R25","Toe Model"),vcell("$S25","Toe Model"),vcell("$T25","Toe Model"),vcell("$U25","Toe Model")))-IF(vcell("$H108","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V25","Toe Model"),CHOOSE(vcell("$H108","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q25","Toe Model"),vcell("$R25","Toe Model"),vcell("$S25","Toe Model"),vcell("$T25","Toe Model"),vcell("$U25","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M108')
    return None
xfunctions['Toe Model!M108']=xcf_Toe_Model_M108

#L108-M108/IF($B108+$M$98>=6,$H52,CHOOSE($B108+$M$98,$C52,$D52,$E52,$F52,$G52))
def xcf_Toe_Model_N108(): 
    try:      
        return vcell("L108","Toe Model")-vcell("M108","Toe Model")/IF(vcell("$B108","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H52","Toe Model"),CHOOSE(vcell("$B108","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C52","Toe Model"),vcell("$D52","Toe Model"),vcell("$E52","Toe Model"),vcell("$F52","Toe Model"),vcell("$G52","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N108')
    return None
xfunctions['Toe Model!N108']=xcf_Toe_Model_N108

#B79-SUM($C108,$E108,$G108,$I108,$K108,$M108)*SIN(RADIANS($D25))
def xcf_Toe_Model_O108(): 
    try:      
        return vcell("B79","Toe Model")-SUM(vcell("$C108","Toe Model"),vcell("$E108","Toe Model"),vcell("$G108","Toe Model"),vcell("$I108","Toe Model"),vcell("$K108","Toe Model"),vcell("$M108","Toe Model"))*SIN(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O108')
    return None
xfunctions['Toe Model!O108']=xcf_Toe_Model_O108

#C79+IF($D25==360,1,-1)*SUM($C108,$E108,$G108,$I108,$K108,$M108)*COS(RADIANS($D25))
def xcf_Toe_Model_P108(): 
    try:      
        return vcell("C79","Toe Model")+IF(vcell("$D25","Toe Model")==360,1,-1)*SUM(vcell("$C108","Toe Model"),vcell("$E108","Toe Model"),vcell("$G108","Toe Model"),vcell("$I108","Toe Model"),vcell("$K108","Toe Model"),vcell("$M108","Toe Model"))*COS(RADIANS(vcell("$D25","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P108')
    return None
xfunctions['Toe Model!P108']=xcf_Toe_Model_P108

#IF(MIN(C$18:C$40)>P108,IF(P107>=MIN(C$18:C$40),O107+(O108-O107)*(MIN(C$18:C$40)-P107)/(P108-P107),Q107+ABS(O108-O107)),O108)
def xcf_Toe_Model_Q108(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P108","Toe Model"),IF(vcell("P107","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O107","Toe Model")+(vcell("O108","Toe Model")-vcell("O107","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P107","Toe Model"))/(vcell("P108","Toe Model")-vcell("P107","Toe Model")),vcell("Q107","Toe Model")+ABS(vcell("O108","Toe Model")-vcell("O107","Toe Model"))),vcell("O108","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q108')
    return None
xfunctions['Toe Model!Q108']=xcf_Toe_Model_Q108

#MAX(C79+IF($D25==360,1,-1)*SUM($C108,$E108,$G108,$I108,$K108,$M108)*COS(RADIANS($D25)),MIN(C$18:C$40))
def xcf_Toe_Model_R108(): 
    try:      
        return MAX(vcell("C79","Toe Model")+IF(vcell("$D25","Toe Model")==360,1,-1)*SUM(vcell("$C108","Toe Model"),vcell("$E108","Toe Model"),vcell("$G108","Toe Model"),vcell("$I108","Toe Model"),vcell("$K108","Toe Model"),vcell("$M108","Toe Model"))*COS(RADIANS(vcell("$D25","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R108')
    return None
xfunctions['Toe Model!R108']=xcf_Toe_Model_R108

#IF(AND($C80<=$B$4,$C80>$B$5),1,IF(AND($C80<=$B$5,$C80>$B$6),2,IF(AND($C80<=$B$6,$C80>$B$7),3,IF(AND($C80<=$B$7,$C80>$B$8),4,IF(AND($C80<=$B$8,$C80>$B$9),5,6)))))
def xcf_Toe_Model_B109(): 
    try:      
        return IF(AND(vcell("$C80","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C80","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C80","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C80","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C80","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C80","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C80","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C80","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C80","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C80","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B109')
    return None
xfunctions['Toe Model!B109']=xcf_Toe_Model_B109

#IF(ISNUMBER($B109),MIN(CHOOSE($B109,$K26,$L26,$M26,$N26,$O26,$P26),CHOOSE($B109,$C53,$D53,$E53,$F53,$G53,$H53)*$D80),0)
def xcf_Toe_Model_C109(): 
    try:      
        return IF(ISNUMBER(vcell("$B109","Toe Model")),MIN(CHOOSE(vcell("$B109","Toe Model"),vcell("$K26","Toe Model"),vcell("$L26","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model"),vcell("$P26","Toe Model")),CHOOSE(vcell("$B109","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model"),vcell("$H53","Toe Model"))*vcell("$D80","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C109')
    return None
xfunctions['Toe Model!C109']=xcf_Toe_Model_C109

#$D80-$C109/CHOOSE($B109,$C53,$D53,$E53,$F53,$G53,$H53)
def xcf_Toe_Model_D109(): 
    try:      
        return vcell("$D80","Toe Model")-vcell("$C109","Toe Model")/CHOOSE(vcell("$B109","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model"),vcell("$H53","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D109')
    return None
xfunctions['Toe Model!D109']=xcf_Toe_Model_D109

#IF(D109>0,MIN(IF($B109+E$98>=6,$H53,CHOOSE($B109+E$98,$C53,$D53,$E53,$F53,$G53))*D109,IF($B109+E$98>=6,$P26,CHOOSE($B109+E$98,$K26,$L26,$M26,$N26,$O26))-IF($B109>=6,$P26,CHOOSE($B109,$K26,$L26,$M26,$N26,$O26))),0)
def xcf_Toe_Model_E109(): 
    try:      
        return IF(vcell("D109","Toe Model")>0,MIN(IF(vcell("$B109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("E$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))*vcell("D109","Toe Model"),IF(vcell("$B109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P26","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("E$98","Toe Model"),vcell("$K26","Toe Model"),vcell("$L26","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model")))-IF(vcell("$B109","Toe Model")>=6,vcell("$P26","Toe Model"),CHOOSE(vcell("$B109","Toe Model"),vcell("$K26","Toe Model"),vcell("$L26","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E109')
    return None
xfunctions['Toe Model!E109']=xcf_Toe_Model_E109

#D109-E109/IF($B109+$E$98>=6,$H53,CHOOSE($B109+$E$98,$C53,$D53,$E53,$F53,$G53))
def xcf_Toe_Model_F109(): 
    try:      
        return vcell("D109","Toe Model")-vcell("E109","Toe Model")/IF(vcell("$B109","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F109')
    return None
xfunctions['Toe Model!F109']=xcf_Toe_Model_F109

#IF(F109>0,MIN(IF($B109+G$98>=6,$H53,CHOOSE($B109+G$98,$C53,$D53,$E53,$F53,$G53))*F109,IF($B109+G$98>=6,$P26,CHOOSE($B109+G$98,$K26,$L26,$M26,$N26,$O26))-IF($B109+E$98>=6,$P26,CHOOSE($B109+E$98,$K26,$L26,$M26,$N26,$O26))),0)
def xcf_Toe_Model_G109(): 
    try:      
        return IF(vcell("F109","Toe Model")>0,MIN(IF(vcell("$B109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("G$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))*vcell("F109","Toe Model"),IF(vcell("$B109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P26","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("G$98","Toe Model"),vcell("$K26","Toe Model"),vcell("$L26","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model")))-IF(vcell("$B109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P26","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("E$98","Toe Model"),vcell("$K26","Toe Model"),vcell("$L26","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G109')
    return None
xfunctions['Toe Model!G109']=xcf_Toe_Model_G109

#F109-G109/IF($B109+$G$98>=6,$H53,CHOOSE($B109+$G$98,$C53,$D53,$E53,$F53,$G53))
def xcf_Toe_Model_H109(): 
    try:      
        return vcell("F109","Toe Model")-vcell("G109","Toe Model")/IF(vcell("$B109","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H109')
    return None
xfunctions['Toe Model!H109']=xcf_Toe_Model_H109

#IF(H109>0,MIN(IF($D109+G$98>=6,$J53,CHOOSE($D109+G$98,$E53,$F53,$G53,$H53,$I53))*H109,IF($D109+G$98>=6,$R26,CHOOSE($D109+G$98,$M26,$N26,$O26,$P26,$Q26))-IF($D109+E$98>=6,$R26,CHOOSE($D109+E$98,$M26,$N26,$O26,$P26,$Q26))),0)
def xcf_Toe_Model_I109(): 
    try:      
        return IF(vcell("H109","Toe Model")>0,MIN(IF(vcell("$D109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J53","Toe Model"),CHOOSE(vcell("$D109","Toe Model")+vcell("G$98","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model"),vcell("$H53","Toe Model"),vcell("$I53","Toe Model")))*vcell("H109","Toe Model"),IF(vcell("$D109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R26","Toe Model"),CHOOSE(vcell("$D109","Toe Model")+vcell("G$98","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model"),vcell("$P26","Toe Model"),vcell("$Q26","Toe Model")))-IF(vcell("$D109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R26","Toe Model"),CHOOSE(vcell("$D109","Toe Model")+vcell("E$98","Toe Model"),vcell("$M26","Toe Model"),vcell("$N26","Toe Model"),vcell("$O26","Toe Model"),vcell("$P26","Toe Model"),vcell("$Q26","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I109')
    return None
xfunctions['Toe Model!I109']=xcf_Toe_Model_I109

#H109-I109/IF($B109+$I$98>=6,$H53,CHOOSE($B109+$I$98,$C53,$D53,$E53,$F53,$G53))
def xcf_Toe_Model_J109(): 
    try:      
        return vcell("H109","Toe Model")-vcell("I109","Toe Model")/IF(vcell("$B109","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J109')
    return None
xfunctions['Toe Model!J109']=xcf_Toe_Model_J109

#IF(J109>0,MIN(IF($F109+G$98>=6,$L53,CHOOSE($F109+G$98,$G53,$H53,$I53,$J53,$K53))*J109,IF($F109+G$98>=6,$T26,CHOOSE($F109+G$98,$O26,$P26,$Q26,$R26,$S26))-IF($F109+E$98>=6,$T26,CHOOSE($F109+E$98,$O26,$P26,$Q26,$R26,$S26))),0)
def xcf_Toe_Model_K109(): 
    try:      
        return IF(vcell("J109","Toe Model")>0,MIN(IF(vcell("$F109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L53","Toe Model"),CHOOSE(vcell("$F109","Toe Model")+vcell("G$98","Toe Model"),vcell("$G53","Toe Model"),vcell("$H53","Toe Model"),vcell("$I53","Toe Model"),vcell("$J53","Toe Model"),vcell("$K53","Toe Model")))*vcell("J109","Toe Model"),IF(vcell("$F109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T26","Toe Model"),CHOOSE(vcell("$F109","Toe Model")+vcell("G$98","Toe Model"),vcell("$O26","Toe Model"),vcell("$P26","Toe Model"),vcell("$Q26","Toe Model"),vcell("$R26","Toe Model"),vcell("$S26","Toe Model")))-IF(vcell("$F109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T26","Toe Model"),CHOOSE(vcell("$F109","Toe Model")+vcell("E$98","Toe Model"),vcell("$O26","Toe Model"),vcell("$P26","Toe Model"),vcell("$Q26","Toe Model"),vcell("$R26","Toe Model"),vcell("$S26","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K109')
    return None
xfunctions['Toe Model!K109']=xcf_Toe_Model_K109

#J109-K109/IF($B109+$K$98>=6,$H53,CHOOSE($B109+$K$98,$C53,$D53,$E53,$F53,$G53))
def xcf_Toe_Model_L109(): 
    try:      
        return vcell("J109","Toe Model")-vcell("K109","Toe Model")/IF(vcell("$B109","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L109')
    return None
xfunctions['Toe Model!L109']=xcf_Toe_Model_L109

#IF(L109>0,MIN(IF($H109+G$98>=6,$N53,CHOOSE($H109+G$98,$I53,$J53,$K53,$L53,$M53))*L109,IF($H109+G$98>=6,$V26,CHOOSE($H109+G$98,$Q26,$R26,$S26,$T26,$U26))-IF($H109+E$98>=6,$V26,CHOOSE($H109+E$98,$Q26,$R26,$S26,$T26,$U26))),0)
def xcf_Toe_Model_M109(): 
    try:      
        return IF(vcell("L109","Toe Model")>0,MIN(IF(vcell("$H109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N53","Toe Model"),CHOOSE(vcell("$H109","Toe Model")+vcell("G$98","Toe Model"),vcell("$I53","Toe Model"),vcell("$J53","Toe Model"),vcell("$K53","Toe Model"),vcell("$L53","Toe Model"),vcell("$M53","Toe Model")))*vcell("L109","Toe Model"),IF(vcell("$H109","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V26","Toe Model"),CHOOSE(vcell("$H109","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q26","Toe Model"),vcell("$R26","Toe Model"),vcell("$S26","Toe Model"),vcell("$T26","Toe Model"),vcell("$U26","Toe Model")))-IF(vcell("$H109","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V26","Toe Model"),CHOOSE(vcell("$H109","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q26","Toe Model"),vcell("$R26","Toe Model"),vcell("$S26","Toe Model"),vcell("$T26","Toe Model"),vcell("$U26","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M109')
    return None
xfunctions['Toe Model!M109']=xcf_Toe_Model_M109

#L109-M109/IF($B109+$M$98>=6,$H53,CHOOSE($B109+$M$98,$C53,$D53,$E53,$F53,$G53))
def xcf_Toe_Model_N109(): 
    try:      
        return vcell("L109","Toe Model")-vcell("M109","Toe Model")/IF(vcell("$B109","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H53","Toe Model"),CHOOSE(vcell("$B109","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C53","Toe Model"),vcell("$D53","Toe Model"),vcell("$E53","Toe Model"),vcell("$F53","Toe Model"),vcell("$G53","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N109')
    return None
xfunctions['Toe Model!N109']=xcf_Toe_Model_N109

#B80-SUM($C109,$E109,$G109,$I109,$K109,$M109)*SIN(RADIANS($D26))
def xcf_Toe_Model_O109(): 
    try:      
        return vcell("B80","Toe Model")-SUM(vcell("$C109","Toe Model"),vcell("$E109","Toe Model"),vcell("$G109","Toe Model"),vcell("$I109","Toe Model"),vcell("$K109","Toe Model"),vcell("$M109","Toe Model"))*SIN(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O109')
    return None
xfunctions['Toe Model!O109']=xcf_Toe_Model_O109

#C80+IF($D26==360,1,-1)*SUM($C109,$E109,$G109,$I109,$K109,$M109)*COS(RADIANS($D26))
def xcf_Toe_Model_P109(): 
    try:      
        return vcell("C80","Toe Model")+IF(vcell("$D26","Toe Model")==360,1,-1)*SUM(vcell("$C109","Toe Model"),vcell("$E109","Toe Model"),vcell("$G109","Toe Model"),vcell("$I109","Toe Model"),vcell("$K109","Toe Model"),vcell("$M109","Toe Model"))*COS(RADIANS(vcell("$D26","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P109')
    return None
xfunctions['Toe Model!P109']=xcf_Toe_Model_P109

#IF(MIN(C$18:C$40)>P109,IF(P108>=MIN(C$18:C$40),O108+(O109-O108)*(MIN(C$18:C$40)-P108)/(P109-P108),Q108+ABS(O109-O108)),O109)
def xcf_Toe_Model_Q109(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P109","Toe Model"),IF(vcell("P108","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O108","Toe Model")+(vcell("O109","Toe Model")-vcell("O108","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P108","Toe Model"))/(vcell("P109","Toe Model")-vcell("P108","Toe Model")),vcell("Q108","Toe Model")+ABS(vcell("O109","Toe Model")-vcell("O108","Toe Model"))),vcell("O109","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q109')
    return None
xfunctions['Toe Model!Q109']=xcf_Toe_Model_Q109

#MAX(C80+IF($D26==360,1,-1)*SUM($C109,$E109,$G109,$I109,$K109,$M109)*COS(RADIANS($D26)),MIN(C$18:C$40))
def xcf_Toe_Model_R109(): 
    try:      
        return MAX(vcell("C80","Toe Model")+IF(vcell("$D26","Toe Model")==360,1,-1)*SUM(vcell("$C109","Toe Model"),vcell("$E109","Toe Model"),vcell("$G109","Toe Model"),vcell("$I109","Toe Model"),vcell("$K109","Toe Model"),vcell("$M109","Toe Model"))*COS(RADIANS(vcell("$D26","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R109')
    return None
xfunctions['Toe Model!R109']=xcf_Toe_Model_R109

#IF(AND($C81<=$B$4,$C81>$B$5),1,IF(AND($C81<=$B$5,$C81>$B$6),2,IF(AND($C81<=$B$6,$C81>$B$7),3,IF(AND($C81<=$B$7,$C81>$B$8),4,IF(AND($C81<=$B$8,$C81>$B$9),5,6)))))
def xcf_Toe_Model_B110(): 
    try:      
        return IF(AND(vcell("$C81","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C81","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C81","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C81","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C81","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C81","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C81","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C81","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C81","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C81","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B110')
    return None
xfunctions['Toe Model!B110']=xcf_Toe_Model_B110

#IF(ISNUMBER($B110),MIN(CHOOSE($B110,$K27,$L27,$M27,$N27,$O27,$P27),CHOOSE($B110,$C54,$D54,$E54,$F54,$G54,$H54)*$D81),0)
def xcf_Toe_Model_C110(): 
    try:      
        return IF(ISNUMBER(vcell("$B110","Toe Model")),MIN(CHOOSE(vcell("$B110","Toe Model"),vcell("$K27","Toe Model"),vcell("$L27","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model"),vcell("$P27","Toe Model")),CHOOSE(vcell("$B110","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model"),vcell("$H54","Toe Model"))*vcell("$D81","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C110')
    return None
xfunctions['Toe Model!C110']=xcf_Toe_Model_C110

#$D81-$C110/CHOOSE($B110,$C54,$D54,$E54,$F54,$G54,$H54)
def xcf_Toe_Model_D110(): 
    try:      
        return vcell("$D81","Toe Model")-vcell("$C110","Toe Model")/CHOOSE(vcell("$B110","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model"),vcell("$H54","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D110')
    return None
xfunctions['Toe Model!D110']=xcf_Toe_Model_D110

#IF(D110>0,MIN(IF($B110+E$98>=6,$H54,CHOOSE($B110+E$98,$C54,$D54,$E54,$F54,$G54))*D110,IF($B110+E$98>=6,$P27,CHOOSE($B110+E$98,$K27,$L27,$M27,$N27,$O27))-IF($B110>=6,$P27,CHOOSE($B110,$K27,$L27,$M27,$N27,$O27))),0)
def xcf_Toe_Model_E110(): 
    try:      
        return IF(vcell("D110","Toe Model")>0,MIN(IF(vcell("$B110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("E$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))*vcell("D110","Toe Model"),IF(vcell("$B110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P27","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("E$98","Toe Model"),vcell("$K27","Toe Model"),vcell("$L27","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model")))-IF(vcell("$B110","Toe Model")>=6,vcell("$P27","Toe Model"),CHOOSE(vcell("$B110","Toe Model"),vcell("$K27","Toe Model"),vcell("$L27","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E110')
    return None
xfunctions['Toe Model!E110']=xcf_Toe_Model_E110

#D110-E110/IF($B110+$E$98>=6,$H54,CHOOSE($B110+$E$98,$C54,$D54,$E54,$F54,$G54))
def xcf_Toe_Model_F110(): 
    try:      
        return vcell("D110","Toe Model")-vcell("E110","Toe Model")/IF(vcell("$B110","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F110')
    return None
xfunctions['Toe Model!F110']=xcf_Toe_Model_F110

#IF(F110>0,MIN(IF($B110+G$98>=6,$H54,CHOOSE($B110+G$98,$C54,$D54,$E54,$F54,$G54))*F110,IF($B110+G$98>=6,$P27,CHOOSE($B110+G$98,$K27,$L27,$M27,$N27,$O27))-IF($B110+E$98>=6,$P27,CHOOSE($B110+E$98,$K27,$L27,$M27,$N27,$O27))),0)
def xcf_Toe_Model_G110(): 
    try:      
        return IF(vcell("F110","Toe Model")>0,MIN(IF(vcell("$B110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("G$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))*vcell("F110","Toe Model"),IF(vcell("$B110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P27","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("G$98","Toe Model"),vcell("$K27","Toe Model"),vcell("$L27","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model")))-IF(vcell("$B110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P27","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("E$98","Toe Model"),vcell("$K27","Toe Model"),vcell("$L27","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G110')
    return None
xfunctions['Toe Model!G110']=xcf_Toe_Model_G110

#F110-G110/IF($B110+$G$98>=6,$H54,CHOOSE($B110+$G$98,$C54,$D54,$E54,$F54,$G54))
def xcf_Toe_Model_H110(): 
    try:      
        return vcell("F110","Toe Model")-vcell("G110","Toe Model")/IF(vcell("$B110","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H110')
    return None
xfunctions['Toe Model!H110']=xcf_Toe_Model_H110

#IF(H110>0,MIN(IF($D110+G$98>=6,$J54,CHOOSE($D110+G$98,$E54,$F54,$G54,$H54,$I54))*H110,IF($D110+G$98>=6,$R27,CHOOSE($D110+G$98,$M27,$N27,$O27,$P27,$Q27))-IF($D110+E$98>=6,$R27,CHOOSE($D110+E$98,$M27,$N27,$O27,$P27,$Q27))),0)
def xcf_Toe_Model_I110(): 
    try:      
        return IF(vcell("H110","Toe Model")>0,MIN(IF(vcell("$D110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J54","Toe Model"),CHOOSE(vcell("$D110","Toe Model")+vcell("G$98","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model"),vcell("$H54","Toe Model"),vcell("$I54","Toe Model")))*vcell("H110","Toe Model"),IF(vcell("$D110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R27","Toe Model"),CHOOSE(vcell("$D110","Toe Model")+vcell("G$98","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model"),vcell("$P27","Toe Model"),vcell("$Q27","Toe Model")))-IF(vcell("$D110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R27","Toe Model"),CHOOSE(vcell("$D110","Toe Model")+vcell("E$98","Toe Model"),vcell("$M27","Toe Model"),vcell("$N27","Toe Model"),vcell("$O27","Toe Model"),vcell("$P27","Toe Model"),vcell("$Q27","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I110')
    return None
xfunctions['Toe Model!I110']=xcf_Toe_Model_I110

#H110-I110/IF($B110+$I$98>=6,$H54,CHOOSE($B110+$I$98,$C54,$D54,$E54,$F54,$G54))
def xcf_Toe_Model_J110(): 
    try:      
        return vcell("H110","Toe Model")-vcell("I110","Toe Model")/IF(vcell("$B110","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J110')
    return None
xfunctions['Toe Model!J110']=xcf_Toe_Model_J110

#IF(J110>0,MIN(IF($F110+G$98>=6,$L54,CHOOSE($F110+G$98,$G54,$H54,$I54,$J54,$K54))*J110,IF($F110+G$98>=6,$T27,CHOOSE($F110+G$98,$O27,$P27,$Q27,$R27,$S27))-IF($F110+E$98>=6,$T27,CHOOSE($F110+E$98,$O27,$P27,$Q27,$R27,$S27))),0)
def xcf_Toe_Model_K110(): 
    try:      
        return IF(vcell("J110","Toe Model")>0,MIN(IF(vcell("$F110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L54","Toe Model"),CHOOSE(vcell("$F110","Toe Model")+vcell("G$98","Toe Model"),vcell("$G54","Toe Model"),vcell("$H54","Toe Model"),vcell("$I54","Toe Model"),vcell("$J54","Toe Model"),vcell("$K54","Toe Model")))*vcell("J110","Toe Model"),IF(vcell("$F110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T27","Toe Model"),CHOOSE(vcell("$F110","Toe Model")+vcell("G$98","Toe Model"),vcell("$O27","Toe Model"),vcell("$P27","Toe Model"),vcell("$Q27","Toe Model"),vcell("$R27","Toe Model"),vcell("$S27","Toe Model")))-IF(vcell("$F110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T27","Toe Model"),CHOOSE(vcell("$F110","Toe Model")+vcell("E$98","Toe Model"),vcell("$O27","Toe Model"),vcell("$P27","Toe Model"),vcell("$Q27","Toe Model"),vcell("$R27","Toe Model"),vcell("$S27","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K110')
    return None
xfunctions['Toe Model!K110']=xcf_Toe_Model_K110

#J110-K110/IF($B110+$K$98>=6,$H54,CHOOSE($B110+$K$98,$C54,$D54,$E54,$F54,$G54))
def xcf_Toe_Model_L110(): 
    try:      
        return vcell("J110","Toe Model")-vcell("K110","Toe Model")/IF(vcell("$B110","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L110')
    return None
xfunctions['Toe Model!L110']=xcf_Toe_Model_L110

#IF(L110>0,MIN(IF($H110+G$98>=6,$N54,CHOOSE($H110+G$98,$I54,$J54,$K54,$L54,$M54))*L110,IF($H110+G$98>=6,$V27,CHOOSE($H110+G$98,$Q27,$R27,$S27,$T27,$U27))-IF($H110+E$98>=6,$V27,CHOOSE($H110+E$98,$Q27,$R27,$S27,$T27,$U27))),0)
def xcf_Toe_Model_M110(): 
    try:      
        return IF(vcell("L110","Toe Model")>0,MIN(IF(vcell("$H110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N54","Toe Model"),CHOOSE(vcell("$H110","Toe Model")+vcell("G$98","Toe Model"),vcell("$I54","Toe Model"),vcell("$J54","Toe Model"),vcell("$K54","Toe Model"),vcell("$L54","Toe Model"),vcell("$M54","Toe Model")))*vcell("L110","Toe Model"),IF(vcell("$H110","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V27","Toe Model"),CHOOSE(vcell("$H110","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q27","Toe Model"),vcell("$R27","Toe Model"),vcell("$S27","Toe Model"),vcell("$T27","Toe Model"),vcell("$U27","Toe Model")))-IF(vcell("$H110","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V27","Toe Model"),CHOOSE(vcell("$H110","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q27","Toe Model"),vcell("$R27","Toe Model"),vcell("$S27","Toe Model"),vcell("$T27","Toe Model"),vcell("$U27","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M110')
    return None
xfunctions['Toe Model!M110']=xcf_Toe_Model_M110

#L110-M110/IF($B110+$M$98>=6,$H54,CHOOSE($B110+$M$98,$C54,$D54,$E54,$F54,$G54))
def xcf_Toe_Model_N110(): 
    try:      
        return vcell("L110","Toe Model")-vcell("M110","Toe Model")/IF(vcell("$B110","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H54","Toe Model"),CHOOSE(vcell("$B110","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C54","Toe Model"),vcell("$D54","Toe Model"),vcell("$E54","Toe Model"),vcell("$F54","Toe Model"),vcell("$G54","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N110')
    return None
xfunctions['Toe Model!N110']=xcf_Toe_Model_N110

#B81-SUM($C110,$E110,$G110,$I110,$K110,$M110)*SIN(RADIANS($D27))
def xcf_Toe_Model_O110(): 
    try:      
        return vcell("B81","Toe Model")-SUM(vcell("$C110","Toe Model"),vcell("$E110","Toe Model"),vcell("$G110","Toe Model"),vcell("$I110","Toe Model"),vcell("$K110","Toe Model"),vcell("$M110","Toe Model"))*SIN(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O110')
    return None
xfunctions['Toe Model!O110']=xcf_Toe_Model_O110

#C81+IF($D27==360,1,-1)*SUM($C110,$E110,$G110,$I110,$K110,$M110)*COS(RADIANS($D27))
def xcf_Toe_Model_P110(): 
    try:      
        return vcell("C81","Toe Model")+IF(vcell("$D27","Toe Model")==360,1,-1)*SUM(vcell("$C110","Toe Model"),vcell("$E110","Toe Model"),vcell("$G110","Toe Model"),vcell("$I110","Toe Model"),vcell("$K110","Toe Model"),vcell("$M110","Toe Model"))*COS(RADIANS(vcell("$D27","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P110')
    return None
xfunctions['Toe Model!P110']=xcf_Toe_Model_P110

#IF(MIN(C$18:C$40)>P110,IF(P109>=MIN(C$18:C$40),O109+(O110-O109)*(MIN(C$18:C$40)-P109)/(P110-P109),Q109+ABS(O110-O109)),O110)
def xcf_Toe_Model_Q110(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P110","Toe Model"),IF(vcell("P109","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O109","Toe Model")+(vcell("O110","Toe Model")-vcell("O109","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P109","Toe Model"))/(vcell("P110","Toe Model")-vcell("P109","Toe Model")),vcell("Q109","Toe Model")+ABS(vcell("O110","Toe Model")-vcell("O109","Toe Model"))),vcell("O110","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q110')
    return None
xfunctions['Toe Model!Q110']=xcf_Toe_Model_Q110

#MAX(C81+IF($D27==360,1,-1)*SUM($C110,$E110,$G110,$I110,$K110,$M110)*COS(RADIANS($D27)),MIN(C$18:C$40))
def xcf_Toe_Model_R110(): 
    try:      
        return MAX(vcell("C81","Toe Model")+IF(vcell("$D27","Toe Model")==360,1,-1)*SUM(vcell("$C110","Toe Model"),vcell("$E110","Toe Model"),vcell("$G110","Toe Model"),vcell("$I110","Toe Model"),vcell("$K110","Toe Model"),vcell("$M110","Toe Model"))*COS(RADIANS(vcell("$D27","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R110')
    return None
xfunctions['Toe Model!R110']=xcf_Toe_Model_R110

#IF(AND($C82<=$B$4,$C82>$B$5),1,IF(AND($C82<=$B$5,$C82>$B$6),2,IF(AND($C82<=$B$6,$C82>$B$7),3,IF(AND($C82<=$B$7,$C82>$B$8),4,IF(AND($C82<=$B$8,$C82>$B$9),5,6)))))
def xcf_Toe_Model_B111(): 
    try:      
        return IF(AND(vcell("$C82","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C82","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C82","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C82","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C82","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C82","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C82","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C82","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C82","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C82","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B111')
    return None
xfunctions['Toe Model!B111']=xcf_Toe_Model_B111

#IF(ISNUMBER($B111),MIN(CHOOSE($B111,$K28,$L28,$M28,$N28,$O28,$P28),CHOOSE($B111,$C55,$D55,$E55,$F55,$G55,$H55)*$D82),0)
def xcf_Toe_Model_C111(): 
    try:      
        return IF(ISNUMBER(vcell("$B111","Toe Model")),MIN(CHOOSE(vcell("$B111","Toe Model"),vcell("$K28","Toe Model"),vcell("$L28","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model"),vcell("$P28","Toe Model")),CHOOSE(vcell("$B111","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model"),vcell("$H55","Toe Model"))*vcell("$D82","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C111')
    return None
xfunctions['Toe Model!C111']=xcf_Toe_Model_C111

#$D82-$C111/CHOOSE($B111,$C55,$D55,$E55,$F55,$G55,$H55)
def xcf_Toe_Model_D111(): 
    try:      
        return vcell("$D82","Toe Model")-vcell("$C111","Toe Model")/CHOOSE(vcell("$B111","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model"),vcell("$H55","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D111')
    return None
xfunctions['Toe Model!D111']=xcf_Toe_Model_D111

#IF(D111>0,MIN(IF($B111+E$98>=6,$H55,CHOOSE($B111+E$98,$C55,$D55,$E55,$F55,$G55))*D111,IF($B111+E$98>=6,$P28,CHOOSE($B111+E$98,$K28,$L28,$M28,$N28,$O28))-IF($B111>=6,$P28,CHOOSE($B111,$K28,$L28,$M28,$N28,$O28))),0)
def xcf_Toe_Model_E111(): 
    try:      
        return IF(vcell("D111","Toe Model")>0,MIN(IF(vcell("$B111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("E$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))*vcell("D111","Toe Model"),IF(vcell("$B111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P28","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("E$98","Toe Model"),vcell("$K28","Toe Model"),vcell("$L28","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model")))-IF(vcell("$B111","Toe Model")>=6,vcell("$P28","Toe Model"),CHOOSE(vcell("$B111","Toe Model"),vcell("$K28","Toe Model"),vcell("$L28","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E111')
    return None
xfunctions['Toe Model!E111']=xcf_Toe_Model_E111

#D111-E111/IF($B111+$E$98>=6,$H55,CHOOSE($B111+$E$98,$C55,$D55,$E55,$F55,$G55))
def xcf_Toe_Model_F111(): 
    try:      
        return vcell("D111","Toe Model")-vcell("E111","Toe Model")/IF(vcell("$B111","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F111')
    return None
xfunctions['Toe Model!F111']=xcf_Toe_Model_F111

#IF(F111>0,MIN(IF($B111+G$98>=6,$H55,CHOOSE($B111+G$98,$C55,$D55,$E55,$F55,$G55))*F111,IF($B111+G$98>=6,$P28,CHOOSE($B111+G$98,$K28,$L28,$M28,$N28,$O28))-IF($B111+E$98>=6,$P28,CHOOSE($B111+E$98,$K28,$L28,$M28,$N28,$O28))),0)
def xcf_Toe_Model_G111(): 
    try:      
        return IF(vcell("F111","Toe Model")>0,MIN(IF(vcell("$B111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("G$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))*vcell("F111","Toe Model"),IF(vcell("$B111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P28","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("G$98","Toe Model"),vcell("$K28","Toe Model"),vcell("$L28","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model")))-IF(vcell("$B111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P28","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("E$98","Toe Model"),vcell("$K28","Toe Model"),vcell("$L28","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G111')
    return None
xfunctions['Toe Model!G111']=xcf_Toe_Model_G111

#F111-G111/IF($B111+$G$98>=6,$H55,CHOOSE($B111+$G$98,$C55,$D55,$E55,$F55,$G55))
def xcf_Toe_Model_H111(): 
    try:      
        return vcell("F111","Toe Model")-vcell("G111","Toe Model")/IF(vcell("$B111","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H111')
    return None
xfunctions['Toe Model!H111']=xcf_Toe_Model_H111

#IF(H111>0,MIN(IF($D111+G$98>=6,$J55,CHOOSE($D111+G$98,$E55,$F55,$G55,$H55,$I55))*H111,IF($D111+G$98>=6,$R28,CHOOSE($D111+G$98,$M28,$N28,$O28,$P28,$Q28))-IF($D111+E$98>=6,$R28,CHOOSE($D111+E$98,$M28,$N28,$O28,$P28,$Q28))),0)
def xcf_Toe_Model_I111(): 
    try:      
        return IF(vcell("H111","Toe Model")>0,MIN(IF(vcell("$D111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J55","Toe Model"),CHOOSE(vcell("$D111","Toe Model")+vcell("G$98","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model"),vcell("$H55","Toe Model"),vcell("$I55","Toe Model")))*vcell("H111","Toe Model"),IF(vcell("$D111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R28","Toe Model"),CHOOSE(vcell("$D111","Toe Model")+vcell("G$98","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model"),vcell("$P28","Toe Model"),vcell("$Q28","Toe Model")))-IF(vcell("$D111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R28","Toe Model"),CHOOSE(vcell("$D111","Toe Model")+vcell("E$98","Toe Model"),vcell("$M28","Toe Model"),vcell("$N28","Toe Model"),vcell("$O28","Toe Model"),vcell("$P28","Toe Model"),vcell("$Q28","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I111')
    return None
xfunctions['Toe Model!I111']=xcf_Toe_Model_I111

#H111-I111/IF($B111+$I$98>=6,$H55,CHOOSE($B111+$I$98,$C55,$D55,$E55,$F55,$G55))
def xcf_Toe_Model_J111(): 
    try:      
        return vcell("H111","Toe Model")-vcell("I111","Toe Model")/IF(vcell("$B111","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J111')
    return None
xfunctions['Toe Model!J111']=xcf_Toe_Model_J111

#IF(J111>0,MIN(IF($F111+G$98>=6,$L55,CHOOSE($F111+G$98,$G55,$H55,$I55,$J55,$K55))*J111,IF($F111+G$98>=6,$T28,CHOOSE($F111+G$98,$O28,$P28,$Q28,$R28,$S28))-IF($F111+E$98>=6,$T28,CHOOSE($F111+E$98,$O28,$P28,$Q28,$R28,$S28))),0)
def xcf_Toe_Model_K111(): 
    try:      
        return IF(vcell("J111","Toe Model")>0,MIN(IF(vcell("$F111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L55","Toe Model"),CHOOSE(vcell("$F111","Toe Model")+vcell("G$98","Toe Model"),vcell("$G55","Toe Model"),vcell("$H55","Toe Model"),vcell("$I55","Toe Model"),vcell("$J55","Toe Model"),vcell("$K55","Toe Model")))*vcell("J111","Toe Model"),IF(vcell("$F111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T28","Toe Model"),CHOOSE(vcell("$F111","Toe Model")+vcell("G$98","Toe Model"),vcell("$O28","Toe Model"),vcell("$P28","Toe Model"),vcell("$Q28","Toe Model"),vcell("$R28","Toe Model"),vcell("$S28","Toe Model")))-IF(vcell("$F111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T28","Toe Model"),CHOOSE(vcell("$F111","Toe Model")+vcell("E$98","Toe Model"),vcell("$O28","Toe Model"),vcell("$P28","Toe Model"),vcell("$Q28","Toe Model"),vcell("$R28","Toe Model"),vcell("$S28","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K111')
    return None
xfunctions['Toe Model!K111']=xcf_Toe_Model_K111

#J111-K111/IF($B111+$K$98>=6,$H55,CHOOSE($B111+$K$98,$C55,$D55,$E55,$F55,$G55))
def xcf_Toe_Model_L111(): 
    try:      
        return vcell("J111","Toe Model")-vcell("K111","Toe Model")/IF(vcell("$B111","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L111')
    return None
xfunctions['Toe Model!L111']=xcf_Toe_Model_L111

#IF(L111>0,MIN(IF($H111+G$98>=6,$N55,CHOOSE($H111+G$98,$I55,$J55,$K55,$L55,$M55))*L111,IF($H111+G$98>=6,$V28,CHOOSE($H111+G$98,$Q28,$R28,$S28,$T28,$U28))-IF($H111+E$98>=6,$V28,CHOOSE($H111+E$98,$Q28,$R28,$S28,$T28,$U28))),0)
def xcf_Toe_Model_M111(): 
    try:      
        return IF(vcell("L111","Toe Model")>0,MIN(IF(vcell("$H111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N55","Toe Model"),CHOOSE(vcell("$H111","Toe Model")+vcell("G$98","Toe Model"),vcell("$I55","Toe Model"),vcell("$J55","Toe Model"),vcell("$K55","Toe Model"),vcell("$L55","Toe Model"),vcell("$M55","Toe Model")))*vcell("L111","Toe Model"),IF(vcell("$H111","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V28","Toe Model"),CHOOSE(vcell("$H111","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q28","Toe Model"),vcell("$R28","Toe Model"),vcell("$S28","Toe Model"),vcell("$T28","Toe Model"),vcell("$U28","Toe Model")))-IF(vcell("$H111","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V28","Toe Model"),CHOOSE(vcell("$H111","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q28","Toe Model"),vcell("$R28","Toe Model"),vcell("$S28","Toe Model"),vcell("$T28","Toe Model"),vcell("$U28","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M111')
    return None
xfunctions['Toe Model!M111']=xcf_Toe_Model_M111

#L111-M111/IF($B111+$M$98>=6,$H55,CHOOSE($B111+$M$98,$C55,$D55,$E55,$F55,$G55))
def xcf_Toe_Model_N111(): 
    try:      
        return vcell("L111","Toe Model")-vcell("M111","Toe Model")/IF(vcell("$B111","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H55","Toe Model"),CHOOSE(vcell("$B111","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C55","Toe Model"),vcell("$D55","Toe Model"),vcell("$E55","Toe Model"),vcell("$F55","Toe Model"),vcell("$G55","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N111')
    return None
xfunctions['Toe Model!N111']=xcf_Toe_Model_N111

#B82-SUM($C111,$E111,$G111,$I111,$K111,$M111)*SIN(RADIANS($D28))
def xcf_Toe_Model_O111(): 
    try:      
        return vcell("B82","Toe Model")-SUM(vcell("$C111","Toe Model"),vcell("$E111","Toe Model"),vcell("$G111","Toe Model"),vcell("$I111","Toe Model"),vcell("$K111","Toe Model"),vcell("$M111","Toe Model"))*SIN(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O111')
    return None
xfunctions['Toe Model!O111']=xcf_Toe_Model_O111

#C82+IF($D28==360,1,-1)*SUM($C111,$E111,$G111,$I111,$K111,$M111)*COS(RADIANS($D28))
def xcf_Toe_Model_P111(): 
    try:      
        return vcell("C82","Toe Model")+IF(vcell("$D28","Toe Model")==360,1,-1)*SUM(vcell("$C111","Toe Model"),vcell("$E111","Toe Model"),vcell("$G111","Toe Model"),vcell("$I111","Toe Model"),vcell("$K111","Toe Model"),vcell("$M111","Toe Model"))*COS(RADIANS(vcell("$D28","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P111')
    return None
xfunctions['Toe Model!P111']=xcf_Toe_Model_P111

#IF(MIN(C$18:C$40)>P111,IF(P110>=MIN(C$18:C$40),O110+(O111-O110)*(MIN(C$18:C$40)-P110)/(P111-P110),Q110+ABS(O111-O110)),O111)
def xcf_Toe_Model_Q111(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P111","Toe Model"),IF(vcell("P110","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O110","Toe Model")+(vcell("O111","Toe Model")-vcell("O110","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P110","Toe Model"))/(vcell("P111","Toe Model")-vcell("P110","Toe Model")),vcell("Q110","Toe Model")+ABS(vcell("O111","Toe Model")-vcell("O110","Toe Model"))),vcell("O111","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q111')
    return None
xfunctions['Toe Model!Q111']=xcf_Toe_Model_Q111

#MAX(C82+IF($D28==360,1,-1)*SUM($C111,$E111,$G111,$I111,$K111,$M111)*COS(RADIANS($D28)),MIN(C$18:C$40))
def xcf_Toe_Model_R111(): 
    try:      
        return MAX(vcell("C82","Toe Model")+IF(vcell("$D28","Toe Model")==360,1,-1)*SUM(vcell("$C111","Toe Model"),vcell("$E111","Toe Model"),vcell("$G111","Toe Model"),vcell("$I111","Toe Model"),vcell("$K111","Toe Model"),vcell("$M111","Toe Model"))*COS(RADIANS(vcell("$D28","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R111')
    return None
xfunctions['Toe Model!R111']=xcf_Toe_Model_R111

#IF(AND($C83<=$B$4,$C83>$B$5),1,IF(AND($C83<=$B$5,$C83>$B$6),2,IF(AND($C83<=$B$6,$C83>$B$7),3,IF(AND($C83<=$B$7,$C83>$B$8),4,IF(AND($C83<=$B$8,$C83>$B$9),5,6)))))
def xcf_Toe_Model_B112(): 
    try:      
        return IF(AND(vcell("$C83","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C83","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C83","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C83","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C83","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C83","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C83","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C83","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C83","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C83","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B112')
    return None
xfunctions['Toe Model!B112']=xcf_Toe_Model_B112

#IF(ISNUMBER($B112),MIN(CHOOSE($B112,$K29,$L29,$M29,$N29,$O29,$P29),CHOOSE($B112,$C56,$D56,$E56,$F56,$G56,$H56)*$D83),0)
def xcf_Toe_Model_C112(): 
    try:      
        return IF(ISNUMBER(vcell("$B112","Toe Model")),MIN(CHOOSE(vcell("$B112","Toe Model"),vcell("$K29","Toe Model"),vcell("$L29","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model"),vcell("$P29","Toe Model")),CHOOSE(vcell("$B112","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model"),vcell("$H56","Toe Model"))*vcell("$D83","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C112')
    return None
xfunctions['Toe Model!C112']=xcf_Toe_Model_C112

#$D83-$C112/CHOOSE($B112,$C56,$D56,$E56,$F56,$G56,$H56)
def xcf_Toe_Model_D112(): 
    try:      
        return vcell("$D83","Toe Model")-vcell("$C112","Toe Model")/CHOOSE(vcell("$B112","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model"),vcell("$H56","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D112')
    return None
xfunctions['Toe Model!D112']=xcf_Toe_Model_D112

#IF(D112>0,MIN(IF($B112+E$98>=6,$H56,CHOOSE($B112+E$98,$C56,$D56,$E56,$F56,$G56))*D112,IF($B112+E$98>=6,$P29,CHOOSE($B112+E$98,$K29,$L29,$M29,$N29,$O29))-IF($B112>=6,$P29,CHOOSE($B112,$K29,$L29,$M29,$N29,$O29))),0)
def xcf_Toe_Model_E112(): 
    try:      
        return IF(vcell("D112","Toe Model")>0,MIN(IF(vcell("$B112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("E$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))*vcell("D112","Toe Model"),IF(vcell("$B112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P29","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("E$98","Toe Model"),vcell("$K29","Toe Model"),vcell("$L29","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model")))-IF(vcell("$B112","Toe Model")>=6,vcell("$P29","Toe Model"),CHOOSE(vcell("$B112","Toe Model"),vcell("$K29","Toe Model"),vcell("$L29","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E112')
    return None
xfunctions['Toe Model!E112']=xcf_Toe_Model_E112

#D112-E112/IF($B112+$E$98>=6,$H56,CHOOSE($B112+$E$98,$C56,$D56,$E56,$F56,$G56))
def xcf_Toe_Model_F112(): 
    try:      
        return vcell("D112","Toe Model")-vcell("E112","Toe Model")/IF(vcell("$B112","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F112')
    return None
xfunctions['Toe Model!F112']=xcf_Toe_Model_F112

#IF(F112>0,MIN(IF($B112+G$98>=6,$H56,CHOOSE($B112+G$98,$C56,$D56,$E56,$F56,$G56))*F112,IF($B112+G$98>=6,$P29,CHOOSE($B112+G$98,$K29,$L29,$M29,$N29,$O29))-IF($B112+E$98>=6,$P29,CHOOSE($B112+E$98,$K29,$L29,$M29,$N29,$O29))),0)
def xcf_Toe_Model_G112(): 
    try:      
        return IF(vcell("F112","Toe Model")>0,MIN(IF(vcell("$B112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("G$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))*vcell("F112","Toe Model"),IF(vcell("$B112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P29","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("G$98","Toe Model"),vcell("$K29","Toe Model"),vcell("$L29","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model")))-IF(vcell("$B112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P29","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("E$98","Toe Model"),vcell("$K29","Toe Model"),vcell("$L29","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G112')
    return None
xfunctions['Toe Model!G112']=xcf_Toe_Model_G112

#F112-G112/IF($B112+$G$98>=6,$H56,CHOOSE($B112+$G$98,$C56,$D56,$E56,$F56,$G56))
def xcf_Toe_Model_H112(): 
    try:      
        return vcell("F112","Toe Model")-vcell("G112","Toe Model")/IF(vcell("$B112","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H112')
    return None
xfunctions['Toe Model!H112']=xcf_Toe_Model_H112

#IF(H112>0,MIN(IF($D112+G$98>=6,$J56,CHOOSE($D112+G$98,$E56,$F56,$G56,$H56,$I56))*H112,IF($D112+G$98>=6,$R29,CHOOSE($D112+G$98,$M29,$N29,$O29,$P29,$Q29))-IF($D112+E$98>=6,$R29,CHOOSE($D112+E$98,$M29,$N29,$O29,$P29,$Q29))),0)
def xcf_Toe_Model_I112(): 
    try:      
        return IF(vcell("H112","Toe Model")>0,MIN(IF(vcell("$D112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J56","Toe Model"),CHOOSE(vcell("$D112","Toe Model")+vcell("G$98","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model"),vcell("$H56","Toe Model"),vcell("$I56","Toe Model")))*vcell("H112","Toe Model"),IF(vcell("$D112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R29","Toe Model"),CHOOSE(vcell("$D112","Toe Model")+vcell("G$98","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model"),vcell("$P29","Toe Model"),vcell("$Q29","Toe Model")))-IF(vcell("$D112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R29","Toe Model"),CHOOSE(vcell("$D112","Toe Model")+vcell("E$98","Toe Model"),vcell("$M29","Toe Model"),vcell("$N29","Toe Model"),vcell("$O29","Toe Model"),vcell("$P29","Toe Model"),vcell("$Q29","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I112')
    return None
xfunctions['Toe Model!I112']=xcf_Toe_Model_I112

#H112-I112/IF($B112+$I$98>=6,$H56,CHOOSE($B112+$I$98,$C56,$D56,$E56,$F56,$G56))
def xcf_Toe_Model_J112(): 
    try:      
        return vcell("H112","Toe Model")-vcell("I112","Toe Model")/IF(vcell("$B112","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J112')
    return None
xfunctions['Toe Model!J112']=xcf_Toe_Model_J112

#IF(J112>0,MIN(IF($F112+G$98>=6,$L56,CHOOSE($F112+G$98,$G56,$H56,$I56,$J56,$K56))*J112,IF($F112+G$98>=6,$T29,CHOOSE($F112+G$98,$O29,$P29,$Q29,$R29,$S29))-IF($F112+E$98>=6,$T29,CHOOSE($F112+E$98,$O29,$P29,$Q29,$R29,$S29))),0)
def xcf_Toe_Model_K112(): 
    try:      
        return IF(vcell("J112","Toe Model")>0,MIN(IF(vcell("$F112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L56","Toe Model"),CHOOSE(vcell("$F112","Toe Model")+vcell("G$98","Toe Model"),vcell("$G56","Toe Model"),vcell("$H56","Toe Model"),vcell("$I56","Toe Model"),vcell("$J56","Toe Model"),vcell("$K56","Toe Model")))*vcell("J112","Toe Model"),IF(vcell("$F112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T29","Toe Model"),CHOOSE(vcell("$F112","Toe Model")+vcell("G$98","Toe Model"),vcell("$O29","Toe Model"),vcell("$P29","Toe Model"),vcell("$Q29","Toe Model"),vcell("$R29","Toe Model"),vcell("$S29","Toe Model")))-IF(vcell("$F112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T29","Toe Model"),CHOOSE(vcell("$F112","Toe Model")+vcell("E$98","Toe Model"),vcell("$O29","Toe Model"),vcell("$P29","Toe Model"),vcell("$Q29","Toe Model"),vcell("$R29","Toe Model"),vcell("$S29","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K112')
    return None
xfunctions['Toe Model!K112']=xcf_Toe_Model_K112

#J112-K112/IF($B112+$K$98>=6,$H56,CHOOSE($B112+$K$98,$C56,$D56,$E56,$F56,$G56))
def xcf_Toe_Model_L112(): 
    try:      
        return vcell("J112","Toe Model")-vcell("K112","Toe Model")/IF(vcell("$B112","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L112')
    return None
xfunctions['Toe Model!L112']=xcf_Toe_Model_L112

#IF(L112>0,MIN(IF($H112+G$98>=6,$N56,CHOOSE($H112+G$98,$I56,$J56,$K56,$L56,$M56))*L112,IF($H112+G$98>=6,$V29,CHOOSE($H112+G$98,$Q29,$R29,$S29,$T29,$U29))-IF($H112+E$98>=6,$V29,CHOOSE($H112+E$98,$Q29,$R29,$S29,$T29,$U29))),0)
def xcf_Toe_Model_M112(): 
    try:      
        return IF(vcell("L112","Toe Model")>0,MIN(IF(vcell("$H112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N56","Toe Model"),CHOOSE(vcell("$H112","Toe Model")+vcell("G$98","Toe Model"),vcell("$I56","Toe Model"),vcell("$J56","Toe Model"),vcell("$K56","Toe Model"),vcell("$L56","Toe Model"),vcell("$M56","Toe Model")))*vcell("L112","Toe Model"),IF(vcell("$H112","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V29","Toe Model"),CHOOSE(vcell("$H112","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q29","Toe Model"),vcell("$R29","Toe Model"),vcell("$S29","Toe Model"),vcell("$T29","Toe Model"),vcell("$U29","Toe Model")))-IF(vcell("$H112","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V29","Toe Model"),CHOOSE(vcell("$H112","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q29","Toe Model"),vcell("$R29","Toe Model"),vcell("$S29","Toe Model"),vcell("$T29","Toe Model"),vcell("$U29","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M112')
    return None
xfunctions['Toe Model!M112']=xcf_Toe_Model_M112

#L112-M112/IF($B112+$M$98>=6,$H56,CHOOSE($B112+$M$98,$C56,$D56,$E56,$F56,$G56))
def xcf_Toe_Model_N112(): 
    try:      
        return vcell("L112","Toe Model")-vcell("M112","Toe Model")/IF(vcell("$B112","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H56","Toe Model"),CHOOSE(vcell("$B112","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C56","Toe Model"),vcell("$D56","Toe Model"),vcell("$E56","Toe Model"),vcell("$F56","Toe Model"),vcell("$G56","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N112')
    return None
xfunctions['Toe Model!N112']=xcf_Toe_Model_N112

#B83-SUM($C112,$E112,$G112,$I112,$K112,$M112)*SIN(RADIANS($D29))
def xcf_Toe_Model_O112(): 
    try:      
        return vcell("B83","Toe Model")-SUM(vcell("$C112","Toe Model"),vcell("$E112","Toe Model"),vcell("$G112","Toe Model"),vcell("$I112","Toe Model"),vcell("$K112","Toe Model"),vcell("$M112","Toe Model"))*SIN(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O112')
    return None
xfunctions['Toe Model!O112']=xcf_Toe_Model_O112

#C83+IF($D29==360,1,-1)*SUM($C112,$E112,$G112,$I112,$K112,$M112)*COS(RADIANS($D29))
def xcf_Toe_Model_P112(): 
    try:      
        return vcell("C83","Toe Model")+IF(vcell("$D29","Toe Model")==360,1,-1)*SUM(vcell("$C112","Toe Model"),vcell("$E112","Toe Model"),vcell("$G112","Toe Model"),vcell("$I112","Toe Model"),vcell("$K112","Toe Model"),vcell("$M112","Toe Model"))*COS(RADIANS(vcell("$D29","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P112')
    return None
xfunctions['Toe Model!P112']=xcf_Toe_Model_P112

#IF(MIN(C$18:C$40)>P112,IF(P111>=MIN(C$18:C$40),O111+(O112-O111)*(MIN(C$18:C$40)-P111)/(P112-P111),Q111+ABS(O112-O111)),O112)
def xcf_Toe_Model_Q112(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P112","Toe Model"),IF(vcell("P111","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O111","Toe Model")+(vcell("O112","Toe Model")-vcell("O111","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P111","Toe Model"))/(vcell("P112","Toe Model")-vcell("P111","Toe Model")),vcell("Q111","Toe Model")+ABS(vcell("O112","Toe Model")-vcell("O111","Toe Model"))),vcell("O112","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q112')
    return None
xfunctions['Toe Model!Q112']=xcf_Toe_Model_Q112

#MAX(C83+IF($D29==360,1,-1)*SUM($C112,$E112,$G112,$I112,$K112,$M112)*COS(RADIANS($D29)),MIN(C$18:C$40))
def xcf_Toe_Model_R112(): 
    try:      
        return MAX(vcell("C83","Toe Model")+IF(vcell("$D29","Toe Model")==360,1,-1)*SUM(vcell("$C112","Toe Model"),vcell("$E112","Toe Model"),vcell("$G112","Toe Model"),vcell("$I112","Toe Model"),vcell("$K112","Toe Model"),vcell("$M112","Toe Model"))*COS(RADIANS(vcell("$D29","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R112')
    return None
xfunctions['Toe Model!R112']=xcf_Toe_Model_R112

#IF(AND($C84<=$B$4,$C84>$B$5),1,IF(AND($C84<=$B$5,$C84>$B$6),2,IF(AND($C84<=$B$6,$C84>$B$7),3,IF(AND($C84<=$B$7,$C84>$B$8),4,IF(AND($C84<=$B$8,$C84>$B$9),5,6)))))
def xcf_Toe_Model_B113(): 
    try:      
        return IF(AND(vcell("$C84","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C84","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C84","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C84","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C84","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C84","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C84","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C84","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C84","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C84","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B113')
    return None
xfunctions['Toe Model!B113']=xcf_Toe_Model_B113

#IF(ISNUMBER($B113),MIN(CHOOSE($B113,$K30,$L30,$M30,$N30,$O30,$P30),CHOOSE($B113,$C57,$D57,$E57,$F57,$G57,$H57)*$D84),0)
def xcf_Toe_Model_C113(): 
    try:      
        return IF(ISNUMBER(vcell("$B113","Toe Model")),MIN(CHOOSE(vcell("$B113","Toe Model"),vcell("$K30","Toe Model"),vcell("$L30","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model"),vcell("$P30","Toe Model")),CHOOSE(vcell("$B113","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model"),vcell("$H57","Toe Model"))*vcell("$D84","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C113')
    return None
xfunctions['Toe Model!C113']=xcf_Toe_Model_C113

#$D84-$C113/CHOOSE($B113,$C57,$D57,$E57,$F57,$G57,$H57)
def xcf_Toe_Model_D113(): 
    try:      
        return vcell("$D84","Toe Model")-vcell("$C113","Toe Model")/CHOOSE(vcell("$B113","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model"),vcell("$H57","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D113')
    return None
xfunctions['Toe Model!D113']=xcf_Toe_Model_D113

#IF(D113>0,MIN(IF($B113+E$98>=6,$H57,CHOOSE($B113+E$98,$C57,$D57,$E57,$F57,$G57))*D113,IF($B113+E$98>=6,$P30,CHOOSE($B113+E$98,$K30,$L30,$M30,$N30,$O30))-IF($B113>=6,$P30,CHOOSE($B113,$K30,$L30,$M30,$N30,$O30))),0)
def xcf_Toe_Model_E113(): 
    try:      
        return IF(vcell("D113","Toe Model")>0,MIN(IF(vcell("$B113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("E$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))*vcell("D113","Toe Model"),IF(vcell("$B113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P30","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("E$98","Toe Model"),vcell("$K30","Toe Model"),vcell("$L30","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model")))-IF(vcell("$B113","Toe Model")>=6,vcell("$P30","Toe Model"),CHOOSE(vcell("$B113","Toe Model"),vcell("$K30","Toe Model"),vcell("$L30","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E113')
    return None
xfunctions['Toe Model!E113']=xcf_Toe_Model_E113

#D113-E113/IF($B113+$E$98>=6,$H57,CHOOSE($B113+$E$98,$C57,$D57,$E57,$F57,$G57))
def xcf_Toe_Model_F113(): 
    try:      
        return vcell("D113","Toe Model")-vcell("E113","Toe Model")/IF(vcell("$B113","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F113')
    return None
xfunctions['Toe Model!F113']=xcf_Toe_Model_F113

#IF(F113>0,MIN(IF($B113+G$98>=6,$H57,CHOOSE($B113+G$98,$C57,$D57,$E57,$F57,$G57))*F113,IF($B113+G$98>=6,$P30,CHOOSE($B113+G$98,$K30,$L30,$M30,$N30,$O30))-IF($B113+E$98>=6,$P30,CHOOSE($B113+E$98,$K30,$L30,$M30,$N30,$O30))),0)
def xcf_Toe_Model_G113(): 
    try:      
        return IF(vcell("F113","Toe Model")>0,MIN(IF(vcell("$B113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("G$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))*vcell("F113","Toe Model"),IF(vcell("$B113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P30","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("G$98","Toe Model"),vcell("$K30","Toe Model"),vcell("$L30","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model")))-IF(vcell("$B113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P30","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("E$98","Toe Model"),vcell("$K30","Toe Model"),vcell("$L30","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G113')
    return None
xfunctions['Toe Model!G113']=xcf_Toe_Model_G113

#F113-G113/IF($B113+$G$98>=6,$H57,CHOOSE($B113+$G$98,$C57,$D57,$E57,$F57,$G57))
def xcf_Toe_Model_H113(): 
    try:      
        return vcell("F113","Toe Model")-vcell("G113","Toe Model")/IF(vcell("$B113","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H113')
    return None
xfunctions['Toe Model!H113']=xcf_Toe_Model_H113

#IF(H113>0,MIN(IF($D113+G$98>=6,$J57,CHOOSE($D113+G$98,$E57,$F57,$G57,$H57,$I57))*H113,IF($D113+G$98>=6,$R30,CHOOSE($D113+G$98,$M30,$N30,$O30,$P30,$Q30))-IF($D113+E$98>=6,$R30,CHOOSE($D113+E$98,$M30,$N30,$O30,$P30,$Q30))),0)
def xcf_Toe_Model_I113(): 
    try:      
        return IF(vcell("H113","Toe Model")>0,MIN(IF(vcell("$D113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J57","Toe Model"),CHOOSE(vcell("$D113","Toe Model")+vcell("G$98","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model"),vcell("$H57","Toe Model"),vcell("$I57","Toe Model")))*vcell("H113","Toe Model"),IF(vcell("$D113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R30","Toe Model"),CHOOSE(vcell("$D113","Toe Model")+vcell("G$98","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model"),vcell("$P30","Toe Model"),vcell("$Q30","Toe Model")))-IF(vcell("$D113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R30","Toe Model"),CHOOSE(vcell("$D113","Toe Model")+vcell("E$98","Toe Model"),vcell("$M30","Toe Model"),vcell("$N30","Toe Model"),vcell("$O30","Toe Model"),vcell("$P30","Toe Model"),vcell("$Q30","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I113')
    return None
xfunctions['Toe Model!I113']=xcf_Toe_Model_I113

#H113-I113/IF($B113+$I$98>=6,$H57,CHOOSE($B113+$I$98,$C57,$D57,$E57,$F57,$G57))
def xcf_Toe_Model_J113(): 
    try:      
        return vcell("H113","Toe Model")-vcell("I113","Toe Model")/IF(vcell("$B113","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J113')
    return None
xfunctions['Toe Model!J113']=xcf_Toe_Model_J113

#IF(J113>0,MIN(IF($F113+G$98>=6,$L57,CHOOSE($F113+G$98,$G57,$H57,$I57,$J57,$K57))*J113,IF($F113+G$98>=6,$T30,CHOOSE($F113+G$98,$O30,$P30,$Q30,$R30,$S30))-IF($F113+E$98>=6,$T30,CHOOSE($F113+E$98,$O30,$P30,$Q30,$R30,$S30))),0)
def xcf_Toe_Model_K113(): 
    try:      
        return IF(vcell("J113","Toe Model")>0,MIN(IF(vcell("$F113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L57","Toe Model"),CHOOSE(vcell("$F113","Toe Model")+vcell("G$98","Toe Model"),vcell("$G57","Toe Model"),vcell("$H57","Toe Model"),vcell("$I57","Toe Model"),vcell("$J57","Toe Model"),vcell("$K57","Toe Model")))*vcell("J113","Toe Model"),IF(vcell("$F113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T30","Toe Model"),CHOOSE(vcell("$F113","Toe Model")+vcell("G$98","Toe Model"),vcell("$O30","Toe Model"),vcell("$P30","Toe Model"),vcell("$Q30","Toe Model"),vcell("$R30","Toe Model"),vcell("$S30","Toe Model")))-IF(vcell("$F113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T30","Toe Model"),CHOOSE(vcell("$F113","Toe Model")+vcell("E$98","Toe Model"),vcell("$O30","Toe Model"),vcell("$P30","Toe Model"),vcell("$Q30","Toe Model"),vcell("$R30","Toe Model"),vcell("$S30","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K113')
    return None
xfunctions['Toe Model!K113']=xcf_Toe_Model_K113

#J113-K113/IF($B113+$K$98>=6,$H57,CHOOSE($B113+$K$98,$C57,$D57,$E57,$F57,$G57))
def xcf_Toe_Model_L113(): 
    try:      
        return vcell("J113","Toe Model")-vcell("K113","Toe Model")/IF(vcell("$B113","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L113')
    return None
xfunctions['Toe Model!L113']=xcf_Toe_Model_L113

#IF(L113>0,MIN(IF($H113+G$98>=6,$N57,CHOOSE($H113+G$98,$I57,$J57,$K57,$L57,$M57))*L113,IF($H113+G$98>=6,$V30,CHOOSE($H113+G$98,$Q30,$R30,$S30,$T30,$U30))-IF($H113+E$98>=6,$V30,CHOOSE($H113+E$98,$Q30,$R30,$S30,$T30,$U30))),0)
def xcf_Toe_Model_M113(): 
    try:      
        return IF(vcell("L113","Toe Model")>0,MIN(IF(vcell("$H113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N57","Toe Model"),CHOOSE(vcell("$H113","Toe Model")+vcell("G$98","Toe Model"),vcell("$I57","Toe Model"),vcell("$J57","Toe Model"),vcell("$K57","Toe Model"),vcell("$L57","Toe Model"),vcell("$M57","Toe Model")))*vcell("L113","Toe Model"),IF(vcell("$H113","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V30","Toe Model"),CHOOSE(vcell("$H113","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q30","Toe Model"),vcell("$R30","Toe Model"),vcell("$S30","Toe Model"),vcell("$T30","Toe Model"),vcell("$U30","Toe Model")))-IF(vcell("$H113","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V30","Toe Model"),CHOOSE(vcell("$H113","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q30","Toe Model"),vcell("$R30","Toe Model"),vcell("$S30","Toe Model"),vcell("$T30","Toe Model"),vcell("$U30","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M113')
    return None
xfunctions['Toe Model!M113']=xcf_Toe_Model_M113

#L113-M113/IF($B113+$M$98>=6,$H57,CHOOSE($B113+$M$98,$C57,$D57,$E57,$F57,$G57))
def xcf_Toe_Model_N113(): 
    try:      
        return vcell("L113","Toe Model")-vcell("M113","Toe Model")/IF(vcell("$B113","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H57","Toe Model"),CHOOSE(vcell("$B113","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C57","Toe Model"),vcell("$D57","Toe Model"),vcell("$E57","Toe Model"),vcell("$F57","Toe Model"),vcell("$G57","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N113')
    return None
xfunctions['Toe Model!N113']=xcf_Toe_Model_N113

#B84-SUM($C113,$E113,$G113,$I113,$K113,$M113)*SIN(RADIANS($D30))
def xcf_Toe_Model_O113(): 
    try:      
        return vcell("B84","Toe Model")-SUM(vcell("$C113","Toe Model"),vcell("$E113","Toe Model"),vcell("$G113","Toe Model"),vcell("$I113","Toe Model"),vcell("$K113","Toe Model"),vcell("$M113","Toe Model"))*SIN(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O113')
    return None
xfunctions['Toe Model!O113']=xcf_Toe_Model_O113

#C84+IF($D30==360,1,-1)*SUM($C113,$E113,$G113,$I113,$K113,$M113)*COS(RADIANS($D30))
def xcf_Toe_Model_P113(): 
    try:      
        return vcell("C84","Toe Model")+IF(vcell("$D30","Toe Model")==360,1,-1)*SUM(vcell("$C113","Toe Model"),vcell("$E113","Toe Model"),vcell("$G113","Toe Model"),vcell("$I113","Toe Model"),vcell("$K113","Toe Model"),vcell("$M113","Toe Model"))*COS(RADIANS(vcell("$D30","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P113')
    return None
xfunctions['Toe Model!P113']=xcf_Toe_Model_P113

#IF(MIN(C$18:C$40)>P113,IF(P112>=MIN(C$18:C$40),O112+(O113-O112)*(MIN(C$18:C$40)-P112)/(P113-P112),Q112+ABS(O113-O112)),O113)
def xcf_Toe_Model_Q113(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P113","Toe Model"),IF(vcell("P112","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O112","Toe Model")+(vcell("O113","Toe Model")-vcell("O112","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P112","Toe Model"))/(vcell("P113","Toe Model")-vcell("P112","Toe Model")),vcell("Q112","Toe Model")+ABS(vcell("O113","Toe Model")-vcell("O112","Toe Model"))),vcell("O113","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q113')
    return None
xfunctions['Toe Model!Q113']=xcf_Toe_Model_Q113

#MAX(C84+IF($D30==360,1,-1)*SUM($C113,$E113,$G113,$I113,$K113,$M113)*COS(RADIANS($D30)),MIN(C$18:C$40))
def xcf_Toe_Model_R113(): 
    try:      
        return MAX(vcell("C84","Toe Model")+IF(vcell("$D30","Toe Model")==360,1,-1)*SUM(vcell("$C113","Toe Model"),vcell("$E113","Toe Model"),vcell("$G113","Toe Model"),vcell("$I113","Toe Model"),vcell("$K113","Toe Model"),vcell("$M113","Toe Model"))*COS(RADIANS(vcell("$D30","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R113')
    return None
xfunctions['Toe Model!R113']=xcf_Toe_Model_R113

#IF(AND($C85<=$B$4,$C85>$B$5),1,IF(AND($C85<=$B$5,$C85>$B$6),2,IF(AND($C85<=$B$6,$C85>$B$7),3,IF(AND($C85<=$B$7,$C85>$B$8),4,IF(AND($C85<=$B$8,$C85>$B$9),5,6)))))
def xcf_Toe_Model_B114(): 
    try:      
        return IF(AND(vcell("$C85","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C85","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C85","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C85","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C85","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C85","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C85","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C85","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C85","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C85","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B114')
    return None
xfunctions['Toe Model!B114']=xcf_Toe_Model_B114

#IF(ISNUMBER($B114),MIN(CHOOSE($B114,$K31,$L31,$M31,$N31,$O31,$P31),CHOOSE($B114,$C58,$D58,$E58,$F58,$G58,$H58)*$D85),0)
def xcf_Toe_Model_C114(): 
    try:      
        return IF(ISNUMBER(vcell("$B114","Toe Model")),MIN(CHOOSE(vcell("$B114","Toe Model"),vcell("$K31","Toe Model"),vcell("$L31","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model"),vcell("$P31","Toe Model")),CHOOSE(vcell("$B114","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model"),vcell("$H58","Toe Model"))*vcell("$D85","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C114')
    return None
xfunctions['Toe Model!C114']=xcf_Toe_Model_C114

#$D85-$C114/CHOOSE($B114,$C58,$D58,$E58,$F58,$G58,$H58)
def xcf_Toe_Model_D114(): 
    try:      
        return vcell("$D85","Toe Model")-vcell("$C114","Toe Model")/CHOOSE(vcell("$B114","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model"),vcell("$H58","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D114')
    return None
xfunctions['Toe Model!D114']=xcf_Toe_Model_D114

#IF(D114>0,MIN(IF($B114+E$98>=6,$H58,CHOOSE($B114+E$98,$C58,$D58,$E58,$F58,$G58))*D114,IF($B114+E$98>=6,$P31,CHOOSE($B114+E$98,$K31,$L31,$M31,$N31,$O31))-IF($B114>=6,$P31,CHOOSE($B114,$K31,$L31,$M31,$N31,$O31))),0)
def xcf_Toe_Model_E114(): 
    try:      
        return IF(vcell("D114","Toe Model")>0,MIN(IF(vcell("$B114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("E$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))*vcell("D114","Toe Model"),IF(vcell("$B114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P31","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("E$98","Toe Model"),vcell("$K31","Toe Model"),vcell("$L31","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model")))-IF(vcell("$B114","Toe Model")>=6,vcell("$P31","Toe Model"),CHOOSE(vcell("$B114","Toe Model"),vcell("$K31","Toe Model"),vcell("$L31","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E114')
    return None
xfunctions['Toe Model!E114']=xcf_Toe_Model_E114

#D114-E114/IF($B114+$E$98>=6,$H58,CHOOSE($B114+$E$98,$C58,$D58,$E58,$F58,$G58))
def xcf_Toe_Model_F114(): 
    try:      
        return vcell("D114","Toe Model")-vcell("E114","Toe Model")/IF(vcell("$B114","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F114')
    return None
xfunctions['Toe Model!F114']=xcf_Toe_Model_F114

#IF(F114>0,MIN(IF($B114+G$98>=6,$H58,CHOOSE($B114+G$98,$C58,$D58,$E58,$F58,$G58))*F114,IF($B114+G$98>=6,$P31,CHOOSE($B114+G$98,$K31,$L31,$M31,$N31,$O31))-IF($B114+E$98>=6,$P31,CHOOSE($B114+E$98,$K31,$L31,$M31,$N31,$O31))),0)
def xcf_Toe_Model_G114(): 
    try:      
        return IF(vcell("F114","Toe Model")>0,MIN(IF(vcell("$B114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("G$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))*vcell("F114","Toe Model"),IF(vcell("$B114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P31","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("G$98","Toe Model"),vcell("$K31","Toe Model"),vcell("$L31","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model")))-IF(vcell("$B114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P31","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("E$98","Toe Model"),vcell("$K31","Toe Model"),vcell("$L31","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G114')
    return None
xfunctions['Toe Model!G114']=xcf_Toe_Model_G114

#F114-G114/IF($B114+$G$98>=6,$H58,CHOOSE($B114+$G$98,$C58,$D58,$E58,$F58,$G58))
def xcf_Toe_Model_H114(): 
    try:      
        return vcell("F114","Toe Model")-vcell("G114","Toe Model")/IF(vcell("$B114","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H114')
    return None
xfunctions['Toe Model!H114']=xcf_Toe_Model_H114

#IF(H114>0,MIN(IF($D114+G$98>=6,$J58,CHOOSE($D114+G$98,$E58,$F58,$G58,$H58,$I58))*H114,IF($D114+G$98>=6,$R31,CHOOSE($D114+G$98,$M31,$N31,$O31,$P31,$Q31))-IF($D114+E$98>=6,$R31,CHOOSE($D114+E$98,$M31,$N31,$O31,$P31,$Q31))),0)
def xcf_Toe_Model_I114(): 
    try:      
        return IF(vcell("H114","Toe Model")>0,MIN(IF(vcell("$D114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J58","Toe Model"),CHOOSE(vcell("$D114","Toe Model")+vcell("G$98","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model"),vcell("$H58","Toe Model"),vcell("$I58","Toe Model")))*vcell("H114","Toe Model"),IF(vcell("$D114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R31","Toe Model"),CHOOSE(vcell("$D114","Toe Model")+vcell("G$98","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model"),vcell("$P31","Toe Model"),vcell("$Q31","Toe Model")))-IF(vcell("$D114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R31","Toe Model"),CHOOSE(vcell("$D114","Toe Model")+vcell("E$98","Toe Model"),vcell("$M31","Toe Model"),vcell("$N31","Toe Model"),vcell("$O31","Toe Model"),vcell("$P31","Toe Model"),vcell("$Q31","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I114')
    return None
xfunctions['Toe Model!I114']=xcf_Toe_Model_I114

#H114-I114/IF($B114+$I$98>=6,$H58,CHOOSE($B114+$I$98,$C58,$D58,$E58,$F58,$G58))
def xcf_Toe_Model_J114(): 
    try:      
        return vcell("H114","Toe Model")-vcell("I114","Toe Model")/IF(vcell("$B114","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J114')
    return None
xfunctions['Toe Model!J114']=xcf_Toe_Model_J114

#IF(J114>0,MIN(IF($F114+G$98>=6,$L58,CHOOSE($F114+G$98,$G58,$H58,$I58,$J58,$K58))*J114,IF($F114+G$98>=6,$T31,CHOOSE($F114+G$98,$O31,$P31,$Q31,$R31,$S31))-IF($F114+E$98>=6,$T31,CHOOSE($F114+E$98,$O31,$P31,$Q31,$R31,$S31))),0)
def xcf_Toe_Model_K114(): 
    try:      
        return IF(vcell("J114","Toe Model")>0,MIN(IF(vcell("$F114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L58","Toe Model"),CHOOSE(vcell("$F114","Toe Model")+vcell("G$98","Toe Model"),vcell("$G58","Toe Model"),vcell("$H58","Toe Model"),vcell("$I58","Toe Model"),vcell("$J58","Toe Model"),vcell("$K58","Toe Model")))*vcell("J114","Toe Model"),IF(vcell("$F114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T31","Toe Model"),CHOOSE(vcell("$F114","Toe Model")+vcell("G$98","Toe Model"),vcell("$O31","Toe Model"),vcell("$P31","Toe Model"),vcell("$Q31","Toe Model"),vcell("$R31","Toe Model"),vcell("$S31","Toe Model")))-IF(vcell("$F114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T31","Toe Model"),CHOOSE(vcell("$F114","Toe Model")+vcell("E$98","Toe Model"),vcell("$O31","Toe Model"),vcell("$P31","Toe Model"),vcell("$Q31","Toe Model"),vcell("$R31","Toe Model"),vcell("$S31","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K114')
    return None
xfunctions['Toe Model!K114']=xcf_Toe_Model_K114

#J114-K114/IF($B114+$K$98>=6,$H58,CHOOSE($B114+$K$98,$C58,$D58,$E58,$F58,$G58))
def xcf_Toe_Model_L114(): 
    try:      
        return vcell("J114","Toe Model")-vcell("K114","Toe Model")/IF(vcell("$B114","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L114')
    return None
xfunctions['Toe Model!L114']=xcf_Toe_Model_L114

#IF(L114>0,MIN(IF($H114+G$98>=6,$N58,CHOOSE($H114+G$98,$I58,$J58,$K58,$L58,$M58))*L114,IF($H114+G$98>=6,$V31,CHOOSE($H114+G$98,$Q31,$R31,$S31,$T31,$U31))-IF($H114+E$98>=6,$V31,CHOOSE($H114+E$98,$Q31,$R31,$S31,$T31,$U31))),0)
def xcf_Toe_Model_M114(): 
    try:      
        return IF(vcell("L114","Toe Model")>0,MIN(IF(vcell("$H114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N58","Toe Model"),CHOOSE(vcell("$H114","Toe Model")+vcell("G$98","Toe Model"),vcell("$I58","Toe Model"),vcell("$J58","Toe Model"),vcell("$K58","Toe Model"),vcell("$L58","Toe Model"),vcell("$M58","Toe Model")))*vcell("L114","Toe Model"),IF(vcell("$H114","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V31","Toe Model"),CHOOSE(vcell("$H114","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q31","Toe Model"),vcell("$R31","Toe Model"),vcell("$S31","Toe Model"),vcell("$T31","Toe Model"),vcell("$U31","Toe Model")))-IF(vcell("$H114","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V31","Toe Model"),CHOOSE(vcell("$H114","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q31","Toe Model"),vcell("$R31","Toe Model"),vcell("$S31","Toe Model"),vcell("$T31","Toe Model"),vcell("$U31","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M114')
    return None
xfunctions['Toe Model!M114']=xcf_Toe_Model_M114

#L114-M114/IF($B114+$M$98>=6,$H58,CHOOSE($B114+$M$98,$C58,$D58,$E58,$F58,$G58))
def xcf_Toe_Model_N114(): 
    try:      
        return vcell("L114","Toe Model")-vcell("M114","Toe Model")/IF(vcell("$B114","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H58","Toe Model"),CHOOSE(vcell("$B114","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C58","Toe Model"),vcell("$D58","Toe Model"),vcell("$E58","Toe Model"),vcell("$F58","Toe Model"),vcell("$G58","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N114')
    return None
xfunctions['Toe Model!N114']=xcf_Toe_Model_N114

#B85-SUM($C114,$E114,$G114,$I114,$K114,$M114)*SIN(RADIANS($D31))
def xcf_Toe_Model_O114(): 
    try:      
        return vcell("B85","Toe Model")-SUM(vcell("$C114","Toe Model"),vcell("$E114","Toe Model"),vcell("$G114","Toe Model"),vcell("$I114","Toe Model"),vcell("$K114","Toe Model"),vcell("$M114","Toe Model"))*SIN(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O114')
    return None
xfunctions['Toe Model!O114']=xcf_Toe_Model_O114

#C85+IF($D31==360,1,-1)*SUM($C114,$E114,$G114,$I114,$K114,$M114)*COS(RADIANS($D31))
def xcf_Toe_Model_P114(): 
    try:      
        return vcell("C85","Toe Model")+IF(vcell("$D31","Toe Model")==360,1,-1)*SUM(vcell("$C114","Toe Model"),vcell("$E114","Toe Model"),vcell("$G114","Toe Model"),vcell("$I114","Toe Model"),vcell("$K114","Toe Model"),vcell("$M114","Toe Model"))*COS(RADIANS(vcell("$D31","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P114')
    return None
xfunctions['Toe Model!P114']=xcf_Toe_Model_P114

#IF(MIN(C$18:C$40)>P114,IF(P113>=MIN(C$18:C$40),O113+(O114-O113)*(MIN(C$18:C$40)-P113)/(P114-P113),Q113+ABS(O114-O113)),O114)
def xcf_Toe_Model_Q114(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P114","Toe Model"),IF(vcell("P113","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O113","Toe Model")+(vcell("O114","Toe Model")-vcell("O113","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P113","Toe Model"))/(vcell("P114","Toe Model")-vcell("P113","Toe Model")),vcell("Q113","Toe Model")+ABS(vcell("O114","Toe Model")-vcell("O113","Toe Model"))),vcell("O114","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q114')
    return None
xfunctions['Toe Model!Q114']=xcf_Toe_Model_Q114

#MAX(C85+IF($D31==360,1,-1)*SUM($C114,$E114,$G114,$I114,$K114,$M114)*COS(RADIANS($D31)),MIN(C$18:C$40))
def xcf_Toe_Model_R114(): 
    try:      
        return MAX(vcell("C85","Toe Model")+IF(vcell("$D31","Toe Model")==360,1,-1)*SUM(vcell("$C114","Toe Model"),vcell("$E114","Toe Model"),vcell("$G114","Toe Model"),vcell("$I114","Toe Model"),vcell("$K114","Toe Model"),vcell("$M114","Toe Model"))*COS(RADIANS(vcell("$D31","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R114')
    return None
xfunctions['Toe Model!R114']=xcf_Toe_Model_R114

#IF(AND($C86<=$B$4,$C86>$B$5),1,IF(AND($C86<=$B$5,$C86>$B$6),2,IF(AND($C86<=$B$6,$C86>$B$7),3,IF(AND($C86<=$B$7,$C86>$B$8),4,IF(AND($C86<=$B$8,$C86>$B$9),5,6)))))
def xcf_Toe_Model_B115(): 
    try:      
        return IF(AND(vcell("$C86","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C86","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C86","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C86","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C86","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C86","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C86","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C86","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C86","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C86","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B115')
    return None
xfunctions['Toe Model!B115']=xcf_Toe_Model_B115

#IF(ISNUMBER($B115),MIN(CHOOSE($B115,$K32,$L32,$M32,$N32,$O32,$P32),CHOOSE($B115,$C59,$D59,$E59,$F59,$G59,$H59)*$D86),0)
def xcf_Toe_Model_C115(): 
    try:      
        return IF(ISNUMBER(vcell("$B115","Toe Model")),MIN(CHOOSE(vcell("$B115","Toe Model"),vcell("$K32","Toe Model"),vcell("$L32","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model"),vcell("$P32","Toe Model")),CHOOSE(vcell("$B115","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model"),vcell("$H59","Toe Model"))*vcell("$D86","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C115')
    return None
xfunctions['Toe Model!C115']=xcf_Toe_Model_C115

#$D86-$C115/CHOOSE($B115,$C59,$D59,$E59,$F59,$G59,$H59)
def xcf_Toe_Model_D115(): 
    try:      
        return vcell("$D86","Toe Model")-vcell("$C115","Toe Model")/CHOOSE(vcell("$B115","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model"),vcell("$H59","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D115')
    return None
xfunctions['Toe Model!D115']=xcf_Toe_Model_D115

#IF(D115>0,MIN(IF($B115+E$98>=6,$H59,CHOOSE($B115+E$98,$C59,$D59,$E59,$F59,$G59))*D115,IF($B115+E$98>=6,$P32,CHOOSE($B115+E$98,$K32,$L32,$M32,$N32,$O32))-IF($B115>=6,$P32,CHOOSE($B115,$K32,$L32,$M32,$N32,$O32))),0)
def xcf_Toe_Model_E115(): 
    try:      
        return IF(vcell("D115","Toe Model")>0,MIN(IF(vcell("$B115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("E$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))*vcell("D115","Toe Model"),IF(vcell("$B115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P32","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("E$98","Toe Model"),vcell("$K32","Toe Model"),vcell("$L32","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model")))-IF(vcell("$B115","Toe Model")>=6,vcell("$P32","Toe Model"),CHOOSE(vcell("$B115","Toe Model"),vcell("$K32","Toe Model"),vcell("$L32","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E115')
    return None
xfunctions['Toe Model!E115']=xcf_Toe_Model_E115

#D115-E115/IF($B115+$E$98>=6,$H59,CHOOSE($B115+$E$98,$C59,$D59,$E59,$F59,$G59))
def xcf_Toe_Model_F115(): 
    try:      
        return vcell("D115","Toe Model")-vcell("E115","Toe Model")/IF(vcell("$B115","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F115')
    return None
xfunctions['Toe Model!F115']=xcf_Toe_Model_F115

#IF(F115>0,MIN(IF($B115+G$98>=6,$H59,CHOOSE($B115+G$98,$C59,$D59,$E59,$F59,$G59))*F115,IF($B115+G$98>=6,$P32,CHOOSE($B115+G$98,$K32,$L32,$M32,$N32,$O32))-IF($B115+E$98>=6,$P32,CHOOSE($B115+E$98,$K32,$L32,$M32,$N32,$O32))),0)
def xcf_Toe_Model_G115(): 
    try:      
        return IF(vcell("F115","Toe Model")>0,MIN(IF(vcell("$B115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("G$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))*vcell("F115","Toe Model"),IF(vcell("$B115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P32","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("G$98","Toe Model"),vcell("$K32","Toe Model"),vcell("$L32","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model")))-IF(vcell("$B115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P32","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("E$98","Toe Model"),vcell("$K32","Toe Model"),vcell("$L32","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G115')
    return None
xfunctions['Toe Model!G115']=xcf_Toe_Model_G115

#F115-G115/IF($B115+$G$98>=6,$H59,CHOOSE($B115+$G$98,$C59,$D59,$E59,$F59,$G59))
def xcf_Toe_Model_H115(): 
    try:      
        return vcell("F115","Toe Model")-vcell("G115","Toe Model")/IF(vcell("$B115","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H115')
    return None
xfunctions['Toe Model!H115']=xcf_Toe_Model_H115

#IF(H115>0,MIN(IF($D115+G$98>=6,$J59,CHOOSE($D115+G$98,$E59,$F59,$G59,$H59,$I59))*H115,IF($D115+G$98>=6,$R32,CHOOSE($D115+G$98,$M32,$N32,$O32,$P32,$Q32))-IF($D115+E$98>=6,$R32,CHOOSE($D115+E$98,$M32,$N32,$O32,$P32,$Q32))),0)
def xcf_Toe_Model_I115(): 
    try:      
        return IF(vcell("H115","Toe Model")>0,MIN(IF(vcell("$D115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J59","Toe Model"),CHOOSE(vcell("$D115","Toe Model")+vcell("G$98","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model"),vcell("$H59","Toe Model"),vcell("$I59","Toe Model")))*vcell("H115","Toe Model"),IF(vcell("$D115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R32","Toe Model"),CHOOSE(vcell("$D115","Toe Model")+vcell("G$98","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model"),vcell("$P32","Toe Model"),vcell("$Q32","Toe Model")))-IF(vcell("$D115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R32","Toe Model"),CHOOSE(vcell("$D115","Toe Model")+vcell("E$98","Toe Model"),vcell("$M32","Toe Model"),vcell("$N32","Toe Model"),vcell("$O32","Toe Model"),vcell("$P32","Toe Model"),vcell("$Q32","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I115')
    return None
xfunctions['Toe Model!I115']=xcf_Toe_Model_I115

#H115-I115/IF($B115+$I$98>=6,$H59,CHOOSE($B115+$I$98,$C59,$D59,$E59,$F59,$G59))
def xcf_Toe_Model_J115(): 
    try:      
        return vcell("H115","Toe Model")-vcell("I115","Toe Model")/IF(vcell("$B115","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J115')
    return None
xfunctions['Toe Model!J115']=xcf_Toe_Model_J115

#IF(J115>0,MIN(IF($F115+G$98>=6,$L59,CHOOSE($F115+G$98,$G59,$H59,$I59,$J59,$K59))*J115,IF($F115+G$98>=6,$T32,CHOOSE($F115+G$98,$O32,$P32,$Q32,$R32,$S32))-IF($F115+E$98>=6,$T32,CHOOSE($F115+E$98,$O32,$P32,$Q32,$R32,$S32))),0)
def xcf_Toe_Model_K115(): 
    try:      
        return IF(vcell("J115","Toe Model")>0,MIN(IF(vcell("$F115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L59","Toe Model"),CHOOSE(vcell("$F115","Toe Model")+vcell("G$98","Toe Model"),vcell("$G59","Toe Model"),vcell("$H59","Toe Model"),vcell("$I59","Toe Model"),vcell("$J59","Toe Model"),vcell("$K59","Toe Model")))*vcell("J115","Toe Model"),IF(vcell("$F115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T32","Toe Model"),CHOOSE(vcell("$F115","Toe Model")+vcell("G$98","Toe Model"),vcell("$O32","Toe Model"),vcell("$P32","Toe Model"),vcell("$Q32","Toe Model"),vcell("$R32","Toe Model"),vcell("$S32","Toe Model")))-IF(vcell("$F115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T32","Toe Model"),CHOOSE(vcell("$F115","Toe Model")+vcell("E$98","Toe Model"),vcell("$O32","Toe Model"),vcell("$P32","Toe Model"),vcell("$Q32","Toe Model"),vcell("$R32","Toe Model"),vcell("$S32","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K115')
    return None
xfunctions['Toe Model!K115']=xcf_Toe_Model_K115

#J115-K115/IF($B115+$K$98>=6,$H59,CHOOSE($B115+$K$98,$C59,$D59,$E59,$F59,$G59))
def xcf_Toe_Model_L115(): 
    try:      
        return vcell("J115","Toe Model")-vcell("K115","Toe Model")/IF(vcell("$B115","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L115')
    return None
xfunctions['Toe Model!L115']=xcf_Toe_Model_L115

#IF(L115>0,MIN(IF($H115+G$98>=6,$N59,CHOOSE($H115+G$98,$I59,$J59,$K59,$L59,$M59))*L115,IF($H115+G$98>=6,$V32,CHOOSE($H115+G$98,$Q32,$R32,$S32,$T32,$U32))-IF($H115+E$98>=6,$V32,CHOOSE($H115+E$98,$Q32,$R32,$S32,$T32,$U32))),0)
def xcf_Toe_Model_M115(): 
    try:      
        return IF(vcell("L115","Toe Model")>0,MIN(IF(vcell("$H115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N59","Toe Model"),CHOOSE(vcell("$H115","Toe Model")+vcell("G$98","Toe Model"),vcell("$I59","Toe Model"),vcell("$J59","Toe Model"),vcell("$K59","Toe Model"),vcell("$L59","Toe Model"),vcell("$M59","Toe Model")))*vcell("L115","Toe Model"),IF(vcell("$H115","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V32","Toe Model"),CHOOSE(vcell("$H115","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q32","Toe Model"),vcell("$R32","Toe Model"),vcell("$S32","Toe Model"),vcell("$T32","Toe Model"),vcell("$U32","Toe Model")))-IF(vcell("$H115","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V32","Toe Model"),CHOOSE(vcell("$H115","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q32","Toe Model"),vcell("$R32","Toe Model"),vcell("$S32","Toe Model"),vcell("$T32","Toe Model"),vcell("$U32","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M115')
    return None
xfunctions['Toe Model!M115']=xcf_Toe_Model_M115

#L115-M115/IF($B115+$M$98>=6,$H59,CHOOSE($B115+$M$98,$C59,$D59,$E59,$F59,$G59))
def xcf_Toe_Model_N115(): 
    try:      
        return vcell("L115","Toe Model")-vcell("M115","Toe Model")/IF(vcell("$B115","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H59","Toe Model"),CHOOSE(vcell("$B115","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C59","Toe Model"),vcell("$D59","Toe Model"),vcell("$E59","Toe Model"),vcell("$F59","Toe Model"),vcell("$G59","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N115')
    return None
xfunctions['Toe Model!N115']=xcf_Toe_Model_N115

#B86-SUM($C115,$E115,$G115,$I115,$K115,$M115)*SIN(RADIANS($D32))
def xcf_Toe_Model_O115(): 
    try:      
        return vcell("B86","Toe Model")-SUM(vcell("$C115","Toe Model"),vcell("$E115","Toe Model"),vcell("$G115","Toe Model"),vcell("$I115","Toe Model"),vcell("$K115","Toe Model"),vcell("$M115","Toe Model"))*SIN(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O115')
    return None
xfunctions['Toe Model!O115']=xcf_Toe_Model_O115

#C86+IF($D32==360,1,-1)*SUM($C115,$E115,$G115,$I115,$K115,$M115)*COS(RADIANS($D32))
def xcf_Toe_Model_P115(): 
    try:      
        return vcell("C86","Toe Model")+IF(vcell("$D32","Toe Model")==360,1,-1)*SUM(vcell("$C115","Toe Model"),vcell("$E115","Toe Model"),vcell("$G115","Toe Model"),vcell("$I115","Toe Model"),vcell("$K115","Toe Model"),vcell("$M115","Toe Model"))*COS(RADIANS(vcell("$D32","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P115')
    return None
xfunctions['Toe Model!P115']=xcf_Toe_Model_P115

#IF(MIN(C$18:C$40)>P115,IF(P114>=MIN(C$18:C$40),O114+(O115-O114)*(MIN(C$18:C$40)-P114)/(P115-P114),Q114+ABS(O115-O114)),O115)
def xcf_Toe_Model_Q115(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P115","Toe Model"),IF(vcell("P114","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O114","Toe Model")+(vcell("O115","Toe Model")-vcell("O114","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P114","Toe Model"))/(vcell("P115","Toe Model")-vcell("P114","Toe Model")),vcell("Q114","Toe Model")+ABS(vcell("O115","Toe Model")-vcell("O114","Toe Model"))),vcell("O115","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q115')
    return None
xfunctions['Toe Model!Q115']=xcf_Toe_Model_Q115

#MAX(C86+IF($D32==360,1,-1)*SUM($C115,$E115,$G115,$I115,$K115,$M115)*COS(RADIANS($D32)),MIN(C$18:C$40))
def xcf_Toe_Model_R115(): 
    try:      
        return MAX(vcell("C86","Toe Model")+IF(vcell("$D32","Toe Model")==360,1,-1)*SUM(vcell("$C115","Toe Model"),vcell("$E115","Toe Model"),vcell("$G115","Toe Model"),vcell("$I115","Toe Model"),vcell("$K115","Toe Model"),vcell("$M115","Toe Model"))*COS(RADIANS(vcell("$D32","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R115')
    return None
xfunctions['Toe Model!R115']=xcf_Toe_Model_R115

#IF(AND($C87<=$B$4,$C87>$B$5),1,IF(AND($C87<=$B$5,$C87>$B$6),2,IF(AND($C87<=$B$6,$C87>$B$7),3,IF(AND($C87<=$B$7,$C87>$B$8),4,IF(AND($C87<=$B$8,$C87>$B$9),5,6)))))
def xcf_Toe_Model_B116(): 
    try:      
        return IF(AND(vcell("$C87","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C87","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C87","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C87","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C87","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C87","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C87","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C87","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C87","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C87","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B116')
    return None
xfunctions['Toe Model!B116']=xcf_Toe_Model_B116

#IF(ISNUMBER($B116),MIN(CHOOSE($B116,$K33,$L33,$M33,$N33,$O33,$P33),CHOOSE($B116,$C60,$D60,$E60,$F60,$G60,$H60)*$D87),0)
def xcf_Toe_Model_C116(): 
    try:      
        return IF(ISNUMBER(vcell("$B116","Toe Model")),MIN(CHOOSE(vcell("$B116","Toe Model"),vcell("$K33","Toe Model"),vcell("$L33","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model"),vcell("$P33","Toe Model")),CHOOSE(vcell("$B116","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model"),vcell("$H60","Toe Model"))*vcell("$D87","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C116')
    return None
xfunctions['Toe Model!C116']=xcf_Toe_Model_C116

#$D87-$C116/CHOOSE($B116,$C60,$D60,$E60,$F60,$G60,$H60)
def xcf_Toe_Model_D116(): 
    try:      
        return vcell("$D87","Toe Model")-vcell("$C116","Toe Model")/CHOOSE(vcell("$B116","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model"),vcell("$H60","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D116')
    return None
xfunctions['Toe Model!D116']=xcf_Toe_Model_D116

#IF(D116>0,MIN(IF($B116+E$98>=6,$H60,CHOOSE($B116+E$98,$C60,$D60,$E60,$F60,$G60))*D116,IF($B116+E$98>=6,$P33,CHOOSE($B116+E$98,$K33,$L33,$M33,$N33,$O33))-IF($B116>=6,$P33,CHOOSE($B116,$K33,$L33,$M33,$N33,$O33))),0)
def xcf_Toe_Model_E116(): 
    try:      
        return IF(vcell("D116","Toe Model")>0,MIN(IF(vcell("$B116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("E$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))*vcell("D116","Toe Model"),IF(vcell("$B116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P33","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("E$98","Toe Model"),vcell("$K33","Toe Model"),vcell("$L33","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model")))-IF(vcell("$B116","Toe Model")>=6,vcell("$P33","Toe Model"),CHOOSE(vcell("$B116","Toe Model"),vcell("$K33","Toe Model"),vcell("$L33","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E116')
    return None
xfunctions['Toe Model!E116']=xcf_Toe_Model_E116

#D116-E116/IF($B116+$E$98>=6,$H60,CHOOSE($B116+$E$98,$C60,$D60,$E60,$F60,$G60))
def xcf_Toe_Model_F116(): 
    try:      
        return vcell("D116","Toe Model")-vcell("E116","Toe Model")/IF(vcell("$B116","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F116')
    return None
xfunctions['Toe Model!F116']=xcf_Toe_Model_F116

#IF(F116>0,MIN(IF($B116+G$98>=6,$H60,CHOOSE($B116+G$98,$C60,$D60,$E60,$F60,$G60))*F116,IF($B116+G$98>=6,$P33,CHOOSE($B116+G$98,$K33,$L33,$M33,$N33,$O33))-IF($B116+E$98>=6,$P33,CHOOSE($B116+E$98,$K33,$L33,$M33,$N33,$O33))),0)
def xcf_Toe_Model_G116(): 
    try:      
        return IF(vcell("F116","Toe Model")>0,MIN(IF(vcell("$B116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("G$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))*vcell("F116","Toe Model"),IF(vcell("$B116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P33","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("G$98","Toe Model"),vcell("$K33","Toe Model"),vcell("$L33","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model")))-IF(vcell("$B116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P33","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("E$98","Toe Model"),vcell("$K33","Toe Model"),vcell("$L33","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G116')
    return None
xfunctions['Toe Model!G116']=xcf_Toe_Model_G116

#F116-G116/IF($B116+$G$98>=6,$H60,CHOOSE($B116+$G$98,$C60,$D60,$E60,$F60,$G60))
def xcf_Toe_Model_H116(): 
    try:      
        return vcell("F116","Toe Model")-vcell("G116","Toe Model")/IF(vcell("$B116","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H116')
    return None
xfunctions['Toe Model!H116']=xcf_Toe_Model_H116

#IF(H116>0,MIN(IF($D116+G$98>=6,$J60,CHOOSE($D116+G$98,$E60,$F60,$G60,$H60,$I60))*H116,IF($D116+G$98>=6,$R33,CHOOSE($D116+G$98,$M33,$N33,$O33,$P33,$Q33))-IF($D116+E$98>=6,$R33,CHOOSE($D116+E$98,$M33,$N33,$O33,$P33,$Q33))),0)
def xcf_Toe_Model_I116(): 
    try:      
        return IF(vcell("H116","Toe Model")>0,MIN(IF(vcell("$D116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J60","Toe Model"),CHOOSE(vcell("$D116","Toe Model")+vcell("G$98","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model"),vcell("$H60","Toe Model"),vcell("$I60","Toe Model")))*vcell("H116","Toe Model"),IF(vcell("$D116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R33","Toe Model"),CHOOSE(vcell("$D116","Toe Model")+vcell("G$98","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model"),vcell("$P33","Toe Model"),vcell("$Q33","Toe Model")))-IF(vcell("$D116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R33","Toe Model"),CHOOSE(vcell("$D116","Toe Model")+vcell("E$98","Toe Model"),vcell("$M33","Toe Model"),vcell("$N33","Toe Model"),vcell("$O33","Toe Model"),vcell("$P33","Toe Model"),vcell("$Q33","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I116')
    return None
xfunctions['Toe Model!I116']=xcf_Toe_Model_I116

#H116-I116/IF($B116+$I$98>=6,$H60,CHOOSE($B116+$I$98,$C60,$D60,$E60,$F60,$G60))
def xcf_Toe_Model_J116(): 
    try:      
        return vcell("H116","Toe Model")-vcell("I116","Toe Model")/IF(vcell("$B116","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J116')
    return None
xfunctions['Toe Model!J116']=xcf_Toe_Model_J116

#IF(J116>0,MIN(IF($F116+G$98>=6,$L60,CHOOSE($F116+G$98,$G60,$H60,$I60,$J60,$K60))*J116,IF($F116+G$98>=6,$T33,CHOOSE($F116+G$98,$O33,$P33,$Q33,$R33,$S33))-IF($F116+E$98>=6,$T33,CHOOSE($F116+E$98,$O33,$P33,$Q33,$R33,$S33))),0)
def xcf_Toe_Model_K116(): 
    try:      
        return IF(vcell("J116","Toe Model")>0,MIN(IF(vcell("$F116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L60","Toe Model"),CHOOSE(vcell("$F116","Toe Model")+vcell("G$98","Toe Model"),vcell("$G60","Toe Model"),vcell("$H60","Toe Model"),vcell("$I60","Toe Model"),vcell("$J60","Toe Model"),vcell("$K60","Toe Model")))*vcell("J116","Toe Model"),IF(vcell("$F116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T33","Toe Model"),CHOOSE(vcell("$F116","Toe Model")+vcell("G$98","Toe Model"),vcell("$O33","Toe Model"),vcell("$P33","Toe Model"),vcell("$Q33","Toe Model"),vcell("$R33","Toe Model"),vcell("$S33","Toe Model")))-IF(vcell("$F116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T33","Toe Model"),CHOOSE(vcell("$F116","Toe Model")+vcell("E$98","Toe Model"),vcell("$O33","Toe Model"),vcell("$P33","Toe Model"),vcell("$Q33","Toe Model"),vcell("$R33","Toe Model"),vcell("$S33","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K116')
    return None
xfunctions['Toe Model!K116']=xcf_Toe_Model_K116

#J116-K116/IF($B116+$K$98>=6,$H60,CHOOSE($B116+$K$98,$C60,$D60,$E60,$F60,$G60))
def xcf_Toe_Model_L116(): 
    try:      
        return vcell("J116","Toe Model")-vcell("K116","Toe Model")/IF(vcell("$B116","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L116')
    return None
xfunctions['Toe Model!L116']=xcf_Toe_Model_L116

#IF(L116>0,MIN(IF($H116+G$98>=6,$N60,CHOOSE($H116+G$98,$I60,$J60,$K60,$L60,$M60))*L116,IF($H116+G$98>=6,$V33,CHOOSE($H116+G$98,$Q33,$R33,$S33,$T33,$U33))-IF($H116+E$98>=6,$V33,CHOOSE($H116+E$98,$Q33,$R33,$S33,$T33,$U33))),0)
def xcf_Toe_Model_M116(): 
    try:      
        return IF(vcell("L116","Toe Model")>0,MIN(IF(vcell("$H116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N60","Toe Model"),CHOOSE(vcell("$H116","Toe Model")+vcell("G$98","Toe Model"),vcell("$I60","Toe Model"),vcell("$J60","Toe Model"),vcell("$K60","Toe Model"),vcell("$L60","Toe Model"),vcell("$M60","Toe Model")))*vcell("L116","Toe Model"),IF(vcell("$H116","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V33","Toe Model"),CHOOSE(vcell("$H116","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q33","Toe Model"),vcell("$R33","Toe Model"),vcell("$S33","Toe Model"),vcell("$T33","Toe Model"),vcell("$U33","Toe Model")))-IF(vcell("$H116","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V33","Toe Model"),CHOOSE(vcell("$H116","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q33","Toe Model"),vcell("$R33","Toe Model"),vcell("$S33","Toe Model"),vcell("$T33","Toe Model"),vcell("$U33","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M116')
    return None
xfunctions['Toe Model!M116']=xcf_Toe_Model_M116

#L116-M116/IF($B116+$M$98>=6,$H60,CHOOSE($B116+$M$98,$C60,$D60,$E60,$F60,$G60))
def xcf_Toe_Model_N116(): 
    try:      
        return vcell("L116","Toe Model")-vcell("M116","Toe Model")/IF(vcell("$B116","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H60","Toe Model"),CHOOSE(vcell("$B116","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C60","Toe Model"),vcell("$D60","Toe Model"),vcell("$E60","Toe Model"),vcell("$F60","Toe Model"),vcell("$G60","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N116')
    return None
xfunctions['Toe Model!N116']=xcf_Toe_Model_N116

#B87-SUM($C116,$E116,$G116,$I116,$K116,$M116)*SIN(RADIANS($D33))
def xcf_Toe_Model_O116(): 
    try:      
        return vcell("B87","Toe Model")-SUM(vcell("$C116","Toe Model"),vcell("$E116","Toe Model"),vcell("$G116","Toe Model"),vcell("$I116","Toe Model"),vcell("$K116","Toe Model"),vcell("$M116","Toe Model"))*SIN(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O116')
    return None
xfunctions['Toe Model!O116']=xcf_Toe_Model_O116

#C87+IF($D33==360,1,-1)*SUM($C116,$E116,$G116,$I116,$K116,$M116)*COS(RADIANS($D33))
def xcf_Toe_Model_P116(): 
    try:      
        return vcell("C87","Toe Model")+IF(vcell("$D33","Toe Model")==360,1,-1)*SUM(vcell("$C116","Toe Model"),vcell("$E116","Toe Model"),vcell("$G116","Toe Model"),vcell("$I116","Toe Model"),vcell("$K116","Toe Model"),vcell("$M116","Toe Model"))*COS(RADIANS(vcell("$D33","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P116')
    return None
xfunctions['Toe Model!P116']=xcf_Toe_Model_P116

#IF(MIN(C$18:C$40)>P116,IF(P115>=MIN(C$18:C$40),O115+(O116-O115)*(MIN(C$18:C$40)-P115)/(P116-P115),Q115+ABS(O116-O115)),O116)
def xcf_Toe_Model_Q116(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P116","Toe Model"),IF(vcell("P115","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O115","Toe Model")+(vcell("O116","Toe Model")-vcell("O115","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P115","Toe Model"))/(vcell("P116","Toe Model")-vcell("P115","Toe Model")),vcell("Q115","Toe Model")+ABS(vcell("O116","Toe Model")-vcell("O115","Toe Model"))),vcell("O116","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q116')
    return None
xfunctions['Toe Model!Q116']=xcf_Toe_Model_Q116

#MAX(C87+IF($D33==360,1,-1)*SUM($C116,$E116,$G116,$I116,$K116,$M116)*COS(RADIANS($D33)),MIN(C$18:C$40))
def xcf_Toe_Model_R116(): 
    try:      
        return MAX(vcell("C87","Toe Model")+IF(vcell("$D33","Toe Model")==360,1,-1)*SUM(vcell("$C116","Toe Model"),vcell("$E116","Toe Model"),vcell("$G116","Toe Model"),vcell("$I116","Toe Model"),vcell("$K116","Toe Model"),vcell("$M116","Toe Model"))*COS(RADIANS(vcell("$D33","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R116')
    return None
xfunctions['Toe Model!R116']=xcf_Toe_Model_R116

#IF(AND($C88<=$B$4,$C88>$B$5),1,IF(AND($C88<=$B$5,$C88>$B$6),2,IF(AND($C88<=$B$6,$C88>$B$7),3,IF(AND($C88<=$B$7,$C88>$B$8),4,IF(AND($C88<=$B$8,$C88>$B$9),5,6)))))
def xcf_Toe_Model_B117(): 
    try:      
        return IF(AND(vcell("$C88","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C88","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C88","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C88","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C88","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C88","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C88","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C88","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C88","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C88","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B117')
    return None
xfunctions['Toe Model!B117']=xcf_Toe_Model_B117

#IF(ISNUMBER($B117),MIN(CHOOSE($B117,$K34,$L34,$M34,$N34,$O34,$P34),CHOOSE($B117,$C61,$D61,$E61,$F61,$G61,$H61)*$D88),0)
def xcf_Toe_Model_C117(): 
    try:      
        return IF(ISNUMBER(vcell("$B117","Toe Model")),MIN(CHOOSE(vcell("$B117","Toe Model"),vcell("$K34","Toe Model"),vcell("$L34","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model"),vcell("$P34","Toe Model")),CHOOSE(vcell("$B117","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model"),vcell("$H61","Toe Model"))*vcell("$D88","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C117')
    return None
xfunctions['Toe Model!C117']=xcf_Toe_Model_C117

#$D88-$C117/CHOOSE($B117,$C61,$D61,$E61,$F61,$G61,$H61)
def xcf_Toe_Model_D117(): 
    try:      
        return vcell("$D88","Toe Model")-vcell("$C117","Toe Model")/CHOOSE(vcell("$B117","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model"),vcell("$H61","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D117')
    return None
xfunctions['Toe Model!D117']=xcf_Toe_Model_D117

#IF(D117>0,MIN(IF($B117+E$98>=6,$H61,CHOOSE($B117+E$98,$C61,$D61,$E61,$F61,$G61))*D117,IF($B117+E$98>=6,$P34,CHOOSE($B117+E$98,$K34,$L34,$M34,$N34,$O34))-IF($B117>=6,$P34,CHOOSE($B117,$K34,$L34,$M34,$N34,$O34))),0)
def xcf_Toe_Model_E117(): 
    try:      
        return IF(vcell("D117","Toe Model")>0,MIN(IF(vcell("$B117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("E$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))*vcell("D117","Toe Model"),IF(vcell("$B117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P34","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("E$98","Toe Model"),vcell("$K34","Toe Model"),vcell("$L34","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model")))-IF(vcell("$B117","Toe Model")>=6,vcell("$P34","Toe Model"),CHOOSE(vcell("$B117","Toe Model"),vcell("$K34","Toe Model"),vcell("$L34","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E117')
    return None
xfunctions['Toe Model!E117']=xcf_Toe_Model_E117

#D117-E117/IF($B117+$E$98>=6,$H61,CHOOSE($B117+$E$98,$C61,$D61,$E61,$F61,$G61))
def xcf_Toe_Model_F117(): 
    try:      
        return vcell("D117","Toe Model")-vcell("E117","Toe Model")/IF(vcell("$B117","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F117')
    return None
xfunctions['Toe Model!F117']=xcf_Toe_Model_F117

#IF(F117>0,MIN(IF($B117+G$98>=6,$H61,CHOOSE($B117+G$98,$C61,$D61,$E61,$F61,$G61))*F117,IF($B117+G$98>=6,$P34,CHOOSE($B117+G$98,$K34,$L34,$M34,$N34,$O34))-IF($B117+E$98>=6,$P34,CHOOSE($B117+E$98,$K34,$L34,$M34,$N34,$O34))),0)
def xcf_Toe_Model_G117(): 
    try:      
        return IF(vcell("F117","Toe Model")>0,MIN(IF(vcell("$B117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("G$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))*vcell("F117","Toe Model"),IF(vcell("$B117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P34","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("G$98","Toe Model"),vcell("$K34","Toe Model"),vcell("$L34","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model")))-IF(vcell("$B117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P34","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("E$98","Toe Model"),vcell("$K34","Toe Model"),vcell("$L34","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G117')
    return None
xfunctions['Toe Model!G117']=xcf_Toe_Model_G117

#F117-G117/IF($B117+$G$98>=6,$H61,CHOOSE($B117+$G$98,$C61,$D61,$E61,$F61,$G61))
def xcf_Toe_Model_H117(): 
    try:      
        return vcell("F117","Toe Model")-vcell("G117","Toe Model")/IF(vcell("$B117","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H117')
    return None
xfunctions['Toe Model!H117']=xcf_Toe_Model_H117

#IF(H117>0,MIN(IF($D117+G$98>=6,$J61,CHOOSE($D117+G$98,$E61,$F61,$G61,$H61,$I61))*H117,IF($D117+G$98>=6,$R34,CHOOSE($D117+G$98,$M34,$N34,$O34,$P34,$Q34))-IF($D117+E$98>=6,$R34,CHOOSE($D117+E$98,$M34,$N34,$O34,$P34,$Q34))),0)
def xcf_Toe_Model_I117(): 
    try:      
        return IF(vcell("H117","Toe Model")>0,MIN(IF(vcell("$D117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J61","Toe Model"),CHOOSE(vcell("$D117","Toe Model")+vcell("G$98","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model"),vcell("$H61","Toe Model"),vcell("$I61","Toe Model")))*vcell("H117","Toe Model"),IF(vcell("$D117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R34","Toe Model"),CHOOSE(vcell("$D117","Toe Model")+vcell("G$98","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model"),vcell("$P34","Toe Model"),vcell("$Q34","Toe Model")))-IF(vcell("$D117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R34","Toe Model"),CHOOSE(vcell("$D117","Toe Model")+vcell("E$98","Toe Model"),vcell("$M34","Toe Model"),vcell("$N34","Toe Model"),vcell("$O34","Toe Model"),vcell("$P34","Toe Model"),vcell("$Q34","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I117')
    return None
xfunctions['Toe Model!I117']=xcf_Toe_Model_I117

#H117-I117/IF($B117+$I$98>=6,$H61,CHOOSE($B117+$I$98,$C61,$D61,$E61,$F61,$G61))
def xcf_Toe_Model_J117(): 
    try:      
        return vcell("H117","Toe Model")-vcell("I117","Toe Model")/IF(vcell("$B117","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J117')
    return None
xfunctions['Toe Model!J117']=xcf_Toe_Model_J117

#IF(J117>0,MIN(IF($F117+G$98>=6,$L61,CHOOSE($F117+G$98,$G61,$H61,$I61,$J61,$K61))*J117,IF($F117+G$98>=6,$T34,CHOOSE($F117+G$98,$O34,$P34,$Q34,$R34,$S34))-IF($F117+E$98>=6,$T34,CHOOSE($F117+E$98,$O34,$P34,$Q34,$R34,$S34))),0)
def xcf_Toe_Model_K117(): 
    try:      
        return IF(vcell("J117","Toe Model")>0,MIN(IF(vcell("$F117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L61","Toe Model"),CHOOSE(vcell("$F117","Toe Model")+vcell("G$98","Toe Model"),vcell("$G61","Toe Model"),vcell("$H61","Toe Model"),vcell("$I61","Toe Model"),vcell("$J61","Toe Model"),vcell("$K61","Toe Model")))*vcell("J117","Toe Model"),IF(vcell("$F117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T34","Toe Model"),CHOOSE(vcell("$F117","Toe Model")+vcell("G$98","Toe Model"),vcell("$O34","Toe Model"),vcell("$P34","Toe Model"),vcell("$Q34","Toe Model"),vcell("$R34","Toe Model"),vcell("$S34","Toe Model")))-IF(vcell("$F117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T34","Toe Model"),CHOOSE(vcell("$F117","Toe Model")+vcell("E$98","Toe Model"),vcell("$O34","Toe Model"),vcell("$P34","Toe Model"),vcell("$Q34","Toe Model"),vcell("$R34","Toe Model"),vcell("$S34","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K117')
    return None
xfunctions['Toe Model!K117']=xcf_Toe_Model_K117

#J117-K117/IF($B117+$K$98>=6,$H61,CHOOSE($B117+$K$98,$C61,$D61,$E61,$F61,$G61))
def xcf_Toe_Model_L117(): 
    try:      
        return vcell("J117","Toe Model")-vcell("K117","Toe Model")/IF(vcell("$B117","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L117')
    return None
xfunctions['Toe Model!L117']=xcf_Toe_Model_L117

#IF(L117>0,MIN(IF($H117+G$98>=6,$N61,CHOOSE($H117+G$98,$I61,$J61,$K61,$L61,$M61))*L117,IF($H117+G$98>=6,$V34,CHOOSE($H117+G$98,$Q34,$R34,$S34,$T34,$U34))-IF($H117+E$98>=6,$V34,CHOOSE($H117+E$98,$Q34,$R34,$S34,$T34,$U34))),0)
def xcf_Toe_Model_M117(): 
    try:      
        return IF(vcell("L117","Toe Model")>0,MIN(IF(vcell("$H117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N61","Toe Model"),CHOOSE(vcell("$H117","Toe Model")+vcell("G$98","Toe Model"),vcell("$I61","Toe Model"),vcell("$J61","Toe Model"),vcell("$K61","Toe Model"),vcell("$L61","Toe Model"),vcell("$M61","Toe Model")))*vcell("L117","Toe Model"),IF(vcell("$H117","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V34","Toe Model"),CHOOSE(vcell("$H117","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q34","Toe Model"),vcell("$R34","Toe Model"),vcell("$S34","Toe Model"),vcell("$T34","Toe Model"),vcell("$U34","Toe Model")))-IF(vcell("$H117","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V34","Toe Model"),CHOOSE(vcell("$H117","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q34","Toe Model"),vcell("$R34","Toe Model"),vcell("$S34","Toe Model"),vcell("$T34","Toe Model"),vcell("$U34","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M117')
    return None
xfunctions['Toe Model!M117']=xcf_Toe_Model_M117

#L117-M117/IF($B117+$M$98>=6,$H61,CHOOSE($B117+$M$98,$C61,$D61,$E61,$F61,$G61))
def xcf_Toe_Model_N117(): 
    try:      
        return vcell("L117","Toe Model")-vcell("M117","Toe Model")/IF(vcell("$B117","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H61","Toe Model"),CHOOSE(vcell("$B117","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C61","Toe Model"),vcell("$D61","Toe Model"),vcell("$E61","Toe Model"),vcell("$F61","Toe Model"),vcell("$G61","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N117')
    return None
xfunctions['Toe Model!N117']=xcf_Toe_Model_N117

#B88-SUM($C117,$E117,$G117,$I117,$K117,$M117)*SIN(RADIANS($D34))
def xcf_Toe_Model_O117(): 
    try:      
        return vcell("B88","Toe Model")-SUM(vcell("$C117","Toe Model"),vcell("$E117","Toe Model"),vcell("$G117","Toe Model"),vcell("$I117","Toe Model"),vcell("$K117","Toe Model"),vcell("$M117","Toe Model"))*SIN(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O117')
    return None
xfunctions['Toe Model!O117']=xcf_Toe_Model_O117

#C88+IF($D34==360,1,-1)*SUM($C117,$E117,$G117,$I117,$K117,$M117)*COS(RADIANS($D34))
def xcf_Toe_Model_P117(): 
    try:      
        return vcell("C88","Toe Model")+IF(vcell("$D34","Toe Model")==360,1,-1)*SUM(vcell("$C117","Toe Model"),vcell("$E117","Toe Model"),vcell("$G117","Toe Model"),vcell("$I117","Toe Model"),vcell("$K117","Toe Model"),vcell("$M117","Toe Model"))*COS(RADIANS(vcell("$D34","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P117')
    return None
xfunctions['Toe Model!P117']=xcf_Toe_Model_P117

#IF(MIN(C$18:C$40)>P117,IF(P116>=MIN(C$18:C$40),O116+(O117-O116)*(MIN(C$18:C$40)-P116)/(P117-P116),Q116+ABS(O117-O116)),O117)
def xcf_Toe_Model_Q117(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P117","Toe Model"),IF(vcell("P116","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O116","Toe Model")+(vcell("O117","Toe Model")-vcell("O116","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P116","Toe Model"))/(vcell("P117","Toe Model")-vcell("P116","Toe Model")),vcell("Q116","Toe Model")+ABS(vcell("O117","Toe Model")-vcell("O116","Toe Model"))),vcell("O117","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q117')
    return None
xfunctions['Toe Model!Q117']=xcf_Toe_Model_Q117

#MAX(C88+IF($D34==360,1,-1)*SUM($C117,$E117,$G117,$I117,$K117,$M117)*COS(RADIANS($D34)),MIN(C$18:C$40))
def xcf_Toe_Model_R117(): 
    try:      
        return MAX(vcell("C88","Toe Model")+IF(vcell("$D34","Toe Model")==360,1,-1)*SUM(vcell("$C117","Toe Model"),vcell("$E117","Toe Model"),vcell("$G117","Toe Model"),vcell("$I117","Toe Model"),vcell("$K117","Toe Model"),vcell("$M117","Toe Model"))*COS(RADIANS(vcell("$D34","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R117')
    return None
xfunctions['Toe Model!R117']=xcf_Toe_Model_R117

#IF(AND($C89<=$B$4,$C89>$B$5),1,IF(AND($C89<=$B$5,$C89>$B$6),2,IF(AND($C89<=$B$6,$C89>$B$7),3,IF(AND($C89<=$B$7,$C89>$B$8),4,IF(AND($C89<=$B$8,$C89>$B$9),5,6)))))
def xcf_Toe_Model_B118(): 
    try:      
        return IF(AND(vcell("$C89","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C89","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C89","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C89","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C89","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C89","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C89","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C89","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C89","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C89","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B118')
    return None
xfunctions['Toe Model!B118']=xcf_Toe_Model_B118

#IF(ISNUMBER($B118),MIN(CHOOSE($B118,$K35,$L35,$M35,$N35,$O35,$P35),CHOOSE($B118,$C62,$D62,$E62,$F62,$G62,$H62)*$D89),0)
def xcf_Toe_Model_C118(): 
    try:      
        return IF(ISNUMBER(vcell("$B118","Toe Model")),MIN(CHOOSE(vcell("$B118","Toe Model"),vcell("$K35","Toe Model"),vcell("$L35","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model"),vcell("$P35","Toe Model")),CHOOSE(vcell("$B118","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model"),vcell("$H62","Toe Model"))*vcell("$D89","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C118')
    return None
xfunctions['Toe Model!C118']=xcf_Toe_Model_C118

#$D89-$C118/CHOOSE($B118,$C62,$D62,$E62,$F62,$G62,$H62)
def xcf_Toe_Model_D118(): 
    try:      
        return vcell("$D89","Toe Model")-vcell("$C118","Toe Model")/CHOOSE(vcell("$B118","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model"),vcell("$H62","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D118')
    return None
xfunctions['Toe Model!D118']=xcf_Toe_Model_D118

#IF(D118>0,MIN(IF($B118+E$98>=6,$H62,CHOOSE($B118+E$98,$C62,$D62,$E62,$F62,$G62))*D118,IF($B118+E$98>=6,$P35,CHOOSE($B118+E$98,$K35,$L35,$M35,$N35,$O35))-IF($B118>=6,$P35,CHOOSE($B118,$K35,$L35,$M35,$N35,$O35))),0)
def xcf_Toe_Model_E118(): 
    try:      
        return IF(vcell("D118","Toe Model")>0,MIN(IF(vcell("$B118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("E$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))*vcell("D118","Toe Model"),IF(vcell("$B118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P35","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("E$98","Toe Model"),vcell("$K35","Toe Model"),vcell("$L35","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model")))-IF(vcell("$B118","Toe Model")>=6,vcell("$P35","Toe Model"),CHOOSE(vcell("$B118","Toe Model"),vcell("$K35","Toe Model"),vcell("$L35","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E118')
    return None
xfunctions['Toe Model!E118']=xcf_Toe_Model_E118

#D118-E118/IF($B118+$E$98>=6,$H62,CHOOSE($B118+$E$98,$C62,$D62,$E62,$F62,$G62))
def xcf_Toe_Model_F118(): 
    try:      
        return vcell("D118","Toe Model")-vcell("E118","Toe Model")/IF(vcell("$B118","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F118')
    return None
xfunctions['Toe Model!F118']=xcf_Toe_Model_F118

#IF(F118>0,MIN(IF($B118+G$98>=6,$H62,CHOOSE($B118+G$98,$C62,$D62,$E62,$F62,$G62))*F118,IF($B118+G$98>=6,$P35,CHOOSE($B118+G$98,$K35,$L35,$M35,$N35,$O35))-IF($B118+E$98>=6,$P35,CHOOSE($B118+E$98,$K35,$L35,$M35,$N35,$O35))),0)
def xcf_Toe_Model_G118(): 
    try:      
        return IF(vcell("F118","Toe Model")>0,MIN(IF(vcell("$B118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("G$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))*vcell("F118","Toe Model"),IF(vcell("$B118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P35","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("G$98","Toe Model"),vcell("$K35","Toe Model"),vcell("$L35","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model")))-IF(vcell("$B118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P35","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("E$98","Toe Model"),vcell("$K35","Toe Model"),vcell("$L35","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G118')
    return None
xfunctions['Toe Model!G118']=xcf_Toe_Model_G118

#F118-G118/IF($B118+$G$98>=6,$H62,CHOOSE($B118+$G$98,$C62,$D62,$E62,$F62,$G62))
def xcf_Toe_Model_H118(): 
    try:      
        return vcell("F118","Toe Model")-vcell("G118","Toe Model")/IF(vcell("$B118","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H118')
    return None
xfunctions['Toe Model!H118']=xcf_Toe_Model_H118

#IF(H118>0,MIN(IF($D118+G$98>=6,$J62,CHOOSE($D118+G$98,$E62,$F62,$G62,$H62,$I62))*H118,IF($D118+G$98>=6,$R35,CHOOSE($D118+G$98,$M35,$N35,$O35,$P35,$Q35))-IF($D118+E$98>=6,$R35,CHOOSE($D118+E$98,$M35,$N35,$O35,$P35,$Q35))),0)
def xcf_Toe_Model_I118(): 
    try:      
        return IF(vcell("H118","Toe Model")>0,MIN(IF(vcell("$D118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J62","Toe Model"),CHOOSE(vcell("$D118","Toe Model")+vcell("G$98","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model"),vcell("$H62","Toe Model"),vcell("$I62","Toe Model")))*vcell("H118","Toe Model"),IF(vcell("$D118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R35","Toe Model"),CHOOSE(vcell("$D118","Toe Model")+vcell("G$98","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model"),vcell("$P35","Toe Model"),vcell("$Q35","Toe Model")))-IF(vcell("$D118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R35","Toe Model"),CHOOSE(vcell("$D118","Toe Model")+vcell("E$98","Toe Model"),vcell("$M35","Toe Model"),vcell("$N35","Toe Model"),vcell("$O35","Toe Model"),vcell("$P35","Toe Model"),vcell("$Q35","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I118')
    return None
xfunctions['Toe Model!I118']=xcf_Toe_Model_I118

#H118-I118/IF($B118+$I$98>=6,$H62,CHOOSE($B118+$I$98,$C62,$D62,$E62,$F62,$G62))
def xcf_Toe_Model_J118(): 
    try:      
        return vcell("H118","Toe Model")-vcell("I118","Toe Model")/IF(vcell("$B118","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J118')
    return None
xfunctions['Toe Model!J118']=xcf_Toe_Model_J118

#IF(J118>0,MIN(IF($F118+G$98>=6,$L62,CHOOSE($F118+G$98,$G62,$H62,$I62,$J62,$K62))*J118,IF($F118+G$98>=6,$T35,CHOOSE($F118+G$98,$O35,$P35,$Q35,$R35,$S35))-IF($F118+E$98>=6,$T35,CHOOSE($F118+E$98,$O35,$P35,$Q35,$R35,$S35))),0)
def xcf_Toe_Model_K118(): 
    try:      
        return IF(vcell("J118","Toe Model")>0,MIN(IF(vcell("$F118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L62","Toe Model"),CHOOSE(vcell("$F118","Toe Model")+vcell("G$98","Toe Model"),vcell("$G62","Toe Model"),vcell("$H62","Toe Model"),vcell("$I62","Toe Model"),vcell("$J62","Toe Model"),vcell("$K62","Toe Model")))*vcell("J118","Toe Model"),IF(vcell("$F118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T35","Toe Model"),CHOOSE(vcell("$F118","Toe Model")+vcell("G$98","Toe Model"),vcell("$O35","Toe Model"),vcell("$P35","Toe Model"),vcell("$Q35","Toe Model"),vcell("$R35","Toe Model"),vcell("$S35","Toe Model")))-IF(vcell("$F118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T35","Toe Model"),CHOOSE(vcell("$F118","Toe Model")+vcell("E$98","Toe Model"),vcell("$O35","Toe Model"),vcell("$P35","Toe Model"),vcell("$Q35","Toe Model"),vcell("$R35","Toe Model"),vcell("$S35","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K118')
    return None
xfunctions['Toe Model!K118']=xcf_Toe_Model_K118

#J118-K118/IF($B118+$K$98>=6,$H62,CHOOSE($B118+$K$98,$C62,$D62,$E62,$F62,$G62))
def xcf_Toe_Model_L118(): 
    try:      
        return vcell("J118","Toe Model")-vcell("K118","Toe Model")/IF(vcell("$B118","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L118')
    return None
xfunctions['Toe Model!L118']=xcf_Toe_Model_L118

#IF(L118>0,MIN(IF($H118+G$98>=6,$N62,CHOOSE($H118+G$98,$I62,$J62,$K62,$L62,$M62))*L118,IF($H118+G$98>=6,$V35,CHOOSE($H118+G$98,$Q35,$R35,$S35,$T35,$U35))-IF($H118+E$98>=6,$V35,CHOOSE($H118+E$98,$Q35,$R35,$S35,$T35,$U35))),0)
def xcf_Toe_Model_M118(): 
    try:      
        return IF(vcell("L118","Toe Model")>0,MIN(IF(vcell("$H118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N62","Toe Model"),CHOOSE(vcell("$H118","Toe Model")+vcell("G$98","Toe Model"),vcell("$I62","Toe Model"),vcell("$J62","Toe Model"),vcell("$K62","Toe Model"),vcell("$L62","Toe Model"),vcell("$M62","Toe Model")))*vcell("L118","Toe Model"),IF(vcell("$H118","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V35","Toe Model"),CHOOSE(vcell("$H118","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q35","Toe Model"),vcell("$R35","Toe Model"),vcell("$S35","Toe Model"),vcell("$T35","Toe Model"),vcell("$U35","Toe Model")))-IF(vcell("$H118","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V35","Toe Model"),CHOOSE(vcell("$H118","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q35","Toe Model"),vcell("$R35","Toe Model"),vcell("$S35","Toe Model"),vcell("$T35","Toe Model"),vcell("$U35","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M118')
    return None
xfunctions['Toe Model!M118']=xcf_Toe_Model_M118

#L118-M118/IF($B118+$M$98>=6,$H62,CHOOSE($B118+$M$98,$C62,$D62,$E62,$F62,$G62))
def xcf_Toe_Model_N118(): 
    try:      
        return vcell("L118","Toe Model")-vcell("M118","Toe Model")/IF(vcell("$B118","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H62","Toe Model"),CHOOSE(vcell("$B118","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C62","Toe Model"),vcell("$D62","Toe Model"),vcell("$E62","Toe Model"),vcell("$F62","Toe Model"),vcell("$G62","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N118')
    return None
xfunctions['Toe Model!N118']=xcf_Toe_Model_N118

#B89-SUM($C118,$E118,$G118,$I118,$K118,$M118)*SIN(RADIANS($D35))
def xcf_Toe_Model_O118(): 
    try:      
        return vcell("B89","Toe Model")-SUM(vcell("$C118","Toe Model"),vcell("$E118","Toe Model"),vcell("$G118","Toe Model"),vcell("$I118","Toe Model"),vcell("$K118","Toe Model"),vcell("$M118","Toe Model"))*SIN(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O118')
    return None
xfunctions['Toe Model!O118']=xcf_Toe_Model_O118

#C89+IF($D35==360,1,-1)*SUM($C118,$E118,$G118,$I118,$K118,$M118)*COS(RADIANS($D35))
def xcf_Toe_Model_P118(): 
    try:      
        return vcell("C89","Toe Model")+IF(vcell("$D35","Toe Model")==360,1,-1)*SUM(vcell("$C118","Toe Model"),vcell("$E118","Toe Model"),vcell("$G118","Toe Model"),vcell("$I118","Toe Model"),vcell("$K118","Toe Model"),vcell("$M118","Toe Model"))*COS(RADIANS(vcell("$D35","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P118')
    return None
xfunctions['Toe Model!P118']=xcf_Toe_Model_P118

#IF(MIN(C$18:C$40)>P118,IF(P117>=MIN(C$18:C$40),O117+(O118-O117)*(MIN(C$18:C$40)-P117)/(P118-P117),Q117+ABS(O118-O117)),O118)
def xcf_Toe_Model_Q118(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P118","Toe Model"),IF(vcell("P117","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O117","Toe Model")+(vcell("O118","Toe Model")-vcell("O117","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P117","Toe Model"))/(vcell("P118","Toe Model")-vcell("P117","Toe Model")),vcell("Q117","Toe Model")+ABS(vcell("O118","Toe Model")-vcell("O117","Toe Model"))),vcell("O118","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q118')
    return None
xfunctions['Toe Model!Q118']=xcf_Toe_Model_Q118

#MAX(C89+IF($D35==360,1,-1)*SUM($C118,$E118,$G118,$I118,$K118,$M118)*COS(RADIANS($D35)),MIN(C$18:C$40))
def xcf_Toe_Model_R118(): 
    try:      
        return MAX(vcell("C89","Toe Model")+IF(vcell("$D35","Toe Model")==360,1,-1)*SUM(vcell("$C118","Toe Model"),vcell("$E118","Toe Model"),vcell("$G118","Toe Model"),vcell("$I118","Toe Model"),vcell("$K118","Toe Model"),vcell("$M118","Toe Model"))*COS(RADIANS(vcell("$D35","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R118')
    return None
xfunctions['Toe Model!R118']=xcf_Toe_Model_R118

#IF(AND($C90<=$B$4,$C90>$B$5),1,IF(AND($C90<=$B$5,$C90>$B$6),2,IF(AND($C90<=$B$6,$C90>$B$7),3,IF(AND($C90<=$B$7,$C90>$B$8),4,IF(AND($C90<=$B$8,$C90>$B$9),5,6)))))
def xcf_Toe_Model_B119(): 
    try:      
        return IF(AND(vcell("$C90","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C90","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C90","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C90","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C90","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C90","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C90","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C90","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C90","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C90","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B119')
    return None
xfunctions['Toe Model!B119']=xcf_Toe_Model_B119

#IF(ISNUMBER($B119),MIN(CHOOSE($B119,$K36,$L36,$M36,$N36,$O36,$P36),CHOOSE($B119,$C63,$D63,$E63,$F63,$G63,$H63)*$D90),0)
def xcf_Toe_Model_C119(): 
    try:      
        return IF(ISNUMBER(vcell("$B119","Toe Model")),MIN(CHOOSE(vcell("$B119","Toe Model"),vcell("$K36","Toe Model"),vcell("$L36","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model"),vcell("$P36","Toe Model")),CHOOSE(vcell("$B119","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model"),vcell("$H63","Toe Model"))*vcell("$D90","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C119')
    return None
xfunctions['Toe Model!C119']=xcf_Toe_Model_C119

#$D90-$C119/CHOOSE($B119,$C63,$D63,$E63,$F63,$G63,$H63)
def xcf_Toe_Model_D119(): 
    try:      
        return vcell("$D90","Toe Model")-vcell("$C119","Toe Model")/CHOOSE(vcell("$B119","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model"),vcell("$H63","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D119')
    return None
xfunctions['Toe Model!D119']=xcf_Toe_Model_D119

#IF(D119>0,MIN(IF($B119+E$98>=6,$H63,CHOOSE($B119+E$98,$C63,$D63,$E63,$F63,$G63))*D119,IF($B119+E$98>=6,$P36,CHOOSE($B119+E$98,$K36,$L36,$M36,$N36,$O36))-IF($B119>=6,$P36,CHOOSE($B119,$K36,$L36,$M36,$N36,$O36))),0)
def xcf_Toe_Model_E119(): 
    try:      
        return IF(vcell("D119","Toe Model")>0,MIN(IF(vcell("$B119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("E$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))*vcell("D119","Toe Model"),IF(vcell("$B119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P36","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("E$98","Toe Model"),vcell("$K36","Toe Model"),vcell("$L36","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model")))-IF(vcell("$B119","Toe Model")>=6,vcell("$P36","Toe Model"),CHOOSE(vcell("$B119","Toe Model"),vcell("$K36","Toe Model"),vcell("$L36","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E119')
    return None
xfunctions['Toe Model!E119']=xcf_Toe_Model_E119

#D119-E119/IF($B119+$E$98>=6,$H63,CHOOSE($B119+$E$98,$C63,$D63,$E63,$F63,$G63))
def xcf_Toe_Model_F119(): 
    try:      
        return vcell("D119","Toe Model")-vcell("E119","Toe Model")/IF(vcell("$B119","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F119')
    return None
xfunctions['Toe Model!F119']=xcf_Toe_Model_F119

#IF(F119>0,MIN(IF($B119+G$98>=6,$H63,CHOOSE($B119+G$98,$C63,$D63,$E63,$F63,$G63))*F119,IF($B119+G$98>=6,$P36,CHOOSE($B119+G$98,$K36,$L36,$M36,$N36,$O36))-IF($B119+E$98>=6,$P36,CHOOSE($B119+E$98,$K36,$L36,$M36,$N36,$O36))),0)
def xcf_Toe_Model_G119(): 
    try:      
        return IF(vcell("F119","Toe Model")>0,MIN(IF(vcell("$B119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("G$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))*vcell("F119","Toe Model"),IF(vcell("$B119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P36","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("G$98","Toe Model"),vcell("$K36","Toe Model"),vcell("$L36","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model")))-IF(vcell("$B119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P36","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("E$98","Toe Model"),vcell("$K36","Toe Model"),vcell("$L36","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G119')
    return None
xfunctions['Toe Model!G119']=xcf_Toe_Model_G119

#F119-G119/IF($B119+$G$98>=6,$H63,CHOOSE($B119+$G$98,$C63,$D63,$E63,$F63,$G63))
def xcf_Toe_Model_H119(): 
    try:      
        return vcell("F119","Toe Model")-vcell("G119","Toe Model")/IF(vcell("$B119","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H119')
    return None
xfunctions['Toe Model!H119']=xcf_Toe_Model_H119

#IF(H119>0,MIN(IF($D119+G$98>=6,$J63,CHOOSE($D119+G$98,$E63,$F63,$G63,$H63,$I63))*H119,IF($D119+G$98>=6,$R36,CHOOSE($D119+G$98,$M36,$N36,$O36,$P36,$Q36))-IF($D119+E$98>=6,$R36,CHOOSE($D119+E$98,$M36,$N36,$O36,$P36,$Q36))),0)
def xcf_Toe_Model_I119(): 
    try:      
        return IF(vcell("H119","Toe Model")>0,MIN(IF(vcell("$D119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J63","Toe Model"),CHOOSE(vcell("$D119","Toe Model")+vcell("G$98","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model"),vcell("$H63","Toe Model"),vcell("$I63","Toe Model")))*vcell("H119","Toe Model"),IF(vcell("$D119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R36","Toe Model"),CHOOSE(vcell("$D119","Toe Model")+vcell("G$98","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model"),vcell("$P36","Toe Model"),vcell("$Q36","Toe Model")))-IF(vcell("$D119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R36","Toe Model"),CHOOSE(vcell("$D119","Toe Model")+vcell("E$98","Toe Model"),vcell("$M36","Toe Model"),vcell("$N36","Toe Model"),vcell("$O36","Toe Model"),vcell("$P36","Toe Model"),vcell("$Q36","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I119')
    return None
xfunctions['Toe Model!I119']=xcf_Toe_Model_I119

#H119-I119/IF($B119+$I$98>=6,$H63,CHOOSE($B119+$I$98,$C63,$D63,$E63,$F63,$G63))
def xcf_Toe_Model_J119(): 
    try:      
        return vcell("H119","Toe Model")-vcell("I119","Toe Model")/IF(vcell("$B119","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J119')
    return None
xfunctions['Toe Model!J119']=xcf_Toe_Model_J119

#IF(J119>0,MIN(IF($F119+G$98>=6,$L63,CHOOSE($F119+G$98,$G63,$H63,$I63,$J63,$K63))*J119,IF($F119+G$98>=6,$T36,CHOOSE($F119+G$98,$O36,$P36,$Q36,$R36,$S36))-IF($F119+E$98>=6,$T36,CHOOSE($F119+E$98,$O36,$P36,$Q36,$R36,$S36))),0)
def xcf_Toe_Model_K119(): 
    try:      
        return IF(vcell("J119","Toe Model")>0,MIN(IF(vcell("$F119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L63","Toe Model"),CHOOSE(vcell("$F119","Toe Model")+vcell("G$98","Toe Model"),vcell("$G63","Toe Model"),vcell("$H63","Toe Model"),vcell("$I63","Toe Model"),vcell("$J63","Toe Model"),vcell("$K63","Toe Model")))*vcell("J119","Toe Model"),IF(vcell("$F119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T36","Toe Model"),CHOOSE(vcell("$F119","Toe Model")+vcell("G$98","Toe Model"),vcell("$O36","Toe Model"),vcell("$P36","Toe Model"),vcell("$Q36","Toe Model"),vcell("$R36","Toe Model"),vcell("$S36","Toe Model")))-IF(vcell("$F119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T36","Toe Model"),CHOOSE(vcell("$F119","Toe Model")+vcell("E$98","Toe Model"),vcell("$O36","Toe Model"),vcell("$P36","Toe Model"),vcell("$Q36","Toe Model"),vcell("$R36","Toe Model"),vcell("$S36","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K119')
    return None
xfunctions['Toe Model!K119']=xcf_Toe_Model_K119

#J119-K119/IF($B119+$K$98>=6,$H63,CHOOSE($B119+$K$98,$C63,$D63,$E63,$F63,$G63))
def xcf_Toe_Model_L119(): 
    try:      
        return vcell("J119","Toe Model")-vcell("K119","Toe Model")/IF(vcell("$B119","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L119')
    return None
xfunctions['Toe Model!L119']=xcf_Toe_Model_L119

#IF(L119>0,MIN(IF($H119+G$98>=6,$N63,CHOOSE($H119+G$98,$I63,$J63,$K63,$L63,$M63))*L119,IF($H119+G$98>=6,$V36,CHOOSE($H119+G$98,$Q36,$R36,$S36,$T36,$U36))-IF($H119+E$98>=6,$V36,CHOOSE($H119+E$98,$Q36,$R36,$S36,$T36,$U36))),0)
def xcf_Toe_Model_M119(): 
    try:      
        return IF(vcell("L119","Toe Model")>0,MIN(IF(vcell("$H119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N63","Toe Model"),CHOOSE(vcell("$H119","Toe Model")+vcell("G$98","Toe Model"),vcell("$I63","Toe Model"),vcell("$J63","Toe Model"),vcell("$K63","Toe Model"),vcell("$L63","Toe Model"),vcell("$M63","Toe Model")))*vcell("L119","Toe Model"),IF(vcell("$H119","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V36","Toe Model"),CHOOSE(vcell("$H119","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q36","Toe Model"),vcell("$R36","Toe Model"),vcell("$S36","Toe Model"),vcell("$T36","Toe Model"),vcell("$U36","Toe Model")))-IF(vcell("$H119","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V36","Toe Model"),CHOOSE(vcell("$H119","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q36","Toe Model"),vcell("$R36","Toe Model"),vcell("$S36","Toe Model"),vcell("$T36","Toe Model"),vcell("$U36","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M119')
    return None
xfunctions['Toe Model!M119']=xcf_Toe_Model_M119

#L119-M119/IF($B119+$M$98>=6,$H63,CHOOSE($B119+$M$98,$C63,$D63,$E63,$F63,$G63))
def xcf_Toe_Model_N119(): 
    try:      
        return vcell("L119","Toe Model")-vcell("M119","Toe Model")/IF(vcell("$B119","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H63","Toe Model"),CHOOSE(vcell("$B119","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C63","Toe Model"),vcell("$D63","Toe Model"),vcell("$E63","Toe Model"),vcell("$F63","Toe Model"),vcell("$G63","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N119')
    return None
xfunctions['Toe Model!N119']=xcf_Toe_Model_N119

#B90-SUM($C119,$E119,$G119,$I119,$K119,$M119)*SIN(RADIANS($D36))
def xcf_Toe_Model_O119(): 
    try:      
        return vcell("B90","Toe Model")-SUM(vcell("$C119","Toe Model"),vcell("$E119","Toe Model"),vcell("$G119","Toe Model"),vcell("$I119","Toe Model"),vcell("$K119","Toe Model"),vcell("$M119","Toe Model"))*SIN(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O119')
    return None
xfunctions['Toe Model!O119']=xcf_Toe_Model_O119

#C90+IF($D36==360,1,-1)*SUM($C119,$E119,$G119,$I119,$K119,$M119)*COS(RADIANS($D36))
def xcf_Toe_Model_P119(): 
    try:      
        return vcell("C90","Toe Model")+IF(vcell("$D36","Toe Model")==360,1,-1)*SUM(vcell("$C119","Toe Model"),vcell("$E119","Toe Model"),vcell("$G119","Toe Model"),vcell("$I119","Toe Model"),vcell("$K119","Toe Model"),vcell("$M119","Toe Model"))*COS(RADIANS(vcell("$D36","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P119')
    return None
xfunctions['Toe Model!P119']=xcf_Toe_Model_P119

#IF(MIN(C$18:C$40)>P119,IF(P118>=MIN(C$18:C$40),O118+(O119-O118)*(MIN(C$18:C$40)-P118)/(P119-P118),Q118+ABS(O119-O118)),O119)
def xcf_Toe_Model_Q119(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P119","Toe Model"),IF(vcell("P118","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O118","Toe Model")+(vcell("O119","Toe Model")-vcell("O118","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P118","Toe Model"))/(vcell("P119","Toe Model")-vcell("P118","Toe Model")),vcell("Q118","Toe Model")+ABS(vcell("O119","Toe Model")-vcell("O118","Toe Model"))),vcell("O119","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q119')
    return None
xfunctions['Toe Model!Q119']=xcf_Toe_Model_Q119

#MAX(C90+IF($D36==360,1,-1)*SUM($C119,$E119,$G119,$I119,$K119,$M119)*COS(RADIANS($D36)),MIN(C$18:C$40))
def xcf_Toe_Model_R119(): 
    try:      
        return MAX(vcell("C90","Toe Model")+IF(vcell("$D36","Toe Model")==360,1,-1)*SUM(vcell("$C119","Toe Model"),vcell("$E119","Toe Model"),vcell("$G119","Toe Model"),vcell("$I119","Toe Model"),vcell("$K119","Toe Model"),vcell("$M119","Toe Model"))*COS(RADIANS(vcell("$D36","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R119')
    return None
xfunctions['Toe Model!R119']=xcf_Toe_Model_R119

#IF(AND($C91<=$B$4,$C91>$B$5),1,IF(AND($C91<=$B$5,$C91>$B$6),2,IF(AND($C91<=$B$6,$C91>$B$7),3,IF(AND($C91<=$B$7,$C91>$B$8),4,IF(AND($C91<=$B$8,$C91>$B$9),5,6)))))
def xcf_Toe_Model_B120(): 
    try:      
        return IF(AND(vcell("$C91","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C91","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C91","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C91","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C91","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C91","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C91","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C91","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C91","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C91","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B120')
    return None
xfunctions['Toe Model!B120']=xcf_Toe_Model_B120

#IF(ISNUMBER($B120),MIN(CHOOSE($B120,$K37,$L37,$M37,$N37,$O37,$P37),CHOOSE($B120,$C64,$D64,$E64,$F64,$G64,$H64)*$D91),0)
def xcf_Toe_Model_C120(): 
    try:      
        return IF(ISNUMBER(vcell("$B120","Toe Model")),MIN(CHOOSE(vcell("$B120","Toe Model"),vcell("$K37","Toe Model"),vcell("$L37","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model"),vcell("$P37","Toe Model")),CHOOSE(vcell("$B120","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model"),vcell("$H64","Toe Model"))*vcell("$D91","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C120')
    return None
xfunctions['Toe Model!C120']=xcf_Toe_Model_C120

#$D91-$C120/CHOOSE($B120,$C64,$D64,$E64,$F64,$G64,$H64)
def xcf_Toe_Model_D120(): 
    try:      
        return vcell("$D91","Toe Model")-vcell("$C120","Toe Model")/CHOOSE(vcell("$B120","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model"),vcell("$H64","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D120')
    return None
xfunctions['Toe Model!D120']=xcf_Toe_Model_D120

#IF(D120>0,MIN(IF($B120+E$98>=6,$H64,CHOOSE($B120+E$98,$C64,$D64,$E64,$F64,$G64))*D120,IF($B120+E$98>=6,$P37,CHOOSE($B120+E$98,$K37,$L37,$M37,$N37,$O37))-IF($B120>=6,$P37,CHOOSE($B120,$K37,$L37,$M37,$N37,$O37))),0)
def xcf_Toe_Model_E120(): 
    try:      
        return IF(vcell("D120","Toe Model")>0,MIN(IF(vcell("$B120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("E$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))*vcell("D120","Toe Model"),IF(vcell("$B120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P37","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("E$98","Toe Model"),vcell("$K37","Toe Model"),vcell("$L37","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model")))-IF(vcell("$B120","Toe Model")>=6,vcell("$P37","Toe Model"),CHOOSE(vcell("$B120","Toe Model"),vcell("$K37","Toe Model"),vcell("$L37","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E120')
    return None
xfunctions['Toe Model!E120']=xcf_Toe_Model_E120

#D120-E120/IF($B120+$E$98>=6,$H64,CHOOSE($B120+$E$98,$C64,$D64,$E64,$F64,$G64))
def xcf_Toe_Model_F120(): 
    try:      
        return vcell("D120","Toe Model")-vcell("E120","Toe Model")/IF(vcell("$B120","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F120')
    return None
xfunctions['Toe Model!F120']=xcf_Toe_Model_F120

#IF(F120>0,MIN(IF($B120+G$98>=6,$H64,CHOOSE($B120+G$98,$C64,$D64,$E64,$F64,$G64))*F120,IF($B120+G$98>=6,$P37,CHOOSE($B120+G$98,$K37,$L37,$M37,$N37,$O37))-IF($B120+E$98>=6,$P37,CHOOSE($B120+E$98,$K37,$L37,$M37,$N37,$O37))),0)
def xcf_Toe_Model_G120(): 
    try:      
        return IF(vcell("F120","Toe Model")>0,MIN(IF(vcell("$B120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("G$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))*vcell("F120","Toe Model"),IF(vcell("$B120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P37","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("G$98","Toe Model"),vcell("$K37","Toe Model"),vcell("$L37","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model")))-IF(vcell("$B120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P37","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("E$98","Toe Model"),vcell("$K37","Toe Model"),vcell("$L37","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G120')
    return None
xfunctions['Toe Model!G120']=xcf_Toe_Model_G120

#F120-G120/IF($B120+$G$98>=6,$H64,CHOOSE($B120+$G$98,$C64,$D64,$E64,$F64,$G64))
def xcf_Toe_Model_H120(): 
    try:      
        return vcell("F120","Toe Model")-vcell("G120","Toe Model")/IF(vcell("$B120","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H120')
    return None
xfunctions['Toe Model!H120']=xcf_Toe_Model_H120

#IF(H120>0,MIN(IF($D120+G$98>=6,$J64,CHOOSE($D120+G$98,$E64,$F64,$G64,$H64,$I64))*H120,IF($D120+G$98>=6,$R37,CHOOSE($D120+G$98,$M37,$N37,$O37,$P37,$Q37))-IF($D120+E$98>=6,$R37,CHOOSE($D120+E$98,$M37,$N37,$O37,$P37,$Q37))),0)
def xcf_Toe_Model_I120(): 
    try:      
        return IF(vcell("H120","Toe Model")>0,MIN(IF(vcell("$D120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J64","Toe Model"),CHOOSE(vcell("$D120","Toe Model")+vcell("G$98","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model"),vcell("$H64","Toe Model"),vcell("$I64","Toe Model")))*vcell("H120","Toe Model"),IF(vcell("$D120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R37","Toe Model"),CHOOSE(vcell("$D120","Toe Model")+vcell("G$98","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model"),vcell("$P37","Toe Model"),vcell("$Q37","Toe Model")))-IF(vcell("$D120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R37","Toe Model"),CHOOSE(vcell("$D120","Toe Model")+vcell("E$98","Toe Model"),vcell("$M37","Toe Model"),vcell("$N37","Toe Model"),vcell("$O37","Toe Model"),vcell("$P37","Toe Model"),vcell("$Q37","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I120')
    return None
xfunctions['Toe Model!I120']=xcf_Toe_Model_I120

#H120-I120/IF($B120+$I$98>=6,$H64,CHOOSE($B120+$I$98,$C64,$D64,$E64,$F64,$G64))
def xcf_Toe_Model_J120(): 
    try:      
        return vcell("H120","Toe Model")-vcell("I120","Toe Model")/IF(vcell("$B120","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J120')
    return None
xfunctions['Toe Model!J120']=xcf_Toe_Model_J120

#IF(J120>0,MIN(IF($F120+G$98>=6,$L64,CHOOSE($F120+G$98,$G64,$H64,$I64,$J64,$K64))*J120,IF($F120+G$98>=6,$T37,CHOOSE($F120+G$98,$O37,$P37,$Q37,$R37,$S37))-IF($F120+E$98>=6,$T37,CHOOSE($F120+E$98,$O37,$P37,$Q37,$R37,$S37))),0)
def xcf_Toe_Model_K120(): 
    try:      
        return IF(vcell("J120","Toe Model")>0,MIN(IF(vcell("$F120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L64","Toe Model"),CHOOSE(vcell("$F120","Toe Model")+vcell("G$98","Toe Model"),vcell("$G64","Toe Model"),vcell("$H64","Toe Model"),vcell("$I64","Toe Model"),vcell("$J64","Toe Model"),vcell("$K64","Toe Model")))*vcell("J120","Toe Model"),IF(vcell("$F120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T37","Toe Model"),CHOOSE(vcell("$F120","Toe Model")+vcell("G$98","Toe Model"),vcell("$O37","Toe Model"),vcell("$P37","Toe Model"),vcell("$Q37","Toe Model"),vcell("$R37","Toe Model"),vcell("$S37","Toe Model")))-IF(vcell("$F120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T37","Toe Model"),CHOOSE(vcell("$F120","Toe Model")+vcell("E$98","Toe Model"),vcell("$O37","Toe Model"),vcell("$P37","Toe Model"),vcell("$Q37","Toe Model"),vcell("$R37","Toe Model"),vcell("$S37","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K120')
    return None
xfunctions['Toe Model!K120']=xcf_Toe_Model_K120

#J120-K120/IF($B120+$K$98>=6,$H64,CHOOSE($B120+$K$98,$C64,$D64,$E64,$F64,$G64))
def xcf_Toe_Model_L120(): 
    try:      
        return vcell("J120","Toe Model")-vcell("K120","Toe Model")/IF(vcell("$B120","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L120')
    return None
xfunctions['Toe Model!L120']=xcf_Toe_Model_L120

#IF(L120>0,MIN(IF($H120+G$98>=6,$N64,CHOOSE($H120+G$98,$I64,$J64,$K64,$L64,$M64))*L120,IF($H120+G$98>=6,$V37,CHOOSE($H120+G$98,$Q37,$R37,$S37,$T37,$U37))-IF($H120+E$98>=6,$V37,CHOOSE($H120+E$98,$Q37,$R37,$S37,$T37,$U37))),0)
def xcf_Toe_Model_M120(): 
    try:      
        return IF(vcell("L120","Toe Model")>0,MIN(IF(vcell("$H120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N64","Toe Model"),CHOOSE(vcell("$H120","Toe Model")+vcell("G$98","Toe Model"),vcell("$I64","Toe Model"),vcell("$J64","Toe Model"),vcell("$K64","Toe Model"),vcell("$L64","Toe Model"),vcell("$M64","Toe Model")))*vcell("L120","Toe Model"),IF(vcell("$H120","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V37","Toe Model"),CHOOSE(vcell("$H120","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q37","Toe Model"),vcell("$R37","Toe Model"),vcell("$S37","Toe Model"),vcell("$T37","Toe Model"),vcell("$U37","Toe Model")))-IF(vcell("$H120","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V37","Toe Model"),CHOOSE(vcell("$H120","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q37","Toe Model"),vcell("$R37","Toe Model"),vcell("$S37","Toe Model"),vcell("$T37","Toe Model"),vcell("$U37","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M120')
    return None
xfunctions['Toe Model!M120']=xcf_Toe_Model_M120

#L120-M120/IF($B120+$M$98>=6,$H64,CHOOSE($B120+$M$98,$C64,$D64,$E64,$F64,$G64))
def xcf_Toe_Model_N120(): 
    try:      
        return vcell("L120","Toe Model")-vcell("M120","Toe Model")/IF(vcell("$B120","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H64","Toe Model"),CHOOSE(vcell("$B120","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C64","Toe Model"),vcell("$D64","Toe Model"),vcell("$E64","Toe Model"),vcell("$F64","Toe Model"),vcell("$G64","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N120')
    return None
xfunctions['Toe Model!N120']=xcf_Toe_Model_N120

#B91-SUM($C120,$E120,$G120,$I120,$K120,$M120)*SIN(RADIANS($D37))
def xcf_Toe_Model_O120(): 
    try:      
        return vcell("B91","Toe Model")-SUM(vcell("$C120","Toe Model"),vcell("$E120","Toe Model"),vcell("$G120","Toe Model"),vcell("$I120","Toe Model"),vcell("$K120","Toe Model"),vcell("$M120","Toe Model"))*SIN(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O120')
    return None
xfunctions['Toe Model!O120']=xcf_Toe_Model_O120

#C91+IF($D37==360,1,-1)*SUM($C120,$E120,$G120,$I120,$K120,$M120)*COS(RADIANS($D37))
def xcf_Toe_Model_P120(): 
    try:      
        return vcell("C91","Toe Model")+IF(vcell("$D37","Toe Model")==360,1,-1)*SUM(vcell("$C120","Toe Model"),vcell("$E120","Toe Model"),vcell("$G120","Toe Model"),vcell("$I120","Toe Model"),vcell("$K120","Toe Model"),vcell("$M120","Toe Model"))*COS(RADIANS(vcell("$D37","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P120')
    return None
xfunctions['Toe Model!P120']=xcf_Toe_Model_P120

#IF(MIN(C$18:C$40)>P120,IF(P119>=MIN(C$18:C$40),O119+(O120-O119)*(MIN(C$18:C$40)-P119)/(P120-P119),Q119+ABS(O120-O119)),O120)
def xcf_Toe_Model_Q120(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P120","Toe Model"),IF(vcell("P119","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O119","Toe Model")+(vcell("O120","Toe Model")-vcell("O119","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P119","Toe Model"))/(vcell("P120","Toe Model")-vcell("P119","Toe Model")),vcell("Q119","Toe Model")+ABS(vcell("O120","Toe Model")-vcell("O119","Toe Model"))),vcell("O120","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q120')
    return None
xfunctions['Toe Model!Q120']=xcf_Toe_Model_Q120

#MAX(C91+IF($D37==360,1,-1)*SUM($C120,$E120,$G120,$I120,$K120,$M120)*COS(RADIANS($D37)),MIN(C$18:C$40))
def xcf_Toe_Model_R120(): 
    try:      
        return MAX(vcell("C91","Toe Model")+IF(vcell("$D37","Toe Model")==360,1,-1)*SUM(vcell("$C120","Toe Model"),vcell("$E120","Toe Model"),vcell("$G120","Toe Model"),vcell("$I120","Toe Model"),vcell("$K120","Toe Model"),vcell("$M120","Toe Model"))*COS(RADIANS(vcell("$D37","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R120')
    return None
xfunctions['Toe Model!R120']=xcf_Toe_Model_R120

#IF(AND($C92<=$B$4,$C92>$B$5),1,IF(AND($C92<=$B$5,$C92>$B$6),2,IF(AND($C92<=$B$6,$C92>$B$7),3,IF(AND($C92<=$B$7,$C92>$B$8),4,IF(AND($C92<=$B$8,$C92>$B$9),5,6)))))
def xcf_Toe_Model_B121(): 
    try:      
        return IF(AND(vcell("$C92","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C92","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C92","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C92","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C92","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C92","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C92","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C92","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C92","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C92","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B121')
    return None
xfunctions['Toe Model!B121']=xcf_Toe_Model_B121

#IF(ISNUMBER($B121),MIN(CHOOSE($B121,$K38,$L38,$M38,$N38,$O38,$P38),CHOOSE($B121,$C65,$D65,$E65,$F65,$G65,$H65)*$D92),0)
def xcf_Toe_Model_C121(): 
    try:      
        return IF(ISNUMBER(vcell("$B121","Toe Model")),MIN(CHOOSE(vcell("$B121","Toe Model"),vcell("$K38","Toe Model"),vcell("$L38","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model"),vcell("$P38","Toe Model")),CHOOSE(vcell("$B121","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model"),vcell("$H65","Toe Model"))*vcell("$D92","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C121')
    return None
xfunctions['Toe Model!C121']=xcf_Toe_Model_C121

#$D92-$C121/CHOOSE($B121,$C65,$D65,$E65,$F65,$G65,$H65)
def xcf_Toe_Model_D121(): 
    try:      
        return vcell("$D92","Toe Model")-vcell("$C121","Toe Model")/CHOOSE(vcell("$B121","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model"),vcell("$H65","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D121')
    return None
xfunctions['Toe Model!D121']=xcf_Toe_Model_D121

#IF(D121>0,MIN(IF($B121+E$98>=6,$H65,CHOOSE($B121+E$98,$C65,$D65,$E65,$F65,$G65))*D121,IF($B121+E$98>=6,$P38,CHOOSE($B121+E$98,$K38,$L38,$M38,$N38,$O38))-IF($B121>=6,$P38,CHOOSE($B121,$K38,$L38,$M38,$N38,$O38))),0)
def xcf_Toe_Model_E121(): 
    try:      
        return IF(vcell("D121","Toe Model")>0,MIN(IF(vcell("$B121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("E$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))*vcell("D121","Toe Model"),IF(vcell("$B121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P38","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("E$98","Toe Model"),vcell("$K38","Toe Model"),vcell("$L38","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model")))-IF(vcell("$B121","Toe Model")>=6,vcell("$P38","Toe Model"),CHOOSE(vcell("$B121","Toe Model"),vcell("$K38","Toe Model"),vcell("$L38","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E121')
    return None
xfunctions['Toe Model!E121']=xcf_Toe_Model_E121

#D121-E121/IF($B121+$E$98>=6,$H65,CHOOSE($B121+$E$98,$C65,$D65,$E65,$F65,$G65))
def xcf_Toe_Model_F121(): 
    try:      
        return vcell("D121","Toe Model")-vcell("E121","Toe Model")/IF(vcell("$B121","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F121')
    return None
xfunctions['Toe Model!F121']=xcf_Toe_Model_F121

#IF(F121>0,MIN(IF($B121+G$98>=6,$H65,CHOOSE($B121+G$98,$C65,$D65,$E65,$F65,$G65))*F121,IF($B121+G$98>=6,$P38,CHOOSE($B121+G$98,$K38,$L38,$M38,$N38,$O38))-IF($B121+E$98>=6,$P38,CHOOSE($B121+E$98,$K38,$L38,$M38,$N38,$O38))),0)
def xcf_Toe_Model_G121(): 
    try:      
        return IF(vcell("F121","Toe Model")>0,MIN(IF(vcell("$B121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("G$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))*vcell("F121","Toe Model"),IF(vcell("$B121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P38","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("G$98","Toe Model"),vcell("$K38","Toe Model"),vcell("$L38","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model")))-IF(vcell("$B121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P38","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("E$98","Toe Model"),vcell("$K38","Toe Model"),vcell("$L38","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G121')
    return None
xfunctions['Toe Model!G121']=xcf_Toe_Model_G121

#F121-G121/IF($B121+$G$98>=6,$H65,CHOOSE($B121+$G$98,$C65,$D65,$E65,$F65,$G65))
def xcf_Toe_Model_H121(): 
    try:      
        return vcell("F121","Toe Model")-vcell("G121","Toe Model")/IF(vcell("$B121","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H121')
    return None
xfunctions['Toe Model!H121']=xcf_Toe_Model_H121

#IF(H121>0,MIN(IF($D121+G$98>=6,$J65,CHOOSE($D121+G$98,$E65,$F65,$G65,$H65,$I65))*H121,IF($D121+G$98>=6,$R38,CHOOSE($D121+G$98,$M38,$N38,$O38,$P38,$Q38))-IF($D121+E$98>=6,$R38,CHOOSE($D121+E$98,$M38,$N38,$O38,$P38,$Q38))),0)
def xcf_Toe_Model_I121(): 
    try:      
        return IF(vcell("H121","Toe Model")>0,MIN(IF(vcell("$D121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J65","Toe Model"),CHOOSE(vcell("$D121","Toe Model")+vcell("G$98","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model"),vcell("$H65","Toe Model"),vcell("$I65","Toe Model")))*vcell("H121","Toe Model"),IF(vcell("$D121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R38","Toe Model"),CHOOSE(vcell("$D121","Toe Model")+vcell("G$98","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model"),vcell("$P38","Toe Model"),vcell("$Q38","Toe Model")))-IF(vcell("$D121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R38","Toe Model"),CHOOSE(vcell("$D121","Toe Model")+vcell("E$98","Toe Model"),vcell("$M38","Toe Model"),vcell("$N38","Toe Model"),vcell("$O38","Toe Model"),vcell("$P38","Toe Model"),vcell("$Q38","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I121')
    return None
xfunctions['Toe Model!I121']=xcf_Toe_Model_I121

#H121-I121/IF($B121+$I$98>=6,$H65,CHOOSE($B121+$I$98,$C65,$D65,$E65,$F65,$G65))
def xcf_Toe_Model_J121(): 
    try:      
        return vcell("H121","Toe Model")-vcell("I121","Toe Model")/IF(vcell("$B121","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J121')
    return None
xfunctions['Toe Model!J121']=xcf_Toe_Model_J121

#IF(J121>0,MIN(IF($F121+G$98>=6,$L65,CHOOSE($F121+G$98,$G65,$H65,$I65,$J65,$K65))*J121,IF($F121+G$98>=6,$T38,CHOOSE($F121+G$98,$O38,$P38,$Q38,$R38,$S38))-IF($F121+E$98>=6,$T38,CHOOSE($F121+E$98,$O38,$P38,$Q38,$R38,$S38))),0)
def xcf_Toe_Model_K121(): 
    try:      
        return IF(vcell("J121","Toe Model")>0,MIN(IF(vcell("$F121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L65","Toe Model"),CHOOSE(vcell("$F121","Toe Model")+vcell("G$98","Toe Model"),vcell("$G65","Toe Model"),vcell("$H65","Toe Model"),vcell("$I65","Toe Model"),vcell("$J65","Toe Model"),vcell("$K65","Toe Model")))*vcell("J121","Toe Model"),IF(vcell("$F121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T38","Toe Model"),CHOOSE(vcell("$F121","Toe Model")+vcell("G$98","Toe Model"),vcell("$O38","Toe Model"),vcell("$P38","Toe Model"),vcell("$Q38","Toe Model"),vcell("$R38","Toe Model"),vcell("$S38","Toe Model")))-IF(vcell("$F121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T38","Toe Model"),CHOOSE(vcell("$F121","Toe Model")+vcell("E$98","Toe Model"),vcell("$O38","Toe Model"),vcell("$P38","Toe Model"),vcell("$Q38","Toe Model"),vcell("$R38","Toe Model"),vcell("$S38","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K121')
    return None
xfunctions['Toe Model!K121']=xcf_Toe_Model_K121

#J121-K121/IF($B121+$K$98>=6,$H65,CHOOSE($B121+$K$98,$C65,$D65,$E65,$F65,$G65))
def xcf_Toe_Model_L121(): 
    try:      
        return vcell("J121","Toe Model")-vcell("K121","Toe Model")/IF(vcell("$B121","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L121')
    return None
xfunctions['Toe Model!L121']=xcf_Toe_Model_L121

#IF(L121>0,MIN(IF($H121+G$98>=6,$N65,CHOOSE($H121+G$98,$I65,$J65,$K65,$L65,$M65))*L121,IF($H121+G$98>=6,$V38,CHOOSE($H121+G$98,$Q38,$R38,$S38,$T38,$U38))-IF($H121+E$98>=6,$V38,CHOOSE($H121+E$98,$Q38,$R38,$S38,$T38,$U38))),0)
def xcf_Toe_Model_M121(): 
    try:      
        return IF(vcell("L121","Toe Model")>0,MIN(IF(vcell("$H121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N65","Toe Model"),CHOOSE(vcell("$H121","Toe Model")+vcell("G$98","Toe Model"),vcell("$I65","Toe Model"),vcell("$J65","Toe Model"),vcell("$K65","Toe Model"),vcell("$L65","Toe Model"),vcell("$M65","Toe Model")))*vcell("L121","Toe Model"),IF(vcell("$H121","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V38","Toe Model"),CHOOSE(vcell("$H121","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q38","Toe Model"),vcell("$R38","Toe Model"),vcell("$S38","Toe Model"),vcell("$T38","Toe Model"),vcell("$U38","Toe Model")))-IF(vcell("$H121","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V38","Toe Model"),CHOOSE(vcell("$H121","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q38","Toe Model"),vcell("$R38","Toe Model"),vcell("$S38","Toe Model"),vcell("$T38","Toe Model"),vcell("$U38","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M121')
    return None
xfunctions['Toe Model!M121']=xcf_Toe_Model_M121

#L121-M121/IF($B121+$M$98>=6,$H65,CHOOSE($B121+$M$98,$C65,$D65,$E65,$F65,$G65))
def xcf_Toe_Model_N121(): 
    try:      
        return vcell("L121","Toe Model")-vcell("M121","Toe Model")/IF(vcell("$B121","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H65","Toe Model"),CHOOSE(vcell("$B121","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C65","Toe Model"),vcell("$D65","Toe Model"),vcell("$E65","Toe Model"),vcell("$F65","Toe Model"),vcell("$G65","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N121')
    return None
xfunctions['Toe Model!N121']=xcf_Toe_Model_N121

#B92-SUM($C121,$E121,$G121,$I121,$K121,$M121)*SIN(RADIANS($D38))
def xcf_Toe_Model_O121(): 
    try:      
        return vcell("B92","Toe Model")-SUM(vcell("$C121","Toe Model"),vcell("$E121","Toe Model"),vcell("$G121","Toe Model"),vcell("$I121","Toe Model"),vcell("$K121","Toe Model"),vcell("$M121","Toe Model"))*SIN(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O121')
    return None
xfunctions['Toe Model!O121']=xcf_Toe_Model_O121

#C92+IF($D38==360,1,-1)*SUM($C121,$E121,$G121,$I121,$K121,$M121)*COS(RADIANS($D38))
def xcf_Toe_Model_P121(): 
    try:      
        return vcell("C92","Toe Model")+IF(vcell("$D38","Toe Model")==360,1,-1)*SUM(vcell("$C121","Toe Model"),vcell("$E121","Toe Model"),vcell("$G121","Toe Model"),vcell("$I121","Toe Model"),vcell("$K121","Toe Model"),vcell("$M121","Toe Model"))*COS(RADIANS(vcell("$D38","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P121')
    return None
xfunctions['Toe Model!P121']=xcf_Toe_Model_P121

#IF(MIN(C$18:C$40)>P121,IF(P120>=MIN(C$18:C$40),O120+(O121-O120)*(MIN(C$18:C$40)-P120)/(P121-P120),Q120+ABS(O121-O120)),O121)
def xcf_Toe_Model_Q121(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P121","Toe Model"),IF(vcell("P120","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O120","Toe Model")+(vcell("O121","Toe Model")-vcell("O120","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P120","Toe Model"))/(vcell("P121","Toe Model")-vcell("P120","Toe Model")),vcell("Q120","Toe Model")+ABS(vcell("O121","Toe Model")-vcell("O120","Toe Model"))),vcell("O121","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q121')
    return None
xfunctions['Toe Model!Q121']=xcf_Toe_Model_Q121

#MAX(C92+IF($D38==360,1,-1)*SUM($C121,$E121,$G121,$I121,$K121,$M121)*COS(RADIANS($D38)),MIN(C$18:C$40))
def xcf_Toe_Model_R121(): 
    try:      
        return MAX(vcell("C92","Toe Model")+IF(vcell("$D38","Toe Model")==360,1,-1)*SUM(vcell("$C121","Toe Model"),vcell("$E121","Toe Model"),vcell("$G121","Toe Model"),vcell("$I121","Toe Model"),vcell("$K121","Toe Model"),vcell("$M121","Toe Model"))*COS(RADIANS(vcell("$D38","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R121')
    return None
xfunctions['Toe Model!R121']=xcf_Toe_Model_R121

#IF(AND($C93<=$B$4,$C93>$B$5),1,IF(AND($C93<=$B$5,$C93>$B$6),2,IF(AND($C93<=$B$6,$C93>$B$7),3,IF(AND($C93<=$B$7,$C93>$B$8),4,IF(AND($C93<=$B$8,$C93>$B$9),5,6)))))
def xcf_Toe_Model_B122(): 
    try:      
        return IF(AND(vcell("$C93","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C93","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C93","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C93","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C93","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C93","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C93","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C93","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C93","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C93","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B122')
    return None
xfunctions['Toe Model!B122']=xcf_Toe_Model_B122

#IF(ISNUMBER($B122),MIN(CHOOSE($B122,$K39,$L39,$M39,$N39,$O39,$P39),CHOOSE($B122,$C66,$D66,$E66,$F66,$G66,$H66)*$D93),0)
def xcf_Toe_Model_C122(): 
    try:      
        return IF(ISNUMBER(vcell("$B122","Toe Model")),MIN(CHOOSE(vcell("$B122","Toe Model"),vcell("$K39","Toe Model"),vcell("$L39","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model"),vcell("$P39","Toe Model")),CHOOSE(vcell("$B122","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model"),vcell("$H66","Toe Model"))*vcell("$D93","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C122')
    return None
xfunctions['Toe Model!C122']=xcf_Toe_Model_C122

#$D93-$C122/CHOOSE($B122,$C66,$D66,$E66,$F66,$G66,$H66)
def xcf_Toe_Model_D122(): 
    try:      
        return vcell("$D93","Toe Model")-vcell("$C122","Toe Model")/CHOOSE(vcell("$B122","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model"),vcell("$H66","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D122')
    return None
xfunctions['Toe Model!D122']=xcf_Toe_Model_D122

#IF(D122>0,MIN(IF($B122+E$98>=6,$H66,CHOOSE($B122+E$98,$C66,$D66,$E66,$F66,$G66))*D122,IF($B122+E$98>=6,$P39,CHOOSE($B122+E$98,$K39,$L39,$M39,$N39,$O39))-IF($B122>=6,$P39,CHOOSE($B122,$K39,$L39,$M39,$N39,$O39))),0)
def xcf_Toe_Model_E122(): 
    try:      
        return IF(vcell("D122","Toe Model")>0,MIN(IF(vcell("$B122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("E$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))*vcell("D122","Toe Model"),IF(vcell("$B122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P39","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("E$98","Toe Model"),vcell("$K39","Toe Model"),vcell("$L39","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model")))-IF(vcell("$B122","Toe Model")>=6,vcell("$P39","Toe Model"),CHOOSE(vcell("$B122","Toe Model"),vcell("$K39","Toe Model"),vcell("$L39","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E122')
    return None
xfunctions['Toe Model!E122']=xcf_Toe_Model_E122

#D122-E122/IF($B122+$E$98>=6,$H66,CHOOSE($B122+$E$98,$C66,$D66,$E66,$F66,$G66))
def xcf_Toe_Model_F122(): 
    try:      
        return vcell("D122","Toe Model")-vcell("E122","Toe Model")/IF(vcell("$B122","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F122')
    return None
xfunctions['Toe Model!F122']=xcf_Toe_Model_F122

#IF(F122>0,MIN(IF($B122+G$98>=6,$H66,CHOOSE($B122+G$98,$C66,$D66,$E66,$F66,$G66))*F122,IF($B122+G$98>=6,$P39,CHOOSE($B122+G$98,$K39,$L39,$M39,$N39,$O39))-IF($B122+E$98>=6,$P39,CHOOSE($B122+E$98,$K39,$L39,$M39,$N39,$O39))),0)
def xcf_Toe_Model_G122(): 
    try:      
        return IF(vcell("F122","Toe Model")>0,MIN(IF(vcell("$B122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("G$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))*vcell("F122","Toe Model"),IF(vcell("$B122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P39","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("G$98","Toe Model"),vcell("$K39","Toe Model"),vcell("$L39","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model")))-IF(vcell("$B122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P39","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("E$98","Toe Model"),vcell("$K39","Toe Model"),vcell("$L39","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G122')
    return None
xfunctions['Toe Model!G122']=xcf_Toe_Model_G122

#F122-G122/IF($B122+$G$98>=6,$H66,CHOOSE($B122+$G$98,$C66,$D66,$E66,$F66,$G66))
def xcf_Toe_Model_H122(): 
    try:      
        return vcell("F122","Toe Model")-vcell("G122","Toe Model")/IF(vcell("$B122","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H122')
    return None
xfunctions['Toe Model!H122']=xcf_Toe_Model_H122

#IF(H122>0,MIN(IF($D122+G$98>=6,$J66,CHOOSE($D122+G$98,$E66,$F66,$G66,$H66,$I66))*H122,IF($D122+G$98>=6,$R39,CHOOSE($D122+G$98,$M39,$N39,$O39,$P39,$Q39))-IF($D122+E$98>=6,$R39,CHOOSE($D122+E$98,$M39,$N39,$O39,$P39,$Q39))),0)
def xcf_Toe_Model_I122(): 
    try:      
        return IF(vcell("H122","Toe Model")>0,MIN(IF(vcell("$D122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J66","Toe Model"),CHOOSE(vcell("$D122","Toe Model")+vcell("G$98","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model"),vcell("$H66","Toe Model"),vcell("$I66","Toe Model")))*vcell("H122","Toe Model"),IF(vcell("$D122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R39","Toe Model"),CHOOSE(vcell("$D122","Toe Model")+vcell("G$98","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model"),vcell("$P39","Toe Model"),vcell("$Q39","Toe Model")))-IF(vcell("$D122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R39","Toe Model"),CHOOSE(vcell("$D122","Toe Model")+vcell("E$98","Toe Model"),vcell("$M39","Toe Model"),vcell("$N39","Toe Model"),vcell("$O39","Toe Model"),vcell("$P39","Toe Model"),vcell("$Q39","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I122')
    return None
xfunctions['Toe Model!I122']=xcf_Toe_Model_I122

#H122-I122/IF($B122+$I$98>=6,$H66,CHOOSE($B122+$I$98,$C66,$D66,$E66,$F66,$G66))
def xcf_Toe_Model_J122(): 
    try:      
        return vcell("H122","Toe Model")-vcell("I122","Toe Model")/IF(vcell("$B122","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J122')
    return None
xfunctions['Toe Model!J122']=xcf_Toe_Model_J122

#IF(J122>0,MIN(IF($F122+G$98>=6,$L66,CHOOSE($F122+G$98,$G66,$H66,$I66,$J66,$K66))*J122,IF($F122+G$98>=6,$T39,CHOOSE($F122+G$98,$O39,$P39,$Q39,$R39,$S39))-IF($F122+E$98>=6,$T39,CHOOSE($F122+E$98,$O39,$P39,$Q39,$R39,$S39))),0)
def xcf_Toe_Model_K122(): 
    try:      
        return IF(vcell("J122","Toe Model")>0,MIN(IF(vcell("$F122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L66","Toe Model"),CHOOSE(vcell("$F122","Toe Model")+vcell("G$98","Toe Model"),vcell("$G66","Toe Model"),vcell("$H66","Toe Model"),vcell("$I66","Toe Model"),vcell("$J66","Toe Model"),vcell("$K66","Toe Model")))*vcell("J122","Toe Model"),IF(vcell("$F122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T39","Toe Model"),CHOOSE(vcell("$F122","Toe Model")+vcell("G$98","Toe Model"),vcell("$O39","Toe Model"),vcell("$P39","Toe Model"),vcell("$Q39","Toe Model"),vcell("$R39","Toe Model"),vcell("$S39","Toe Model")))-IF(vcell("$F122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T39","Toe Model"),CHOOSE(vcell("$F122","Toe Model")+vcell("E$98","Toe Model"),vcell("$O39","Toe Model"),vcell("$P39","Toe Model"),vcell("$Q39","Toe Model"),vcell("$R39","Toe Model"),vcell("$S39","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K122')
    return None
xfunctions['Toe Model!K122']=xcf_Toe_Model_K122

#J122-K122/IF($B122+$K$98>=6,$H66,CHOOSE($B122+$K$98,$C66,$D66,$E66,$F66,$G66))
def xcf_Toe_Model_L122(): 
    try:      
        return vcell("J122","Toe Model")-vcell("K122","Toe Model")/IF(vcell("$B122","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L122')
    return None
xfunctions['Toe Model!L122']=xcf_Toe_Model_L122

#IF(L122>0,MIN(IF($H122+G$98>=6,$N66,CHOOSE($H122+G$98,$I66,$J66,$K66,$L66,$M66))*L122,IF($H122+G$98>=6,$V39,CHOOSE($H122+G$98,$Q39,$R39,$S39,$T39,$U39))-IF($H122+E$98>=6,$V39,CHOOSE($H122+E$98,$Q39,$R39,$S39,$T39,$U39))),0)
def xcf_Toe_Model_M122(): 
    try:      
        return IF(vcell("L122","Toe Model")>0,MIN(IF(vcell("$H122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N66","Toe Model"),CHOOSE(vcell("$H122","Toe Model")+vcell("G$98","Toe Model"),vcell("$I66","Toe Model"),vcell("$J66","Toe Model"),vcell("$K66","Toe Model"),vcell("$L66","Toe Model"),vcell("$M66","Toe Model")))*vcell("L122","Toe Model"),IF(vcell("$H122","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V39","Toe Model"),CHOOSE(vcell("$H122","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q39","Toe Model"),vcell("$R39","Toe Model"),vcell("$S39","Toe Model"),vcell("$T39","Toe Model"),vcell("$U39","Toe Model")))-IF(vcell("$H122","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V39","Toe Model"),CHOOSE(vcell("$H122","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q39","Toe Model"),vcell("$R39","Toe Model"),vcell("$S39","Toe Model"),vcell("$T39","Toe Model"),vcell("$U39","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M122')
    return None
xfunctions['Toe Model!M122']=xcf_Toe_Model_M122

#L122-M122/IF($B122+$M$98>=6,$H66,CHOOSE($B122+$M$98,$C66,$D66,$E66,$F66,$G66))
def xcf_Toe_Model_N122(): 
    try:      
        return vcell("L122","Toe Model")-vcell("M122","Toe Model")/IF(vcell("$B122","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H66","Toe Model"),CHOOSE(vcell("$B122","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C66","Toe Model"),vcell("$D66","Toe Model"),vcell("$E66","Toe Model"),vcell("$F66","Toe Model"),vcell("$G66","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N122')
    return None
xfunctions['Toe Model!N122']=xcf_Toe_Model_N122

#B93-SUM($C122,$E122,$G122,$I122,$K122,$M122)*SIN(RADIANS($D39))
def xcf_Toe_Model_O122(): 
    try:      
        return vcell("B93","Toe Model")-SUM(vcell("$C122","Toe Model"),vcell("$E122","Toe Model"),vcell("$G122","Toe Model"),vcell("$I122","Toe Model"),vcell("$K122","Toe Model"),vcell("$M122","Toe Model"))*SIN(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O122')
    return None
xfunctions['Toe Model!O122']=xcf_Toe_Model_O122

#C93+IF($D39==360,1,-1)*SUM($C122,$E122,$G122,$I122,$K122,$M122)*COS(RADIANS($D39))
def xcf_Toe_Model_P122(): 
    try:      
        return vcell("C93","Toe Model")+IF(vcell("$D39","Toe Model")==360,1,-1)*SUM(vcell("$C122","Toe Model"),vcell("$E122","Toe Model"),vcell("$G122","Toe Model"),vcell("$I122","Toe Model"),vcell("$K122","Toe Model"),vcell("$M122","Toe Model"))*COS(RADIANS(vcell("$D39","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P122')
    return None
xfunctions['Toe Model!P122']=xcf_Toe_Model_P122

#IF(MIN(C$18:C$40)>P122,IF(P121>=MIN(C$18:C$40),O121+(O122-O121)*(MIN(C$18:C$40)-P121)/(P122-P121),Q121+ABS(O122-O121)),O122)
def xcf_Toe_Model_Q122(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P122","Toe Model"),IF(vcell("P121","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O121","Toe Model")+(vcell("O122","Toe Model")-vcell("O121","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P121","Toe Model"))/(vcell("P122","Toe Model")-vcell("P121","Toe Model")),vcell("Q121","Toe Model")+ABS(vcell("O122","Toe Model")-vcell("O121","Toe Model"))),vcell("O122","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q122')
    return None
xfunctions['Toe Model!Q122']=xcf_Toe_Model_Q122

#MAX(C93+IF($D39==360,1,-1)*SUM($C122,$E122,$G122,$I122,$K122,$M122)*COS(RADIANS($D39)),MIN(C$18:C$40))
def xcf_Toe_Model_R122(): 
    try:      
        return MAX(vcell("C93","Toe Model")+IF(vcell("$D39","Toe Model")==360,1,-1)*SUM(vcell("$C122","Toe Model"),vcell("$E122","Toe Model"),vcell("$G122","Toe Model"),vcell("$I122","Toe Model"),vcell("$K122","Toe Model"),vcell("$M122","Toe Model"))*COS(RADIANS(vcell("$D39","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R122')
    return None
xfunctions['Toe Model!R122']=xcf_Toe_Model_R122

#IF(AND($C94<=$B$4,$C94>$B$5),1,IF(AND($C94<=$B$5,$C94>$B$6),2,IF(AND($C94<=$B$6,$C94>$B$7),3,IF(AND($C94<=$B$7,$C94>$B$8),4,IF(AND($C94<=$B$8,$C94>$B$9),5,6)))))
def xcf_Toe_Model_B123(): 
    try:      
        return IF(AND(vcell("$C94","Toe Model")<=vcell("$B$4","Toe Model"),vcell("$C94","Toe Model")>vcell("$B$5","Toe Model")),1,IF(AND(vcell("$C94","Toe Model")<=vcell("$B$5","Toe Model"),vcell("$C94","Toe Model")>vcell("$B$6","Toe Model")),2,IF(AND(vcell("$C94","Toe Model")<=vcell("$B$6","Toe Model"),vcell("$C94","Toe Model")>vcell("$B$7","Toe Model")),3,IF(AND(vcell("$C94","Toe Model")<=vcell("$B$7","Toe Model"),vcell("$C94","Toe Model")>vcell("$B$8","Toe Model")),4,IF(AND(vcell("$C94","Toe Model")<=vcell("$B$8","Toe Model"),vcell("$C94","Toe Model")>vcell("$B$9","Toe Model")),5,6)))))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B123')
    return None
xfunctions['Toe Model!B123']=xcf_Toe_Model_B123

#IF(ISNUMBER($B123),MIN(CHOOSE($B123,$K40,$L40,$M40,$N40,$O40,$P40),CHOOSE($B123,$C67,$D67,$E67,$F67,$G67,$H67)*$D94),0)
def xcf_Toe_Model_C123(): 
    try:      
        return IF(ISNUMBER(vcell("$B123","Toe Model")),MIN(CHOOSE(vcell("$B123","Toe Model"),vcell("$K40","Toe Model"),vcell("$L40","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model"),vcell("$P40","Toe Model")),CHOOSE(vcell("$B123","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model"),vcell("$H67","Toe Model"))*vcell("$D94","Toe Model")),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C123')
    return None
xfunctions['Toe Model!C123']=xcf_Toe_Model_C123

#$D94-$C123/CHOOSE($B123,$C67,$D67,$E67,$F67,$G67,$H67)
def xcf_Toe_Model_D123(): 
    try:      
        return vcell("$D94","Toe Model")-vcell("$C123","Toe Model")/CHOOSE(vcell("$B123","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model"),vcell("$H67","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_D123')
    return None
xfunctions['Toe Model!D123']=xcf_Toe_Model_D123

#IF(D123>0,MIN(IF($B123+E$98>=6,$H67,CHOOSE($B123+E$98,$C67,$D67,$E67,$F67,$G67))*D123,IF($B123+E$98>=6,$P40,CHOOSE($B123+E$98,$K40,$L40,$M40,$N40,$O40))-IF($B123>=6,$P40,CHOOSE($B123,$K40,$L40,$M40,$N40,$O40))),0)
def xcf_Toe_Model_E123(): 
    try:      
        return IF(vcell("D123","Toe Model")>0,MIN(IF(vcell("$B123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("E$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))*vcell("D123","Toe Model"),IF(vcell("$B123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P40","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("E$98","Toe Model"),vcell("$K40","Toe Model"),vcell("$L40","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model")))-IF(vcell("$B123","Toe Model")>=6,vcell("$P40","Toe Model"),CHOOSE(vcell("$B123","Toe Model"),vcell("$K40","Toe Model"),vcell("$L40","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_E123')
    return None
xfunctions['Toe Model!E123']=xcf_Toe_Model_E123

#D123-E123/IF($B123+$E$98>=6,$H67,CHOOSE($B123+$E$98,$C67,$D67,$E67,$F67,$G67))
def xcf_Toe_Model_F123(): 
    try:      
        return vcell("D123","Toe Model")-vcell("E123","Toe Model")/IF(vcell("$B123","Toe Model")+vcell("$E$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("$E$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_F123')
    return None
xfunctions['Toe Model!F123']=xcf_Toe_Model_F123

#IF(F123>0,MIN(IF($B123+G$98>=6,$H67,CHOOSE($B123+G$98,$C67,$D67,$E67,$F67,$G67))*F123,IF($B123+G$98>=6,$P40,CHOOSE($B123+G$98,$K40,$L40,$M40,$N40,$O40))-IF($B123+E$98>=6,$P40,CHOOSE($B123+E$98,$K40,$L40,$M40,$N40,$O40))),0)
def xcf_Toe_Model_G123(): 
    try:      
        return IF(vcell("F123","Toe Model")>0,MIN(IF(vcell("$B123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("G$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))*vcell("F123","Toe Model"),IF(vcell("$B123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$P40","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("G$98","Toe Model"),vcell("$K40","Toe Model"),vcell("$L40","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model")))-IF(vcell("$B123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$P40","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("E$98","Toe Model"),vcell("$K40","Toe Model"),vcell("$L40","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_G123')
    return None
xfunctions['Toe Model!G123']=xcf_Toe_Model_G123

#F123-G123/IF($B123+$G$98>=6,$H67,CHOOSE($B123+$G$98,$C67,$D67,$E67,$F67,$G67))
def xcf_Toe_Model_H123(): 
    try:      
        return vcell("F123","Toe Model")-vcell("G123","Toe Model")/IF(vcell("$B123","Toe Model")+vcell("$G$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("$G$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_H123')
    return None
xfunctions['Toe Model!H123']=xcf_Toe_Model_H123

#IF(H123>0,MIN(IF($D123+G$98>=6,$J67,CHOOSE($D123+G$98,$E67,$F67,$G67,$H67,$I67))*H123,IF($D123+G$98>=6,$R40,CHOOSE($D123+G$98,$M40,$N40,$O40,$P40,$Q40))-IF($D123+E$98>=6,$R40,CHOOSE($D123+E$98,$M40,$N40,$O40,$P40,$Q40))),0)
def xcf_Toe_Model_I123(): 
    try:      
        return IF(vcell("H123","Toe Model")>0,MIN(IF(vcell("$D123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$J67","Toe Model"),CHOOSE(vcell("$D123","Toe Model")+vcell("G$98","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model"),vcell("$H67","Toe Model"),vcell("$I67","Toe Model")))*vcell("H123","Toe Model"),IF(vcell("$D123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$R40","Toe Model"),CHOOSE(vcell("$D123","Toe Model")+vcell("G$98","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model"),vcell("$P40","Toe Model"),vcell("$Q40","Toe Model")))-IF(vcell("$D123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$R40","Toe Model"),CHOOSE(vcell("$D123","Toe Model")+vcell("E$98","Toe Model"),vcell("$M40","Toe Model"),vcell("$N40","Toe Model"),vcell("$O40","Toe Model"),vcell("$P40","Toe Model"),vcell("$Q40","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_I123')
    return None
xfunctions['Toe Model!I123']=xcf_Toe_Model_I123

#H123-I123/IF($B123+$I$98>=6,$H67,CHOOSE($B123+$I$98,$C67,$D67,$E67,$F67,$G67))
def xcf_Toe_Model_J123(): 
    try:      
        return vcell("H123","Toe Model")-vcell("I123","Toe Model")/IF(vcell("$B123","Toe Model")+vcell("$I$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("$I$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_J123')
    return None
xfunctions['Toe Model!J123']=xcf_Toe_Model_J123

#IF(J123>0,MIN(IF($F123+G$98>=6,$L67,CHOOSE($F123+G$98,$G67,$H67,$I67,$J67,$K67))*J123,IF($F123+G$98>=6,$T40,CHOOSE($F123+G$98,$O40,$P40,$Q40,$R40,$S40))-IF($F123+E$98>=6,$T40,CHOOSE($F123+E$98,$O40,$P40,$Q40,$R40,$S40))),0)
def xcf_Toe_Model_K123(): 
    try:      
        return IF(vcell("J123","Toe Model")>0,MIN(IF(vcell("$F123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$L67","Toe Model"),CHOOSE(vcell("$F123","Toe Model")+vcell("G$98","Toe Model"),vcell("$G67","Toe Model"),vcell("$H67","Toe Model"),vcell("$I67","Toe Model"),vcell("$J67","Toe Model"),vcell("$K67","Toe Model")))*vcell("J123","Toe Model"),IF(vcell("$F123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$T40","Toe Model"),CHOOSE(vcell("$F123","Toe Model")+vcell("G$98","Toe Model"),vcell("$O40","Toe Model"),vcell("$P40","Toe Model"),vcell("$Q40","Toe Model"),vcell("$R40","Toe Model"),vcell("$S40","Toe Model")))-IF(vcell("$F123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$T40","Toe Model"),CHOOSE(vcell("$F123","Toe Model")+vcell("E$98","Toe Model"),vcell("$O40","Toe Model"),vcell("$P40","Toe Model"),vcell("$Q40","Toe Model"),vcell("$R40","Toe Model"),vcell("$S40","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_K123')
    return None
xfunctions['Toe Model!K123']=xcf_Toe_Model_K123

#J123-K123/IF($B123+$K$98>=6,$H67,CHOOSE($B123+$K$98,$C67,$D67,$E67,$F67,$G67))
def xcf_Toe_Model_L123(): 
    try:      
        return vcell("J123","Toe Model")-vcell("K123","Toe Model")/IF(vcell("$B123","Toe Model")+vcell("$K$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("$K$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_L123')
    return None
xfunctions['Toe Model!L123']=xcf_Toe_Model_L123

#IF(L123>0,MIN(IF($H123+G$98>=6,$N67,CHOOSE($H123+G$98,$I67,$J67,$K67,$L67,$M67))*L123,IF($H123+G$98>=6,$V40,CHOOSE($H123+G$98,$Q40,$R40,$S40,$T40,$U40))-IF($H123+E$98>=6,$V40,CHOOSE($H123+E$98,$Q40,$R40,$S40,$T40,$U40))),0)
def xcf_Toe_Model_M123(): 
    try:      
        return IF(vcell("L123","Toe Model")>0,MIN(IF(vcell("$H123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$N67","Toe Model"),CHOOSE(vcell("$H123","Toe Model")+vcell("G$98","Toe Model"),vcell("$I67","Toe Model"),vcell("$J67","Toe Model"),vcell("$K67","Toe Model"),vcell("$L67","Toe Model"),vcell("$M67","Toe Model")))*vcell("L123","Toe Model"),IF(vcell("$H123","Toe Model")+vcell("G$98","Toe Model")>=6,vcell("$V40","Toe Model"),CHOOSE(vcell("$H123","Toe Model")+vcell("G$98","Toe Model"),vcell("$Q40","Toe Model"),vcell("$R40","Toe Model"),vcell("$S40","Toe Model"),vcell("$T40","Toe Model"),vcell("$U40","Toe Model")))-IF(vcell("$H123","Toe Model")+vcell("E$98","Toe Model")>=6,vcell("$V40","Toe Model"),CHOOSE(vcell("$H123","Toe Model")+vcell("E$98","Toe Model"),vcell("$Q40","Toe Model"),vcell("$R40","Toe Model"),vcell("$S40","Toe Model"),vcell("$T40","Toe Model"),vcell("$U40","Toe Model")))),0)
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_M123')
    return None
xfunctions['Toe Model!M123']=xcf_Toe_Model_M123

#L123-M123/IF($B123+$M$98>=6,$H67,CHOOSE($B123+$M$98,$C67,$D67,$E67,$F67,$G67))
def xcf_Toe_Model_N123(): 
    try:      
        return vcell("L123","Toe Model")-vcell("M123","Toe Model")/IF(vcell("$B123","Toe Model")+vcell("$M$98","Toe Model")>=6,vcell("$H67","Toe Model"),CHOOSE(vcell("$B123","Toe Model")+vcell("$M$98","Toe Model"),vcell("$C67","Toe Model"),vcell("$D67","Toe Model"),vcell("$E67","Toe Model"),vcell("$F67","Toe Model"),vcell("$G67","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_N123')
    return None
xfunctions['Toe Model!N123']=xcf_Toe_Model_N123

#B94-SUM($C123,$E123,$G123,$I123,$K123,$M123)*SIN(RADIANS($D40))
def xcf_Toe_Model_O123(): 
    try:      
        return vcell("B94","Toe Model")-SUM(vcell("$C123","Toe Model"),vcell("$E123","Toe Model"),vcell("$G123","Toe Model"),vcell("$I123","Toe Model"),vcell("$K123","Toe Model"),vcell("$M123","Toe Model"))*SIN(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_O123')
    return None
xfunctions['Toe Model!O123']=xcf_Toe_Model_O123

#C94+IF($D40==360,1,-1)*SUM($C123,$E123,$G123,$I123,$K123,$M123)*COS(RADIANS($D40))
def xcf_Toe_Model_P123(): 
    try:      
        return vcell("C94","Toe Model")+IF(vcell("$D40","Toe Model")==360,1,-1)*SUM(vcell("$C123","Toe Model"),vcell("$E123","Toe Model"),vcell("$G123","Toe Model"),vcell("$I123","Toe Model"),vcell("$K123","Toe Model"),vcell("$M123","Toe Model"))*COS(RADIANS(vcell("$D40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_P123')
    return None
xfunctions['Toe Model!P123']=xcf_Toe_Model_P123

#IF(MIN(C$18:C$40)>P123,IF(P122>=MIN(C$18:C$40),O122+(O123-O122)*(MIN(C$18:C$40)-P122)/(P123-P122),Q122+ABS(O123-O122)),O123)
def xcf_Toe_Model_Q123(): 
    try:      
        return IF(MIN(vcell("C$18:C$40","Toe Model"))>vcell("P123","Toe Model"),IF(vcell("P122","Toe Model")>=MIN(vcell("C$18:C$40","Toe Model")),vcell("O122","Toe Model")+(vcell("O123","Toe Model")-vcell("O122","Toe Model"))*(MIN(vcell("C$18:C$40","Toe Model"))-vcell("P122","Toe Model"))/(vcell("P123","Toe Model")-vcell("P122","Toe Model")),vcell("Q122","Toe Model")+ABS(vcell("O123","Toe Model")-vcell("O122","Toe Model"))),vcell("O123","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_Q123')
    return None
xfunctions['Toe Model!Q123']=xcf_Toe_Model_Q123

#MAX(C94+IF($D40==360,1,-1)*SUM($C123,$E123,$G123,$I123,$K123,$M123)*COS(RADIANS($D40)),MIN(C$18:C$40))
def xcf_Toe_Model_R123(): 
    try:      
        return MAX(vcell("C94","Toe Model")+IF(vcell("$D40","Toe Model")==360,1,-1)*SUM(vcell("$C123","Toe Model"),vcell("$E123","Toe Model"),vcell("$G123","Toe Model"),vcell("$I123","Toe Model"),vcell("$K123","Toe Model"),vcell("$M123","Toe Model"))*COS(RADIANS(vcell("$D40","Toe Model"))),MIN(vcell("C$18:C$40","Toe Model")))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_R123')
    return None
xfunctions['Toe Model!R123']=xcf_Toe_Model_R123

#B18-Q101
def xcf_Toe_Model_B129(): 
    try:      
        return vcell("B18","Toe Model")-vcell("Q101","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B129')
    return None
xfunctions['Toe Model!B129']=xcf_Toe_Model_B129

#C18-R101
def xcf_Toe_Model_C129(): 
    try:      
        return vcell("C18","Toe Model")-vcell("R101","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C129')
    return None
xfunctions['Toe Model!C129']=xcf_Toe_Model_C129

#B19-Q102
def xcf_Toe_Model_B130(): 
    try:      
        return vcell("B19","Toe Model")-vcell("Q102","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B130')
    return None
xfunctions['Toe Model!B130']=xcf_Toe_Model_B130

#C19-R102
def xcf_Toe_Model_C130(): 
    try:      
        return vcell("C19","Toe Model")-vcell("R102","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C130')
    return None
xfunctions['Toe Model!C130']=xcf_Toe_Model_C130

#B20-Q103
def xcf_Toe_Model_B131(): 
    try:      
        return vcell("B20","Toe Model")-vcell("Q103","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B131')
    return None
xfunctions['Toe Model!B131']=xcf_Toe_Model_B131

#C20-R103
def xcf_Toe_Model_C131(): 
    try:      
        return vcell("C20","Toe Model")-vcell("R103","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C131')
    return None
xfunctions['Toe Model!C131']=xcf_Toe_Model_C131

#B21-Q104
def xcf_Toe_Model_B132(): 
    try:      
        return vcell("B21","Toe Model")-vcell("Q104","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B132')
    return None
xfunctions['Toe Model!B132']=xcf_Toe_Model_B132

#C21-R104
def xcf_Toe_Model_C132(): 
    try:      
        return vcell("C21","Toe Model")-vcell("R104","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C132')
    return None
xfunctions['Toe Model!C132']=xcf_Toe_Model_C132

#B22-Q105
def xcf_Toe_Model_B133(): 
    try:      
        return vcell("B22","Toe Model")-vcell("Q105","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B133')
    return None
xfunctions['Toe Model!B133']=xcf_Toe_Model_B133

#C22-R105
def xcf_Toe_Model_C133(): 
    try:      
        return vcell("C22","Toe Model")-vcell("R105","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C133')
    return None
xfunctions['Toe Model!C133']=xcf_Toe_Model_C133

#B23-Q106
def xcf_Toe_Model_B134(): 
    try:      
        return vcell("B23","Toe Model")-vcell("Q106","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B134')
    return None
xfunctions['Toe Model!B134']=xcf_Toe_Model_B134

#C23-R106
def xcf_Toe_Model_C134(): 
    try:      
        return vcell("C23","Toe Model")-vcell("R106","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C134')
    return None
xfunctions['Toe Model!C134']=xcf_Toe_Model_C134

#B24-Q107
def xcf_Toe_Model_B135(): 
    try:      
        return vcell("B24","Toe Model")-vcell("Q107","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B135')
    return None
xfunctions['Toe Model!B135']=xcf_Toe_Model_B135

#C24-R107
def xcf_Toe_Model_C135(): 
    try:      
        return vcell("C24","Toe Model")-vcell("R107","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C135')
    return None
xfunctions['Toe Model!C135']=xcf_Toe_Model_C135

#B25-Q108
def xcf_Toe_Model_B136(): 
    try:      
        return vcell("B25","Toe Model")-vcell("Q108","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B136')
    return None
xfunctions['Toe Model!B136']=xcf_Toe_Model_B136

#C25-R108
def xcf_Toe_Model_C136(): 
    try:      
        return vcell("C25","Toe Model")-vcell("R108","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C136')
    return None
xfunctions['Toe Model!C136']=xcf_Toe_Model_C136

#B26-Q109
def xcf_Toe_Model_B137(): 
    try:      
        return vcell("B26","Toe Model")-vcell("Q109","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B137')
    return None
xfunctions['Toe Model!B137']=xcf_Toe_Model_B137

#C26-R109
def xcf_Toe_Model_C137(): 
    try:      
        return vcell("C26","Toe Model")-vcell("R109","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C137')
    return None
xfunctions['Toe Model!C137']=xcf_Toe_Model_C137

#B27-Q110
def xcf_Toe_Model_B138(): 
    try:      
        return vcell("B27","Toe Model")-vcell("Q110","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B138')
    return None
xfunctions['Toe Model!B138']=xcf_Toe_Model_B138

#C27-R110
def xcf_Toe_Model_C138(): 
    try:      
        return vcell("C27","Toe Model")-vcell("R110","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C138')
    return None
xfunctions['Toe Model!C138']=xcf_Toe_Model_C138

#B28-Q111
def xcf_Toe_Model_B139(): 
    try:      
        return vcell("B28","Toe Model")-vcell("Q111","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B139')
    return None
xfunctions['Toe Model!B139']=xcf_Toe_Model_B139

#C28-R111
def xcf_Toe_Model_C139(): 
    try:      
        return vcell("C28","Toe Model")-vcell("R111","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C139')
    return None
xfunctions['Toe Model!C139']=xcf_Toe_Model_C139

#B29-Q112
def xcf_Toe_Model_B140(): 
    try:      
        return vcell("B29","Toe Model")-vcell("Q112","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B140')
    return None
xfunctions['Toe Model!B140']=xcf_Toe_Model_B140

#C29-R112
def xcf_Toe_Model_C140(): 
    try:      
        return vcell("C29","Toe Model")-vcell("R112","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C140')
    return None
xfunctions['Toe Model!C140']=xcf_Toe_Model_C140

#B30-Q113
def xcf_Toe_Model_B141(): 
    try:      
        return vcell("B30","Toe Model")-vcell("Q113","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B141')
    return None
xfunctions['Toe Model!B141']=xcf_Toe_Model_B141

#C30-R113
def xcf_Toe_Model_C141(): 
    try:      
        return vcell("C30","Toe Model")-vcell("R113","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C141')
    return None
xfunctions['Toe Model!C141']=xcf_Toe_Model_C141

#B31-Q114
def xcf_Toe_Model_B142(): 
    try:      
        return vcell("B31","Toe Model")-vcell("Q114","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B142')
    return None
xfunctions['Toe Model!B142']=xcf_Toe_Model_B142

#C31-R114
def xcf_Toe_Model_C142(): 
    try:      
        return vcell("C31","Toe Model")-vcell("R114","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C142')
    return None
xfunctions['Toe Model!C142']=xcf_Toe_Model_C142

#B32-Q115
def xcf_Toe_Model_B143(): 
    try:      
        return vcell("B32","Toe Model")-vcell("Q115","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B143')
    return None
xfunctions['Toe Model!B143']=xcf_Toe_Model_B143

#C32-R115
def xcf_Toe_Model_C143(): 
    try:      
        return vcell("C32","Toe Model")-vcell("R115","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C143')
    return None
xfunctions['Toe Model!C143']=xcf_Toe_Model_C143

#B33-Q116
def xcf_Toe_Model_B144(): 
    try:      
        return vcell("B33","Toe Model")-vcell("Q116","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B144')
    return None
xfunctions['Toe Model!B144']=xcf_Toe_Model_B144

#C33-R116
def xcf_Toe_Model_C144(): 
    try:      
        return vcell("C33","Toe Model")-vcell("R116","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C144')
    return None
xfunctions['Toe Model!C144']=xcf_Toe_Model_C144

#B34-Q117
def xcf_Toe_Model_B145(): 
    try:      
        return vcell("B34","Toe Model")-vcell("Q117","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B145')
    return None
xfunctions['Toe Model!B145']=xcf_Toe_Model_B145

#C34-R117
def xcf_Toe_Model_C145(): 
    try:      
        return vcell("C34","Toe Model")-vcell("R117","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C145')
    return None
xfunctions['Toe Model!C145']=xcf_Toe_Model_C145

#B35-Q118
def xcf_Toe_Model_B146(): 
    try:      
        return vcell("B35","Toe Model")-vcell("Q118","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B146')
    return None
xfunctions['Toe Model!B146']=xcf_Toe_Model_B146

#C35-R118
def xcf_Toe_Model_C146(): 
    try:      
        return vcell("C35","Toe Model")-vcell("R118","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C146')
    return None
xfunctions['Toe Model!C146']=xcf_Toe_Model_C146

#B36-Q119
def xcf_Toe_Model_B147(): 
    try:      
        return vcell("B36","Toe Model")-vcell("Q119","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B147')
    return None
xfunctions['Toe Model!B147']=xcf_Toe_Model_B147

#C36-R119
def xcf_Toe_Model_C147(): 
    try:      
        return vcell("C36","Toe Model")-vcell("R119","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C147')
    return None
xfunctions['Toe Model!C147']=xcf_Toe_Model_C147

#B37-Q120
def xcf_Toe_Model_B148(): 
    try:      
        return vcell("B37","Toe Model")-vcell("Q120","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B148')
    return None
xfunctions['Toe Model!B148']=xcf_Toe_Model_B148

#C37-R120
def xcf_Toe_Model_C148(): 
    try:      
        return vcell("C37","Toe Model")-vcell("R120","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C148')
    return None
xfunctions['Toe Model!C148']=xcf_Toe_Model_C148

#B38-Q121
def xcf_Toe_Model_B149(): 
    try:      
        return vcell("B38","Toe Model")-vcell("Q121","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B149')
    return None
xfunctions['Toe Model!B149']=xcf_Toe_Model_B149

#C38-R121
def xcf_Toe_Model_C149(): 
    try:      
        return vcell("C38","Toe Model")-vcell("R121","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C149')
    return None
xfunctions['Toe Model!C149']=xcf_Toe_Model_C149

#B39-Q122
def xcf_Toe_Model_B150(): 
    try:      
        return vcell("B39","Toe Model")-vcell("Q122","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B150')
    return None
xfunctions['Toe Model!B150']=xcf_Toe_Model_B150

#C39-R122
def xcf_Toe_Model_C150(): 
    try:      
        return vcell("C39","Toe Model")-vcell("R122","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C150')
    return None
xfunctions['Toe Model!C150']=xcf_Toe_Model_C150

#B40-Q123
def xcf_Toe_Model_B151(): 
    try:      
        return vcell("B40","Toe Model")-vcell("Q123","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_B151')
    return None
xfunctions['Toe Model!B151']=xcf_Toe_Model_B151

#C40-R123
def xcf_Toe_Model_C151(): 
    try:      
        return vcell("C40","Toe Model")-vcell("R123","Toe Model")
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C151')
    return None
xfunctions['Toe Model!C151']=xcf_Toe_Model_C151

#MAX(B129:B151)
def xcf_Toe_Model_C153(): 
    try:      
        return MAX(vcell("B129:B151","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C153')
    return None
xfunctions['Toe Model!C153']=xcf_Toe_Model_C153

#0.5*ABS(B19*C20+B20*C21+B21*C22+B22*C23+B23*C24+B24*C25+B25*C26+B26*C27+B27*C28+B28*C29+B29*C30+B30*C31+B31*C32+B32*C33+B33*C34+B34*R117+Q117*R116+Q116*R115+Q115*R114+Q114*R113+Q113*R112+Q112*R111+Q111*R110+Q110*R109+Q109*R108+Q108*R107+Q107*R106+Q106*R105+Q105*R104+Q104*R103+Q103*R102+Q102*C19-C19*B20-C20*B21-C21*B22-C22*B23-C23*B24-C24*B25-C25*B26-C26*B27-C27*B28-C28*B29-C29*B30-C30*B31-C31*B32-C32*B33-C33*B34-C34*Q117-R117*Q116-R116*Q115-R115*Q114-R114*Q113-R113*Q112-R112*Q111-R111*Q110-R110*Q109-R109*Q108-R108*Q107-R107*Q106-R106*Q105-R105*Q104-R104*Q103-R103*Q102-R102*B19)
def xcf_Toe_Model_C154(): 
    try:      
        return 0.5*ABS(vcell("B19","Toe Model")*vcell("C20","Toe Model")+vcell("B20","Toe Model")*vcell("C21","Toe Model")+vcell("B21","Toe Model")*vcell("C22","Toe Model")+vcell("B22","Toe Model")*vcell("C23","Toe Model")+vcell("B23","Toe Model")*vcell("C24","Toe Model")+vcell("B24","Toe Model")*vcell("C25","Toe Model")+vcell("B25","Toe Model")*vcell("C26","Toe Model")+vcell("B26","Toe Model")*vcell("C27","Toe Model")+vcell("B27","Toe Model")*vcell("C28","Toe Model")+vcell("B28","Toe Model")*vcell("C29","Toe Model")+vcell("B29","Toe Model")*vcell("C30","Toe Model")+vcell("B30","Toe Model")*vcell("C31","Toe Model")+vcell("B31","Toe Model")*vcell("C32","Toe Model")+vcell("B32","Toe Model")*vcell("C33","Toe Model")+vcell("B33","Toe Model")*vcell("C34","Toe Model")+vcell("B34","Toe Model")*vcell("R117","Toe Model")+vcell("Q117","Toe Model")*vcell("R116","Toe Model")+vcell("Q116","Toe Model")*vcell("R115","Toe Model")+vcell("Q115","Toe Model")*vcell("R114","Toe Model")+vcell("Q114","Toe Model")*vcell("R113","Toe Model")+vcell("Q113","Toe Model")*vcell("R112","Toe Model")+vcell("Q112","Toe Model")*vcell("R111","Toe Model")+vcell("Q111","Toe Model")*vcell("R110","Toe Model")+vcell("Q110","Toe Model")*vcell("R109","Toe Model")+vcell("Q109","Toe Model")*vcell("R108","Toe Model")+vcell("Q108","Toe Model")*vcell("R107","Toe Model")+vcell("Q107","Toe Model")*vcell("R106","Toe Model")+vcell("Q106","Toe Model")*vcell("R105","Toe Model")+vcell("Q105","Toe Model")*vcell("R104","Toe Model")+vcell("Q104","Toe Model")*vcell("R103","Toe Model")+vcell("Q103","Toe Model")*vcell("R102","Toe Model")+vcell("Q102","Toe Model")*vcell("C19","Toe Model")-vcell("C19","Toe Model")*vcell("B20","Toe Model")-vcell("C20","Toe Model")*vcell("B21","Toe Model")-vcell("C21","Toe Model")*vcell("B22","Toe Model")-vcell("C22","Toe Model")*vcell("B23","Toe Model")-vcell("C23","Toe Model")*vcell("B24","Toe Model")-vcell("C24","Toe Model")*vcell("B25","Toe Model")-vcell("C25","Toe Model")*vcell("B26","Toe Model")-vcell("C26","Toe Model")*vcell("B27","Toe Model")-vcell("C27","Toe Model")*vcell("B28","Toe Model")-vcell("C28","Toe Model")*vcell("B29","Toe Model")-vcell("C29","Toe Model")*vcell("B30","Toe Model")-vcell("C30","Toe Model")*vcell("B31","Toe Model")-vcell("C31","Toe Model")*vcell("B32","Toe Model")-vcell("C32","Toe Model")*vcell("B33","Toe Model")-vcell("C33","Toe Model")*vcell("B34","Toe Model")-vcell("C34","Toe Model")*vcell("Q117","Toe Model")-vcell("R117","Toe Model")*vcell("Q116","Toe Model")-vcell("R116","Toe Model")*vcell("Q115","Toe Model")-vcell("R115","Toe Model")*vcell("Q114","Toe Model")-vcell("R114","Toe Model")*vcell("Q113","Toe Model")-vcell("R113","Toe Model")*vcell("Q112","Toe Model")-vcell("R112","Toe Model")*vcell("Q111","Toe Model")-vcell("R111","Toe Model")*vcell("Q110","Toe Model")-vcell("R110","Toe Model")*vcell("Q109","Toe Model")-vcell("R109","Toe Model")*vcell("Q108","Toe Model")-vcell("R108","Toe Model")*vcell("Q107","Toe Model")-vcell("R107","Toe Model")*vcell("Q106","Toe Model")-vcell("R106","Toe Model")*vcell("Q105","Toe Model")-vcell("R105","Toe Model")*vcell("Q104","Toe Model")-vcell("R104","Toe Model")*vcell("Q103","Toe Model")-vcell("R103","Toe Model")*vcell("Q102","Toe Model")-vcell("R102","Toe Model")*vcell("B19","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C154')
    return None
xfunctions['Toe Model!C154']=xcf_Toe_Model_C154

#0.5*ABS(B34*C35+B35*C36+B36*C37+B37*C38+B38*C39+B39*R122+Q122*R121+Q121*R120+Q120*R119+Q119*R118+Q118*R117+Q117*C34-C34*B35-C35*B36-C36*B37-C37*B38-C38*B39-C39*Q122-R122*Q121-R121*Q120-R120*Q119-R119*Q118-R118*Q117-R117*B34)
def xcf_Toe_Model_C155(): 
    try:      
        return 0.5*ABS(vcell("B34","Toe Model")*vcell("C35","Toe Model")+vcell("B35","Toe Model")*vcell("C36","Toe Model")+vcell("B36","Toe Model")*vcell("C37","Toe Model")+vcell("B37","Toe Model")*vcell("C38","Toe Model")+vcell("B38","Toe Model")*vcell("C39","Toe Model")+vcell("B39","Toe Model")*vcell("R122","Toe Model")+vcell("Q122","Toe Model")*vcell("R121","Toe Model")+vcell("Q121","Toe Model")*vcell("R120","Toe Model")+vcell("Q120","Toe Model")*vcell("R119","Toe Model")+vcell("Q119","Toe Model")*vcell("R118","Toe Model")+vcell("Q118","Toe Model")*vcell("R117","Toe Model")+vcell("Q117","Toe Model")*vcell("C34","Toe Model")-vcell("C34","Toe Model")*vcell("B35","Toe Model")-vcell("C35","Toe Model")*vcell("B36","Toe Model")-vcell("C36","Toe Model")*vcell("B37","Toe Model")-vcell("C37","Toe Model")*vcell("B38","Toe Model")-vcell("C38","Toe Model")*vcell("B39","Toe Model")-vcell("C39","Toe Model")*vcell("Q122","Toe Model")-vcell("R122","Toe Model")*vcell("Q121","Toe Model")-vcell("R121","Toe Model")*vcell("Q120","Toe Model")-vcell("R120","Toe Model")*vcell("Q119","Toe Model")-vcell("R119","Toe Model")*vcell("Q118","Toe Model")-vcell("R118","Toe Model")*vcell("Q117","Toe Model")-vcell("R117","Toe Model")*vcell("B34","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C155')
    return None
xfunctions['Toe Model!C155']=xcf_Toe_Model_C155

#0.5*ABS(B39*C40+B40*R123+Q123*R122+Q122*C39-C39*B40-C40*Q123-R123*Q122-R122*B39)
def xcf_Toe_Model_C156(): 
    try:      
        return 0.5*ABS(vcell("B39","Toe Model")*vcell("C40","Toe Model")+vcell("B40","Toe Model")*vcell("R123","Toe Model")+vcell("Q123","Toe Model")*vcell("R122","Toe Model")+vcell("Q122","Toe Model")*vcell("C39","Toe Model")-vcell("C39","Toe Model")*vcell("B40","Toe Model")-vcell("C40","Toe Model")*vcell("Q123","Toe Model")-vcell("R123","Toe Model")*vcell("Q122","Toe Model")-vcell("R122","Toe Model")*vcell("B39","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C156')
    return None
xfunctions['Toe Model!C156']=xcf_Toe_Model_C156

#SUM(C154:C156)
def xcf_Toe_Model_C157(): 
    try:      
        return SUM(vcell("C154:C156","Toe Model"))
    except Exception as ex:
        print(ex,'on xcf_Toe_Model_C157')
    return None
xfunctions['Toe Model!C157']=xcf_Toe_Model_C157

#Input_Geometry!E138
def xcf_Calculations_D4(): 
    try:      
        return vcell("Input_Geometry!E138","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D4')
    return None
xfunctions['Calculations!D4']=xcf_Calculations_D4

#Input_Geometry!E140
def xcf_Calculations_D5(): 
    try:      
        return vcell("Input_Geometry!E140","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D5')
    return None
xfunctions['Calculations!D5']=xcf_Calculations_D5

#Input_Geometry!C114
def xcf_Calculations_F5(): 
    try:      
        return vcell("Input_Geometry!C114","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F5')
    return None
xfunctions['Calculations!F5']=xcf_Calculations_F5

#IF(Input_Geometry!E114==Input_Geometry!E$136,Input_Geometry!E114+0.000000001,Input_Geometry!E114)
def xcf_Calculations_G5(): 
    try:      
        return IF(vcell("Input_Geometry!E114","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E114","Calculations")+0.000000001,vcell("Input_Geometry!E114","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G5')
    return None
xfunctions['Calculations!G5']=xcf_Calculations_G5

#F6
def xcf_Calculations_P5(): 
    try:      
        return vcell("F6","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_P5')
    return None
xfunctions['Calculations!P5']=xcf_Calculations_P5

#G6
def xcf_Calculations_Q5(): 
    try:      
        return vcell("G6","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q5')
    return None
xfunctions['Calculations!Q5']=xcf_Calculations_Q5

#Input_Geometry!C115
def xcf_Calculations_F6(): 
    try:      
        return vcell("Input_Geometry!C115","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F6')
    return None
xfunctions['Calculations!F6']=xcf_Calculations_F6

#IF(Input_Geometry!E115==Input_Geometry!E$136,Input_Geometry!E115+0.000000001,Input_Geometry!E115)
def xcf_Calculations_G6(): 
    try:      
        return IF(vcell("Input_Geometry!E115","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E115","Calculations")+0.000000001,vcell("Input_Geometry!E115","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G6')
    return None
xfunctions['Calculations!G6']=xcf_Calculations_G6

#Input_Geometry!E93
def xcf_Calculations_Q6(): 
    try:      
        return vcell("Input_Geometry!E93","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q6')
    return None
xfunctions['Calculations!Q6']=xcf_Calculations_Q6

#Input_Geometry!C116
def xcf_Calculations_F7(): 
    try:      
        return vcell("Input_Geometry!C116","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F7')
    return None
xfunctions['Calculations!F7']=xcf_Calculations_F7

#IF(Input_Geometry!E116==Input_Geometry!E$136,Input_Geometry!E116+0.000000001,Input_Geometry!E116)
def xcf_Calculations_G7(): 
    try:      
        return IF(vcell("Input_Geometry!E116","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E116","Calculations")+0.000000001,vcell("Input_Geometry!E116","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G7')
    return None
xfunctions['Calculations!G7']=xcf_Calculations_G7

#IF(Q5>Q$5-MAX(0.001,$K$4),IF(Q6>Q$5-MAX(0.001,$K$4),(Q5+Q6-2*(Q$5-MAX(0.001,$K$4)))/MAX(0.001,$K$4),(1-($Q$5-Q5)/MAX(0.001,$K$4))*(Q5-(Q$5-MAX(0.001,$K$4)))/(Q5-Q6)),0)
def xcf_Calculations_K7(): 
    try:      
        return IF(vcell("Q5","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),IF(vcell("Q6","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),(vcell("Q5","Calculations")+vcell("Q6","Calculations")-2*(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/MAX(0.001,vcell("$K$4","Calculations")),(1-(vcell("$Q$5","Calculations")-vcell("Q5","Calculations"))/MAX(0.001,vcell("$K$4","Calculations")))*(vcell("Q5","Calculations")-(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/(vcell("Q5","Calculations")-vcell("Q6","Calculations"))),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_K7')
    return None
xfunctions['Calculations!K7']=xcf_Calculations_K7

#$K7*Bank_Model_Output!H$89
def xcf_Calculations_L7(): 
    try:      
        return vcell("$K7","Calculations")*vcell("Bank_Model_Output!H$89","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_L7')
    return None
xfunctions['Calculations!L7']=xcf_Calculations_L7

#Input_Geometry!E94
def xcf_Calculations_Q7(): 
    try:      
        return vcell("Input_Geometry!E94","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q7')
    return None
xfunctions['Calculations!Q7']=xcf_Calculations_Q7

#Input_Geometry!C117
def xcf_Calculations_F8(): 
    try:      
        return vcell("Input_Geometry!C117","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F8')
    return None
xfunctions['Calculations!F8']=xcf_Calculations_F8

#IF(Input_Geometry!E117==Input_Geometry!E$136,Input_Geometry!E117+0.000000001,Input_Geometry!E117)
def xcf_Calculations_G8(): 
    try:      
        return IF(vcell("Input_Geometry!E117","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E117","Calculations")+0.000000001,vcell("Input_Geometry!E117","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G8')
    return None
xfunctions['Calculations!G8']=xcf_Calculations_G8

#IF(Q6>Q$5-MAX(0.001,$K$4),IF(Q7>Q$5-MAX(0.001,$K$4),(Q6+Q7-2*(Q$5-MAX(0.001,$K$4)))/MAX(0.001,$K$4),(1-($Q$5-Q6)/MAX(0.001,$K$4))*(Q6-(Q$5-MAX(0.001,$K$4)))/(Q6-Q7)),0)
def xcf_Calculations_K8(): 
    try:      
        return IF(vcell("Q6","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),IF(vcell("Q7","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),(vcell("Q6","Calculations")+vcell("Q7","Calculations")-2*(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/MAX(0.001,vcell("$K$4","Calculations")),(1-(vcell("$Q$5","Calculations")-vcell("Q6","Calculations"))/MAX(0.001,vcell("$K$4","Calculations")))*(vcell("Q6","Calculations")-(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/(vcell("Q6","Calculations")-vcell("Q7","Calculations"))),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_K8')
    return None
xfunctions['Calculations!K8']=xcf_Calculations_K8

#$K8*Bank_Model_Output!H$89
def xcf_Calculations_L8(): 
    try:      
        return vcell("$K8","Calculations")*vcell("Bank_Model_Output!H$89","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_L8')
    return None
xfunctions['Calculations!L8']=xcf_Calculations_L8

#Input_Geometry!E95
def xcf_Calculations_Q8(): 
    try:      
        return vcell("Input_Geometry!E95","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q8')
    return None
xfunctions['Calculations!Q8']=xcf_Calculations_Q8

#Bank_Model_Output!G84
def xcf_Calculations_D9(): 
    try:      
        return vcell("Bank_Model_Output!G84","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D9')
    return None
xfunctions['Calculations!D9']=xcf_Calculations_D9

#Input_Geometry!C118
def xcf_Calculations_F9(): 
    try:      
        return vcell("Input_Geometry!C118","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F9')
    return None
xfunctions['Calculations!F9']=xcf_Calculations_F9

#IF(Input_Geometry!E118==Input_Geometry!E$136,Input_Geometry!E118+0.000000001,Input_Geometry!E118)
def xcf_Calculations_G9(): 
    try:      
        return IF(vcell("Input_Geometry!E118","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E118","Calculations")+0.000000001,vcell("Input_Geometry!E118","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G9')
    return None
xfunctions['Calculations!G9']=xcf_Calculations_G9

#IF(Q7>Q$5-MAX(0.001,$K$4),IF(Q8>Q$5-MAX(0.001,$K$4),(Q7+Q8-2*(Q$5-MAX(0.001,$K$4)))/MAX(0.001,$K$4),(1-($Q$5-Q7)/MAX(0.001,$K$4))*(Q7-(Q$5-MAX(0.001,$K$4)))/(Q7-Q8)),0)
def xcf_Calculations_K9(): 
    try:      
        return IF(vcell("Q7","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),IF(vcell("Q8","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),(vcell("Q7","Calculations")+vcell("Q8","Calculations")-2*(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/MAX(0.001,vcell("$K$4","Calculations")),(1-(vcell("$Q$5","Calculations")-vcell("Q7","Calculations"))/MAX(0.001,vcell("$K$4","Calculations")))*(vcell("Q7","Calculations")-(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/(vcell("Q7","Calculations")-vcell("Q8","Calculations"))),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_K9')
    return None
xfunctions['Calculations!K9']=xcf_Calculations_K9

#$K9*Bank_Model_Output!H$89
def xcf_Calculations_L9(): 
    try:      
        return vcell("$K9","Calculations")*vcell("Bank_Model_Output!H$89","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_L9')
    return None
xfunctions['Calculations!L9']=xcf_Calculations_L9

#Input_Geometry!E96
def xcf_Calculations_Q9(): 
    try:      
        return vcell("Input_Geometry!E96","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q9')
    return None
xfunctions['Calculations!Q9']=xcf_Calculations_Q9

#Bank_Model_Output!G85
def xcf_Calculations_D10(): 
    try:      
        return vcell("Bank_Model_Output!G85","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D10')
    return None
xfunctions['Calculations!D10']=xcf_Calculations_D10

#Input_Geometry!C119
def xcf_Calculations_F10(): 
    try:      
        return vcell("Input_Geometry!C119","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F10')
    return None
xfunctions['Calculations!F10']=xcf_Calculations_F10

#IF(Input_Geometry!E119==Input_Geometry!E$136,Input_Geometry!E119+0.000000001,Input_Geometry!E119)
def xcf_Calculations_G10(): 
    try:      
        return IF(vcell("Input_Geometry!E119","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E119","Calculations")+0.000000001,vcell("Input_Geometry!E119","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G10')
    return None
xfunctions['Calculations!G10']=xcf_Calculations_G10

#IF(Q8>Q$5-MAX(0.001,$K$4),IF(Q9>Q$5-MAX(0.001,$K$4),(Q8+Q9-2*(Q$5-MAX(0.001,$K$4)))/MAX(0.001,$K$4),(1-($Q$5-Q8)/MAX(0.001,$K$4))*(Q8-(Q$5-MAX(0.001,$K$4)))/(Q8-Q9)),0)
def xcf_Calculations_K10(): 
    try:      
        return IF(vcell("Q8","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),IF(vcell("Q9","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),(vcell("Q8","Calculations")+vcell("Q9","Calculations")-2*(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/MAX(0.001,vcell("$K$4","Calculations")),(1-(vcell("$Q$5","Calculations")-vcell("Q8","Calculations"))/MAX(0.001,vcell("$K$4","Calculations")))*(vcell("Q8","Calculations")-(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/(vcell("Q8","Calculations")-vcell("Q9","Calculations"))),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_K10')
    return None
xfunctions['Calculations!K10']=xcf_Calculations_K10

#$K10*Bank_Model_Output!H$89
def xcf_Calculations_L10(): 
    try:      
        return vcell("$K10","Calculations")*vcell("Bank_Model_Output!H$89","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_L10')
    return None
xfunctions['Calculations!L10']=xcf_Calculations_L10

#Input_Geometry!E97
def xcf_Calculations_Q10(): 
    try:      
        return vcell("Input_Geometry!E97","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q10')
    return None
xfunctions['Calculations!Q10']=xcf_Calculations_Q10

#Bank_Model_Output!G86
def xcf_Calculations_D11(): 
    try:      
        return vcell("Bank_Model_Output!G86","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D11')
    return None
xfunctions['Calculations!D11']=xcf_Calculations_D11

#Input_Geometry!C120
def xcf_Calculations_F11(): 
    try:      
        return vcell("Input_Geometry!C120","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F11')
    return None
xfunctions['Calculations!F11']=xcf_Calculations_F11

#IF(Input_Geometry!E120==Input_Geometry!E$136,Input_Geometry!E120+0.000000001,Input_Geometry!E120)
def xcf_Calculations_G11(): 
    try:      
        return IF(vcell("Input_Geometry!E120","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E120","Calculations")+0.000000001,vcell("Input_Geometry!E120","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G11')
    return None
xfunctions['Calculations!G11']=xcf_Calculations_G11

#IF(Q9>Q$5-MAX(0.001,$K$4),IF(Q10>Q$5-MAX(0.001,$K$4),(Q9+MIN(Q10,Q$5-MAX(0.001,$K$4))-2*(Q$5-MAX(0.001,$K$4)))/MAX(0.001,$K$4),(1-($Q$5-Q9)/MAX(0.001,$K$4))*(Q9-(Q$5-MAX(0.001,$K$4)))/(Q9-Q10)),0)
def xcf_Calculations_K11(): 
    try:      
        return IF(vcell("Q9","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),IF(vcell("Q10","Calculations")>vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")),(vcell("Q9","Calculations")+MIN(vcell("Q10","Calculations"),vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations")))-2*(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/MAX(0.001,vcell("$K$4","Calculations")),(1-(vcell("$Q$5","Calculations")-vcell("Q9","Calculations"))/MAX(0.001,vcell("$K$4","Calculations")))*(vcell("Q9","Calculations")-(vcell("Q$5","Calculations")-MAX(0.001,vcell("$K$4","Calculations"))))/(vcell("Q9","Calculations")-vcell("Q10","Calculations"))),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_K11')
    return None
xfunctions['Calculations!K11']=xcf_Calculations_K11

#$K11*Bank_Model_Output!H$89
def xcf_Calculations_L11(): 
    try:      
        return vcell("$K11","Calculations")*vcell("Bank_Model_Output!H$89","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_L11')
    return None
xfunctions['Calculations!L11']=xcf_Calculations_L11

#Bank_Model_Output!G87
def xcf_Calculations_D12(): 
    try:      
        return vcell("Bank_Model_Output!G87","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D12')
    return None
xfunctions['Calculations!D12']=xcf_Calculations_D12

#Input_Geometry!C121
def xcf_Calculations_F12(): 
    try:      
        return vcell("Input_Geometry!C121","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F12')
    return None
xfunctions['Calculations!F12']=xcf_Calculations_F12

#IF(Input_Geometry!E121==Input_Geometry!E$136,Input_Geometry!E121+0.000000001,Input_Geometry!E121)
def xcf_Calculations_G12(): 
    try:      
        return IF(vcell("Input_Geometry!E121","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E121","Calculations")+0.000000001,vcell("Input_Geometry!E121","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G12')
    return None
xfunctions['Calculations!G12']=xcf_Calculations_G12

#Bank_Model_Output!G88
def xcf_Calculations_D13(): 
    try:      
        return vcell("Bank_Model_Output!G88","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D13')
    return None
xfunctions['Calculations!D13']=xcf_Calculations_D13

#Input_Geometry!C122
def xcf_Calculations_F13(): 
    try:      
        return vcell("Input_Geometry!C122","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F13')
    return None
xfunctions['Calculations!F13']=xcf_Calculations_F13

#IF(Input_Geometry!E122==Input_Geometry!E$136,Input_Geometry!E122+0.000000001,Input_Geometry!E122)
def xcf_Calculations_G13(): 
    try:      
        return IF(vcell("Input_Geometry!E122","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E122","Calculations")+0.000000001,vcell("Input_Geometry!E122","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G13')
    return None
xfunctions['Calculations!G13']=xcf_Calculations_G13

#Bank_Model_Output!H84+L7
def xcf_Calculations_D14(): 
    try:      
        return vcell("Bank_Model_Output!H84","Calculations")+vcell("L7","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D14')
    return None
xfunctions['Calculations!D14']=xcf_Calculations_D14

#Input_Geometry!C123
def xcf_Calculations_F14(): 
    try:      
        return vcell("Input_Geometry!C123","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F14')
    return None
xfunctions['Calculations!F14']=xcf_Calculations_F14

#IF(Input_Geometry!E123==Input_Geometry!E$136,Input_Geometry!E123+0.000000001,Input_Geometry!E123)
def xcf_Calculations_G14(): 
    try:      
        return IF(vcell("Input_Geometry!E123","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E123","Calculations")+0.000000001,vcell("Input_Geometry!E123","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G14')
    return None
xfunctions['Calculations!G14']=xcf_Calculations_G14

#Bank_Model_Output!H85+L8
def xcf_Calculations_D15(): 
    try:      
        return vcell("Bank_Model_Output!H85","Calculations")+vcell("L8","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D15')
    return None
xfunctions['Calculations!D15']=xcf_Calculations_D15

#Input_Geometry!C124
def xcf_Calculations_F15(): 
    try:      
        return vcell("Input_Geometry!C124","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F15')
    return None
xfunctions['Calculations!F15']=xcf_Calculations_F15

#IF(Input_Geometry!E124==Input_Geometry!E$136,Input_Geometry!E124+0.000000001,Input_Geometry!E124)
def xcf_Calculations_G15(): 
    try:      
        return IF(vcell("Input_Geometry!E124","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E124","Calculations")+0.000000001,vcell("Input_Geometry!E124","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G15')
    return None
xfunctions['Calculations!G15']=xcf_Calculations_G15

#Bank_Model_Output!H86+L9
def xcf_Calculations_D16(): 
    try:      
        return vcell("Bank_Model_Output!H86","Calculations")+vcell("L9","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D16')
    return None
xfunctions['Calculations!D16']=xcf_Calculations_D16

#Input_Geometry!C125
def xcf_Calculations_F16(): 
    try:      
        return vcell("Input_Geometry!C125","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F16')
    return None
xfunctions['Calculations!F16']=xcf_Calculations_F16

#IF(Input_Geometry!E125==Input_Geometry!E$136,Input_Geometry!E125+0.000000001,Input_Geometry!E125)
def xcf_Calculations_G16(): 
    try:      
        return IF(vcell("Input_Geometry!E125","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E125","Calculations")+0.000000001,vcell("Input_Geometry!E125","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G16')
    return None
xfunctions['Calculations!G16']=xcf_Calculations_G16

#Bank_Model_Output!H87+L10
def xcf_Calculations_D17(): 
    try:      
        return vcell("Bank_Model_Output!H87","Calculations")+vcell("L10","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D17')
    return None
xfunctions['Calculations!D17']=xcf_Calculations_D17

#Input_Geometry!C126
def xcf_Calculations_F17(): 
    try:      
        return vcell("Input_Geometry!C126","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F17')
    return None
xfunctions['Calculations!F17']=xcf_Calculations_F17

#IF(Input_Geometry!E126==Input_Geometry!E$136,Input_Geometry!E126+0.000000001,Input_Geometry!E126)
def xcf_Calculations_G17(): 
    try:      
        return IF(vcell("Input_Geometry!E126","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E126","Calculations")+0.000000001,vcell("Input_Geometry!E126","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G17')
    return None
xfunctions['Calculations!G17']=xcf_Calculations_G17

#Bank_Model_Output!H88+L11
def xcf_Calculations_D18(): 
    try:      
        return vcell("Bank_Model_Output!H88","Calculations")+vcell("L11","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D18')
    return None
xfunctions['Calculations!D18']=xcf_Calculations_D18

#Input_Geometry!C127
def xcf_Calculations_F18(): 
    try:      
        return vcell("Input_Geometry!C127","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F18')
    return None
xfunctions['Calculations!F18']=xcf_Calculations_F18

#IF(Input_Geometry!E127==Input_Geometry!E$136,Input_Geometry!E127+0.000000001,Input_Geometry!E127)
def xcf_Calculations_G18(): 
    try:      
        return IF(vcell("Input_Geometry!E127","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E127","Calculations")+0.000000001,vcell("Input_Geometry!E127","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G18')
    return None
xfunctions['Calculations!G18']=xcf_Calculations_G18

#Bank_Model_Output!I84
def xcf_Calculations_D19(): 
    try:      
        return vcell("Bank_Model_Output!I84","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D19')
    return None
xfunctions['Calculations!D19']=xcf_Calculations_D19

#Input_Geometry!C128
def xcf_Calculations_F19(): 
    try:      
        return vcell("Input_Geometry!C128","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F19')
    return None
xfunctions['Calculations!F19']=xcf_Calculations_F19

#IF(Input_Geometry!E128==Input_Geometry!E$136,Input_Geometry!E128+0.000000001,Input_Geometry!E128)
def xcf_Calculations_G19(): 
    try:      
        return IF(vcell("Input_Geometry!E128","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E128","Calculations")+0.000000001,vcell("Input_Geometry!E128","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G19')
    return None
xfunctions['Calculations!G19']=xcf_Calculations_G19

#Bank_Model_Output!I85
def xcf_Calculations_D20(): 
    try:      
        return vcell("Bank_Model_Output!I85","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D20')
    return None
xfunctions['Calculations!D20']=xcf_Calculations_D20

#Input_Geometry!C129
def xcf_Calculations_F20(): 
    try:      
        return vcell("Input_Geometry!C129","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F20')
    return None
xfunctions['Calculations!F20']=xcf_Calculations_F20

#IF(Input_Geometry!E129==Input_Geometry!E$136,Input_Geometry!E129+0.000000001,Input_Geometry!E129)
def xcf_Calculations_G20(): 
    try:      
        return IF(vcell("Input_Geometry!E129","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E129","Calculations")+0.000000001,vcell("Input_Geometry!E129","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G20')
    return None
xfunctions['Calculations!G20']=xcf_Calculations_G20

#Bank_Model_Output!I86
def xcf_Calculations_D21(): 
    try:      
        return vcell("Bank_Model_Output!I86","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D21')
    return None
xfunctions['Calculations!D21']=xcf_Calculations_D21

#Input_Geometry!C130
def xcf_Calculations_F21(): 
    try:      
        return vcell("Input_Geometry!C130","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F21')
    return None
xfunctions['Calculations!F21']=xcf_Calculations_F21

#IF(Input_Geometry!E130==Input_Geometry!E$136,Input_Geometry!E130+0.000000001,Input_Geometry!E130)
def xcf_Calculations_G21(): 
    try:      
        return IF(vcell("Input_Geometry!E130","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E130","Calculations")+0.000000001,vcell("Input_Geometry!E130","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G21')
    return None
xfunctions['Calculations!G21']=xcf_Calculations_G21

#Bank_Model_Output!I87
def xcf_Calculations_D22(): 
    try:      
        return vcell("Bank_Model_Output!I87","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D22')
    return None
xfunctions['Calculations!D22']=xcf_Calculations_D22

#Input_Geometry!C131
def xcf_Calculations_F22(): 
    try:      
        return vcell("Input_Geometry!C131","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F22')
    return None
xfunctions['Calculations!F22']=xcf_Calculations_F22

#IF(Input_Geometry!E131==Input_Geometry!E$136,Input_Geometry!E131+0.000000001,Input_Geometry!E131)
def xcf_Calculations_G22(): 
    try:      
        return IF(vcell("Input_Geometry!E131","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E131","Calculations")+0.000000001,vcell("Input_Geometry!E131","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G22')
    return None
xfunctions['Calculations!G22']=xcf_Calculations_G22

#$P$15-(Q22-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_P22(): 
    try:      
        return vcell("$P$15","Calculations")-(vcell("Q22","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_P22')
    return None
xfunctions['Calculations!P22']=xcf_Calculations_P22

#Q16
def xcf_Calculations_Q22(): 
    try:      
        return vcell("Q16","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q22')
    return None
xfunctions['Calculations!Q22']=xcf_Calculations_Q22

#Bank_Model_Output!I88
def xcf_Calculations_D23(): 
    try:      
        return vcell("Bank_Model_Output!I88","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D23')
    return None
xfunctions['Calculations!D23']=xcf_Calculations_D23

#Input_Geometry!C132
def xcf_Calculations_F23(): 
    try:      
        return vcell("Input_Geometry!C132","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F23')
    return None
xfunctions['Calculations!F23']=xcf_Calculations_F23

#IF(Input_Geometry!E132==Input_Geometry!E$136,Input_Geometry!E132+0.000000001,Input_Geometry!E132)
def xcf_Calculations_G23(): 
    try:      
        return IF(vcell("Input_Geometry!E132","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E132","Calculations")+0.000000001,vcell("Input_Geometry!E132","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G23')
    return None
xfunctions['Calculations!G23']=xcf_Calculations_G23

#IF(Q23+0.000001>=$D$4,$P$15-(Q23-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140)),"")
def xcf_Calculations_P23(): 
    try:      
        return IF(vcell("Q23","Calculations")+0.000001>=vcell("$D$4","Calculations"),vcell("$P$15","Calculations")-(vcell("Q23","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),"")
    except Exception as ex:
        print(ex,'on xcf_Calculations_P23')
    return None
xfunctions['Calculations!P23']=xcf_Calculations_P23

#D30
def xcf_Calculations_Q23(): 
    try:      
        return vcell("D30","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q23')
    return None
xfunctions['Calculations!Q23']=xcf_Calculations_Q23

#Bank_Model_Output!F84
def xcf_Calculations_D24(): 
    try:      
        return vcell("Bank_Model_Output!F84","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D24')
    return None
xfunctions['Calculations!D24']=xcf_Calculations_D24

#Input_Geometry!C133
def xcf_Calculations_F24(): 
    try:      
        return vcell("Input_Geometry!C133","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F24')
    return None
xfunctions['Calculations!F24']=xcf_Calculations_F24

#IF(Input_Geometry!E133==Input_Geometry!E$136,Input_Geometry!E133+0.000000001,Input_Geometry!E133)
def xcf_Calculations_G24(): 
    try:      
        return IF(vcell("Input_Geometry!E133","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E133","Calculations")+0.000000001,vcell("Input_Geometry!E133","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G24')
    return None
xfunctions['Calculations!G24']=xcf_Calculations_G24

#IF(Q24+0.000001>=$D$4,$P$15-(Q24-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140)),"")
def xcf_Calculations_P24(): 
    try:      
        return IF(vcell("Q24","Calculations")+0.000001>=vcell("$D$4","Calculations"),vcell("$P$15","Calculations")-(vcell("Q24","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),"")
    except Exception as ex:
        print(ex,'on xcf_Calculations_P24')
    return None
xfunctions['Calculations!P24']=xcf_Calculations_P24

#D31
def xcf_Calculations_Q24(): 
    try:      
        return vcell("D31","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q24')
    return None
xfunctions['Calculations!Q24']=xcf_Calculations_Q24

#Bank_Model_Output!F85
def xcf_Calculations_D25(): 
    try:      
        return vcell("Bank_Model_Output!F85","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D25')
    return None
xfunctions['Calculations!D25']=xcf_Calculations_D25

#Input_Geometry!C134
def xcf_Calculations_F25(): 
    try:      
        return vcell("Input_Geometry!C134","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F25')
    return None
xfunctions['Calculations!F25']=xcf_Calculations_F25

#IF(Input_Geometry!E134==Input_Geometry!E$136,Input_Geometry!E134+0.000000001,Input_Geometry!E134)
def xcf_Calculations_G25(): 
    try:      
        return IF(vcell("Input_Geometry!E134","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E134","Calculations")+0.000000001,vcell("Input_Geometry!E134","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G25')
    return None
xfunctions['Calculations!G25']=xcf_Calculations_G25

#IF(Q25+0.000001>=$D$4,$P$15-(Q25-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140)),"")
def xcf_Calculations_P25(): 
    try:      
        return IF(vcell("Q25","Calculations")+0.000001>=vcell("$D$4","Calculations"),vcell("$P$15","Calculations")-(vcell("Q25","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),"")
    except Exception as ex:
        print(ex,'on xcf_Calculations_P25')
    return None
xfunctions['Calculations!P25']=xcf_Calculations_P25

#D32
def xcf_Calculations_Q25(): 
    try:      
        return vcell("D32","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q25')
    return None
xfunctions['Calculations!Q25']=xcf_Calculations_Q25

#Bank_Model_Output!F86
def xcf_Calculations_D26(): 
    try:      
        return vcell("Bank_Model_Output!F86","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D26')
    return None
xfunctions['Calculations!D26']=xcf_Calculations_D26

#Input_Geometry!C135
def xcf_Calculations_F26(): 
    try:      
        return vcell("Input_Geometry!C135","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F26')
    return None
xfunctions['Calculations!F26']=xcf_Calculations_F26

#IF(Input_Geometry!E135==Input_Geometry!E$136,Input_Geometry!E135+0.000000001,Input_Geometry!E135)
def xcf_Calculations_G26(): 
    try:      
        return IF(vcell("Input_Geometry!E135","Calculations")==vcell("Input_Geometry!E$136","Calculations"),vcell("Input_Geometry!E135","Calculations")+0.000000001,vcell("Input_Geometry!E135","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_G26')
    return None
xfunctions['Calculations!G26']=xcf_Calculations_G26

#IF(Q26+0.000001>=$D$4,$P$15-(Q26-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140)),"")
def xcf_Calculations_P26(): 
    try:      
        return IF(vcell("Q26","Calculations")+0.000001>=vcell("$D$4","Calculations"),vcell("$P$15","Calculations")-(vcell("Q26","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),"")
    except Exception as ex:
        print(ex,'on xcf_Calculations_P26')
    return None
xfunctions['Calculations!P26']=xcf_Calculations_P26

#D33
def xcf_Calculations_Q26(): 
    try:      
        return vcell("D33","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q26')
    return None
xfunctions['Calculations!Q26']=xcf_Calculations_Q26

#Bank_Model_Output!F87
def xcf_Calculations_D27(): 
    try:      
        return vcell("Bank_Model_Output!F87","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D27')
    return None
xfunctions['Calculations!D27']=xcf_Calculations_D27

#Input_Geometry!C136
def xcf_Calculations_F27(): 
    try:      
        return vcell("Input_Geometry!C136","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_F27')
    return None
xfunctions['Calculations!F27']=xcf_Calculations_F27

#Input_Geometry!E136
def xcf_Calculations_G27(): 
    try:      
        return vcell("Input_Geometry!E136","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_G27')
    return None
xfunctions['Calculations!G27']=xcf_Calculations_G27

#Bank_Model_Output!F88
def xcf_Calculations_D28(): 
    try:      
        return vcell("Bank_Model_Output!F88","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D28')
    return None
xfunctions['Calculations!D28']=xcf_Calculations_D28

#Input_Geometry!E92
def xcf_Calculations_D29(): 
    try:      
        return vcell("Input_Geometry!E92","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D29')
    return None
xfunctions['Calculations!D29']=xcf_Calculations_D29

#D29-D30
def xcf_Calculations_E29(): 
    try:      
        return vcell("D29","Calculations")-vcell("D30","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_E29')
    return None
xfunctions['Calculations!E29']=xcf_Calculations_E29

#Input_Geometry!E93
def xcf_Calculations_D30(): 
    try:      
        return vcell("Input_Geometry!E93","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D30')
    return None
xfunctions['Calculations!D30']=xcf_Calculations_D30

#D30-D31
def xcf_Calculations_E30(): 
    try:      
        return vcell("D30","Calculations")-vcell("D31","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_E30')
    return None
xfunctions['Calculations!E30']=xcf_Calculations_E30

#Input_Geometry!E94
def xcf_Calculations_D31(): 
    try:      
        return vcell("Input_Geometry!E94","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D31')
    return None
xfunctions['Calculations!D31']=xcf_Calculations_D31

#D31-D32
def xcf_Calculations_E31(): 
    try:      
        return vcell("D31","Calculations")-vcell("D32","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_E31')
    return None
xfunctions['Calculations!E31']=xcf_Calculations_E31

#Input_Geometry!E95
def xcf_Calculations_D32(): 
    try:      
        return vcell("Input_Geometry!E95","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D32')
    return None
xfunctions['Calculations!D32']=xcf_Calculations_D32

#D32-D33
def xcf_Calculations_E32(): 
    try:      
        return vcell("D32","Calculations")-vcell("D33","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_E32')
    return None
xfunctions['Calculations!E32']=xcf_Calculations_E32

#Input_Geometry!E96
def xcf_Calculations_D33(): 
    try:      
        return vcell("Input_Geometry!E96","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_D33')
    return None
xfunctions['Calculations!D33']=xcf_Calculations_D33

#D33-D35
def xcf_Calculations_E33(): 
    try:      
        return vcell("D33","Calculations")-vcell("D35","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_E33')
    return None
xfunctions['Calculations!E33']=xcf_Calculations_E33

#1000*D36*B73/9.807
def xcf_Calculations_E36(): 
    try:      
        return 1000*vcell("D36","Calculations")*vcell("B73","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_E36')
    return None
xfunctions['Calculations!E36']=xcf_Calculations_E36

#1000*$D24*$D36*Bank_Model_Output!J84/9.807
def xcf_Calculations_F36(): 
    try:      
        return 1000*vcell("$D24","Calculations")*vcell("$D36","Calculations")*vcell("Bank_Model_Output!J84","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_F36')
    return None
xfunctions['Calculations!F36']=xcf_Calculations_F36

#1000*D37*C73/9.807
def xcf_Calculations_E37(): 
    try:      
        return 1000*vcell("D37","Calculations")*vcell("C73","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_E37')
    return None
xfunctions['Calculations!E37']=xcf_Calculations_E37

#1000*$D25*$D37*Bank_Model_Output!J85/9.807
def xcf_Calculations_F37(): 
    try:      
        return 1000*vcell("$D25","Calculations")*vcell("$D37","Calculations")*vcell("Bank_Model_Output!J85","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_F37')
    return None
xfunctions['Calculations!F37']=xcf_Calculations_F37

#IF($P$15-(Q37-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140))>P15,P15,$P$15-(Q37-$Q$15)/TAN(RADIANS(Input_Geometry!$E$140)))
def xcf_Calculations_P37(): 
    try:      
        return IF(vcell("$P$15","Calculations")-(vcell("Q37","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))>vcell("P15","Calculations"),vcell("P15","Calculations"),vcell("$P$15","Calculations")-(vcell("Q37","Calculations")-vcell("$Q$15","Calculations"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_P37')
    return None
xfunctions['Calculations!P37']=xcf_Calculations_P37

#Input_Geometry!B92
def xcf_Calculations_Q37(): 
    try:      
        return vcell("Input_Geometry!B92","Calculations")
    except Exception as ex:
        print(ex,'on xcf_Calculations_Q37')
    return None
xfunctions['Calculations!Q37']=xcf_Calculations_Q37

#1000*D38*D73/9.807
def xcf_Calculations_E38(): 
    try:      
        return 1000*vcell("D38","Calculations")*vcell("D73","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_E38')
    return None
xfunctions['Calculations!E38']=xcf_Calculations_E38

#1000*$D26*$D38*Bank_Model_Output!J86/9.807
def xcf_Calculations_F38(): 
    try:      
        return 1000*vcell("$D26","Calculations")*vcell("$D38","Calculations")*vcell("Bank_Model_Output!J86","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_F38')
    return None
xfunctions['Calculations!F38']=xcf_Calculations_F38

#1000*D39*E73/9.807
def xcf_Calculations_E39(): 
    try:      
        return 1000*vcell("D39","Calculations")*vcell("E73","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_E39')
    return None
xfunctions['Calculations!E39']=xcf_Calculations_E39

#1000*$D27*$D39*Bank_Model_Output!J87/9.807
def xcf_Calculations_F39(): 
    try:      
        return 1000*vcell("$D27","Calculations")*vcell("$D39","Calculations")*vcell("Bank_Model_Output!J87","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_F39')
    return None
xfunctions['Calculations!F39']=xcf_Calculations_F39

#1000*D40*F73/9.807
def xcf_Calculations_E40(): 
    try:      
        return 1000*vcell("D40","Calculations")*vcell("F73","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_E40')
    return None
xfunctions['Calculations!E40']=xcf_Calculations_E40

#1000*$D28*$D40*Bank_Model_Output!J88/9.807
def xcf_Calculations_F40(): 
    try:      
        return 1000*vcell("$D28","Calculations")*vcell("$D40","Calculations")*vcell("Bank_Model_Output!J88","Calculations")/9.807
    except Exception as ex:
        print(ex,'on xcf_Calculations_F40')
    return None
xfunctions['Calculations!F40']=xcf_Calculations_F40

#SUM(D36:D40)
def xcf_Calculations_D41(): 
    try:      
        return SUM(vcell("D36:D40","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D41')
    return None
xfunctions['Calculations!D41']=xcf_Calculations_D41

#SUM(E36:E40)
def xcf_Calculations_E41(): 
    try:      
        return SUM(vcell("E36:E40","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E41')
    return None
xfunctions['Calculations!E41']=xcf_Calculations_E41

#SUM(F36:F40)
def xcf_Calculations_F41(): 
    try:      
        return SUM(vcell("F36:F40","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F41')
    return None
xfunctions['Calculations!F41']=xcf_Calculations_F41

#IF($Q$16>=Q5,IF(Q6>$Q$15,(Q5-Q6)/SIN(RADIANS(Input_Geometry!$E$140)),(Q5-$Q$15)/SIN(RADIANS(Input_Geometry!$E$140))),IF(Q6>$Q$16,0,($Q$16-Q6)/SIN(RADIANS(Input_Geometry!$E$140))))
def xcf_Calculations_D44(): 
    try:      
        return IF(vcell("$Q$16","Calculations")>=vcell("Q5","Calculations"),IF(vcell("Q6","Calculations")>vcell("$Q$15","Calculations"),(vcell("Q5","Calculations")-vcell("Q6","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),(vcell("Q5","Calculations")-vcell("$Q$15","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))),IF(vcell("Q6","Calculations")>vcell("$Q$16","Calculations"),0,(vcell("$Q$16","Calculations")-vcell("Q6","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D44')
    return None
xfunctions['Calculations!D44']=xcf_Calculations_D44

#IF($Q$16>=Q6,IF(Q7>$Q$15,(Q6-Q7)/SIN(RADIANS(Input_Geometry!$E$140)),(Q6-$Q$15)/SIN(RADIANS(Input_Geometry!$E$140))),IF(Q7>$Q$16,0,($Q$16-Q7)/SIN(RADIANS(Input_Geometry!$E$140))))
def xcf_Calculations_D45(): 
    try:      
        return IF(vcell("$Q$16","Calculations")>=vcell("Q6","Calculations"),IF(vcell("Q7","Calculations")>vcell("$Q$15","Calculations"),(vcell("Q6","Calculations")-vcell("Q7","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),(vcell("Q6","Calculations")-vcell("$Q$15","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))),IF(vcell("Q7","Calculations")>vcell("$Q$16","Calculations"),0,(vcell("$Q$16","Calculations")-vcell("Q7","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D45')
    return None
xfunctions['Calculations!D45']=xcf_Calculations_D45

#IF($Q$16>=Q7,IF(Q8>$Q$15,(Q7-Q8)/SIN(RADIANS(Input_Geometry!$E$140)),(Q7-$Q$15)/SIN(RADIANS(Input_Geometry!$E$140))),IF(Q8>$Q$16,0,($Q$16-Q8)/SIN(RADIANS(Input_Geometry!$E$140))))
def xcf_Calculations_D46(): 
    try:      
        return IF(vcell("$Q$16","Calculations")>=vcell("Q7","Calculations"),IF(vcell("Q8","Calculations")>vcell("$Q$15","Calculations"),(vcell("Q7","Calculations")-vcell("Q8","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),(vcell("Q7","Calculations")-vcell("$Q$15","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))),IF(vcell("Q8","Calculations")>vcell("$Q$16","Calculations"),0,(vcell("$Q$16","Calculations")-vcell("Q8","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D46')
    return None
xfunctions['Calculations!D46']=xcf_Calculations_D46

#IF($Q$16>=Q8,IF(Q9>$Q$15,(Q8-Q9)/SIN(RADIANS(Input_Geometry!$E$140)),(Q8-$Q$15)/SIN(RADIANS(Input_Geometry!$E$140))),IF(Q9>$Q$16,0,($Q$16-Q9)/SIN(RADIANS(Input_Geometry!$E$140))))
def xcf_Calculations_D47(): 
    try:      
        return IF(vcell("$Q$16","Calculations")>=vcell("Q8","Calculations"),IF(vcell("Q9","Calculations")>vcell("$Q$15","Calculations"),(vcell("Q8","Calculations")-vcell("Q9","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),(vcell("Q8","Calculations")-vcell("$Q$15","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))),IF(vcell("Q9","Calculations")>vcell("$Q$16","Calculations"),0,(vcell("$Q$16","Calculations")-vcell("Q9","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D47')
    return None
xfunctions['Calculations!D47']=xcf_Calculations_D47

#IF($Q$16>=Q9,IF(Q10>$Q$15,(Q9-Q10)/SIN(RADIANS(Input_Geometry!$E$140)),(Q9-$Q$15)/SIN(RADIANS(Input_Geometry!$E$140))),IF(Q10>$Q$16,0,($Q$16-Q10)/SIN(RADIANS(Input_Geometry!$E$140))))
def xcf_Calculations_D48(): 
    try:      
        return IF(vcell("$Q$16","Calculations")>=vcell("Q9","Calculations"),IF(vcell("Q10","Calculations")>vcell("$Q$15","Calculations"),(vcell("Q9","Calculations")-vcell("Q10","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations"))),(vcell("Q9","Calculations")-vcell("$Q$15","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))),IF(vcell("Q10","Calculations")>vcell("$Q$16","Calculations"),0,(vcell("$Q$16","Calculations")-vcell("Q10","Calculations"))/SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D48')
    return None
xfunctions['Calculations!D48']=xcf_Calculations_D48

#SUM(D44:D48)
def xcf_Calculations_D49(): 
    try:      
        return SUM(vcell("D44:D48","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D49')
    return None
xfunctions['Calculations!D49']=xcf_Calculations_D49

#MAX(D14*D44,0)
def xcf_Calculations_D52(): 
    try:      
        return MAX(vcell("D14","Calculations")*vcell("D44","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D52')
    return None
xfunctions['Calculations!D52']=xcf_Calculations_D52

#MAX(D15*D45,0)
def xcf_Calculations_D53(): 
    try:      
        return MAX(vcell("D15","Calculations")*vcell("D45","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D53')
    return None
xfunctions['Calculations!D53']=xcf_Calculations_D53

#MAX(D16*D46,0)
def xcf_Calculations_D54(): 
    try:      
        return MAX(vcell("D16","Calculations")*vcell("D46","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D54')
    return None
xfunctions['Calculations!D54']=xcf_Calculations_D54

#MAX(D17*D47,0)
def xcf_Calculations_D55(): 
    try:      
        return MAX(vcell("D17","Calculations")*vcell("D47","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D55')
    return None
xfunctions['Calculations!D55']=xcf_Calculations_D55

#MAX(D18*D48,0)
def xcf_Calculations_D56(): 
    try:      
        return MAX(vcell("D18","Calculations")*vcell("D48","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D56')
    return None
xfunctions['Calculations!D56']=xcf_Calculations_D56

#SUM(D52:D56)
def xcf_Calculations_D57(): 
    try:      
        return SUM(vcell("D52:D56","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D57')
    return None
xfunctions['Calculations!D57']=xcf_Calculations_D57

#IF(Input_Geometry!C109==2,IF(Input_Geometry!C55<0.0001,-9999,Input_Geometry!C55+0.0001),IF(Input_Geometry!C55<=(Input_Geometry!E42+0.0001),-9999,Input_Geometry!C55+0.0001))
def xcf_Calculations_B63(): 
    try:      
        return IF(vcell("Input_Geometry!C109","Calculations")==2,IF(vcell("Input_Geometry!C55","Calculations")<0.0001,-9999,vcell("Input_Geometry!C55","Calculations")+0.0001),IF(vcell("Input_Geometry!C55","Calculations")<=(vcell("Input_Geometry!E42","Calculations")+0.0001),-9999,vcell("Input_Geometry!C55","Calculations")+0.0001))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B63')
    return None
xfunctions['Calculations!B63']=xcf_Calculations_B63

#IF(Input_Geometry!J92!=0,Input_Geometry!M92,0)
def xcf_Calculations_B66(): 
    try:      
        return IF(vcell("Input_Geometry!J92","Calculations")!=0,vcell("Input_Geometry!M92","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_B66')
    return None
xfunctions['Calculations!B66']=xcf_Calculations_B66

#IF(Input_Geometry!J93!=0,Input_Geometry!M93,0)
def xcf_Calculations_C66(): 
    try:      
        return IF(vcell("Input_Geometry!J93","Calculations")!=0,vcell("Input_Geometry!M93","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_C66')
    return None
xfunctions['Calculations!C66']=xcf_Calculations_C66

#IF(Input_Geometry!J94!=0,Input_Geometry!M94,0)
def xcf_Calculations_D66(): 
    try:      
        return IF(vcell("Input_Geometry!J94","Calculations")!=0,vcell("Input_Geometry!M94","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D66')
    return None
xfunctions['Calculations!D66']=xcf_Calculations_D66

#IF(Input_Geometry!J95!=0,Input_Geometry!M95,0)
def xcf_Calculations_E66(): 
    try:      
        return IF(vcell("Input_Geometry!J95","Calculations")!=0,vcell("Input_Geometry!M95","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_E66')
    return None
xfunctions['Calculations!E66']=xcf_Calculations_E66

#IF(Input_Geometry!J96!=0,Input_Geometry!M96,0)
def xcf_Calculations_F66(): 
    try:      
        return IF(vcell("Input_Geometry!J96","Calculations")!=0,vcell("Input_Geometry!M96","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_F66')
    return None
xfunctions['Calculations!F66']=xcf_Calculations_F66

#IF(B66>0,-B66*TAN(RADIANS($D$19)),-B66*TAN(RADIANS($D$9)))*MAX(D44,0)
def xcf_Calculations_B67(): 
    try:      
        return IF(vcell("B66","Calculations")>0,-vcell("B66","Calculations")*TAN(RADIANS(vcell("$D$19","Calculations"))),-vcell("B66","Calculations")*TAN(RADIANS(vcell("$D$9","Calculations"))))*MAX(vcell("D44","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_B67')
    return None
xfunctions['Calculations!B67']=xcf_Calculations_B67

#IF(C66>0,-C66*TAN(RADIANS($D$20)),-C66*TAN(RADIANS($D$10)))*MAX(D45,0)
def xcf_Calculations_C67(): 
    try:      
        return IF(vcell("C66","Calculations")>0,-vcell("C66","Calculations")*TAN(RADIANS(vcell("$D$20","Calculations"))),-vcell("C66","Calculations")*TAN(RADIANS(vcell("$D$10","Calculations"))))*MAX(vcell("D45","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_C67')
    return None
xfunctions['Calculations!C67']=xcf_Calculations_C67

#IF(D66>0,-D66*TAN(RADIANS($D$21)),-D66*TAN(RADIANS($D$11)))*MAX(D46,0)
def xcf_Calculations_D67(): 
    try:      
        return IF(vcell("D66","Calculations")>0,-vcell("D66","Calculations")*TAN(RADIANS(vcell("$D$21","Calculations"))),-vcell("D66","Calculations")*TAN(RADIANS(vcell("$D$11","Calculations"))))*MAX(vcell("D46","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D67')
    return None
xfunctions['Calculations!D67']=xcf_Calculations_D67

#IF(E66>0,-E66*TAN(RADIANS($D$22)),-E66*TAN(RADIANS($D$12)))*MAX(D47,0)
def xcf_Calculations_E67(): 
    try:      
        return IF(vcell("E66","Calculations")>0,-vcell("E66","Calculations")*TAN(RADIANS(vcell("$D$22","Calculations"))),-vcell("E66","Calculations")*TAN(RADIANS(vcell("$D$12","Calculations"))))*MAX(vcell("D47","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_E67')
    return None
xfunctions['Calculations!E67']=xcf_Calculations_E67

#IF(F66>0,-F66*TAN(RADIANS($D$23)),-F66*TAN(RADIANS($D$13)))*MAX(D48,0)
def xcf_Calculations_F67(): 
    try:      
        return IF(vcell("F66","Calculations")>0,-vcell("F66","Calculations")*TAN(RADIANS(vcell("$D$23","Calculations"))),-vcell("F66","Calculations")*TAN(RADIANS(vcell("$D$13","Calculations"))))*MAX(vcell("D48","Calculations"),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_F67')
    return None
xfunctions['Calculations!F67']=xcf_Calculations_F67

#SUM(B67:F67)
def xcf_Calculations_L67(): 
    try:      
        return SUM(vcell("B67:F67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_L67')
    return None
xfunctions['Calculations!L67']=xcf_Calculations_L67

#B73*$D$36*COS(RADIANS(Input_Geometry!$E$140))*TAN(RADIANS($D$9))
def xcf_Calculations_B68(): 
    try:      
        return vcell("B73","Calculations")*vcell("$D$36","Calculations")*COS(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$9","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B68')
    return None
xfunctions['Calculations!B68']=xcf_Calculations_B68

#C73*$D$37*COS(RADIANS(Input_Geometry!$E$140))*TAN(RADIANS($D$10))
def xcf_Calculations_C68(): 
    try:      
        return vcell("C73","Calculations")*vcell("$D$37","Calculations")*COS(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$10","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C68')
    return None
xfunctions['Calculations!C68']=xcf_Calculations_C68

#D73*$D$38*COS(RADIANS(Input_Geometry!$E$140))*TAN(RADIANS($D$11))
def xcf_Calculations_D68(): 
    try:      
        return vcell("D73","Calculations")*vcell("$D$38","Calculations")*COS(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$11","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D68')
    return None
xfunctions['Calculations!D68']=xcf_Calculations_D68

#E73*$D$39*COS(RADIANS(Input_Geometry!$E$140))*TAN(RADIANS($D$12))
def xcf_Calculations_E68(): 
    try:      
        return vcell("E73","Calculations")*vcell("$D$39","Calculations")*COS(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$12","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E68')
    return None
xfunctions['Calculations!E68']=xcf_Calculations_E68

#F73*$D$40*COS(RADIANS(Input_Geometry!$E$140))*TAN(RADIANS($D$13))
def xcf_Calculations_F68(): 
    try:      
        return vcell("F73","Calculations")*vcell("$D$40","Calculations")*COS(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$13","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F68')
    return None
xfunctions['Calculations!F68']=xcf_Calculations_F68

#SUM(B68:F68)
def xcf_Calculations_L68(): 
    try:      
        return SUM(vcell("B68:F68","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_L68')
    return None
xfunctions['Calculations!L68']=xcf_Calculations_L68

#B69*COS(RADIANS(B70-Input_Geometry!$E$140))*TAN(RADIANS($D$9))
def xcf_Calculations_B71(): 
    try:      
        return vcell("B69","Calculations")*COS(RADIANS(vcell("B70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$9","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B71')
    return None
xfunctions['Calculations!B71']=xcf_Calculations_B71

#C69*COS(RADIANS(C70-Input_Geometry!$E$140))*TAN(RADIANS($D$10))
def xcf_Calculations_C71(): 
    try:      
        return vcell("C69","Calculations")*COS(RADIANS(vcell("C70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$10","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C71')
    return None
xfunctions['Calculations!C71']=xcf_Calculations_C71

#D69*COS(RADIANS(D70-Input_Geometry!$E$140))*TAN(RADIANS($D$11))
def xcf_Calculations_D71(): 
    try:      
        return vcell("D69","Calculations")*COS(RADIANS(vcell("D70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$11","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D71')
    return None
xfunctions['Calculations!D71']=xcf_Calculations_D71

#E69*COS(RADIANS(E70-Input_Geometry!$E$140))*TAN(RADIANS($D$12))
def xcf_Calculations_E71(): 
    try:      
        return vcell("E69","Calculations")*COS(RADIANS(vcell("E70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$12","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E71')
    return None
xfunctions['Calculations!E71']=xcf_Calculations_E71

#F69*COS(RADIANS(F70-Input_Geometry!$E$140))*TAN(RADIANS($D$13))
def xcf_Calculations_F71(): 
    try:      
        return vcell("F69","Calculations")*COS(RADIANS(vcell("F70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))*TAN(RADIANS(vcell("$D$13","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F71')
    return None
xfunctions['Calculations!F71']=xcf_Calculations_F71

#SUM(B71:F71)
def xcf_Calculations_L71(): 
    try:      
        return SUM(vcell("B71:F71","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_L71')
    return None
xfunctions['Calculations!L71']=xcf_Calculations_L71

#B69*SIN(RADIANS(B70-Input_Geometry!$E$140))
def xcf_Calculations_B72(): 
    try:      
        return vcell("B69","Calculations")*SIN(RADIANS(vcell("B70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B72')
    return None
xfunctions['Calculations!B72']=xcf_Calculations_B72

#C69*SIN(RADIANS(C70-Input_Geometry!$E$140))
def xcf_Calculations_C72(): 
    try:      
        return vcell("C69","Calculations")*SIN(RADIANS(vcell("C70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C72')
    return None
xfunctions['Calculations!C72']=xcf_Calculations_C72

#D69*SIN(RADIANS(D70-Input_Geometry!$E$140))
def xcf_Calculations_D72(): 
    try:      
        return vcell("D69","Calculations")*SIN(RADIANS(vcell("D70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D72')
    return None
xfunctions['Calculations!D72']=xcf_Calculations_D72

#E69*SIN(RADIANS(E70-Input_Geometry!$E$140))
def xcf_Calculations_E72(): 
    try:      
        return vcell("E69","Calculations")*SIN(RADIANS(vcell("E70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E72')
    return None
xfunctions['Calculations!E72']=xcf_Calculations_E72

#F69*SIN(RADIANS(F70-Input_Geometry!$E$140))
def xcf_Calculations_F72(): 
    try:      
        return vcell("F69","Calculations")*SIN(RADIANS(vcell("F70","Calculations")-vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F72')
    return None
xfunctions['Calculations!F72']=xcf_Calculations_F72

#SUM(B72:F72)
def xcf_Calculations_L72(): 
    try:      
        return SUM(vcell("B72:F72","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_L72')
    return None
xfunctions['Calculations!L72']=xcf_Calculations_L72

#MAX(IF(B66>0,$D$60*B66+$D$24,$D$24),0)
def xcf_Calculations_B73(): 
    try:      
        return MAX(IF(vcell("B66","Calculations")>0,vcell("$D$60","Calculations")*vcell("B66","Calculations")+vcell("$D$24","Calculations"),vcell("$D$24","Calculations")),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_B73')
    return None
xfunctions['Calculations!B73']=xcf_Calculations_B73

#MAX(IF(C66>0,$D$60*C66+$D$25,$D$25),0)
def xcf_Calculations_C73(): 
    try:      
        return MAX(IF(vcell("C66","Calculations")>0,vcell("$D$60","Calculations")*vcell("C66","Calculations")+vcell("$D$25","Calculations"),vcell("$D$25","Calculations")),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_C73')
    return None
xfunctions['Calculations!C73']=xcf_Calculations_C73

#MAX(IF(D66>0,$D$60*D66+$D$26,$D$26),0)
def xcf_Calculations_D73(): 
    try:      
        return MAX(IF(vcell("D66","Calculations")>0,vcell("$D$60","Calculations")*vcell("D66","Calculations")+vcell("$D$26","Calculations"),vcell("$D$26","Calculations")),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_D73')
    return None
xfunctions['Calculations!D73']=xcf_Calculations_D73

#MAX(IF(E66>0,$D$60*E66+$D$27,$D$27),0)
def xcf_Calculations_E73(): 
    try:      
        return MAX(IF(vcell("E66","Calculations")>0,vcell("$D$60","Calculations")*vcell("E66","Calculations")+vcell("$D$27","Calculations"),vcell("$D$27","Calculations")),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_E73')
    return None
xfunctions['Calculations!E73']=xcf_Calculations_E73

#MAX(IF(F66>0,$D$60*F66+$D$28,$D$28),0)
def xcf_Calculations_F73(): 
    try:      
        return MAX(IF(vcell("F66","Calculations")>0,vcell("$D$60","Calculations")*vcell("F66","Calculations")+vcell("$D$28","Calculations"),vcell("$D$28","Calculations")),0)
    except Exception as ex:
        print(ex,'on xcf_Calculations_F73')
    return None
xfunctions['Calculations!F73']=xcf_Calculations_F73

#B73*$D$36*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_B74(): 
    try:      
        return vcell("B73","Calculations")*vcell("$D$36","Calculations")*SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B74')
    return None
xfunctions['Calculations!B74']=xcf_Calculations_B74

#C73*$D$37*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_C74(): 
    try:      
        return vcell("C73","Calculations")*vcell("$D$37","Calculations")*SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C74')
    return None
xfunctions['Calculations!C74']=xcf_Calculations_C74

#D73*$D$38*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_D74(): 
    try:      
        return vcell("D73","Calculations")*vcell("$D$38","Calculations")*SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D74')
    return None
xfunctions['Calculations!D74']=xcf_Calculations_D74

#E73*$D$39*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_E74(): 
    try:      
        return vcell("E73","Calculations")*vcell("$D$39","Calculations")*SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E74')
    return None
xfunctions['Calculations!E74']=xcf_Calculations_E74

#F73*$D$40*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_Calculations_F74(): 
    try:      
        return vcell("F73","Calculations")*vcell("$D$40","Calculations")*SIN(RADIANS(vcell("Input_Geometry!$E$140","Calculations")))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F74')
    return None
xfunctions['Calculations!F74']=xcf_Calculations_F74

#SUM(B74:F74)
def xcf_Calculations_L74(): 
    try:      
        return SUM(vcell("B74:F74","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_L74')
    return None
xfunctions['Calculations!L74']=xcf_Calculations_L74

#IF(B68+B71-B67<0,0,B68+B71-B67)
def xcf_Calculations_B76(): 
    try:      
        return IF(vcell("B68","Calculations")+vcell("B71","Calculations")-vcell("B67","Calculations")<0,0,vcell("B68","Calculations")+vcell("B71","Calculations")-vcell("B67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B76')
    return None
xfunctions['Calculations!B76']=xcf_Calculations_B76

#IF(C68+C71-C67<0,0,C68+C71-C67)
def xcf_Calculations_C76(): 
    try:      
        return IF(vcell("C68","Calculations")+vcell("C71","Calculations")-vcell("C67","Calculations")<0,0,vcell("C68","Calculations")+vcell("C71","Calculations")-vcell("C67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C76')
    return None
xfunctions['Calculations!C76']=xcf_Calculations_C76

#IF(D68+D71-D67<0,0,D68+D71-D67)
def xcf_Calculations_D76(): 
    try:      
        return IF(vcell("D68","Calculations")+vcell("D71","Calculations")-vcell("D67","Calculations")<0,0,vcell("D68","Calculations")+vcell("D71","Calculations")-vcell("D67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D76')
    return None
xfunctions['Calculations!D76']=xcf_Calculations_D76

#IF(E68+E71-E67<0,0,E68+E71-E67)
def xcf_Calculations_E76(): 
    try:      
        return IF(vcell("E68","Calculations")+vcell("E71","Calculations")-vcell("E67","Calculations")<0,0,vcell("E68","Calculations")+vcell("E71","Calculations")-vcell("E67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E76')
    return None
xfunctions['Calculations!E76']=xcf_Calculations_E76

#IF(F68+F71-F67<0,0,F68+F71-F67)
def xcf_Calculations_F76(): 
    try:      
        return IF(vcell("F68","Calculations")+vcell("F71","Calculations")-vcell("F67","Calculations")<0,0,vcell("F68","Calculations")+vcell("F71","Calculations")-vcell("F67","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F76')
    return None
xfunctions['Calculations!F76']=xcf_Calculations_F76

#$D$57+SUM(B76:F76)
def xcf_Calculations_K76(): 
    try:      
        return vcell("$D$57","Calculations")+SUM(vcell("B76:F76","Calculations"))
    except Exception as ex:
        print(ex,'on xcf_Calculations_K76')
    return None
xfunctions['Calculations!K76']=xcf_Calculations_K76

#IF(L74-L72>=0,L74-L72,-1)
def xcf_Calculations_L76(): 
    try:      
        return IF(vcell("L74","Calculations")-vcell("L72","Calculations")>=0,vcell("L74","Calculations")-vcell("L72","Calculations"),-1)
    except Exception as ex:
        print(ex,'on xcf_Calculations_L76')
    return None
xfunctions['Calculations!L76']=xcf_Calculations_L76

#IF(P15>=P16,IF(K76/L76<0,99999999,K76/L76),99999999)
def xcf_Calculations_L77(): 
    try:      
        return IF(vcell("P15","Calculations")>=vcell("P16","Calculations"),IF(vcell("K76","Calculations")/vcell("L76","Calculations")<0,99999999,vcell("K76","Calculations")/vcell("L76","Calculations")),99999999)
    except Exception as ex:
        print(ex,'on xcf_Calculations_L77')
    return None
xfunctions['Calculations!L77']=xcf_Calculations_L77

#D14+((1+B$66)*TAN(RADIANS(D19)))
def xcf_Calculations_B78(): 
    try:      
        return vcell("D14","Calculations")+((1+vcell("B$66","Calculations"))*TAN(RADIANS(vcell("D19","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_B78')
    return None
xfunctions['Calculations!B78']=xcf_Calculations_B78

#D15+((1+C$66)*TAN(RADIANS(D20)))
def xcf_Calculations_C78(): 
    try:      
        return vcell("D15","Calculations")+((1+vcell("C$66","Calculations"))*TAN(RADIANS(vcell("D20","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_C78')
    return None
xfunctions['Calculations!C78']=xcf_Calculations_C78

#D16+((1+D$66)*TAN(RADIANS(D21)))
def xcf_Calculations_D78(): 
    try:      
        return vcell("D16","Calculations")+((1+vcell("D$66","Calculations"))*TAN(RADIANS(vcell("D21","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_D78')
    return None
xfunctions['Calculations!D78']=xcf_Calculations_D78

#D17+((1+E$66)*TAN(RADIANS(D22)))
def xcf_Calculations_E78(): 
    try:      
        return vcell("D17","Calculations")+((1+vcell("E$66","Calculations"))*TAN(RADIANS(vcell("D22","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_E78')
    return None
xfunctions['Calculations!E78']=xcf_Calculations_E78

#D18+((1+F$66)*TAN(RADIANS(D23)))
def xcf_Calculations_F78(): 
    try:      
        return vcell("D18","Calculations")+((1+vcell("F$66","Calculations"))*TAN(RADIANS(vcell("D23","Calculations"))))
    except Exception as ex:
        print(ex,'on xcf_Calculations_F78')
    return None
xfunctions['Calculations!F78']=xcf_Calculations_F78

#SUM(D4*B$65,E4*E$65,F4*H$65,G4*K$65,H4*N$65,I4*9.807)
def xcf_VertSliceCalcs_J4(): 
    try:      
        return SUM(vcell("D4","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E4","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F4","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G4","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H4","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I4","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J4')
    return None
xfunctions['VertSliceCalcs!J4']=xcf_VertSliceCalcs_J4

#SUM(D5*B$65,E5*E$65,F5*H$65,G5*K$65,H5*N$65,I5*9.807)
def xcf_VertSliceCalcs_J5(): 
    try:      
        return SUM(vcell("D5","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E5","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F5","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G5","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H5","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I5","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J5')
    return None
xfunctions['VertSliceCalcs!J5']=xcf_VertSliceCalcs_J5

#P32
def xcf_VertSliceCalcs_P5(): 
    try:      
        return vcell("P32","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P5')
    return None
xfunctions['VertSliceCalcs!P5']=xcf_VertSliceCalcs_P5

#Q32
def xcf_VertSliceCalcs_Q5(): 
    try:      
        return vcell("Q32","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q5')
    return None
xfunctions['VertSliceCalcs!Q5']=xcf_VertSliceCalcs_Q5

#SUM(D6*B$65,E6*E$65,F6*H$65,G6*K$65,H6*N$65,I6*9.807)
def xcf_VertSliceCalcs_J6(): 
    try:      
        return SUM(vcell("D6","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E6","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F6","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G6","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H6","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I6","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J6')
    return None
xfunctions['VertSliceCalcs!J6']=xcf_VertSliceCalcs_J6

#P33
def xcf_VertSliceCalcs_P6(): 
    try:      
        return vcell("P33","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P6')
    return None
xfunctions['VertSliceCalcs!P6']=xcf_VertSliceCalcs_P6

#SUM(D7*B$65,E7*E$65,F7*H$65,G7*K$65,H7*N$65,I7*9.807)
def xcf_VertSliceCalcs_J7(): 
    try:      
        return SUM(vcell("D7","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E7","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F7","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G7","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H7","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I7","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J7')
    return None
xfunctions['VertSliceCalcs!J7']=xcf_VertSliceCalcs_J7

#P34
def xcf_VertSliceCalcs_P7(): 
    try:      
        return vcell("P34","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P7')
    return None
xfunctions['VertSliceCalcs!P7']=xcf_VertSliceCalcs_P7

#SUM(D8*B$65,E8*E$65,F8*H$65,G8*K$65,H8*N$65,I8*9.807)
def xcf_VertSliceCalcs_J8(): 
    try:      
        return SUM(vcell("D8","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E8","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F8","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G8","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H8","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I8","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J8')
    return None
xfunctions['VertSliceCalcs!J8']=xcf_VertSliceCalcs_J8

#P35
def xcf_VertSliceCalcs_P8(): 
    try:      
        return vcell("P35","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P8')
    return None
xfunctions['VertSliceCalcs!P8']=xcf_VertSliceCalcs_P8

#SUM(D9*B$65,E9*E$65,F9*H$65,G9*K$65,H9*N$65,I9*9.807)
def xcf_VertSliceCalcs_J9(): 
    try:      
        return SUM(vcell("D9","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E9","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F9","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G9","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H9","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I9","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J9')
    return None
xfunctions['VertSliceCalcs!J9']=xcf_VertSliceCalcs_J9

#P36
def xcf_VertSliceCalcs_P9(): 
    try:      
        return vcell("P36","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P9')
    return None
xfunctions['VertSliceCalcs!P9']=xcf_VertSliceCalcs_P9

#SUM(D10*B$65,E10*E$65,F10*H$65,G10*K$65,H10*N$65,I10*9.807)
def xcf_VertSliceCalcs_J10(): 
    try:      
        return SUM(vcell("D10","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E10","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F10","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G10","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H10","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I10","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J10')
    return None
xfunctions['VertSliceCalcs!J10']=xcf_VertSliceCalcs_J10

#P37
def xcf_VertSliceCalcs_P10(): 
    try:      
        return vcell("P37","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P10')
    return None
xfunctions['VertSliceCalcs!P10']=xcf_VertSliceCalcs_P10

#SUM(D11*B$65,E11*E$65,F11*H$65,G11*K$65,H11*N$65,I11*9.807)
def xcf_VertSliceCalcs_J11(): 
    try:      
        return SUM(vcell("D11","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E11","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F11","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G11","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H11","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I11","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J11')
    return None
xfunctions['VertSliceCalcs!J11']=xcf_VertSliceCalcs_J11

#P38
def xcf_VertSliceCalcs_P11(): 
    try:      
        return vcell("P38","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P11')
    return None
xfunctions['VertSliceCalcs!P11']=xcf_VertSliceCalcs_P11

#SUM(D12*B$65,E12*E$65,F12*H$65,G12*K$65,H12*N$65,I12*9.807)
def xcf_VertSliceCalcs_J12(): 
    try:      
        return SUM(vcell("D12","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E12","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F12","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G12","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H12","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I12","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J12')
    return None
xfunctions['VertSliceCalcs!J12']=xcf_VertSliceCalcs_J12

#P39
def xcf_VertSliceCalcs_P12(): 
    try:      
        return vcell("P39","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P12')
    return None
xfunctions['VertSliceCalcs!P12']=xcf_VertSliceCalcs_P12

#SUM(D13*B$65,E13*E$65,F13*H$65,G13*K$65,H13*N$65,I13*9.807)
def xcf_VertSliceCalcs_J13(): 
    try:      
        return SUM(vcell("D13","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E13","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F13","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G13","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H13","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I13","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J13')
    return None
xfunctions['VertSliceCalcs!J13']=xcf_VertSliceCalcs_J13

#P40
def xcf_VertSliceCalcs_P13(): 
    try:      
        return vcell("P40","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P13')
    return None
xfunctions['VertSliceCalcs!P13']=xcf_VertSliceCalcs_P13

#SUM(D14*B$65,E14*E$65,F14*H$65,G14*K$65,H14*N$65,I14*9.807)
def xcf_VertSliceCalcs_J14(): 
    try:      
        return SUM(vcell("D14","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E14","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F14","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G14","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H14","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I14","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J14')
    return None
xfunctions['VertSliceCalcs!J14']=xcf_VertSliceCalcs_J14

#P41
def xcf_VertSliceCalcs_P14(): 
    try:      
        return vcell("P41","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P14')
    return None
xfunctions['VertSliceCalcs!P14']=xcf_VertSliceCalcs_P14

#SUM(D15*B$65,E15*E$65,F15*H$65,G15*K$65,H15*N$65,I15*9.807)
def xcf_VertSliceCalcs_J15(): 
    try:      
        return SUM(vcell("D15","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E15","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F15","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G15","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H15","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I15","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J15')
    return None
xfunctions['VertSliceCalcs!J15']=xcf_VertSliceCalcs_J15

#P42
def xcf_VertSliceCalcs_P15(): 
    try:      
        return vcell("P42","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P15')
    return None
xfunctions['VertSliceCalcs!P15']=xcf_VertSliceCalcs_P15

#SUM(D16*B$65,E16*E$65,F16*H$65,G16*K$65,H16*N$65,I16*9.807)
def xcf_VertSliceCalcs_J16(): 
    try:      
        return SUM(vcell("D16","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E16","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F16","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G16","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H16","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I16","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J16')
    return None
xfunctions['VertSliceCalcs!J16']=xcf_VertSliceCalcs_J16

#P43
def xcf_VertSliceCalcs_P16(): 
    try:      
        return vcell("P43","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P16')
    return None
xfunctions['VertSliceCalcs!P16']=xcf_VertSliceCalcs_P16

#SUM(D17*B$65,E17*E$65,F17*H$65,G17*K$65,H17*N$65,I17*9.807)
def xcf_VertSliceCalcs_J17(): 
    try:      
        return SUM(vcell("D17","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E17","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F17","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G17","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H17","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I17","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J17')
    return None
xfunctions['VertSliceCalcs!J17']=xcf_VertSliceCalcs_J17

#P44
def xcf_VertSliceCalcs_P17(): 
    try:      
        return vcell("P44","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P17')
    return None
xfunctions['VertSliceCalcs!P17']=xcf_VertSliceCalcs_P17

#SUM(D18*B$65,E18*E$65,F18*H$65,G18*K$65,H18*N$65,I18*9.807)
def xcf_VertSliceCalcs_J18(): 
    try:      
        return SUM(vcell("D18","VertSliceCalcs")*vcell("B$65","VertSliceCalcs"),vcell("E18","VertSliceCalcs")*vcell("E$65","VertSliceCalcs"),vcell("F18","VertSliceCalcs")*vcell("H$65","VertSliceCalcs"),vcell("G18","VertSliceCalcs")*vcell("K$65","VertSliceCalcs"),vcell("H18","VertSliceCalcs")*vcell("N$65","VertSliceCalcs"),vcell("I18","VertSliceCalcs")*9.807)
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J18')
    return None
xfunctions['VertSliceCalcs!J18']=xcf_VertSliceCalcs_J18

#P45
def xcf_VertSliceCalcs_P18(): 
    try:      
        return vcell("P45","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P18')
    return None
xfunctions['VertSliceCalcs!P18']=xcf_VertSliceCalcs_P18

#SUM(D4:D18)
def xcf_VertSliceCalcs_D19(): 
    try:      
        return SUM(vcell("D4:D18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D19')
    return None
xfunctions['VertSliceCalcs!D19']=xcf_VertSliceCalcs_D19

#SUM(E4:E18)
def xcf_VertSliceCalcs_E19(): 
    try:      
        return SUM(vcell("E4:E18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E19')
    return None
xfunctions['VertSliceCalcs!E19']=xcf_VertSliceCalcs_E19

#SUM(F4:F18)
def xcf_VertSliceCalcs_F19(): 
    try:      
        return SUM(vcell("F4:F18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F19')
    return None
xfunctions['VertSliceCalcs!F19']=xcf_VertSliceCalcs_F19

#SUM(G4:G18)
def xcf_VertSliceCalcs_G19(): 
    try:      
        return SUM(vcell("G4:G18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G19')
    return None
xfunctions['VertSliceCalcs!G19']=xcf_VertSliceCalcs_G19

#SUM(H4:H18)
def xcf_VertSliceCalcs_H19(): 
    try:      
        return SUM(vcell("H4:H18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H19')
    return None
xfunctions['VertSliceCalcs!H19']=xcf_VertSliceCalcs_H19

#SUM(I4:I18)
def xcf_VertSliceCalcs_I19(): 
    try:      
        return SUM(vcell("I4:I18","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I19')
    return None
xfunctions['VertSliceCalcs!I19']=xcf_VertSliceCalcs_I19

#SUM(J4:J18)/9.807
def xcf_VertSliceCalcs_J19(): 
    try:      
        return SUM(vcell("J4:J18","VertSliceCalcs"))/9.807
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J19')
    return None
xfunctions['VertSliceCalcs!J19']=xcf_VertSliceCalcs_J19

#P46
def xcf_VertSliceCalcs_P19(): 
    try:      
        return vcell("P46","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P19')
    return None
xfunctions['VertSliceCalcs!P19']=xcf_VertSliceCalcs_P19

#SUM(D19:H19)
def xcf_VertSliceCalcs_J20(): 
    try:      
        return SUM(vcell("D19:H19","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J20')
    return None
xfunctions['VertSliceCalcs!J20']=xcf_VertSliceCalcs_J20

#P47
def xcf_VertSliceCalcs_P20(): 
    try:      
        return vcell("P47","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P20')
    return None
xfunctions['VertSliceCalcs!P20']=xcf_VertSliceCalcs_P20

#1000*SUM(D19*B65,E19*E65,F19*H65,G19*K65,H19*N65)/9.807
def xcf_VertSliceCalcs_J21(): 
    try:      
        return 1000*SUM(vcell("D19","VertSliceCalcs")*vcell("B65","VertSliceCalcs"),vcell("E19","VertSliceCalcs")*vcell("E65","VertSliceCalcs"),vcell("F19","VertSliceCalcs")*vcell("H65","VertSliceCalcs"),vcell("G19","VertSliceCalcs")*vcell("K65","VertSliceCalcs"),vcell("H19","VertSliceCalcs")*vcell("N65","VertSliceCalcs"))/9.807
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J21')
    return None
xfunctions['VertSliceCalcs!J21']=xcf_VertSliceCalcs_J21

#IF(OR(P33==0,P32==0),0,SQRT((P33-P32)^2+(Q32-Q33)^2))
def xcf_VertSliceCalcs_D22(): 
    try:      
        return IF(OR(vcell("P33","VertSliceCalcs")==0,vcell("P32","VertSliceCalcs")==0),0,SQRT((vcell("P33","VertSliceCalcs")-vcell("P32","VertSliceCalcs"))**2+(vcell("Q32","VertSliceCalcs")-vcell("Q33","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D22')
    return None
xfunctions['VertSliceCalcs!D22']=xcf_VertSliceCalcs_D22

#1000*SUM(D19*Calculations!D24*Bank_Model_Output!J84,E19*Calculations!D25*Bank_Model_Output!J85,F19*Calculations!D26*Bank_Model_Output!J86,G19*Calculations!D27*Bank_Model_Output!J87,H19*Calculations!D28*Bank_Model_Output!J88)/9.807
def xcf_VertSliceCalcs_J22(): 
    try:      
        return 1000*SUM(vcell("D19","VertSliceCalcs")*vcell("Calculations!D24","VertSliceCalcs")*vcell("Bank_Model_Output!J84","VertSliceCalcs"),vcell("E19","VertSliceCalcs")*vcell("Calculations!D25","VertSliceCalcs")*vcell("Bank_Model_Output!J85","VertSliceCalcs"),vcell("F19","VertSliceCalcs")*vcell("Calculations!D26","VertSliceCalcs")*vcell("Bank_Model_Output!J86","VertSliceCalcs"),vcell("G19","VertSliceCalcs")*vcell("Calculations!D27","VertSliceCalcs")*vcell("Bank_Model_Output!J87","VertSliceCalcs"),vcell("H19","VertSliceCalcs")*vcell("Calculations!D28","VertSliceCalcs")*vcell("Bank_Model_Output!J88","VertSliceCalcs"))/9.807
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J22')
    return None
xfunctions['VertSliceCalcs!J22']=xcf_VertSliceCalcs_J22

#IF(OR(P34==0,P33==0),0,SQRT((P34-P33)^2+(Q33-Q34)^2))
def xcf_VertSliceCalcs_D23(): 
    try:      
        return IF(OR(vcell("P34","VertSliceCalcs")==0,vcell("P33","VertSliceCalcs")==0),0,SQRT((vcell("P34","VertSliceCalcs")-vcell("P33","VertSliceCalcs"))**2+(vcell("Q33","VertSliceCalcs")-vcell("Q34","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D23')
    return None
xfunctions['VertSliceCalcs!D23']=xcf_VertSliceCalcs_D23

#IF(OR(P35==0,P34==0),0,SQRT((P35-P34)^2+(Q34-Q35)^2))
def xcf_VertSliceCalcs_D24(): 
    try:      
        return IF(OR(vcell("P35","VertSliceCalcs")==0,vcell("P34","VertSliceCalcs")==0),0,SQRT((vcell("P35","VertSliceCalcs")-vcell("P34","VertSliceCalcs"))**2+(vcell("Q34","VertSliceCalcs")-vcell("Q35","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D24')
    return None
xfunctions['VertSliceCalcs!D24']=xcf_VertSliceCalcs_D24

#IF(OR(P36==0,P35==0),0,SQRT((P36-P35)^2+(Q35-Q36)^2))
def xcf_VertSliceCalcs_D25(): 
    try:      
        return IF(OR(vcell("P36","VertSliceCalcs")==0,vcell("P35","VertSliceCalcs")==0),0,SQRT((vcell("P36","VertSliceCalcs")-vcell("P35","VertSliceCalcs"))**2+(vcell("Q35","VertSliceCalcs")-vcell("Q36","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D25')
    return None
xfunctions['VertSliceCalcs!D25']=xcf_VertSliceCalcs_D25

#IF(OR(P37==0,P36==0),0,SQRT((P37-P36)^2+(Q36-Q37)^2))
def xcf_VertSliceCalcs_D26(): 
    try:      
        return IF(OR(vcell("P37","VertSliceCalcs")==0,vcell("P36","VertSliceCalcs")==0),0,SQRT((vcell("P37","VertSliceCalcs")-vcell("P36","VertSliceCalcs"))**2+(vcell("Q36","VertSliceCalcs")-vcell("Q37","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D26')
    return None
xfunctions['VertSliceCalcs!D26']=xcf_VertSliceCalcs_D26

#IF(OR(P38==0,P37==0),0,SQRT((P38-P37)^2+(Q37-Q38)^2))
def xcf_VertSliceCalcs_D27(): 
    try:      
        return IF(OR(vcell("P38","VertSliceCalcs")==0,vcell("P37","VertSliceCalcs")==0),0,SQRT((vcell("P38","VertSliceCalcs")-vcell("P37","VertSliceCalcs"))**2+(vcell("Q37","VertSliceCalcs")-vcell("Q38","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D27')
    return None
xfunctions['VertSliceCalcs!D27']=xcf_VertSliceCalcs_D27

#IF(OR(P39==0,P38==0),0,SQRT((P39-P38)^2+(Q38-Q39)^2))
def xcf_VertSliceCalcs_D28(): 
    try:      
        return IF(OR(vcell("P39","VertSliceCalcs")==0,vcell("P38","VertSliceCalcs")==0),0,SQRT((vcell("P39","VertSliceCalcs")-vcell("P38","VertSliceCalcs"))**2+(vcell("Q38","VertSliceCalcs")-vcell("Q39","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D28')
    return None
xfunctions['VertSliceCalcs!D28']=xcf_VertSliceCalcs_D28

#IF(OR(P40==0,P39==0),0,SQRT((P40-P39)^2+(Q39-Q40)^2))
def xcf_VertSliceCalcs_D29(): 
    try:      
        return IF(OR(vcell("P40","VertSliceCalcs")==0,vcell("P39","VertSliceCalcs")==0),0,SQRT((vcell("P40","VertSliceCalcs")-vcell("P39","VertSliceCalcs"))**2+(vcell("Q39","VertSliceCalcs")-vcell("Q40","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D29')
    return None
xfunctions['VertSliceCalcs!D29']=xcf_VertSliceCalcs_D29

#IF(OR(P41==0,P40==0),0,SQRT((P41-P40)^2+(Q40-Q41)^2))
def xcf_VertSliceCalcs_D30(): 
    try:      
        return IF(OR(vcell("P41","VertSliceCalcs")==0,vcell("P40","VertSliceCalcs")==0),0,SQRT((vcell("P41","VertSliceCalcs")-vcell("P40","VertSliceCalcs"))**2+(vcell("Q40","VertSliceCalcs")-vcell("Q41","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D30')
    return None
xfunctions['VertSliceCalcs!D30']=xcf_VertSliceCalcs_D30

#IF(OR(P42==0,P41==0),0,SQRT((P42-P41)^2+(Q41-Q42)^2))
def xcf_VertSliceCalcs_D31(): 
    try:      
        return IF(OR(vcell("P42","VertSliceCalcs")==0,vcell("P41","VertSliceCalcs")==0),0,SQRT((vcell("P42","VertSliceCalcs")-vcell("P41","VertSliceCalcs"))**2+(vcell("Q41","VertSliceCalcs")-vcell("Q42","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D31')
    return None
xfunctions['VertSliceCalcs!D31']=xcf_VertSliceCalcs_D31

#IF(OR(P43==0,P42==0),0,SQRT((P43-P42)^2+(Q42-Q43)^2))
def xcf_VertSliceCalcs_D32(): 
    try:      
        return IF(OR(vcell("P43","VertSliceCalcs")==0,vcell("P42","VertSliceCalcs")==0),0,SQRT((vcell("P43","VertSliceCalcs")-vcell("P42","VertSliceCalcs"))**2+(vcell("Q42","VertSliceCalcs")-vcell("Q43","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D32')
    return None
xfunctions['VertSliceCalcs!D32']=xcf_VertSliceCalcs_D32

#Calculations!$P$15-(Q32-Calculations!$Q$15)/TAN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_P32(): 
    try:      
        return vcell("Calculations!$P$15","VertSliceCalcs")-(vcell("Q32","VertSliceCalcs")-vcell("Calculations!$Q$15","VertSliceCalcs"))/TAN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P32')
    return None
xfunctions['VertSliceCalcs!P32']=xcf_VertSliceCalcs_P32

#Calculations!Q16
def xcf_VertSliceCalcs_Q32(): 
    try:      
        return vcell("Calculations!Q16","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q32')
    return None
xfunctions['VertSliceCalcs!Q32']=xcf_VertSliceCalcs_Q32

#IF(OR(P44==0,P43==0),0,SQRT((P44-P43)^2+(Q43-Q44)^2))
def xcf_VertSliceCalcs_D33(): 
    try:      
        return IF(OR(vcell("P44","VertSliceCalcs")==0,vcell("P43","VertSliceCalcs")==0),0,SQRT((vcell("P44","VertSliceCalcs")-vcell("P43","VertSliceCalcs"))**2+(vcell("Q43","VertSliceCalcs")-vcell("Q44","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D33')
    return None
xfunctions['VertSliceCalcs!D33']=xcf_VertSliceCalcs_D33

#IF(OR(P45==0,P44==0),0,SQRT((P45-P44)^2+(Q44-Q45)^2))
def xcf_VertSliceCalcs_D34(): 
    try:      
        return IF(OR(vcell("P45","VertSliceCalcs")==0,vcell("P44","VertSliceCalcs")==0),0,SQRT((vcell("P45","VertSliceCalcs")-vcell("P44","VertSliceCalcs"))**2+(vcell("Q44","VertSliceCalcs")-vcell("Q45","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D34')
    return None
xfunctions['VertSliceCalcs!D34']=xcf_VertSliceCalcs_D34

#IF(OR(P46==0,P45==0),0,SQRT((P46-P45)^2+(Q45-Q46)^2))
def xcf_VertSliceCalcs_D35(): 
    try:      
        return IF(OR(vcell("P46","VertSliceCalcs")==0,vcell("P45","VertSliceCalcs")==0),0,SQRT((vcell("P46","VertSliceCalcs")-vcell("P45","VertSliceCalcs"))**2+(vcell("Q45","VertSliceCalcs")-vcell("Q46","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D35')
    return None
xfunctions['VertSliceCalcs!D35']=xcf_VertSliceCalcs_D35

#IF(P46==0,0,SQRT((P25-P46)^2+(Q46-Q25)^2))
def xcf_VertSliceCalcs_D36(): 
    try:      
        return IF(vcell("P46","VertSliceCalcs")==0,0,SQRT((vcell("P25","VertSliceCalcs")-vcell("P46","VertSliceCalcs"))**2+(vcell("Q46","VertSliceCalcs")-vcell("Q25","VertSliceCalcs"))**2))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D36')
    return None
xfunctions['VertSliceCalcs!D36']=xcf_VertSliceCalcs_D36

#SUM(D22:D36)
def xcf_VertSliceCalcs_D37(): 
    try:      
        return SUM(vcell("D22:D36","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D37')
    return None
xfunctions['VertSliceCalcs!D37']=xcf_VertSliceCalcs_D37

#Calculations!$D$14*D22
def xcf_VertSliceCalcs_D40(): 
    try:      
        return vcell("Calculations!$D$14","VertSliceCalcs")*vcell("D22","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D40')
    return None
xfunctions['VertSliceCalcs!D40']=xcf_VertSliceCalcs_D40

#Calculations!$D$14*D23
def xcf_VertSliceCalcs_D41(): 
    try:      
        return vcell("Calculations!$D$14","VertSliceCalcs")*vcell("D23","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D41')
    return None
xfunctions['VertSliceCalcs!D41']=xcf_VertSliceCalcs_D41

#Calculations!$D$14*D24
def xcf_VertSliceCalcs_D42(): 
    try:      
        return vcell("Calculations!$D$14","VertSliceCalcs")*vcell("D24","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D42')
    return None
xfunctions['VertSliceCalcs!D42']=xcf_VertSliceCalcs_D42

#Calculations!$D$15*D25
def xcf_VertSliceCalcs_D43(): 
    try:      
        return vcell("Calculations!$D$15","VertSliceCalcs")*vcell("D25","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D43')
    return None
xfunctions['VertSliceCalcs!D43']=xcf_VertSliceCalcs_D43

#Calculations!$D$15*D26
def xcf_VertSliceCalcs_D44(): 
    try:      
        return vcell("Calculations!$D$15","VertSliceCalcs")*vcell("D26","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D44')
    return None
xfunctions['VertSliceCalcs!D44']=xcf_VertSliceCalcs_D44

#Calculations!$D$15*D27
def xcf_VertSliceCalcs_D45(): 
    try:      
        return vcell("Calculations!$D$15","VertSliceCalcs")*vcell("D27","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D45')
    return None
xfunctions['VertSliceCalcs!D45']=xcf_VertSliceCalcs_D45

#Calculations!$D$16*D28
def xcf_VertSliceCalcs_D46(): 
    try:      
        return vcell("Calculations!$D$16","VertSliceCalcs")*vcell("D28","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D46')
    return None
xfunctions['VertSliceCalcs!D46']=xcf_VertSliceCalcs_D46

#Calculations!$D$16*D29
def xcf_VertSliceCalcs_D47(): 
    try:      
        return vcell("Calculations!$D$16","VertSliceCalcs")*vcell("D29","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D47')
    return None
xfunctions['VertSliceCalcs!D47']=xcf_VertSliceCalcs_D47

#Calculations!$D$16*D30
def xcf_VertSliceCalcs_D48(): 
    try:      
        return vcell("Calculations!$D$16","VertSliceCalcs")*vcell("D30","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D48')
    return None
xfunctions['VertSliceCalcs!D48']=xcf_VertSliceCalcs_D48

#Calculations!$D$17*D31
def xcf_VertSliceCalcs_D49(): 
    try:      
        return vcell("Calculations!$D$17","VertSliceCalcs")*vcell("D31","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D49')
    return None
xfunctions['VertSliceCalcs!D49']=xcf_VertSliceCalcs_D49

#Calculations!$D$17*D32
def xcf_VertSliceCalcs_D50(): 
    try:      
        return vcell("Calculations!$D$17","VertSliceCalcs")*vcell("D32","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D50')
    return None
xfunctions['VertSliceCalcs!D50']=xcf_VertSliceCalcs_D50

#Calculations!$D$17*D33
def xcf_VertSliceCalcs_D51(): 
    try:      
        return vcell("Calculations!$D$17","VertSliceCalcs")*vcell("D33","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D51')
    return None
xfunctions['VertSliceCalcs!D51']=xcf_VertSliceCalcs_D51

#Calculations!$D$18*D34
def xcf_VertSliceCalcs_D52(): 
    try:      
        return vcell("Calculations!$D$18","VertSliceCalcs")*vcell("D34","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D52')
    return None
xfunctions['VertSliceCalcs!D52']=xcf_VertSliceCalcs_D52

#Calculations!$D$18*D35
def xcf_VertSliceCalcs_D53(): 
    try:      
        return vcell("Calculations!$D$18","VertSliceCalcs")*vcell("D35","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D53')
    return None
xfunctions['VertSliceCalcs!D53']=xcf_VertSliceCalcs_D53

#Calculations!$D$18*D36
def xcf_VertSliceCalcs_D54(): 
    try:      
        return vcell("Calculations!$D$18","VertSliceCalcs")*vcell("D36","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D54')
    return None
xfunctions['VertSliceCalcs!D54']=xcf_VertSliceCalcs_D54

#SUM(D40:D54)
def xcf_VertSliceCalcs_D55(): 
    try:      
        return SUM(vcell("D40:D54","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D55')
    return None
xfunctions['VertSliceCalcs!D55']=xcf_VertSliceCalcs_D55

#Input_Geometry!$G72
def xcf_VertSliceCalcs_B59(): 
    try:      
        return vcell("Input_Geometry!$G72","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B59')
    return None
xfunctions['VertSliceCalcs!B59']=xcf_VertSliceCalcs_B59

#Input_Geometry!$G73
def xcf_VertSliceCalcs_C59(): 
    try:      
        return vcell("Input_Geometry!$G73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C59')
    return None
xfunctions['VertSliceCalcs!C59']=xcf_VertSliceCalcs_C59

#Input_Geometry!$G74
def xcf_VertSliceCalcs_D59(): 
    try:      
        return vcell("Input_Geometry!$G74","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D59')
    return None
xfunctions['VertSliceCalcs!D59']=xcf_VertSliceCalcs_D59

#Input_Geometry!$G75
def xcf_VertSliceCalcs_E59(): 
    try:      
        return vcell("Input_Geometry!$G75","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E59')
    return None
xfunctions['VertSliceCalcs!E59']=xcf_VertSliceCalcs_E59

#Input_Geometry!$G76
def xcf_VertSliceCalcs_F59(): 
    try:      
        return vcell("Input_Geometry!$G76","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F59')
    return None
xfunctions['VertSliceCalcs!F59']=xcf_VertSliceCalcs_F59

#Input_Geometry!$G77
def xcf_VertSliceCalcs_G59(): 
    try:      
        return vcell("Input_Geometry!$G77","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G59')
    return None
xfunctions['VertSliceCalcs!G59']=xcf_VertSliceCalcs_G59

#Input_Geometry!$G78
def xcf_VertSliceCalcs_H59(): 
    try:      
        return vcell("Input_Geometry!$G78","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H59')
    return None
xfunctions['VertSliceCalcs!H59']=xcf_VertSliceCalcs_H59

#Input_Geometry!$G79
def xcf_VertSliceCalcs_I59(): 
    try:      
        return vcell("Input_Geometry!$G79","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I59')
    return None
xfunctions['VertSliceCalcs!I59']=xcf_VertSliceCalcs_I59

#Input_Geometry!$G80
def xcf_VertSliceCalcs_J59(): 
    try:      
        return vcell("Input_Geometry!$G80","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J59')
    return None
xfunctions['VertSliceCalcs!J59']=xcf_VertSliceCalcs_J59

#Input_Geometry!$G81
def xcf_VertSliceCalcs_K59(): 
    try:      
        return vcell("Input_Geometry!$G81","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K59')
    return None
xfunctions['VertSliceCalcs!K59']=xcf_VertSliceCalcs_K59

#Input_Geometry!$G82
def xcf_VertSliceCalcs_L59(): 
    try:      
        return vcell("Input_Geometry!$G82","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L59')
    return None
xfunctions['VertSliceCalcs!L59']=xcf_VertSliceCalcs_L59

#Input_Geometry!$G83
def xcf_VertSliceCalcs_M59(): 
    try:      
        return vcell("Input_Geometry!$G83","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M59')
    return None
xfunctions['VertSliceCalcs!M59']=xcf_VertSliceCalcs_M59

#Input_Geometry!$G84
def xcf_VertSliceCalcs_N59(): 
    try:      
        return vcell("Input_Geometry!$G84","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N59')
    return None
xfunctions['VertSliceCalcs!N59']=xcf_VertSliceCalcs_N59

#Input_Geometry!$G85
def xcf_VertSliceCalcs_O59(): 
    try:      
        return vcell("Input_Geometry!$G85","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O59')
    return None
xfunctions['VertSliceCalcs!O59']=xcf_VertSliceCalcs_O59

#Input_Geometry!$G86
def xcf_VertSliceCalcs_P59(): 
    try:      
        return vcell("Input_Geometry!$G86","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P59')
    return None
xfunctions['VertSliceCalcs!P59']=xcf_VertSliceCalcs_P59

#IF(IF(B$59>0,-B$59*$D22*TAN(RADIANS(Calculations!$D$19)),-B$59*$D22*TAN(RADIANS(Calculations!$D$9)))>(0.5*$J4),0.5*$J4,(IF(B$59>0,-B$59*$D22*TAN(RADIANS(Calculations!$D$19)),-B$59*$D22*TAN(RADIANS(Calculations!$D$9)))))
def xcf_VertSliceCalcs_B60(): 
    try:      
        return IF(IF(vcell("B$59","VertSliceCalcs")>0,-vcell("B$59","VertSliceCalcs")*vcell("$D22","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("B$59","VertSliceCalcs")*vcell("$D22","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))>(0.5*vcell("$J4","VertSliceCalcs")),0.5*vcell("$J4","VertSliceCalcs"),(IF(vcell("B$59","VertSliceCalcs")>0,-vcell("B$59","VertSliceCalcs")*vcell("$D22","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("B$59","VertSliceCalcs")*vcell("$D22","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B60')
    return None
xfunctions['VertSliceCalcs!B60']=xcf_VertSliceCalcs_B60

#IF(IF(C$59>0,-C$59*$D23*TAN(RADIANS(Calculations!$D$19)),-C$59*$D23*TAN(RADIANS(Calculations!$D$9)))>(0.5*$J5),0.5*$J5,(IF(C$59>0,-C$59*$D23*TAN(RADIANS(Calculations!$D$19)),-C$59*$D23*TAN(RADIANS(Calculations!$D$9)))))
def xcf_VertSliceCalcs_C60(): 
    try:      
        return IF(IF(vcell("C$59","VertSliceCalcs")>0,-vcell("C$59","VertSliceCalcs")*vcell("$D23","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("C$59","VertSliceCalcs")*vcell("$D23","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))>(0.5*vcell("$J5","VertSliceCalcs")),0.5*vcell("$J5","VertSliceCalcs"),(IF(vcell("C$59","VertSliceCalcs")>0,-vcell("C$59","VertSliceCalcs")*vcell("$D23","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("C$59","VertSliceCalcs")*vcell("$D23","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C60')
    return None
xfunctions['VertSliceCalcs!C60']=xcf_VertSliceCalcs_C60

#IF(IF(D$59>0,-D$59*$D24*TAN(RADIANS(Calculations!$D$19)),-D$59*$D24*TAN(RADIANS(Calculations!$D$9)))>(0.5*$J6),0.5*$J6,(IF(D$59>0,-D$59*$D24*TAN(RADIANS(Calculations!$D$19)),-D$59*$D24*TAN(RADIANS(Calculations!$D$9)))))
def xcf_VertSliceCalcs_D60(): 
    try:      
        return IF(IF(vcell("D$59","VertSliceCalcs")>0,-vcell("D$59","VertSliceCalcs")*vcell("$D24","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("D$59","VertSliceCalcs")*vcell("$D24","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))>(0.5*vcell("$J6","VertSliceCalcs")),0.5*vcell("$J6","VertSliceCalcs"),(IF(vcell("D$59","VertSliceCalcs")>0,-vcell("D$59","VertSliceCalcs")*vcell("$D24","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$19","VertSliceCalcs"))),-vcell("D$59","VertSliceCalcs")*vcell("$D24","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D60')
    return None
xfunctions['VertSliceCalcs!D60']=xcf_VertSliceCalcs_D60

#IF(IF(E$59>0,-E$59*$D25*TAN(RADIANS(Calculations!$D$20)),-E$59*$D25*TAN(RADIANS(Calculations!$D$10)))>(0.5*$J7),0.5*$J7,(IF(E$59>0,-E$59*$D25*TAN(RADIANS(Calculations!$D$20)),-E$59*$D25*TAN(RADIANS(Calculations!$D$10)))))
def xcf_VertSliceCalcs_E60(): 
    try:      
        return IF(IF(vcell("E$59","VertSliceCalcs")>0,-vcell("E$59","VertSliceCalcs")*vcell("$D25","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("E$59","VertSliceCalcs")*vcell("$D25","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))>(0.5*vcell("$J7","VertSliceCalcs")),0.5*vcell("$J7","VertSliceCalcs"),(IF(vcell("E$59","VertSliceCalcs")>0,-vcell("E$59","VertSliceCalcs")*vcell("$D25","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("E$59","VertSliceCalcs")*vcell("$D25","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E60')
    return None
xfunctions['VertSliceCalcs!E60']=xcf_VertSliceCalcs_E60

#IF(IF(F$59>0,-F$59*$D26*TAN(RADIANS(Calculations!$D$20)),-F$59*$D26*TAN(RADIANS(Calculations!$D$10)))>(0.5*$J8),0.5*$J8,(IF(F$59>0,-F$59*$D26*TAN(RADIANS(Calculations!$D$20)),-F$59*$D26*TAN(RADIANS(Calculations!$D$10)))))
def xcf_VertSliceCalcs_F60(): 
    try:      
        return IF(IF(vcell("F$59","VertSliceCalcs")>0,-vcell("F$59","VertSliceCalcs")*vcell("$D26","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("F$59","VertSliceCalcs")*vcell("$D26","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))>(0.5*vcell("$J8","VertSliceCalcs")),0.5*vcell("$J8","VertSliceCalcs"),(IF(vcell("F$59","VertSliceCalcs")>0,-vcell("F$59","VertSliceCalcs")*vcell("$D26","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("F$59","VertSliceCalcs")*vcell("$D26","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F60')
    return None
xfunctions['VertSliceCalcs!F60']=xcf_VertSliceCalcs_F60

#IF(IF(G$59>0,-G$59*$D27*TAN(RADIANS(Calculations!$D$20)),-G$59*$D27*TAN(RADIANS(Calculations!$D$10)))>(0.5*$J9),0.5*$J9,(IF(G$59>0,-G$59*$D27*TAN(RADIANS(Calculations!$D$20)),-G$59*$D27*TAN(RADIANS(Calculations!$D$10)))))
def xcf_VertSliceCalcs_G60(): 
    try:      
        return IF(IF(vcell("G$59","VertSliceCalcs")>0,-vcell("G$59","VertSliceCalcs")*vcell("$D27","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("G$59","VertSliceCalcs")*vcell("$D27","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))>(0.5*vcell("$J9","VertSliceCalcs")),0.5*vcell("$J9","VertSliceCalcs"),(IF(vcell("G$59","VertSliceCalcs")>0,-vcell("G$59","VertSliceCalcs")*vcell("$D27","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$20","VertSliceCalcs"))),-vcell("G$59","VertSliceCalcs")*vcell("$D27","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G60')
    return None
xfunctions['VertSliceCalcs!G60']=xcf_VertSliceCalcs_G60

#IF(IF(H$59>0,-H$59*$D28*TAN(RADIANS(Calculations!$D$21)),-H$59*$D28*TAN(RADIANS(Calculations!$D$11)))>(0.5*$J10),0.5*$J10,(IF(H$59>0,-H$59*$D28*TAN(RADIANS(Calculations!$D$21)),-H$59*$D28*TAN(RADIANS(Calculations!$D$11)))))
def xcf_VertSliceCalcs_H60(): 
    try:      
        return IF(IF(vcell("H$59","VertSliceCalcs")>0,-vcell("H$59","VertSliceCalcs")*vcell("$D28","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("H$59","VertSliceCalcs")*vcell("$D28","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))>(0.5*vcell("$J10","VertSliceCalcs")),0.5*vcell("$J10","VertSliceCalcs"),(IF(vcell("H$59","VertSliceCalcs")>0,-vcell("H$59","VertSliceCalcs")*vcell("$D28","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("H$59","VertSliceCalcs")*vcell("$D28","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H60')
    return None
xfunctions['VertSliceCalcs!H60']=xcf_VertSliceCalcs_H60

#IF(IF(I$59>0,-I$59*$D29*TAN(RADIANS(Calculations!$D$21)),-I$59*$D29*TAN(RADIANS(Calculations!$D$11)))>(0.5*$J11),0.5*$J11,(IF(I$59>0,-I$59*$D29*TAN(RADIANS(Calculations!$D$21)),-I$59*$D29*TAN(RADIANS(Calculations!$D$11)))))
def xcf_VertSliceCalcs_I60(): 
    try:      
        return IF(IF(vcell("I$59","VertSliceCalcs")>0,-vcell("I$59","VertSliceCalcs")*vcell("$D29","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("I$59","VertSliceCalcs")*vcell("$D29","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))>(0.5*vcell("$J11","VertSliceCalcs")),0.5*vcell("$J11","VertSliceCalcs"),(IF(vcell("I$59","VertSliceCalcs")>0,-vcell("I$59","VertSliceCalcs")*vcell("$D29","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("I$59","VertSliceCalcs")*vcell("$D29","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I60')
    return None
xfunctions['VertSliceCalcs!I60']=xcf_VertSliceCalcs_I60

#IF(IF(J$59>0,-J$59*$D30*TAN(RADIANS(Calculations!$D$21)),-J$59*$D30*TAN(RADIANS(Calculations!$D$11)))>(0.5*$J12),0.5*$J12,(IF(J$59>0,-J$59*$D30*TAN(RADIANS(Calculations!$D$21)),-J$59*$D30*TAN(RADIANS(Calculations!$D$11)))))
def xcf_VertSliceCalcs_J60(): 
    try:      
        return IF(IF(vcell("J$59","VertSliceCalcs")>0,-vcell("J$59","VertSliceCalcs")*vcell("$D30","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("J$59","VertSliceCalcs")*vcell("$D30","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))>(0.5*vcell("$J12","VertSliceCalcs")),0.5*vcell("$J12","VertSliceCalcs"),(IF(vcell("J$59","VertSliceCalcs")>0,-vcell("J$59","VertSliceCalcs")*vcell("$D30","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$21","VertSliceCalcs"))),-vcell("J$59","VertSliceCalcs")*vcell("$D30","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J60')
    return None
xfunctions['VertSliceCalcs!J60']=xcf_VertSliceCalcs_J60

#IF(IF(K$59>0,-K$59*$D31*TAN(RADIANS(Calculations!$D$22)),-K$59*$D31*TAN(RADIANS(Calculations!$D$12)))>(0.5*$J13),0.5*$J13,(IF(K$59>0,-K$59*$D31*TAN(RADIANS(Calculations!$D$22)),-K$59*$D31*TAN(RADIANS(Calculations!$D$12)))))
def xcf_VertSliceCalcs_K60(): 
    try:      
        return IF(IF(vcell("K$59","VertSliceCalcs")>0,-vcell("K$59","VertSliceCalcs")*vcell("$D31","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("K$59","VertSliceCalcs")*vcell("$D31","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))>(0.5*vcell("$J13","VertSliceCalcs")),0.5*vcell("$J13","VertSliceCalcs"),(IF(vcell("K$59","VertSliceCalcs")>0,-vcell("K$59","VertSliceCalcs")*vcell("$D31","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("K$59","VertSliceCalcs")*vcell("$D31","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K60')
    return None
xfunctions['VertSliceCalcs!K60']=xcf_VertSliceCalcs_K60

#IF(IF(L$59>0,-L$59*$D32*TAN(RADIANS(Calculations!$D$22)),-L$59*$D32*TAN(RADIANS(Calculations!$D$12)))>(0.5*$J14),0.5*$J14,(IF(L$59>0,-L$59*$D32*TAN(RADIANS(Calculations!$D$22)),-L$59*$D32*TAN(RADIANS(Calculations!$D$12)))))
def xcf_VertSliceCalcs_L60(): 
    try:      
        return IF(IF(vcell("L$59","VertSliceCalcs")>0,-vcell("L$59","VertSliceCalcs")*vcell("$D32","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("L$59","VertSliceCalcs")*vcell("$D32","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))>(0.5*vcell("$J14","VertSliceCalcs")),0.5*vcell("$J14","VertSliceCalcs"),(IF(vcell("L$59","VertSliceCalcs")>0,-vcell("L$59","VertSliceCalcs")*vcell("$D32","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("L$59","VertSliceCalcs")*vcell("$D32","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L60')
    return None
xfunctions['VertSliceCalcs!L60']=xcf_VertSliceCalcs_L60

#IF(IF(M$59>0,-M$59*$D33*TAN(RADIANS(Calculations!$D$22)),-M$59*$D33*TAN(RADIANS(Calculations!$D$12)))>(0.5*$J15),0.5*$J15,(IF(M$59>0,-M$59*$D33*TAN(RADIANS(Calculations!$D$22)),-M$59*$D33*TAN(RADIANS(Calculations!$D$12)))))
def xcf_VertSliceCalcs_M60(): 
    try:      
        return IF(IF(vcell("M$59","VertSliceCalcs")>0,-vcell("M$59","VertSliceCalcs")*vcell("$D33","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("M$59","VertSliceCalcs")*vcell("$D33","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))>(0.5*vcell("$J15","VertSliceCalcs")),0.5*vcell("$J15","VertSliceCalcs"),(IF(vcell("M$59","VertSliceCalcs")>0,-vcell("M$59","VertSliceCalcs")*vcell("$D33","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$22","VertSliceCalcs"))),-vcell("M$59","VertSliceCalcs")*vcell("$D33","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M60')
    return None
xfunctions['VertSliceCalcs!M60']=xcf_VertSliceCalcs_M60

#IF(IF(N$59>0,-N$59*$D34*TAN(RADIANS(Calculations!$D$23)),-N$59*$D34*TAN(RADIANS(Calculations!$D$13)))>(0.5*$J16),0.5*$J16,(IF(N$59>0,-N$59*$D34*TAN(RADIANS(Calculations!$D$23)),-N$59*$D34*TAN(RADIANS(Calculations!$D$13)))))
def xcf_VertSliceCalcs_N60(): 
    try:      
        return IF(IF(vcell("N$59","VertSliceCalcs")>0,-vcell("N$59","VertSliceCalcs")*vcell("$D34","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("N$59","VertSliceCalcs")*vcell("$D34","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))>(0.5*vcell("$J16","VertSliceCalcs")),0.5*vcell("$J16","VertSliceCalcs"),(IF(vcell("N$59","VertSliceCalcs")>0,-vcell("N$59","VertSliceCalcs")*vcell("$D34","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("N$59","VertSliceCalcs")*vcell("$D34","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N60')
    return None
xfunctions['VertSliceCalcs!N60']=xcf_VertSliceCalcs_N60

#IF(IF(O$59>0,-O$59*$D35*TAN(RADIANS(Calculations!$D$23)),-O$59*$D35*TAN(RADIANS(Calculations!$D$13)))>(0.5*$J17),0.5*$J17,(IF(O$59>0,-O$59*$D35*TAN(RADIANS(Calculations!$D$23)),-O$59*$D35*TAN(RADIANS(Calculations!$D$13)))))
def xcf_VertSliceCalcs_O60(): 
    try:      
        return IF(IF(vcell("O$59","VertSliceCalcs")>0,-vcell("O$59","VertSliceCalcs")*vcell("$D35","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("O$59","VertSliceCalcs")*vcell("$D35","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))>(0.5*vcell("$J17","VertSliceCalcs")),0.5*vcell("$J17","VertSliceCalcs"),(IF(vcell("O$59","VertSliceCalcs")>0,-vcell("O$59","VertSliceCalcs")*vcell("$D35","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("O$59","VertSliceCalcs")*vcell("$D35","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O60')
    return None
xfunctions['VertSliceCalcs!O60']=xcf_VertSliceCalcs_O60

#IF(IF(P$59>0,-P$59*$D36*TAN(RADIANS(Calculations!$D$23)),-P$59*$D36*TAN(RADIANS(Calculations!$D$13)))>(0.5*$J18),0.5*$J18,(IF(P$59>0,-P$59*$D36*TAN(RADIANS(Calculations!$D$23)),-P$59*$D36*TAN(RADIANS(Calculations!$D$13)))))
def xcf_VertSliceCalcs_P60(): 
    try:      
        return IF(IF(vcell("P$59","VertSliceCalcs")>0,-vcell("P$59","VertSliceCalcs")*vcell("$D36","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("P$59","VertSliceCalcs")*vcell("$D36","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))>(0.5*vcell("$J18","VertSliceCalcs")),0.5*vcell("$J18","VertSliceCalcs"),(IF(vcell("P$59","VertSliceCalcs")>0,-vcell("P$59","VertSliceCalcs")*vcell("$D36","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$23","VertSliceCalcs"))),-vcell("P$59","VertSliceCalcs")*vcell("$D36","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs"))))))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P60')
    return None
xfunctions['VertSliceCalcs!P60']=xcf_VertSliceCalcs_P60

#SUM(B60:P60)
def xcf_VertSliceCalcs_Q60(): 
    try:      
        return SUM(vcell("B60:P60","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q60')
    return None
xfunctions['VertSliceCalcs!Q60']=xcf_VertSliceCalcs_Q60

#SUM(B61:P61)
def xcf_VertSliceCalcs_Q61(): 
    try:      
        return SUM(vcell("B61:P61","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q61')
    return None
xfunctions['VertSliceCalcs!Q61']=xcf_VertSliceCalcs_Q61

#B63*TAN(RADIANS(Calculations!$D$9))
def xcf_VertSliceCalcs_B62(): 
    try:      
        return vcell("B63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B62')
    return None
xfunctions['VertSliceCalcs!B62']=xcf_VertSliceCalcs_B62

#C63*TAN(RADIANS(Calculations!$D$9))
def xcf_VertSliceCalcs_C62(): 
    try:      
        return vcell("C63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C62')
    return None
xfunctions['VertSliceCalcs!C62']=xcf_VertSliceCalcs_C62

#D63*TAN(RADIANS(Calculations!$D$9))
def xcf_VertSliceCalcs_D62(): 
    try:      
        return vcell("D63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$9","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D62')
    return None
xfunctions['VertSliceCalcs!D62']=xcf_VertSliceCalcs_D62

#E63*TAN(RADIANS(Calculations!$D$10))
def xcf_VertSliceCalcs_E62(): 
    try:      
        return vcell("E63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E62')
    return None
xfunctions['VertSliceCalcs!E62']=xcf_VertSliceCalcs_E62

#F63*TAN(RADIANS(Calculations!$D$10))
def xcf_VertSliceCalcs_F62(): 
    try:      
        return vcell("F63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F62')
    return None
xfunctions['VertSliceCalcs!F62']=xcf_VertSliceCalcs_F62

#G63*TAN(RADIANS(Calculations!$D$10))
def xcf_VertSliceCalcs_G62(): 
    try:      
        return vcell("G63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$10","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G62')
    return None
xfunctions['VertSliceCalcs!G62']=xcf_VertSliceCalcs_G62

#H63*TAN(RADIANS(Calculations!$D$11))
def xcf_VertSliceCalcs_H62(): 
    try:      
        return vcell("H63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H62')
    return None
xfunctions['VertSliceCalcs!H62']=xcf_VertSliceCalcs_H62

#I63*TAN(RADIANS(Calculations!$D$11))
def xcf_VertSliceCalcs_I62(): 
    try:      
        return vcell("I63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I62')
    return None
xfunctions['VertSliceCalcs!I62']=xcf_VertSliceCalcs_I62

#J63*TAN(RADIANS(Calculations!$D$11))
def xcf_VertSliceCalcs_J62(): 
    try:      
        return vcell("J63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$11","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J62')
    return None
xfunctions['VertSliceCalcs!J62']=xcf_VertSliceCalcs_J62

#K63*TAN(RADIANS(Calculations!$D$12))
def xcf_VertSliceCalcs_K62(): 
    try:      
        return vcell("K63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K62')
    return None
xfunctions['VertSliceCalcs!K62']=xcf_VertSliceCalcs_K62

#L63*TAN(RADIANS(Calculations!$D$12))
def xcf_VertSliceCalcs_L62(): 
    try:      
        return vcell("L63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L62')
    return None
xfunctions['VertSliceCalcs!L62']=xcf_VertSliceCalcs_L62

#M63*TAN(RADIANS(Calculations!$D$12))
def xcf_VertSliceCalcs_M62(): 
    try:      
        return vcell("M63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$12","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M62')
    return None
xfunctions['VertSliceCalcs!M62']=xcf_VertSliceCalcs_M62

#N63*TAN(RADIANS(Calculations!$D$13))
def xcf_VertSliceCalcs_N62(): 
    try:      
        return vcell("N63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N62')
    return None
xfunctions['VertSliceCalcs!N62']=xcf_VertSliceCalcs_N62

#O63*TAN(RADIANS(Calculations!$D$13))
def xcf_VertSliceCalcs_O62(): 
    try:      
        return vcell("O63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O62')
    return None
xfunctions['VertSliceCalcs!O62']=xcf_VertSliceCalcs_O62

#P63*TAN(RADIANS(Calculations!$D$13))
def xcf_VertSliceCalcs_P62(): 
    try:      
        return vcell("P63","VertSliceCalcs")*TAN(RADIANS(vcell("Calculations!$D$13","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P62')
    return None
xfunctions['VertSliceCalcs!P62']=xcf_VertSliceCalcs_P62

#SUM(B62:P62)
def xcf_VertSliceCalcs_Q62(): 
    try:      
        return SUM(vcell("B62:P62","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q62')
    return None
xfunctions['VertSliceCalcs!Q62']=xcf_VertSliceCalcs_Q62

#$J4*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_B63(): 
    try:      
        return vcell("$J4","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B63')
    return None
xfunctions['VertSliceCalcs!B63']=xcf_VertSliceCalcs_B63

#$J5*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_C63(): 
    try:      
        return vcell("$J5","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C63')
    return None
xfunctions['VertSliceCalcs!C63']=xcf_VertSliceCalcs_C63

#$J6*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_D63(): 
    try:      
        return vcell("$J6","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D63')
    return None
xfunctions['VertSliceCalcs!D63']=xcf_VertSliceCalcs_D63

#$J7*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_E63(): 
    try:      
        return vcell("$J7","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E63')
    return None
xfunctions['VertSliceCalcs!E63']=xcf_VertSliceCalcs_E63

#$J8*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_F63(): 
    try:      
        return vcell("$J8","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F63')
    return None
xfunctions['VertSliceCalcs!F63']=xcf_VertSliceCalcs_F63

#$J9*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_G63(): 
    try:      
        return vcell("$J9","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G63')
    return None
xfunctions['VertSliceCalcs!G63']=xcf_VertSliceCalcs_G63

#$J10*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_H63(): 
    try:      
        return vcell("$J10","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H63')
    return None
xfunctions['VertSliceCalcs!H63']=xcf_VertSliceCalcs_H63

#$J11*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_I63(): 
    try:      
        return vcell("$J11","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I63')
    return None
xfunctions['VertSliceCalcs!I63']=xcf_VertSliceCalcs_I63

#$J12*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_J63(): 
    try:      
        return vcell("$J12","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J63')
    return None
xfunctions['VertSliceCalcs!J63']=xcf_VertSliceCalcs_J63

#$J13*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_K63(): 
    try:      
        return vcell("$J13","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K63')
    return None
xfunctions['VertSliceCalcs!K63']=xcf_VertSliceCalcs_K63

#$J14*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_L63(): 
    try:      
        return vcell("$J14","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L63')
    return None
xfunctions['VertSliceCalcs!L63']=xcf_VertSliceCalcs_L63

#$J15*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_M63(): 
    try:      
        return vcell("$J15","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M63')
    return None
xfunctions['VertSliceCalcs!M63']=xcf_VertSliceCalcs_M63

#$J16*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_N63(): 
    try:      
        return vcell("$J16","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N63')
    return None
xfunctions['VertSliceCalcs!N63']=xcf_VertSliceCalcs_N63

#$J17*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_O63(): 
    try:      
        return vcell("$J17","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O63')
    return None
xfunctions['VertSliceCalcs!O63']=xcf_VertSliceCalcs_O63

#$J18*COS(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_P63(): 
    try:      
        return vcell("$J18","VertSliceCalcs")*COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P63')
    return None
xfunctions['VertSliceCalcs!P63']=xcf_VertSliceCalcs_P63

#B63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_B64(): 
    try:      
        return vcell("B63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B64')
    return None
xfunctions['VertSliceCalcs!B64']=xcf_VertSliceCalcs_B64

#C63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_C64(): 
    try:      
        return vcell("C63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C64')
    return None
xfunctions['VertSliceCalcs!C64']=xcf_VertSliceCalcs_C64

#D63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_D64(): 
    try:      
        return vcell("D63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D64')
    return None
xfunctions['VertSliceCalcs!D64']=xcf_VertSliceCalcs_D64

#E63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_E64(): 
    try:      
        return vcell("E63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E64')
    return None
xfunctions['VertSliceCalcs!E64']=xcf_VertSliceCalcs_E64

#F63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_F64(): 
    try:      
        return vcell("F63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F64')
    return None
xfunctions['VertSliceCalcs!F64']=xcf_VertSliceCalcs_F64

#G63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_G64(): 
    try:      
        return vcell("G63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G64')
    return None
xfunctions['VertSliceCalcs!G64']=xcf_VertSliceCalcs_G64

#H63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_H64(): 
    try:      
        return vcell("H63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H64')
    return None
xfunctions['VertSliceCalcs!H64']=xcf_VertSliceCalcs_H64

#I63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_I64(): 
    try:      
        return vcell("I63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I64')
    return None
xfunctions['VertSliceCalcs!I64']=xcf_VertSliceCalcs_I64

#J63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_J64(): 
    try:      
        return vcell("J63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J64')
    return None
xfunctions['VertSliceCalcs!J64']=xcf_VertSliceCalcs_J64

#K63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_K64(): 
    try:      
        return vcell("K63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K64')
    return None
xfunctions['VertSliceCalcs!K64']=xcf_VertSliceCalcs_K64

#L63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_L64(): 
    try:      
        return vcell("L63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L64')
    return None
xfunctions['VertSliceCalcs!L64']=xcf_VertSliceCalcs_L64

#M63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_M64(): 
    try:      
        return vcell("M63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M64')
    return None
xfunctions['VertSliceCalcs!M64']=xcf_VertSliceCalcs_M64

#N63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_N64(): 
    try:      
        return vcell("N63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N64')
    return None
xfunctions['VertSliceCalcs!N64']=xcf_VertSliceCalcs_N64

#O63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_O64(): 
    try:      
        return vcell("O63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O64')
    return None
xfunctions['VertSliceCalcs!O64']=xcf_VertSliceCalcs_O64

#P63*SIN(RADIANS(Input_Geometry!$E$140))
def xcf_VertSliceCalcs_P64(): 
    try:      
        return vcell("P63","VertSliceCalcs")*SIN(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P64')
    return None
xfunctions['VertSliceCalcs!P64']=xcf_VertSliceCalcs_P64

#SUM(B64:P64)
def xcf_VertSliceCalcs_Q64(): 
    try:      
        return SUM(vcell("B64:P64","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q64')
    return None
xfunctions['VertSliceCalcs!Q64']=xcf_VertSliceCalcs_Q64

#Calculations!$B73
def xcf_VertSliceCalcs_B65(): 
    try:      
        return vcell("Calculations!$B73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_B65')
    return None
xfunctions['VertSliceCalcs!B65']=xcf_VertSliceCalcs_B65

#Calculations!$B73
def xcf_VertSliceCalcs_C65(): 
    try:      
        return vcell("Calculations!$B73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_C65')
    return None
xfunctions['VertSliceCalcs!C65']=xcf_VertSliceCalcs_C65

#Calculations!$B73
def xcf_VertSliceCalcs_D65(): 
    try:      
        return vcell("Calculations!$B73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_D65')
    return None
xfunctions['VertSliceCalcs!D65']=xcf_VertSliceCalcs_D65

#Calculations!$C73
def xcf_VertSliceCalcs_E65(): 
    try:      
        return vcell("Calculations!$C73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_E65')
    return None
xfunctions['VertSliceCalcs!E65']=xcf_VertSliceCalcs_E65

#Calculations!$C73
def xcf_VertSliceCalcs_F65(): 
    try:      
        return vcell("Calculations!$C73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_F65')
    return None
xfunctions['VertSliceCalcs!F65']=xcf_VertSliceCalcs_F65

#Calculations!$C73
def xcf_VertSliceCalcs_G65(): 
    try:      
        return vcell("Calculations!$C73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_G65')
    return None
xfunctions['VertSliceCalcs!G65']=xcf_VertSliceCalcs_G65

#Calculations!$D73
def xcf_VertSliceCalcs_H65(): 
    try:      
        return vcell("Calculations!$D73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_H65')
    return None
xfunctions['VertSliceCalcs!H65']=xcf_VertSliceCalcs_H65

#Calculations!$D73
def xcf_VertSliceCalcs_I65(): 
    try:      
        return vcell("Calculations!$D73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_I65')
    return None
xfunctions['VertSliceCalcs!I65']=xcf_VertSliceCalcs_I65

#Calculations!$D73
def xcf_VertSliceCalcs_J65(): 
    try:      
        return vcell("Calculations!$D73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_J65')
    return None
xfunctions['VertSliceCalcs!J65']=xcf_VertSliceCalcs_J65

#Calculations!$E73
def xcf_VertSliceCalcs_K65(): 
    try:      
        return vcell("Calculations!$E73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_K65')
    return None
xfunctions['VertSliceCalcs!K65']=xcf_VertSliceCalcs_K65

#Calculations!$E73
def xcf_VertSliceCalcs_L65(): 
    try:      
        return vcell("Calculations!$E73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_L65')
    return None
xfunctions['VertSliceCalcs!L65']=xcf_VertSliceCalcs_L65

#Calculations!$E73
def xcf_VertSliceCalcs_M65(): 
    try:      
        return vcell("Calculations!$E73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_M65')
    return None
xfunctions['VertSliceCalcs!M65']=xcf_VertSliceCalcs_M65

#Calculations!$F73
def xcf_VertSliceCalcs_N65(): 
    try:      
        return vcell("Calculations!$F73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_N65')
    return None
xfunctions['VertSliceCalcs!N65']=xcf_VertSliceCalcs_N65

#Calculations!$F73
def xcf_VertSliceCalcs_O65(): 
    try:      
        return vcell("Calculations!$F73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_O65')
    return None
xfunctions['VertSliceCalcs!O65']=xcf_VertSliceCalcs_O65

#Calculations!$F73
def xcf_VertSliceCalcs_P65(): 
    try:      
        return vcell("Calculations!$F73","VertSliceCalcs")
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_P65')
    return None
xfunctions['VertSliceCalcs!P65']=xcf_VertSliceCalcs_P65

#COS(RADIANS(Input_Geometry!$E$140))*($D$55+$Q$62-$Q$60)/($Q$64-$Q$61)
def xcf_VertSliceCalcs_Q66(): 
    try:      
        return COS(RADIANS(vcell("Input_Geometry!$E$140","VertSliceCalcs")))*(vcell("$D$55","VertSliceCalcs")+vcell("$Q$62","VertSliceCalcs")-vcell("$Q$60","VertSliceCalcs"))/(vcell("$Q$64","VertSliceCalcs")-vcell("$Q$61","VertSliceCalcs"))
    except Exception as ex:
        print(ex,'on xcf_VertSliceCalcs_Q66')
    return None
xfunctions['VertSliceCalcs!Q66']=xcf_VertSliceCalcs_Q66
