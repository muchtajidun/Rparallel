#Data in the mstData from runing the main IO_main file.
class(mstData)
colnames(mstData)
#Idea is built the tree of related locations.
#root , node with no parents
mstData$LOC_PARENT_ID
mstData$LOC_ID

  locations
  loc<-unique(locations)
  loc
  setdiff(locPa,loc)
LT<-mstData$LEAD_TIME
unique(LT)
#All existing paths
  