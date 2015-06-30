#!/usr/bin/Rscript
########################################################################################################################
########################################################################################################################
########################################################################################################################
#
# Copyright ©2015 Accenture and/or its affiliates.  All Rights Reserved.  
# You may not use, copy, modify, and/or distribute this code and/or its documentation without permission from Accenture.
# Please contact the Advanced Analytics-Operations Analytics team and/or Frode Huse Gjendem (lead) with any questions.
#
########################################################################################################################
########################################################################################################################
########################################################################################################################
#
# MAIN for BASF/PMI
#
# Step  1:  Fetch IPOT data from database
# Step  2:  Material and location hierarchy data checks and transformations
# Step  3:  Merge material and location hierarchy
# Step  4:  Identify network nodes at level 0
# Step  5:  Group common information from multiple parents and remove duplicate records
# Step  6:  Find network IDs and remaining network node levels
# Step  7:  Identify network nodes with children
# Step  8:  Demand and forecasts checks and transformations
# Step  9:  Merge materials/locations with demand/forecasts
# Step 10:  Derive lead times components
# Step 11:  Derive lead times distributions for Monte Carlo simulation
# Step 12:  Identify and replace demand outliers at bottom network nodes
# Step 13:  (Multi-echelon): Rollup demand from bottom nodes
# Step 14:  Dates and data filtering
# Step 15:  Derive demand or forecast error distribution fits
# Step 16:  Optimization setup
# Step 17:  Optimization
# Step 18:  Stock calculations
# Step 19:  Save IO results to .csv file(s) and to database
# Step 20:  Forecasts checks and transformations
# Step 21:  Safety stock projections
# Step 22:  Save projections to database
#
# Clear up workspace and do garbage collection
rm(list=ls(all=T)); invisible(gc(verbose = FALSE))

# Set home, data download, log, GitHub, R scripts and working directory
homeDir <- "~/"
downDir <- paste0(homeDir,"DATA_DOWNLOAD/")
logsDir <- paste0(homeDir,"LOG/")
logiDir <- paste0(logsDir,"IO/")
if(!file.exists(logsDir)) dir.create(logsDir)
if(!file.exists(logiDir)) dir.create(logiDir)
if(!file.exists(downDir)) dir.create(downDir)
gitDir <- paste0(homeDir,"IPOT_BASE/")
scrtDir <- paste0(gitDir,"SCRIPTS/")
source(paste0(scrtDir,"IO_funcs_data.R"))
outliersFile <- paste0(scrtDir,"IO_funcs_outliers.R")
setwd(homeDir)

# Set global options
setGlobOptions()

# Get database parameters. Attach object so that its elements can be accessed by name
dbPars <- getDbPars(); attach(dbPars)

# Read command-line arguments
cmdArgs <- c("USER_ID","USER","SCN_ID",0,"IO_TYPE","ME","EXT_DEMAND","F","SO_LAST_DATE","2015-01-01","DEM_PTS",365,
"FCS_PTS",45,"FCS_LAG",1,"FCS_FREQ",12,"IO_DEM_TYPE","DEMAND") # development
#cmdArgs <- commandArgs(TRUE)             # production
# Get command-line argument values
cmdPars <- getCmdPars(cmdArgs); attach(cmdPars)

# Open an output file connection for detailed information/error messages
outTxt <- paste0(logiDir,"IO_main_",usrID,"_",strftime(sysTime,format="%Y-%m-%d"),"_",strftime(sysTime, format = "%H_%M_%S"),".txt")
logFile <- file(outTxt, "w")

# Initialize global variable for log line in LOG table, and create data frame to insert line into LOG table
logLine <- -1; logRow <- data.table(USER_ID=usrID,LOG_ID=logId,LOG_DATE=sysTime,LOG_LINE=logLine,LOG_CONTENT="",LOG_UDT="")

# Open global database connection
gbConn <- odbcConnect(DSN); if (gbConn == -1) { q <- "Failed to connect to database"; cat.IO(q); stop(q) }

# Main IO function
main <- function()
{
  # Get all demand dates
  ipDm <- getDemDates(toDate(lDemDat),demPts); attach(ipDm)
  
  # Set iPOT data options
  ipotOpts <- setIpotOpts(); attach(ipotOpts)
  
  # Work with data.table for faster subset, grouping, update, ordered joins... by reference, not copy.
  # Note: setDF and SetDT convert between data.frame and data.table by reference
  
  # Step 1: Fetch data from database
  
  conn <- odbcConnect(DSN); if (conn == -1) { q <- "Failed to connect to database"; cat.IO(q); stop(q) }
  sqlSave.LOG("Fetching records from database...")
  q <- sprintf("CALL %s(%d)", matStp,scnID)
  matData <- sqlQuery(conn, q)
  q <- paste("Fetched",nrow(matData),"records from",matStp); cat.IO(q); sqlSave.LOG(q)
  if (!nrow(matData)) { q <- "No material hierarchy data on database"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  q <- sprintf("CALL %s(%d)", mstStp,scnID)
  locData <- sqlQuery(conn, q)
  q <- paste("Fetched",nrow(locData),"records from",mstStp); cat.IO(q); sqlSave.LOG(q)
  if (!nrow(locData)) { q <- "No location hierarchy data on database"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  q <- sprintf("CALL %s(%d,'%s',%d)", potStp,scnID,lDemDat,demPts)
  potData <- sqlQuery(conn, q)
  q <- paste("Fetched",nrow(potData),"records from",potStp); cat.IO(q); sqlSave.LOG(q)
  if (!nrow(potData)) { q <- "No demand data on database"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  
  if (demType == demType1) fcsData <- data.table() else {
    # Fetch forecasts
    q <- sprintf("CALL %s(%d,%d,%d,'%s',%d)", pfcStp,scnID,fcsLag,fcsFreq,lDemDat,demPts)
    fcsData <- sqlQuery(conn, q)
    q <- paste("Fetched",nrow(fcsData),"records from",pfcStp); cat.IO(q); sqlSave.LOG(q)
    if (!nrow(fcsData)) { q <- "No forecasts on database"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  }
  odbcClose(conn)
  
  # Step 2: Material and location hierarchy data checks and transformations
  
  matData <- matDataChecks(setDT(matData))
  locData <- locDataChecks(setDT(locData))
  
  # Step 3: Merge material and location hierarchy
  
  matData[,LOC_PARENT_ID := LOC_ID]
  locData[,MAT_PARENT_ID := MAT_ID]
  # Group similar content into new columns and then delete individual column components
  cols <- c("START_DATE","END_DATE","GEN_LEAD_TIME","GEN_LEAD_TIME_SD","TRANS_TYPE","TRANS_FACTOR","TRANS_COST","UNIT_TRANS_SIZE","MIN_TRANS_SIZE")
  matData[,(cols) := list(PROD_START_DATE,PROD_END_DATE,PROD_TIME,PROD_TIME_SD,producType,MAT_PARENT_QTY,PROD_SETUP_COST,BATCH_SIZE,BATCH_SIZE)]
  matData[,c("PROD_START_DATE","PROD_END_DATE","PROD_TIME","PROD_TIME_SD","MAT_PARENT_QTY","PROD_SETUP_COST","BATCH_SIZE") := NULL]
  locData[,(cols) := list(AVA_START_DATE,AVA_END_DATE,LEAD_TIME,LEAD_TIME_SD,transpType,SOURCE_RATIO,ORDERING_COST,LOT_SIZE,MOQ)]
  locData[,c("AVA_START_DATE","AVA_END_DATE","LEAD_TIME","LEAD_TIME_SD","SOURCE_RATIO","ORDERING_COST","LOT_SIZE","MOQ") := NULL]
  # Reorder one table's colums in order to match tables by columns
  setcolorder(matData,names(locData))
  
  ################ TEMPORARY !!!!!! ##############
  matData[,STAGED := TRUE]
  locData[,STAGED := TRUE]
  #################################################
  
  #
  # BEFORE BINDING THEM, WE MAY NEED TO CHECK THAT RECORDS FROM THE TWO TABLES DO NOT OVERLAP BY (MAT_ID,LOC_ID)
  #
  setkey(matData,MAT_ID,LOC_ID)
  setkey(locData,key(matData))
  if (nrow(matData[locData])) {
    q <- "Material and location hierarchy data should not overlap, revise your IPOT data model !";
    cat.IO(q); sqlSave.LOG(q); stop(q) }
  
  # Bind the two tables into one and get rid of the other one
  locData <- rbindlist(list(matData,locData)); rm(matData)
  
  ################ TEMPORARY !!!!!! ##############
  if (ioType == ioType1) {
    # Filter out unstaged SKUs
    setkey(locData,STAGED)
    locData <- locData[J(TRUE)]
  }
  #################################################
  
  # Step 4: Identify network nodes at level 0
  
  # Nodes at level 0 have parents which do not appear themselves as nodes
  setkey(locData, MAT_PARENT_ID,LOC_PARENT_ID)
  locData[!.(MAT_ID,LOC_ID),LEVEL := 0]
  
  # Step 5: Group common information from multiple parents and remove duplicate records
  
  setkey(locData, MAT_ID,LOC_ID)
  # A node is at level 0 if and only if all of its records are at level 0
  locData[any(LEVEL==0),LEVEL := ifelse(any(is.na(LEVEL)),NA_real_,0),by=key(locData)]
  
  # Save node parents information (material/location/transition factor) in a new column
  locData[,c("MAT","LOC","TRANS") := list(list(MAT_PARENT_ID),list(LOC_PARENT_ID),list(TRANS_FACTOR)), by = key(locData)]
  locData[,PARENT := mapply(function(x,y,z) list(MAT_PARENT_ID=x,LOC_PARENT_ID=y,TRANS_FACTOR=z),
                            MAT,LOC,TRANS,SIMPLIFY=FALSE)][,c("MAT_PARENT_ID","LOC_PARENT_ID","TRANS_FACTOR","MAT","LOC","TRANS") := NULL]
  # Collapse redundant information
  locData <- unique(locData)
  
  q <- paste("Identified",sum(locData$LEVEL==0,na.rm=T),"nodes at LEVEL 0..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
  
  # Step 6: Find network IDs and remaining node levels
  
  q <- "Identifying network IDs and all node levels..."; cat.IO(q); sqlSave.LOG(q)
  findNetLevs(locData)
  
  # Step 7: Identify nodes with children 
  
  # For SEIO, all nodes are childless by definition
  if (ioType == ioType1) locData[,HAS_CHILDREN := FALSE] else {
    # Could search globally, however we search in parallel by network ID for efficiency
    q <- "Identifying nodes with children..."; cat.IO(q); sqlSave.LOG(q)
    idx <- "NETWORK_ID"
    locData <- setDT(ddply(setkeyv(locData,idx), idx, hasChildren, .parallel = TRUE))
  }
  
  # Step 8: Demand and forecasts checks and transformations
  
  q <- "Demand checks and formatting..."; cat.IO(q); sqlSave.LOG(q)
  # Ensure correct data types, and cast demand to long format
  setDT(potData)[,c("MAT_ID","LOC_ID") := list(as.character(MAT_ID),as.character(LOC_ID))]
  potData <- dcast.data.table(potData, MAT_ID+LOC_ID ~ DEM_REQ_DATE, value.var = 'DEM_QTY')
  idx <- key(potData)
  # Fill in missing demand dates
  npNms <- !dmNms %in% colnames(potData)
  if (any(npNms)) invisible(potData[,dmNms[npNms] := NA_real_])
  # Reorder demand dates
  setcolorder(potData,c(idx,dmNms))
  
  if (demType != demType1) {
    q <- "Forecasts checks and formatting..."; cat.IO(q); sqlSave.LOG(q)
    # Ensure correct data types, and cast forecasts to long format
    setDT(fcsData)[,c("MAT_ID","LOC_ID") := list(as.character(MAT_ID),as.character(LOC_ID))]
    fcsData <- dcast.data.table(fcsData, MAT_ID+LOC_ID ~ FCS_PERIOD, value.var = 'FCS_QTY')
    # Fill in missing forecast dates
    npNms <- !dmNms %in% colnames(fcsData)
    if (any(npNms)) invisible(fcsData[,dmNms[npNms] := NA_real_])
    # Reorder forecast dates
    setcolorder(fcsData,c(idx,dmNms))
    # Consider only demand and forecasts in common
    fcsData <- fcsData[potData,c(idx,dmNms),with=FALSE]
    potData <- potData[fcsData,c(idx,dmNms),with=FALSE]
  }
  
  # Step 9: Merge materials/locations with demand/forecasts
  
  # Merge including materials/locations with missing demand
  setkeyv(locData, idx)
  potData <- potData[locData, nomatch = NA]
  # Checks on demand values
  dataChecks(potData)
  
  if (demType != demType1) {
    # Merge including material/locations with missing forecasts
    fcsData <- fcsData[locData, nomatch = NA]
    # Checks on forecasts values
    dataChecks(fcsData)
  }
  rm(locData) # not needed any longer
  
  # Step 10: Derive lead times components
  
  q <- "Getting lead times components..."; cat.IO(q); sqlSave.LOG(q)
  getLeadTimes(potData, ltNms)
  
  # Step 11: Derive lead times distributions for Monte Carlo simulation
  
  q <- "Getting lead times distributions..."; cat.IO(q); sqlSave.LOG(q)
  potData[,ltDist := lapply(lt, fitLeadTimes, ltpDfb, ltuDfb, ltExp, ltUbn, ltLbn,
                            discDist, contDist, fitMethod)][,lt:=NULL] # column not needed anymore
  
  # Step 12: Identify and replace demand outliers at bottom nodes
  
  if (file.exists(outliersFile)) {
    source(outliersFile)
    # Set outlier treatment options
    outlOpts <- setOutlierOpts(); attach(outlOpts)
    q <- "Identifying outliers in demand..."; cat.IO(q); sqlSave.LOG(q)
    # Apply only to nodes without children
    setkey(potData, HAS_CHILDREN)
    potChildless <- potData[J(FALSE)]
    id <- c("MAT_ID","LOC_ID")
    dmAdj <- alply(setDF(potChildless), 1, findOutliers, id, dmNms, dmpTrd, dmuTrd, dmCor, dmThr, .parallel = TRUE)
    if (length(dmAdj)) {
      dmAdj <- rbindlist(lapply(dmAdj, function(x) if(!is.null(x))
        melt(x, id.vars=id, variable.name="Req_date",value.name="Dad_qty",variable.factor=FALSE,verbose=FALSE)))      
      q <- "Replacing outliers in demand..."; cat.IO(q); sqlSave.LOG(q)
      setkeyv(setDT(potChildless), id); setkeyv(dmAdj, id)
      for(col in dmNms) potChildless[dmAdj[col==Req_date], (col) := Dad_qty]
      potData <- rbindlist(list(potData[J(TRUE)],potChildless))
    }
    detach(outlOpts)
  }
  
  # Step 13 (Multi-echelon): Rollup demand from bottom nodes
  
  if (ioType != ioType1) {  
    idx <- "NETWORK_ID"
    setkeyv(potData, idx)
    # Discard demand networks which do not pass rollup checks
    potData <- rollupChecks(potData)
    # Set demand of upper nodes to zero when not adding external demand
    if (!extDem) setkey(potData,HAS_CHILDREN)[J(TRUE),(dmNms) := 0]
    q <- "Demand rollup..."; cat.IO(q); sqlSave.LOG(q)
    potData <- setDT(ddply(setkeyv(potData,idx), idx, signalRollup, dmNms, .parallel = TRUE))
    if (nrow(fcsData)) {
      setkeyv(fcsData, idx)
      # Discard forecast networks which do not pass rollup checks
      fcsData <- rollupChecks(fcsData)
      # Set forecasts of upper nodes to zero when not adding external forecasts
      if (!extDem) setkey(fcsData,HAS_CHILDREN)[J(TRUE),(dmNms) := 0]
      q <- "Forecasts rollup..."; cat.IO(q); sqlSave.LOG(q)
      fcsData <- setDT(ddply(setkeyv(fcsData,idx), idx, signalRollup, dmNms, .parallel = TRUE))
    }
  }
  
  # Step 14: Dates and data filtering
  # Step 15: Derive demand or forecast error distribution fits
  
  q <- "Demand filtering..."; cat.IO(q); sqlSave.LOG(q)
  potData <- potDateChecks(potData)
  df <- adply(setDF(potData), 1, dateFilter, dmNms, dmDts, dmDts[1], dmDts[length(dmDts)],
              .expand = FALSE, .parallel = TRUE, .id = NULL)
  setDT(potData)[,(dmNms) := df][,c("START_DATE","END_DATE"):=NULL]
  
  if (demType == demType1) {
    # List of demand without NA
    potData[,dm := listSignal(potData[,dmNms,with=FALSE])]
    # Filter out records with no data
    potData <- potDataFilter(potData)
    q <- "Demand distribution fits..."; cat.IO(q); sqlSave.LOG(q)
    invisible(potData[,sgDist := lapply(dm, fitSignal, normDist, contDist, discDist, fitMethod)][,dm := NULL])
  } else {
    q <- "Forecasts filtering..."; cat.IO(q); sqlSave.LOG(q)
    idx <- c("MAT_ID","LOC_ID")
    setkeyv(fcsData,idx); setkeyv(potData, idx)
    fcsData <- fcsData[potData,names(fcsData),with=FALSE]
    # No need to run through dates check routine
    df <- adply(setDF(fcsData), 1, dateFilter, dmNms, dmDts, dmDts[1], dmDts[length(dmDts)],
                .expand = FALSE, .parallel = TRUE, .id = NULL)
    setDT(fcsData)[,(dmNms) := df][,c("START_DATE","END_DATE"):=NULL]
    # Derive forecast error
    errData <- potData[,dmNms,with=FALSE]-fcsData[,dmNms,with=FALSE]
    # Lists of demand, forecasts, and forecast errors without NA
    potData[,dm := listSignal(potData[,dmNms,with=FALSE])]
    potData[,fc := listSignal(fcsData[,dmNms,with=FALSE])]
    potData[,fe := listSignal(errData)]
    # Filter out records with no data
    potData <- potDataFilter(potData)
    if (varCorr) feChangeStd(potData)
    q <- "Forecast error distribution fits..."; cat.IO(q); sqlSave.LOG(q)
    potData[,sgDist := lapply(fe, fitSignal, normDist, contDist, discDist, fitMethod)][,dm := NULL]
    invisible(potData[,FCS_BIAS := sapply(sgDist,function(x) x$sgAvg)][,fe := NULL])
  }
  potData[,(dmNms) := NULL] # columns not needed anymore (from here on we lose the temporal information)
  
  # Step 16: Optimization setup 
  
  q <- "Optimization setup..."; cat.IO(q); sqlSave.LOG(q)
  
  # Inventory optimization by network ID
  idx <- "NETWORK_ID"
  potData <- potOptFilter(setkeyv(potData,idx))
  
  # Set optimization options
  source(paste0(scrtDir,"IO_funcs_optm.R"))
  optOpts <- setOptOpts()
  
  # Step 17: Optimization
  
  q <- paste("Optimizing",nrow(potData),"SKUs",
             if(ioType != ioType1) paste("(",nrow(unique(potData)),ioType,"networks)"),"...")
  cat.IO(q); sqlSave.LOG(q); sqlSave.LOG("This may take a while, sit back and relax...")
  
  IORes <- setDT(ddply(potData, idx, inventoryOptim, optOpts, .parallel = TRUE))
  
  # Step 18: Stock calculations
  
  if (demType != demType1)
    # Substitute forecast error distribution fits for forecast fits
    invisible(potData[,sgDist := lapply(fc, fitSignal, normDist, contDist, discDist, fitMethod)][,fc := NULL])
  # Merge IPOT input with IO result
  idx <- c("MAT_ID","LOC_ID","NETWORK_ID")
  setkeyv(potData,idx); setkeyv(IORes,idx)
  IORes <- potData[IORes]
  q <- "Calculating optimal stocks..."; cat.IO(q); sqlSave.LOG(q)
  calcIoStocks(IORes)
  
  # Step 19: Save IO results to .csv file(s) and to database
  
  saveIoFile(IORes); saveIoDb(IORes)
  
  # Step 20: Forecasts checks and transformations
  
  conn <- odbcConnect(DSN)
  if (conn == -1) { q <- "Failed to connect to database"; cat.IO(q); stop(q) }
  sqlSave.LOG("Fetching future forecasts from database...")
  q <- sprintf("CALL %s(%d,%d,'%s',%d)", ffcStp,scnID,fcsFreq,toDate(lDemDat)+1,fcsPts)
  fcsData <- sqlQuery(conn, q)
  q <- paste("Fetched",nrow(fcsData),"records from",ffcStp); cat.IO(q); sqlSave.LOG(q)
  odbcClose(conn)
  
  if (nrow(fcsData)) {
    q <- "Forecasts checks and formatting..."; cat.IO(q); sqlSave.LOG(q)
    # Ensure correct data types, and cast forecasts to long format
    setDT(fcsData)[,c("MAT_ID","LOC_ID") := list(as.character(MAT_ID),as.character(LOC_ID))]
    fcsData <- dcast.data.table(fcsData, MAT_ID+LOC_ID+FCS_VALUE_TYPE+FCS_FREQ ~ FCS_PERIOD, value.var = 'FCS_QTY')
    # Fill in missing forecast dates
    dmNms <- as.character(seq(toDate(lDemDat)+1,toDate(lDemDat)+fcsPts, by="days"))
    npNms <- !dmNms %in% colnames(fcsData)
    if (any(npNms)) invisible(fcsData[,dmNms[npNms] := NA_real_])
    # Reorder forecast dates
    setcolorder(fcsData,c(key(fcsData),dmNms))
    # Consider only forecasts in common with IO output
    idx <- c("MAT_ID","LOC_ID")
    setkeyv(IORes,idx); setkeyv(fcsData,idx)
    # Include additional fields needed later:
    # Average forecast needed by forecasts check routine,
    # IO_SAFETY_STOCK_QTY and (average lead time + review period) needed by safety stock projections routine,
    fcsData <- fcsData[IORes, c(names(fcsData),"sgDist","ltDist","IO_SAFETY_STOCK_QTY"),with=FALSE]
    if (nrow(fcsData)) {
      fcsData[,`:=`(sgAvg=sapply(sgDist,function(x) x$sgAvg),
                    ltTotal=sapply(ltDist,function(x) x$ltTotal))][,c("sgDist","ltDist"):=NULL]
      # Checks on forecasts values
      fcsData <- ffcChecks(fcsData, dmNms)
    }
  }
  if (!nrow(fcsData)) cat.IO("No forecasts available") else {
    
    # Step 21: Safety stock projections
    
    q <- paste("Safety stocks projections for",nrow(fcsData),"SKUs..."); cat.IO(q); sqlSave.LOG(q)
    sqlSave.LOG("This may take another while, have a relaxing cup of cafe con leche... ")
    IOPro <- setDT(adply(setDF(fcsData), 1, projectSafetyStock, dmNms, .expand = FALSE, .parallel = TRUE, .id = NULL))
    
    # Step 22: Save projections to database
    
    saveIopDb(IORes,IOPro)
  }
  sqlSave.LOG("***************************************************")
  q <- "INVENTORY OPTIMIZATION TERMINATED SUCCESSFULLY !"; cat.IO(paste0("\t",q)); sqlSave.LOG(q)
  sqlSave.LOG("***************************************************")
}

# Exception handling and final cleanup
tryCatch.IO <- function(expr) {
  tryCatch(expr,error = function(e) {
    cat.IO(paste0(e,"\v"))
    sqlSave.LOG("***************************************************")
    q <- "INVENTORY OPTIMIZATION TERMINATED WITH ERRORS"; cat.IO(paste0("\t",q)); sqlSave.LOG(q)
    sqlSave.LOG("***************************************************")
  }, finally = {
    # Unregister for parallel processing and close open connections
    registerDoSEQ(); odbcClose(gbConn); close(logFile)
    if (!file.info(outTxt)$size) invisible(file.remove(outTxt))
  })
}

# Execute main IO function
tryCatch.IO(main())
