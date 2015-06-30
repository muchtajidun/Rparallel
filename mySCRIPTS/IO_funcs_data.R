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
# Global output functions using global variables
#
# Redirect to file
cat.IO <- function(...)
  cat("\n",..., file = logFile)
write.table.IO <- function(...)
  write.table(..., file = logFile, quote = FALSE, row.names = FALSE)
write.csv.IO <- function(...)
  write.csv2(..., na = "", row.names=FALSE)
# Generic write to database table
sqlSave.IO <- function(...)
  sqlSave(..., append = TRUE, rownames = FALSE)
# Specific write to log table
sqlSave.LOG <- function(txt) {
  assign("logLine", logLine+1, envir = .GlobalEnv)
  logRow[,c("LOG_LINE","LOG_CONTENT","LOG_UDT"):=list(logLine,txt,strftime(Sys.time()))]
  sqlSave.IO(gbConn, logRow, logTbl)
}
#
# DATA TREATMENT FUNCTIONS
#
################################################################################
#
# Set global options
#
setGlobOptions <- function()
{
  # Install (if not already installed) required packages from local CRAN mirror
  #
  local({
    r <- getOption("repos")
    r["CRAN"] <- "http://cran.es.r-project.org"
    r["CRANextra"] <- "http://www.stats.ox.ac.uk/pub/RWin"
    options(repos = r)
  })
  pkgs <- c("RODBC","plyr","reshape2","data.table","lubridate","MASS","fitdistrplus",
            "truncdist","nloptr","ADGofTest","SCperf","setRNG","doParallel","Rcpp","inline")
  # Notes: - function mma() in packages "nloptr" and "evd" mask each other but it is OK
  #          as they are not used anywhere in this application
  #        - microbenchmark is only necessary for performance testing
  lapply(pkgs, function(x) {
    if (!is.element(x, installed.packages()[,1]))
      install.packages(x, dep = TRUE)
    suppressPackageStartupMessages(library(x, character.only = TRUE))
  })
  
  # Do not convert character vectors in data frames to factors
  options(stringsAsFactors = FALSE)
  # No need to adjust names of variables in data frames if they are duplicated or syntactically incorrect
  options(check.names = FALSE)
  # Return 0 rather than NA rows when no match to datatable key
  options(datatable.nomatch = 0)
}
# Get database tables and fields names. These are all in capital letters.
# Connection credentials are stored in ~/.odbc.ini user DSN,
# overriding global settings in /etc/odbc.ini
#
getDbPars <- function()
{
  pars <- list()
  
  # [DSN name] in .odbc.ini
  pars$DSN        <- "CLIENT"
  
  # Tables and stored procedures names
  pars$potStp     <- "IPOT_DATA_DMD_SP"             # IPOT demand stored procedure
  pars$mstStp     <- "GET_IO_TRANSP_DATA_BY_SCN_SP" # IPOT master data stored procedure
  pars$matStp     <- "GET_IO_PROD_DATA_BY_SCN_SP"   # materials hierarchy stored procedure
  pars$pfcStp     <- "PAST_DEMAND_FORECAST_SP"      # past forecasts stored procedure
  pars$ffcStp     <- "FUTURE_DEMAND_FORECAST_SP"    # future forecasts stored procedure
  pars$iorTbl     <- "IO_RESULT"                    # IO results table
  pars$iopTbl     <- "IOP_RESULT"                   # IO projections table
  pars$logTbl     <- "LOG"                          # Log table
  
  # LOG_ID on table LOG table
  pars$logId       <- "IO"
  # IO_VALUE_TYPE on tables IO_RESULT, IOP_RESULT
  pars$ioValueType <- "STATISTICAL"
  
  return(pars)
}
#
# Extract execution parameters from console. If not found, use default values
#
getCmdPars <- function(args)
{
  pars <- list()
  # Console execution parameters or default values
  pars$usrID    <- ifelse(is.na(args[match("USER_ID",args)+1]),"USER",args[match("USER_ID",args)+1])
  pars$scnID    <- ifelse(is.na(args[match("SCN_ID",args)+1]),as.integer(0),as.integer(args[match("SCN_ID",args)+1]))
  pars$sysTime  <- ifelse(is.na(args[match("LOG_DATE",args)+1]),strftime(Sys.time()),args[match("LOG_DATE",args)+1])
  pars$ioType   <- ifelse(is.na(args[match("IO_TYPE",args)+1]),"SE",args[match("IO_TYPE",args)+1])
  pars$extDem   <- ifelse(is.na(args[match("EXT_DEMAND",args)+1]),FALSE,as.logical(args[match("EXT_DEMAND",args)+1]))
  # Default for latest demand date is yesterday
  pars$lDemDat  <- ifelse(is.na(args[match("SO_LAST_DATE",args)+1]),as.character(Sys.Date()-1),args[match("SO_LAST_DATE",args)+1])
  pars$demType  <- ifelse(is.na(args[match("IO_DEM_TYPE",args)+1]),"DEMAND",args[match("IO_DEM_TYPE",args)+1])
  pars$demPts   <- ifelse(is.na(args[match("DEM_PTS",args)+1]),as.integer(365),as.integer(args[match("DEM_PTS",args)+1]))
  pars$fcsPts   <- ifelse(is.na(args[match("FCS_PTS",args)+1]),as.integer(30),as.integer(args[match("FCS_PTS",args)+1]))
  pars$fcsLag   <- ifelse(is.na(args[match("FCS_LAG",args)+1]),as.integer(1),as.integer(args[match("FCS_LAG",args)+1]))
  pars$fcsFreq  <- ifelse(is.na(args[match("FCS_FREQ",args)+1]),as.integer(12),as.integer(args[match("FCS_FREQ",args)+1]))
  # Forecast bias correction term is only applicable when IO_DEM_TYPE is FORECAST
  pars$biasCorr <- ifelse(is.na(args[match("IO_BIAS_CORR",args)+1]),TRUE,as.logical(args[match("IO_BIAS_CORR",args)+1]))
  # Forecast error variability correction term is only applicable when IO_DEM_TYPE is FORECAST
  pars$varCorr  <- ifelse(is.na(args[match("FCS_ERR_ADJ",args)+1]),as.numeric(0),as.numeric(args[match("FCS_ERR_ADJ",args)+1]))
  # Global service level minimum and service level tolerance in MEIO
  pars$glSlMin  <- ifelse(is.na(args[match("GL_SL_MIN",args)+1]),as.numeric(0.0),as.numeric(args[match("GL_SL_MIN",args)+1]))
  pars$glSlTol  <- ifelse(is.na(args[match("GL_SL_TOL",args)+1]),as.numeric(0.005),as.numeric(args[match("GL_SL_TOL",args)+1]))
  # Parallelize execution, use all available CPU cores but one
  pars$cpuCores  <- ifelse(is.na(args[match("NUM_CORES",args)+1]),as.integer(detectCores()-1),as.integer(args[match("NUM_CORES",args)+1]))
  return(pars)
}
#
# Converts value from character to date type
#
toDate <- function(d) as.Date(d, format = "%Y-%m-%d")
#
# Get complete demand dates sequence
#
getDemDates <- function(ld,pts) {
  dmD <- seq(ld-pts+1, ld, by="days") # demand dates
  dmN <- as.character(dmD)
  lt <- character(0)                  # no lead time columns yet
  # return demand dates and lead times
  return(list(dmDts=dmD,dmNms=dmN,ltNms=lt))
}
#
# Set iPOT options
#
setIpotOpts <- function()
{
  # Register for parallele execution
  registerDoParallel(cores=cpuCores)
  
  cat.IO("Running scenario parameters:")
  scnPars <- sapply(cmdPars,function(x) x)
  for (i in seq.int(1:length(scnPars))) cat.IO(names(scnPars[i]),"\t",scnPars[i])
  
  pars <- list()
  
  # Execution parameter values
  pars$demType1 <- "DEMAND"
  pars$demType2 <- "FORECAST"
  pars$ioType1  <- "SE"
  pars$ioType2  <- "ME"
  
  sqlSave.LOG("***************************************************")
  sqlSave.LOG("******** IPOT v2.0 INVENTORY OPTIMIZATION *********")
  sqlSave.LOG(sprintf("* Time: %s", sysTime))
  sqlSave.LOG(sprintf("* User: %s", usrID))
  sqlSave.LOG(sprintf("* Scenario: %d", scnID))
  sqlSave.LOG(sprintf("* IO type: %s", ioType))
  sqlSave.LOG(sprintf("* Demand type: %s", demType))
  sqlSave.LOG(sprintf("* Demand points: %d", demPts))
  sqlSave.LOG(sprintf("* External demand: %s", extDem))
  if (demType != pars$demType1) {
    sqlSave.LOG(sprintf("* Forecast points: %d", fcsPts))
    sqlSave.LOG(sprintf("* Forecast lag: %d", fcsLag))
    sqlSave.LOG(sprintf("* Forecast frequency: %d", fcsFreq))
    sqlSave.LOG(sprintf("* Forecast bias correction: %s", biasCorr))
  }
  sqlSave.LOG(sprintf("* Number of CPU cores used for parallel execution: %d", getDoParWorkers()))
  sqlSave.LOG(sprintf("* Full log: %s", outTxt))
  sqlSave.LOG("***************************************************")
  
  pars$producType <- "PRODUCTION"
  pars$transpType <- "TRANSPORTATION"
  
  # Probability density functions (PDF) to fit to demand/forecast and lead time data
  pars$normDist <- "norm"                       # for discrete or continuous data
  pars$contDist <- c("norm", "exp", "gamma")    # for discrete or continuous data >=0
  pars$discDist <- c("pois", "nbinom", "geom")  # for discrete data >=0 (warnings when used with continuous data)
  
  # PDF fitting method (maximum likelihood "mle" quite often fails to converge or gives other errors)
  pars$fitMethod <- "mme"  # moment matching estimation
  
  # Requirements on demand/forecast and LT data for distribution fitting / bootstrapping
  pars$dmpDfb   <- 10   # minimum number of non-zero values
  pars$dmuDfb   <- 3    # minimum number of distinct non-zero values
  pars$dmInt    <- 0.95 # maximum intermittency allowed in demand/forecast (0.95 => at least 5% of data must be > 0)
  pars$ltpDfb   <- 10   # minimum number of values
  pars$ltuDfb   <- 3    # minimum number of distinct values
  
  # Parameters of truncated Gamma distribution when lead time cannot be modelled from data.
  # The Poisson approximation with parameter lambda = (average)LT is replaced by a Gamma(a,s)
  # approximations, where "a" is the shape parameter and "s" the scale parameter
  # (r=1/s is the rate parameter), a and s > 0, with mean m=a*s and variance v=m*s.
  # When v is not given, we approximate it as follows:
  # we want v < m, where v is an exponential function of m, so that v is much less
  # than m for small values of LT. Hence, we parametrize v as a function of m as follows:
  # v=(m^t)/(2m)^(t-1), with t=ltExp > 1 (can be a real number).
  # Parameters a and s are then derived as follows: s=v/m, a=m/s
  pars$ltExp <- 2.0
  # Upper bound of truncated Gamma is (ltUbn * LT), where ltUbn > 1 (can be a real number).
  # This will prevent the generation of unrealistically high simulated lead time values, as the region
  # of support of the Gamma distribution is (0,+Inf). Truncation is equivalent to cutting off unwanted
  # regions of support (the result is not a probability density function anymore, as it does not integrate
  # to 1) followed by rescaling of the remaining region so that it integrates to 1. An absolute lower
  # bound ltLbn is also given (defaults to zero)
  pars$ltUbn <- 2.0
  pars$ltLbn <- 0
  
  return(pars)
}
#
# Material hierarchy data checks and corrections. Input is data.table object.
# N.B.: object is changed by copy, as deleting rows cannot be done by
# reference yet. Hence, the changed object needs to be returned.
#
matDataChecks <- function(mat)
{
  # Ensure correct data types
  mat[,c("MAT_ID","MAT_PARENT_ID","LOC_ID") := list(as.character(MAT_ID),as.character(MAT_PARENT_ID),as.character(LOC_ID))]
  mat[,c("PROD_START_DATE","PROD_END_DATE") := list(toDate(PROD_START_DATE),toDate(PROD_END_DATE))]
  
  q <- "Setting NULL service levels, batch sizes, review periods, production times, costs and demand to zero..."; cat.IO(q); sqlSave.LOG(q)
  cols <- c("SL_MIN","BATCH_SIZE","REVIEW_PERIOD","PROD_TIME","PROD_SETUP_COST","HOLDING_COST","BACKLOG_COST","MAT_PARENT_QTY")
  mat[,(cols) := lapply(.SD, function(x) { x[is.na(x)] <- 0; x } ), .SDcols=cols]
  
  # Check service levels
  hll <- with(mat, which(SL_MIN < 0 | SL_MIN > 1))
  if (length(hll)) {
    q <- paste(length(hll),"Master records with invalid SL_MIN..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(mat[hll,.(MAT_ID,LOC_ID,SL_MIN)])
    q <- "Deleting records with invalid service level..."; cat.IO(q); sqlSave.LOG(q)
    mat <- mat[-hll]
  }
  # Check production times
  hll <- with(mat, which(is.na(PROD_TIME) | PROD_TIME < 0 | PROD_TIME >= length(dmNms)))
  if (length(hll)) {
    q <- paste(length(hll),"Material Hierarchy records with invalid PROD_TIME..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(mat[hll,.(MAT_ID,MAT_PARENT_ID,LOC_ID,PROD_TIME)])
    q <- "Deleting records with invalid production time..."; cat.IO(q); sqlSave.LOG(q)
    mat <- mat[-hll]
  }
  q <- "Checking for MAT_PARENT_QTY negative values..."; cat.IO(q); sqlSave.LOG(q)
  hll <- with(mat, which(MAT_PARENT_QTY < 0 ))
  if (length(hll)) {
    q <- paste(length(hll),"Records with negative values..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(mat[hll,.(MAT_ID,MAT_PARENT_ID,LOC_ID)])
    q <- "Setting negative values to 0..."; cat.IO(q); sqlSave.LOG(q)
    mat[hll,MAT_PARENT_QTY := 0]
  }
  if (!nrow(mat)) { q <- "No material hierarchy records left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(mat)
}
#
# Location hierarchy data checks. Input is master data.table object.
# N.B.: object is changed by copy, as deleting rows cannot be done by
# reference yet. Hence, the changed object needs to be returned.
#
locDataChecks <- function(loc)
{
  # Ensure correct data types 
  loc[,c("MAT_ID","LOC_ID","LOC_PARENT_ID") := list(as.character(MAT_ID),as.character(LOC_ID),as.character(LOC_PARENT_ID))]
  loc[,c("AVA_START_DATE","AVA_END_DATE") := list(toDate(AVA_START_DATE),toDate(AVA_END_DATE))]
  
  q <- "Setting NULL service levels, lot sizes, minimum order quantities, review periods, lead times and costs to zero..."; cat.IO(q); sqlSave.LOG(q)
  cols <- c("SL_MIN","LOT_SIZE","MOQ","REVIEW_PERIOD","LEAD_TIME","ORDERING_COST","HOLDING_COST","BACKLOG_COST")
  loc[,(cols) := lapply(.SD, function(x) { x[is.na(x)] <- 0; x } ), .SDcols=cols]
  
  # Check service levels
  hll <- with(loc, which(SL_MIN < 0 | SL_MIN > 1))
  if (length(hll)) {
    q <- paste(length(hll),"Location hierarchy records with invalid SL_MIN..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(loc[hll,.(MAT_ID,LOC_ID,SL_MIN)])
    q <- "Deleting records with invalid service level..."; cat.IO(q); sqlSave.LOG(q)
    loc <- loc[-hll]
  }
  hll <- with(loc, which(is.na(SOURCE_RATIO)))
  if (length(hll)) {
    q <- paste("Setting",length(hll),"NULL sourcing ratios to 1..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    loc[hll,SOURCE_RATIO := 1]
  }
  # Check sourcing ratios
  hll <- with(loc, which(SOURCE_RATIO < 0 | SOURCE_RATIO > 1))
  if (length(hll)) {
    q <- paste(length(hll),"Location hierarchy records with invalid SOURCE_RATIO..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(loc[hll,.(MAT_ID,LOC_ID,LOC_PARENT_ID,SOURCE_RATIO)])
    q <- "Deleting records with invalid sourcing ratios..."; cat.IO(q); sqlSave.LOG(q)
    loc <- loc[-hll]
  }
  if (!nrow(loc)) { q <- "No location hierarchy records left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(loc)
}
#
# IPOT demand or past forecasts checks. Input is data table object.
# N.B.: object is changed by reference, i.e., changed on the spot
#
dataChecks <- function(dto)
{
  q <- "Setting NULL values to zero..."; cat.IO(q); sqlSave.LOG(q)
  dto[,(dmNms) := lapply(.SD, function(x) { x[is.na(x)] <- 0; x }), .SDcols=dmNms]
  
  q <- "Checking for negative values..."; cat.IO(q); sqlSave.LOG(q)
  hll <- dto[,list(hll=.I[which(apply(.SD,1, function(x) any(x<0)))]), .SDcols = dmNms]$hll
  if (length(hll)) {
    q <- paste(length(hll),"Records with negative values..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(dto[hll,.(MAT_ID,LOC_ID)])
    q <- "Setting negative values to 0..."; cat.IO(q); sqlSave.LOG(q)
    dto[,(dmNms) := lapply(.SD, function(x) { x[x<0] <- 0; x }), .SDcols=dmNms]
  }
}
#
# Future forecasts checks. Input is data table object and vector of dates columns.
# N.B.: object is changed by copy, as deleting rows cannot be done by
# reference yet. Hence, the changed object needs to be returned
#
ffcChecks <- function(fcs, dns)
{
  # Replace NA forecast values with historical demand average
  hll <- fcs[,list(hll=.I[which(apply(.SD,1, function(x) any(is.na(x))))]), .SDcols = dns]$hll
  if (length(hll)) {
    q <- paste(length(hll),"Forecast records containing NULL forecasts..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(fcs[hll,.(MAT_ID,LOC_ID)])
    q <- "Setting NULL forecasts to historical demand average..."; cat.IO(q); sqlSave.LOG(q)
    df <- adply(setDF(fcs), 1, function(x) {
      xn <- x[dns]; xn[is.na(xn)] <- x$sgAvg; return(xn)},
      .expand = FALSE, .parallel = TRUE, .id = NULL)
    setDT(fcs)[,(dns) := df]
  }
  # Check whether some forecast series contains negative values
  hll <- fcs[,list(hll=.I[which(apply(.SD,1, function(x) any(x<0)))]), .SDcols = dns]$hll
  if (length(hll)) {
    q <- paste(length(hll),"Forecast records containing negative forecasts..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(fcs[hll,.(MAT_ID,LOC_ID)])
    q <- "Setting negative forecasts to 0..."; cat.IO(q); sqlSave.LOG(q)
    fcs[,(dns) := lapply(.SD, function(x) { x[x<0] <- 0; x }), .SDcols=dns]
  }
  # Remove rows where all forecast values are zero
  hll <- fcs[,list(hll=.I[which(rowSums(.SD)==0)]), .SDcols = dns]$hll
  if (length(hll)) {
    q <- paste(length(hll),"Forecast records where all forecasts are zero..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(fcs[hll,.(MAT_ID,LOC_ID)])
    q <- "Deleting records with all zero forecasts..."; cat.IO(q); sqlSave.LOG(q)
    fcs <- fcs[-hll]
  }
  if (!nrow(fcs)) { q <- "No forecast records left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(fcs[,sgAvg:=NULL])
}
#
# IPOT dates checks. Input is IPOT data.table object.
# N.B.: object is changed by copy (deleting rows cannot be done by
# reference yet) hence a copy of the object needs to be returned
#
potDateChecks <- function(pot)
{
  # Remove rows where demand is outside [START_DATE,END_DATE]
  hll <- pot[,which(START_DATE>dmDts[length(dmDts)] | END_DATE<dmDts[1])]
  if (length(hll)) {
    q <- paste(length(hll),"IPOT records with demand outside [START_DATE,END_DATE]..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,.(MAT_ID,LOC_ID,START_DATE,END_DATE)])
    q <- "Deleting products with demand outside valid dates..."; cat.IO(q); sqlSave.LOG(q)
    pot <- pot[-hll]
  }
  if (!nrow(pot)) { q <- "No demand records left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(pot)
}
#
# Set demand before START_DATE (the date the product was launched onto the market) and after
# END_DATE (the date the product was taken off the market) to NA. If START_DATE is NA,
# treat it as origin of time. If END_DATE is NA, treat it as end of time.
# Also set to NA all zero values at or after START_DATE before first non-zero value,
# and all zero values at or before END_DATE after last non-zero value.
# This function assumes that rows with demand outside [START_DATE,END_DATE] have been removed.
# Inputs: - row of demand data containing demand and START_DATE, END_DATE
#         - vectors of demand names (nms), dates (dts), and first and last dates (dtf, dtl)
# Output:   numeric vector containing updated demand
#
dateFilter <- function(x, dns, dts, dtf, dtl)
{
  sales <- as.numeric(x[dns])
  # If non-NA demand is all zero, set all to NA and return
  if (all(sales==0,na.rm=T)) { is.na(sales) <- TRUE; return(sales) }
  lens <- length(sales)
  strdate <- toDate(x$START_DATE)
  enddate <- toDate(x$END_DATE)
  
  if (is.na(strdate) || strdate < dtf) {  # set zero demand before first non-zero value to NA
    ind <- which.min(sales==0); is.na(sales[0:(ind-1)]) <- TRUE } else {
      ind <- match(strdate,dts); is.na(sales[0:(ind-1)]) <- TRUE       # set demand before START_DATE to NA
      # If updated demand is all zero, set all to NA and return. Otherwise, set to NA only
      # zero demand before first non-zero value
      if (all(sales==0,na.rm=T)) { is.na(sales) <- TRUE; return(sales) }
      ind <- which.min(sales==0); is.na(sales[0:(ind-1)]) <- TRUE
    }
  if (is.na(enddate) || enddate >= dtl) { # set zero demand after last non-zero value to NA
    ind <- lens-which.max(rev(sales)!=0)+1; if (ind<lens) is.na(sales[(ind+1):lens]) <- TRUE } else {
    ind <- match(enddate,dts); is.na(sales[(ind+1):lens]) <- TRUE    # set demand after END_DATE to NA
    # If updated demand is all zero, set all to NA. Otherwise, set to NA only
    # zero demand after last non-zero value
    if (all(sales==0,na.rm=T)) is.na(sales) <- TRUE else {
      ind <- lens-which.max(rev(sales)!=0)+1; is.na(sales[(ind+1):lens]) <- TRUE }
  }
  return(sales)
}
#
# Data filtering. Input is IPOT data table object.
# If the minimum requirements imposed on demand/forecasts/forecast errors in setIpotOpts()
# are not met, the corresponding records will be removed.
# N.B.: object is changed by copy (deleting rows cannot be done by
# reference yet) hence a copy of the object does need to be returned.
#
potDataFilter <- function(pot)
{
  if (demType == demType1) {  # Filter out demand
    hll <- pot[,which(sapply(dm,function(x) {
      l <- length(x); x0 <- x[x!=0]; !l || length(x0) < dmpDfb || length(unique(x0)) < dmuDfb ||
        (l-length(x0))/l > dmInt }))]
  } else {                    # Filter out demand, forecasts, and forecast errors
    hll <- pot[,which(mapply(function(x,y,z) {
      xl <- length(x); yl <- length(y); zl <- length(z);
      x0 <- x[x!=0]; y0 <- y[y!=0]; z0 <- z[z!=0];
      !xl || !yl || !zl || length(x0) < dmpDfb || length(y0) < dmpDfb || length(z0) < dmpDfb ||
        length(unique(x0)) < dmuDfb || length(unique(y0)) < dmuDfb || length(unique(z0)) < dmuDfb ||
        (xl-length(x0))/xl > dmInt || (yl-length(y0))/yl > dmInt || (zl-length(z0))/zl > dmInt },dm,fc,fe))]
  }
  if (length(hll)) {
    q <- paste(length(hll),"Records with insufficient or no values..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,.(MAT_ID,LOC_ID)])
    q <- "Deleting records with insufficient or no values..."; cat.IO(q); sqlSave.LOG(q)
    pot <- pot[-hll]
  }
  if (!nrow(pot)) { q <- "No records left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(pot)
}
#
# IPOT optimization filtering actions. Input is IPOT data table object keyed by NETWORK_ID.
# Input is changed by copy (deleting rows cannot be done by reference yet)
# hence a copy of the object needs to be returned
#
potOptFilter <- function(pot)
{
  idx <- key(pot)
  # Do not perform optimization if network is broken at some echelon level
  hll <- with(pot, which(!(seq.int(min(LEVEL),max(LEVEL)) %in% LEVEL)),by=idx)
  if (length(hll)) {
    q <- paste(length(hll),"broken multi-echelon network(s)..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,c(idx,"MAT_ID","LOC_ID","LEVEL","NETWORK_ID"),with=FALSE])
    q <- "Deleting broken network(s)..."; cat.IO(q); sqlSave.LOG(q)
    pot <- pot[-hll]
  }
  # Do not perform optimization if lead time at supplier is not available
  hll <- pot[, if (!0 %in% LEVEL || sapply(ltDist[LEVEL==0], function(x) is.na(x$ltAvg))) list(hll=.I), by=idx]$hll
  if (length(hll)) {
    q <- paste(length(hll),"network(s) without supplier lead time..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,c(idx,"MAT_ID","LOC_ID","LEVEL","NETWORK_ID"),with=FALSE])
    q <- "Deleting network(s) without supplier lead time..."; cat.IO(q); sqlSave.LOG(q)
    pot <- pot[-hll]
  }
  # Do not perform optimization if neither service level nor
  # holding or backordering costs are available at any one location
  hll <- pot[, if (any(mapply(function(x,y,z) x==0 & y==0 & z==0, HOLDING_COST, BACKLOG_COST, SL_MIN))) list(hll=.I), by=idx]$hll
  if (length(hll)) {
    q <- paste(length(hll),"network(s) without service level and cost information..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,c(idx,"MAT_ID","LOC_ID","LEVEL","NETWORK_ID"),with=FALSE])
    q <- "Deleting network(s) without service level and cost information..."; cat.IO(q); sqlSave.LOG(q)
    pot <- pot[-hll]
  }
  # Do not perform MEIO optimization if there are no staged nodes to optimize
  if (ioType == ioType2) {
    hll <- pot[, if (!(HAS_CHILDREN && STAGED)) list(hll=.I), by=idx]$hll
    if (length(hll)) {
      q <- paste(length(hll),"network(s) without staged nodes..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
      write.table.IO(pot[hll,c(idx,"MAT_ID","LOC_ID","LEVEL","NETWORK_ID"),with=FALSE])
      q <- "Deleting network(s) without staged nodes..."; cat.IO(q); sqlSave.LOG(q)
      pot <- pot[-hll]
    }
  }
  if (!nrow(pot)) { q <- "No networks left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(pot)
}
#
# Increment/decrement standard deviation of forecast error by a given fraction varCorr in (-1,1).
# IPOT data.table input object is changed by reference 
#
feChangeStd <- function(pot)
{
  q <- paste(ifelse(varCorr>0,"Incrementing","Decrementing"),"forecast error Std dev by",abs(varCorr)*100,"%..."); cat.IO(q); sqlSave.LOG(q)
  hll <- pot[,which(lapply(fe,sd) < 1e-03)]
  if (length(hll)) {
    q <- paste(length(hll),"Records with forecast error Std dev close to or equal to 0 will not be updated..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(pot[hll,.(MAT_ID,LOC_ID)])
  }
  pot[!hll,fe  := lapply(fe, function(x) { z <- (x- .Internal(mean(x)))/sd(x); z <- z*(1+varCorr)*sd(x)+ .Internal(mean(x)) })]
}
#
# Calculate optimal stocks. Input is changed by reference.
# Demand-based optimization optimizes reorder point, while
# forecast-error-based optimization optimizes safety stock
#
calcIoStocks <- function(res)
{ 
  # Economic Order Quantity. Formula is EOQ = sqrt(2FD/C), where F = TRANS_COST (fixed cost per order or production setup),
  # C = HOLDING_COST (unit carrying cost per unit time), D = units of demand/forecast per unit time). Using SCperf
  # package is equivalent to the following calculation but offers additional option of planned shortages
  # (EOQ model with backorders). Note that EOQ calculation does not depend on result from IO
  # res[,IO_EOQ := mapply(function(x,y,z) ifelse(x&z,sqrt(2*x*y$sgAvg/z),NA_real_), TRANS_COST,sgDist,HOLDING_COST)]
  res[,IO_EOQ := mapply(function(x,y,z) ifelse(x&z,EOQ(y$sgAvg,x,z),NA_real_), TRANS_COST,sgDist,HOLDING_COST)]
  
  if (demType == demType1) {
    # Subtract average lead time demand from reorder point to obtain safety stock (result can be negative)
    res[,IO_SAFETY_STOCK_QTY := mapply(function(x,y,z) x-y$sgAvg*z$ltTotal,IO_ROP,sgDist,ltDist)]
    # Safety Stock may be changed according to BASF formula
    res[,IO_SAFETY_STOCK_QTY := mapply(function(x,y,z,w) ifelse(is.na(x) | x<0.5*y$sgAvg*z$rwp,w,
                                                                w+0.5*y$sgAvg*z$rwp-(y$sgSd)^2/(2*y$sgAvg)),
                                       MIN_TRANS_SIZE,sgDist,ltDist,IO_SAFETY_STOCK_QTY)]
  } else {
    res[,IO_SAFETY_STOCK_QTY := IO_ROP]
    # Add average lead time forecast to safety stock to obtain reorder point
    res[,IO_ROP := mapply(function(x,y,z) x+y$sgAvg*z$ltTotal,IO_SAFETY_STOCK_QTY,sgDist,ltDist)]
  }
  # Pipeline Stock is average lead time demand/forecast excluding review period
  res[,IO_PIPELINE_STOCK_QTY := mapply(function(x,y) x$sgAvg*y$ltAvg,sgDist,ltDist)]
  res[,IO_MIN_STOCK_QTY      := IO_SAFETY_STOCK_QTY+IO_PIPELINE_STOCK_QTY]
  # Cycle Stock is maximum half-value between MIN_TRANS_SIZE, EOQ and average demand/forecast during review period
  res[,IO_CYCLE_STOCK_QTY    := mapply(function(x,y,z,w) max(x$sgAvg*y$rwp,z,w,na.rm=TRUE)/2, sgDist,ltDist,MIN_TRANS_SIZE,IO_EOQ)]
  res[,IO_TARGET_STOCK_QTY   := IO_MIN_STOCK_QTY+IO_CYCLE_STOCK_QTY]
  res[,IO_MAX_STOCK_QTY      := IO_TARGET_STOCK_QTY+IO_CYCLE_STOCK_QTY]
  res[,IO_DEM_QTY            := sapply(sgDist, function(x) x$sgAvg)]
  # Subtract bias correction term from safety stock when forecast-error-based optimization with forecast bias correction
  if (demType != demType1 && biasCorr) {
    res[,IO_FCS_BIAS_CORRECTION := mapply(function(x,y) x*y$ltTotal,FCS_BIAS,ltDist)]
    res[,IO_SAFETY_STOCK_QTY := IO_SAFETY_STOCK_QTY - IO_FCS_BIAS_CORRECTION]
  } else res[,IO_FCS_BIAS_CORRECTION := NA_real_]
  # Safety stock coverage
  res[,IO_SAFETY_STOCK_COV   := mapply(function(x,y) x/y$sgAvg, IO_SAFETY_STOCK_QTY,sgDist)]
  # Optimal quantities not currently calculated
  invisible(res[,c("IO_AVG_STOCK_QTY","IO_TRANSIT_STOCK_QTY","IO_WIP_STOCK_QTY","IO_OPTIMAL_STOCK_QTY",
         "IO_HOLDING_COST","IO_BACKLOG_COST") := list(NA_real_)])
}
#
# Save IO results to .csv files. Input is IO results object
#
saveIoFile <- function(res)
{
  outCsv <- sub(logiDir, downDir, outTxt)
  outCsv <- sub(".txt", ".csv", outCsv)
  
  # Selected fields for external user
  cols <- c("IO_ROP","IO_SL","IO_SAFETY_STOCK_QTY","IO_SAFETY_STOCK_COV", "IO_FCS_BIAS_CORRECTION",
            "IO_PIPELINE_STOCK_QTY","IO_MIN_STOCK_QTY","IO_CYCLE_STOCK_QTY","IO_TARGET_STOCK_QTY","IO_MAX_STOCK_QTY")
  # We format a copy of the object rather than changing the original object
  csv <- res[,c(key(res),cols),with=FALSE]
  # Round all numeric values to 2 decimal figures
  csv[,(cols) := lapply(.SD, function(x) round(x,2)), .SDcols = cols]
  q <- "********* Saving results in CSV format... *********"; cat.IO(q); sqlSave.LOG(q)
  write.csv.IO(csv, outCsv)
  q <- paste(usrID,"you can now download",outCsv); cat.IO(q); sqlSave.LOG(q)
}
#
# Save IO results to database. Input is IO results object
#
saveIoDb <- function(res)
{
  conn <- odbcConnect(DSN)
  if (conn == -1) stop("Failed to connect to database")
  tbCols <- sqlColumns(conn, iorTbl)$COLUMN_NAME
  # Add fields from IO results
  ioRes <- res[,tbCols[tbCols %in% names(res)],with=FALSE]
  # Add scenario details
  ioNms <- c("USER_ID","SCN_ID","IO_DATE","IO_VALUE_TYPE","IO_DEM_TYPE")
  ioRes[,(ioNms):=list(usrID,scnID,sysTime,ioValueType,demType)]
  # Fill in remaining table columns
  otCols <- tbCols[!tbCols %in% names(ioRes)]
  if (length(otCols)) ioRes[, (otCols) := list(NA_character_)]
  sqlSave.IO(conn, ioRes, iorTbl)
  q <- paste("Inserted",nrow(ioRes),"records into",iorTbl,"..."); cat.IO(q); sqlSave.LOG(q)
  odbcClose(conn)
}
#
# Save IO projections to database. Inputs are IO and projection result
# data table objects. The first object is already keyed by (MAT_ID,LOC_ID)
#
saveIopDb <- function(res,ior)
{
  # Add IPOT details
  ioNms <- c("SUOM_ID","TUOM_ID","CUR_ID")
  setkeyv(ior,key(res))[res,(ioNms):=list(SUOM_ID,TUOM_ID,CUR_ID)]
  # Add scenario details
  ioNms <- c("USER_ID","SCN_ID","IO_DATE","IO_VALUE_TYPE","IO_DEM_TYPE")
  ior[,(ioNms):=list(usrID,scnID,sysTime,ioValueType,demType)]
  
  # Fill in remaining table columns
  conn <- odbcConnect(DSN)
  if (conn == -1) stop("Failed to connect to database")
  tbCols <- sqlColumns(conn, iopTbl)$COLUMN_NAME
  otCols <- tbCols[!tbCols %in% names(ior)]
  if (length(otCols)) ior[, (otCols) := list(NA_character_)]
  sqlSave.LOG("***************************************************")
  q <- paste("Inserting",nrow(ior),"records into",iopTbl,"..."); cat.IO(q); sqlSave.LOG(q)
  sqlSave.IO(conn, ior, iopTbl)
  odbcClose(conn)
}
#
# Checks before demand or forecasts rollup. Rollup can only be applied when
# the following conditions hold for any NETWORK_ID combination:
# - topmost nodes have level 0
# - more than one level is present
# - all levels in between are present
# - generic lead time is available at all levels (supplier lead time is not necessary)
#
checksRollup <- function(levs,ltdist) {
  roll <- TRUE
  minL <- min(levs)   # lowest level (should be 0)
  maxL <- max(levs)   # highest level
  # check whether all levels are present
  notL <- which(!(seq.int(minL,maxL) %in% levs))
  
  # Availability of generic lead time
  nolt <- sapply(ltdist[levs!=minL], function(x) is.na(x$ltAvg))
  
  if (minL || (maxL==minL) || length(notL) || any(nolt)) roll <- FALSE
  return(roll)
}
#
# Demand or forecast rollup checks. Input is data.table object keyed by NETWORK_ID.
# Object is changed by copy, as deleting rows cannot be done by
# reference yet. Hence, the changed object needs to be returned.
#
rollupChecks <- function(dto)
{
  hll <- dto[,if (!checksRollup(LEVEL,ltDist)) list(hll=.I), by=key(dto)]$hll
  if (length(hll)) {
    q <- paste(length(hll),"Networks that did not pass rollup checks..."); cat.IO(paste0(q,"\n")); sqlSave.LOG(q)
    write.table.IO(dto[hll,c("MAT_ID","LOC_ID","LEVEL",key(dto)),with=FALSE])
    q <- "Deleting networks that did not pass rollup checks..."; cat.IO(q); sqlSave.LOG(q)
    dto <- dto[-hll]
  }
  if (!nrow(dto)) { q <- "No networks left !"; cat.IO(q); sqlSave.LOG(q); stop(q) }
  return(dto)
}
#
# Roll up demand or forecast starting at the bottom level and ending at the top.
# Use rounded mean generic lead time from the LT model plus service time.
# Input is IPOT data frame for one NETWORK_ID. Output is the same data frame
# updated to include the rolled up demand at all levels
#
signalRollup <- function(pot, dns)
{
  levs <- sort.int(unique(pot$LEVEL),decreasing=TRUE)
  idx <- c("MAT_ID","LOC_ID")
  setDT(pot)
  for (i in levs[levs>0]) {
    setkey(pot,LEVEL)
    potL <- pot[J(i), which=TRUE]
    # Bind child material/location/demand to parent(s) material/location/transaction_factor
    potB <- cbind(pot[potL,c(idx,"ltDist",(dns)),with=FALSE],rbindlist(pot[potL,PARENT]))
    
    # Bind average LT vector to binded dataframe, apply the lag function to each row in turn to get
    # the lagged demand, and sum the resultant matrix by row to get the rolled up demand. Use sapply()
    # to get the vector of average LT + review period, and na.rm = TRUE in rowSums
    levSum <- setDT(ddply(potB, .(MAT_PARENT_ID,LOC_PARENT_ID), function(x) {
      rowSums(apply(cbind(sapply(x$ltDist, function(y) round(y$ltAvg)), x$TRANS_FACTOR,
                          x[,dns]), 1, lagFn),na.rm=TRUE) }))
    z2 <- setkeyv(pot,idx)[levSum,which=TRUE]
    
    # Add external demand to rolled up demand
    pot[z2,(dns):= pot[z2,(dns),with=FALSE] + levSum[,-c(1:2),with=FALSE]]
  }
  return(pot)
}
#
# Lag function for rolling up demand from downstream locations. Input is a vector of numeric
# values, with the first element representing the wanted lag, and the rest being the demand series
#
lagFn <- function(x)
{
  lags <- x[1]; tfac <- x[2]; data <- x[-c(1:2)]
  return(c(tfac*data[(lags+1):length(data)],rep(NA, lags)))
}
#
# Add a new column to IPOT data table object, to store following as a list:
# GEN_LEAD_TIME, GEN_LEAD_TIME_SD, REVIEW_PERIOD, and historical LT data series
# (numeric(0) when not present). Input object is changed by reference
#
getLeadTimes <- function(data, ltn)
{
  if (length(ltn)) {
    ltSeries <- apply(data[,ltn,with=FALSE],1,function(x) rep(as.numeric(ltn),x))
    data[,lt:= list(mapply(function(x,y,z,v) list(clt=x,csd=y,rp=z,lts=v),
                      data[,GEN_LEAD_TIME],data[,GEN_LEAD_TIME_SD],
                      data[,REVIEW_PERIOD],ltSeries,SIMPLIFY=FALSE))]
  } else { # No lead time columns, set LT series to empty vector
    ltSeries <- as.numeric(ltn)
    data[,lt:= list(mapply(function(x,y,z) list(clt=x,csd=y,rp=z,lts=ltSeries),
                      data[,GEN_LEAD_TIME],data[,GEN_LEAD_TIME_SD],
                      data[,REVIEW_PERIOD],SIMPLIFY=FALSE))]
  }
  # Can drop columns not needed anymore
  data[,c("GEN_LEAD_TIME","GEN_LEAD_TIME_SD","REVIEW_PERIOD"):=NULL]
  if (length(ltn)) data[,(ltn):=NULL]
  
  # Nothing to return, as data table has been updated in here
  return(invisible(NULL))
}
#
# Model distribution of generic LT (transport/production lead time + transaction period).
# Input is the list object returned by getLeadTimes().
# If the "minPts" and "minUnq" requirements are not met: (a) Lead Time is assumed to
# follow a truncated Gamma distribution; (b) if Lead Time is NA or 0, no model is returned;
# (c) if the standard deviation is not available, the one set up in setIpotOpts() will be used.
# If the requirements are met: (a) a discrete or continuous distribution is chosen based
# on the minimum AIC; (b) if the fit is poor non-parametric bootstrap is chosen.
# Output is a list containing details of LT model, and more precisely:
# - lts = time series of past lead times + constant review period
# - ltsPts = total number of lts values
# - ltsUnq = number of different lts values
# - ltTotal = average lead time + review period
# - ltSd = lead time standard deviation
# - ltAvg = average lead time
# - rwp = review period
# - distr = lead time probability model
# - method = parametric (probabilistic) or non-parametric (bootstrapped)
# - pars = parameters of parametric model

#
fitLeadTimes <- function(ltime, minPts, minUnq, ltexp, ubn, lbn, ddistr, cdistr, meth)
{
  # GEN_LEAD_TIME, GEN_LEAD_TIME_SD, REVIEW_PERIOD, GEN_LEAD_TIME+REVIEW_PERIOD, LT time series
  clt <-ltime$clt; csd <- ltime$csd; rp <- ltime$rp; lts <- ltime$lts
  ltPts <- length(lts)          # total number of LT values
  ltUnq <- length(unique(lts))  # number of distinct LT values
  
  ltMean <- ltStd <- distn <- ltMeth <- pars <- NA_real_
  
  if (ltPts >= minPts && ltUnq >= minUnq) {
    lts <- lts+rp # adjusting the mean
    # Try parametric fitting with discrete or continuous distribution for discrete data, continuous
    # for continous data (fitting discrete distribution to continuous data generates warnings)
    if (isTRUE(all.equal(as.integer(lts), as.numeric(lts))))
      fitObj <- fitPDF(lts, c(ddistr, cdistr), meth)
    else fitObj <- fitPDF(lts, cdistr, meth)
    if (!is.null(fitObj$distname)) { # mean and standard deviation of fitted distribution
      ltMeth <- paste0("PDF (",fitObj$distname,")")
      names(fitObj$estimate) <- names(fitObj$mean) <- names(fitObj$std) <- NULL
      pars <- fitObj$estimate; ltMean <- fitObj$mean; ltStd <- fitObj$std
    } else { # mean and standard deviation of original data
      ltMeth <- "Bootstrap"; ltMean <- sum(lts)/ltPts; ltStd  <- sd(lts)
    }
  }
  else if (!is.na(clt) && clt>0) { # fit a truncated Gamma
    # Fit one Gamma or another depending on availability of standard deviation
    if (!is.na(csd) && csd>0) vr <- csd^2 else
      vr <- clt^ltexp/(clt*2)^(ltexp-1) # variance v=(m^t)/(2m)^(t-1)
    # Adjust mean and bounds of truncated Gamma
    lbn <- lbn+rp; ubn <- clt*ubn+rp
    distn <- "trunc gamma"; ltMeth <- "Trunc Gamma"
    # scale and shape parameters 
    s <- vr/(clt+rp); a <- (clt+rp)/s
    pars <- c(lbn, ubn, a, 1/s)     # lower and upper bounds, shape, rate=1/scale
    #pars <- c(0, Inf, a, 1/s)      # (untruncated Gamma option)
    
    # Mean and standard deviation of truncated Gamma (mean of Gamma = a*s, variance = a*s^2)
    ltMean <- extrunc("gamma",pars[1],pars[2],pars[3],pars[4])
    ltStd <- sqrt(vartrunc("gamma",pars[1],pars[2],pars[3],pars[4]))
  }
  # Return parameters of LT model (lead time + transaction period) together
  # with its random and fixed components (lead time and transaction period)
  result <- list(lts = lts, ltsPts = ltPts, ltsUnq = ltUnq, ltTotal = ltMean, ltSd = ltStd,
                 ltAvg = ltMean-rp, rwp = rp, distr = distn, method = ltMeth, pars = pars)
  return(result)  
}
#
# Model distribution of demand/forecast/forecast error. Input is a numeric vector of
# non NA data which passed the minimum requirements set in setIpotOpts().
# An appropriate discrete or continuous distribution is chosen based on
# the minimum AIC. If the fit is poor non-parametric bootstrap is chosen.
# Output is a list containing details of the chosen model, and more precisely:
# - sgs= signal time series
# - sgAvg = signal average
# - sgSd = signal standard deviation
# - distr = signal probability model
# - method = parametric (probabilistic) or non-parametric (bootstrapped)
# - pars = parameters of parametric model 
#
fitSignal <- function(signal, ndistr, cdistr, ddistr, meth)
{
  sgMean <- sgStd <- distn <- sgMeth <- pars <- NA_real_
  # Try parametric fitting with discrete or continuous distribution for discrete data, continuous
  # for continous data (fitting discrete distribution to continuous data generates warnings)
  if (any(signal<0)) fitObj <- fitPDF(signal, ndistr, meth)
  else if (isTRUE(all.equal(as.integer(signal), as.numeric(signal))))
    fitObj <- fitPDF(signal, c(ddistr, cdistr), meth)
  else fitObj <- fitPDF(signal, cdistr, meth)
  if (!is.null(fitObj$distname)) { # mean and standard deviation of fitted distribution
    sgMeth <- paste0("PDF (",fitObj$distname,")")
    names(fitObj$estimate) <- names(fitObj$mean) <- names(fitObj$std) <- NULL
    pars <- fitObj$estimate; sgMean <- fitObj$mean; sgStd <- fitObj$std
  } else { # mean and standard deviation of original data
    sgMeth <- "Bootstrap"; sgMean <- sum(signal)/length(signal); sgStd  <- sd(signal)
  }
  return(list(sgs= signal, sgAvg = sgMean, sgSd = sgStd, distr = distn, method = sgMeth, pars = pars))
}
#
# Change table of signal data (demand/forecast/forecast error) to list of items.
# Input is an n-row data table of signal time series. Output is a list of n items,
# where each item holds the input signal without NA values
#
listSignal <- function(data)
{
  # NA values are not needed anymore. If such values are removed from a matrix or data frame,
  # the two-dimensional structure collapses to a list; whereas if no NA values exist, the original
  # structure is retained. For consistency, change data frame to list before removing NA
  data <- as.list(as.data.frame(t(data), optional = TRUE))
  # Return list of input signal data without NA
  return(lapply(data, function(x) x[!is.na(x)]))
}
#
# Returns mean of fitted distribution, as this is not necessarily equal to
# the first fitted parameter estimate. Used inside fitPDF()
#
estMean <- function(fitObj)
{
  switch(fitObj$distname,
         pois   = fitObj$estimate[1],
         nbinom = fitObj$estimate[1]*(1-fitObj$estimate[2])/fitObj$estimate[2],
         geom   = (1-fitObj$estimate[1])/fitObj$estimate[1],
         norm   = fitObj$estimate[1],
         exp    = 1/fitObj$estimate[1],
         gamma  = fitObj$estimate[1]/fitObj$estimate[2])
}
#
# Returns standard deviation of fitted distribution, as this is not necessarily equal to
# the first or second fitted parameter estimate. Used inside fitPDF()
#
estStd <- function(fitObj)
{
  switch(fitObj$distname,
         pois   = sqrt(fitObj$estimate[1]),
         nbinom = sqrt(fitObj$estimate[1]*(1-fitObj$estimate[2]))/fitObj$estimate[2],
         geom   = sqrt(1-fitObj$estimate[1])/fitObj$estimate[1],
         norm   = fitObj$estimate[2],
         exp    = 1/fitObj$estimate[1],
         gamma  = sqrt(fitObj$estimate[1])/fitObj$estimate[2])
}
#
# Fits a series of probability distributions from "distr" to "data" by "meth" method.
# Chooses PDF with lowest AIC and performs goodness-of-fit tests on it. If the fit is
# acceptable, a non-null list object with details of the fit is returned.
# When the number n of data is small and the number k of fitted parameters is large,
# AICc is recommended over AIC, as it places greater penalty for extra parameters.
# However, we use AIC here because AICc penalizes 2-parameter in favor of 1-parameter
# distributions (the AIC penalizes already, so it would be unfair to penalize further)
#
fitPDF <- function(data, distr, meth)
{
  fitg <- NULL # fitted object to return
  aic <- numeric(length(distr))
  for (i in seq_along(aic)) aic[i] <- fitdist(data, distr[i], method = meth)$aic
  #AICc = AIC + 2*k*(k+1)/(n-k-1), n=length(data), k=length(fitdist(...)$estimate)
  aic[!is.finite(aic)] <- NA_real_  # AIC could be NaN or Inf (e.g., discrete PDF fitted
  # to continuous data and/or to "norm" data with -ve mean
  iAic <- which.min(aic)
  if (length(iAic)) { # at least one AIC value
    # save PDF with lowest AIC value. NOTE: quantity exp((AICmin-AICi)/2) is the
    # relative likelihood of model i. If for example AICmin=100 and AICi=101, model i
    # is 0.61 times as probable as the first model to minimize the information loss    
    fitg <- fitdist(data, distr[iAic], method = meth)
    # Perform goodness of fit tests H0-H1
    # 1) chi-squared test: applies to discrete and continuous distributions.
    # disadvantages: (i) value of test statistic depends on how data is binned
    # (ii) requires sufficient sample size in order for test statistic to be valid
    # 2) Kolmogorov-Smirnov (KS) test: restricted to continuous distributions.
    # advantage: more powerful than chi-squared with small sample size
    # disadvantage: distribution parameters must be pre-specified, not estimated from data
    # (3) Anderson-Darling (AD) test: removes disadvantage of KS test. May be used with very small
    # sample sizes. Very large sample sizes may reject H0 with only slight imperfections, as
    # they increase the sensitivity of the test that any minute deviation from H0 will return H1.
    chisq <- ks <- ad <- 0
    # Solution: avoid chi-squared when more than 20% of table cells have expected cell
    # frequencies less than 5 (Cochran). Avoid KS and AD tests otherwise (very sensitive to
    # small deviations from H0 with sample sizes > 100, possibly due to discrete data)
    #
    # gofstat() fails for e.g. demand data = c(rep(0,364),3,1000), or when fitting  a nbinom to
    # rpois(50,10). In such cases reject H0, use try() to avoid exit failure and suppressWarnings()
    # to avoid warnings)
    # gofstat() fails for e.g. demand data = c(rep(0,364),3,1000), or when fitting  a nbinom to
    # rpois(50,10). In such cases reject H0; ad.test() may also fail with discrete data, and KS test
    # gives warnings about ties. Use try() to avoid exit failure, and suppressWarnings() to avoid warnings
    gof <- NULL
    suppressWarnings(try(gof <- gofstat(fitg), silent = TRUE))
    if (is.null(gof)) fitg <- NULL
    else if ((sum(gof$chisqtable[ ,2] < 5)/nrow(gof$chisqtable) > 0.2))
    { # KS and AD tests
      pDist <- paste0("p", fitg$distname); ad <- NULL
      if (length(fitg$estimate) == 2)
      { # 2-parameter distribution
        ks <- suppressWarnings(ks.test(data, pDist, fitg$estimate[1],fitg$estimate[2]))$p.value
        suppressWarnings(try(ad <- ad.test(data, get(pDist), fitg$estimate[1], fitg$estimate[2])$p.value, silent = TRUE))
      } else { # 1-parameter distribution
        ks <- suppressWarnings(ks.test(data, pDist, fitg$estimate))$p.value
        suppressWarnings(try(ad <- ad.test(data, get(pDist), fitg$estimate)$p.value, silent = TRUE))
      }
      if (is.null(ad)) fitg <- NULL
    } else chisq <- gof$chisqpvalue # chi-squared p value
    if (!is.null(fitg)) {
      # Accept fit if and only if H0 could not be rejected at the 5% significance level
      # (Type I error, false positive). For KS and AD tests, take mean p value (ad hoc)
      pvalue <- 0.05
      if (sum(c(chisq, .Internal(mean(c(ks, ad)))) > pvalue) < 1) fitg <- NULL
      else {
        if (fitg$distname == "nbinom") {
          # 2nd fitted parameter is mean "mu", change to "prob" = size/(size+mu) for subsequent use
          # (random number generation function rnbinom() uses size as 1st and prob as 2nd parameter)
          fitg$estimate[2] <- fitg$estimate[1]/(fitg$estimate[1] + fitg$estimate[2])
          names(fitg$estimate)[2] <- "prob"
        }
        # Estimates of mean and standard deviation
        fitg$mean <- estMean(fitg); fitg$std  <- estStd(fitg)
      }
    }
  }
  result <- list(distname = fitg$distname, estimate = fitg$estimate, mean = fitg$mean, std = fitg$std)
  return(result)
}

#################################
### Identify MEIO networks section ###
################################# 
#
# Identify nodes with and without children.
# Input is material/location data frame for one NETWORK_ID.
# A new column HAS_CHILDREN of type logical (TRUE or FALSE) is added
#
hasChildren <- function(loc)
{
  idx <- c("MAT_ID","LOC_ID")
  setkey(setDT(loc),LEVEL)
  z <- data.table()
  for (i in unique(loc$LEVEL)) {
    locL <- loc[J(i)]
    # Bind children to parents nodes
    locB <- setkey(unique(rbindlist(loc[!locL,PARENT])[,.(MAT_PARENT_ID,LOC_PARENT_ID)]))
    z <- rbindlist(list(z,locB[setkeyv(locL,idx),.(MAT_ID,LOC_ID)]))
  }
  # New column identifying nodes with and without children
  loc[,HAS_CHILDREN := 1:nrow(loc) %in% setkeyv(loc,idx)[setkeyv(z,idx),which=TRUE]]
}
#
# Separate networks and assign node levels.
# Input is material/location data table keyed by (MAT_ID,LOC_ID),
# with nodes at level 0 already having been identified.
# Input is changed by reference:
# - updating column LEVEL with the other node levels;
# - creating column NETWORK_ID to group nodes under a same network
#
findNetLevs <- function(loc)
{
  #create temporary data table to store the levels and the networks found during the process
  tmpDT <- data.table( MAT_ID = loc$MAT_ID, LOC_ID = loc$LOC_ID, PARENT = loc$PARENT, LEVEL  = loc$LEVEL)
  assign("tmpDT", tmpDT, envir = .GlobalEnv)
  
  #assign the network corresponding to the top node parents matId@locId
  tmpDT[LEVEL== 0 ,NETWORK_ID := paste0(MAT_ID,"@",LOC_ID)]
  
  # while there are some matlocs without any network or level assigned, continue the search
  while (any(is.na(tmpDT$LEVEL))) {
    # assign next level of the matloc hierarchy
    tmpDT[is.na(LEVEL), LEVEL := sapply(PARENT, findParentLvl)]
    # assign network to the next level of the hierarchy
    tmpDT[is.na(NETWORK_ID), NETWORK_ID := sapply(PARENT, findNetwork)]
  }
  # Update input data table (keyed by MAT_ID,LOC_ID) with new columns
  setkeyv(tmpDT, key(loc))
  loc[,c("LEVEL","NETWORK_ID") := list(tmpDT[,LEVEL],tmpDT[,NETWORK_ID])]
  # Delete newly created temporary data table
  rm(tmpDT, envir = .GlobalEnv)
}
###
#
#   given a matloc, assign the level to it by searching the level of their parents and adding 1
#
###
findParentLvl <- function(parents)
{
  parents <- data.frame(MAT_ID = parents$MAT_PARENT_ID, LOC_ID = parents$LOC_PARENT_ID)
  
  # find the levels of the parents
  levs <- apply(parents, 1, function(x)	tmpDT[which(x[1] == tmpDT$MAT_ID & x[2] == tmpDT$LOC_ID), .(LEVEL)])
  
  #return the levels of the parents plus one
  return(max(unlist(levs))+1)
}
###
#
# given a matloc, assign the network to which it belongs by loking for the network of their parent. In case of entangled networks
# update the network of their parents too
#
###
findNetwork <- function(parents)
{
  parents <- data.frame(MAT_ID = parents$MAT_PARENT_ID, LOC_ID = parents$LOC_PARENT_ID)
  
  #fint the network of the parents
  nets <- apply(parents, 1, function(x)	tmpDT[which(x[1] == tmpDT$MAT_ID & x[2] == tmpDT$LOC_ID), .(NETWORK_ID)])
  
  #for all matlocs that have been found with one parent containing an already existing network
  if (!is.na(nets[[1]])) {
    # create the network identifier separating with '#' between consecutive matId@locId
    res <- paste0(unlist(nets), collapse="#")
    
    # update the network ID of the parents if necessary
    for (i in 1:dim(parents)[1])
      tmpDT[which(tmpDT$MAT_ID == parents[i, "MAT_ID"] & tmpDT$LOC_ID == parents[i, "LOC_ID"]), NETWORK_ID := res]
  } else res <- NA
  
  # update tmpDT global variable
  assign("tmpDT", tmpDT, envir = .GlobalEnv)
  return(res)
}

#################################
### Generate Safety Stock projections section ###
#################################
#
projectSafetyStock <- function(data, dns)
{
  cat.IO(" *")
  with(data, {
    fcs_dem <- data[,dns]
    
    lenFcs <- length(fcs_dem)
    # convert forecast annual frequency to forecast period in days
    period <- switch(as.character(FCS_FREQ), "12" = 30, "52" = 7, "365" = 1)
    # generate forecast dates, add months to date using package lubridate "m+" function
    if (period == 30) calendar <- toDate(dns[1]) %m+% months(c(0:(lenFcs-1))) else
      calendar <- toDate(dns[1]) + seq.int(0, by = period, length.out = lenFcs)
    
    sfstcov_forecast <- forecastMean <- sfstcov_days_STR_exDLT_mean <- sfstcov_days_STR_Weekly_mean <- sfstcov_days_demand_exDLT <- 0
    if (sum(fcs_dem)) {
      weeks <- 1:lenFcs
      forecast_edlt <- unlist(sapply(weeks, getWeekSfSt, as.numeric(ltTotal), period, fcs_dem))
      exDLT_forecast <- .Internal(mean(forecast_edlt[forecast_edlt > 0]))
      
      sfstcov_edlt <- round(as.numeric(IO_SAFETY_STOCK_QTY) / as.numeric(exDLT_forecast), 2)
      sfstcov_days_STR_exDLT_mean <- max(round(sfstcov_edlt * as.numeric(ltTotal), 0),1)
      ### Forecast based projection #######
      forecastMean <- .Internal(mean(fcs_dem[fcs_dem != 0])) # get the average weekly forecast
      ### str weekly demand projection #######
      sfst_projection <- round(unlist(sapply(weeks, getWeekSfSt, sfstcov_days_STR_exDLT_mean, period, fcs_dem)))
      ind <- match(TRUE, fcs_dem > 0)   # find first week of forecast
      sfst_projection[0:(ind-1)] <- 0       # assign SfSt to the first weeks not containing forecast
    } else sfst_projection <- rep(0, lenFcs)
    result <- cbind(MAT_ID,LOC_ID,IO_FREQ = FCS_FREQ, IO_PERIOD = as.character(calendar), IO_SAFETY_STOCK_QTY = sfst_projection)
    return(result)
  })
}
# 
#   Given a week of the forecast series, calculate the associated Safety Stock in that week.
#   
#   @input parameters:
#       week: number of forecast periods
#       sfstcov: safety stock in days
#
getWeekSfSt <- function(week, sfstcov, period, forecast)
{
  result <- 0
  if(sfstcov <= period) result <- (forecast[week]*sfstcov)/period else {
    numweeks <- trunc(sfstcov/period)
    days <- sfstcov%%period
    i <- 0
    forecastweeks <- length(forecast)
    while (i < numweeks & week+i <= forecastweeks)
    {
      result <- result + forecast[week+i]
      i <- i+1
    }
    if (week + numweeks < forecastweeks ) result <- result + (forecast[week + numweeks]*days)/period
  }
  return(result)
}
