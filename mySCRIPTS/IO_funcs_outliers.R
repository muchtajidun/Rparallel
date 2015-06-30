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
# Set options for outliers treatment
#
setOutlierOpts <- function()
{
  pars <- list()
  
  # Parameters for demand outliers routine
  pars$dmThr    <- 0.95  # upper q sample quantile used to truncate extreme values
  pars$dmCor    <- 0.5   # minimum value of correlation coefficient neeeded to estimate linear trend
  pars$dmpTrd   <- 10    # minimum number of non-zero values needed by routine
  pars$dmuTrd   <- 3     # minimum number of unique non-zero values needed by routine
  
  return(pars)
}
#
# Find outliers in demand data. Subtract a possible linear trend before the analysis.
# Inputs: - row of iPot data containing demand and row index data (Mat_id, Loc_id)
#     - vectors of index (id) and demand names (dns)
#     - minimum number of non-zero total and distinct demand points
#	    - minimum serial correlation value to justify a linear trend
#	    - (q/100) sample quantile of (possibly detrended) non-zero demand data to be used as threshold
# Output: row of replacement values
#
findOutliers <- function(x, id, dns, minPts, minUnq, crr, q)
{
  dm <- as.numeric(x[dns])
  tData <- 1:length(dm)
  
  idx <- dm>0 # not necessary to include # & !is.na(dm) # as NA were previously set to 0
  dm <- dm[idx]; tData <- tData[idx]
  
  ret <- NULL # return value
  
  # Minimum "minPts" non-zero and non-NA values and "minUnq" distinct non-zero and non-NA values
  # for correlation, linear model and quantile estimation
  if (length(dm) >= minPts && length(unique(dm)) >= minUnq)
  { # Check if there is time correlation
    cr <- cor(dm, tData, method="k") # robust measure of correlation
    # If there is enough linear correlation we conclude there is a trend
    if (!is.na(cr) && abs(cr)>=crr)                              # the case with just 1 unique dm gives cor=NA
      slope <- lqs(dm~tData,method="lts",adjust=FALSE)$coef[2]  # robust slope estimate
    # Alternative: slope <- rlm(dm~tData)$coef[2]
    else slope <- 0                                             # no trend
    # Replace extreme demand values with threshold based on q-quantile of detrended data
    # NOTE: threshold based on quantile will probably be replaced by threshold based
    # on median absolute deviation in future version: http://www.r-bloggers.com/winsorization/
    thr <- ifelse(slope, quantile(dm-slope*tData,q), quantile(dm,q))
    # Derive threshold values for original trended data
    if (slope) thr <- thr+slope*tData else thr <- rep_len(thr,length(tData))
    # Discrete threshold if original demand is discrete
    if(isTRUE(all.equal(as.integer(dm), dm))) thr <- round(thr)
    # Find demand points above threshold
    ind <- which(dm > thr)
    if(length(ind)) { # Replace extreme demand values
      ret <- thr[ind]; names(ret) <- dns[tData[ind]]; ret <- as.data.table(c(x[id],ret))
    }
  }
  return(ret)
}
