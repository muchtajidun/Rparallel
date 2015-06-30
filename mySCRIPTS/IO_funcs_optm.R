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
# DATA OPTIMIZATION FUNCTIONS
#
# Set optimization options, mainly MEIO algorithm choice and stopping criteria.
# SEIO now uses Leibnitz rule rather than 1-D optimization
#
setOptOpts <- function()
{
  opt <- list()
  
  # Number of random samples generated from PDF model of LT, or drawn with replacement
  # from LT data. For each random sample, a number of demand data equal to the sample
  # value will be generated from PDF model of demand, or drawn with replacement from
  # the demand data. Used in Monte Carlo simulation
  opt$nB <- 5000
  
  # The following sets random number generator in order to reproduce the same results
  # from one session to the other (even when R is restarted)
  setRNG(kind="Wichmann-Hill", seed=c(979,1479,1542), normal.kind="Box-Muller")
  
  if (ioType == ioType2) {
    # MEIO Stopping criteria ( nloptr.print.options() ):
    # stopval: Stop when value of cost function G(R,D) <= stopval       (default -Inf)
    # ftol_rel: Stop when G(R,D) changes by less than ftol_rel*G(R,D)   (default 0.0)
    # ftol_abs: Stop when G(R,D) changes by less than ftol_abs          (default 0.0)
    # xtol_rel: Stop when every ROP changes by less than xtol_rel*ROP   (default 1e-4)
    # xtol_abs: Stop when every ROP[i] changes by less than xtol_abs[i] (default 0.0)
    # maxeval: Stop when number of G(RD) evaluations > maxeval          (default 100)
    # maxtime: Stop when time (in seconds) > maxtime                    (default -1.0)
    # tol_constraints_ineq:  Stop when ROP(SLmin)-ROP(SL) <= tolerance  (default 1e-8)
    #
    # MEIO algorithms choice:
    # algorithm: one of NLOPT_GN_ORIG_DIRECT, NLOPT_GN_ORIG_DIRECT_L, NLOPT_GN_ISRES for
    # global nonlinear optimization with (non)-linear inequality constraints; optionally
    # followed by NLOPT_LN_COBYLA for local nonlinear optimization with (non)-linear
    # inequality constraints (NULL to skip local optimization step)
    #
    # Other options for NLOPT_GN_ISRES:
    # population: initial population size > 0                           (heuristic default 0)
    # ranseed: greater than 0 to reproduce same result from run to run  (default 0)
    
    opt$glb <- list("algorithm"= "NLOPT_GN_ORIG_DIRECT_L",  # NLOPT_GN_ORIG_DIRECT, NLOPT_GN_ISRES
                    "print_level" = 0,                    # 1,2,3
                    "stopval" = 0,
                    "ftol_rel" = 5e-02,
                    "ftol_abs" = 1,
                    "xtol_rel" = 5e-02,
                    "xtol_abs" = 1,
                    "maxeval" = 100,
                    "ranseed" = 1)
    opt$loc <- list("algorithm"="NLOPT_LN_COBYLA",      # NULL
                    "print_level" = 0,
                    "stopval" = 0,
                    "ftol_rel" = 1e-02,
                    "ftol_abs" = 1e-01,
                    "xtol_rel" = 1e-03,
                    "xtol_abs" = 1,
                    "maxeval" = 200)
    # Set lower and upper bounds [(1-opts$bnd)*ROP,(1+opts$bnd)*ROP]
    # for local optimization, around ROP value from global optimization 
    opt$lbd <- 1e-01
  }
  return(opt)
}
#
# Generates nB random samples from random distribution distr with parameter(s) pars.
# This may include a truncated Gamma
#
generatePDF <- function(distr, pars, nB)
{
  if (distr == "trunc gamma") rSim <- rtrunc(nB,"gamma",pars[1],pars[2],pars[3],pars[4]) else {
    rDist <- paste0("r", distr); rSim <- do.call(rDist, as.list(c(nB, pars)))
  }
  return(rSim)
}
#
# Generates simulated signal values. Inputs are two lists containing all the
# necessary signal and LT data and fit details, with nB being the number of
# random samples generated from PDF model of LT (parametric bootstrap) or
# drawn with replacement from LT data (non parametric bootstrap).
# N.B.: Monte Carlo simulation of signal (demand/forecast/forecast error) during Lead Time
# is only necessary for staged nodes. For unstaged nodes simulation of Lead Time is sufficient.
# Output is a list object containing two numeric vectors of length nB, one containing simulated
# lead time samples, and the other containing simulated lead time signal samples (only for staged nodes).
# NOTE: If a model could not be fitted to either signal or lead time because of lack
# of data values or parameters, empty vectors will be returned
#
simulateSignals <- function(sgd, ltd, nB, staged)
{
  ltSim <- jdSim <- numeric(0) # the two returned vectors
  
  # Generate nB simulated LT values
  if (!is.na(ltd$distr))            # parametric fitting of LT
    ltSim <- generatePDF(ltd$distr, ltd$pars, nB)
  else if (!is.na(ltd$method))     # Non parametric bootstrap of LT
    ltSim <- sample(ltd$lts, nB, replace = TRUE)
  
  # Generate nB simulated values of lead time signal (for staged nodes)
  if (staged && !is.null(ltSim)) jdSim <- simulate2(sgd, ltSim)
  return(list(ltSim = ltSim, jdSim = jdSim))
}
#
# Generates simulated lead time signal values. Inputs are a list containing signal
# (demand/forecast/forecast error) data and fit details, and a vector of lead time
# LT values. As the LT values are in general not discrete (they may follow a truncated
# Gamma distribution), R function ceiling() is used to allow the generation of a discrete
# number of signal values for each LT value (example: if LT value = 5.3, generate 6 signal
# values, sum 5 signal values and add 0.3 times the 6th value). Output is a one-item list
# which contains the vector of simulated lead time signal values
#
simulate2 <- function(sgd, lts)
{
  jds <- numeric(0) # returned vector
  
  if (!is.na(sgd$distr))       # Parametric fitting of signal
    jds <- sapply(lts,function(x)   sumPartial(x, generatePDF(sgd$distr,sgd$pars,ceiling(x))))
  else if (!is.na(sgd$method)) # Non parametric bootstrap of signal
    jds <- sapply(lts,function(x,y) sumPartial(x, sample(y,ceiling(x),replace=TRUE)),y=sgd$sgs)
  return(jds)
}
#
# Sums all values in vector y if x is a whole number, otherwise sum all but the last value
# and add a fraction of the last value proportional to the fractional part of x.
# Use inline C++code for faster execution
#
src <- '
double x = as<double>(x_); Rcpp::NumericVector y(y_);
double sump = 0, xf = x - floor(x); int ny = y.size();
for (int i = 0; i < ny; i++) sump += y[i];
if (xf) sump -= (1-xf)*y[ny-1];
return(wrap(sump));'
sumPartial <- cxxfunction(signature(x_="numeric",y_="numeric"),
                          body=src, plugin="Rcpp")
#
# Initialize IPOT quantities which will not need to be changed between MEIO iterations:
# - lead time LT and lead time demand LTD at level 0 (not affected by effective lead time eLT)
# Input is IPOT data table, changed by reference along the way as eLT and eLTD are added.
# N.B.: Unstaged nodes will not be optimized hence eLTD is not calculated at such nodes
#
ropMeioIni <- function(pot, nB)
{
  # List of items holding simulated LT and LTD vectors at level 0; these are already
  # equal to eLT and eLTD (probability of stocking out at supplier is zero) so they
  # do not need to be simulated again at each iteration of the optimization
  ltd <- with(pot[LEVEL==0], mapply(simulateSignals, sgDist, ltDist, nB, STAGED, SIMPLIFY = FALSE))
  # eLT required for staged and unstaged nodes 
  pot[LEVEL==0, elt := list(lapply(ltd, function(x) x$ltSim))]
  # eLTD only required for staged nodes
  invisible(pot[LEVEL==0 & STAGED, eltd := list(lapply(ltd, function(x) x$jdSim))])
}
#
# Estimate lower ROP bounds needed as inputs to MEIO optimization function.
# Lowest ROP values should be obtained with null shifts of eLTD with respect to LTD,
# this will occur at SL_MAX = 1 (prob.StockOut = 0). Input is IPOT data table,
# changed by reference along the way as eLT, eLTD and ROP are added.
# N.B.: Unstaged nodes are assigned prob.StockOut = 1 and will not be optimized,
# hence eLTD is not calculated at such nodes
#
ropEstimateMin <- function(pot, nB)
{
  # List of items holding starting simulated LT and LTD vectors; these will be updated to hold
  # eLT and eLTD vectors. Not need to do this for level 0 (LT and LTD are already equal to eLT and eLTD)
  ltd <- with(pot[LEVEL>0], mapply(simulateSignals, sgDist, ltDist, nB, STAGED, SIMPLIFY = FALSE))
  # eLT required for staged and unstaged nodes
  pot[LEVEL>0, elt := list(lapply(ltd, function(x) x$ltSim))]
  # eLTD only required for staged nodes
  pot[LEVEL>0 & STAGED, eltd := list(lapply(ltd, function(x) x$jdSim))]
  
  levs <- sort.int(unique(pot$LEVEL))
  for (i in levs[levs>0]) {
    # Children rows at level i
    rowsCh <- pot[LEVEL==i,which=TRUE]
    for (k in rowsCh) {
      potB <- rbindlist(pot[k,PARENT])[,.(MAT_PARENT_ID,LOC_PARENT_ID)]
      # Parent row(s) of child k
      rowsPr <- pot[potB,which=TRUE]
      # ROP at staged node(s) as quantiles of eLTD corresponding to SL_MIN   
      pot[rowsPr, IO_ROP := ifelse(STAGED, mapply(function(x,y) quantile(x,y), eltd, SL_MIN), NA_real_)]
      # p.StockOut(parent) = 1 at unstaged node, 0 at staged node
      pot[rowsPr, pStOut := ifelse(STAGED, 0, 1)]
      # Find parent row with highest contribution to eLT
      rowPr <- rowsPr[pot[rowsPr,which.max(pStOut*sapply(ltDist,function(x) x$ltTotal))]]
      # Variable part of child k eLT
      eltv <- pot[rowPr,pStOut*unlist(elt)]
      # Sum child k fixed and variable eLT parts and update column
      pot[k,elt := list(list(unlist(elt)+eltv))]
      if (pot[k]$STAGED) {
        # Simulate variable part of child k eLTD
        eltdv <- pot[k, mapply(simulate2, sgDist, eltv)]
        # Sum child k fixed and variable eLTD parts and update column
        pot[k,eltd := list(list(unlist(eltd)+eltdv))]
      }
    }
  }
}
#
# Estimate upper ROP bounds needed as inputs to MEIO optimization function.
# Highest ROP values should be obtained with maximum shifts of eLTD with respect to LTD,
# this will occur at SL_MIN (highest prob.StockOut). Input is IPOT data table,
# changed by reference along the way as eLT, eLTD and ROP are added.
# N.B.: Unstaged nodes are assigned prob.StockOut = 1 and will not be optimized,
# hence eLTD is not calculated at such nodes
#
ropEstimateMax <- function(pot, nB)
{
  # List of items holding starting simulated LT and LTD vectors; these will be updated to hold
  # eLT and eLTD vectors. Not need to do this for level 0 (LT and LTD are already equal to eLT and eLTD)
  ltd <- with(pot[LEVEL>0], mapply(simulateSignals, sgDist, ltDist, nB, STAGED, SIMPLIFY = FALSE))
  # eLT required for staged and unstaged nodes
  pot[LEVEL>0,elt := list(lapply(ltd, function(x) x$ltSim))]
  # eLTD only required for staged nodes
  pot[LEVEL>0 & STAGED,eltd := list(lapply(ltd, function(x) x$jdSim))]
  
  levs <- sort.int(unique(pot$LEVEL))
  for (i in levs[levs>0]) {
    # Children rows at level i
    rowsCh <- pot[LEVEL==i,which=TRUE]
    for (k in rowsCh) {
      potB <- rbindlist(pot[k,PARENT])[,.(MAT_PARENT_ID,LOC_PARENT_ID)]
      # Parent row(s) of child k
      rowsPr <- pot[potB,which=TRUE]
      # ROP at staged node as quantile of eLTD corresponding to SL_MAX
      pot[rowsPr, IO_ROP := ifelse(STAGED, sapply(eltd, function(x) max(x)), NA_real_)]
      # p.StockOut(parent) = 1 at unstaged node, 1 - SL_MIN(parent) at staged node
      pot[rowsPr, pStOut := ifelse(STAGED, 1-SL_MIN, 1)]
      # Find parent row with highest contribution to eLT
      rowPr <- rowsPr[pot[rowsPr,which.max(pStOut*sapply(ltDist,function(x) x$ltTotal))]]
      # Variable part of child k eLT
      eltv <- pot[rowPr,pStOut*unlist(elt)]
      # Sum child k fixed and variable eLT parts and update column
      pot[k,elt := list(list(unlist(elt)+eltv))]
      if (pot[k]$STAGED) {
        # Simulate variable part of child k eLTD
        eltdv <- pot[k,mapply(simulate2, sgDist, eltv)]
        # Sum child k fixed and variable eLTD parts and update column
        pot[k,eltd := list(list(unlist(eltd)+eltdv))]
      }
    }
  }
}
#
# Input is IPOT data table, changed by reference along the way as eLT and eLTD
# are updated to hold 'effective' lead time eLT and lead time demand eLTD,
# level by level starting at 1 and ending at bottom.
# N.B.: Unstaged nodes are assigned prob.StockOut = 1 and will not be optimized,
# hence eLTD is not calculated at such nodes
#
effectiveLTD <- function(pot, nB)
{ 
  levs <- sort.int(unique(pot$LEVEL))
  for (i in levs[levs>0]) {
    # Children rows at level i
    rowsCh <- pot[LEVEL==i,which=TRUE]
    for (k in rowsCh) {
      potB <- rbindlist(pot[k,PARENT])[,.(MAT_PARENT_ID,LOC_PARENT_ID)]
      # Parents row(s) of child k
      rowsPr <- pot[potB,which=TRUE]
      # p.StockOut(parent) = 1 at unstaged node, 1 - SL(parent) at staged node, where SL(parent)
      # can be obtained from empirical cumulative distribution function of eLTD(parent)
      pot[rowsPr,pStOut := ifelse(STAGED, 1- mapply(function(x,y) x(y), lapply(eltd, ecdf), IO_ROP), 1)]
      # Find parent row with highest contribution to eLT
      rowPr <- rowsPr[pot[rowsPr,which.max(pStOut*sapply(ltDist,function(x) x$ltTotal))]]
      # Variable part of child k eLT
      eltv <- pot[rowPr,pStOut*unlist(elt)]
      # Sum child k fixed and variable eLT parts and update column
      pot[k,elt := list(list(unlist(elt)+eltv))]
      if (pot[k]$STAGED) {
        # Simulate variable part of child k eLTD
        eltdv <- pot[k,mapply(simulate2, sgDist, eltv)]
        # Sum child k fixed and variable eLTD parts and update column
        pot[k,eltd := list(list(unlist(eltd)+eltdv))]
      }
    }
  }
}
#
# Calculate global cost function G(R,D) at all locations.
# r = vector of global reorder points (variables in global optimization)
# pot = IPOT data table
# nB = number of replicates for MonteCarlo simulation
# lwb = ROP lower bounds needed in inequality constraints
#
glbCostFn <- function(r, pot, nB, lwb)
{
  # Calculate eLTD at all staged locations
  pot[(HAS_CHILDREN & STAGED), IO_ROP := r]
  effectiveLTD(pot, nB)
  # Local optimization at locations specified by two-stage optimization
  ropSEIO(pot)
  # Global cost function is sum of local cost functions
  with(pot[(STAGED)], sum(mapply(locCostFn, IO_ROP, eltd, HOLDING_COST, BACKLOG_COST)))
}
#
# Expected cost function G(R,D) at one location.
# r = Reorder Point (R), x = effective lead time demand (D),
# h = unit holding cost, b = unit backordering cost.
# Use inline C++code for faster execution
#
src <- '
double r = as<double>(r_), h = as<double>(h_), b = as<double>(b_);
Rcpp::NumericVector x(x_); double sumh, sumb; int nx = x.size();
for (int i = 0; i < nx; i++)
if (x[i]>r) sumb += r-x[i]; else sumh += r-x[i];
return(wrap((h*sumh-b*sumb)/nx));'
locCostFn <- cxxfunction(signature(r_="numeric",x_="numeric",
                                   h_="numeric",b_="numeric"),
                         body=src, plugin="Rcpp")
#
# Find optimum reorder point ROP and corresponding service level SL at all locations (M>=1)
# of a single echelon (N=1). Inputs is IPOT data table holding LTD and costs + SL restrictions.
# Leibnitz rule is applied to find optimum ROP which minimizes the cost functions G(R,D)
# at all locations where holding and backordering costs are specified.
# IPOT data table is updated by reference with optimal ROP and SL values.
# N.B.: This function is called from within SEIO and MEIO, acting only on bottom nodes (those
# without children) in the MEIO case; for SEIO all nodes are childless by definition
#
ropSEIO <- function(pot)
{
  with(pot[!(HAS_CHILDREN)], {
    rop <- mapply(function(x,y,h,b)
      ifelse(h|b,max(quantile(x,b/(b+h)),quantile(x,y)), quantile(x,y)),
      x=eltd, y=SL_MIN, h=HOLDING_COST, b=BACKLOG_COST)
    # Ensure SL minima when no backordering cost
    slev <- mapply(function(x,y,b,s)
      ifelse(b,x(y),s), x=lapply(eltd, ecdf), y=rop,
      b=BACKLOG_COST, s=SL_MIN)
    pot[!(HAS_CHILDREN),c("IO_ROP","IO_SL") := list(rop,slev)]
  })
}
#
# Function defining MEIO inequality constraints, i.e. returning <=0 for all components.
# In particular, we want proposed ROP >= (ROP corresponding to SL minima)
#
ineqConstr <- function(r, pot, nB, lwb) {
  # Proposed ROPs cannot be smaller than algorithm lower bounds or to those corresponding to SL mimima
  rop <- pmax(lwb, pot[(HAS_CHILDREN & STAGED), mapply(function(x,y) quantile(x,y), eltd, SL_MIN)])
  # ROP constraints: ROP >= ROP(SL_min), i.e., ROP(SL_min)-ROP <= 0
  return(rop-r)
}
#
# Find optimum reorder points ROP and corresponding service levels SL
# at all staged nodes of a multi echelon network.
# Inputs are the IPOT data table and the list of optimization options.
# The NLopt free/open-source library for nonlinear optimization is used
# to optimize at all staged nodes with children. SEIO optimization
# is used for staged nodes without children.
# The total cost function G(R,D) is minimized subject to minimum agreed SL.
# Holding and backordering costs are per unit. Ordering cost is not included,
# as it is a fixed cost per order. Output is an empty list on successful completion,
# otherwise a list with three elements: algorithm, error code and error message.
# ROP and SL sets are added to IPOT data table by reference
#
ropMEIO <- function(pot, opts)
{
  result <- list() # success exit value
  
  # Order network by node ID for effective LT and LTD calculation
  setkey(pot,MAT_ID,LOC_ID)
  
  # Simulate lead time demand LTD at level 0 and set prob.StockOut = 1 at unstaged nodes
  ropMeioIni(pot, opts$nB)
    
  # ROPs are bounded below by SL minima (may be zero) and above by SL maxima (1.0);
  # in MEIO, the fixed (LT) part of the effective lead time eLT in the formula
  # eLT(i) = LT(i) + prob.StockOut(i-1) * LT(i-1) means that the distribution of eLTD
  # is in general a right-shifted version of LTD, except when prob.StockOut = 0.
  # Hence, ROP bounds based on SL calculated from LTD will be biased downwards
  # and cannot be used as global bounds.
  #
  # # *** NLopt reference: http://ab-initio.mit.edu/wiki/index.php/NLopt_Algorithms ***
  #
  # All of the NLopt GLOBAL-optimization algorithms need bound constraints on all
  # optimization parameters, hence setting ROP upper bounds to +Inf is not an option
  #
  # The approach taken here is to derive lower and upper ROP bounds from minimum and
  # maximum expected shifts of eLTD with respect to LTD. Minimum shifs occur with
  # SL maxima, maximum shifts with SL minima ( prob.StockOut = 1-SL )
  
  ropEstimateMin(pot, opts$nB) # ROPs corresponding to SL maxima
  ropMin <- pot[(HAS_CHILDREN & STAGED), IO_ROP]   # ROP lower bounds
  
  ropEstimateMax(pot, opts$nB) # ROPs corresponding to SL minima
  ropMax <- pot[(HAS_CHILDREN & STAGED), IO_ROP]   # ROP upper bounds
  
  # Run a global derivative-free optimization algorithm (global to avoid local minima,
  # derivative-free because gradient information is not available). The following
  # support inequality constraints: ORIG_DIRECT, ORIG_DIRECT_L, ISRES
  # Use middle ROP values between lower and upper boundaries as initial estimates
  optGrd <- nloptr(x0 = (ropMin+ropMax)/2, eval_f = glbCostFn, lb = ropMin, ub = ropMax,
                   eval_g_ineq = ineqConstr, opts = opts$glb, # global optimizer options
                   pot = pot, nB = opts$nB, lwb = ropMin)
  
  optGrd$method     <- sub('NLOPT_','',opts$glb$algorithm)
  optGrd$message    <- strsplit(optGrd$message,":")[[1]][1]
  if (optGrd$status < 0) {
    result$algm <- optGrd$method; result$errCode <- optGrd$status; result$errMsg <- optGrd$message; return(result) }
  
  # After running the global optimizer, it is often worthwhile to use the global optimum
  # as a starting point for a local optimization to "polish" the optimum to greater accuracy,
  # as many of the global optimization algorithms devote more effort to searching the global
  # parameter space than in finding the precise position of the local optimum
  
  if (!is.null(opts$loc$algorithm)) {
    #
    # *** http://ab-initio.mit.edu/wiki/index.php/NLopt_Reference#Return_values ***
    #
    # Run COBYLA, a local derivative-free algorithm which supports inequality constraints.
    # Use ROP values from global optimization as starting values for local optimization.
    # Use relative opts$bnd values to set lower and upper ROP bounds
    ropMin <- pmax(ropMin,optGrd$solution-opts$lbd*abs(optGrd$solution))
    ropMax <- pmin(ropMax,optGrd$solution+opts$lbd*abs(optGrd$solution))
    locGrd <- nloptr(x0 = optGrd$solution, eval_f = glbCostFn, lb = ropMin, ub = ropMax,
                     eval_g_ineq = ineqConstr, opts = opts$loc, # local optimizer options
                     pot = pot, nB = opts$nB, lwb = ropMin)
    
    locGrd$method     <- sub('NLOPT_','',opts$loc$algorithm)
    locGrd$message <- strsplit(locGrd$message,":")[[1]][1]
    if (locGrd$status < 0) {
      result$algm <- locGrd$method; result$errCode <- locGrd$status; result$errMsg <- locGrd$message; return(result) }
    
    # Update global with local solution
    optGrd$solution   <- locGrd$solution
    optGrd$objective  <- paste0(round(optGrd$objective,2),"|",round(locGrd$objective,2))
    optGrd$method     <- paste0(optGrd$method,"+",locGrd$method)
    optGrd$iterations <- paste0(optGrd$iterations,"+",locGrd$iterations)
    optGrd$message    <- paste0(optGrd$message," | ",locGrd$message)
  }
  # Calculate service levels corresponding to optimal ROPs
  pot[(HAS_CHILDREN), IO_SL := mapply(function(x,y) x(y), lapply(eltd, ecdf), IO_ROP)]
  
  # Ensure that service levels are not below agreed minima (some of the
  # inequality constraints may not be enforced at all iterations)
  ### HERE !!!
#  sl <- opd$sl[lc]
#  sll <- slev<sl
#  if (any(sll)) {
#    q <- paste("Promoting SL =",toString(slev[sll]),"to SL_TRG = ",toString(sl[sll]))
#    cat.IO(q)
#    slev[sll] <- sl[sll]
#    # Recalculate affected ROPs
#    r[sll] <- mapply(function(x,y) quantile(x,y), eltd[which(sll)], slev[sll])
#  }
  
  
  pot[, c("optMeth","nIter","fVal","optMsg") := list(optGrd$method,optGrd$iterations,optGrd$objective,optGrd$message)]
  return(result)
}
#
# Simulate effective lead time demand at all locations and find best set of reorder points
# which minimize cost function to give optimal service levels.
# If inventory optimization is not executed, an error message is sent to the logfile.
#
inventoryOptim <- function(pot, opts)
{
  cat.IO(" *")
  
  # Work with data.table for efficiency
  setDT(pot)
  # Perform optimization. NOTE !!! IO_ROP is optimal safety stock when IO is based
  # on forecast error rather than on demand
  if (ioType == ioType1) {
    # List of items holding simulated LT and LTD vectors
    ltd <- with(pot, mapply(simulateSignals, sgDist, ltDist, nB, STAGED, SIMPLIFY = FALSE))
    pot[,eltd := lapply(ltd, function(x) x$jdSim)]
    ropSEIO(pot)
    return(pot[,.(MAT_ID,LOC_ID,IO_ROP,IO_SL)])
  } else {
    if (is.null(optRes <- tryCatch.MEIO(ropMEIO(pot, opts),unique(pot$NETWORK_ID)))) return(NULL)
    if (length(optRes)) {
      q <- paste0("NETWORK_ID = ",unique(pot$NETWORK_ID),": IO Algorithm = ",optRes$algm,", error code = ",optRes$errCode,": ",optRes$errMsg)
      cat.IO(q); sqlSave.LOG(q); return(NULL)
    } else return(pot[,.(MAT_ID,LOC_ID,IO_ROP,IO_SL,optMeth,nIter,fVal,optMsg)])
  }
}
#
# MEIO exception handling
#
tryCatch.MEIO <- function(expr,ntw) {
  tryCatch(expr,ntw,error = function(e) {
    q <- paste0("NETWORK_ID = ",ntw,":"); cat.IO(q); cat.IO(paste0(e,"\v"))
    return(NULL)
  })
}
#