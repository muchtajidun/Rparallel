paralleGap<-function (x, FUNcluster, K.max, B = 100, verbose = interactive(),...) {
  
  print("Function Starts......")
  
  stopifnot(is.function(FUNcluster), length(dim(x)) == 2, K.max >= 2, (n <- nrow(x)) >= 1, (p <- ncol(x)) >= 1)
  
  if (B != (B. <- as.integer(B)) || (B <- B.) <= 0) 
    stop("'B' has to be a positive integer")
  
  if (is.data.frame(x)) 
    x <- as.matrix(x)
  
  ii <- seq_len(n)
  
  W.k <- function(X, kk) {
    clus <- if (kk > 1) 
            FUNcluster(X, kk, ...)$cluster
            else rep.int(1L, nrow(X))
    0.5 * sum(vapply(split(ii, clus), function(I) {
      xs <- X[I, , drop = FALSE]
      sum(dist(xs)/nrow(xs))
    }, 0))
  }
   
  
  logW <- E.logW <- SE.sim <- numeric(K.max)
  if (verbose) 
    cat("Clustering k = 1,2,..., K.max (= ", K.max, "): .. ", 
        sep = "")
  
  
  for (k in 1:K.max) logW[k] <- log(W.k(x, k))
  if (verbose) 
    cat("done\n")
  
  xs <- scale(x, center = TRUE, scale = FALSE)
  m.x <- rep(attr(xs, "scaled:center"), each = n)
  V.sx <- svd(xs)$v
  rng.x1 <- apply(xs %*% V.sx, 2, range)
  logWks <- matrix(0, B, K.max)
  if (verbose) 
    cat("Bootstrapping, b = 1,2,..., B (= ", B, ")  [one \".\" per sample]:\n", 
        sep = "")
  
  print("First Loop.....")
  
  #this is the part that needs to be parallelize.......
  #To send to the closure wee ned to send B , nn , M , V , m , logWks , W.k
  
  paralellize<-function(B , nn , M , V , m , logWks , W.k){
  
          for (b in 1:B) {
            print(b)
            z1 <- apply(rng.x1, 2, function(M, nn) runif(nn, min = M[1], 
                                                         max = M[2]), nn = n)
            
            print("z1 is been calculated")
        
            z <- tcrossprod(z1, V.sx) + m.x
            for (k in 1:K.max) {
              logWks[b, k] <- log(W.k(z, k))
            }
            if (verbose) 
              cat(".", if (b%%50 == 0) 
                paste(b, "\n"))
            }
          
  }
  
  paralellize(B , nn , M , V , m , logWks , W.k)
  
  if (verbose && (B%%50 != 0)) 
    cat("", B, "\n")
  E.logW <- colMeans(paralellize)
  SE.sim <- sqrt((1 + 1/B) * apply(paralellize, 2, var))
  structure(class = "clusGap", list(Tab = cbind(logW, E.logW, 
                                                gap = E.logW - logW, SE.sim), n = n, B = B, FUNcluster = FUNcluster))
  print("end of process")
}