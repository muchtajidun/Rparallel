
library(cluster)
library(data.table)
source(paste0(getwd(),"/IPOT_BASE/mySCRIPTS/IO_parallel_funcs.R"))

# Function to generate all the different types of demand we can escounter. 
# Demands will be generated also with normal.
# 
set.seed(123,"L'Ecuyer") 
diritchlet<-function(alpha,n){
  
  sample <- rgamma(n, shape=alpha, rate = 1)
  diritchletSample <- sapply(sample,function(x) x/sum(sample))
  diritchletSample
  
}


generate<-function(rep){ 
 len<- 20 # Length of the sample ,
 alphaSet1<-seq(0.01,0.1,by=0.01) # Level 1 of alpha values
 alphaSet2<-seq(0.1,1,by=0.1) # level 2 of alpha values
 alphaSet<-c(alphaSet1,alphaSet2)
 cutOffDown<-0.0005 # Creating 0s by puting a lower limit
 cutOffUp<-0.95 # cleaning  unlikely values created by the sampling distribution.
 
 sprintf("Numer of Samples Created :  %d ",rep*(length(alphaSet1)+length(alphaSet2)))
 print(rep*(length(alphaSet1)+length(alphaSet2)))
 old.mar= par("mar")
 par(mar=c(1,1,1,1)) 
 old.par <- par(mfrow=c(4, 5))
 
 #Number of samples 
 for ( j in 1:rep){
   #Number of alpha used per sample
   for (alpha in alphaSet){
        
        scaler<-len*100*alpha#Making the absolute values of the demand somehow more realistic.
        elem <-diritchlet(alpha,len) # Drawing a sample from a dirithlet ( alpha )
        elem[elem<cutOffDown]<-0 # Crating 0 demands points
        elem[elem>cutOffUp]<-0  # getting rid of unlikely high values
        
        a<-paste0("Alpha",alpha,"_",j,"R")

        if(j==1&alpha==alphaSet[1]){
          print(a)
          expr <- parse(text = paste0(a," = as.vector(scaler*elem)"))
          print(expr)
          samples<<-data.table(eval(expr))
          print(colnames(samples))
          
         }
        else{
          
          expr <- parse(text = paste0(a,":=as.vector(scaler*elem)"))
          samples[,eval(expr)]
        }
        
        if(j==1){
            plot(1:len,scaler*elem)#ploting the last of the set of samples ( 1 for ech alpha )
         }
        
     }
 }
 par(old.par) 
}

generate(30)#generating all samples.
 
#CREATING THE COVARIATES. 
  
 # 1. Sparsity as countimg the number of 0s 
  numOfCeros<-function(demand){length(demand[demand==0])/length(demand)} 
 #2.  Precision , var2 
  precision<-function(demand){
    if (numOfCeros(demand)==1) 1
    else mean(demand)/sd(demand)
  }
 #to make them invarian to scale
 
  aproxToKurtosis<-function(demand){
                            if (numOfCeros(demand)==1) 1
                            else{
                            clean<-demand[demand!=0]
                            a<-quantile(clean, c(.25, .75))
                            (a[2]-a[1])/(mean(clean)+0.000001)
                            }
  }
   
  aproxLowQuantile<-function(demand){
    if (numOfCeros(demand)==1) 1
    else {
          clean<-demand[demand!=0]
          a<-quantile(clean, c(.0, .25))
          (a[2]-a[1])/(a[2]+0.000001)
    }
  }
   
  aproxHighQuantile<-function(demand){
    if (numOfCeros(demand)==1) 1
    else{
     clean<-demand[demand!=0]
     a<-quantile(clean, c(.75, 1.0))
     (a[2]-a[1])/(a[2]+0.000001)
      }
  }

 
 #aproxToKurtosis(c)
 #aproxLowQuantile(c)
 #aproxHighQuantile(c)
 

#adding to the data.table
ceros<-c()
pre <- c()
kurt <- c() 
lowQ <- c()
highQ <- c() 

for ( serie in 1:length(samples)){
  
    elem <- as.matrix(samples[,serie, with=FALSE])
    ceros[serie] <- numOfCeros(elem)
    pre[serie] <- precision(elem)
    kurt[serie] <- aproxToKurtosis(elem)
    lowQ[serie] <- aproxLowQuantile(elem)
    highQ[serie] <- aproxHighQuantile(elem)
    
   if(serie==length(samples)){
     
     old.mar= par("mar")
     par(mar=c(1,1,1,1)) 
     old.par <- par(mfrow=c(3, 3))
     
     hist(ceros,breaks=100)
     hist(pre,breaks=100)
     hist(pre*ceros,breaks=100)
     hist(kurt,breaks=100)
     hist(kurt*ceros,breaks=100)
     hist(lowQ,breaks=100)
     hist(highQ,breaks=100)

     
     #Cleaninf for posible nans/inf.
     
     xDataTable<<-data.table(ceros=ceros,
                            precision=pre,
                            precision_ceros = pre*ceros,
                            kurtosis=kurt,
                            kurtosis_ceros = kurt*ceros, 
                            lowQ=lowQ,
                            highQ=highQ,
                            seriesNames=colnames(samples)
                            )
     
     #xDataTable[complete.cases(xDataTable),]
     
       }
   
  }

#for ( l in 1:10){ 
#Partitioning (clustering) of the data into k clusters “around medoids”, a more robust version of K-means.
#Medoids are representative objects of a data set or a cluster with a data set whose average 
#dissimilarity to all the objects in the cluster is minimal
 
data <- as.matrix(xDataTable[,1:(length(colnames(xDataTable))-1),with=FALSE])

#clusteredData<-pam(data,
#                   stand = TRUE,
#                   k = 7)

 
#Selcting the optimal number of clusters.
#The choice will be done using the Gap statistics :
  #Tibshirani, R., Walther, G. and Hastie, T. (2001). 
  #Estimating the number of data clusters via the Gap statistic. 
  #Journal of the Royal Statistical Society B, 63, 411–423. Tibshirani, R., Walther, G. and Hastie, T. (2000).

for (i in 1:2) {
   #
   # Estimate optimal cluster count and perform K-medoids with it.
   #
   # This process is too costly computationaly speking , so a paralellize version needs to be implemented.
   #gap <- clusGap(data,pam, K.max=10, B=500)
   gap <- paralleGap(data,pam, K.max=10, B=500)
   print("Gap value matrix")
   print(gap$Tab) 
   k <- maxSE(gap$Tab[, "gap"], gap$Tab[, "SE.sim"], method="Tibs2001SEmax")
   print(sprintf("Optimal Numer of of clusters :  %d , on iteration %d",k,i))
   clusteredData <- pam(data,
               stand = TRUE,
               k = k)
   
   #
   #finding and printing the medoids.
   #

   for (medoids in 1:length(clusteredData["medoids"][[1]][,1])){
     
     elem <- as.vector(clusteredData["medoids"][[1]][medoids,])
     row<-xDataTable[ceros==elem[1]&
                       precision==elem[2]&
                       precision_ceros ==elem[3]&
                       kurtosis==elem[4]&
                       kurtosis_ceros ==elem[5]&
                       lowQ==elem[6]&
                       highQ==elem[7]
                     ] 
     
     serieNameMedoidsOuput <-parse(text =row[,seriesNames])[1]
     print(serieNameMedoidsOuput)
     valueSerieNameMedoidsOuput<-samples[,eval(serieNameMedoidsOuput)]
     
     if(medoids==1){
       newNameMedoidsSerie <-parse(text =paste0(row[,seriesNames],"=valueSerieNameMedoidsOuput"))
       prototipes<<-data.table(eval(newNameMedoidsSerie))
     }
     else{
       newNameMedoidsSerie <-parse(text =paste0(row[,seriesNames]," :=valueSerieNameMedoidsOuput"))
       prototipes<<-prototipes[,eval(newNameMedoidsSerie)]
     }
   }
   print(prototipes)
   print(clusteredData["medoids"])
} 
 

#Finding the optimla number of clusters

# 1 . Calculate the Demand a Lead Time distribution for this N prototipes using a very large amount of simulations "S"
# 2 .  Define a metric KL , seems to be the most apropiate : D_{\mathrm{KL}}(P\|Q) = \sum_i P(i) \, \ln\frac{P(i)}{Q(i)}.
#      Where Q is the distribution simulated previously.
# 3 From i=1 to  TotalMedoidsSeries:
#    3.1 From t=1 to NumberOfSimulationsToDoPerSerie:
#        3.1.1 Reduce by S(t)=S(t-1)-s.
#        3.1.2 Simulate S(t) monte carlo elements.
#        3.1.3 Calculate KL(P(t)/P(Optimal)) , P(Optimal) is the distribution with the maximum number of simulations.
#     3. 2 Stop if a criteria is met, criteria still to be defined ( i might not define it at all , run to the ned of loop)
#         print the series and decide.
# 4 Stop when i=TotalMedoidsSeries
 
# Once the minimum number of simulations needed per cluter to maintain consistency, in order assing new demand series to the 
# cluster the only thing left to do is :
# 1 . per new demand serie , caculate the explanatory variables values : 
#         numOfCeros
#         precision
#         aproxToKurtosis
#         aproxLowQuantile
#         aproxHighQuantile
# 2. Clcuate the distance to all the N clusters and chose the closet one.
# 3. Use the minimum number of simulation needed for the Cluster to simulate The Demand  at lead time.
 
# Some Work needs to be done is the alha parameter of the diritchlet distribution as well as in the number of randon starts for 
# the clustering algorithm.Keep in mind this is a much more expensive algorithm  than the kmeans.
# Also more explanatory vaiables might be added to help with the cluster separation.

# Building the optimal target distribution :
# We need to feed this Demand distribution to the Simulation function.
# The other quantity that we need is the LT Gamma distribution.
 
prototipes   



 
 
 