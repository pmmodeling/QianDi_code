## monitor a h2o instance on Odyssey
# read from a file called "IP.txt", two columns: IP,name,connection,sys_load,free_mem,Running
# run on 10GB memory, preferred
## also ge to know how many jobs connected to each h2o instances
library(R.utils)
library(h2o)

while(TRUE)
{
  # get list of connected R jobs
  # A <- readRDS("C:\\Users\\qid335\\Downloads\\aaa.rds")
  A <- system("squeue -u xqiu",intern=TRUE)
  RJob_List <- data.frame( do.call( rbind, strsplit( A[2:length(A)], '\\s+') ) ) 
  colnames(RJob_List)<-strsplit( A[1], '\\s+')[[1]]
  RJob_List <- RJob_List[which(RJob_List$NAME == "Predicti" | RJob_List$NAME == "DataMerg"),]
  
  # get list of  h2o instances
  H2oJob_List <- read.csv("~/h2o_instances.txt",colClasses = c("numeric","character","character","character","numeric","numeric","character","numeric","character"),sep = ",")
  
  # check status of connected R jobs, whether they are running
  cat(sprintf("%s...number of R jobs running:%d\n",format(Sys.time(), "%a %b %d %X %Y"),nrow(RJob_List)))
  for(i in 1:nrow(H2oJob_List))
  {
    if(!is.na(H2oJob_List[i, "RJobID"]))
    {
      if(nchar(H2oJob_List[i,"RJobID"])>0)
      {
        RJob_Temp <- strsplit(H2oJob_List[i,"RJobID"],"\\s+")[[1]]
        for(j in 1:length(RJob_Temp))
        {
          # this job is not running any more
          if(nchar(RJob_Temp[j])>1)
          {
            if(!is.element(RJob_Temp[j],RJob_List$JOBID))
            {
              RJob_Temp[j]<-""
              H2oJob_List[i,"Connections"] <- H2oJob_List[i,"Connections"] - 1
            }
          }
        }
        H2oJob_List[i,"RJobID"]<-paste(RJob_Temp,collapse = " ")
      }
    }
  }
  write.csv(H2oJob_List,file = "~/h2o_instances.txt",quote=FALSE,row.names = FALSE)
  
  # A <- system("squeue -u xqiu",intern=TRUE)
  # RJob_List <- data.frame( do.call( rbind, strsplit( A[2:length(A)], '\\s+') ) ) 
  # colnames(RJob_List)<-strsplit( A[1], '\\s+')[[1]]
  # RJob_List <- RJob_List[which(RJob_List$NAME == "20G20C"),]
  # check status of h2o instances
  for(i in 1:nrow(H2oJob_List))
  {
    ## get IP
    Result <- tryCatch({
      
      res <- NULL;
      tryCatch({
        res <- evalWithTimeout({
          Temp <- h2o.connect(ip = H2oJob_List[i,"IP"],port = 54321)
          if(!h2o.clusterIsUp())
          {
            H2oJob_List[i,"Running"] = "Failed"
          }else{
            TempStatus <- h2o.clusterStatus()
            H2oJob_List[i,"sys_load"]<-TempStatus$sys_load
            H2oJob_List[i,"free_mem"]<-as.numeric(TempStatus$free_mem)/1024^3
            H2oJob_List[i,"Running"] = "T"
          }   
        }, timeout=10);
      }, TimeoutException=function(ex) {
        cat("Timeout. Skipping.\n");
      })
      
      
    }, error = function(err) {
      return(err)
    })
    
    if(inherits(Result,"error"))
    {
      H2oJob_List[i,"Running"] = "Failed"
    }
    write.csv(H2oJob_List[H2oJob_List$Running == "T",],file = "~/h2o_instances.txt",quote=FALSE,row.names = FALSE)
  }
  
  cat(sprintf("%s... done with checking h2o\n",format(Sys.time(), "%a %b %d %X %Y")))
  Sys.sleep(600)
}


res <- evalWithTimeout({
  Temp <- h2o.connect(ip = "10.31.165.193",port = 54321)
}, timeout=5)
