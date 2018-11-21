## start a h2o instance on Odyssey
library(h2odyssey)

node_list <- detect_nodes()
ips <- get_ips(node_list)
h2o.init(nthreads = -1,min_mem_size = "15g")
JobID <- Sys.getenv("SLURM_JOB_ID")
print(JobID)


##write IP address into a text file
fileConn<-file("~/h2o_instances.txt","a")
if(runif(1)>0.5)
{
  writeLines(paste0(JobID,",",ips,",20core30G,Merge,1,20,T,0,"), fileConn)
}else
{
  writeLines(paste0(JobID,",",ips,",20core30G,Prediction,1,20,T,0,"), fileConn)
}
close(fileConn)

# let the h2o instance run forever!!!
Now1 = Sys.time()
Now2 = Sys.time()
while(TRUE)
{
  Sys.sleep(300)
  cat(sprintf("%s...\n",format(Sys.time(), "%a %b %d %X %Y")))
  print(h2o.clusterStatus())
  cat(sprintf("\n\n\n",format(Sys.time(), "%a %b %d %X %Y")))
  
  if(difftime(Sys.time(),Now1,units = "hours")>1)
  {
    h2o:::.h2o.garbageCollect()
    gc()
    Now1 <- Sys.time()
    cat(sprintf("%s...collecting garbage\n",format(Sys.time(), "%a %b %d %X %Y")))
  }
  
  if(difftime(Sys.time(),Now2,units = "hours")>12 | !h2o.clusterIsUp())
  {
    # restart
    h2o.removeAll()
    h2o:::.h2o.garbageCollect()
    # h2o.shutdown()
    # rm(list=ls())
    # system("R --no-save")
    Now1 <- Sys.time()
    
    # h2o.init(nthreads = -1,min_mem_size = "15g")
    Now2 <- Sys.time()
    cat(sprintf("%s... restarting h2o\n",format(Sys.time(), "%a %b %d %X %Y")))
  }
  
}
