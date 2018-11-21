## start a h2o instance on Odyssey
library(h2odyssey)
node_list <- detect_nodes()
ips <- get_ips(node_list)
h2o.init(nthreads = -1,min_mem_size = "350g")

##write IP address into a text file
fileConn<-file("IP.txt","a")
writeLines(ips, fileConn)
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
  
  if(difftime(Sys.time(),Now2,units = "hours")>16 | !h2o.clusterIsUp())
  {
    # restart
    h2o.removeAll()
    h2o:::.h2o.garbageCollect()
    # h2o.shutdown()
    # rm(list=ls())
    # system("R --no-save")
    Now1 <- Sys.time()
    
    # h2o.init(nthreads = -1,min_mem_size = "350g")
    Now2 <- Sys.time()
    cat(sprintf("%s... restarting h2o\n",format(Sys.time(), "%a %b %d %X %Y")))
  }
  
}
