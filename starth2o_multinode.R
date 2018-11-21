## start a h2o instance on Odyssey
library(h2odyssey)
node_list <- detect_nodes()
ips <- get_ips(node_list)
# h2o.init(nthreads = -1,min_mem_size = "280g")
start_h2o_cluster(memory = 30) 

# let the h2o instance run for one year!!!
# Sys.sleep(3600*24*365)
Now1 = Sys.time()
Now2 = Sys.time()
while(TRUE)
{
  Sys.sleep(3600)
  if(difftime(Sys.time(),Now1,units = "hours")>1)
  {
    h2o:::.h2o.garbageCollect()
    Now1 <- Sys.time()
    cat(sprintf("%s...collecting garbage\n",format(Sys.time(), "%a %b %d %X %Y")))
  }
  
  if(difftime(Sys.time(),Now2,units = "hours")>24)
  {
    # restart
    h2o.removeAll()
    h2o:::.h2o.garbageCollect()
    h2o.shutdown()
    rm(list=ls())
    system("R --no-save")
    Now1 <- Sys.time()
    start_h2o_cluster(memory = 30) 
#    h2o.init(nthreads = -1,min_mem_size = "350g")
    Now2 <- Sys.time()
    cat(sprintf("%s... restarting h2o\n",format(Sys.time(), "%a %b %d %X %Y")))
  }
  
}