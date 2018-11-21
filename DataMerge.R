## data merge and imputation for each region and each day;
## this code does a super loop to do data imputation for each region and each day, and save a temporary to indicate data imputation is currently undergoing for a particular day. So, other threads will not interfere with it. That means, this code is parallelizable

args = commandArgs(trailingOnly=TRUE)
StartID <- as.numeric(args[1])
YEAR <- as.numeric(args[2])
IP <- (args[3])
VariableID <- as.numeric(args[4])

# StartID <- 6
# YEAR = 2005
# IP = "10.31.130.243"
# VariableID = 99940

cat(sprintf("StartID:%d,YEAR:%d,IP:%s,VariableID:%d\n",StartID,YEAR,IP,VariableID))

#######################################
#### DO NOT CHANGE BELOW ##############
## macro-definition
source("ModelFunctions.R")
NAME = "PM25"
SiteName_Train = "AQRVPM25"
DirPath = "/n/regal/jschwrtz_lab/qdi/USTemperature/"
Sep = "/"
OPTION = "prediction"
GCS = "North_America_Equidistant_Conic"
SiteData_Train<-ReadLocation(paste0(DirPath,"processed_data/",SiteName_Train,"/Location/",SiteName_Train,"Site_",GCS))

##read csv configuration file
col = c(rep("character",6))
col[c(2,3)] = "logical"
col[c(7,8)] = "numeric"
VariableList = read.csv(paste0(DirPath,"assembled_data",Sep,"VariableList_",99930,".csv"),colClasses = col)
VariableList = VariableList[!is.na(VariableList$READ),]
UniqueID <- sample(1:1000000, 1, replace=F)


## connect to a specific h2o instance or the h2o instance with smallest system load
if(""==IP)
{
  Sys.sleep(StartID*10+runif(1)*100)
  H2oJob_List <- read.csv("/n/home02/xqiu/h2o_instances.txt",colClasses = c("numeric","character","character","character","numeric","numeric","character","numeric","character"),sep = ",")
  Index <- which(H2oJob_List$Usage == "Merge" & H2oJob_List$Running =="T" & H2oJob_List$Connections<2 )
  h2o.connect(ip = H2oJob_List[Index[1],"IP"], port = 54321)
  if(is.na(H2oJob_List[Index[1],"Connections"]))
  {
    H2oJob_List[Index[1],"Connections"]<-0
  }
  H2oJob_List[Index[1],"Connections"]<-H2oJob_List[Index[1],"Connections"]+1
  H2oJob_List[Index[1],"RJobID"] <- paste(H2oJob_List[Index[1],"RJobID"],paste0(Sys.getenv("SLURM_ARRAY_JOB_ID"),"_",Sys.getenv("SLURM_ARRAY_TASK_ID")),collapse = " ")
  write.csv(H2oJob_List,file = "/n/home02/xqiu/h2o_instances.txt",quote=FALSE,row.names = FALSE)
}else
{
  h2o.connect(ip = IP, port = 54321)
}

## doing data imputation for every region
for(ID in c(seq(StartID,50),seq(1,StartID)))
{
  SiteName_Predict= paste0("USGrid",ID)
  cat(sprintf("STARTED!!!ID:%d,YEAR:%d,IP:%s,VariableID:%d\n",ID,YEAR,IP,VariableID))
  
  SiteData<-ReadLocation(paste0(DirPath,"processed_data/",SiteName_Predict,"/Location/",SiteName_Predict,"Site_",GCS))
  DirPath_PredInput = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Input",Sep)
  if(!dir.exists(DirPath_PredInput))
  {
    dir.create(DirPath_PredInput)
  }
  fileConn <- file(paste0("DataMerge_",YEAR,"_",ID,".txt"),open = "a")
  while(TRUE)
  {
    ####################
    ## this section is just double check whether all files have been processed and properly saved
    if(file.exists(paste0(DirPath_PredInput,"InputComplete_",NAME,"_",SiteName_Predict,"_",YEAR,"_",YEAR,".rds")))
    {
      break
    }
      
    ## if all files exist and can be properly read into memory, then we put a marker file indicating that this year has been properly processed.
    i = 1:as.numeric(as.Date(paste0(YEAR,"-12-31")) - as.Date(paste0(YEAR,"-01-01"))+1)
    InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"),"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"),".rds")
    if(as.numeric(as.Date(paste0(YEAR,"-12-31")) - as.Date(paste0(YEAR,"-01-01"))+1) == sum(file.exists(InputFileStep1)))
    {
      IsFlag = 0
      ## double check whether input files are not corrupted
      for(i in 1:as.numeric(as.Date(paste0(YEAR,"-12-31")) - as.Date(paste0(YEAR,"-01-01"))+1))
      {
        InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"),"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"))
        Result <- tryCatch({
        InputData <- readRDS(paste0(InputFileStep1,".rds"))
        },error = function(err) {
          return(err)
        })
        
        if(inherits(Result,"error"))
        {
          cat(sprintf("Error!!! Double checking failed!...%s\n",InputFileStep1))
          file.remove(paste0(InputFileStep1,".rds"))
        }else
        {
          if(nrow(InputData) == nrow(SiteData))
          {
            cat(sprintf("Double checking successful!...%s\n",InputFileStep1))
            IsFlag = IsFlag + 1
          }else
          {
            cat(sprintf("Error!!! Double checking failed!...%s\n",InputFileStep1))
            file.remove(paste0(InputFileStep1,".rds"))
          }
        }
      }
      
      if(as.numeric(as.Date(paste0(YEAR,"-12-31")) - as.Date(paste0(YEAR,"-01-01"))+1) == IsFlag)
      {
        ## save a marker file indicating that this year has all been properly processed
        saveRDS(Sep,paste0(DirPath_PredInput,"InputComplete_",NAME,"_",SiteName_Predict,"_",YEAR,"_",YEAR,".rds"))  
        break
      }
      
    }
    
    #####################
    ## doing data imputation for every day
    for(i in 1:as.numeric(as.Date(paste0(YEAR,"-12-31")) - as.Date(paste0(YEAR,"-01-01"))+1))
    {
      InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"),"_",format(as.Date(paste0(YEAR,"-01-01"))+i-1,"%Y%m%d"))
      
      ## if a file exist and exceeds 10 KB, skip
      IsProcessFlag = FALSE
      if(file.exists(paste0(InputFileStep1,".rds")))
      { 
        IsProcessFlag = FALSE
      }else
      {  ## has been process before...
        if(file.exists(paste0(InputFileStep1,".part")))
        { ## has been done in longer than 5 minutes ago
          if(difftime(Sys.time(),file.info(paste0(InputFileStep1,".part"))$mtime,units="hours")>0.1)
          {# done long ago... needs to be done again
            IsProcessFlag = TRUE
          }else
          {
            IsProcessFlag = FALSE
          }
        }else
        {
          IsProcessFlag = TRUE
        }
      }
      
      if(IsProcessFlag)
      {
        ## actually read the data and do imputation
        x<-tryCatch({
            
            saveRDS(Sep,paste0(InputFileStep1,".part"))
            ## merge data
            if(file.exists(paste0(InputFileStep1,"_nonimputed.rds")))
            {
              cat(sprintf("reading non-imputed...%s\n",InputFileStep1))
              InputData<-readRDS(paste0(InputFileStep1,"_nonimputed.rds"))
            }else
            {
              InputData <- ReadData(DirPath,Sep,99930,NAME,SiteName_Predict,as.Date(paste0(YEAR,"-01-01"))+i-1,as.Date(paste0(YEAR,"-01-01"))+i-1,"prediction",SiteData,VariableList)
              
              ## if with valid output, save it, otherwise, skip
              if(is.null(InputData))
              {
                cat(sprintf("error!! skipping non-imputed...%s\n",InputFileStep1))
                writeLines(paste("errors\t",ID,"\t",YEAR,"\t",format(Sys.time(), "%a %b %d %X %Y"),"\t",InputFileStep1,"\n\n"), fileConn)
                next
                
              }else
              {
                cat(sprintf("saving non-imputed...%s\n",InputFileStep1))
                # saveRDS(InputData,paste0(InputFileStep1,"_nonimputed.rds"))
              }
            }
            
            InputData <- StandardData(DirPath,InputData,Sep,paste0(YEAR,"_",VariableID))
            print(system.time({
              
            InputData_h2o = as.h2o(InputData,destination_frame = paste0("InputData_",UniqueID))
            InputData_h2o = ImputeData(DirPath,Sep,InputData_h2o,InputData,OPTION,paste0(YEAR,"_",VariableID),SiteData_Train)
            }))
            InputData <- as.data.frame(InputData_h2o)
            cat(sprintf("saving...%s\n",InputFileStep1))
            file.remove(paste0(InputFileStep1,".part"))
            saveRDS(InputData,paste0(InputFileStep1,".rds"))
            
          }, error = function(e) {
            writeLines(paste("errors\t",ID,"\t",YEAR,"\t",format(Sys.time(), "%a %b %d %X %Y"),"\t",InputFileStep1,"\t",toString(e),"\n\n"), fileConn)
            cat(sprintf("skipping...%s\n",InputFileStep1))
            
            
          }
        )
        
        if(inherits(x,"error"))
        {next}
      }
      
      # use another h2o instance
      if(!h2o.clusterIsUp())
      {
        if(""==IP)
        {
          H2oJob_List <- read.csv("/n/home02/xqiu/h2o_instances.txt",colClasses = c("numeric","character","character","character","numeric","numeric","character","numeric","character"),sep = ",")
          Index <- which(H2oJob_List$Usage == "Merge" & H2oJob_List$Running =="T" & H2oJob_List$Connections<2 )
          h2o.connect(ip = H2oJob_List[Index[1],"IP"], port = 54321)
          if(is.na(H2oJob_List[Index[1],"Connections"]))
          {
            H2oJob_List[Index[1],"Connections"]<-0
          }
          H2oJob_List[Index[1],"Connections"]<-H2oJob_List[Index[1],"Connections"]+1
          H2oJob_List[Index[1],"RJobID"] <- paste(H2oJob_List[Index[1],"RJobID"],paste0(Sys.getenv("SLURM_ARRAY_JOB_ID"),"_",Sys.getenv("SLURM_ARRAY_TASK_ID")),collapse = " ")
          write.csv(H2oJob_List,file = "/n/home02/xqiu/h2o_instances.txt",quote=FALSE,row.names = FALSE)
        }else
        {
          h2o.connect(ip = IP, port = 54321)
        }
      }
    
  
    }
    
  }
  cat(sprintf("DONE!!!ID:%d,YEAR:%d,IP:%s,VariableID:%d\n",ID,YEAR,IP,VariableID))
  close(fileConn)
}