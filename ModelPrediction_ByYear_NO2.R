######### ABOT THE FILE #############################
## this is the main function to do model prediction;
## the work flow is: (1) first make prediction for each of the 50 regions for every day, (2) aggregate daily prediction together;
## the difficult part is: model prediction is done in a "two-step" process: we need to make prediction for gradient boosting, neural network, random forest separately, then use ensemble model (here is a gam model) to get a preliminary prediction (Step 1 prediction), then calculate the averaged prediction from nearby days and regions, and use them as additional predictor variables, do gradient boosting, neural network, random forest separately, then use ensemble model again, to get the final prediction (Step 2 prediction)
## this prediction relies on h2o instance. This code file read all input data and send them to a connected h2o instance to do gradient boosting, neural network, random forest calculation

# 2018-Oct-16
## this file is adapted from ModelPrediction_ByYear.R, and created only for NO2 modeling. Basically, it reads input data from PM2.5 model and add a few variables for prediction

######### PARAMETER #################################
## this prediction file takes four 
## StartID: the Region ID that we make predictions at (1 ~ 50)
## YEAR: at which year we will make predictions
## IP: the IP address of the h2o instance
## VariableID: the configuration file ID that specifies the input variables

######### HOW TO RUN ################################
## (1) be sure to put trained model under the folder: ./USTemperature/assembled_data/training/;put imputation model under ./n/regal/jschwrtz_lab/qdi/USTemperature/processed_data/TempData/
## (2) run DataMerge.R to complete data imputation for all 50 regions; imputed data file are stored at ./assembled_data/prediction/PM25_USGrid[Region ID]_Input
## (3) run starth2o_shared.R to start several h2o instances; and record their IP addresses
## (4) run this code to do model prediction; results will be placed at ./assembled_data/prediction/PM25_USGrid/

library(rhdf5)
library(h2o)
library(rgeos)
library(sp)
library(rgdal)
library(mgcv)
library(parallel)

## take parameters from SLURM script input
args = commandArgs(trailingOnly=TRUE)
StartID <- as.numeric(args[1])
YEAR <- as.numeric(args[2])
IP <- (args[3])
VariableID <- as.numeric(args[4])

## for testing purpose
#StartID <- 1
#YEAR = 2000
#IP = ""
#VariableID = 200

####################################################
### DO NOT CHANGE BELOW ###########################

## air pollutants we are modeling-- VariableID_List, NAME_List, and SiteName_Train_List have the same length
# NAME_List = c("PM25","Ozone","NO2")
NAME_List = c("PM25")

## in the same order, the configuration file that specifies the input variables
VariableID_List = c(VariableID)

## in the same order, the name of monitoring sites
# SiteName_Train_List = c("AQRVPM25","EPACastNetOzone","EPANO2")
SiteName_Train_List = c("EPANO2")

##### ABOUT THIS FUNCTION ################
## this function takes parameters and make prediction (Step 1 and Step 2) for a given region in a given year; this function is also flexible to make prediction for a time duration within one year;

##### PARAMETER ##########################
# DirPath: the root directory;
# Sep: slash, "\\" for windows; "/" for linux
# VariableID: the configuration file ID that specifies the input variables
# NAME: name of the air pollutants we are modeling
# StartDate: the start date of prediction
# EndDate: the end date of prediction
# Window_Time = time window width used to calculate two-step modeling, we will calculate averaged prediction from nearby 7 days;
# OPTION = always be "prediction" --- we will do prediction here
# SiteName_Train: the name of grid used for model training;
# SiteName_Predict: the name of grid used for model prediction;
# GCS: currently used coordinate system


##### EXAMPLES ########################
# DirPath = "D:\\Google Drive\\Research\\USTemperature\\"
# Sep = "\\"
# VariableID = 99940
# NAME = "PM25"
# StartDate = as.Date("2008-01-07")
# EndDate = as.Date("2008-02-07")
# Window_Time = 7 
# OPTION = "prediction"
# SiteName_Train = "AQRVPM25"
# SiteName_Predict = "AQRVPM25_test1"
# GCS = "North_America_Equidistant_Conic"
# h2o.init()
# ModelPrediction_Fun(DirPath,Sep,VariableID,NAME,StartDate,EndDate,Window_Time,GCS,OPTION,SiteName_Train,SiteName_Predict)

ModelPrediction_Fun<-function(DirPath,Sep,VariableID,NAME,StartDate,EndDate,Window_Time,GCS,OPTION,SiteName_Train,SiteName_Predict,VariableList)
{
  ## we will only make prediction for the same year, and does not support prediction for multiple years.
  if(as.numeric(format(StartDate,"%Y"))!=as.numeric(format(EndDate,"%Y")))
  {
    stop("ModelPrediction_Fun only makes prediction within one year")
  }
  
  ## create folders
  YEAR = as.numeric(format(StartDate,"%Y"))
  ## path
  DirPath_Assembled = paste0(DirPath,"assembled_data",Sep)
  DirPath_Processed = paste0(DirPath,"processed_data",Sep)
  DirPath_Model = paste0(DirPath,"assembled_data",Sep,"training",Sep,NAME,"_",YEAR,"_",VariableID,Sep)
  # put Step 2 prediction results
  DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep)
  # put Step 1 prediction results
  DirPath_PredStep1 = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Step1",Sep)
  # put merged inpout data
  DirPath_PredInput = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Input",Sep)
  if(!dir.exists(DirPath_Pred))
  {
    dir.create(DirPath_Pred)
  }
  if(!dir.exists(DirPath_PredStep1))
  {
    dir.create(DirPath_PredStep1)  
  }
  if(!dir.exists(DirPath_PredInput))
  {
    dir.create(DirPath_PredInput)  
  }
  
  ## read location files of grid cells used for model training and model prediction
  SiteData_Train<-ReadLocation(paste0(DirPath_Processed,SiteName_Train,Sep,"Location",Sep,SiteName_Train,"Site_",GCS))
  SiteData_Predict<-ReadLocation(paste0(DirPath_Processed,SiteName_Predict,Sep,"Location",Sep,SiteName_Predict,"Site_",GCS))
  N_Site_Train <- nrow(SiteData_Train)
  N_Site_Predict <- nrow(SiteData_Predict)

  ## read trained model for step 1 and step 2
  ## step 1 --- neural network
  TempDir = paste0(DirPath_Model,"NeuralNetwork_Step1",Sep)
  if(length(list.files(TempDir))>0)
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 1 neural network...%s\n",paste0(TempDir,Model[1])))
    mod_nn_1 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 1 --- random forest
  TempDir = paste0(DirPath_Model,"RandomForest_Step1",Sep)
  if(length(list.files(TempDir))>0)
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 1 random forest...%s\n",paste0(TempDir,Model[1])))
    mod_rf_1 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 1 --- gradient boosting
  TempDir = paste0(DirPath_Model,"GradientBoosting_Step1",Sep)
  if(length(list.files(TempDir))>0)
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 1 gradient boosting...%s\n",paste0(TempDir,Model[1])))
    mod_gbm_1 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 1 --- ensemble model
  if(file.exists(paste0(DirPath_Model,"Ensemble_Step1.rds")))
  {
    mod_ensemble_1 <- readRDS_robust(paste0(DirPath_Model,"Ensemble_Step1.rds"))
  }else
  {
    stop("cannot find model ",paste0(DirPath_Model,"Ensemble_Step1.rds"))
  }
  
  ## step 2 --- neural network
  TempDir = paste0(DirPath_Model,"NeuralNetwork_Step2",Sep)
  if(length(list.files(TempDir))>0)
  {
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 2 neural network...%s\n",paste0(TempDir,Model[1])))
    mod_nn_2 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 2 --- random forest
  TempDir = paste0(DirPath_Model,"RandomForest_Step2",Sep)
  if(length(list.files(TempDir))>0)
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 2 random forest...%s\n",paste0(TempDir,Model[1])))
    mod_rf_2 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 2 --- gradient boosting
  TempDir = paste0(DirPath_Model,"GradientBoosting_Step2",Sep)
  if(length(list.files(TempDir))>0)
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    Model <- list.files(TempDir)
    cat(sprintf("  reading Step 2 gradient boosting...%s\n",paste0(TempDir,Model[1])))
    mod_gbm_2 <- h2o.loadModel(paste0(TempDir,Model[1]))
  }else
  {
    stop("cannot find model ",TempDir)
  }
  
  ## step 2 --- ensemble model
  if(file.exists(paste0(DirPath_Model,"Ensemble_Step2.rds")))
  {
    if(length(list.files(TempDir))>1)
    {
      stop("more than one model here exist!",TempDir)
    }
    mod_ensemble_2 <- readRDS_robust(paste0(DirPath_Model,"Ensemble_Step2.rds"))
  }else
  {
    stop("cannot find model ",paste0(DirPath_Model,"Ensemble_Step2.rds"))
  }
  
  #################################
  
  ## queue used to do two-step modeling
  # the row name and list name is the corresponding date, for example, "13876"  is 2008-01-01
  Temp <- readRDS_robust(paste0(DirPath,Sep,"assembled_data",Sep,"training",Sep,NAME,"_",YEAR,"_",VariableID,Sep,"OutputData.rds"))
  N_Day = max(Temp$CalendarDay) - min(Temp$CalendarDay) + 1
  Pred_Spatial = as.matrix(Temp$pred_ensemble_1)
  dim(Pred_Spatial)<-c(N_Day,N_Site_Train)
  rownames(Pred_Spatial)<-seq(min(Temp$CalendarDay),max(Temp$CalendarDay),1)
  
  Pred_Temporal = matrix(data = NA,nrow = 0 ,ncol = N_Site_Predict)
  InputData_list = list()
  
  ## make prediction for the whole year --- do both step 1 and 2 predictions 
  for(i in as.numeric(StartDate):as.numeric(EndDate))
  {
    ## temporary step 2 prediction file name and input file name
    PredFileStep2 = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
    InputFileStep2 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
    
    ## check Step 2 prediction has been done or in the middle of processing
    IsProcessFlag = FALSE
    if(file.exists(PredFileStep2))
    { 
      IsProcessFlag = FALSE
    }else
    {  ## has been process before...
      if(file.exists(paste0(PredFileStep2,".part")))
      { ## has been done in longer than 5 minutes ago
        if(difftime(Sys.time(),file.info(paste0(PredFileStep2,".part"))$mtime,units="hours")>0.1)
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
    
    ## after checking, not all step 2 predictions are done, then we will do Step 2 prediction!!!
    if(IsProcessFlag)
    {# step 2 results does not exist, we will make Step 2 prediction
      cat(sprintf("making Step 2 prediction...%s\n",PredFileStep2))
      saveRDS(Sep,paste0(PredFileStep2,".part"))
      
      # update step 1 result in the nearby 7 days (Pred_Temporal), as well as inpudata_list
      for(j in (i-(Window_Time-1)/2): (i+(Window_Time-1)/2))
      {
        ## temporary step 1 prediction file name
        PredFileStep1 = paste0(DirPath_PredStep1,"PredictionStep1_",NAME,"_",SiteName_Predict,"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
        cat(sprintf("  For Step 1 prediction...%s\n",PredFileStep1))
        
        ## temporary step 1 inputfile name
        if(j<as.numeric(as.Date(paste0("2000-01-01"))))
        {
          InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_20000101_20000101.rds")
        }else
        {
          InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
        }
        
        # Step 1 results exist in memory (variable Pred_Temporal)
        if(as.character(j) %in% rownames(Pred_Temporal))
        {
          next
        }else
        { 
          #Step 1 results does NOT exist in memory (in variable Pred_Temporal)
          # whether Step 1 result exist on disk??
          IsProcessFlag = FALSE
          if(file.exists(PredFileStep1))
          { 
            IsProcessFlag = FALSE
          }else
          {  ## has been process before...
            if(file.exists(paste0(PredFileStep1,".part")))
            { ## has been done in longer than 5 minutes ago
              if(difftime(Sys.time(),file.info(paste0(PredFileStep1,".part"))$mtime,units="hours")>0.1)
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
          
          if(!IsProcessFlag)
          { # load Step 1 results into memory (variable Pred_Temporal)
            cat(sprintf("    reading Step 1 prediction from disk...%s\n",PredFileStep1))
            Temp = readRDS_robust(PredFileStep1)
            rownames(Temp)<-as.character(j)
            Pred_Temporal = rbind(Pred_Temporal,Temp)
          }else
          {# Step 1 results does NOT exist on disk -- we have to make prediction
            # load input data from memory
            cat(sprintf("    make Step 1 prediction... %s\n",PredFileStep1))
            saveRDS(Sep,paste0(PredFileStep1,".part"))
            
            if(as.character(j) %in% names(InputData_list))
            {
              cat(sprintf("    loading input data...%s\n",PredFileStep1))
              InputData <- InputData_list[as.character(j)]
            }else
            { # input data not available in memory
              # load available merged file
              cat(sprintf("    reading input data...%s\n",InputFileStep1))
              IsProcessFlag = FALSE
              if(file.exists(InputFileStep1))
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
              
              if(!IsProcessFlag)
              {
                InputData <-readRDS_robust(InputFileStep1)
                X_Var = which(!names(InputData) %in% c("GEOSChem_BC","GEOSChem_NH4", "GEOSChem_NO2","GEOSChem_PM25","GEOSChem_SO4","GEOSChem_OA"))
                InputData <- InputData[,X_Var]
                
                ### the only thing changed here for NO2 modeling, add more variables
                if(j<as.numeric(as.Date(paste0("2000-01-01"))))
                {
                  InputData_addition = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date("2000-01-01"),as.Date("2000-01-01"),OPTION,SiteData_Predict,VariableList)
                }else if(j>as.numeric(as.Date(paste0("2006-12-31"))))
                {
                  InputData_addition = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date("2016-12-31"),as.Date("2016-12-31"),OPTION,SiteData_Predict,VariableList)
                }
                else
                {
                  InputData_addition = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date(j,origin = as.Date("1970-01-01")),as.Date(j,origin = as.Date("1970-01-01")),OPTION,SiteData_Predict,VariableList)
                }
                InputData_addition$MonitorData <- NULL
                InputData_addition$CalendarDay<-NULL
                InputData_addition$SiteCode<-NULL
                InputData_addition$Other_Lat<-NULL
                InputData_addition$Other_Lon <- NULL
                
                InputData_addition <- StandardData(DirPath,InputData_addition,Sep,paste0(YEAR,"_",VariableID))
                InputData <- cbind(InputData,InputData_addition)
#saveRDS(InputData,paste0(InputFileStep1,"new"))
                InputData_h2o = as.h2o(InputData,destination_frame = paste0("InputData_",UniqueID))
              }else
              { # read and impute from scratch
                # sink("Temp.txt")
                saveRDS(Sep,paste0(InputFileStep1,".part"))
                if(j<as.numeric(as.Date(paste0("2000-01-01"))))
                {
                  InputData = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date("2000-01-01"),as.Date("2000-01-01"),OPTION,SiteData_Predict,VariableList)
                }else if(j>as.numeric(as.Date(paste0("2006-12-31"))))
                {
                  InputData = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date("2016-12-31"),as.Date("2016-12-31"),OPTION,SiteData_Predict,VariableList)
                }
                else
                {
                  InputData = ReadData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date(j,origin = as.Date("1970-01-01")),as.Date(j,origin = as.Date("1970-01-01")),OPTION,SiteData_Predict,VariableList)
                }
                InputData <- StandardData(DirPath,InputData,Sep,paste0(YEAR,"_",VariableID))
                InputData_h2o = as.h2o(InputData,destination_frame = paste0("InputData_",UniqueID))
                InputData_h2o = ImputeData(DirPath,Sep,InputData_h2o,InputData,OPTION,paste0(YEAR,"_",VariableID),SiteData_Train)
                InputData <- as.data.frame(InputData_h2o)
                h2o.rm(InputData_h2o)
                # save merged data
                saveRDS(InputData,InputFileStep1)
                file.remove(paste0(InputFileStep1,".part"))
                # delete date
                #DeleteData(DirPath,Sep,VariableID,NAME,SiteName_Predict,as.Date(j,origin = as.Date("1970-01-01")),as.Date(j,origin = as.Date("1970-01-01")),OPTION,SiteData_Predict,VariableList)
                InputData_list[[as.character(j)]] = InputData
                # sink()
              }
            }
            InputData_h2o = as.h2o(InputData,destination_frame = paste0("InputData_",UniqueID))
            # make Step 1 prediction on day j
            InputData$pred_nn_1<-as.vector(h2o.predict(mod_nn_1,newdata=InputData_h2o)$predict)
            InputData$pred_gbm_1<-as.vector(h2o.predict(mod_gbm_1,newdata=InputData_h2o)$predict)
            InputData$pred_rf_1<-as.vector(h2o.predict(mod_rf_1,newdata=InputData_h2o)$predict)
            Pred<-predict.bam(mod_ensemble_1,newdata = InputData)# class(Pred) : "numeric"
            cat(sprintf("  predicting now \n"))
            dim(Pred)<-c(1,N_Site_Predict)# change to matrix
            cat(sprintf("  saving prediction now 1 ...\n"))
            rownames(Pred)<-as.character(j)
            cat(sprintf("  saving prediction now 2 ...%s\n",PredFileStep1))
            #saveRDS(Pred,paste0("/n/home02/xqiu/","PredictionStep1_",NAME,"_",SiteName_Predict,"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds"))
            saveRDS(Pred,PredFileStep1)
            cat(sprintf("  saving prediction now 3 ...\n"))
            file.remove(paste0(PredFileStep1,".part"))
            gc()
            h2o:::.h2o.garbageCollect()
            Pred_Temporal = rbind(Pred_Temporal,Pred)
          }
        }
      }
      
      cat(sprintf("    update averaged weights...\n"))
      # update weighted avareges based on Pred_Temporal
      Pred_Temporal = Pred_Temporal[as.character((i-(Window_Time-1)/2):(i+(Window_Time-1)/2)),]
      # InputData_list = InputData_list[as.character((i-(Window_Time-1)/2):(i+(Window_Time-1)/2))]
      
      # get data and do prediction!
      # if(as.character(i) %in% names(InputData_list))
      # {
      #   InputData <- InputData_list[[as.character(i)]]
      #   InputData_h2o <- as.h2o(InputData)
      # }else
      # {
        InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
        InputData <-readRDS_robust(InputFileStep1)
        InputData_h2o = as.h2o(InputData,destination_frame = paste0("InputData_",UniqueID))
      # }
      
      # update input data
      Pred_1 = apply(Pred_Temporal,2,function(x) as.numeric(filter(x, rep(1/7,7),sides = 2,circular = TRUE)))
      Pred_2 = apply(Pred_Temporal,2,function(x) as.numeric(filter(x, c(1/16,2/16,3/16,4/16,3/16,2/16,1/16),sides = 2,circular = TRUE)))
      Pred_3 = apply(Pred_Temporal,2,function(x) as.numeric(filter(x, c(1/44,4/44,9/44,16/44,9/44,4/44,1/44),sides = 2,circular = TRUE)))
      
      cat(sprintf("    reading weight matrix...%s\n",paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak41_",SiteName_Train,"_",SiteName_Predict,".rds")))
      Weight = readRDS_robust(paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak41_",SiteName_Train,"_",SiteName_Predict,".rds"))
      Pred_4 = t(as.matrix(Pred_Spatial[as.character(i),]%*%Weight))
      rm(list=c("Weight"))
      gc()
      
      cat(sprintf("    reading weight matrix...%s\n",paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak42_",SiteName_Train,"_",SiteName_Predict,".rds")))
      Weight = readRDS_robust(paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak42_",SiteName_Train,"_",SiteName_Predict,".rds"))
      Pred_5 = t(as.matrix(Pred_Spatial[as.character(i),]%*%Weight))
      rm(list=c("Weight"))
      gc()

      cat(sprintf("    reading weight matrix...%s\n",paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak43_",SiteName_Train,"_",SiteName_Predict,".rds")))
      Weight = readRDS_robust(paste0(DirPath_Processed,SiteName_Predict,Sep,"Temp",Sep,"SpatialLaggedWeightPeak43_",SiteName_Train,"_",SiteName_Predict,".rds"))
      Pred_6 = t(as.matrix(Pred_Spatial[as.character(i),]%*%Weight))
      rm(list=c("Weight"))
      gc()
      
      Pred_1 = Pred_1[(Window_Time+1)/2,]
      Pred_2 = Pred_2[(Window_Time+1)/2,]
      Pred_3 = Pred_3[(Window_Time+1)/2,]
      dim(Pred_1)<-c(1*N_Site_Predict)
      dim(Pred_2)<-c(1*N_Site_Predict)
      dim(Pred_3)<-c(1*N_Site_Predict)
      
      Temp_h2o = as.h2o(as.data.frame(cbind(Pred_1,Pred_2,Pred_3,Pred_4,Pred_5,Pred_6)),destination_frame = paste0("Temp_h2o_",UniqueID))
      colnames(Temp_h2o)<-c("Temporal_Lagged_1","Temporal_Lagged_2","Temporal_Lagged_3","Spatial_Lagged_1","Spatial_Lagged_2","Spatial_Lagged_3")
      InputData_h2o = h2o.cbind(InputData_h2o,Temp_h2o)
      
      ## step 2 prediction
      InputData$pred_nn_2<-as.vector(h2o.predict(mod_nn_2,newdata=InputData_h2o)$predict)
      InputData$pred_gbm_2<-as.vector(h2o.predict(mod_gbm_2,newdata=InputData_h2o)$predict)
      InputData$pred_rf_2<-as.vector(h2o.predict(mod_rf_2,newdata=InputData_h2o)$predict)
      Pred<-predict.bam(mod_ensemble_2,newdata = InputData)
      dim(Pred)<-c(1,N_Site_Predict)  
      Pred <- exp(Pred)
      Pred[which(Pred>200)]=NA
      cat(sprintf("make Step 2 Prediction...%s\n",PredFileStep1))
      # save Step 2 results and delete temporal files
      saveRDS(Pred,PredFileStep2)
      file.remove(paste0(PredFileStep2,".part"))
      gc()
      h2o:::.h2o.garbageCollect()
      # ## for testing only
      # InputFileTemp = paste0(DirPath_PredInput,"InputTemp_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
      # InputData = cbind(InputData,as.data.frame(Temp_h2o))
      # saveRDS(InputData,InputFileTemp)
      h2o.rm(paste0("InputData_",UniqueID))
      h2o.rm(paste0("Temp_h2o_",UniqueID))
    }
  }
  
  ##remove h2o object
  #h2o.rm(mod_nn_1)
  #h2o.rm(mod_nn_2)
  #h2o.rm(mod_rf_1)
  #h2o.rm(mod_rf_2)
  #h2o.rm(mod_gbm_1)
  #h2o.rm(mod_gbm_2)
  
}

##### ABOUT THIS FUNCTION ################
## main function to do prediction for the entire country for one year; so this function uses a loop to go over all regions in the country, and complete 2-step predictions for each region in the given year

##### PARAMETER ##########################
## StartID: the Region ID that we make predictions at (1 ~ 50)
## YEAR: at which year we will make predictions
ModelPrediction <- function(StartID,YEAR)
{
  source("ModelFunctions.R")
  # ##################
  # ## for testing purpose
  # DirPath = "D:\\Google Drive\\Research\\USTemperature\\"
  # Sep = "\\"
  # #time window width used to calculate two-step modeling
  # Window_Time = 7
  # # the name of 1 km grid cells for the entire U.S.
  # SiteName_Predict_Total = "AQRVPM25_test"
  # # the name of 1 km grid cell tiles
  # SiteName_Predict_List = c("AQRVPM25_test1","AQRVPM25_test2")
  
  # # for prediction purpose
  DirPath = "/n/regal/jschwrtz_lab/qdi/USTemperature/"
  # DirPath = "Z:/"
  Sep = "/"
  #time window width used to calculate two-step modeling
  Window_Time = 7
  # the name of 1 km grid cells for the entire U.S.
  SiteName_Predict_Total = "USGrid"
  # the name of 1 km grid cell tiles
  SiteName_Predict_List = c("USGrid1","USGrid2","USGrid3","USGrid4","USGrid5","USGrid6","USGrid7","USGrid8","USGrid9","USGrid10","USGrid11","USGrid12","USGrid13","USGrid14","USGrid15","USGrid16","USGrid17","USGrid18","USGrid19","USGrid20","USGrid21","USGrid22","USGrid23","USGrid24","USGrid25","USGrid26","USGrid27","USGrid28","USGrid29","USGrid30","USGrid31","USGrid32","USGrid33","USGrid34","USGrid35","USGrid36","USGrid37","USGrid38","USGrid39","USGrid40","USGrid41","USGrid42","USGrid43","USGrid44","USGrid45","USGrid46","USGrid47","USGrid48","USGrid49","USGrid50")
   
  
  ###################################################
  ############# DO NOT CHANGE BELOW #################
  # path
  DirPath_Assembled = paste0(DirPath,"assembled_data",Sep)
  DirPath_Processed = paste0(DirPath,"processed_data",Sep)
  # coordinate system
  GCS = "North_America_Equidistant_Conic"
  GCS_project <-  CRS(" +proj=eqdc +lat_0=40 +lon_0=-96 +lat_1=20 +lat_2=60 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0")
  US<-readOGR(dsn = paste0(DirPath,"raw_data",Sep,"data",Sep,"shapefile",Sep,"US_",GCS,".shp"), layer = paste0("US_",GCS))
  # read location file
  SiteData_Predict_Total<-ReadLocation(paste0(DirPath_Processed,SiteName_Predict_Total,Sep,"Location",Sep,SiteName_Predict_Total,"Site_",GCS))
  coordinates(SiteData_Predict_Total)<-~Lat+Lon
  OPTION = "prediction"#"prediction"#"training"
  fileConn<-file(paste0(DirPath_Assembled,"prediction",Sep,"error_",StartID,"_",YEAR,".txt"), open="a")
  
  
  ### prediction arrary; change the order of tile being processed; make sure each thread start with different tile
  if(1 == StartID)
  {
    Index_SiteName_Predict_List = seq(StartID,length(SiteName_Predict_List),1)
  }else if(1<StartID && StartID<=length(SiteName_Predict_List))
  {
    Index_SiteName_Predict_List = c(seq(StartID,length(SiteName_Predict_List),1),seq(1,StartID - 1,1))
  }else
  {
    stop("make sure to begin with 1 and ",length(SiteName_Predict_List))
  }

  ## folder path
  ## put Step 2 prediction results
  # DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep)
  ## put Step 1 prediction results
  # DirPath_PredStep1 = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Step1",Sep)
  ## put merged inpout data
  # DirPath_PredInput = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Input",Sep)
  ## make maps: only for overall grid cells
  # DirPath_PredPic = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep,"Picture",Sep)

  ## FileName
  # Step 1
  # PredFileStep1 = paste0(DirPath_PredStep1,"PredictionStep1_",NAME,"_",SiteName_Predict,"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
  # merged inputfile name
  # InputFileStep1 = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(j,origin = as.Date("1970-01-01")),"%Y%m%d"))
  # PredFileStep2 = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
  ## only work for overall
  # PredFileInterp = paste0(DirPath_Pred,"PredictionInterp_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
  # PredFilePic = paste0(DirPath_PredPic,"PredictionStep2_",NAME,"_",SiteName_Predict,"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(i,origin = as.Date("1970-01-01")),"%Y%m%d"),".jpg")

  # check whether all Step 2 prediction at overall grid cells are done
  while(TRUE)
  {
    # start h2o
    x = tryCatch({
      # h2o.init(max_mem_size = "10g",nthreads = -1)
      ## connect to a specific h2o instance or the h2o instance with smallest system load
      if(!h2o.clusterIsUp())
      {
        if(""==IP)
        {
Sys.sleep(StartID*10+runif(1)*100)

          H2oJob_List <- read.csv("/n/home02/xqiu/h2o_instances.txt",colClasses = c("numeric","character","character","character","numeric","numeric","character","numeric","character"),sep = ",")
          Index <- which(H2oJob_List$Usage == "Prediction" & H2oJob_List$Running =="T" & H2oJob_List$Connections<5)
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
      
    }, error = function(e) {
      writeLines(paste("errors\t",StartID,"\t",YEAR,"\t",format(Sys.time(), "%a %b %d %X %Y"),"\t",toString(e),"\n\n"), fileConn)
    })
    
    ####make prediction here
    x = tryCatch({
    
      # get all maps and assembled files
      FilesList = c()
      TempDate = format((seq(as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),1)),"%Y%m%d")
      for(IndexName in 1:length(NAME_List))
      {
        NAME = NAME_List[IndexName]
        VariableID = VariableID_List[IndexName]
        
        DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep)
        DirPath_PredPic = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep,"Picture",Sep)
        if(!dir.exists(DirPath_Pred))
        {
          dir.create(DirPath_Pred)
        }
        if(!dir.exists(DirPath_PredPic))
        {
          dir.create(DirPath_PredPic)
        }
        PredFileStep2 = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",TempDate,"_",TempDate,".rds")
        PredFilePic = paste0(DirPath_PredPic,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",TempDate,"_",TempDate,".jpg")
        
        FilesList = c(FilesList,PredFileStep2,PredFilePic)
      }
      
      # check all maps and assembled files are done, it means predictions done
      if(all(file.exists(FilesList)))# all predictions at all air pollutants are done! Now delete temporary files
      {
        while(TRUE)
        { ## all predictions at all air pollutants are done! Now delete temporary files
  
          ## delete Step 1 at all grid cells; but not deleting the whole thing
          FilesList = c()
          for(IndexGrid in 1:length(SiteName_Predict_List))
          {
            SiteName_Predict = SiteName_Predict_List[IndexGrid]
            for(IndexName in 1:length(NAME_List))
            {
              NAME = NAME_List[IndexName]
              VariableID = VariableID_List[IndexName]
              
              cat(sprintf("deleting temporary files...%s %s\n",SiteName_Predict,NAME))
              # folder
              DirPath_PredStep1 = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Step1",Sep)
              DirPath_PredInput = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Input",Sep)
              DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep)
              # date
              StartDate = as.Date(paste0(YEAR,"-01-01"))+(Window_Time-1)/2
              EndDate = as.Date(paste0(YEAR,"-12-31"))-(Window_Time-1)/2
              TempDate = format((seq(StartDate,EndDate,1)),"%Y%m%d")
              # delete Step 1 results
              TempFiles = paste0(DirPath_PredStep1,"PredictionStep1_",NAME,"_",SiteName_Predict,"_",TempDate,"_",TempDate,".rds")
              FilesList = c(FilesList,TempFiles)
              # delete merged input files
              # TempFiles = paste0(DirPath_PredInput,"Input_",NAME,"_",SiteName_Predict,"_",TempDate,"_",TempDate,".rds")
              # FilesList = c(FilesList,TempFiles)
              # delete Step 2 results at each tile
              TempDate = format((seq(as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),1)),"%Y%m%d")
              TempFiles = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict,"_",TempDate,"_",TempDate,".rds")
              FilesList = c(FilesList,TempFiles)
            }
          }
        
          if(any(file.exists(FilesList)))
          {
            ## delete matlab files;
            file.remove(FilesList)
          }else
          {
            break
          }
        }
        # predictions are done, and deletion is done
        cat(sprintf("%d is done!\n",YEAR))
        break
      }else
      {
        # maps and assemble files are not done, check whether assembled files are done
        ##MATLAB process input data
        #all predictions at all air pollutants are NOT done! make predictions now
        for(j in 1:length(NAME_List))
        {
          ###get localized parameters
          NAME = NAME_List[j]
          SiteName_Train = SiteName_Train_List[j]
          VariableID = VariableID_List[j]
  
          # make prediction for all tile, until all done
          while(TRUE)
          {
            ##check Step 2 prediction on all tiles are done
            ## check whether Step 2 prediction on all tiles are done -- true: all prediction on that grid cell tile is done
            CheckList = rep(FALSE,length(SiteName_Predict_List))
            for(IndexGrid in 1:length(SiteName_Predict_List))
            {
              SiteName_Predict = SiteName_Predict_List[IndexGrid]
              # get Step 2 file names
              DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep)
              TempDate = format((seq(as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),1)),"%Y%m%d")
              TempFiles = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict,"_",TempDate,"_",TempDate,".rds")
              CheckList[IndexGrid] = all(file.exists(TempFiles))
            }
  
            if(all(CheckList))
            { # all Step 2 predictions on grid cell tiles are done; now, assemble then and make maps
              while(TRUE)
              { # assemble prediction results, making maps
                for(IndexDate in as.numeric(as.Date(paste0(YEAR,"-01-01")):as.Date(paste0(YEAR,"-12-31"))))
                {
                  ## assemble files section
                  DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep)
                  PredFileStep2 = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
  
                  # check whether the file has been done or in the middle of processing
                  IsProcessFlag = FALSE
                  if(file.exists(PredFileStep2))
                  {
                    IsProcessFlag = FALSE
                  }else
                  {  ## has been process before...
                    if(file.exists(paste0(PredFileStep2,".part")))
                    { ## has been done in longer than 5 minutes ago
                      if(difftime(Sys.time(),file.info(paste0(PredFileStep2,".part"))$mtime,units="hours")>0.1)
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
                  # now assemble files!
                  if(IsProcessFlag)
                  {
                    cat(sprintf("assembling prediction...%s\n",PredFileStep2))
                    saveRDS(Sep,paste0(PredFileStep2,".part"))
                    Temp = matrix(NA,nrow = 1,ncol = 0)
                    for(IndexGrid in 1:length(SiteName_Predict_List))
                    {
                      TempPath = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_List[IndexGrid],Sep)
                      TempFile = paste0(TempPath,"PredictionStep2_",NAME,"_",SiteName_Predict_List[IndexGrid],"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),".rds")
                      Temp1 <- readRDS_robust(TempFile)
                      Temp <- cbind(Temp,Temp1)
                    }
                    saveRDS(Temp,PredFileStep2)
                    file.remove(paste0(PredFileStep2,".part"))
                  }
  
                  ## make maps
                  DirPath_PredPic = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep,"Picture",Sep)
                  PredFilePic = paste0(DirPath_PredPic,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),"_",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y%m%d"),".jpg")
                  
                  ## check whether the file has been done or in the middle of processing
                  IsProcessFlag = FALSE
                  if(file.exists(PredFilePic))
                  {
                    IsProcessFlag = FALSE
                  }else
                  {  ## has been process before...
                    if(file.exists(paste0(PredFilePic,".part")))
                    { ## has been done in longer than 5 minutes ago
                      if(difftime(Sys.time(),file.info(paste0(PredFilePic,".part"))$mtime,units="hours")>0.1)
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
                  # make maps
                  if(IsProcessFlag)
                  { # make map
                    if(file.exists(PredFileStep2))
                    {
                      cat(sprintf("making maps...%s\n",PredFilePic))
                      Temp = readRDS_robust(PredFileStep2)
                      rbPal <- colorRampPalette(c('red','blue'))
                      Col <- rbPal(10)[as.numeric(cut(Temp,breaks = 10))]
                      jpeg(PredFilePic,width=8,height=6,res=600,units='in')
                      plot(US,main=paste0(NAME," ",format(as.Date(IndexDate,origin = as.Date("1970-01-01")),"%Y-%m-%d")))
                      points(SiteData_Predict_Total$Lat ~ SiteData_Predict_Total$Lon, col = Col, cex=0.2, pch = 15)
                      dev.off()
                    }
                  }
                }
                # find out all assembled, maps,check whether all of them are done
                DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep)
                DirPath_PredPic = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict_Total,Sep,"Picture",Sep)
                TempDate = format((seq(as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),1)),"%Y%m%d")
                PredFileStep2 = paste0(DirPath_Pred,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",TempDate,"_",TempDate,".rds")
                PredFilePic = paste0(DirPath_PredPic,"PredictionStep2_",NAME,"_",SiteName_Predict_Total,"_",TempDate,"_",TempDate,".jpg")
                CheckList1 = all(file.exists(c(PredFileStep2,PredFilePic)))
                # if all done, break;
                if(CheckList1)
                {
                  break
                }
              }
              # all Step 2 prediction are done, assemble, map-making are done
              break
  
            }else
            { # all Step 2 predictions on grid cell tiles are NOT done;
              ##### make prediction for each prediction grid cells
              CheckList = CheckList[Index_SiteName_Predict_List]# change order# tested by on 05/09/2018: CheckList = c(F,F,T,F,F,F,T,F,T,F),  Index_SiteName_Predict_List = c(4,5,6,7,8,9,10,1,2,3)
              #Index_SiteName_Predict_List[which(!CheckList)]
              for(IndexGrid in Index_SiteName_Predict_List[which(!CheckList)])
              {
                # get current grid cell name
                SiteName_Predict = SiteName_Predict_List[IndexGrid]
                # make predictions for each air pollutant
                cat(sprintf("%s\nmaking prediction on grid cell:%s; YEAR:%d,air pollutant: %s; Variable ID: %s; Monitoring data: %s\n%s\n",paste(rep("*",50),collapse = ""),SiteName_Predict,YEAR,NAME,VariableID,SiteName_Train,paste(rep("*",50),collapse = "")))
                #####folder path######
                DirPath_Model = paste0(DirPath,"assembled_data",Sep,"training",Sep,NAME,"_",YEAR,"_",VariableID,Sep)
                # put Step 2 prediction results
                DirPath_Pred = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep)
                DirPath_PredPic = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,Sep,"Picture",Sep)
                # put Step 1 prediction results
                DirPath_PredStep1 = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Step1",Sep)
                # put merged inpout data
                DirPath_PredInput = paste0(DirPath,"assembled_data",Sep,"prediction",Sep,NAME,"_",SiteName_Predict,"_Input",Sep)
                if(!dir.exists(DirPath_Pred))
                {
                  dir.create(DirPath_Pred)
                }
                if(!dir.exists(DirPath_PredStep1))
                {
                  dir.create(DirPath_PredStep1)
                }
                if(!dir.exists(DirPath_PredInput))
                {
                  dir.create(DirPath_PredInput)
                }
                if(!dir.exists(DirPath_PredPic))
                {
                  dir.create(DirPath_PredPic)
                }
                col = c(rep("character",6))
                col[c(2,3)] = "logical"
                col[c(7,8)] = "numeric"
                VariableList = read.csv(paste0(DirPath,"assembled_data",Sep,"VariableList_",VariableID,".csv"),colClasses = col)
                VariableList = VariableList[!is.na(VariableList$READ),]
                #### prepare input data set
                if(CheckData(DirPath,Sep,NAME,SiteName_Predict,YEAR,VariableList))
                {
                  # ModelPrediction_Fun(DirPath,Sep,VariableID,NAME,YEAR,Window_Time,GCS,"prediction",SiteName_Train,SiteName_Predict)
                  ModelPrediction_Fun(DirPath,Sep,VariableID,NAME,as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),Window_Time,GCS,"prediction",SiteName_Train,SiteName_Predict,VariableList)
                }else
                {
                  # code to switch the code directory and process data
                  #Code = c(paste0("cd('",DirPath,"raw_data",Sep,"matlab_to_interpolate",Sep,"')"),
                  #         paste0("Run_InterpolateDataSummary('",IndexGrid,"',[datenum(",YEAR,",1,1),datenum(",YEAR,",12,31)],0,nan)"))
                  #res = run_matlab_code(Code)
                  ModelPrediction_Fun(DirPath,Sep,VariableID,NAME,as.Date(paste0(YEAR,"-01-01")),as.Date(paste0(YEAR,"-12-31")),Window_Time,GCS,"prediction",SiteName_Train,SiteName_Predict,VariableList)
                }
              }
            }
          }
        }
      }
    
    }, error = function(e) {
      writeLines(paste("errors\t",StartID,"\t",YEAR,"\t",format(Sys.time(), "%a %b %d %X %Y"),"\t",toString(e),"\n\n"), fileConn)
    })
    
  }    
  
  close(fileConn)
}


## connect to an existing h2o instance using ip addresses
if(""==IP)
{
  Sys.sleep(StartID*3+runif(1)*100)
  H2oJob_List <- read.csv("/n/home02/xqiu/h2o_instances.txt",colClasses = c("numeric","character","character","character","numeric","numeric","character","numeric","character"),sep = ",")
  Index <- which(H2oJob_List$Usage == "Prediction" & H2oJob_List$Running =="T" & H2oJob_List$Connections<5)
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

source("ModelFunctions.R")
UniqueID <- sample(1:1000000, 1, replace=F)
ModelPrediction(StartID,YEAR)

