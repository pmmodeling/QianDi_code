library(rhdf5)
library(h2o)
## read data

# # ####################
# # ## marco-definition
# DirPath = "D:\\Google Drive\\Research\\USTemperature\\"
# # DirPath = "/nfs/bigdata_nobackup/a/airpred_d_scratch/"
# 
# # ########### for training ########################
# Sep = "\\"
# VariableID = 99940
# NAME = "PM25"
# SITENAME = "AQRVPM25"
# STARTDATE = as.Date("2008-01-01")
# ENDDATE = as.Date("2008-12-31")
# OPTION = "training"#"training""training"#
# SiteData<-ReadLocation(paste0(DirPath,"processed_data/AQRVPM25/Location/AQRVPM25Site"))
# GCS = "North_America_Equidistant_Conic"
# #
# ##read csv configuration file
# col = c(rep("character",6))
# col[c(2,3)] = "logical"
# col[c(7,8)] = "numeric"
# VariableList = read.csv(paste0(DirPath,"assembled_data",Sep,"VariableList_",VariableID,".csv"),colClasses = col)
# VariableList = VariableList[!is.na(VariableList$READ),]

# read hdf 5 files, and delte it if reading unsuccessful
#Temp <- h5read_robust("C:\\Users\\qid335\\Downloads\\Qian Di Conditions .docx",name = "Result")

h5read_robust<-function(FilePath,name)
{
  Temp = NA
  x = result = tryCatch({
    Temp <- h5read(FilePath,name = name)
  },error = function(e) {
    file.remove(FilePath)
    stop("file reading error!",FilePath)
  })
  
  return(Temp)
}



# read location of SiteData
ReadLocation <- function(FilePath)
{
  if(file.exists(paste0(FilePath,".h5")))
  {
    SiteCode <- as.character(h5read_robust(paste0(FilePath,".h5"),name = "SiteCode"))
    Lat <- as.numeric(h5read_robust(paste0(FilePath,".h5"),name = "Latitude"))
    Lon <- as.numeric(h5read_robust(paste0(FilePath,".h5"),name = "Longitude"))
    SiteData = data.frame(SiteCode,Lat,Lon)
    return(SiteData)
  }else if(file.exists(paste0(FilePath,".rds")))
  {
    SiteData = readRDS(paste0(FilePath,".rds"))
    return(SiteData)
  }else
  {
    stop(FilePath," does not exist!")
  }
}

## examples
## read data from HDF files
## InputData = ReadData(DirPath,Sep,VariableID,NAME,SITENAME,STARTDATE,ENDDATE,OPTION,SiteData,VariableList)
ReadData<-function(DirPath,Sep,VariableID,NAME,SITENAME,STARTDATE,ENDDATE,OPTION,SiteData,VariableList)
{
  if("prediction" == OPTION && (STARTDATE!=ENDDATE))
  {
    stop("prediction is done at daily basis. ENDDATE and STARTDATE must be equal!")
  }
  Year_Start = as.numeric(format(STARTDATE,'%Y'))
  Year_End = as.numeric(format(ENDDATE,'%Y'))

  ## path
  DirPath_Assembled = paste0(DirPath,"assembled_data",Sep)
  DirPath_Processed = paste0(DirPath,"processed_data",Sep)

  N_Site = nrow(SiteData)

  InputData_All = data.frame()
  for(Year in Year_Start:Year_End)
  {
    if("prediction" == OPTION)
    {
      N_Day = 1
    }else if("training" == OPTION)
    {
      N_Day = as.numeric(as.Date(paste0(Year,"-12-31")) - as.Date(paste0(Year,"-01-01")) + 1)
    }
    
    InputData = data.frame(matrix(NA,ncol = sum(VariableList$READ),nrow = N_Day*N_Site))
    names(InputData) <- VariableList$VariableName[VariableList$READ]
    Flag = FALSE
    ## for each line
    for(i in 1:nrow(VariableList))
    {
      if(VariableList$READ[i])
      {
        # get the correct file name
        if("2" == VariableList$format[i])
        {
          TempFileName = gsub("\\.mat",".h5",VariableList$NameTemplate[i])
          TempFileName = gsub("\\$NAME\\$",NAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_DATA\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_MODEL\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$StartYear\\$",Year,TempFileName)
          TempFileName = gsub("\\$EndYear\\$",Year,TempFileName)
          TempFileName = gsub("\\$StartDate\\$",paste0(Year,"0101"),TempFileName)
          TempFileName = gsub("\\$EndDate\\$",paste0(Year,"1231"),TempFileName)
        }else if("1" == VariableList$format[i])
        {
          TempFileName = gsub("\\.mat",".h5",VariableList$NameTemplate[i])
          TempFileName = gsub("\\$NAME\\$",NAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_DATA\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_MODEL\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$StartYear\\$",Year,TempFileName)
          TempFileName = gsub("\\$EndYear\\$",Year,TempFileName)
          if("training" == OPTION)
          {
            TempFileName = gsub("\\$StartDate\\$",paste0(Year,"0101"),TempFileName)
            TempFileName = gsub("\\$EndDate\\$",paste0(Year,"1231"),TempFileName)
          }else if("prediction" == OPTION)
          {
            TempFileName = gsub("\\$StartDate\\$",format(STARTDATE,'%Y%m%d'),TempFileName)
            TempFileName = gsub("\\$EndDate\\$",format(ENDDATE,'%Y%m%d'),TempFileName)
          }

        }
        #generate folder
        TempFolderName = paste0(DirPath_Processed,SITENAME,Sep,VariableList$Folder[i],Sep)
        # read data
        Temp = rep(NA,time = N_Day*N_Site)
        x = tryCatch(
          {
            if("1" == VariableList$format[i])
            {
              Temp <- h5read_robust(paste0(TempFolderName,TempFileName),name = "Result")
              dim(Temp)<-c(N_Day*N_Site,1)
              H5close()
            }else if("2" == VariableList$format[i])
            {
              Temp <- h5read_robust(paste0(TempFolderName,TempFileName),name = "Result")
              Temp <- rep(Temp,each = N_Day)
              H5close()
            }
            cat(sprintf("reading...%d %s\n",i,paste0(TempFolderName,TempFileName)))
          },error = function(err)
          {
            if("prediction" == OPTION && "Monitor" == VariableList$Folder[i])
            {
              cat(sprintf("skipping...%d %s\n",i,paste0(TempFolderName,TempFileName)))
              return()
            }
            if(Year<VariableList$StartYear[i] || Year>VariableList$EndYear[i])
            {
              cat(sprintf("skipping...%d %s\n",i,paste0(TempFolderName,TempFileName)))
              return()
            }
            
            print(err)
            # return(NULL)
          }
        )
        # merge data
        if(inherits(x, "error"))
        {
          Flag = TRUE
        }else
        {
          InputData[,VariableList$VariableName[i]] = as.numeric(Temp)
          rm(list = c("Temp"))
        }
      }
    }
    
    if(Flag)
    {
      return(NULL)
    }

    ## add more variables
    InputData[,"Other_Lat"]<-rep(SiteData$Lat,each = N_Day)
    InputData[,"Other_Lon"]<-rep(SiteData$Lon,each = N_Day)
    InputData[,"SiteCode"]<-rep(SiteData$SiteCode,each = N_Day)
    
    if("prediction" == OPTION)
    {
      InputData[,"CalendarDay"]<-rep(as.numeric(STARTDATE),time = N_Site)
    }else if("training" == OPTION)
    {
      InputData[,"CalendarDay"]<-rep(seq(as.numeric(as.Date(paste0(Year,"-01-01"))),as.numeric(as.Date(paste0(Year,"-12-31"))),1),time = N_Site)
    }
    
    
    if("REANALYSIS_uwnd_10m_DailyMax" %in% names(InputData) && "REANALYSIS_vwnd_10m_DailyMax" %in% names(InputData))
    {
      InputData[,"REANALYSIS_windspeed_10m_DailyMax"] = sqrt(InputData[,"REANALYSIS_uwnd_10m_DailyMax"]^2+InputData[,"REANALYSIS_vwnd_10m_DailyMax"]^2)
      InputData[,"REANALYSIS_uwnd_10m_DailyMax"]<-NULL
      InputData[,"REANALYSIS_vwnd_10m_DailyMax"]<-NULL
    }
    if("REANALYSIS_uwnd_10m_DailyMean" %in% names(InputData) && "REANALYSIS_vwnd_10m_DailyMean" %in% names(InputData))
    {
      InputData[,"REANALYSIS_windspeed_10m_DailyMean"] = sqrt(InputData[,"REANALYSIS_uwnd_10m_DailyMean"]^2+InputData[,"REANALYSIS_vwnd_10m_DailyMean"]^2)
      InputData[,"REANALYSIS_uwnd_10m_DailyMean"]<-NULL
      InputData[,"REANALYSIS_vwnd_10m_DailyMean"]<-NULL
    }
    if("REANALYSIS_uwnd_10m_DailyMin" %in% names(InputData) && "REANALYSIS_vwnd_10m_DailyMin" %in% names(InputData))
    {
      InputData[,"REANALYSIS_windspeed_10m_DailyMin"] = sqrt(InputData[,"REANALYSIS_uwnd_10m_DailyMin"]^2+InputData[,"REANALYSIS_vwnd_10m_DailyMin"]^2)
      InputData[,"REANALYSIS_uwnd_10m_DailyMin"]<-NULL
      InputData[,"REANALYSIS_vwnd_10m_DailyMin"]<-NULL
    }
    if("REANALYSIS_uwnd_10m_1Day" %in% names(InputData) && "REANALYSIS_vwnd_10m_1Day" %in% names(InputData))
    {
      InputData[,"REANALYSIS_windspeed_10m_1Day"] = sqrt(InputData[,"REANALYSIS_uwnd_10m_1Day"]^2+InputData[,"REANALYSIS_vwnd_10m_1Day"]^2)
      InputData[,"REANALYSIS_uwnd_10m_1Day"]<-NULL
      InputData[,"REANALYSIS_vwnd_10m_1Day"]<-NULL
    }
    if("REANALYSIS_uwnd_10m_5Day" %in% names(InputData) && "REANALYSIS_vwnd_10m_5Day" %in% names(InputData))
    {
      InputData[,"REANALYSIS_windspeed_10m_5Day"] = sqrt(InputData[,"REANALYSIS_uwnd_10m_5Day"]^2+InputData[,"REANALYSIS_vwnd_10m_5Day"]^2)
      InputData[,"REANALYSIS_uwnd_10m_5Day"]<-NULL
      InputData[,"REANALYSIS_vwnd_10m_5Day"]<-NULL
    }

    InputData_All = rbind(InputData_All,InputData)
  }
  return(InputData_All)
}

## standardize data frame
StandardData<-function(DirPath,InputData,Sep,VariableID)
{
  TempFile = paste0(DirPath,"processed_data",Sep,"TempData",Sep,"Standardization_",VariableID,".rds")
  Index_var = names(InputData)[as.numeric(which(!names(InputData) %in% c("MonitorData","SiteCode", "CalendarDay","PM25_Region","NO2_Region","Ozone_Region") ))]
  
  if(file.exists(TempFile))
  { 
    Data = readRDS(TempFile)
    df_mean = Data[1,]
    df_sd = Data[2,]
  }else
  {  
    df_mean = sapply(InputData[,Index_var], mean, na.rm = TRUE)
    df_sd = sapply(InputData[,Index_var], sd, na.rm = TRUE)
    Data = rbind(df_mean,df_sd)
    saveRDS(Data,TempFile)
  }
  for(i in 1:length(Index_var))
  {
    InputData[,Index_var[i]] <- (InputData[,Index_var[i]] - df_mean[Index_var[i]])/df_sd[Index_var[i]]
  }
  return(InputData)
}

# # ## check whether data used for one year of modeling is all there; TRUE -- all files are there
# DirPath = "D:\\Google Drive\\Research\\USTemperature\\"
# Sep = "\\"
# VariableID = 99940
# NAME = "PM25"
# SITENAME = "AQRVPM25_test2"
# Year = 2008
# CheckData("D:\\Google Drive\\Research\\USTemperature\\","\\","PM25","AQRVPM25_test2",2008,VariableList)
CheckData<-function(DirPath,Sep,NAME,SITENAME,Year,VariableList)
{
  ## path
  DirPath_Assembled = paste0(DirPath,"assembled_data",Sep)
  DirPath_Processed = paste0(DirPath,"processed_data",Sep)

  FileList = c()
  TempDate = format((seq(as.Date(paste0(2008,"-01-01")),as.Date(paste0(2008,"-12-31")),1)),"%Y%m%d")
  ## for each line
  for(i in 1:nrow(VariableList))
  {
    if("MonitorData" == VariableList[i,]$VariableName)
    {
      next
    }
    if(VariableList$READ[i])
    {
      # get the correct file name
      if("2" == VariableList$format[i])
      {
        TempFileName = gsub("\\.mat",".h5",VariableList$NameTemplate[i])
        TempFileName = gsub("\\$NAME\\$",NAME,TempFileName)
        TempFileName = gsub("\\$SITENAME_DATA\\$",SITENAME,TempFileName)
        TempFileName = gsub("\\$SITENAME_MODEL\\$",SITENAME,TempFileName)
        TempFileName = gsub("\\$StartYear\\$",Year,TempFileName)
        TempFileName = gsub("\\$EndYear\\$",Year,TempFileName)
        TempFileName = gsub("\\$StartDate\\$",paste0(Year,"0101"),TempFileName)
        TempFileName = gsub("\\$EndDate\\$",paste0(Year,"1231"),TempFileName)
      }else if("1" == VariableList$format[i])
      {
        TempFileName = gsub("\\.mat",".h5",VariableList$NameTemplate[i])
        TempFileName = gsub("\\$NAME\\$",NAME,TempFileName)
        TempFileName = gsub("\\$SITENAME_DATA\\$",SITENAME,TempFileName)
        TempFileName = gsub("\\$SITENAME_MODEL\\$",SITENAME,TempFileName)
        TempFileName = gsub("\\$StartYear\\$",Year,TempFileName)
        TempFileName = gsub("\\$EndYear\\$",Year,TempFileName)
        TempFileName = gsub("\\$StartDate\\$","\\$EndDate\\$",TempFileName)
        TempFileName = sapply(TempDate, FUN = function(x) gsub("\\$EndDate\\$",x,TempFileName)) 
      }
      #generate folder
      TempFolderName = paste0(DirPath_Processed,SITENAME,Sep,VariableList$Folder[i],Sep)
      # read data
      TempFileName = paste0(TempFolderName,TempFileName)
      FileList = c(FileList,TempFileName)
    }
  }
  # FileList[which(!file.exists(FileList))] --- missing files.
  return(all(file.exists(FileList)))
}

## delete HDF files in one year
## DeleteData = ReadData(DirPath,Sep,VariableID,NAME,SITENAME,STARTDATE,ENDDATE,OPTION,SiteData,VariableList)
DeleteData<-function(DirPath,Sep,VariableID,NAME,SITENAME,STARTDATE,ENDDATE,OPTION,SiteData,VariableList)
{
  # STARTDATE must equal to ENDDATE
  Year_Start = as.numeric(format(STARTDATE,'%Y'))
  Year_End = as.numeric(format(ENDDATE,'%Y'))

  ## path
  DirPath_Assembled = paste0(DirPath,"assembled_data",Sep)
  DirPath_Processed = paste0(DirPath,"processed_data",Sep)

  ##
  N_Site = nrow(SiteData)

  InputData_All = data.frame()
  for(Year in Year_Start:Year_End)
  {
    N_Day = as.numeric(as.Date(paste0(Year,"-12-31")) - as.Date(paste0(Year,"-01-01")) + 1)
    ## for each line
    for(i in 1:nrow(VariableList))
    {
      if(VariableList$READ[i])
      {
        # get the correct file name
        if("prediction" == OPTION)
        {
          TempFileName = gsub("\\.mat",".h5",VariableList$NameTemplate[i])
          TempFileName = gsub("\\$NAME\\$",NAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_DATA\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$SITENAME_MODEL\\$",SITENAME,TempFileName)
          TempFileName = gsub("\\$StartYear\\$",Year,TempFileName)
          TempFileName = gsub("\\$EndYear\\$",Year,TempFileName)
          TempFileName = gsub("\\$StartDate\\$",format(STARTDATE,'%Y%m%d'),TempFileName)
          TempFileName = gsub("\\$EndDate\\$",format(ENDDATE,'%Y%m%d'),TempFileName)
        }
        #generate folder
        TempFolderName = paste0(DirPath_Processed,SITENAME,Sep,VariableList$Folder[i],Sep)
        # delete data
        if(grepl(format(STARTDATE,'%Y%m%d'),TempFileName) &  grepl(format(ENDDATE,'%Y%m%d'),TempFileName))
        {
          cat(sprintf("deleting...%d %s\n",i,paste0(TempFolderName,TempFileName)))
          file.remove(paste0(TempFolderName,TempFileName))
        }
      }
    }
  }
}

## data imputation
# DirPath = "D:\\Google Drive\\Research\\USTemperature\\"
# Sep = "\\"
# VariableID = 99937
# GCS = "North_America_Equidistant_Conic"
# SiteData<-ReadLocation(paste0(DirPath,"processed_data/AQRVPM25_test1/Location/AQRVPM25_test1Site_",GCS))
# ##read csv configuration file
# col = c(rep("character",6))
# col[c(2,3)] = "logical"
# col[c(7,8)] = "numeric"
# VariableList = read.csv(paste0(DirPath,"assembled_data",Sep,"VariableList_",VariableID,".csv"),colClasses = col)
# VariableList = VariableList[!is.na(VariableList$READ),]
# 
# InputData <- ReadData(DirPath,Sep,VariableID,"PM25","AQRVPM25_test1",as.Date("2008-01-01"),as.Date("2008-01-01"),"prediction",SiteData,VariableList)
# 
# InputData <- StandardData(DirPath,InputData,Sep,paste0(YEAR,"_",VariableID))
# InputData_h2o <- as.h2o(InputData)

ImputeData<-function(DirPath,Sep,InputData_h2o,InputData,OPTION,VariableID,SiteData)
{
  ## just impute those variables
  if("training" == OPTION)
  {
    ## the monitoring site we care about, within continental U.S.
    # SiteCode_Full =  SiteData[which(SiteData$Lat > 24.3 & SiteData$Lat <49.4  & SiteData$Lon > -124.8 & SiteData$Lon < -66.8),"SiteCode"]
    SiteCode_Full = as.character(unique(InputData[!is.na(InputData$MERRA2aer_SO4),"SiteCode"]))
    Index = is.element(as.vector(InputData$SiteCode),SiteCode_Full)
    # vaariable used to do imputation
    Index_var = names(InputData)
    # note: percentage of missing for each variable: apply(is.na(InputData[Index,]),2,sum)/nrow(InputData[Index,])
    ## variables used as predictors to do imputation
    Index_var_full = as.numeric(which(
      as.vector(apply(is.na(InputData[Index,]),2,sum)/nrow(InputData[Index,]) < 0.01) &
        !names(InputData) %in% c("MonitorData","SiteCode", "CalendarDay","PM25_Region","NO2_Region","Ozone_Region") ))
    
    ## variables to be imputed
    Index_var_impute = as.numeric(which(
      as.vector(apply(is.na(InputData[Index,]),2,sum)/nrow(InputData[Index,]) > 0.01) &
        !names(InputData) %in% c("MonitorData","SiteCode", "CalendarDay","PM25_Region","NO2_Region","Ozone_Region") ))
    
    ## impute every variable
    for(i in 1:length(Index_var_impute))
    {
      ## doing imputation
      TempDir = paste0(DirPath,"processed_data",Sep,"TempData",Sep,"RFimputation_",Index_var[Index_var_impute[i]],"_",VariableID,Sep)
      if(length(list.files(TempDir))>0)
      {
        cat(sprintf('loading imputation model...%s\n',Index_var[Index_var_impute[i]]))
        Model <- list.files(TempDir)
        h2o.loadModel(paste0(TempDir,Model[1]))
        mod_rf <- h2o.getModel(Model[1])
      }else
      {
        cat(sprintf('making imputation prediction...%s\n',Index_var[Index_var_impute[i]]))
        mod_rf_temp=h2o.randomForest(x = Index_var_full,
                                y = Index_var_impute[i],
                                training_frame = InputData_h2o,nfolds=10,
                                fold_assignment="Modulo",seed=271828,keep_cross_validation_predictions = TRUE,
                                ntrees=100,max_depth = 9,nbins = 20,nbins_cats = 449,sample_rate = 0.41536)
        Model_TempDir <- h2o.saveModel(mod_rf_temp,force = TRUE,TempDir)
        mod_rf <- h2o.getModel(mod_rf_temp@model_id)
      }
      
      ## imputation prediction
      Temp<-h2o.predict(mod_rf,newdata=InputData_h2o)$predict
      TempIndex = which(is.na(InputData[,Index_var_impute[i]]))
      InputData_h2o[TempIndex,Index_var_impute[i]] = Temp[TempIndex]
      # h2o.rm(Temp)
      # h2o.rm(mod_rf)
    }
    
  }else if("prediction" == OPTION)
  {
    ImputeList = list.dirs(path = paste0(DirPath,"processed_data",Sep,"TempData",Sep),full.names = FALSE)
    for(i in 1:length(ImputeList))
    {
      if(grepl(VariableID,ImputeList[i]))
      {
        TempVar = gsub("RFimputation_","",gsub(paste0("_",VariableID),"",ImputeList[i]))
        
        cat(sprintf('making imputation prediction...%s\n',TempVar))
        Model <- list.files(paste0(DirPath,"processed_data",Sep,"TempData",Sep,ImputeList[i],Sep))
        h2o.loadModel(paste0(DirPath,"processed_data",Sep,"TempData",Sep,ImputeList[i],Sep,Model[1]))
        mod_rf <- h2o.getModel(Model[1])
        
        ## imputation prediction
        Temp<-h2o.predict(mod_rf,newdata=InputData_h2o)$predict
        TempIndex = which(is.na(InputData[,TempVar]))
        if(length(TempIndex)>0)
        {
          InputData_h2o[TempIndex,TempVar] = Temp[TempIndex]
        }
        # h2o.rm(Temp)
        # h2o.rm(mod_rf)
      }
    }
  }
  return(InputData_h2o)
}





