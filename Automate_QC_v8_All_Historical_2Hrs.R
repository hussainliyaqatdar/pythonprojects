#setwd("~/DataPipelineQC")
require(httr)
require(RMySQL)
require(RPostgreSQL)
require(plyr)
require(rJava)
require(mailR)
require(knitr)
require(tableHTML)
require(pander)
require(lubridate)



#--------- Creating MySQl Connection
user = ''
password = ''
dbname = ""
host = ""
port =3306
mydb=dbConnect(MySQL(),dbname = dbname,user =user,password = password,
               host = host,port = port)

#-----------Creating Redshift Connection
user_red = ''
password_red = ''
dbname_red = ""
host_red = ""
port_red =5439
redshiftdb=dbConnect(PostgreSQL(),dbname = dbname_red,user =user_red,password = password_red,
                     host = host_red,port = port_red)

#Fetch Table Names from Cron Time Table
get_all_tables_Query <- c("select distinct name from d11.cron_timetable 
                          where name not in ('Dream11_PlayerMatchPointsBreakupFootball'
                          ,'Dream11_PlayerMatchPointsBreakupCricket'
                          ,'Dream11_PlayerMatchPointsBreakupKabaddi','Dream11_PlayerMatchPointsBreakupRugby','Dream11_ContestMaster','Dream11_ContestWLSRelation','Dream11_ContestUserRelation')")
all_table_list <- as.data.frame(dbGetQuery(redshiftdb,get_all_tables_Query))

#This Function does standard checks through One Table by taking two List of tables
compare_datasets <- function(x,z,cur_min_7,cur) {
  for(i  in 1: nrow(x) ) {
    # -------Querying for Count in MYSQL
    tryCatch({table1 <- c()
    mysqlquery <- c()
    MySQL_Data <- data.frame()
    table1 <- x[i,1]
    print(table1)
    mysqlquery <- paste("select date(",z,"),count(*),count(distinct id) from dbd11live29june15.",table1," where ",z," <= '",cur,"' and date(",z,") <> '0000-00-00' group by 1",sep="")
    MySQL_Data <- dbGetQuery(mydb,mysqlquery)
    print(paste0("MySQL query executed for ",table1))
    colnames(MySQL_Data) <- c('Date','Count_s_mysql','Count_distinct_mysql')
    
    # ---------Querying for Count in Redshift
    redshiftquery <- c()
    RED_Data <- data.frame()
    redshiftquery <- paste("select date(",z,"),count(*),count(distinct id) from d11.",table1," where ",z," <= '",cur,"' group by 1",sep="")
    RED_Data <- dbGetQuery(redshiftdb,redshiftquery)
    print(paste0("Redshift query executed for ",table1))
    if(nrow(RED_Data) != 0 ) {colnames(RED_Data) <- c('Date','Count_s_red','Count_distinct_red')}
    text0 <-data.frame()
    text0<- as.data.frame(paste(table1," Checked on column ",z))
    names(text0) <- c("message")
    callouts <- data.frame()
    #---------Creating a Merged dataset with MySQL Data as the Left Table
    if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
      merged_dataset <- data.frame()
      merged_dataset <- join(MySQL_Data,RED_Data,by = "Date") 
      #-----------Iterating Through the Merged DataSet for QC
      for(j in 1:nrow(merged_dataset)) {
        text<-data.frame()
        text1<-data.frame()
        text2<-data.frame()
        if(is.na(merged_dataset$Count_distinct_red[j])) {
          text<-as.data.frame(paste("Missing Data for",merged_dataset$Date[j],"in Redshift table"))
          names(text) <- c("message")
        }
        else if(merged_dataset$Count_distinct_mysql[j] > merged_dataset$Count_distinct_red[j] && merged_dataset$Count_distinct_mysql[j] != merged_dataset$Count_distinct_red[j]) {
          text1<-as.data.frame.character(paste("Count Mismatch of",merged_dataset[j,3]- merged_dataset[j,5],"rows on",merged_dataset$Date[j],"in Redshift table "))
          names(text1) <- c("message")
        }
        else if(merged_dataset$Count_distinct_mysql[j] >= merged_dataset$Count_distinct_red[j] && merged_dataset$Count_s_red[j] != merged_dataset$Count_distinct_red[j]) {
          text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_dataset$Date[j],"in Redshift table "))
          names(text2) <- c("message")
          
        }
        callouts <- rbind(callouts,text,text1,text2)
      }
      
    }
    else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
      text3<-as.data.frame(paste("There is no data for the past seven days in Redshift table "))
      names(text3) <- c("message")
      callouts <- rbind(callouts,text3)
    }
    else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
      callouts <- rbind(callouts)
    } 
    if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
    },error= function(e){print(paste("An error occured while execution for:",table1))
      local_error <- as.data.frame(paste(table1,":",e))
      names(local_error) <- c("error")
      error_message <<-rbind(error_message,local_error)}
    , finally = {next})
  }
}


# Initialising Empty Message Data Frame 
message<-data.frame()
error_message<- data.frame()
# ---- Running Functions for Performing Checks ------------

# Start the clock!
ptm <- proc.time()
current_ts<- format(as.POSIXlt(Sys.time(),tz= "GMT")- hours(2), '%Y-%m-%d %H:%M:%S')
current_ts_minus_7<-format(as.POSIXlt(Sys.time(),tz= "GMT")- hours(2) -days(7), '%Y-%m-%d %H:%M:%S') 

compare_datasets(all_table_list,c("recupdatedat"),current_ts_minus_7,current_ts)


#Replace NAs with Blanks

message <- sapply(message,as.character)
message[is.na(message)] <- ""
message <- as.data.frame(message)

error_message <- sapply(error_message,as.character)
error_message[is.na(error_message)] <- ""
error_message <- as.data.frame(error_message)

#Send an email if there are any callouts
if(nrow(message) >0){
  currentDate <<- Sys.Date()
  #write.csv(message,paste0("Data_QC_Alert_",table2,"_",currentDate,".csv"))
  # Trigger a Mail Alert Here
  send.mail(from = "fantasycricket@dream11.com",
            #to = c("hussain@dream11.com","neha@dream11.com","dhanraj@dream11.com","aditya@dream11.com"),
            to = c("datateam@dream11.com"),
            subject = paste0("Kafka Data Pipeline Alerts for QC on ",currentDate),
            body = tableHTML(message),
            smtp = list(host.name = "smtp.sendgrid.net", port = 465, user.name = "apikey",
                        passwd = "", ssl = TRUE),
            html = TRUE,
            authenticate = TRUE,
            send = TRUE,
            #attach.files = c(paste0("Data_QC_Alert_",table2,"_",currentDate,".csv")),
            #file.names = c(paste0("Data_QC_Alert_",table2,"_",currentDate,".csv")), # optional parameter
            #file.descriptions = c("Description for download log", "Description for upload log", "DropBox File"), # optional parameter
            debug = TRUE)}



if(nrow(error_message) >0){
  currentDate <<- Sys.Date() 
  #write.csv(message,paste0("Data_QC_Alert_",table2,"_",currentDate,".csv"))
  # Trigger a Mail Alert Here
  send.mail(from = "fantasycricket@dream11.com",
            #to = c("hussain@dream11.com","neha@dream11.com","dhanraj@dream11.com","aditya@dream11.com"),
            to = c("datateam@dream11.com"),
            subject = paste0("Error Alerts for QC script on ",currentDate),
            body = tableHTML(error_message),
            smtp = list(host.name = "smtp.sendgrid.net", port = 465, user.name = "apikey",
                        passwd = "", ssl = TRUE),
            html = TRUE,
            authenticate = TRUE,
            send = TRUE,
            #attach.files = c(paste0("Data_QC_Alert_",table2,"_",currentDate,".csv")),
            #file.names = c(paste0("Data_QC_Alert_",table2,"_",currentDate,".csv")), # optional parameter
            #file.descriptions = c("Description for download log", "Description for upload log", "DropBox File"), # optional parameter
            debug = TRUE)}

dbDisconnect(mydb)
dbDisconnect(redshiftdb)
# Stop the clock
proc.time() - ptm