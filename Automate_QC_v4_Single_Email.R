setwd("~/DataPipelineQC")
require(RMySQL)
require(RPostgreSQL)
require(plyr)
require(rJava)
require(mailR)
require(knitr)
require(tableHTML)
require(pander)


#--------- Creating MySQl Connection
user = ''
password = ''
dbname = ""
host = ""
port =
mydb=dbConnect(MySQL(),dbname = dbname,user =user,password = password,
               host = host,port = port)

#-----------Creating Redshift Connection
user_red = ''
password_red = ''
dbname_red = ""
host_red = ""
port_red =
redshiftdb=dbConnect(PostgreSQL(),dbname = dbname_red,user =user_red,password = password_red,
                     host = host_red,port = port_red)

#This Function does standard checks through One Table by taking two List of tables
compare_datasets <- function(x,z) {
  for(i  in 1: length(x) ) {
    # -------Querying for Count in MYSQL
    table1 <- c()
    mysqlquery <- c()
    MySQL_Data <- data.frame()
    table1 <- x[i]
    print(table1)
    mysqlquery <- paste("select date(",z,"),count(*),count(distinct id) from dbd11live29june15.",table1," where ",z," between DATE_FORMAT(CURDATE() - INTERVAL 7 DAY ,'%Y-%m-%d %00:%00:%00')  and DATE_FORMAT(NOW()-INTERVAL 2 HOUR,'%Y-%m-%d %h:%00:%00') and date(",z,") <> '0000-00-00' group by 1",sep="")
    MySQL_Data <- dbGetQuery(mydb,mysqlquery)
    print(paste0("MySQL query executed for ",table1))
    colnames(MySQL_Data) <- c('Date','Count_s_mysql','Count_distinct_mysql')
    
    # ---------Querying for Count in Redshift
    redshiftquery <- c()
    RED_Data <- data.frame()
    redshiftquery <- paste("select date(",z,"),count(*),count(distinct id) from d11.",table1," where ",z," between TO_TIMESTAMP(CAST(CURRENT_DATE-INTERVAL '7 Days' as TIMESTAMP),'YYYY-MM-DD HH:00:00') and  TO_TIMESTAMP(CAST(CURRENT_TIMESTAMP-INTERVAL '2 Hours' as TIMESTAMP),'YYYY-MM-DD HH:00:00') group by 1",sep="")
    RED_Data <- dbGetQuery(redshiftdb,redshiftquery)
    print(paste0("Redshift query executed for ",table1))
    if(nrow(RED_Data) != 0 ) {colnames(RED_Data) <- c('Date','Count_s_red','Count_distinct_red')}
    
    #---------Creating a Merged dataset with MySQL Data as the Left Table
    if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0){
      merged_dataset <- data.frame()
      merged_dataset <- join(MySQL_Data,RED_Data,by = "Date")
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      message <<- rbind(message,text0)
      #-----------Iterating Through the Merged DataSet for QC
      for(j in 1:nrow(merged_dataset)) {
        text<-data.frame()
        text1<-data.frame()
        text2<-data.frame()
        if(is.na(merged_dataset$Count_distinct_red[j])) {
          text<-as.data.frame(paste("Missing Data for",merged_dataset$Date[j],"in Redshift table"))
          names(text) <- c("message")
        }
        else if(merged_dataset$Count_distinct_mysql[j] != merged_dataset$Count_distinct_red[j]) {
          text1<-as.data.frame.character(paste("Count Mismatch of",merged_dataset[j,3]- merged_dataset[j,5],"rows on",merged_dataset$Date[j],"in Redshift table "))
          names(text1) <- c("message")
        }
        else if(merged_dataset$Count_s_red[j] != merged_dataset$Count_distinct_red[j]) {
          text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_dataset$Date[j],"in Redshift table "))
          names(text2) <- c("message")
          
        }
        message <<- rbind(message,text,text1,text2)
      }
    }
    else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
      text3<-as.data.frame(paste("There is no data for the past seven days in Redshift table "))
      names(text3) <- c("message")
      message <<- rbind(message,text3)
    }
    else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
      message <<- rbind(message)
    } 
    
  }
  
}

compare_datasets_recupdatedat <- function(x,mysql_querylist,red_querylist,z) {
  for(i  in 1: length(x) ) {
    # -------Querying for Count in MYSQL
    table1 <- c()
    my_query <-c()
    red_query <- c()
    id <-c()
    MySQL_Data <- data.frame()
    table1 <- x[i]
    my_query <- mysql_querylist[i]
    red_query <- red_querylist[i]
    id <- z[i]
    print(table1)
    MySQL_Data <- dbGetQuery(mydb,my_query)
    print(paste0("MySQL query executed for ",table1))
    colnames(MySQL_Data) <- c('ID','Count_mysql')
    
    # ---------Querying for Count in Redshift
    RED_Data <- data.frame()
    RED_Data <- dbGetQuery(redshiftdb,red_query)
    print(paste0("Redshift query executed for ",table1))
    if(nrow(RED_Data) != 0 ) {colnames(RED_Data) <- c('ID','Count_red')}
    
    #---------Creating a Merged dataset with MySQL Data as the Left Table
    if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0){
      merged_dataset <- data.frame()
      merged_dataset <- join(MySQL_Data,RED_Data,by = "ID")}
    text0 <-data.frame()
    text0 <- as.data.frame(paste(table1," Checked on column ",id))
    names(text0) <- c("message")
    message <<- rbind(message,text0)
    #-----------Iterating Through the Merged DataSet for QC
    for(j in 1:nrow(merged_dataset)) {
      text<-data.frame()
      text1<-data.frame()
      if(is.na(merged_dataset$Count_red[j])) {
        text<-as.data.frame(paste("Missing Data for",id,"-",merged_dataset$ID[j],"in Redshift table ",table1))
        names(text) <- c("message")
      }
      else if(merged_dataset$Count_mysql[j] != merged_dataset$Count_red[j]) {
        text1<-as.data.frame.character(paste("Count Mismatch of",merged_dataset[j,2]- merged_dataset[j,3],"rows for",merged_dataset$ID[j],"in Redshift table ",table1))
        names(text1) <- c("message")
      }
      
      message <<- rbind(message,text,text1)
    }
  }
  
} 


#--------------------Creating Vectors with Table Names----------------
table_list_lur <- c("Dream11_LeagueUserRelation")
table_list_createdat <- c('Dream11_UWMaster',
                          'Dream11_UWTransactions',
                          'Dream11_UserLeagueBreakup',
                          'Dream11_LeagueMaster',
                          'Dream11_UserRoundTeamRelationFinal',
                          'Dream11_MoneyBackUserDetails',
                          'Dream11_OneSignalUsers',
                          'Dream11_RoundMaster',
                          'Dream11_LeagueWinnersMaster',
                          'Dream11_LeagueWinnersTemplate',
                          'Dream11_GameSquadPlayerAttributeMaster',
                          'Dream11_GameSquadPlayerTypeMaster',
                          'Dream11_MatchMaster',
                          'Dream11_SquadMaster',
                          'Dream11_SquadPlayerMaster',
                          'Dream11_TourMaster',
                          'Dream11_AvatarMaster',
                          'Dream11_GameMaster',
                          'Dream11_IndexFootersHtml')

table_list_createdon <- c('Dream11_LeagueWLSRelation',
                          'Dream11_UserReferralMap',
                          'Dream11_PlayerMatchPointsBreakupCricketComplete',
                          'Dream11_PlayerMatchPointsBreakupFootballComplete',
                          'Dream11_PlayerMatchPointsBreakupKabaddiComplete',
                          'Dream11_PlayerMatchPointsBreakupRugbyComplete'
)

table_list_recupdatedat <- c(
  'Dream11_ProductDetails',
  'Dream11_StateMaster',
  'Dream11_RoundWLSRelation',
  'Dream11_CountryMaster',
  'Dream11_CS_Settings',
  'Dream11_IndexFootersHtml',
  'Dream11_LastUpdatedId',
  'Dream11_RoundMatchRelation')

mysql_querylist <-c('select id,count(id) from Dream11_ProductDetails group by 1 ',
                    'select countryid,count(id) from Dream11_StateMaster group by 1',
                    'select roundid,count(id) from Dream11_RoundWLSRelation group by 1',
                    'select countryCode , count(id) from Dream11_CountryMaster group by 1',
                    'select type,count(settings) from Dream11_CS_Settings group by 1',
                    'select WLSId,count(id) from Dream11_IndexFootersHtml group by 1',
                    'select processName,count(last_id) from Dream11_LastUpdatedId group by 1',
                    'select RoundId,count(MatchId) from Dream11_RoundMatchRelation group by 1'
)

redshift_querylist <-c('select id,count(id) from d11.Dream11_ProductDetails group by 1 ',
                       'select countryid,count(id) from d11.Dream11_StateMaster group by 1',
                       'select roundid,count(id) from d11.Dream11_RoundWLSRelation group by 1',
                       'select countryCode , count(id) from d11.Dream11_CountryMaster group by 1',
                       'select type,count(settings) from d11.Dream11_CS_Settings group by 1',
                       'select WLSId,count(id) from d11.Dream11_IndexFootersHtml group by 1',
                       'select processName,count(last_id) from d11.Dream11_LastUpdatedId group by 1',
                       'select RoundId,count(MatchId) from d11.Dream11_RoundMatchRelation group by 1'
)
Id_List <- (c('id','countryid','roundid','countryCode','type','WLSId','processName','RoundId'))



# Initialising Empty Message Data Frame 
message<-data.frame()

# ---- Running Functions for Performing Checks ------------

# Start the clock!
ptm <- proc.time()
compare_datasets_recupdatedat(table_list_recupdatedat,mysql_querylist,redshift_querylist,Id_List)
compare_datasets(table_list_lur,c("leaguejoindate"))
compare_datasets(table_list_createdat,c("createdat"))
compare_datasets(table_list_createdon,c("createdon"))
compare_datasets(c("Dream11_UserRegistration"),c("createddate"))
compare_datasets(c("Dream11_UWPaymentTransStatus"),c("reqtransactiondate"))
compare_datasets(c("Dream11_UWWithdrawalUserInfo"),c("InsertedOn"))
compare_datasets(c("Dream11_UserAttributes"),c("DepositDate"))
compare_datasets(c("Dream11_PaymentOptions"),c("DateOfAdd"))
compare_datasets(c("Dream11_PanVerificationStatusInfo"),c("CreateDate"))
compare_datasets(c("Dream11_RoundSummaryDetails"),c("RoundSummaryDateTime"))
compare_datasets(c("Dream11_UWWithdrawRequest"),c("WithdrawReqOn"))
compare_datasets(c("Dream11_TourMatchTypeMaster"),c("CreatedDate"))
compare_datasets(c("Dream11_UserCouponUsed"),c("UsedDate"))
compare_datasets(c("Dream11_AdminUser"),c("CreatedAt"))
compare_datasets(c("Dream11_AvatarMaster"),c("CreatedAt"))
compare_datasets(c("Dream11_BlacklistedDomains"),c("Created_At"))
compare_datasets(c("Dream11_DreamboxMessages"),c("UpdatedOn"))
compare_datasets(c("Dream11_MasterLogin"),c("LoginTime"))
compare_datasets(c("Dream11_UserCoupon"),c("RedeemedDate"))


# Send an email if there are any callouts
if(nrow(message) >0){
  currentDate <<- Sys.Date() 
  #write.csv(message,paste0("Data_QC_Alert_",table2,"_",currentDate,".csv"))
  # Trigger a Mail Alert Here
  send.mail(from = "test@dataqc.com",
            to = c(""),
            subject = paste0("Kafka Data Pipeline Alerts for QC on ",currentDate),
            body = tableHTML(message),
            smtp = list(host.name = "", port = , user.name = "", passwd = "", ssl = TRUE),
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









