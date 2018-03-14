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
    
    # Fetch Table Names from Cron Time Table
    get_all_tables_Query <- c("select distinct name from d11.cron_timetable 
                              where name in ('Dream11_UWTransactions'
                              ,'Dream11_LeagueMaster'
                              ,'Dream11_LeagueUserRelation','Dream11_UserLeagueBreakup','Dream11_UWPaymentTransStatus'
                              ,'Dream11_LeagueWLSRelation','Dream11_UWWithdrawRequest','Dream11_UWMaster')")
    
    all_table_list <- as.data.frame(dbGetQuery(redshiftdb,get_all_tables_Query))
    cur_min_2<- format(as.POSIXlt(Sys.time(),tz= "GMT")- hours(1), '%Y-%m-%d %H:%M:%S')
    cur_min_3<-format(as.POSIXlt(Sys.time(),tz= "GMT")- hours(2) , '%Y-%m-%d %H:%M:%S') 
    cur_min_24<- format(as.POSIXlt(Sys.time(),tz= "GMT")- hours(24), '%Y-%m-%d %H:%M:%S')
    
    compare_datasets_league_master <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,round(sum(entryfee),2) as EF,sum(currentsize) as Current_Size from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1 order by 1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,round(sum(entryfee),2) as EF,sum(currentsize) as Current_Size  from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1 order by 1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x="Minofhour",by.y="minofhour")
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,3]- merged_data[j,7],"rows for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,,"in Redshift table "))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$EF[j] > merged_data$ef[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,4]- merged_data[j,8],"EntryFee for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$Current_Size[j] > merged_data$current_size[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,9],"Current_Size for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text5<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table "))
        names(text5) <- c("message")
        callouts <- rbind(callouts,text5)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
    },error= function(e){print(paste("An error occured while execution for:",table1))
      local_error <- as.data.frame(paste(table1,":",e))
      names(local_error) <- c("error")
      error_message <<-rbind(error_message,local_error)})
    }

    compare_datasets_LeagueUserRelation <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct userid) as Users ,count(distinct leagueid) as LeaguesJoined ,round(sum(amountwoninr),2) as AmountwonInr,round(sum(amountwoninraftertax),2) as AmountwonInraftertax from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1 order by 1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct userid) as Users ,count(distinct leagueid) as LeaguesJoined ,round(sum(amountwoninr),2) as AmountwonInr,round(sum(amountwoninraftertax),2) as AmountwonInraftertax  from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1 order by 1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x="Minofhour",by.y="minofhour")
        j=1
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          text5<-data.frame()
          text6<-data.frame()
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for",merged_data$minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,3]-merged_data[j,9],"rows for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$Users[j] > merged_data$users[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,4]- merged_data[j,10],"Users for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$LeaguesJoined[j] > merged_data$leaguesjoined[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,11],"Leagues_joined for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          else if(merged_data$AmountwonInr[j] > merged_data$amountwoninr[j]) 
          {
            text5<-as.data.frame(paste("Difference of",merged_data[j,6]- merged_data[j,12],"AmountwonInr for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text5) <- c("message")
          }
          else if(merged_data$AmountwonInraftertax[j] > merged_data$amountwoninraftertax[j]) 
          {
            text6<-as.data.frame(paste("Difference of",merged_data[j,7]- merged_data[j,13],"AmountwonInraftertax for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text6) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4,text5,text6)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text7<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table"))
        names(text7) <- c("message")
        callouts <- rbind(callouts,text7)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
    },error= function(e){print(paste("An error occured while execution for:",table1))
      local_error <- as.data.frame(paste(table1,":",e))
      names(local_error) <- c("error")
      error_message <<-rbind(error_message,local_error)})
    }
    # table1 <- c("Dream11_UserLeagueBreakup")
    # z<- c("recupdatedat")
    compare_datasets_UserLeagueBreakup <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct leagueid) as LeaguesJoined,count(distinct userid) as Users,round(sum(amount),2) as EF,round(sum(cashbonusbalance),2) as CB_Balance,round(sum(depositbalance),2) as Dep_Balance,round(sum(winningsbalance),2) as Win_Balance from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1 order by 1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct leagueid) as LeaguesJoined,count(distinct userid) as Users,round(sum(amount),2) as EF,round(sum(cashbonusbalance),2) as CB_Balance,round(sum(depositbalance),2) as Dep_Balance,round(sum(winningsbalance),2) as Win_Balance from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1 order by 1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x="Minofhour",by.y="minofhour")
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          text5<-data.frame()
          text6<-data.frame()
          text7<-data.frame()
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for",merged_data$minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,3]- merged_data[j,11],"rows for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,,"in Redshift table "))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$Users[j] > merged_data$users[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,13],"Users for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$LeaguesJoined[j] > merged_data$leaguesjoined[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,4]- merged_data[j,12],"Leagues_joined for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          else if(merged_data$CB_Balance[j] > merged_data$cb_balance[j]) 
          {
            text5<-as.data.frame(paste("Difference of",merged_data[j,7]- merged_data[j,15],"Cashbonus Balance for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text5) <- c("message")
          }
          else if(merged_data$Dep_Balance[j] > merged_data$dep_balance[j]) 
          {
            text6<-as.data.frame(paste("Difference of",merged_data[j,8]- merged_data[j,16],"Deposit Balance for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text6) <- c("message")
          }
          else if(merged_data$Win_Balance[j] > merged_data$win_balance[j]) 
          {
            text6<-as.data.frame(paste("Difference of",merged_data[j,9]- merged_data[j,17],"Winnings Balance for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text6) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4,text5,text6)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text7<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table"))
        names(text7) <- c("message")
        callouts <- rbind(callouts,text7)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
      },error= function(e){print(paste("An error occured while execution for:",table1))
      local_error <- as.data.frame(paste(table1,":",e))
      names(local_error) <- c("error")
      error_message <<-rbind(error_message,local_error)})
    }
    compare_datasets_UWTransactions <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,transtype as Transtype,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct userid) as Users, round(sum(amount),2) as EF from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1,2 order by 2,1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,transtype as Transtype,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct userid) as Users, round(sum(amount),2) as EF from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1,2 order by 2,1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x=c("Transtype","Minofhour"),by.y=c("transtype","minofhour"))
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          text7<-data.frame()
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for transype",merged_data$Transtype[j],"for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,4]- merged_data[j,8],"rows transype",merged_data$Transtype[j],"for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,,"in Redshift table "))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for transype",merged_data$Transtype[j],"for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$Users[j] > merged_data$users[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,9],"Users for transype",merged_data$Transtype[j],"for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$EF[j] > merged_data$ef[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,6]- merged_data[j,10],"Entry Fees for transype",merged_data$Transtype[j],"for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text7<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table"))
        names(text7) <- c("message")
        callouts <- rbind(callouts,text7)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
    },error= function(e){print(paste("An error occured while execution for:",table1))
      local_error <- as.data.frame(paste(table1,":",e))
      names(local_error) <- c("error")
      error_message <<-rbind(error_message,local_error)})
    }
    
    
    compare_datasets_LeagueWLSRelation <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct leagueid) as Leagues,round(sum(leagueamount),2) as LeagueAmount,round(sum(servicefee),2) as ServiceFee from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1 order by 1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct leagueid) as Leagues,round(sum(leagueamount),2) as LeagueAmount,round(sum(servicefee),2) as ServiceFee from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1 order by 1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x=c("Minofhour"),by.y=c("minofhour"))
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          text5<-data.frame()
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for",merged_data$minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,3]- merged_data[j,8],"rows for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,,"in Redshift table "))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$Leagues[j] > merged_data$leagues[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,4]- merged_data[j,9],"Leagues for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$LeagueAmount[j] > merged_data$leagueamount[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,10],"LeagueAmount for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          else if(merged_data$ServiceFee[j] > merged_data$servicefee[j]) 
          {
            text5<-as.data.frame(paste("Difference of",merged_data[j,6]- merged_data[j,11],"ServiceFee for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text5) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4,text5)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text7<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table"))
        names(text7) <- c("message")
        callouts <- rbind(callouts,text7)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
        },error= function(e){print(paste("An error occured while execution for:",table1))
          local_error <- as.data.frame(paste(table1,":",e))
          names(local_error) <- c("error")
          error_message <<-rbind(error_message,local_error)})
    }
    # table1 <- c("Dream11_UWWithdrawRequest")
    # z<- c("recupdatedat")
    compare_datasets_UWWithdrawRequest <- function(table1,z,cur_min_2,cur_min_3) {
      tryCatch(
        {
      #------ Initialising Vectors
      print(table1)
      mysqlquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,Status as Status , Count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct transactionid) as Txns, count(distinct userid) as Users,round(sum(amount),2) as Amount from dbd11live29june15.",table1," where ",z," between '",cur_min_3 , "' and '" ,cur_min_2, "' and date(",z,") <> '0000-00-00' group by 1,2 order by 2,1",sep="")
      redshiftquery <- paste("select EXTRACT(minute from ",z,") as Minofhour,Status as Status , Count(*) as Count_S,count(distinct id) as Count_Dist,count(distinct transactionid) as Txns, count(distinct userid) as Users,round(sum(amount),2) as Amount from d11.",table1," where ",z," between '",cur_min_3 ,"' and  '",cur_min_2 ,"' group by 1,2 order by 2,1",sep="")
      MySQL_Data <- as.data.frame(dbGetQuery(mydb,mysqlquery))
      print(paste0("MySQL query executed for ",table1))
      RED_Data <- as.data.frame(dbGetQuery(redshiftdb,redshiftquery))
      print(paste0("Redshift query executed for ",table1))
      text0 <-data.frame()
      text0<- as.data.frame(paste(table1," Checked on column ",z))
      names(text0) <- c("message")
      callouts <- data.frame()
      if(nrow(RED_Data) != 0 && nrow(MySQL_Data) != 0) {
        merged_data = merge.data.frame(x=MySQL_Data,y=RED_Data,by.x=c("Status","Minofhour"),by.y=c("status","minofhour"))
        for (j in 1:nrow(merged_data)){
          text<-data.frame()
          text1<-data.frame()
          text2<-data.frame()
          text3<-data.frame()
          text4<-data.frame()
          j=1
          if(is.na(merged_data$count_dist[j])) {
            text<-as.data.frame(paste("Missing Data for",merged_data$minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table"))
            names(text) <- c("message")
          }
          else if(merged_data$Count_Dist[j] > merged_data$count_dist[j] && merged_data$Count_Dist[j] != merged_data$count_dist[j]) {
            text1<-as.data.frame(paste("Count Mismatch of",merged_data[j,4]- merged_data[j,9],"rows for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,,"in Redshift table "))
            names(text1) <- c("message")
          }
          else if(merged_data$Count_Dist[j] >= merged_data$count_dist[j] && merged_data$count_s[j] != merged_data$count_dist[j]) 
          {
            text2<-as.data.frame(paste("Duplicate entries in Redshift for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text2) <- c("message")
          }
          else if(merged_data$Txns[j] > merged_data$txns[j]) 
          {
            text3<-as.data.frame(paste("Difference of",merged_data[j,5]- merged_data[j,10],"Txns for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text3) <- c("message")
          }
          else if(merged_data$Users[j] > merged_data$users[j]) 
          {
            text4<-as.data.frame(paste("Difference of",merged_data[j,6]- merged_data[j,11],"Users for",merged_data$Minofhour[j],"th minute between",cur_min_3,"and",cur_min_2,"in Redshift table "))
            names(text4) <- c("message")
          }
          callouts <- rbind(callouts,text,text1,text3,text4)}
      }
      else if(nrow(MySQL_Data) !=0 && nrow(RED_Data) == 0){ 
        text7<-as.data.frame(paste("There is no data between",cur_min_3,"and",cur_min_2,"in Redshift table"))
        names(text7) <- c("message")
        callouts <- rbind(callouts,text7)
      }
      else if(nrow(MySQL_Data) ==0 && nrow(RED_Data) == 0){ 
        callouts <- rbind(callouts)
      } 
      if(nrow(callouts)>0 ) { message <<- rbind(message,text0,callouts,"") }
        },error= function(e){print(paste("An error occured while execution for:",table1))
          local_error <- as.data.frame(paste(table1,":",e))
          names(local_error) <- c("error")
          error_message <<-rbind(error_message,local_error)})
    }
    error_message<- data.frame()
    message<-data.frame()
    compare_datasets_league_master(c("Dream11_LeagueMaster"),c("recupdatedat"),cur_min_2,cur_min_3)
    compare_datasets_LeagueUserRelation(c("Dream11_LeagueUserRelation"),c("recupdatedat"),cur_min_2,cur_min_3)
    compare_datasets_UserLeagueBreakup(c("Dream11_UserLeagueBreakup"),c("recupdatedat"),cur_min_2,cur_min_3)
    compare_datasets_UWTransactions(c("Dream11_UWTransactions"),c("recupdatedat"),cur_min_2,cur_min_3)
    compare_datasets_LeagueWLSRelation(c("Dream11_LeagueWLSRelation"),c("recupdatedat"),cur_min_2,cur_min_3)
    compare_datasets_UWWithdrawRequest (c("Dream11_UWWithdrawRequest"),c("recupdatedat"),cur_min_2,cur_min_3)

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
  # Trigger a Mail Alert Here
  send.mail(from = "fantasycricket@dream11.com",
            #to = c("hussain@dream11.com","neha@dream11.com","dhanraj@dream11.com","aditya@dream11.com"),
            to = c("datateam@dream11.com"),
            subject = paste0("QC Alerts for Major Tables(Past Hour Data) on ",currentDate),
            body = tableHTML(message),
            smtp = list(host.name = "smtp.sendgrid.net", port = 465, user.name = "apikey",
                        passwd = "", ssl = TRUE),
            html = TRUE,
            authenticate = TRUE,
            send = TRUE,
            debug = TRUE)}

if(nrow(error_message) >0){
  currentDate <<- Sys.Date() 
  # Trigger a Mail Alert Here
  send.mail(from = "fantasycricket@dream11.com",
            #to = c("hussain@dream11.com","neha@dream11.com","dhanraj@dream11.com","aditya@dream11.com"),
            to = c("datateam@dream11.com"),
            subject = paste0("Error Alerts for Major tables(Past Hour Data) QC script on ",currentDate),
            body = tableHTML(error_message),
            smtp = list(host.name = "smtp.sendgrid.net", port = 465, user.name = "apikey",
                        passwd = "", ssl = TRUE),
            html = TRUE,
            authenticate = TRUE,
            send = TRUE,
            debug = TRUE)}
dbDisconnect(mydb)
dbDisconnect(redshiftdb)


