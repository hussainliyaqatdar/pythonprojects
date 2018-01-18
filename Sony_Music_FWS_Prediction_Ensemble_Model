options(expressions=1e5)

#start

wk1<-read.csv("week5_test.csv")
wk1t<-read.csv("t_week5.csv")
a<-combinations(nrow(wk1t) , 4 , as.character(wk1t$IDV))
a1<-as.data.frame(a)
rejected_comb < data.frame()
r<-0     
master_comb<- data.frame()
lin_cook<-c()
for(j in 1:nrow(a1))
{
  lin<-lm(as.formula(paste("first_week_sale~",paste(as.character(a[j,1]),as.character(a[j,2]),as.character(a[j,3]),as.character(a[j,4]),sep="+"),sep=""))
          ,wk1)
  
  cd<-cooks.distance(lin)
  
  dset<-cbind(wk1,cd)
  
  nxtdset<-dset[abs(dset$cd)<= (4/nrow(dset)),]
  nxtdset <- nxtdset[,1:(ncol(nxtdset)-1)]
  
  outliers<-dset[abs(dset$cd)>= (4/nrow(dset)),]
  
  out <- paste0(outliers$Project_ID, collapse="$")
  
  for(i in 1:5){
    try({
      sel_data<-nxtdset[sample(nrow(nxtdset),round(0.8*nrow(nxtdset))),]
      sel_data <- as.data.frame(sel_data)
      train<- sel_data
      test<-nxtdset[-(1:nrow(train)),]
      
      trm<-lm(as.formula(paste("first_week_sale~",paste(as.character(a[j,1]),as.character(a[j,2]),as.character(a[j,3]),as.character(a[j,4]),sep="+"),sep="")),train)
      
      pvalue<-paste0(summary(trm)$coeff[,4],collapse="$")
      vif_val <- paste0(vif(trm), collapse="$")
      coeff <- paste0(trm$coeff , collapse="$")
      rsq <- paste0(summary(trm)$r.squared , collapse="$")
      projID <- paste0(test$Project_ID , collapse = "$")
      fws <- paste0(test$first_week_sale, collapse = "$")
      pfws <- paste0(predict(trm,test), collapse = "$")
      
      #if(max(vif(trm)) < 10 & max(summary(trm)$coeff[,4]) < 0.1){
      
      mape<-mean(abs((predict(trm,test)-test$first_week_sale)/predict(trm,test)))
      
      mse <- mean((predict(trm,test)-test$first_week_sale)^2)
      
      
      #test$Project_Id
      #train$first_week_sale
      tst<-cbind("5",toString(paste(as.character(a[j,1]),as.character(a[j,2]),as.character(a[j,3])
                                    ,as.character(a[j,4]),sep=","))
                 ,i, pvalue ,vif_val,coeff
                 , rsq ,projID, fws ,pfws
                 ,mape, out , mse)
      
      
      
      master_comb<-rbind(master_comb, tst )
      #}
      
      print((((j-1)*5)+i)*100/(nrow(a)*5))
      
      gc()
    },silent=TRUE)
  }
}
#end
