


#filepath = '/opt/env/analysis/nav.json'
#nav = jsonlite::stream_in(file(filepath))
#nav = nav$result[[1]]

#BigBro(df,'fh_0052','fund_sec_pct','10.0.5.107')  
BigBro<-function(nav_json,fund_name,fund_db_name,mongo_ip,factor_long_minimum = 0.3,industry_max_n = 3){

  library(RMySQL)
  library(zoo)
  library(lubridate)
  library(mongolite)
  library(jsonlite)
  nav = fromJSON(nav_json)$result
   #print(nav$nav_acc)
   #print(nav$nav_date)
  #nav = nav_json
  channel=dbConnect(MySQL(),user="mg",   #�û���
                  password="Abcd1234",     #����
                  dbname="fof_ams_dev",       #���ݿ�����
                  host="10.0.3.66",
                  encode = 'utf-8');


  local_w_tdays <- function(start,end){
    wind_trade_date = dbGetQuery(channel,"SELECT * FROM wind_trade_date")[,]
    trimmed_date = wind_trade_date[wind_trade_date<=end & wind_trade_date>=start]
    trimmed_date
  }
  Pct_Change <- function(X,lag = 1,dir = 'past'){
    n = length(X)
    Y = diff(X,lag = lag)/X[1:(n-lag)]
    if(dir == 'future'){
      Y = c(Y,rep(0,lag))
    }
    else if(dir == 'past'){
      Y = c(rep(0,lag),Y)
    }
    Y
  }
  which_mul <- function(A,B){
    n = length(B)
    for(i in 1:n){
      if(i == 1){
        res = which(A == B[i])
      }else{
        res = c(res,which(A == B[i]))
      }
    }
    return(res)
  }
  read_db <- function(dbname,dt = NULL,cond = 'eq',ip = mongo_ip){
    label = is.null(dt)
    if(label){
      query = '{}'
    }else{
      dt = paste0(as.Date(dt)-1,'T16:00:00Z')
      query = paste0('{"DATETIME": { "$',cond,'" : { "$date" :"' ,dt, '"}}}')
      
    }
    db_index_list = c('HS300Comp','ZZ500Comp','SZ50Comp','CSIndustry','CI_Index',
                      'HS300Index','ZZ500Index','SZ50Index',
                      'HS300_Weight','ZZ500_Weight','SZ50_Weight',
                      'HS300_CI_Weight','ZZ500_CI_Weight','SZ50_CI_Weight')
    if(dbname %in% db_index_list){
      url = paste0("mongodb://",ip,'/index')
      con = mongo(dbname,url = url)
      x = con$find(query)
      x$DATETIME = as.character(x$DATETIME)
      rm(con)
      return(x)
    }
    if(dbname == 'StockFactor'){
      url = paste0("mongodb://",ip,'/stock')
      con = mongo('Factor',url = url)
      x = con$find(query)
      x$DATETIME = as.character(x$DATETIME)
      rm(con)
      return(x)
    }
    if(dbname == 'BASIC'){
      url = paste0("mongodb://",ip,'/stock')
      con = mongo('BASIC2',url =  url)
      x = con$find(query)
      x$DATETIME = as.character(x$DATETIME)
      rm(con)
      return(x)
    }
    if(dbname == 'CIList'){
      url = paste0("mongodb://",ip,'/index')
      con = mongo('CSIndustryCodeList',url = url )
      x = con$find()
      rm(con)
      return(x)
    }
    if(dbname == 'IPO'){
      url = paste0("mongodb://",ip,'/stock')
      con = mongo('IPO',url = url)
      x = con$find(query)
      x$DATETIME = as.character(x$DATETIME)
      rm(con)
      return(x)
    }
  }
  portfolio_industry_distribution <- function(x,dt){
    CI = read_db('BASIC',dt)
    CI = as.data.frame(cbind(CI$CODE,CI$CI),stringsAsFactors = F)
    #browser()
    colnames(CI) <- c('CODE','CI')
    colnames(x)[1] <- 'CODE'
    x = merge(x,CI,all.x = T)
    ci = aggregate(x[,2], by=list(CODE=x$CI), FUN=sum)
    
    CIList = read_db('CIList')
    y = merge(CIList,ci,all.x = T)
    colnames(y)[3] = 'WEIGHT'
    y$WEIGHT[is.na(y$WEIGHT)] = 0
    return(y)
  }
  extract_long_earn <- function(df){
    #index_list = unique(df$sec_code[df$sec_type == 1])
    daylist = unique(df$nav_date)
    daily_short_earn = array(0,length(daylist))
    daily_nav = array(0,length(daylist))
    for(i in 1:length(daylist)){
      x = df[df$nav_date == daylist[i] & df$sec_type == 1,names(df) %in% c('nav_date','cost_tot','value_tot')]
      daily_short_earn[i] = sum(x$cost_tot - x$value_tot)
      x1 = df[df$nav_date == daylist[i] ,names(df) %in% c('nav_date','value_tot','value_pct')]
      x1 = x1[x1$value_pct >= 0.002,]
      daily_nav[i]  = median(x1$value_tot/x1$value_pct)
      #daily_nav[i]  = sum(x1$value_tot)/sum(x1$value_pct)
      
    }
    daily_nav = na.locf(zoo(daily_nav),na.rm = F)
    daily_short_earn = c(daily_short_earn[1],diff(daily_short_earn))
    daily_earn = c(0,diff(daily_nav))
    daily_long_earn = (daily_earn - daily_short_earn)
    daily_long_earn_pch = daily_long_earn/daily_nav
    daily_long_earn_pch
  }
  
  df = dbGetQuery(channel,paste0("SELECT * FROM ",fund_db_name))
  df = df[df$wind_code == fund_name,]
  df = df[order(df$nav_date),]
  daylist = intersect(unique(df$nav_date),unique(nav$nav_date))
  ##only use trading days
  daylist1 = as.character(local_w_tdays(head(daylist,1),tail(daylist,1)))
  daylist = intersect(daylist1,daylist)
  
  nav = nav[which_mul(nav$nav_date,daylist),]
  df = df[which_mul(df$nav_date,daylist),]
  nav$pch = Pct_Change(nav$nav_acc,dir = 'past')
  
  
  ##capital exposure analysis
  Capital_exposure = data.frame(matrix(0,length(daylist),5))
  colnames(Capital_exposure) = c('long_value','short_value','long_pct','short_pct','expo_pct')
  for(i in 1:length(daylist)){
    x = df[df$nav_date == daylist[i],]
    Capital_exposure$long_pct[i] = sum(x$value_pct[x$sec_type ==0])
    Capital_exposure$short_pct[i]  = sum(x$value_pct[x$sec_type == 1])
    Capital_exposure$long_value[i] = sum(x$value_tot[x$sec_type == 0])
    Capital_exposure$short_value[i]  = sum(x$value_tot[x$sec_type == 1])
  }
  Capital_exposure$expo_pct = Capital_exposure$long_pct + Capital_exposure$short_pct
  
  
  ##factor analysis
  Factor_score = data.frame(matrix(0,length(daylist),3))
  colnames(Factor_score) = c('momentum','reverse','size')
  for(i in 1:length(daylist)){
    df_factor = read_db('StockFactor',daylist[i])
    df_factor = df_factor[,names(df_factor) %in% c('CODE','size_f','inverse_f_5','momentum_f_20')]
    names(df_factor)[1] = 'sec_code'
    x = df[df$nav_date == daylist[i],]
    x_stock = x[x$sec_type == 0,which(names(x) %in% c('sec_code','value_pct'))]
    x_stock = merge(x_stock,df_factor,all.x = T)
    x_stock[is.na(x_stock)] = 0
    Factor_score$size[i] = x_stock$value_pct %*% x_stock$size_f
    Factor_score$momentum[i] = x_stock$value_pct %*% x_stock$momentum_f_20
    Factor_score$reverse[i] = x_stock$value_pct %*% x_stock$inverse_f_5
  }
  Factor_score$momentum = Factor_score$momentum / Capital_exposure$long_pct
  Factor_score$reverse  = Factor_score$reverse  / Capital_exposure$long_pct
  Factor_score$size     = Factor_score$size     / Capital_exposure$long_pct
  
  ##industry analysis
  CIList = read_db('CIList')
  IF_CI_Weight = read_db('HS300_CI_Weight')
  IC_CI_Weight = read_db('ZZ500_CI_Weight')
  IH_CI_Weight = read_db('SZ50_CI_Weight')
  CI_DIST = matrix(0,length(daylist),nrow(CIList)) ##CI_distribution of long side, un-scaled
  CI_EXPO = matrix(0,length(daylist),nrow(CIList)) ## CI_exposure
  CI_EXPO_EARN = array(0,length(daylist))
  for(i in 1:length(daylist)){
    x = df[df$nav_date == daylist[i],]
    x_stock = x[x$sec_type == 0,which(names(x) %in% c('sec_code','value_pct'))]
    x_index = x[x$sec_type == 1,which(names(x) %in% c('sec_code','value_pct'))]
    x_index$sec_code = substr(x_index$sec_code,1,2)
    
    index_sub =sapply(c('IC','IF','IH'), function(x){which(x_index$sec_code %in% x)})
    index_value_pct = sapply(index_sub,function(x){sum(x_index$value_pct[x])})
    ##index value_oct is un-scaled
    
    x_ci = portfolio_industry_distribution(x_stock,daylist[i])
    CI_DIST[i,] =  x_ci[,3]
    
    IF_CI_Weight_i = IF_CI_Weight[IF_CI_Weight$DATETIME == daylist[i],] #scaled
    IC_CI_Weight_i = IC_CI_Weight[IC_CI_Weight$DATETIME == daylist[i],] #scaled
    IH_CI_Weight_i = IH_CI_Weight[IH_CI_Weight$DATETIME == daylist[i],] #scaled
    Index_CI_Weight_i = -cbind(IC_CI_Weight_i$WEIGHT * index_value_pct[1],
                               IF_CI_Weight_i$WEIGHT * index_value_pct[2],
                               IH_CI_Weight_i$WEIGHT * index_value_pct[3])
    CI_EXPO[i,] = x_ci[,3] -  rowSums(Index_CI_Weight_i)  #both are un-sacled
    
    ci_index = read_db('CI_Index',daylist[i])
    ci_index = ci_index[order(ci_index$CODE),]
    sub_max_ci_expo = which.max(abs(CI_EXPO[i,]))
    sub_max_ci_expo = order(abs(CI_EXPO[i,]))[1:industry_max_n]
    
    CI_EXPO_EARN[i] = ci_index$PCT_CHG[sub_max_ci_expo] %*% CI_EXPO[i,sub_max_ci_expo]
  }
  
  
  ##capital Exposure plot
  hs300 = read_db('HS300Index')
  hs300 = hs300[which_mul(hs300$DATETIME,daylist),]
  
  cor_expo_nav = round(cor(Capital_exposure$expo_pct,nav$pch),3)
  cor_expo_hs300 = round(cor(Capital_exposure$expo_pct,hs300$PCT_CHG),3)
  capital_analysis = list(cor_expo_hs300 = cor_expo_hs300,cor_expo_nav = cor_expo_nav,
                          Capital_exposure = Capital_exposure)
  
  
  
  
  ##factor plot
  Factor_score$momentum = Factor_score$momentum  - 50
  Factor_score$reverse = Factor_score$reverse  - 50
  Factor_score$size = Factor_score$size  - 50
  
  daily_long_earn_pch  = extract_long_earn(df)
  y = (daily_long_earn_pch/Capital_exposure$long_pct)
  y = as.numeric(y)
  Factor_score$y = y
  Factor_score_m = Factor_score
  Factor_score_m[Capital_exposure$long_pct<factor_long_minimum,] = 0
  f = lm(y~momentum+reverse+size,Factor_score_m)
  summary_f = summary(f)
  f_list = list(f_coef = f$coefficients,
                f_resi = f$residuals,
                f_fitted = f$fitted.values,
                sigma = summary_f$sigma,
                df = summary_f$df,
                r2 = summary_f$r.squared,
                rd_adj = summary_f$adj.r.squared,
                f_summary_coef = summary_f$coefficients,
                f_statistic = summary_f$fstatistic
  )
  
  factor_analysis = list(momentum = Factor_score_m$momentum,
                         reverse  = Factor_score_m$reverse,
                         size     = Factor_score_m$size
                         )
  
  ##Industry plot
  n_ci = length(CIList$SEC_NAME)
  CI_DIST_UGLY = list(n_ci)
  CI_EXPO_UGLY = list(n_ci)
  
  for(i_ci in 1:n_ci){
    CI_DIST_UGLY[[i_ci]] = CI_DIST[,i_ci]
    CI_EXPO_UGLY[[i_ci]] = CI_EXPO[,i_ci]
    
  }
  names(CI_DIST_UGLY) = CIList$SEC_NAME
  names(CI_EXPO_UGLY) = CIList$SEC_NAME
  
  industry_analysis = list(CI_DIST = CI_DIST_UGLY,CI_EXPO = CI_EXPO_UGLY,
                           CI_EXPO_EARN = CI_EXPO_EARN,CI_NAMES = CIList$SEC_NAME)

  out_list = list(factor_analysis = factor_analysis,capital_analysis = capital_analysis,
              industry_analysis = industry_analysis,day_list = daylist,f_list = f_list)
  jsonlite::write_json(out_list,'test_factor1.json')
  return(out_list)
}







