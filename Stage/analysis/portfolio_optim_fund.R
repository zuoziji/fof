PortOptimMultiConsFOF<-function(jsonFund,jsonDB){

    # compared with PortOptimFOF, in PortOptimMultiConsFOF we have more constraints and we only support vol as risk measure #
    # compared with PortOptimMultiConsFOF2, in PortOptimMultiConsFOF3 we have jsonFund,jsonDB and many more types of contraints #
    # json='{"volatility":0.15,"subjective_views":[{"value":0.05,"strategy2":"arbitrage","strategy1":"alpha"}],"annual_return":0,"funds":[{"strategies":[{"percent":50,"strategy":"alpha"},{"percent":30,"strategy":"arbitrage"},{"percent":20,"strategy":"cta"}],"fund":"J11039.OF"},{"strategies":[{"percent":20,"strategy":"alpha"},{"percent":80,"strategy":"arbitrage"}],"fund":"J12092.OF"},{"strategies":[{"percent":30,"strategy":"alpha"},{"percent":70,"strategy":"arbitrage"}],"fund":"J12118.OF"},{"strategies":[{"percent":60,"strategy":"alpha"},{"percent":40,"strategy":"cta"}], "fund":"J13440.OF"}],"strategies":[{"percent":50,"strategy":"alpha"},{"percent":20,"strategy":"arbitrage"},{"percent":20,"strategy":"cta"}],"cvar":0}';
    # json='{"cvar":0,"annual_return":0,"volatility":0.15,"subjective_views":[{"value":0.05,"strategy2":"arbitrage","strategy1":"alpha"}],"strategies":[{"percent":50,"strategy":"alpha"},{"percent":30,"strategy":"arbitrage"},{"percent":20,"strategy":"cta"}],"funds":[{"strategies":[{"percent":50,"strategy":"alpha"},{"percent":30,"strategy":"arbitrage"},{"percent":20,"strategy":"cta"}],"fund":"J11039.OF"},{"strategies":[{"percent":20,"strategy":"alpha"},{"percent":80,"strategy":"arbitrage"}],"fund":"J12092.OF"},{"strategies":[{"percent":30,"strategy":"alpha"},{"percent":70,"strategy":"arbitrage"}],"fund":"J12118.OF"},{"strategies":[{"percent":60,"strategy":"alpha"},{"percent":40,"strategy":"cta"}],"fund":"J13440.OF"}]}';
    # jsonFund='{"subjective_views":[{"value":0.05,"strategy2":"arbitrage","strategy1":"alpha"}],"expectations":{"cvar":0,"annual_return":0,"volatility":0.15},"strategies":[{"strategy":"alpha","operator":"largerorequal","percent":50},{"strategy":"arbitrage","operator":"smallerorequal","percent":30},{"strategy":"cta","operator":"smallerorequal","percent":20}],"funds":[{"fund":"J11039.OF","strategies":[{"strategy":"alpha","percent":50},{"strategy":"arbitrage","percent":30}, {"strategy": "cta", "percent": 20}]}, {"fund": "J12092.OF", "strategies": [{"strategy": "alpha", "percent": 20}, {"strategy": "arbitrage", "percent": 80}]}, {"fund": "J12118.OF", "strategies": [{"strategy": "alpha", "percent": 30}, {"strategy": "arbitrage", "percent": 70}]}, {"fund": "J13440.OF", "strategies":[{"strategy":"alpha","percent":60},{"strategy":"cta","percent":40}]}]}';
    # jsonDB='{"DB_PORT":"3306","DB_NAME":"fof_ams_dev","DB_PASSWORD":"Abcd1234","DB_USER":"mg","DB_IP":"10.0.3.66"}';
    # strategyCons=list("alpha"=0.6,"cta"=0.1,"arbitrage"=0.3);
    PasteAll<-function(A,Sep){
        nx=length(A)
        for (s in 1:(nx-1)){
             if (s==1) OP=paste(A[s],A[s+1],sep=Sep);
             if (s>1)  OP=paste(OP,A[s+1],sep=Sep);
        }
        return(OP)
    }
    library("rjson");
    library("RMySQL");
    library("xts");
    library("timeSeries");
    library("fPortfolio");
    library("robust");
    library("quadprog");
    JSONobject=fromJSON(jsonFund);
    jsondb=fromJSON(jsonDB);
    # channel=dbConnect(MySQL(),user="wangch",      # ÓÃ»§Ãû
    #                  password="Abcd1234",         # ÃÜÂë
    #                  dbname="quant_db",           # ÊýŸÝ¿âÃû³Æ
    #                  host="10.0.3.61");           # Ö÷»úµØÖ·
    # channel=dbConnect(MySQL(),user="wangch",      # ÓÃ»§Ãû
    #                   password="Abcd1234",        # ÃÜÂë
    #                   dbname="fof_ams_dev",       # ÊýŸÝ¿âÃû³Æ
    #                   host="10.0.3.66");          # Ö÷»úµØÖ·
    channel=dbConnect(MySQL(),user=jsondb$DB_USER,  # ÓÃ»§Ãû
                      password=jsondb$DB_PASSWORD,  # ÃÜÂë
                      dbname=jsondb$DB_NAME,        # ÊýŸÝ¿âÃû³Æ
                      host=jsondb$DB_IP);           # Ö÷»úµØÖ·


    # dbListTables(channel);                   # ²é¿ŽÊýŸÝ¿âÖÐµÄËùÓÐ±í  #
    # dbListFields(channel,"fundnav");         # ²é¿Žfundnav±íÖÐµÄÁÐÃû #
    nfund=length(JSONobject$funds);
    CodeX=array(0,nfund);
    for (i in 1:nfund){
         CodeX[i]=PasteAll(unlist(strsplit(as.character(JSONobject$funds[[i]]$fund[1]),"")),'');
    }
    fundX=list();
    DateList=list();
    for (i in 1:nfund){
         X=PasteAll(c("SELECT wind_code,concat(year(nav_date),'-',lpad(cast(weekofyear(nav_date) as char),2,'0')),max(nav_acc) from fund_nav WHERE wind_code='",CodeX[i],"' group by nav_date order by nav_date desc"),"");
         funda=dbGetQuery(channel,X);
         names(funda)=c("Code","Date","NAV");
         funda$Date=rev(funda$Date);
         funda$NAV=rev(funda$NAV);
         fundX[[i]]=funda;
         DateList[[i]]=funda$Date;
         if (i==1) DateCommon=as.character(funda$Date);
         if (i>1)  DateCommon=intersect(DateCommon,as.character(funda$Date));
    }

    T=length(DateCommon);
    Re=array(0,c(T,nfund));
    for (i in 1:nfund){
         jx=which(is.element(as.character(fundX[[i]]$Date),DateCommon));
         Price=fundX[[i]]$NAV[jx];
         Re[2:T,i]=Price[2:T]/Price[1:(T-1)]-1;
    }

    DateX=array(0,T);
    for (t in 1:T){
         DateX[t]=as.character(as.Date("1900-01-01")+t-1);
    }
    nmemory=T;
    tarvol=JSONobject$expectations$volatility;
    tarcvar=JSONobject$expectations$cvar;
    if (tarvol==0 && tarcvar!=0){
        riskMeasure="CVaR";
        tarisk=JSONobject$cvar;
    }
    if (tarvol!=0 && tarcvar==0){
        riskMeasure="Vol";
        tarisk=JSONobject$volatility;
    }
    if (tarvol==0 && tarcvar==0) stop("something's wrong");
    substrategy=list();
    for (i in 1:nfund){
         nsub=length(JSONobject$funds[[i]]$strategies);
         subx=array(0,c(nsub,2));
         for (j in 1:nsub){
              subx[j,]=c(JSONobject$funds[[i]]$strategies[[j]]$strategy,JSONobject$funds[[i]]$strategies[[j]]$percent)
         }
         substrategy[[i]]=subx;
         if (i==1) strategyU=subx[,1];
         if (i>1)  strategyU=unique(c(strategyU,subx[,1]));
    }
    nstrategy=length(strategyU);
    StrategyMatrix=array(0,c(nstrategy,nfund));
    for (i in 1:nfund){
         nsub=length(substrategy[[i]][,1]);
         for (j in 1:nsub){
              ia=which(is.element(substrategy[[i]][,1],substrategy[[i]][j,1]));
              StrategyMatrix[ia,i]=substrategy[[i]][j,2];
         }
    }
    StrategyMatrix=data.frame(StrategyMatrix);
    row.names(StrategyMatrix)=strategyU;
    names(StrategyMatrix)=CodeX;


    mstrategy=length(JSONobject$strategies);
    if (mstrategy!=nstrategy) stop("something's wrong, mstrategy!=nstrategy");
    strategyConX=array(0,nstrategy);
    strategyConType=array(0,nstrategy);
    strategyName=array(0,nstrategy);
    for (i in 1:nstrategy){
         strategyName[i]=JSONobject$strategies[[i]]$strategy;
         strategyConType[i]=JSONobject$strategies[[i]]$operator;
    }
    for (i in 1:nstrategy){
         jx=which(strategyName==strategyU[i]);
         strategyConX[i]=as.numeric(JSONobject$strategies[[jx]]$percent)/100;
    }
    # Part A #
    StrategyMatrix2=array(0,c(nstrategy,nfund));
    for (i in 1:nstrategy) StrategyMatrix2[i,]=as.numeric(as.matrix(StrategyMatrix[i,]))/100;
    Dmat=cov(as.matrix(Re[(T-nmemory+1):T,]));
    dvec=colMeans(Re[(T-nmemory+1):T,]);
    Amat=rbind(rep(1,nfund),StrategyMatrix2,diag(nfund),-diag(nfund));
    bvec=array(0,2*nfund+1+nstrategy);
    bvec[1]=1;                                                                            # sum of the weights equal to 1 #
    bvec[2:(nstrategy+1)]=strategyConX;
    # bvec[2:(nstrategy+1)]=c(0.4,0.55,0.05);                                             # #
    bvec[(nstrategy+2):(nfund+nstrategy+1)]=0.3/nfund;                                    # minimum weight is 0.25/n      #
    bvec[(nfund+nstrategy+2):(2*nfund+nstrategy+1)]=-min(3/nfund,1);                     # maximum weight is 4/n         #
    result=try(solve.QP(Dmat,dvec,t(Amat),bvec,meq=1+nstrategy,factorized=FALSE));
    if (length(result)>1)    WeightA=result$solution/sum(result$solution);
    if (length(result)==1)   WeightA=array(0,nfund);
    # Part B #
    StrategyMatrix2=array(0,c(nstrategy,nfund));
    for (i in 1:nstrategy) StrategyMatrix2[i,]=as.numeric(as.matrix(StrategyMatrix[i,]))/100;
    Dmat=cov(as.matrix(Re[(T-nmemory+1):T,]));
    dvec=colMeans(Re[(T-nmemory+1):T,]);
    Amat=rbind(rep(1,nfund),StrategyMatrix2,diag(nfund),-diag(nfund));
    bvec=array(0,2*nfund+1+nstrategy);
    bvec[1]=1;                                                                            # sum of the weights equal to 1 #
    for (i in 1:nstrategy){
         if (strategyConType[i]=="largerorequal")  bvec[i+1]=strategyConX[i];             # minimum is strategyConX[i]
         if (strategyConType[i]=="smallerorequal") bvec[i+1]=-strategyConX[i];            # mximum is strategyConX[i]
    }
    # bvec[2:(nstrategy+1)]=c(0.4,0.55,0.05);                                             # #
    bvec[(nstrategy+2):(nfund+nstrategy+1)]=0.3/nfund;                                    # minimum weight is 0.25/n      #
    bvec[(nfund+nstrategy+2):(2*nfund+nstrategy+1)]=-min(3/nfund,1);                     # maximum weight is 4/n         #
    result=try(solve.QP(Dmat,dvec,t(Amat),bvec,meq=1,factorized=FALSE));
    if (length(result)>1)    WeightB=result$solution/sum(result$solution);
    if (length(result)==1)   WeightB=array(0,nfund);
    # Part C #
    StrategyMatrix2=array(0,c(nstrategy,nfund));
    for (i in 1:nstrategy) StrategyMatrix2[i,]=as.numeric(as.matrix(StrategyMatrix[i,]))/100;
    Dmat=cov(as.matrix(Re[(T-nmemory+1):T,]));
    dvec=colMeans(Re[(T-nmemory+1):T,]);
    Amat=rbind(rep(1,nfund),StrategyMatrix2[1,],diag(nfund),-diag(nfund));
    bvec=array(0,2*nfund+1+1);
    bvec[1]=1;                                                                            # sum of the weights equal to 1 #
    if (strategyConType[1]=="largerorequal")       bvec[2]=strategyConX[1];
    if (strategyConType[1]=="smallerorequal")      bvec[2]=-strategyConX[1];
    # bvec[2:(nstrategy+1)]=c(0.4,0.55,0.05);                                             # #
    bvec[(1+2):(nfund+1+1)]=0.3/nfund;                                                    # minimum weight is 0.25/n      #
    bvec[(nfund+1+2):(2*nfund+1+1)]=-min(3/nfund,1);                                      # maximum weight is 4/n         #
    result=try(solve.QP(Dmat,dvec,t(Amat),bvec,meq=1,factorized=FALSE));
    if (length(result)>1)    WeightC=result$solution/sum(result$solution);
    if (length(result)==1)   WeightC=array(0,nfund);

    strategyWeightA=array(0,nstrategy);
    strategyWeightB=array(0,nstrategy);
    strategyWeightC=array(0,nstrategy);
    for (i in 1:nstrategy){
         strategyWeightA[i]=sum(as.numeric(as.matrix(StrategyMatrix[i,]))/100*as.numeric(as.matrix(WeightA)));
         strategyWeightB[i]=sum(as.numeric(as.matrix(StrategyMatrix[i,]))/100*as.numeric(as.matrix(WeightB)));
         strategyWeightC[i]=sum(as.numeric(as.matrix(StrategyMatrix[i,]))/100*as.numeric(as.matrix(WeightC)));
    }
    names(strategyWeightA)=strategyU;
    names(strategyWeightB)=strategyU;
    names(strategyWeightC)=strategyU;


    op=list(
       "fundWeightA"=as.data.frame(WeightA),
       "fundWeightB"=as.data.frame(WeightB),
       "fundWeightC"=as.data.frame(WeightC),
       "strategyWeightA"=as.data.frame(strategyWeightA),
       "strategyWeightB"=as.data.frame(strategyWeightB),
       "strategyWeightC"=as.data.frame(strategyWeightC),
       "ISNA"=any(is.na(Re)));

    return(op)

}


