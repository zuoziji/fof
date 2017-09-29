PortOptimStrategy<-function(jsonStrategy,jsonDB){

    # compared with PortOptimStrategy, in PortOptimStrategy2 we use, we accept vol/var/cvar as risk measure but we don't have constraints #
    # compared with PortOptimStrategy2, in PortOptimStrategy3 we only take vol as risk measure #
    # depends on strategy_index_val
    # json='{"annual_return":0,"volatility":0.15,"cvar":0,"strategies":["alpha","arbitrage","cta"],"subjective_views":[{"value":0.05,"strategy2":"arbitrage","strategy1":"alpha"}]}';
    # jsonStrategy='{"expectations":{"cvar":0,"volatility":0.15,"annual_return":0},"strategies":["alpha","arbitrage","cta"],"subjective_views":[{"strategy2":"arbitrage","strategy1":"alpha","value":0.05}]}'
    # jsonDB='{"DB_PORT":"3306","DB_NAME":"fof_ams_dev","DB_PASSWORD":"Abcd1234","DB_USER":"mg","DB_IP":"10.0.3.66"}';
    PasteAll<-function(A,Sep){
        nx=length(A)
        for (s in 1:(nx-1)){
             if (s==1) OP=paste(A[s],A[s+1],sep=Sep);
             if (s>1)  OP=paste(OP,A[s+1],sep=Sep);
        }
        return(OP)
    }
    library("rjson");
    library(RMySQL);
    library("xts");
    library("timeSeries");
    library("fPortfolio");
    library("quadprog");
    JSONobject=fromJSON(jsonStrategy);
    jsondb=fromJSON(jsonDB);
    # channel=dbConnect(MySQL(),user="wangch",   #ÓÃ»§Ãû
    #                   password="Abcd1234",     #ÃÜÂë
    #                   dbname="quant_db",       #ÊýŸÝ¿âÃû³Æ
    #                   host="10.0.3.61");       #Ö÷»úµØÖ·
    channel=dbConnect(MySQL(),user=jsondb$DB_USER,  # ÓÃ»§Ãû
                      password=jsondb$DB_PASSWORD,  # ÃÜÂë
                      dbname=jsondb$DB_NAME,        # ÊýŸÝ¿âÃû³Æ
                      host=jsondb$DB_IP);           # Ö÷»úµØÖ·
    # dbListTables(channel);                   # ²é¿ŽÊýŸÝ¿âÖÐµÄËùÓÐ±í   #
    # dbListFields(channel,"fundnav");         # ²é¿Žfundnav±íÖÐµÄÁÐÃû #
    strategyX=list();
    strategyZ=dbGetQuery(channel,"select * from strategy_index_val");
    names(strategyZ)=c("strategyName","Date","Value");
    strategyIndex=strategyZ$strategyName;
    strategyU=unique(strategyIndex);
    nstrategy=length(strategyU);
    strategyList=list();
    for (i in 1:nstrategy){
         jx=which(strategyZ$strategyName==strategyU[i]);
         strategyList[[i]]=strategyZ[jx,];
         if (i==1) DateCommon=as.character(strategyList[[i]]$Date);
         if (i>1)  DateCommon=intersect(DateCommon,as.character(strategyList[[i]]$Date));
    }
    T=length(DateCommon);
    IndexValue=array(0,c(T,nstrategy));
    Re=array(0,c(T,nstrategy));
    for (i in 1:nstrategy){
         jx=which(is.element(strategyList[[i]]$Date,DateCommon));
         IndexValue[,i]=strategyList[[i]]$Value[jx];
         Re[2:T,i]=IndexValue[2:T,i]/IndexValue[1:(T-1),i]-1;
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
    if (tarvol==0 && tarcvar==0) stop("something's wrong") ;
    strategies=JSONobject$strategies;
    jdel=which(is.element(strategyU,strategies)==FALSE);
    ldel=length(jdel);
    cons1=PasteAll(c("minW[1:",nstrategy,"]=",1/nstrategy/3),"");
    cons2=PasteAll(c("maxW[1:",nstrategy,"]=",min(1,3/nstrategy)),"");
    constraints=c(cons1,cons2);
    dataRe=data.frame(Re[(T-nmemory+1):T,]);
    names(dataRe)=as.character(strategyU);

    # if (ldel>0){
    #    MatrixA=xts(dataRe[,-jdel],as.Date(DateX));
    #    MatrixA=timeSeries(MatrixA);
    #    globminSpec=portfolioSpec();
    #    # longFrontier=try(portfolioFrontier(data=MatrixA,spec=globminSpec,constraints=constraints));
    #    longFrontier=try(portfolioFrontier(data=MatrixA,spec=globminSpec));
    #    if (is.character(longFrontier)==FALSE){
    #        TarRisk=getTargetRisk(longFrontier);
    #        if (riskMeasure=="Vol")  jopt=which(abs(TarRisk[,2]-tarisk)==min(abs(TarRisk[,2]-tarisk)));
    #        if (riskMeasure=="CVaR") jopt=which(abs(TarRisk[,3]-tarisk)==min(abs(TarRisk[,3]-tarisk)));
    #        if (riskMeasure=="VaR")  jopt=which(abs(TarRisk[,4]-tarisk)==min(abs(TarRisk[,4]-tarisk)));
    #        weightx=getWeights(longFrontier)[jopt,];
    #        Weight=array(0,nstrategy);
    #        Weight[which(is.element(strategyU,strategies)==TRUE)]=weightx;
    #        names(Weight)=as.character(strategyU);
    #    }
    # }
    # if (ldel==0){
    #    MatrixA=xts(dataRe,as.Date(DateX));
    #    MatrixA=timeSeries(MatrixA);
    #    globminSpec=portfolioSpec();
    #    # longFrontier=try(portfolioFrontier(data=MatrixA,spec=globminSpec,constraints=constraints));
    #    longFrontier=try(portfolioFrontier(data=MatrixA,spec=globminSpec));
    #    if (is.character(longFrontier)==FALSE){
    #        TarRisk=getTargetRisk(longFrontier);
    #        if (riskMeasure=="Vol")  jopt=which(abs(TarRisk[,2]-tarisk)==min(abs(TarRisk[,2]-tarisk)));
    #        if (riskMeasure=="CVaR") jopt=which(abs(TarRisk[,3]-tarisk)==min(abs(TarRisk[,3]-tarisk)));
    #        if (riskMeasure=="VaR")  jopt=which(abs(TarRisk[,4]-tarisk)==min(abs(TarRisk[,4]-tarisk)));
    #        Weight=getWeights(longFrontier)[jopt,];
    #    }
    # }


    strategies=JSONobject$strategies;
    jsel=which(is.element(strategyU,strategies)==TRUE);
    lsel=length(jsel);
    Dmat=cov(as.matrix(Re[(T-nmemory+1):T,jsel]));
    dvec=colMeans(Re[(T-nmemory+1):T,jsel]);
    Amat=rbind(rep(1,lsel),diag(lsel),-diag(lsel));
    bvec=array(0,2*lsel+1);
    bvec[1]=1;                                                                          # sum of the weights equal to 1 #
    bvec[2:(lsel+1)]=0.33/lsel;                                                         # minimum weight is 0.33/n      #
    bvec[(lsel+2):(2*lsel+1)]=-min(3/nstrategy,1);                                      # maximum weight is 3/n  #
    result=try(solve.QP(Dmat,dvec,t(Amat),bvec,meq=1,factorized=FALSE));
    Weight=array(0,nstrategy);
    Weight[jsel]=result$solution/sum(result$solution);
    names(Weight)=strategyU;


    op=list(
       "strategyWeight"=as.data.frame(Weight));

    return(op)

}


