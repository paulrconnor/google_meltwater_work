# Get required packages
require(devtools)
install_github("PMassicotte/gtrendsR")
require(gtrendsR)
require(jsonlite)
require(data.table)
require(anytime)
require(RMySQL)
require(XML)
require(RCurl)
require(rvest)
require(httr)

# set timezone
Sys.setenv(TZ = "US/Pacific")

# create coindf (has names of coins, tickers, search terms to use)
#####
tickers<-c("BTC","ETH","XRP","BCH","ADA","LTC","XLM","NEO","EOS","XEM","IOTA","DASH","XMR","USDT","TRX","LSK","ETC","VEN","QTUM","BTG","PPT","NANO","ICX","ZEC","OMG","STEEM","BNB","SNT","BCN","XVG","STRAT","SC","BTS","MKR","AE","VERI","REP","WAVES","WTC","DGD","KCS","ZRX","DOGE","DCR","RHOC","ARDR","HSR","KNC","KMD","ZIL","ARK","DCN","DRGN","BAT","LRC","GAS","ETN","ELF","PIVX","DGB","BTM","GBYTE","CNX","QASH","R","ZCL","GNT","GXS","BTX","PLR","NAS","SYS","IOST","DENT","CND","ETHOS","MONA","SMART","SALT","AION","FCT","POWR","FUN","LINK","QSP","AGI","NXT","XZC","MAID","RDD","BNT","PAY","ENG","NXS","REQ","IGNIS","ICN","VTC","GAME","WAX","PART","NEBL","ELA","KIN","NCASH","JNT","STORJ","STORM","WAN","BTCP","ONT","MITH")

# get coin names
response = fromJSON('https://www.cryptocompare.com/api/data/coinlist')
df = data.table::rbindlist(response$Data, fill=TRUE)

# combine with tickers
coindf <- data.frame(ticker=tickers)
coindf <- merge(coindf,df[,c("Symbol","CoinName")],by.x="ticker",by.y="Symbol",all.x=T)

# fixing errors
coindf$CoinName[coindf$ticker=="ETHOS"] <- "Ethos"
coindf$CoinName[coindf$ticker=="IOTA"] <- "IOTA"
coindf$CoinName[coindf$ticker=="NANO"] <- "Nano"

# adding different searches for minor coins or coins with overky generic names 
# (gtrends does not handle Boolean operators well at present)
coindf$searchterm <- coindf$CoinName

# generic names
gennames <- c("ARK","Dent","Enigma","EOS","Ethos","Factoids","FunFair","Gamecredits","Gas",
              "Kin","Komodo","Lisk","IOTA","Maker","Nano","Nebulas",
              "NEO","Nexus","Nxt","Ontology","Pillar","Power Ledger","Populous",
              "Augur","Storm","Waves","NEM","Stellar","Verge")
for(i in 1:length(gennames)){
  namedf <- coindf[coindf$CoinName==gennames[i],]
  namedf <- rbind.data.frame(namedf,namedf,namedf)
  namedf$searchterm <- c(paste0(gennames[i]," coin"),paste0(gennames[i]," price"),paste0(gennames[i]," crypto"))
  coindf <- rbind.data.frame(coindf[-which(coindf$CoinName==gennames[i]),],namedf)
}

# network tokens, projects
nts <- c("Bancor Network Token","Golem Network Token","ICON Project","IOS token","Jibrel Network Token","Private Instant Verified Transaction","Status Network Token","Worldwide Asset eXchange")
for(i in 1:length(nts)){
  namedf <- coindf[coindf$CoinName==nts[i],]
  namedf <- rbind.data.frame(namedf,namedf,namedf,namedf)
  n <- gsub(" Network Token","",nts[i])
  n <- gsub(" token","",n)
  n <- gsub(" Project","",n)
  n <- gsub("Private Instant Verified Transaction","PIVX",n)
  n <- gsub("Worldwide Asset eXchange","WAX",n)
  namedf$searchterm <- c(nts[i],paste0(n," coin"),paste0(n," price"),paste0(n," crypto"))
  coindf <- rbind.data.frame(coindf[-which(coindf$CoinName==nts[i]),],namedf)
}

# miscellaneous
coindf$searchterm[coindf$searchterm=="Bitcoin Cash / BCC"] <- "Bitcoin Cash"

# choose minor coin for all other searches to be scaled to 
reference <- "BCN"

# rearranges data 
coindf <- rbind.data.frame(coindf[coindf$ticker==reference,],
                           coindf[-which(coindf$ticker%in%c(reference)),])

# save coindf to file
write.csv(coindf,file="~/Dropbox/Meltwater/coindf.csv",row.names = F) # csv to dropbox
con <- dbConnect(RMySQL::MySQL(), user="root",dbname="meltwater") # creates connection to MySQL
dbWriteTable(con,"coindf",coindf,overwrite=TRUE) # table to MySQL

# clean environment
rm(list=ls())
########

# function to scrape google data (makes first coin in coinstoscrape the reference coin with max = 100)
scrapegoogle <- function(coinstoscrape,time){
  
  suppressWarnings(rm(res,timedat,risingdat,topdat,countrydat))
  
  # get google hits for coins (in case of an error, waits and tries again twice, if still an error skips coins)
  hitsOK <- TRUE
  tryCatch({
    res <- gtrends(keyword=coinstoscrape,time=time)
  },error=function(e){
    Sys.sleep(60)
    tryCatch({
      res <- gtrends(keyword=coinstoscrape,time=time)
    },error=function(e){
      Sys.sleep(300)
      tryCatch({
        res <- gtrends(keyword=coinstoscrape,time=time)
      },error=function(e){
        Sys.sleep(600)
        tryCatch({
          res <- gtrends(keyword=coinstoscrape,time=time)
        },error=function(e){
          hitsOK <<- FALSE
        })
      })
    })
  })
  
  if(hitsOK){
    
    # get data of interest (time series data)
    timedat <- as.data.frame(res$interest_over_time)
    timedat$hits[timedat$hits=="<1"] <- 0
    timedat$hits <- as.numeric(timedat$hits)
    
    # convert to US Pacific time
    timedat$date <- timedat$date - 7200 # gtrends quirk
    
    # adjust data so reference coin = 100
    multiplier=100/max(timedat$hits[timedat$keyword==coinstoscrape[1]])
    timedat$hits <- timedat$hits*multiplier
    
    # save to data frame
    searchdat <- data.frame(time=timedat$date,searchterm=timedat$keyword,hits=timedat$hits)
    
    # get interest by region
    if(!is.null(res$interest_by_country)){
      countryres <- res$interest_by_country
      countryres$from <- min(timedat$date)
      countryres$to <- max(timedat$date)
      countryres$country <- countryres$location
      countryres$searchterm <- countryres$keyword
      countryres <- countryres[,c("searchterm","country","hits","from","to")]
    }
    
    # get rising and top related searches
    if(!is.null(res$related_queries)){ 
      
      # rising 
      related <- as.data.frame(res$related_queries)
      rising <- related[related$related_queries=="rising",]
      rising$subject[rising$subject=="Breakout"] <- "+5000"
      rising$increase <- as.numeric(gsub(",","",gsub("\\+","",gsub("%","",rising$subject))))
      risingdat <- data.frame(searchterm=rising$keyword,risingquery=rising$value,increase=rising$increase,time=paste0(res$interest_over_time$date[1]," to ",res$interest_over_time$date[length(res$interest_over_time$date)]))
      
      # top
      top <- related[related$related_queries=="top",]
      topdat <- data.frame(searchterm=top$keyword,topquery=top$val,hits=as.numeric(top$subject),time=paste0(res$interest_over_time$date[1]," to ",res$interest_over_time$date[length(res$interest_over_time$date)]))
    }
    
    reslist <- list(searchdat)
    
    if(exists("risingdat")&exists("topdat")&exists("countryres")){
      reslist[[2]] <- risingdat
      reslist[[3]] <- topdat
      reslist[[4]] <- countryres
    }
    return(reslist)
  } else {
    return(0)
  }
}
    
# function to scrape price data
scrapeprice <- function(coinstoscrape,coindf,time){
  
  # get price data
  priceOK <- TRUE
  for (j in 1:length(coinstoscrape)){
    
    tryCatch({
      ifelse(time=="now 1-H",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=60")),
             ifelse(time=="now 4-H", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=240")),
                    ifelse(time=="now 1-d",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=24")),
                           ifelse(time=="now 7-d", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=168")),
                                  ifelse(time=="today 1-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=30")),
                                         ifelse(time=="today 12-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=365")),NA))))))
    },error=function(e){
      Sys.sleep(60)
      tryCatch({
        ifelse(time=="now 1-H",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=60")),
               ifelse(time=="now 4-H", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=240")),
                      ifelse(time=="now 1-d",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=24")),
                             ifelse(time=="now 7-d", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=168")),
                                    ifelse(time=="today 1-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=30")),
                                           ifelse(time=="today 12-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=365")),NA))))))
      },error=function(e){
        Sys.sleep(120)
        tryCatch({
          ifelse(time=="now 1-H",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=60")),
                 ifelse(time=="now 4-H", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=240")),
                        ifelse(time=="now 1-d",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=24")),
                               ifelse(time=="now 7-d", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=168")),
                                      ifelse(time=="today 1-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=30")),
                                             ifelse(time=="today 12-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=365")),NA))))))
        },error=function(e){
          Sys.sleep(180)
          tryCatch({
            ifelse(time=="now 1-H",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=60")),
                   ifelse(time=="now 4-H", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histominute?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=240")),
                          ifelse(time=="now 1-d",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=24")),
                                 ifelse(time=="now 7-d", cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histohour?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=168")),
                                        ifelse(time=="today 1-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=30")),
                                               ifelse(time=="today 12-m",cc <- fromJSON(paste0("https://min-api.cryptocompare.com/data/histoday?fsym=",coindf$ticker[coindf$searchterm==coinstoscrape[j]],"&tsym=USD&limit=365")),NA))))))
          },error=function(e){
            priceOK <<- FALSE
          })
        })
      })
    })
    
    if(priceOK){
      ccdat <- cc$Data
      ccdat$time <- anytime(cc$Data$time)
      #attributes(ccdat$time)$tzone <- "US/Pacific" 
      ccdat$price <- ccdat$close
      ccdat$searchterm <- coinstoscrape[j]
      ccdat <- ccdat[,c("time","price","searchterm")]
      if(j==1){
        newpricedat <- ccdat
      } else {
        newpricedat <- rbind(newpricedat,ccdat)
      }
    } # price OK end
  } # j loop end
  if(exists("newpricedat")){
    return(newpricedat)
  } else {
    return(0)
  }
}

# function for combining timeseries datasets
combinefunc <- function(olddat,newdat,hits="TRUE",rm_reference=NA,final=FALSE){
  if(hits){
    if(!is.na(rm_reference)){
      adjustby <- rm_reference
      overlap1 <- olddat$hits[olddat[,which(names(olddat)=="searchterm"|names(olddat)=="coin")]==adjustby&olddat$time%in%newdat$time]
      overlap2 <- newdat$hits[newdat[,which(names(newdat)=="searchterm"|names(newdat)=="coin")]==adjustby&newdat$time%in%olddat$time]
      multiplier <- sum(overlap1)/sum(overlap2)
      newdat$hits <- newdat$hits*multiplier
      newdat <- newdat[newdat[,which(names(newdat)=="searchterm"|names(newdat)=="coin")]!=adjustby,]
      combineddat <- rbind(olddat,newdat)
    } else {
      adjustby <- newdat[newdat$hits==max(newdat$hits,na.rm=T),which(names(newdat)=="searchterm"|names(newdat)=="coin")][1]
      overlap1 <- olddat$hits[olddat[,which(names(olddat)=="searchterm"|names(olddat)=="coin")]==adjustby&olddat$time%in%newdat$time]
      overlap2 <- newdat$hits[newdat[,which(names(newdat)=="searchterm"|names(newdat)=="coin")]==adjustby&newdat$time%in%olddat$time]
      multiplier <- sum(overlap1)/sum(overlap2)
      newdat$hits <- newdat$hits*multiplier
      if(final){
        combineddat <- rbind(olddat[!olddat$coindate%in%newdat$coindate,],newdat)
      } else {
        combineddat <- rbind(olddat[olddat$time<min(newdat$time),],newdat)
      }
    }
  } else {
    if(!is.na(rm_reference)){
      newdat <- newdat[!newdat[,which(names(newdat)=="searchterm"|names(newdat)=="coin")]==rm_reference,]
      combineddat <- rbind(olddat,newdat)
    } else {
      if(final){
        combineddat <- rbind(olddat[!olddat$coindate%in%newdat$coindate,],newdat)
      } else {
        combineddat <- rbind(olddat[!olddat$time%in%newdat$time,],newdat)
      }
    }
  }
  return(combineddat)
}

# function to aggregate data for coins with multiple search terms
dat <- fullhitsdat
aggcoinfunc <- function(dat,coindf,hits=TRUE){
  dat <- merge(dat,coindf[,c("searchterm","CoinName")],by.x="searchterm",by.y="searchterm",all.x=T)
  
  if(hits){
    dat$coindate <-  paste0(dat$CoinName,dat$time)
    dat <- aggregate(dat[,c("time","CoinName","hits")],by=list(dat$coindate),
                     FUN=function(x){if(class(x)[1]!="numeric"){return(x[1])} else {return(sum(x))}})[,2:4]
    names(dat) <- c("time","coin","hits")
  } else {
    if("price"%in%names(dat)){
      dat$coindate <-  paste0(dat$CoinName,dat$time)
      dat <- aggregate(dat[,c("time","CoinName","price")],by=list(dat$coindate),
                       FUN=function(x){if(class(x)[1]!="numeric"){return(x[1])} else {return(sum(x))}})[,2:4]
      names(dat) <- c("time","coin","price")
    } else {
      if("country"%in%names(dat)){
        dat$coin <- dat$CoinName
        dat$countrycoin <- paste0(dat$country,dat$coin)
        dat$hits <- ifelse(dat$hits=="<1",0,dat$hits)
        dat$hits <- ifelse(is.na(dat$hits)|dat$hits=="",0,dat$hits)
        dat$hits <- as.numeric(dat$hits)
        dat <- aggregate(dat[,c("coin","country","hits","from","to")],by=list(dat$countrycoin),
                         FUN=function(x){if(class(x)[1]!="numeric"){return(x[1])} else {return(mean(x,na.rm=T))}})[,2:6 ]
        dat$time <- paste0(dat$from," to ",dat$to)
      } else {
        dat$coin <- dat$CoinName
        dat <- dat[,c("coin","time",names(dat)[2:3],"url")]
      }
    }
  }
  dat$coindate <- paste0(dat$coin,dat$time)
  return(dat)
}

# function to aggregate to hourly level data more than 30 days old
dat <- hitsdatE
aggolddat <- function(dat){
  dat$time <- as.POSIXct(dat$time)
  now <- Sys.time()
  thirty <- now-(60*60*24*30)
  dattoagg <- dat[dat$time<thirty,]
  coinstoagg <- unique(dattoagg$coin)
  for(j in 1:length(coinstoagg)){
    cdat <- dattoagg[dattoagg$coin==coinstoagg[j],]
    for(k in 1:nrow(cdat)){
      if(nrow(cdat)>0){
        rowtime <- cdat$time[nrow(cdat)]
        withinhour <- cdat[cdat$time>(rowtime-3600)&cdat$time<=rowtime,]
        newrow <- cdat[nrow(cdat),]
        newrow[,grepl("hits",names(newrow))|grepl("price",names(newrow))] <- mean(withinhour[,grepl("hits",names(newrow))|grepl("price",names(newrow))],na.rm=T)
        if(k==1){
          newcdat <- newrow
        } else {
          newcdat <- rbind(newcdat,newrow)
        }
        cdat <- cdat[cdat$time<=rowtime-3600|cdat$time>rowtime,]
      } else {
        break
      }
    }
    if(j==1){
      aggdat <- newcdat
    } else {
      aggdat <- rbind(aggdat,newcdat)
    }
  }
  dattokeep <- dat[dat$time>=thirty,]
  aggdat <- rbind(aggdat,dattokeep)
  return(aggdat)
}

# function to close db connections
killDbConnections <- function () {
  
  all_cons <- dbListConnections(MySQL())
  
  print(all_cons)
  
  for(con in all_cons)
    +  dbDisconnect(con)
  
  print(paste(length(all_cons), " connections killed."))
  
}

# function for pulling top google search results
getGoogle <- function(search.term,domain,quotes=FALSE){
  search.term <- gsub(' ', '%20', search.term)
  if(quotes) search.term <- paste('%22', search.term, '%22', sep='') 
  getGoogleURL <- paste('http://www.google', domain, '/search?q=',
                        search.term, sep='')
  main.page <- read_html(x = getGoogleURL)
  links <- main.page %>% 
    html_nodes(".r a") %>% # get the a nodes with an r class
    html_attr("href") # get the href attributes
  #clean the text  
  links = gsub('/url\\?q=','',sapply(strsplit(links[as.vector(grep('url',links))],split='&'),'[',1))
  return(links)
}

# function for getting urls for top rising or related queries
geturls <- function(dat){
  top <- ifelse("hits"%in%names(dat),TRUE,FALSE)
  dat$url <- NA
  searchterms <- unique(dat$searchterm)
  for(j in 1:length(searchterms)){
    if(top){
      queries <- dat[dat$searchterm==searchterms[j]&dat$hits==100,"topquery"]
    } else {
      queries <- dat[dat$searchterm==searchterms[j]&dat$increase==5000,"risingquery"]
    }
    if(length(queries)>0){
      for(k in 1:length(queries)){
        links <- getGoogle(search.term = paste0(searchterms[j]," ",queries[k]),domain=".com")
        # find the top link that works
        workinglink <- NA
        for(l in 1:length(links)){
          res <- !http_error(links[l])
          if(res){
            workinglink <- links[l]
            break
          }
        }
        # add working link to risingdat df
        if(top){
          dat$url[dat$searchterm==searchterms[j]&dat$topquery==queries[k]] <- workinglink
        } else {
          dat$url[dat$searchterm==searchterms[j]&dat$risingquery==queries[k]] <- workinglink
        }
      }
    }
  }
  return(dat)
}

# time options are ("now 1-H","now 4-H","now 1-d","now 7-d","today 1-m","today 12-m")

# loop to get and save historical data (CAREFUL - will overwrite previously saved data)

# load coindf
coindf <- read.csv("~/Dropbox/Meltwater/coindf.csv")
con <- dbConnect(RMySQL::MySQL(), user="root",dbname="meltwater") # creates connection to MySQL
coindf <- dbReadTable(con, "coindf")

########################################################################################################################
# this loop climbs by 4 because we scrape google info on five coins at a time, and one is always a reference coin
for(i in seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4)){
  
  print(i)

  # select coins to scrape (google can return 5 per query)
  coinstoscrape <- c(coindf$searchterm[1],coindf$searchterm[-which(coindf$CoinName=="Bitcoin")][(i):(i+3)][(i):(i+3)<=nrow(coindf)])
  
  # if it is the last scrape, we will get bitcoin data separately
  if(i==max(seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4))){
    coinstoscrape <- list(coinstoscrape,c(coindf$searchterm[1],"Bitcoin"))
  } else {
    coinstoscrape <- list(coinstoscrape)
  }
  
  # cycle through sets of coinstoscrape
  for(j in 1:length(coinstoscrape)){
    
    # 1. get google data
    suppressWarnings(rm(googlemonth,googleweek,hitsdat,risingdat,topdat,countrydat))
    googlemonth <- scrapegoogle(coinstoscrape[[j]],"today 1-m")
    googleweek <- scrapegoogle(coinstoscrape[[j]],"now 7-d")
    if(class(googlemonth)=="list"&class(googleweek)=="list"){
      hitsdat <- combinefunc(googlemonth[[1]],googleweek[[1]],hits=TRUE,rm_reference = NA)
      if(length(googlemonth)==4&length(googleweek)==4){
        risingdat <- combinefunc(googlemonth[[2]],googleweek[[2]],hits=FALSE,rm_reference = NA)
        topdat <- combinefunc(googlemonth[[3]],googleweek[[3]],hits=FALSE,rm_reference = NA)
        countrydat <- rbind(googlemonth[[4]],googleweek[[4]])
        
        # get urls for breakout queries and top related searches
        risingdat <- geturls(risingdat)
        topdat <- geturls(topdat)
      }
    }
  
    
    
    # get + combine price data
    suppressWarnings(rm(pricemonth.priceweek,pricedat))
    pricemonth <- scrapeprice(coinstoscrape[[j]],coindf,"today 1-m")
    priceweek <- scrapeprice(coinstoscrape[[j]],coindf,"now 7-d")
    if(class(pricemonth)=="data.frame"&class(priceweek)=="data.frame"){
      pricedat <- combinefunc(pricemonth,priceweek,hits=FALSE,rm_reference=NA)
    }
    
    if(i==2){
      suppressWarnings(rm(fullhitsdat,fullpricedat,fullrisingdat,fulltopdat,fullcountrydat))
    }
    
    if(!exists("fullhitsdat")){
      if(exists("hitsdat")){
        fullhitsdat <- hitsdat
      }
    } else {
      if(exists("hitsdat")){
        fullhitsdat <- combinefunc(fullhitsdat,hitsdat,hits=TRUE,rm_reference=coindf$searchterm[1])
      }
    }
    
    if(!exists("fullpricedat")){
      if(exists("pricedat")){
        fullpricedat <- pricedat
      }
    } else {
      if(exists("pricedat")){
        fullpricedat <- combinefunc(fullpricedat,pricedat,hits=FALSE,rm_reference=coindf$searchterm[1])
      }
    }
    
    if(!exists("fullrisingdat")){
      if(exists("risingdat")){
        fullrisingdat <- risingdat
      }
    } else {
      if(exists("risingdat")){
        fullrisingdat <- combinefunc(fullrisingdat,risingdat,hits=FALSE,rm_reference=coindf$searchterm[1])
      }
    }
    
    if(!exists("fulltopdat")){
      if(exists("topdat")){
        fulltopdat <- topdat
      }
    } else {
      if(exists("topdat")){
        fulltopdat <- combinefunc(fulltopdat,topdat,hits=FALSE,rm_reference=coindf$searchterm[1])
      }
    }
    
    if(!exists("fullcountrydat")){
      if(exists("countrydat")){
        fullcountrydat <- countrydat
      }
    } else {
      if(exists("countrydat")){
        countrydat <- countrydat[countrydat$searchterm!=coinstoscrape[1],]
        fullcountrydat <- rbind(fullcountrydat,countrydat)
      }
    }
  }
  
  if(i==max(seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4))){break}
  
  if(i==max(seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4))){
    
    # aggregate data for coins with multiple search terms (also adds coindate variable)
    fullhitsdat <- aggcoinfunc(fullhitsdat,coindf,hits=TRUE)
    fullpricedat <- aggcoinfunc(fullpricedat,coindf,hits=FALSE)
    fullrisingdat <- aggcoinfunc(fullrisingdat,coindf,hits=FALSE)
    fulltopdat <- aggcoinfunc(fulltopdat,coindf,hits=FALSE)
    fullcountrydat <- aggcoinfunc(fullcountrydat,coindf,hits=FALSE)
    
    # save data
    
    # to csv
    write.csv(fullhitsdat,file="~/Dropbox/Meltwater/fullhitsdat.csv",row.names = F)
    write.csv(fullpricedat,file="~/Dropbox/Meltwater/fullpricedat.csv",row.names = F)
    write.csv(fullrisingdat,file="~/Dropbox/Meltwater/fullrisingdat.csv",row.names=F)
    write.csv(fulltopdat,file="~/Dropbox/Meltwater/fulltopdat.csv",row.names=F)
    write.csv(fullcountrydat,file="~/Dropbox/Meltwater/fullcountrydat.csv",row.names=F)
    
    # to SQL
    con <- dbConnect(RMySQL::MySQL(), user="root",dbname="meltwater") # creates connection to MySQL
    dbWriteTable(con,"fullhitsdat",fullhitsdat,overwrite=TRUE) # table to MySQL
    dbWriteTable(con,"fullpricedat",fullpricedat,overwrite=TRUE)
    dbWriteTable(con,"fullrisingdat",fullrisingdat,overwrite=TRUE)
    dbWriteTable(con,"fulltopdat",fulltopdat,overwrite=TRUE)
    dbWriteTable(con,"fullcountrydat",fullcountrydat,overwrite=TRUE)
  }
}
########################################################################################################################

########################################################################################################################
# loop to update data 
for(h in 1:10000){
  
  # load existing data
  
  # load existing data
  hitsdatE <- read.csv("~/Dropbox/Meltwater/fullhitsdat.csv")
  pricedatE <- read.csv("~/Dropbox/Meltwater/fullpricedat.csv")
  risingdatE <- read.csv("~/Dropbox/Meltwater/fullrisingdat.csv")
  topdatE <- read.csv("~/Dropbox/Meltwater/fulltopdat.csv")
  countrydatE <- read.csv("~/Dropbox/Meltwater/fullcountrydat.csv")
  
  # from SQL
  con <- dbConnect(RMySQL::MySQL(), user="root",dbname="meltwater") # creates connection to MySQL
  hitsdatE <- dbReadTable(con,"fullhitsdat") # table to MySQL
  pricedatE <- dbReadTable(con,"fullpricedat")
  risingdatE <- dbReadTable(con,"fullrisingdat")
  topdatE <- dbReadTable(con,"fulltopdat")
  countrydatE <- dbReadTable(con,"fullcountrydat")
  
  # convert time to POSIX
  hitsdatE$time <- as.POSIXct(hitsdatE$time)
  pricedatE$time <- as.POSIXct(pricedatE$time)
  
  # aggregate hits and price data older than 30 days
  hitsdatE <- aggolddat(hitsdatE)
  pricedatE <- aggolddat(pricedatE)
  
  # limit size of other datasets
  if(nrow(risingdatE)>2000000){
    risingdatE <- risingdatE[(nrow(risingdatE)-2000000):nrow(risingdatE),]
  }
  if(nrow(topdatE)>2000000){
    topdatE <- topdatE[(nrow(topdatE)-2000000):nrow(topdatE),]
  }
  if(nrow(countrydatE)>2000000){
    countrydatE <- countrydatE[(nrow(countrydatE)-2000000):nrow(countrydatE),]
  }
  
  # calculate time needed to fill
  now <- Sys.time()
  mostrecent <- max(hitsdatE$time)
  timetofill <- difftime(now,mostrecent,units="hours")
  mytime <- ifelse(timetofill>4,"now 1-d","now 4-H")
  
  # load coindf
  
  # old way
  coindf <- read.csv("~/Dropbox/Meltwater/coindf.csv")
  # new way
  coindf <- dbReadTable(con, "coindf")
  
  for(i in seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4)){
    
    print(i)
    
    # select coins to scrape (google can return 5 per query)
    coinstoscrape <- c(coindf$searchterm[1],coindf$searchterm[-which(coindf$CoinName=="Bitcoin")][(i):(i+3)][(i):(i+3)<=nrow(coindf)])
    
    if(i==max(seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4))){
      coinstoscrape <- list(coinstoscrape,c(coindf$searchterm[1],"Bitcoin"))
    } else {
      coinstoscrape <- list(coinstoscrape)
    }
    
    for(j in 1:length(coinstoscrape)){
      # get google data
      suppressWarnings(rm(googledat,hitsdat,risingdat,topdat,countrydat))
      googledat <- scrapegoogle(coinstoscrape[[j]],mytime)
      if(class(googledat)=="list"){
        hitsdat <- googledat[[1]]
        if(length(googledat)==4){
          risingdat <- googledat[[2]]
          topdat <- googledat[[3]]
          countrydat <- googledat[[4]]
          
          # get urls for breakout queries and top related searches
          risingdat <- geturls(risingdat)
          topdat <- geturls(topdat)
        }
      }
      
      # get price data
      suppressWarnings(rm(pricedat))
      pricedat <- scrapeprice(coinstoscrape[[j]],coindf,mytime)
      
      if(i==2){
        suppressWarnings(rm(fullhitsdat,fullpricedat,fullrisingdat,fulltopdat,fullcountrydat))
      }
      
      if(!exists("fullhitsdat")){
        if(exists("hitsdat")){
          fullhitsdat <- hitsdat
        }
      } else {
        if(exists("hitsdat")){
          fullhitsdat <- combinefunc(fullhitsdat,hitsdat,hits=TRUE,rm_reference=coindf$searchterm[1])
        }
      }
      
      if(!exists("fullpricedat")){
        if(exists("pricedat")&class(pricedat)=="data.frame"){
          fullpricedat <- pricedat
        }
      } else {
        if(exists("pricedat")&class(pricedat)=="data.frame"){
          fullpricedat <- combinefunc(fullpricedat,pricedat,hits=FALSE,rm_reference=coindf$searchterm[1])
        }
      }
      
      if(!exists("fullrisingdat")){
        if(exists("risingdat")){
          fullrisingdat <- risingdat
        }
      } else {
        if(exists("risingdat")){
          fullrisingdat <- combinefunc(fullrisingdat,risingdat,hits=FALSE,rm_reference=coindf$searchterm[1])
        }
      }
      
      if(!exists("fulltopdat")){
        if(exists("topdat")){
          fulltopdat <- topdat
        }
      } else {
        if(exists("topdat")){
          fulltopdat <- combinefunc(fulltopdat,topdat,hits=FALSE,rm_reference=coindf$searchterm[1])
        }
      }
      
      if(!exists("fullcountrydat")){
        if(exists("countrydat")){
          fullcountrydat <- countrydat
        }
      } else {
        if(exists("countrydat")){
          fullcountrydat <- combinefunc(fullcountrydat,countrydat,hits=FALSE,rm_reference=coindf$searchterm[1])
        }
      }
    }
    
    # clean, combine, and save final dataframes
    if(i==max(seq(2,nrow(coindf[-which(coindf$CoinName=="Bitcoin"),]),4))){
      
      # aggregate data for coins with multiple search terms (also adds coindate variable)
      fullhitsdat <- aggcoinfunc(fullhitsdat,coindf,hits=TRUE)
      fullpricedat <- aggcoinfunc(fullpricedat,coindf,hits=FALSE)
      fullrisingdat <- aggcoinfunc(fullrisingdat,coindf,hits=FALSE)
      fulltopdat <- aggcoinfunc(fulltopdat,coindf,hits=FALSE)
      fullcountrydat <- aggcoinfunc(fullcountrydat,coindf,hits=FALSE)
      
      # combine old and new data
      fullhitsdat <- combinefunc(hitsdatE,fullhitsdat,hits=TRUE,rm_reference = NA,final=TRUE)
      fullpricedat <- combinefunc(pricedatE,fullpricedat,hits=FALSE,rm_reference = NA,final=TRUE)
      fullrisingdat <- combinefunc(risingdatE,fullrisingdat,hits=FALSE,rm_reference = NA,final=TRUE)
      fulltopdat <- combinefunc(topdatE,fulltopdat,hits=FALSE,rm_reference = NA,final=TRUE)
      fullcountrydat <- combinefunc(countrydatE,fullcountrydat,hits=FALSE,rm_reference = NA,final=TRUE)
      
      # save to csv
      write.csv(fullhitsdat,file="~/Dropbox/Meltwater/fullhitsdat.csv",row.names = F)
      write.csv(fullpricedat,file="~/Dropbox/Meltwater/fullpricedat.csv",row.names = F)
      write.csv(fullrisingdat,file="~/Dropbox/Meltwater/fullrisingdat.csv",row.names=F)
      write.csv(fulltopdat,file="~/Dropbox/Meltwater/fulltopdat.csv",row.names=F)
      write.csv(fullcountrydat,file="~/Dropbox/Meltwater/fullcountrydat.csv",row.names=F)
      
      # save to SQL
      dbWriteTable(con,"fullhitsdat",fullhitsdat,overwrite=TRUE) # table to MySQL
      dbWriteTable(con,"fullpricedat",fullpricedat,overwrite=TRUE)
      dbWriteTable(con,"fullrisingdat",fullrisingdat,overwrite=TRUE)
      dbWriteTable(con,"fulltopdat",fulltopdat,overwrite=TRUE)
      dbWriteTable(con,"fullcountrydat",fullcountrydat,overwrite=TRUE)
    }
  }
  
  # clean environment and go to sleep
  killDbConnections()
  keep <- c("killDbConnections","scrapegoogle","scrapeprice","combinefunc","aggolddat","aggcoinfunc","getGoogle","geturls")
  rm(list=ls()[!ls()%in%keep])
  Sys.sleep(10000)
}
########################################################################################################################



