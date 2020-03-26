
################################################################################################
#################################### TESI LEROY MERLIN #########################################
################################################################################################

library(dplyr)
library(forecast)
library(xts)
library(RJDBC)
library(futile.logger)
library(optparse)
library(ggplot2)
library(gtools)
library(xlsxjars)
library(xlsx)
library(lubridate)
library(scales)
library(MLmetrics)
library(robustHD)
library(caret)
library(cluster)
library(magrittr)


# From Environment ETL
scontr_bologna
trans_bologna 
art_bologna

# Creating Dataset

hourly_transaction <- trans_bologna %>%
  mutate(ora=as.integer(HEU_TRN/10000)) %>%
  group_by(DAT_TRN,ora,NUM_ETT,NUM_TYPTRN) %>%
  inner_join(art_bologna,by=c('NUM_ART'='num_art')) %>%
  ungroup() %>%
  group_by(DAT_TRN,NUM_ETT,num_ray,NUM_TYPTRN,ora) %>%
  mutate(num_bollettini = ifelse(NUM_TYPTRN==4,n(),0),
         num_preventivi=ifelse(NUM_TYPTRN==2,n(),0),
         num_ordini=ifelse(NUM_TYPTRN==1,n(),0)) %>%
  ungroup() %>%
  group_by(DAT_TRN,NUM_ETT,num_ray,ora) %>%
  arrange(DAT_TRN,ora) %>%
  summarise(num_bollettini=sum(unique(num_bollettini)),num_preventivi=sum(unique(num_preventivi)),
            num_ordini=sum(unique(num_ordini)))

hourly_transaction$DAT_TRN <- as.Date(hourly_transaction$DAT_TRN,'%Y-%m-%d')

hourly_scontrini <- scontr_bologna %>%
  filter(cod_typvte==1 ) %>%
  mutate(ora=as.integer(heu_tic/10000)) %>%
  group_by(dat_vte,num_ett,num_ray,ora) %>%
  summarise(scontr_tot=n_distinct(num_tic),
            prodotti=sum(qte_art),
            turnover=sum(prx_vtemag*qte_art))

hourly_scontrini$dat_vte <-  as.Date(hourly_scontrini$dat_vte,'%Y-%m-%d')

hourly_timbr <- timbrature_tot %>%
  filter(num_ett==47 & num_ray==5) %>%
  group_by(data_timbr,num_ett,num_ray,ore) %>%
  summarise(pers=n_distinct(matricola))

hourly_timbr$data_timbr <- as.Date(hourly_timbr$data_timbr,'%Y-%m-%d')

calendar <- seq(as.Date(min(hourly_timbr$data_timbr)),
                as.Date(max(hourly_timbr$data_timbr)), "days")

mese <- month(calendar)

giorno <- as.numeric(format(calendar, '%u'))

calendar_hourly <- as.character( format(seq(from = as.POSIXlt(min(hourly_timbr$data_timbr)),
                                            to = as.POSIXlt(max(hourly_timbr$data_timbr)), by = "hour"),'%Y-%m-%d:%H'))

new <- do.call( rbind , strsplit( as.character( calendar_hourly ) , ":" ) )

calendarOrario <- as.data.frame(cbind( Date = new[,1] , ora = as.integer (new[,2]) ))

calendarOrario$ora <-  as.integer(calendarOrario$ora)
calendarOrario$Date <- as.Date(calendarOrario$Date,'%Y-%m-%d')

Calendar <- data_frame(calendar,giorno,mese)

calendarTrans <- calendarOrario %>%
  filter(ora >= 6 & ora <= 20 ) %>%
  inner_join(Calendar,by=c('Date'='calendar')) %>%
  arrange(Date,ora)

full_calendar <- as.data.frame(calendarTrans %>%
                                 mutate(num_ett=rep(hourly_timbr$num_ett[1],nrow(calendarTrans)),
                                        num_ray=rep(hourly_timbr$num_ray[1],nrow(calendarTrans))))
full_calendar$Date <- as.Date(full_calendar$Date,'%Y-%m-%d')

hourly_df <- full_calendar %>%
  left_join(hourly_timbr,by=c('Date'='data_timbr','ora'='ore','num_ett','num_ray')) %>%
  left_join(hourly_scontrini,by=c('Date'='dat_vte','num_ett','num_ray','ora')) %>%
  left_join(hourly_transaction,by=c('Date'='DAT_TRN','num_ett'='NUM_ETT','num_ray','ora'))

hourly_df %<>%
  as_tibble() %>%
  replace_na(replace=list(pers=0,scontr_tot=0,num_preventivi=0,num_bollettini=0,num_ordini=0,prodotti=0,turnover=0))

flog.info("Added all missing days to receipt Dataframe")

####### FULL CALENDAR RETRIEVE
features<- c("scontr_tot","prodotti","turnover","num_bollettini","num_preventivi","num_ordini")
remove(std_hourly)
#for (variables in names(hourly_df)[c(8:13)]) {
for (variables in features) {
  flog.info(paste0('Standardizing variable ',variables,' for clustering'))
  
  scaled_huorly <- as.data.frame (scales::rescale(hourly_df[[variables]], to=c(0,1)))
  #scaled_huorly <- as.data.frame(diff(hourly_df[[variables]]))
  names(scaled_huorly)<- as.character(variables)
  
  if (exists('std_hourly')){
    std_hourly <- as.data.frame(cbind(std_hourly,scaled_huorly))
  }  else {
    std_hourly <- as.data.frame(scaled_huorly)
  }
  
}

# K-means clustering with 10 clusters of sizes 160, 586, 358, 1540, 105, 520, 1648, 1346, 623, 1091
#
# Cluster means:
#   scontr_tot    prodotti    turnover num_bollettini num_preventivi  num_ordini
# 1  0.74958882 0.647370089 0.513377608    0.304821429   0.0225543478 0.070200893
# 2  0.43649332 0.315998131 0.247798965    0.125012189   0.0100163229 0.032819356
# 3  0.57639910 0.444845678 0.353784639    0.212370311   0.0061938305 0.048184358
# 4  0.12273012 0.069901075 0.046702390    0.036066790   0.0037267081 0.007339981
# 5  0.32222222 0.220978937 0.174837081    0.536326531   0.0004140787 0.030952381
# 6  0.33568657 0.226793897 0.187793228    0.239835165   0.0061872910 0.026888736
# 7  0.01259368 0.006437884 0.009825853    0.004178225   0.0000000000 0.002351335
# 8  0.22864042 0.143153351 0.098018556    0.046529399   0.0030040700 0.014885375
# 9  0.20075470 0.120694461 0.080941895    0.182939693   0.0036290041 0.011637239
# 10 0.31864779 0.213984914 0.171949599    0.077360220   0.0082094608 0.031589629

std_hourly_df <- hourly_df
std_hourly_df$scontr_tot <-std_hourly$scontr_tot
std_hourly_df$prodotti<-std_hourly$prodotti
std_hourly_df$num_bollettini<-std_hourly$num_bollettini
std_hourly_df$num_preventivi<-std_hourly$num_preventivi
std_hourly_df$num_ordini <-std_hourly$num_ordini
std_hourly_df$turnover <- std_hourly$turnover

# Assegnazione delle persone ai cluster
#silhouette(std_hourly)
variables<- list('scontr_tot','prodotti','turnover','num_bollettini','num_preventivi','num_preventivi','num_ordini')

std_kmedie <- kmeans(std_hourly_df[,8:13],max(std_hourly_df$pers))
std_kmedie <- clara(std_hourly_df[,8:13],length(unique(std_hourly_df$pers)))
std_kmedie <- pam(std_hourly_df[,8:13],max(std_hourly_df$pers))

#std_kmedie <- clara(std_hourly_df[,6:11],5)

#std_kmedie <- pam(std_hourly_df[,6:11],length(pca$center))


#std_kmedie <- flexclust::kcca(std_hourly_df[,6:11],max(std_hourly_df$pers),family =  kccaFamily("kmedians",which = "kmedians"))
pca<-prcomp(std_hourly_df[,6:11])
length(pca$center)
summary(std_kmedie)
std_kmedie$clustering

# Call:	 clara(x = std_hourly_df[, 6:11], k = max(std_hourly_df$pers))
# Medoids:
#   scontr_tot   prodotti   turnover num_bollettini num_preventivi num_ordini
# [1,] 0.00000000 0.00000000 0.00000000     0.00000000     0.00000000 0.00000000
# [2,] 0.06578947 0.04055767 0.02179797     0.00000000     0.00000000 0.00000000
# [3,] 0.25000000 0.16856781 0.09217027     0.11428571     0.00000000 0.00000000
# [4,] 0.12719298 0.08745247 0.03684067     0.02857143     0.00000000 0.00000000
# [5,] 0.23684211 0.13688213 0.09741285     0.05714286     0.00000000 0.01785714
# [6,] 0.20614035 0.14575412 0.05152333     0.14285714     0.69565217 0.00000000
# [7,] 0.37719298 0.26742712 0.21263503     0.14285714     0.00000000 0.00000000
# [8,] 0.61842105 0.43472750 0.42005242     0.17142857     0.00000000 0.05357143
# [9,] 0.70175439 0.66539924 0.54999544     0.28571429     0.08695652 0.08928571
# [10,] 0.82456140 0.57667934 0.64918049     0.60000000     0.00000000 0.10714286
# Objective function:	 0.09443068
# Clustering vector: 	 int [1:7977] 1 1 1 2 3 3 3 3 3 3 3 3 3 3 4 1 1 1 ...
# Cluster sizes:	    	 1345 659 1163 1317 1370 20 1556 399 123 25
# Best sample:
#   [1]   53  241  322  495  543  592 1064 1183 1196 1304 1348 1394 1424 1665 1757 2166 2700 2724 2806 2814 2882 2887 3196
# [24] 3396 3405 3483 3618 3621 3963 4022 4137 4209 4316 4328 4597 4722 4762 5087 5097 5160 5369 5553 5748 5767 5900 5963
# [47] 6019 6041 6119 6543 6577 6718 6726 6799 6890 7134 7255 7263 7350 7417

std_hourly_df['cluster']=std_kmedie$cluster
#for PAM and CLARA
std_hourly_df['cluster']=std_kmedie$clustering

std_hourly_df %>%
  dplyr::filter(cluster==2) %>%
  summary()

std_hourly_df %>%
  ggplot(aes(x=pers)) +
  geom_histogram(binwidth=.5, colour="black", fill="orange") +
  facet_grid(cluster~.) + 
  ggtitle('Discrete distribution of Sales Advisors \nper each cluster\n ') 

hourly_df %>%
  dplyr::select(scontr_tot,pers) %>%
  ggplot(aes(x=scontr_tot))+
  geom_histogram()+
  facet_grid(pers~.)

std_hourly_clus_df <- std_hourly_df %>%
  group_by(cluster) %>%
  mutate(pers_hat=median(pers))

###### numero ottimale con la moda

getmode <- function(v) {
       uniqv <- unique(v)
       uniqv[which.max(tabulate(match(v, uniqv)))]
   }


std_hourly_clus_df <- std_hourly_df %>%
    group_by(cluster) %>%
    mutate(pers_hat=getmode(pers))

######################################################

std_hourly_df %>% 
  write.csv(file="std_hourly_df.csv",sep = ';')

# std_hourly_clus_df %>%
#   filter(ore==6) %>%
#   distinct(pers)

std_hourly_clus_df %>%
  ggplot(aes(x=pers_hat)) +
  geom_histogram(binwidth=.5, colour="black", fill="green") +
  facet_grid(cluster~.) + 
  ggtitle('Median value per each cluster \n ') 


View(std_hourly_clus_df)

plot(std_kmedie$id.med)
# define an 80%/20% train/test split of the dataset
split=0.70
trainIndex <- createDataPartition(std_hourly_clus_df$pers_hat, p=split, list=FALSE)
data_train <- std_hourly_clus_df[ trainIndex,]

data_test <- std_hourly_clus_df[-trainIndex,]
View(data_train)
##### REAPETED CROSS VALIDATION #####

# define training control
train_control <- trainControl(method="repeatedcv", number=3, repeats=3)

# train the model
model <- caret::train(as.factor(pers_hat)~num_bollettini+num_preventivi+turnover+scontr_tot+num_preventivi+num_ordini+prodotti,
                      data=data_train, trControl=train_control, method="nnet")

modelRF <- caret::train(as.factor(pers_hat)~num_bollettini+num_preventivi+turnover+scontr_tot+num_preventivi+num_ordini+prodotti,
                      data=data_train, trControl=train_control, method="rf")

pred_model<-predict(model,data_test[,c(8:13)])
#pred_model<-predict(modelRF,data_test[,c(8:13)])

#predict(model,pred_df)
y_test <- data_test %>%
  ungroup() %>%
  dplyr::select(pers_hat) %>%
  mutate(pers_hat=as.factor(pers_hat))

confusionMatrix(pred_model,y_test$pers_hat)
accuracy<-confusionMatrix(pred_model,y_test$pers_hat)

flog.info (paste0('Accuracy of the model\n',accuracy$overall[1]))


#####################################################################
################# CONCATENATE HOURLY ################################
#####################################################################

std_hourly_df %<>%
  filter(ora==14)

remove(pred_df)
for (covariates in names(std_hourly_df)[c(8:13)]){
  flog.info('WORKING ON COVARIATA: %s',covariates)
  
  if (length(unique(std_hourly_df[[covariates]]))==1){
    flog.info('NO DATA DIFFERENT FROM 0 FOR COVARIATA: %s',covariates)
    
    pers_estimated <- as.data.frame(rep(0,30))
  } else{
    
    flog.info( paste0("Starting estimate with Neural Network the Covariate ",covariates))
    #NN <- ts(as.data.frame(std_hourly_df)[[covariates]],start=c(2017,01,02),frequency=365.25)
    NN <- diff(ts(as.data.frame(std_hourly_df)[[covariates]],start=c(2017,01,02),frequency=365.25))
    NN_model <- nnetar(as.numeric(NN),scale.inputs = F, P=1, p=21,size=11)#,P=1,p=21,lambda = "auto",size = 11,repeats =50)#,
    # xreg = scaled$giorno)
    flog.info(paste0('Model Built for variable ',covariates,' for hour(',8,')'))
    fcst.NN <-  forecast(NN_model,PI=T,h=30)#,xreg = scaled$giorno)
    #autoplot(fcst.NN$mean)
    
    pers_estimated <- as.data.frame(fcst.NN$mean)
    flog.info(paste0("Forecast NN for covariate ",covariates," --> done"))
  }
  if (exists('pred_df')){
    pred_df <- as.data.frame(cbind(pred_df,pers_estimated))
  }  else {
    pred_df <- as.data.frame(pers_estimated)
  }
}
names(pred_df)<-c('scontr_tot','prodotti','turnover','num_bollettini','num_preventivi','num_ordini')

flog.info("Predicted Dataset of Covariates --> BUILT")

autoplot(forecast(NN_model,PI = T,h=30),xlab='Times Series interval',
         ylab='Standardized values',main='Forecast from NNAR(21,11)\nfor predictor Receipts')


forecastHours<- predict(model,pred_df)
forecastHours<- predict(modelRF,pred_df)
if (exists('planning')){
  planning <- data.frame(planning,cbind(forecastHours))
}  else {
  planning <-as.data.frame(forecastHours)
}

print(planning)
# planning <- data.frame(planning,rbind(forecastHours),row.names = orario)
orario=1
orario<- as.character(orario)
planning %>% rename(!!orario:= forecastHours)
orario='ciao'



timbrature_tot %>%
  filter(num_ett==5 & num_ray==9)

library(ggplot2)
timbrature_tot %>%
  filter(num_ett==5 ,
         num_ray==9 ,
         data_timbr>='2018-05-01') %>%
  group_by(data_timbr, ore) %>%
  summarise(n=n()) %>%
  ungroup() %>%
  arrange(data_timbr,ore) %>%
  mutate(positionInCategory = 1:n())  %>%
  ggplot(aes(x=positionInCategory,y=n))+
  geom_line()

timbrature_tot %>%
  filter(num_ett==5 ,
         num_ray==9 ,
         data_timbr>='2018-05-01',
         ore<=16) %>%
  group_by(data_timbr, ore) %>%
  summarise(n=n()) %>%
  ggplot(aes(x=n))+
  geom_histogram()+
  facet_grid(.~ore)

timbrature_tot %>%
  filter(num_ett==5 ,
         num_ray==9 ,
         data_timbr>='2018-05-01',
         ore<=16) %>%
  group_by(data_timbr,ore) %>%
  summarise(n=n()) %>%
  ungroup() %>%
  group_by(ore) %>%
  summarise(med=median(n),
            quant=quantile(n,0.75),
            mean=mean(n))

std_hourly_clus_df %>% group_by(cluster,pers) %>%
  summarise(n=n()) %>%
  ungroup() %>%
  group_by(cluster) %>%
  filter(n==max(n))

summarise(med=median(pers),
          quant=quantile(pers,0.75),
          mean=mean(pers))
