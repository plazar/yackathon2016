library(rjson)
library(plyr)
library(dplyr)

# URLs of datasets
url <- "/home/gsumit/Desktop/yelp/yelp_academic_dataset_business.json"
url <- "/home/gsumit/Desktop/yelp/yelp_academic_dataset_checkin.json"
url <- "/home/gsumit/Desktop/yelp/yelp_academic_dataset_review.json"

# Parsing json data
con <- file(url, "r")
input <- readLines(con, -1L)
my_results <- lapply(X=input,fromJSON)
close(con)
yelp_business <- ldply(lapply(input, function(x) t(unlist(fromJSON(x)))))
yelp_checkin <- ldply(lapply(input, function(x) t(unlist(fromJSON(x)))))

# Cleaning empty strings
yelp_business[yelp_business == ""] <- NA

# Subsetting state by QC
yelp_business_montreal <- subset(yelp_business, as.character(state) == "QC")

# Aggregating data by city and pruning the set
agg <- as.data.frame(count(distinct(yelp_business_montreal), city))
agg$city <- as.character(agg$city)
agg <- agg[order(agg$n, decreasing = T),]

# Final pruned list of cities
city_list <- subset(agg, n >= 10, select = city)

# Subsetting by city_list
yelp_business_montreal <- yelp_business_montreal[yelp_business_montreal$city %in% city_list$city,]

# Merging business & checkin data
yelp_total <- join(yelp_business_montreal, yelp_checkin, by = "business_id", type = "inner")

# Mashing the datasets business info, transport and clusters and joining by "business_id"
yelp_total <- read.csv("/home/gsumit/Desktop/Tinni-resumes/yelp/yelp_total.csv", header = T, stringsAsFactors = F)
yelp_transport <- read.csv("/home/gsumit/Desktop/Tinni-resumes/yelp/closest_bus-metro_stops.csv", header = T, stringsAsFactors = F)
yelp_clusters <- read.csv("/home/gsumit/Desktop/Tinni-resumes/yelp/RestaurantsWithClusters.csv", header = T, stringsAsFactors = F)
yelp_total <- join(yelp_total, yelp_transport, by = "business_id", type = "inner", match = "all")
yelp_total <- join(yelp_total, yelp_clusters, by = "business_id", type = "inner", match = "all")
yelp_total$X <- NULL
yelp_restaurant <- yelp_total

# Summarizing the amount of missing data
propmiss <- function(dataframe) 
{
    m <- sapply(dataframe, function(x) 
       {
          data.frame(nmiss=sum(is.na(x)),n=length(x),propmiss=sum(is.na(x))/length(x))
        })
    d <- data.frame(t(m))
    d <- sapply(d, unlist)
    d <- as.data.frame(d)
    d$variable <- row.names(d)
    row.names(d) <- NULL
    d <- cbind(d[ncol(d)],d[-ncol(d)])
    return(d[order(d$propmiss), ])
    }
rest_missing <- propmiss(yelp_restaurant)

# aggregating the hourly checkins to weekly checkins
checkin_vars <- grep("checkin_info", names(yelp_restaurant), value = T)
yelp_chek <- yelp_restaurant[,checkin_vars]
yelp_restaurant$total_checkin <- rowSums(yelp_chek, na.rm = T)

# aggregating the hourly open hours to weekly open hours
open_vars <- grep("hours", names(yelp_restaurant), value = T)
yelp_open <- yelp_restaurant[,open_vars]
yelp_open[,open_vars] <- sapply(yelp_open,function(x) strptime(x,format = "%H:%M"))
yelp_open$hours_fri <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,2], yelp_open[,1], units = "hours"))))
yelp_open$hours_tues <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,4], yelp_open[,3], units = "hours"))))                              
yelp_open$hours_thurs <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,6], yelp_open[,5], units = "hours"))))                                      
yelp_open$hours_wed <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,8], yelp_open[,7], units = "hours"))))
yelp_open$hours_mon <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,10], yelp_open[,9], units = "hours"))))
yelp_open$hours_sun <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,12], yelp_open[,11], units = "hours"))))
yelp_open$hours_sat <- sapply(yelp_open,function(x) abs(as.numeric(difftime(yelp_open[,14], yelp_open[,13], units = "hours"))))
yelp_open[,15:21] <- as.numeric(unlist(yelp_open[15:21]))
yelp_open$hours_open <- rowSums(yelp_open[,15:21], na.rm = T)
yelp_open$hours_open[yelp_open$hours_open == 0.0] <- NA
means <- colMeans(yelp_open[,15:22], na.rm = T)
yelp_open$hours_open <- ifelse(is.na(yelp_open$hours_open), means[[8]], yelp_open$hours_open)
yelp_restaurant$hours_open <- yelp_open$hours_open

# Creating a new variable "success_factor" which is the target variable
yelp_restaurant$success_factor <- yelp_restaurant$total_checkin/yelp_restaurant$hours_open
specify_decimal <- function(x, k) format(round(x, k), nsmall=k)
yelp_restaurant$success_factor <- as.numeric(specify_decimal(yelp_restaurant$success_factor, 2))
yelp_restaurant <- yelp_restaurant[,!names(yelp_restaurant) %in% checkin_vars]
yelp_restaurant <- yelp_restaurant[,!names(yelp_restaurant) %in% open_vars]
keep_var <- subset(rest_missing, propmiss <= 0.5, select = variable)
yelp_restaurant <- yelp_restaurant[,names(yelp_restaurant) %in% keep_var[,1]]

# Creating a new variable "popularity_factor"
yelp_restaurant$popularity_factor <- yelp_restaurant$stars/yelp_restaurant$review_count
yelp_restaurant$popularity_factor <- as.numeric(specify_decimal(yelp_restaurant$popularity_factor, 2))

# Pulling the cleaned data into R
rest_clean <- read.csv("/home/gsumit/Desktop/yelp/rest_clean.csv", header = T, stringsAsFactors = F)

# Further data cleaning, removal of unnecessary variables and outlier treatment
rest_clean[mapply(is.na, rest_clean)] <- "UNKNOWN"
rest_clean$categories1 <- NULL
rest_clean$categories2 <- NULL
rest_clean$city <- NULL
rest_clean$Density <- NULL
rest_clean$popularity_factor <- NULL
result <- filter(rest_clean,rest_clean$closest_bstop_dist > 0.01)
result1 <- filter(rest_clean,rest_clean$closest_mstop_dist > 0.04)
rest_clean <- rest_clean[!rest_clean$business_id %in% result$business_id,]
rest_clean <- rest_clean[!rest_clean$business_id %in% result1$business_id,]
nlist <- c("Anjou", "Hampstead", "Mercier-Hochelaga-Maisonneuve", "Montréal-Nord", "Westmount")
rest_clean <- rest_clean[!rest_clean$neighborhoods %in% nlist,]
rest_clean$popularity_factor <- rest_clean$popularity_factor/range(rest_clean$popularity_factor)
rest_clean$popularity_factor <- as.numeric(specify_decimal(rest_clean$popularity_factor, 2))
rest_clean$popularity_factor_density <- rest_clean$popularity_factor/rest_clean$Density
result <- filter(rest_clean,rest_clean$popularity_factor_density > 10000)
rest_clean <- rest_clean[!rest_clean$business_id %in% result$business_id,]

# Feature selection using Boruta
library(Boruta)
library(randomForest)
set.seed(123)
boruta.yelp <- Boruta(success_factor~.-business_id, data = rest_clean, doTrace = 2)
boruta.final <- TentativeRoughFix(boruta.yelp)
plot(boruta.final, xlab = "", xaxt = "n")
lz <-lapply(1:ncol(boruta.final$ImpHistory),function(i) boruta.final$ImpHistory[is.finite(boruta.final$ImpHistory[,i]),i])
names(lz) <- colnames(boruta.final$ImpHistory)
Labels <- sort(sapply(lz,median))
axis(side = 1,las = 2,labels = names(Labels), at = 1:ncol(boruta.final$ImpHistory), cex.axis = 0.7)
rejected.vars <- subset(rownames(df), df$decision == "Rejected")

# Retaining the variables confirmed by Boruta
rest_clean <- rest_clean[,!names(rest_clean) %in% rejected.vars]

# Fitting a linear regression model
set.seed(123)
lm.fit <- lm(success_factor~.-business_id, data = rest_clean)

     



