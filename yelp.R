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