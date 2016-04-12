# Extracting data from shapefile into R

# Reading the shapefile into R
library(rgdal)
shapefile <- readOGR("/home/gsumit/Desktop/yelp/affectation_du_sol", "affectation_du_sol")

# Converting the shapefile to a data frame
yelp_shape <- fortify(shapefile)
shape_cat <- shapefile@data

# Adding a column "categorie" to the data frame
yelp_shape$categorie <- rep(NA, nrow(yelp_shape))
yelp_shape$id <- as.numeric(yelp_shape$id)
shape_cat$categorie <- as.character(shape_cat$categorie)
emplois_cat <- which(shape_cat$categorie == "emplois")
infrastructure_cat <- which(shape_cat$categorie == "infrastructure")
institution_cat <- which(shape_cat$categorie == "institution")
mixte_cat <- which(shape_cat$categorie == "mixte")
parc_cat <- which(shape_cat$categorie == "parc")
religieux_cat <- which(shape_cat$categorie == "religieux")
residentiel_cat <- which(shape_cat$categorie == "residentiel")
rural_cat <- which(shape_cat$categorie == "rural")
transport_cat <- which(shape_cat$categorie == "transport")
for(i in 1:nrow(yelp_shape)){
  yelp_shape$id[i] <- yelp_shape$id[i] + 1
}
yelp_shape$categorie <- ifelse(yelp_shape$id %in% emplois_cat, "emplois",
                               ifelse(yelp_shape$id %in% infrastructure_cat, "infrastructure",
                                      ifelse(yelp_shape$id %in% institution_cat, "institution",
                                             ifelse(yelp_shape$id %in% mixte_cat, "mixte",
                                                    ifelse(yelp_shape$id %in% parc_cat, "parc",
                                                           ifelse(yelp_shape$id %in% religieux_cat, "religieux",
                                                                  ifelse(yelp_shape$id %in% residentiel_cat, "residentiel",
                                                                         ifelse(yelp_shape$id %in% rural_cat, "rural",
                                                                                "transport"))))))))
