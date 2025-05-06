
library(dplyr)
library(readr)
library(tidyr)
library(gamlss)

options(readr.show_col_types = FALSE, readr.show_progress = FALSE)

meta <- read_csv("results/selected_stations.csv")

meta <- meta |> filter(!intermittent & valid_for_trend_analysis)

# Filter UK stations only 
meta <- meta |> filter(ohdb_source == "NRFA")

# # Pick an ID at random 
# id <- meta$ohdb_id[sample(1:nrow(meta), 1)]
# x <- read_csv(file.path("results/low_flow_metrics", paste0(id, "_mam7d.csv")))

ids <- meta$ohdb_id 
v5_start <- rep(NA, length(ids))
v5_end <- rep(NA, length(ids)) 
nonstationary <- rep(FALSE, length(ids))

pb <- txtProgressBar(min = 0, max = length(ids))
for (i in 1:length(ids)) { 

  id <- ids[i] 
  x <- read_csv(file.path("results/low_flow_metrics", paste0(id, "_mam7d.csv")))
  mod_nonst <- try(gamlss(x$Q_accum_7d ~ x$water_year, sigma.formula = ~ x$water_year, family = "GA", trace = FALSE), silent = TRUE)
  mod_st <- try(gamlss(x$Q_accum_7d ~ 1, sigma.formula = ~ 1, family = "GA", trace = FALSE), silent = TRUE)
  if (inherits(mod_nonst, "try-error") | inherits(mod_st, "try-error")) { 
    next
  }
  aic_nonst <- AIC(mod_nonst)
  aic_st <- AIC(mod_st)
  if (aic_nonst < aic_st) { 
    best_model <- mod_nonst
    nonstationary[i] <- TRUE
  } else { 
    best_model <- mod_st
  }

  #  Extract parameters
  x$mu <- fitted(best_model, what = c("mu"))
  x$sigma <- fitted(best_model, what = c("sigma"))

  # Extract parameters - official method
  mu <- fitted(best_model, what = 'mu')
  sigma <- fitted(best_model, what = 'sigma')
  Q01 <-  qGA(0.01, mu, sigma)
  Q05 <- qGA(0.05, mu, sigma)
  Q20 <- qGA(0.20, mu, sigma)
  Q50 <- qGA(0.50, mu, sigma)
  Q99 <- qGA(0.99, mu, sigma)
  a <- tibble("water_year" = x$water_year,
              "Q01" = Q01,
              "Q05" = Q05,
              "Q20" = Q20,
              "Q50" = Q50,
              "Q99" = Q99,
              "obs" = x$Q_accum_7d)

  # Extract the magnitude of the 5-y, 20-y, 50-y and 100-y flow
  v5_end[i] <- a[a$water_year == max(a$water_year), "Q05", drop = TRUE]
  v5_start[i] <- a[a$water_year == min(a$water_year), "Q05", drop = TRUE]

  setTxtProgressBar(pb, i)
}

out <- tibble(id = ids, nonst = nonstationary, v5_end = v5_end, v5_start = v5_start) |>
  filter(!is.na(v5_end) & !is.na(v5_start)) |>
  filter(nonst)

out <- out |> mutate(drier = v5_start > v5_end, wetter = v5_start < v5_end) |> filter(drier)

stop() 

# TODO changepoint analysis

id <- "OHDB_011000173"
id <- "OHDB_011000162"
id <- "OHDB_011000028"
x <- read_csv(file.path("results/low_flow_metrics", paste0(id, "_mam7d.csv")))
best_model <- gamlss(x$Q_accum_7d ~ x$water_year, sigma.formula = ~ x$water_year, family = "GA", trace = FALSE)

#  Extract parameters
x$mu <- fitted(best_model, what = c("mu"))
x$sigma <- fitted(best_model, what = c("sigma"))

# Extract parameters - official method
mu <- fitted(best_model, what = 'mu')
sigma <- fitted(best_model, what = 'sigma')
Q01 <-  qGA(0.01, mu, sigma)
Q05 <- qGA(0.05, mu, sigma)
Q20 <- qGA(0.20, mu, sigma)
Q50 <- qGA(0.50, mu, sigma)
Q99 <- qGA(0.99, mu, sigma)
a <- tibble("water_year" = x$water_year,
            "Q01" = Q01,
            "Q05" = Q05,
            "Q20" = Q20,
            "Q50" = Q50,
            "Q99" = Q99,
            "obs" = x$Q_accum_7d)

# Make plot
p <- ggplot() + theme_bw() + xlab("") +
    # ylab(bquote('AMAX ('*m^3*'/'*s*'/'*km^2*')'))+ #instantaneous peak flow
    geom_ribbon(data = a, aes(x = water_year, ymin = Q01, ymax = Q99),  fill="ivory2")+
    geom_line(data = a, aes(x = water_year, y = Q01), col="grey50", size=1)+#lower is darker
    geom_line(data = a, aes(x = water_year, y = Q05), col="grey50", size=1)+#lower is darker
    geom_line(data = a, aes(x = water_year, y = Q20), col="red", size = 1.5)+
    # geom_line(data = a, aes(x = water_year, y = Q95), col="grey50", size=1)+
    geom_line(data = a, aes(x = water_year, y = Q99),col = "grey50", size = 1)+
    geom_point(data = a, aes(x=water_year, y=obs), pch=21, color="black", fill="grey50", size=2)
    # annotate(geom ='label', label = "1.01-year", x = 1900, y = 0.05,  size=4,col="grey30")+
    # annotate(geom ='label', label = "20-year", x = 1900, y = 0.113,  size=4,col="grey30")+
    # annotate(geom ='label', label = "50-year", x = 1907, y = 0.125,  size=4, col="red")+
    # annotate(geom ='label', label = "100-year", x = 1915, y = 0.1375,  size=4,col="grey30")+
    # annotate(geom ='text', label = "Red line indicates nonstationary 50-y flood",
    #          x = 1890, y = 0.19,  size=4.5,col="black", hjust = 0)+#hjust 0 is left align
    # annotate(geom ='text', label = "in every year",
    #          x = 1890, y = 0.175,  size=4.5,col="black", hjust = 0)+
    # scale_x_continuous(lim=c(1886,2020),
    #                  breaks=seq(1850,2020,10),
    #                  expand=c(0,0))+
    # # coord_cartesian(xlim=c(1920,2020))+
    # theme(legend.position = c(0.2, 0.8),
    #   axis.text.x=element_blank(),
    #   legend.title = element_blank(),
    #   plot.background = element_rect(fill = "transparent", color = NA),
    #   plot.margin=unit(c(1,5,-10,1),"mm"),# top, right, bottom and left.
    #   plot.tag=element_text(size=20))+labs(tag="a")


# v50_1950 = round(a[a$Year==1950,"Q98"],6)
# v50_1970 = round(a[a$Year==1970,"Q98"],6)
# v50_2015 = round(a[a$Year==2015,"Q98"],6)

# v100_1950 = round(a[a$Year==1950,"Q99"],6)
# v100_1970 = round(a[a$Year==1970,"Q99"],6)
# v100_2015 = round(a[a$Year==2015,"Q99"],6)

# if (length(v20_1950)==0) {v20_1950 = NA}
# if (length(v50_1950)==0) {v50_1950 = NA}
# if (length(v100_1950)==0) {v100_1950 = NA}

# df$y100 <- a$Q99
# df$y50 <- a$Q98
 
# partA <-
#     ggplot()+ theme_bw()+ xlab("")+
#     ylab(bquote('AMAX ('*m^3*'/'*s*'/'*km^2*')'))+ #instantaneous peak flow
#     geom_ribbon(data=a, aes(x=Year, ymin=Q01, ymax=Q99),  fill="ivory2")+
#     geom_line(data = a, aes(x=Year, y=Q01),col="grey50", size=1)+#lower is darker
#     geom_line(data = a, aes(x=Year, y=Q95),col="grey50", size=1)+
#     geom_line(data = a, aes(x=Year, y=Q98),col="red", size=1.5)+
#     # geom_point(data = a, aes(x=Year,y=Q98),col="grey50")+#red
#     geom_line(data = a, aes(x=Year, y=Q99),col="grey50", size=1)+

#     geom_point(data = df, aes(x=Year, y=Q),
#              pch=21, color="black", fill="grey50", size=2)+
#     annotate(geom ='label', label = "1.01-year", x = 1900, y = 0.05,  size=4,col="grey30")+
#     annotate(geom ='label', label = "20-year", x = 1900, y = 0.113,  size=4,col="grey30")+
#     annotate(geom ='label', label = "50-year", x = 1907, y = 0.125,  size=4, col="red")+
#     annotate(geom ='label', label = "100-year", x = 1915, y = 0.1375,  size=4,col="grey30")+

#     annotate(geom ='text', label = "Red line indicates nonstationary 50-y flood",
#              x = 1890, y = 0.19,  size=4.5,col="black", hjust = 0)+#hjust 0 is left align
#     annotate(geom ='text', label = "in every year",
#              x = 1890, y = 0.175,  size=4.5,col="black", hjust = 0)+
#     scale_x_continuous(lim=c(1886,2020),
#                      breaks=seq(1850,2020,10),
#                      expand=c(0,0))+
#     # coord_cartesian(xlim=c(1920,2020))+
#     theme(legend.position = c(0.2, 0.8),
#       axis.text.x=element_blank(),
#       legend.title = element_blank(),
#       plot.background = element_rect(fill = "transparent", color = NA),
#       plot.margin=unit(c(1,5,-10,1),"mm"),# top, right, bottom and left.
#       plot.tag=element_text(size=20))+labs(tag="a")

# # ====================================================================
# # CALCULATE CONFIDENCE INTERVALS

# data = data.frame(df[,c("Year","Q")]) # a data frame containing the variables occurring in the formula.
# xname = "Year" #  name of the unique explanatory variable (it has to be the same as in the original fitted model) 
# xvalues = as.numeric(df$Year) #seq(1948,2015,1)
# cent = c(95,98,99)# a vector with the % centile values to be evaluated
# B = 1000 #the number of bootstraps

# obj <- gamlss(Q~Year,sigma.formula=~Year,data=data,family="GA",trace=FALSE)

# bootresults <- centiles.boot(obj, data =data, xname=xname, xvalues=xvalues, cent=cent, parallel="TRUE",B=B)

# df$c1_1970_20 <- summary(bootresults, fun="quantile", 0.05)[,"95"] #0.025
# df$c2_1970_20 <- summary(bootresults, fun="quantile", 0.95)[,"95"] #0.975

# df$c1_1970_50 <- summary(bootresults, fun="quantile", 0.05)[,"98"] #0.025
# df$c2_1970_50 <- summary(bootresults, fun="quantile", 0.95)[,"98"] #0.975

# df$c1_1970_100 <- summary(bootresults, fun="quantile", 0.05)[,"99"] #0.025
# df$c2_1970_100 <- summary(bootresults, fun="quantile", 0.95)[,"99"] #0.975

# v20_1970_c1 <-  as.numeric(df[df$Year==1970,"c1_1970_20"])
# v20_1970_c2 <-   as.numeric(df[df$Year==1970,"c2_1970_20"])

# v50_1970_c1 <-  as.numeric(df[df$Year==1970,"c1_1970_50"])
# v50_1970_c2 <-   as.numeric(df[df$Year==1970,"c2_1970_50"])

# v100_1970_c1 <-  as.numeric(df[df$Year==1970,"c1_1970_100"])
# v100_1970_c2 <-   as.numeric(df[df$Year==1970,"c2_1970_100"])

# v20_2015_c1 <-  as.numeric(df[df$Year==2015,"c1_1970_20"])
# v20_2015_c2 <-as.numeric(df[df$Year==2015,"c2_1970_20"])

# v50_2015_c1 <-  as.numeric(df[df$Year==2015,"c1_1970_50"])
# v50_2015_c2 <-   as.numeric(df[df$Year==2015,"c2_1970_50"])

# v100_2015_c1 <-  as.numeric(df[df$Year==2015,"c1_1970_100"])
# v100_2015_c2 <-  as.numeric( df[df$Year==2015,"c2_1970_100"])
  
# # ====================================================================


# aaa = round(v50_1970,2)
# bbb = round(v50_2015,2)
# # Just extracting the 50-year flood magnitude from previous plot
# partB <-
#   ggplot(data= df)+ theme_bw()+
#   geom_ribbon(aes(x=Year, ymin=c1_1970_50, ymax=c2_1970_50),  fill="ivory2")+
#   geom_line(aes(x=Year, y=y50),col="red", size=1.5)+
#   geom_line(aes(x=Year, y=c1_1970_50), linetype="dashed")+
#   geom_line(aes(x=Year, y=c2_1970_50), linetype="dashed")+
#   geom_point(data= df[df$Year==1970,], aes(x=Year, y=y50), 
#              col="black", pch=21,fill="red", size=3)+
#   geom_point(data= df[df$Year==2015,], aes(x=Year, y=y50), 
#              col="black", pch=21,fill="red", size=3)+
#   annotate(geom ='text', label= paste0("The 50-year flood magnitude has increased") , 
#            x = 1890, y = 0.19,  size=4.5,col="black", hjust=0)+
#   annotate(geom ='text', 
#            label= paste0("from ",aaa," (1970) to ",bbb," cms/km2 (2015)") , 
#            x = 1890, y = 0.18,  size=4.5,col="black", hjust=0)+
#   xlab("")+
#   ylab(bquote('50-y AMAX ('*m^3*'/'*s*'/'*km^2*')'))+ # * == no space, ~ space
#   scale_x_continuous(lim=c(1886,2020),
#                    breaks=seq(1850,2020,10),
#                    expand=c(0,0))+
#   theme(plot.margin=unit(c(1,5,-10,1),"mm"),# top, right, bottom and left.
#         axis.text.x=element_blank(),
#         plot.background = element_rect(fill = "transparent", color = NA),
#         plot.tag=element_text(size=20))+
#   labs(tag="b")


# # ===========================================================================
# # NEXT STEP IS TO CALCULATE THE RECURRENCE INTERVAL

# # Also calculate the recurrence for 1970 20-y flood
# df$Recurrence1970_20 <- 1/(1-pGA(v20_1970, mu = df$mu , sigma = df$sigma ))
# df$Recurrence1970_20_c1 <- 1/(1-pGA(v20_1970_c1, mu = df$mu , sigma =  df$sigma ))
# df$Recurrence1970_20_c2 <- 1/(1-pGA(v20_1970_c2, mu = df$mu , sigma = df$sigma ))  

# # Estimate the probability of the 1970 50-year flood given the distribution for in every year
# df$Recurrence1970_50 <- 1/(1-pGA(v50_1970, mu = df$mu , sigma = df$sigma ))
# df$Recurrence1970_50_c1 <- 1/(1-pGA(v50_1970_c1, mu = df$mu , sigma = df$sigma ))
# df$Recurrence1970_50_c2 <- 1/(1-pGA(v50_1970_c2, mu = df$mu , sigma = df$sigma ))  

# # Also calculate the recurrence for 1970 100-y flood
# df$Recurrence1970_100 <- 1/(1-pGA(v100_1970, mu = df$mu , sigma = df$sigma ))
# df$Recurrence1970_100_c1 <- 1/(1-pGA(v100_1970_c1, mu = df$mu , sigma = df$sigma ))
# df$Recurrence1970_100_c2 <- 1/(1-pGA(v100_1970_c2, mu = df$mu , sigma = df$sigma ))  


# RI20y1970 <- round(as.numeric(df[df$Year =="1970" , "Recurrence1970_20"]),3)      
# RI20y1970in2015 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_20"]),3)
# RI20y1970in2015_c1 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_20_c1"]),3)
# RI20y1970in2015_c2 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_20_c2"]),3)

# RI50y1970 <- round(as.numeric(df[df$Year =="1970" , "Recurrence1970_50"]),3)
# RI50y1970in2015 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_50"]),3)
# RI50y1970in2015_c1 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_50_c1"]),3)
# RI50y1970in2015_c2 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_50_c2"]),3)

# RI100y1970 <- round(as.numeric(df[df$Year =="1970" , "Recurrence1970_100"]),3)
# RI100y1970in2015 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_100"]),3)
# RI100y1970in2015_c1 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_100_c1"]),3)
# RI100y1970in2015_c2 <- round(as.numeric(df[df$Year =="2015" , "Recurrence1970_100_c2"]),3)

# # Make figure
# paste0("The 50-year flood (in 1970) now recurs every ",round(RI50y1970in2015,2)," years (in 2015)")
# paste0("The 20-year flood (in 1970) now recurs every ",round(RI20y1970in2015,2)," years (in 2015)")

# partC <-
#     ggplot(data=df)+ theme_bw()+
#     geom_ribbon(aes(x=Year, ymin=Recurrence1970_50_c1, ymax=Recurrence1970_50_c2),  fill="ivory2")+
#     geom_line(aes(x=Year, y=Recurrence1970_50),col="red", size=1.5)+
#     geom_line(aes(x=Year, y=Recurrence1970_50_c1), linetype="dashed")+
#     geom_line(aes(x=Year, y=Recurrence1970_50_c2), linetype="dashed")+
#     geom_point(data= df[df$Year==1970,], aes(x=Year, y=Recurrence1970_50), 
#                col="black", pch=21,fill="red", size=3)+
#     geom_point(data= df[df$Year==2015,], aes(x=Year, y=Recurrence1970_50), 
#                col="black", pch=21,fill="red", size=3)+
#     annotate(geom ='text', label= paste0("The former 50-year flood (from 1970)") , 
#              x = 1930, y = 11000,  size=4.5,col="grey10", hjust = 0)+
#     annotate(geom ='text', label= paste0("now recurs every ",round(RI50y1970in2015,2)," years (in 2015)") , 
#              x = 1930, y = 5000,  size=4.5,col="grey10", hjust = 0)+  #h-just means left align
#     ylab("Return period (years)")+
#     scale_y_log10() +
#     scale_x_continuous(lim=c(1886,2020),
#                      breaks=seq(1850,2020,10),
#                      expand=c(0,0))+
#    # coord_cartesian(xlim=c(1920,2020))+
#     theme(plot.margin=unit(c(1,5,1,1),"mm"),# top, right, bottom and left.
#           plot.background = element_rect(fill = "transparent", color = NA),
#           plot.tag=element_text(size=20),
#           axis.title.x=element_blank())+
#   labs(tag="c")




# # ========================================================
# # Let's have a look at probabiilities

# # probability of startyearinendyear
# ss = (100/RI50y1970in2015)/100
# ss1 = (1-((1-ss )^30))*100

# df$a <- (100/df$Recurrence1970_50)/100
# df$a1 <- (100/df$Recurrence1970_50_c1)/100
# df$a2 <- (100/df$Recurrence1970_50_c2)/100
# df$newprob <- (1-((1-df$a )^30))*100
# df$newprob_C1 <- (1-((1-df$a1 )^30))*100
# df$newprob_C2 <- (1-((1-df$a2 )^30))*100

# partD <- ggplot(data=df)+ theme_bw()+
#   geom_ribbon(aes(x=Year, ymin=newprob_C1, ymax=newprob_C2),  fill="ivory2")+
#   geom_line(aes(x=Year, y=newprob_C1), linetype="dashed")+
#   geom_line(aes(x=Year, y=newprob_C2), linetype="dashed")+
#   geom_line(aes(x=Year, y=newprob),col="red", size=1.5)+
#   geom_point(data= df[df$Year==1970,], aes(x=Year, y=newprob), 
#              col="black", pch=21,fill="red", size=3)+
#   geom_point(data= df[df$Year==2015,], aes(x=Year, y=newprob), 
#              col="black", pch=21,fill="red", size=3)+
#   annotate(geom ='text', label= paste0("The probability of the former 50-year flood (from 1970)") , 
#            x = 1900, y = 95,  size=4.5,col="grey10", hjust = 0)+
#   annotate(geom ='text', label= paste0("occurring in a 30-y window is now ",round(ss1,2)," %") , 
#            x = 1900, y = 85,  size=4.5,col="grey10", hjust = 0)+  #h-just means left align
#   ylab("Probability (y)")+
#   scale_x_continuous(lim=c(1886,2020),
#                      breaks=seq(1850,2020,10),
#                      expand=c(0,0))+
#   # coord_cartesian(xlim=c(1920,2020))+
#   theme(plot.margin=unit(c(1,5,1,1),"mm"),# top, right, bottom and left.
#         plot.background = element_rect(fill = "transparent", color = NA),
#         plot.tag=element_text(size=20),
#         axis.title.x=element_blank())+
#   labs(tag="d")

# # ===============================================================
# # Combined plot

# library(cowplot)
# right_plot <- plot_grid(partA, partB, partC, partD,
#                         ncol=1, rel_heights = c(0.25,0.25,0.25,0.25), align="v")

# # 
# # plot.new()
# # png(paste0("./Site_27009.png"),
# #     width = 20, height=24, res=300, units="cm", bg = "transparent") 
# # print(right_plot)
# # dev.off()


# plot.new()
# pdf("./Site_27009.pdf", 
#     onefile=TRUE, paper="special",useDingbats=FALSE,
#     width=7, height=9)
# print(right_plot)
# dev.off()
