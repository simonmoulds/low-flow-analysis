library(dplyr)
library(readr)
library(foreach)
library(doParallel)

DATADIR <- '/Users/smoulds/projects/streamflow-data/results'
START <- as.Date('1970-01-01')
END <- as.Date('2023-12-31')

compute_availability <- function(x, ...) { 
  # Filter to the defined period
  complete_ts <- tibble(date = seq(START, END, by = "1 day"))
  x <- complete_ts |> left_join(x, by = "date")
  availability <- x |> 
    mutate(year = lubridate::year(date)) |> 
    group_by(year) |> 
    summarize(availability = sum(!is.na(Q)) / n()) |> 
    filter(availability > 0.95)
  availability
}

is_valid <- function(x, ...) { 
  # Filter to the defined period
  availability <- compute_availability(x)
  if (nrow(availability) >= 30) { 
    return(TRUE)
  } else { 
    return(FALSE)
  }
}

is_intermittent <- function(x, ...) { 
  availability <- compute_availability(x)
  years <- availability$year
  x <- x |> mutate(year = lubridate::year(date)) |> filter(year %in% years)
  n_zero <- x |> 
    group_by(year) |> 
    summarize(n_zero = sum(Q < 0.001, na.rm = TRUE)) |> 
    pull(n_zero)
  # Mean number of zero flow days per year
  mean_n_zero <- mean(n_zero)
  # Fraction of years that are classed as intermittent by our definition
  intermittent_frac <- sum(n_zero >= 5) / length(years)
  if (isTRUE(mean_n_zero >= 5 & intermittent_frac > 0.6666667)) {
    return(TRUE)
  } else { 
    return(FALSE)
  }
}

fs <- list.files(file.path(DATADIR, "OHDB_data/discharge/daily"), pattern = "^OHDB_[0-9]{9}.csv$", full.names = TRUE)
intermittent <- rep(FALSE, length(fs))
pb <- txtProgressBar(min = 1, max = length(fs))
for (i in 1:length(fs)) { 
  x <- read.csv(fs[i])
  x <- x |> as_tibble() |> mutate(date = as.Date(date, format = "%Y-%m-%d"))
#   valid <- is_valid(x)
#   intermittent <- is_intermittent(x)
  if (is_valid(x) && is_intermittent(x)) {
    intermittent[i] <- TRUE
  }
  setTxtProgressBar(pb, i)
}
intermittent_fs <- fs[intermittent]

meta <- read.csv(file.path(DATADIR, "OHDB_metadata/OHDB_metadata.csv")) |> as_tibble()
intermittent_ids <- basename(intermittent_fs) %>% gsub("^(OHDB_[0-9]+).csv$", "\\1", .)
intermittent_meta <- meta |> filter(ohdb_id %in% intermittent_ids)

write_csv(intermittent_meta, 'intermittent_stations.csv')

make_complete_ts <- function(x, ...) {  
  ts <- x |> filter(!is.na(Q)) |> pull(date)
  start <- ts[1]
  end <- rev(ts)[1]
  complete_ts <- tibble(date = seq(start, end, by = "1 day"))
  x <- complete_ts |> left_join(x, by = "date")
  x
}

read_ohdb <- function(id, ...) { 
  x <- read.csv(file.path(DATADIR, "OHDB_data/discharge/daily", paste0(id, ".csv")))
  x <- x |> as_tibble() |> mutate(date = as.Date(date, format = "%Y-%m-%d"))
  x <- x |> make_complete_ts()
  x
}

# F0 
no_flow_probability <- function(x, ...) {
  x |> 
    mutate(date = as.Date(date, format = "%Y-%m-%d")) |> 
    mutate(year = lubridate::year(date)) |> 
    group_by(year) |> 
    summarize(intermittency = sum(Q <= 0.001, na.rm = TRUE) / n()) |> 
    ungroup() |> 
    summarize(intermittency = mean(intermittency)) |> 
    pull(intermittency)
}

# Flow magnitudes [Qp, p=1, 5, 90, 95]
flow_magnitude <- function(x, quantiles = c(0.01, 0.05, 0.9, 0.95), ...) { 
  quantile(x$Q, quantiles, na.rm = TRUE)
}

# No flow duration
no_flow_duration <- function(x, ...) { 
  x <- x |> mutate(noflow = as.numeric(Q <= 0.001), noflow = replace_na(noflow, 0))
  rle_no_flow <- rle(x$noflow)
  lengths <- rle_no_flow$lengths
  lengths
  # values <- rle_no_flow$values 
  # event_end_indices <- cumsum(lengths)
  # event_end_dates <- x$date[event_end_indices[values == 1]]
  # event_years <- lubridate::year(event_end_dates)
  # low_flow_events <- tibble(year = event_years, duration = lengths[values == 1], end_date = event_end_dates)
  # longest_low_flow_event <- low_flow_events |> group_by(year) |> summarize(duration = max(duration))
}

no_flow_duration_rp <- function(x, quantile = 0.8) { 
  x <- x |> mutate(noflow = as.numeric(Q <= 0.001), noflow = replace_na(noflow, 0))
  rle_no_flow <- rle(x$noflow)
  x$event_id <- rep(1:length(rle_no_flow$lengths), times = rle_no_flow$lengths)
  x$no_flow_event <- rep(rle_no_flow$values, times = rle_no_flow$lengths)
  no_flow_durations <- x |> 
    filter(no_flow_event == 1) |> 
    group_by(event_id) %>% 
    summarize(start_date = min(date), end_date = max(date), duration = as.numeric(max(date) - min(date) + 1))

  # Assign each event to the year in which it ends (block maxima approach)
  no_flow_durations <- no_flow_durations %>%
    mutate(end_year = format(end_date, "%Y"))

  # Get the maximum duration of no-flow event for each year
  max_durations_per_year <- no_flow_durations %>%
    group_by(end_year) %>%
    summarise(max_duration = max(duration))

  # Compute D80: the 80th percentile of the maximum durations
  D80 <- quantile(max_durations_per_year$max_duration, quantile)
  D80
}

# Frequency [mean, median, sd]
no_flow_frequency <- function(x, ...) { 
  x <- x |> mutate(noflow = as.numeric(Q <= 0.001), noflow = replace_na(noflow, 0))
  rle_no_flow <- rle(x$noflow)
  x$event_id <- rep(1:length(rle_no_flow$lengths), times = rle_no_flow$lengths)
  x$no_flow_event <- rep(rle_no_flow$values, times = rle_no_flow$lengths)
  x <- x |> mutate(year = lubridate::year(date))
  no_flow_durations <- x |> 
    filter(no_flow_event == 1) |> 
    group_by(year, event_id) %>% 
    summarize(start_date = min(date), end_date = max(date), duration = as.numeric(max(date) - min(date) + 1))
  
  # Assign each event to the year in which it ends (block maxima approach)
  all_years <- x |> arrange(year) |> distinct(year) |> pull(year)
  no_flow_durations <- no_flow_durations |>
    group_by(year) |> 
    summarize(n = n())

  no_flow_durations <- tibble(year = all_years) |> left_join(no_flow_durations) |> mutate(n = replace_na(n, 0))
  no_flow_durations
}

# Function to calculate the mean direction (θ) and regularity (r)
circular_stats <- function(dates, hemisphere = "Northern") {
  # Convert dates to day-of-year
  day_of_year <- as.numeric(format(dates, "%j"))
  
  # Convert day-of-year to angular values (t_i in radians, scaled to [0, 2π])
  angles <- 2 * pi * day_of_year / 365
  
  # Calculate mean cosines and sines
  mean_cos <- mean(cos(angles))
  mean_sin <- mean(sin(angles))
  
  # Calculate the angle of the mean vector (mean direction θ)
  theta <- atan2(mean_sin, mean_cos)
  
  # Adjust θ for Southern Hemisphere (optional adjustment for timing shift)
  if (hemisphere == "Southern") {
    if (any(day_of_year >= 182 & day_of_year <= 365)) {
      theta <- theta - pi
    } else if (any(day_of_year >= 1 & day_of_year <= 181)) {
      theta <- theta + pi
    }
  }
  
  # Normalize θ to be within [0, 2π]
  if (theta < 0) {
    theta <- theta + 2 * pi
  }
  
  # Calculate the norm of the mean vector (r) for regularity/concentration
  r <- sqrt(mean_cos^2 + mean_sin^2)
  
  # Return θ (mean direction) and r (regularity)
  return(list(theta = theta, regularity = r))
}

# Mean and dispersion of the occurrence of no flows within the year 
no_flow_timing <- function(x, ...) { 
  x <- x |> mutate(noflow = as.numeric(Q <= 0.001), noflow = replace_na(noflow, 0))
  rle_no_flow <- rle(x$noflow)
  x$event_id <- rep(1:length(rle_no_flow$lengths), times = rle_no_flow$lengths)
  x$no_flow_event <- rep(rle_no_flow$values, times = rle_no_flow$lengths)
  x <- x |> mutate(year = lubridate::year(date))
  no_flow_dates <- x |> filter(no_flow_event == 1) |> pull(date)
  cs <- circular_stats(no_flow_dates)
  cs
}

# Seasonal predictability of dry periods 
seasonal_predictability <- function(x, ...) { 
  df <- x |> mutate(noflow = as.numeric(Q <= 0.001), noflow = replace_na(noflow, 0))
  rle_no_flow <- rle(df$noflow)
  df$event_id <- rep(1:length(rle_no_flow$lengths), times = rle_no_flow$lengths)
  df$no_flow_event <- rep(rle_no_flow$values, times = rle_no_flow$lengths)
  df <- df |> mutate(year = lubridate::year(date), month = lubridate::month(date))
  monthly_no_flow <- df |> group_by(month) |> summarize(frequency = sum(no_flow_event) / n()) |> arrange(month)

  n_months <- nrow(monthly_no_flow)
  rolling_sums <- list()
  for (i in 1:n_months) {
    # Get the indices for the six-month window, wrapping around the end of the data if necessary
    indices <- (i:(i + 5)) %% n_months
    indices[indices == 0] <- n_months  # Handle wrapping

    # Sum the frequencies for this six-month window
    rolling_sums[[i]] <- data.frame(
      start_month = monthly_no_flow$month[i],
      end_month = monthly_no_flow$month[indices[length(indices)]],
      total_no_flow_freq = sum(monthly_no_flow$frequency[indices])
    )
  }

  rolling_sums <- bind_rows(rolling_sums)
  
  # Identify the driest and wettest six-month periods
  driest_six_months <- rolling_sums %>%
    arrange(desc(total_no_flow_freq)) %>%
    head(1)

  wettest_six_months <- rolling_sums %>%
    arrange(total_no_flow_freq) %>%
    head(1)

  average_monthly_zero_flow <- function(x, start_month, n = 6, ...) { 
    months <- seq(start_month, start_month + n - 1)
    months <- (months - 1) %% 12 + 1
    mean_F06 <- x |> 
      filter(month %in% months) |> 
      group_by(year, month) |> 
      summarize(has_no_flow = sum(no_flow_event) > 0) |> 
      group_by(month) |> 
      summarize(mn = mean(has_no_flow)) |> pull(mn) |> sum()
    return(mean_F06)
  }

  Fd_dry <- average_monthly_zero_flow(df, driest_six_months$start_month) 
  Fd_wet <- average_monthly_zero_flow(df, wettest_six_months$start_month)
  SD6 <- 1 - Fd_wet / Fd_dry
  SD6
}

seasonal_recession_timescale <- function(x, ...) { 
  # FIXME not sure whether this implementation is correct
  df <- x |> 
    mutate(day = lubridate::yday(date)) |> 
    filter(day %in% 1:365) |> 
    group_by(day) |> 
    summarize(Q = mean(Q, na.rm = T))
  df <- bind_rows(df, df[1:30,])
  df <- df |>
    mutate(Q_30d = zoo::rollmean(Q, k = 30, align = "left", na.pad = TRUE)) |> 
    filter(!duplicated(day))
  max_idx <- which.max(df$Q_30d)
  df <- bind_rows(df[max_idx:365,], df[1:(max_idx - 1),])
  min_idx <- which.min(df$Q_30d)
  df <- df[1:min_idx,]
  q50 <- quantile(df$Q_30d, 0.5)
  q90 <- quantile(df$Q_30d, 0.9)
  Drec <- df |> 
    filter(Q_30d >= q50 & Q_30d <= q90) |> 
    nrow()
  Drec
}

concavity_index <- function(x, ...) { 
  # TODO check how to compute FDC, and whether this is correct
  Q <- x |> filter(!is.na(Q)) |> pull(Q)
  q <- quantile(Q, c(0.01, 0.1, 0.99))
  IC <- (q[2] - q[3]) / (q[1] - q[3])
  IC
}

LH <- function(Q, beta = 0.925, return_exceed = FALSE) {
  # Args:
  #   Q: A numeric vector of streamflow values
  #   beta: Filter parameter (0.925 recommended by Nathan & McMahon, 1990)
  #   return_exceed: If TRUE, counts how often baseflow exceeds streamflow

  # Initialize baseflow vector
  if (return_exceed) {
    b <- numeric(length(Q) + 1)
  } else {
    b <- numeric(length(Q))
  }

  # First pass (forward filter)
  b[1] <- Q[1]
  for (i in 1:(length(Q) - 1)) {
    b[i + 1] <- beta * b[i] + (1 - beta) / 2 * (Q[i] + Q[i + 1])
    if (b[i + 1] > Q[i + 1]) {
      b[i + 1] <- Q[i + 1]
      if (return_exceed) {
        b[length(b)] <- b[length(b)] + 1
      }
    }
  }

  # Second pass (backward filter)
  b1 <- b
  for (i in (length(Q) - 1):1) {
    b[i] <- beta * b[i + 1] + (1 - beta) / 2 * (b1[i + 1] + b1[i])
    if (b[i] > b1[i]) {
      b[i] <- b1[i]
      if (return_exceed) {
        b[length(b)] <- b[length(b)] + 1
      }
    }
  }

  return(b)
}

# UKIH <- function(Q, b_LH, return_exceed = FALSE) {
#   # Args:
#   #   Q: A numeric vector of streamflow values
#   #   b_LH: Baseflow values that will be applied at the start and end of the series
#   #   return_exceed: If TRUE, counts how often baseflow exceeds streamflow
  
#   # Step 1: Split streamflow data into blocks of N = 5
#   N <- 5
#   block_end <- floor(length(Q) / N) * N
#   Q_blocks <- matrix(Q[1:block_end], ncol = N, byrow = TRUE)
  
#   # Step 2: Find the index of minimum flow in each block
#   idx_min <- apply(Q_blocks, 1, which.min)
#   idx_min <- idx_min + seq(0, block_end - N, by = N)
  
#   # Step 3: Identify turning points using UKIH_turn
#   idx_turn <- UKIH_turn(Q, idx_min)
  
#   # Step 4: Ensure at least 3 turning points are found
#   if (length(idx_turn) < 3) {
#     stop("Less than 3 turning points found")
#   }
  
#   # Step 5: Perform linear interpolation
#   b <- linear_interpolation(Q, idx_turn, return_exceed)
  
#   # Step 6: Apply baseflow (b_LH) at the start and end of the series
#   b[1:(idx_turn[1] - 1)] <- b_LH[1:(idx_turn[1] - 1)]
#   b[(idx_turn[length(idx_turn)] + 1):length(b)] <- b_LH[(idx_turn[length(idx_turn)] + 1):length(b)]
  
#   return(b)
# }

# # Supporting Function to Identify Turning Points
# UKIH_turn <- function(Q, idx_min) {
#   idx_turn <- integer(length(idx_min))
#   for (i in 1:(length(idx_min) - 2)) {
#     if ((0.9 * Q[idx_min[i + 1]] < Q[idx_min[i]]) && (0.9 * Q[idx_min[i + 1]] < Q[idx_min[i + 2]])) {
#       idx_turn[i] <- idx_min[i + 1]
#     }
#   }
#   return(idx_turn[idx_turn != 0])
# }

# # Supporting Function for Linear Interpolation
# linear_interpolation <- function(Q, idx_turn, return_exceed = FALSE) {
#   b <- numeric(length(Q))
  
#   n <- 1
#   for (i in idx_turn[1]:idx_turn[length(idx_turn)]) {
#     if (i == idx_turn[n + 1]) {
#       n <- n + 1
#       b[i] <- Q[i]
#     } else {
#       b[i] <- Q[idx_turn[n]] + (Q[idx_turn[n + 1]] - Q[idx_turn[n]]) / 
#                 (idx_turn[n + 1] - idx_turn[n]) * (i - idx_turn[n])
#     }
    
#     if (b[i] > Q[i]) {
#       b[i] <- Q[i]
#       if (return_exceed) {
#         b[length(b) + 1] <- b[length(b) + 1] + 1
#       }
#     }
#   }
  
#   return(b)
# }

# meta <- read_csv('intermittent_stations.csv')
# ids <- meta$ohdb_id 

# for (i in 1:length(ids)) { 
#   fn <- file.path(DATADIR, "OHDB_data/discharge/daily", paste0(ids[i], ".csv"))
#   x <- read.csv(fn)
# }

# Sources: 
# ANA [Brazil] - Catchment areas from CAMELS-BR?
# ARSO [Slovenia] - Catchment boundaries from EStreams  
# BOM - ???
# CEDEX - Catchment boundaries from EStreams 
# EAUFRANCE - Catchment boundaries from EStreams 
# GDC - ??? [Anya?]
# GRDC - GRDC watershed boundaries 
# HBRC - ??? [Anya?]
# HYDAT - ???
# IDEAM - ??? 
# IMGW - EStreams 
# MDC - ??? [Anya?]
# NRFA - NRFA 
# NVE - 
# SIEREM - ??? 
# USGS - ???
# WRC ??? [Anya?]
# WRIS - CAMELS-INDIA 
# XihuiGu - ???

# library(sf)
# x <- st_read("GRIT_full_catchment_domain_EU_EPSG8857_snap Boen Zhang.gpkg")
# x <- x |> filter(!is.na(ohdb_catchment_area) & !is.na(ohdb_catchment_area_hydrosheds) & !is.na(grit_catchment_area))
# plot(x$grit_catchment_area, x$ohdb_catchment_area, xlim = c(0, 1500000), ylim = c(0, 1500000), asp=1)
# ohdb_meta <- st_read("OHDB_metadata_fill_hydrosheds_fcc Boen Zhang.gpkg")
# # meta <- read_csv(file.path(DATADIR, "OHDB_metadata/OHDB_metadata.csv"))

# x <- read_csv(intermittent_fs[100])
# options(warn=2)
# problem_fs <- c()
# for (i in 1:length(fs)) { 
#   x <- try(read.csv(fs[i])) #, show_col_types = FALSE, progress = FALSE))
#   if (inherits(x, "try-error")) { 
#     problem_fs <- c(problem_fs, fs[i])
#   }
# }
# problem_ids <- basename(problem_fs) %>% gsub(".csv", "", .)
# meta <- meta |> filter(ohdb_id %in% problem_ids)

# intermittent_fs <- fs[intermittent]

# # Define number of cores (detect the number of available cores)
# num_cores <- detectCores() - 1  # Leave one core free

# # Create a cluster
# cl <- makeCluster(num_cores)

# # Export necessary objects and functions to the cluster

# intermittent <- list()
# intermittent <- foreach(i = 1:length(fs)) %dopar% {
#   x <- read_csv(file)
#   intermittent[i] <- is_intermittent(x)
# }

# # Convert the list returned by parLapply to a logical vector
# intermittent <- unlist(intermittent)

# # Stop the cluster
# stopCluster(cl)
