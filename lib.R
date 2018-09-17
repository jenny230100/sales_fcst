
library(R6)
library(stringr)

# --------------------------------------------------------------------------------------------x
# Description   : Generate year, week index for rolling calc unique combination of year week
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
get_week_year_index <- function() {
  # generate year-calendar week index mapping tbl for all possible combinations of year+calendar week 
  yr_cw_ind <- rbind(
    data.table(year = 2015, cw = 1:53),
    data.table(year = 2016, cw = 1:52),
    data.table(year = 2017, cw = 1:52),
    data.table(year = 2018, cw = 1:52)
  )
  # Order
  yr_cw_ind <- yr_cw_ind[order(year, cw)]
  # Add index
  yr_cw_ind[, yr_cw_ind := 1:.N]
  return(yr_cw_ind)
}

# --------------------------------------------------------------------------------------------x
# Description   : read tables from sqlite database
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
read_db <- function(db){
  dt <- as.data.table(dbReadTable(dbConnect(SQLite(),db),dbListTables(dbConnect(SQLite(),db))))
  dbDisconnect(dbConnect(SQLite(),db))
  return(dt)
}

# --------------------------------------------------------------------------------------------x
# Description   : database indexing
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
index_db <- function(db, db_tab){
  con <- dbConnect(SQLite(), db)
  sql <- paste0("CREATE INDEX IF NOT EXISTS idx_id ON `", db_tab, "` (id ASC);")
  dbGetQuery(con, sql)
  sql <- paste0("CREATE INDEX IF NOT EXISTS idx_ma_nr ON `", db_tab, "` (ma_nr ASC);")
  dbGetQuery(con, sql)
  sql <- paste0("CREATE INDEX IF NOT EXISTS idx_yr_cw_ind ON `", db_tab, "` (yr_cw_ind ASC);")
  dbGetQuery(con, sql)
  sql <- paste0("CREATE INDEX IF NOT EXISTS idx_yr_cw_ind ON `", db_tab, "` (ma_nr, yr_cw_ind ASC);")
  dbGetQuery(con, sql)
  dbDisconnect(con)
}

# --------------------------------------------------------------------------------------------x
# Description   : Write db table; Checks if table exists and appends in this case; Otherwise 
#                 creates new table
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
append_table <- function(name, value, db) {
  con <- dbConnect(SQLite(), db)
  if (dbExistsTable(con, name = name)) {
    opackrwrite <- F
    append <- T
  } else {
    opackrwrite <- T
    append <- F
  }
  dbWriteTable(conn = con,
               name = name,
               value = value,
               opackrwrite = opackrwrite,
               append = append)
  dbDisconnect(con)
}

# --------------------------------------------------------------------------------------------x
# Description   : Delete table if exists; to initialise a clean writing and appending
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
delete_table_if_exists <- function (tab, db) {
  con <- dbConnect(SQLite(), db)
  if(dbExistsTable(con, name = tab)) {
    dbRemopackTable(con, name = tab)
  }
  dbDisconnect(con)
}

# --------------------------------------------------------------------------------------------x
# Description   : Get simulated supply, incl. pack constraints if desired
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
set_sim_sup <- function(dt, uplift, is_pack_constraint, is_min_one_pack_sup) {
  
  # General uplift for simulated supug
  dt[, sim_sup_menge := round(est * uplift)]
  
  # Set simulated supply
  if(is_pack_constraint) {
    # Apply constraints to single due to pack: round up to next pack; zeros stay zeros
    dt[typ == "single", n_pack_req := ceiling(sim_sup_menge / pack_plan)]
    
    # Lists diff_price
    # How many pack would be required for each fuss
    dt[typ_liste == "diff_price", 
       n_pack_req_list_up := ceiling(sim_sup_menge / pack_plan),
       by = c("ma_nr", "yr_cw_ind", "id_head")]
    dt[typ_liste == "diff_price" & is.finite(n_pack_req_list_up), 
       n_pack_req := max(n_pack_req_list_up), 
       by = c("ma_nr", "yr_cw_ind", "id_head")]
    
    # Lists same_price
    dt[typ_liste == "same_price", 
       n_pack_req_list_gp := ceiling(sim_sup_menge / pack_plan)]
    dt[typ_liste == "same_price" & is.finite(n_pack_req_list_gp), 
       n_pack_req := max(n_pack_req_list_gp), 
       by = c("ma_nr", "yr_cw_ind", "id_head")]
    
    # Compute supuge via number of pack required
    dt[, sim_sup_menge := n_pack_req * pack_plan]
    
  } # If no pack constraint, do nothing
  
  # Set minimum pack
  if(is_min_one_pack_sup) {
    dt[typ != "head" & sim_sup_menge == 0, 
       sim_sup_menge := pack_plan] # pack_plan is number of articles for one pack
  }
  
  return(dt) # not sure why needed as := should modify dt by reference
}

# --------------------------------------------------------------------------------------------x
# Description   : Get simulated sales
# Input         : 
# Review status : 
# --------------------------------------------------------------------------------------------x
set_sim_packr = function(dt, uplift, uplift_sellout) {
  # Compute potential packr menge based on sales
  dt[, sim_sales_qty_pot := sales_qty] # Init
  
  # Uplift sellout
  dt[aq_cut == 100, 
     sim_sales_qty_pot := round(sales_qty * uplift_sellout) ]
  
  # Compute sim_sales_qty
  dt[, sim_sales_qty := sim_sales_qty_pot] # Init: Sell full potential when available
  # Constraint packr <= sup
  dt[sim_sup_menge < sim_sales_qty_pot, 
     sim_sales_qty := sim_sup_menge] # Cannot sell more than menge sup
  
  return(dt) # not sure why needed as := should modify dt by reference
  
  # Debug: checking dt[sim_sup_menge < sim_sales_qty_pot][1:2], ok
}

##################################################################################################x
# STEP 4: Define classes ----
##################################################################################################x

# ================================================================================================x
# R6:Aktionswoche ----
# Assumptions 
#  + supply is calculated using 3 wks total (Aktionswoche, Aktionswoche-1, Aktionswoche-2)
#  + packrkaufsmenge is calculated using 5 wks total (Aktionswoche, Aktionswoche-1, Aktionswoche+1
#                                                                  Aktionswoche+2, Aktionswoche+3)
#  + Non-integer supply are not rounded
#  + Absatzquote (aq) is calculated as packrkaufsmenge / supply on different aggregation lepackls
#  + For opackrall aq and market aq, data points with NA supply are not used in the aggregation
#  + Negatipack supply and sales are ignored
#  + Participating markets are defined as markets, which obtain (80%) of distinct articles for week
# ================================================================================================x
Aktionswoche <- R6Class(
  classname = "Aktionswoche",
  
  # ==============================================================================================x
  # Private ----
  # ==============================================================================================x
  private = list(
    # Mapping for cw and year to date
    date_match = readRDS(paste0(ROOT, "/05_Data/02_preprocessed/004_date_match_v05.rds"))
  ), # End private
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    cw = NA,
    year = NA,
    wk = NA,
    plan = NA, # Werbeplan data
    dwh = list(), #  list of dwh data.tables
    db = NA, # WHERE SQL clause
    tab = NA,  # Which tables to load from db
    dat_mar_art = NA, # result tbl for aq analysis
    vb = NA, # predefined packrtriebsbereiche 
    thresh_ma_part = NA, # How much % of different articles (head+single) of aw needs store to get in order to be declared to be a participating market
    
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(db, tab, cw, year, wk, vb, thresh_ma_part) {
      self$db <- db
      self$cw <- cw
      self$year <- year
      self$wk <- wk
      self$tab <- tab
      self$vb <- vb
      self$thresh_ma_part <- thresh_ma_part
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Easy getter for ID of aktionswoche, e.g. for logging output
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    id_string = function(con) {
      id <- sprintf("%d-%d", self$year, self$cw)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Load relevant plan data
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    load_plan = function(con) {
      sql <- paste0("SELECT * FROM '", self$tab$plan, "' ",
                    " WHERE cw = ", self$cw, 
                    " AND year = ", self$year, 
                    " AND werbekreis IN ('", paste0(self$wk, collapse = "', '"), "')")
      self$plan <- as.data.table(dbGetQuery(con, sql))
      
      # Check if no plan data
      if (nrow(self$plan) == 0) {
        stop("No articles in plan data: ", self$year, "-", self$cw)
      }
      
      # Check duplicates in ids
      n_dup <- sum(duplicated(self$plan$id))
      if (n_dup > 0) {
        loginfo("%s: %d duplicates found in plan data.", self$id_string(), n_dup)
        # Leapack dups in as will be aggregated within analysis. Duplicates can occur epackn with
        # one item in stueckliste and one in single.
      } # if any dups
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Load relevant sup data
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    load_sup = function(con, ids_plan) {
      # Load from db
      sql <- paste0("SELECT * FROM '", self$tab$sup, "' ",
                    " WHERE id IN ('", paste0(ids_plan, collapse = "', '"), "')")
      self$dwh$sup <- as.data.table(dbGetQuery(con, sql))
      
      # join week-year index to sup
      yr_cw <- get_week_year_index()
      self$dwh$sup <- merge(self$dwh$sup, yr_cw[, c("year","cw","yr_cw_ind")], by=c("year","cw"), all.x=T)
      
      # Check if all plan ids in sup?
      ids_exp <- self$plan[typ %in% c("single", "head"), id]
      ids_sup_miss <- ids_exp[ids_exp %ni% self$dwh$sup[, unique(id)]]
      if (length(ids_sup_miss) > 0) {
        logwarn("%s: ids of plan missing in supply: %s", self$id_string(), ids_sup_miss)
      }
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Load relevant packr da
    # Input         : 
    # Review status :  
    # --------------------------------------------------------------------------------------------x
    load_packr = function(con, ids_plan) {
      # Load from db
      sql <- paste0("SELECT * FROM '", self$tab$packr, "' ",
                    " WHERE id IN ('", paste0(ids_plan, collapse = "', '"), "')")
      self$dwh$packr <- as.data.table(dbGetQuery(con, sql))
      # Check if all plan ids in packr?
      ids_exp <- self$plan[typ %in% c("single", "fuss"), id]
      ids_packr_miss <- ids_exp[ids_exp %ni% self$dwh$packr[, unique(id)]]
      if (length(ids_packr_miss) > 0) {
        logwarn("%s: ids of plan missing in sales: %s", self$id_string(), ids_packr_miss)
      }
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Load relevant plan data, supply and packraeufe for relevant ids as well as 
    #                 markt and artikel master tables
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    load_data = function() {
      con <- dbConnect(SQLite(), self$db)
      
      # Plan
      self$load_plan(con)
      # supply
      self$load_sup(con, self$plan$id)
      self$dwh$sup[, cc_date_format := paste(cw, year, sep = "-")]
      self$dwh$sup <- merge(self$dwh$sup, private$date_match, by = "cc_date_format", all.x = T)
      # sales
      self$load_packr(con, self$plan$id)
      self$dwh$packr[, cc_date_format := paste(cw, year, sep = "-")]
      self$dwh$packr <- merge(self$dwh$packr, private$date_match, by = "cc_date_format", all.x = T)
      # Artikel
      self$dwh$art <- as.data.table(dbReadTable(con, self$tab$art))
      # Markt
      self$dwh$mar <- as.data.table(dbReadTable(con, self$tab$mar))
      
      dbDisconnect(con)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get participating markets for Aktionswoche; Needs to work on raw dwh data, as
    #                 required in get_supply() method to dermine self$dat_mar_art
    #                 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_ma_part = function(){
      # get yr_cw_ind for user-defined year and cw (t0)
      t0 <- get_week_year_index()[year==self$year & cw == self$cw, yr_cw_ind]
      # get ids of head and single of aw
      id_ <- self$plan[typ %in% c("single", "head"), unique(id)]
      # get supply for aw
      sup_aw <-  self$dwh$sup[sup_menge > 0 & # Only positipack menge
                                yr_cw_ind %in% (t0 - 2):t0  &# Only three weeks
                                id %in% id_]  #  only single and head of aw
      # How many single and head expected?
      n_exp_ <- self$plan[typ %in% c("single", "head"), .N]
      # How many different articles receipackd per market?
      sup_stat <- sup_aw[, .(n_art_dist = length(unique(id)), 
                             n_art_dist_rel = length(unique(id)) / n_exp_), 
                         by = ma_nr][ order(n_art_dist)]
      # Declare participating markets as those, who got for x% of single and head supply in dwh
      mar_par_sup <- sup_stat[n_art_dist_rel >= self$thresh_ma_part, unique(ma_nr)]
      # Filter VB
      mar_vb <- self$dwh$mar[ma_ber_nr %in% self$vb, unique(ma_nr)]
      
      mar_par <- intersect(mar_par_sup, mar_vb)
      
      # Check
      if(length(mar_par) == 0) {
        logwarn("Zero participating markets identified? Threshold too high?")
      }
      
      return(mar_par)
    },
    
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Calculate supply on the market-article lepackl and add the result to the
    #                 attribute dat_mar_art
    # Input         : 
    # Review status : QC JLI 20180705
    # TODO          : further inpackstigation / handling for dup in plan 
    # --------------------------------------------------------------------------------------------x
    get_supply = function(){
      # build grid of all sotre and article combinations: 
      self$dat_mar_art <- as.data.table(expand.grid(self$get_ma_part(), 
                                                    self$plan[, unique(id)], 
                                                    stringsAsFactors=F)) 
      setnames(self$dat_mar_art, c("ma_nr", "id"))
      # dups are temporarily deleted 
      self$dat_mar_art <- merge(self$dat_mar_art, 
                                self$plan[!duplicated(id), 
                                          .(id, typ, id_head, pack_plan, pack_plan_head, typ_liste)], 
                                by="id", 
                                all.x=T) 
      # filter and aggregate supply for cw, cw-1 & cw-2
      # get yr_cw_ind for user-defined year and cw (t0)
      t0 <- as.integer(self$dwh$sup[year==self$year & cw == self$cw, unique(yr_cw_ind)])
      sup_cw <- self$dwh$sup[yr_cw_ind == t0 | 
                               yr_cw_ind == t0 - 1 | 
                               yr_cw_ind == t0 - 2]
      sup_cw_agg <- sup_cw[sup_menge >0, 
                           .(sup_menge = sum(sup_menge),
                             sup_price = sum(sup_price),
                             sup_roher = sum(sup_roher)),
                           by = c("id", "ma_nr")] 
      sup_cw_agg[, id := as.character(id)]
      # add 3 wks sum to supply
      self$dat_mar_art <- merge(self$dat_mar_art, sup_cw_agg, by = c("id","ma_nr"), all.x=T)
      # add head Menge to supply
      menge_head <- self$dat_mar_art[!is.na(sup_menge) & typ == "head", .(id_head, ma_nr, sup_menge)]
      setnames(menge_head, "sup_menge","menge_head")
      # check dup for left join
      loginfo("%s: %d duplicates found in head Menge", self$id_string(), sum(duplicated(menge_head))) 
      # join head Menge to supply
      self$dat_mar_art <- merge(self$dat_mar_art, menge_head, by = c("id_head", "ma_nr"), all.x=T)
      # add factor for same_price
      self$dat_mar_art[typ_liste == "same_price", factor_liste_gleich := pack_plan / pack_plan_head]
      # compute menge per article - single 
      self$dat_mar_art[typ == "single", sup_menge_art := sup_menge]
      # compute menge per article - diff_price
      self$dat_mar_art[typ_liste == "diff_price" & typ == "fuss", 
                       sup_menge_art := menge_head * pack_plan]
      # compute menge per article - same_price
      self$dat_mar_art[typ_liste == "same_price" & typ == "fuss", 
                       sup_menge_art := menge_head * factor_liste_gleich]
      # check if always integer & sapack result in log info
      loginfo("%s: %d non-integer supply", self$id_string(), 
              sum(self$dat_mar_art[!is.na(sup_menge_art) & 
                                     typ_liste == "same_price" & 
                                     typ == "fuss", 
                                   sup_menge_art %% 1 != 0])) 
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Calculate the 5 weeks packrkaufsmenge and add the result to the attribute dat_mar_art
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    get_sales = function(){
      # base tbl - no head
      packr <- self$dat_mar_art[typ %in% c("single", "fuss")]
      t0 = as.integer(unique(self$dwh$packr[year==self$year & cw == self$cw, c("yr_cw_ind")])) 
      # agg for 5 weeks
      packr_cw_agg <- self$dwh$packr[sales_qty > 0 &
                                   (yr_cw_ind == t0 | 
                                      yr_cw_ind == t0-1 | 
                                      yr_cw_ind == t0+1 | 
                                      yr_cw_ind == t0+2 | 
                                      yr_cw_ind == t0+3),
                                 .(sales_qty = sum(sales_qty),
                                   packr_um_br = sum(packr_um_br),
                                   packr_roher = sum(packr_roher)),
                                 by = c("id", "ma_nr")]
      packr_cw_agg[, id := as.character(id)]
      # join to base tbl
      packr <- merge(packr, packr_cw_agg, by = c("id","ma_nr"), all.x=T)
      # set NA to zero
      packr[is.na(sales_qty), sales_qty := 0]
      # add packr to dat_mar_art
      self$dat_mar_art <- merge(self$dat_mar_art, 
                                packr[, c("id", "ma_nr", "sales_qty", "packr_um_br", "packr_roher"), with = F], 
                                by = c("id", "ma_nr"), 
                                all.x =T)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Run get_supply & get_sales & rename cols 
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    get_data = function(){
      # run get_supply() to get supply auf Fu?ebene
      self$get_supply()
      # run get_sales() to get packrkaufmenge
      self$get_sales()
      # rename cols
      setnames(self$dat_mar_art, 
               c("sup_menge_art", "sup_menge"),
               c("sup_menge", "sup_menge_dwh")) 
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Compute aq per market per article 
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    compute_aq_mar_art = function(){
      self$dat_mar_art[typ %in% c("single", "fuss"), 
                       aq := sales_qty / sup_menge * 100]
      # Compute aq cut at 100%
      self$dat_mar_art[, aq_cut := aq]
      self$dat_mar_art[aq_cut > 100, aq_cut := 100]
      
    },
    # --------------------------------------------------------------------------------------------x
    # Description   : Compute opackrall aq and return the result as a function output
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    compute_aq_opackrall = function(){
      aq <- self$dat_mar_art[typ %in% c("single", "fuss"), mean(aq_cut, na.rm = T)] 
      return(aq)
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Compute aq per market and return the result as a data table
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    compute_aq_mar = function(){
      aq_mar <- self$dat_mar_art[typ %in% c("single", "fuss"), 
                                 .(aq = mean(aq_cut, na.rm = T)),
                                 by = c("ma_nr")][order(aq, decreasing = T)]
      return(aq_mar)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Compute aq per article and add relevant cols from plan 
    # Input         : 
    # Review status : QC JLI 20180705
    # --------------------------------------------------------------------------------------------x
    compute_aq_art = function(){
      aq_art_tmp <- self$dat_mar_art[typ %in% c("single", "fuss"), 
                                     .(aq = mean(aq_cut, na.rm = T)),
                                     by = c("id")][order(aq, decreasing = T)]
      aq_art <- merge(aq_art_tmp, 
                      self$plan[, c("id","typ","id_head","typ_liste","count_liste","n_liste")], 
                      by="id", all.x=T)
      return(aq_art)
    }
    
  ) # End public
) # End class


# ================================================================================================x
# R6:Dem_est_single_store ----
#   Usable for backtesting as well as prediction in operations
# ================================================================================================x
Dem_est_single_store <- R6Class(
  classname = "Dem_est_single_store",
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    cw = NA,
    year = NA,
    yr_cw_ind = NA,
    art2pred = NA,
    mar2pred = NA,
    hist_plan = NA,
    tab_hist_art_mar = NA,
    plan = NA,
    res = NA,
    res_same = data.table(),
    res_group_1 = data.table(),
    res_group_2 = data.table(),
    seed = 123,
    db_res = NA,
    s = NA, # Settings
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(year, 
                          cw, 
                          yr_cw_ind, 
                          mar2pred, 
                          art2pred, 
                          hist_plan, 
                          tab_hist_art_mar, 
                          plan, 
                          db_res, 
                          s) {
      # Store attributes
      self$year <- year
      self$cw <- cw
      self$yr_cw_ind <- yr_cw_ind
      self$mar2pred <- mar2pred
      self$art2pred <- art2pred 
      self$hist_plan <- hist_plan 
      self$tab_hist_art_mar <- tab_hist_art_mar 
      self$plan <- plan 
      self$db_res <- db_res
      self$s <- s
      # Init result table
      self$res <- as.data.table(
        expand.grid(ma_nr = self$mar2pred, 
                    id = self$art2pred, 
                    stringsAsFactors = F))
      self$res$yr_cw_ind <- self$yr_cw_ind
      # Add article attributes
      self$res <- merge(self$res,
                        self$plan[typ != "head", 
                                  .(price = mean(price), 
                                    group_2 = .SD$group_2[1], 
                                    group_1 = .SD$group_1[1]), 
                                  by = c("id", "yr_cw_ind")],
                        by = c("id", "yr_cw_ind"),
                        all.x = T)      

    }, # Constructor
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get historic data for stores
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_hist_store = function(mar_sel) {
      # Load historic data
      con <- dbConnect(SQLite(), self$db_res)
      sql <- paste0("SELECT * FROM '", self$tab_hist_art_mar, "' ",
                    "WHERE ma_nr IN ('", paste(mar_sel, collapse = "', '"), "') ",
                    "AND yr_cw_ind < ", self$yr_cw_ind, ";")
      hist_mar <- as.data.table(dbGetQuery(con, sql))
      dbDisconnect(con)
      # Compute uplift for out of stock
      hist_mar[, sales_qty_uplift := sales_qty] # Init
      hist_mar[aq_cut == 100, 
               sales_qty_uplift := round(sales_qty * self$s$uplift_sellout)] 
      
      # Add cols from plan data
      hist_mar <- merge(hist_mar,
                        self$hist_plan[, .(id, yr_cw_ind, price, group_2, group_1)],
                        by = c("id", "yr_cw_ind"),
                        all.x = T)
      return(hist_mar)
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_2 estimation for each store individually 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_wg = function() {
      # WG estimator
      # For each store
      for (mar_sel in self$mar2pred) {
        # Debug: mar_sel="44 65 9457"
        hist_mar <- self$get_hist_store(mar_sel)
        if (nrow(hist_mar) == 0) next
        self$estim_group_1_single_store(mar_sel, hist_mar, is_plot = F)
        # self$estim_group_2_single_store(mar_sel, hist_mar, is_plot = F)
      }
      
      # Store in single result table
      
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_2 estimation for each store individually 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_group_2_single_store = function(mar_sel, hist_mar, is_plot = F) {
      # cat("Processing", self$year, "-", self$cw, "group_2 market", mar_sel, "...\n")
      # For all group_2 to predict
      for (group_2_sel in self$plan[typ != "head", unique(group_2)]) {
        # Debug: group_2_sel=75
        hist <- hist_mar[typ != "head" & group_2 == group_2_sel]
        if (nrow(hist) == 0) next
        self$estim_group_2_single_store_pred(hist, mar_sel, group_2_sel, is_plot = is_plot)
        
      } # for all group_2
      
    } , # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_2 estimation for gipackn store and gipackn group_2
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_group_2_single_store_pred = function(hist, mar_sel, group_2_sel, is_plot = F) {
      # Get market selected
      check <- hist[, unique(ma_nr)]
      if (length(check) > 1) {
        stop("Wrong historic data.")
      }
      # model
      fit <- self$dec_tree(hist)
      # Predict and store some stats
      self$res[ma_nr == mar_sel & group_2 == group_2_sel, 
               group_2_dem_est := round(predict(fit, newdata = .SD))]
      self$res[ma_nr == mar_sel & group_2 == group_2_sel,
               group_2_n_hist := nrow(hist)]
      # Plot
      if(is_plot) {
        # Get prediction
        pred_plot <- data.table(price = seq(from = 1, to = hist[, max(price, na.rm = T)], length.out = 100))
        pred_plot[, sales_qty := round(predict(fit, newdata = .SD))]
        dt_plot <- rbind(
          cbind(hist[, .(price, sales_qty)], Legende = "Historie"),
          cbind(pred_plot, Legende = "Vorhersage")
        )
        # Plot target and prediction
        p1 <- ggplot(dt_plot) +
          geom_point(aes(x = price, y = sales_qty, color = Legende)) + 
          xlab("packrkaufspreis") +
          ylab("packrkaufsmenge") +
          ggtitle(paste0("Vorhersage fuer group_2 '", group_2_sel, "'\nund Markt '", mar_sel, "'"))
        print(p1)
        readline(prompt="Press [enter] to continue")
      }
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_1 estimation for each store individually 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_group_1_single_store = function(mar_sel, hist_mar, is_plot = F) {
      # cat("Processing", self$year, "-", self$cw, "group_1 market", mar_sel, "...\n")
      # For all group_1 to predict
      for (group_1_sel in self$plan[typ != "head", unique(group_1)]) {
        # Debug: group_1_sel=7522;mar_sel="44 65 9457"
        hist <- hist_mar[typ != "head" & group_1 == group_1_sel]
        if (nrow(hist) == 0) next
        self$estim_group_1_single_store_pred(hist, mar_sel, group_1_sel, is_plot = is_plot)
      } # for all group_1
      
    } , # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_1 estimation for each store individually 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_group_1_single_store_pred = function(hist, mar_sel, group_1_sel, is_plot = F) {
      # Get market selected
      check <- hist[, unique(ma_nr)]
      if (length(check) > 1) {
        stop("Wrong historic data.")
      }
      # Model
      fit <- self$dec_tree(hist)
      # Predict and store some stats
      self$res[ma_nr == mar_sel & group_1 == group_1_sel, 
               group_1_dem_est := round(predict(fit, newdata = .SD))]
      self$res[ma_nr == mar_sel & group_1 == group_1_sel, 
               group_1_n_hist := nrow(hist)]
      if (is.null(fit$splits)){
        splits_ <- 0
      } else {
        splits_ <- nrow(fit$splits)
      }
      self$res[ma_nr == mar_sel & group_1 == group_1_sel, 
               group_1_n_splits := splits_]
      # Plot
      if(is_plot) {
        # Get prediction
        pred_plot <- data.table(price = seq(from = 1, to = hist[, max(price, na.rm = T)], length.out = 100))
        pred_plot[, sales_qty := round(predict(fit, newdata = .SD))]
        dt_plot <- rbind(
          cbind(hist[, .(price, sales_qty)], Legende = "Historie"),
          cbind(pred_plot, Legende = "Vorhersage")
        )
        # Plot target and prediction
        p1 <- ggplot(dt_plot) +
          geom_point(aes(x = price, y = sales_qty, color = Legende)) + 
          xlab("packrkaufspreis") +
          ylab("packrkaufsmenge") +
          ggtitle(paste0("Vorhersage fuer group_1 '", group_1_sel, "'\nund Markt '", mar_sel, "'"))
        print(p1)
        readline(prompt="Press [enter] to continue")
      }
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Estimate demand using data from same article in same store only
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_same_single_store = function() {
      # Get data
      hist <- self$get_hist_store(self$mar2pred)
      # only keep for id to predict
      hist <- hist[id %in% self$art2pred]
      # Estimate using max where data is available
      res_tmp <- hist[id %in% id_scope, 
                      .(yr_cw_ind = self$yr_cw_ind,
                        same_dem_est = as.numeric(round(max(sales_qty_uplift))),
                        same_n_hist = as.numeric(.N)), 
                      by = c("id", "ma_nr")]
      # Store in object
      self$res_same <- rbind(self$res_same, res_tmp)

    }, # method
    

    # --------------------------------------------------------------------------------------------x
    # Description   : Compute decision tree
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    dec_tree = function(hist) {
      # Build predictor
      set.seed(self$seed)
      fit <- rpart(
        as.formula("sales_qty_uplift ~ price"),
        data = hist,
        method = "anova", # class, anova
        control = rpart.control(
          # minsplit = 5, # Min number observations in node to attempt a split
          minbucket = 5, # Min number observatiions in leaf
          cp = 0.01, # Min impropackment for split to be acceptable; the lower the more splits are done
          xval = 10, # Number cross validations
          maxdepth = 3 # Max allowable tree depth
        ))
      fit <- auto_prune(fit, se_factor = 0)
      return(fit)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Store data from all estimators in res
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    consolidate_result_data = function(hist) {
      # Same estimator
      nrow_b4 <- nrow(self$res)
      self$res <- merge(self$res, 
                        self$res_same, 
                        by = c("id", "ma_nr", "yr_cw_ind"), 
                        all.x = T)
      # Check join
      if(nrow_b4 != nrow(self$res)) {
        stop("Join failed")
      }
      # Set n to zero when NA
      self$res[is.na(same_n_hist), same_n_hist := 0]
      loginfo("%d-%d %f perc estimates for same estimator", 
              self$year, 
              self$cw, 
              self$res[same_n_hist != 0, .N] / nrow(self$res) * 100)
      
    } # method
    

    
  ) # Public
) # Class


# ================================================================================================x
# R6:Dem_est ----
# ================================================================================================x
Dem_est <- R6Class(
  classname = "Dem_est",
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    cw = NA,
    year = NA,
    yr_cw_ind = NA,
    art2pred = NA,
    mar2pred = NA,
    hist_plan = NA,
    tab_master = NA,
    plan = NA,
    res = data.table(),
    res_raw = data.table(),
    seed = 123,
    db_res = NA,
    tab_res = NA,
    s = NA, # Settings
    
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(year, 
                          cw, 
                          yr_cw_ind, 
                          mar2pred, 
                          art2pred, 
                          hist_plan, 
                          tab_master, 
                          plan, 
                          db_res, 
                          tab_res,
                          s) {
      # Store attributes
      self$year <- year
      self$cw <- cw
      self$yr_cw_ind <- yr_cw_ind
      self$mar2pred <- mar2pred
      self$art2pred <- art2pred 
      self$hist_plan <- hist_plan 
      self$tab_master <- tab_master 
      self$plan <- plan 
      self$db_res <- db_res
      self$tab_res <- tab_res
      self$s <- s
      
    }, # Constructor
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get historic data 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_hist_data_group_1_multi = function(group_1_sel) {
      # Load historic data
      con <- dbConnect(SQLite(), self$db_res)
      sql <- paste0("SELECT * FROM '", self$tab_master, "' ",
                    "WHERE group_1 IN ('", paste(group_1_sel, collapse = "', '"), "') ",
                    "AND typ in ('single', 'fuss') ",
                    "AND yr_cw_ind <= ", self$yr_cw_ind, ";") # Get train and test
      dt <- as.data.table(dbGetQuery(con, sql))
      dbDisconnect(con)
      # Compute uplift for out of stock
      dt[, sales_qty_uplift := sales_qty] # Init
      dt[aq_cut == 100, 
               sales_qty_uplift := round(sales_qty * self$s$uplift_sellout)] 
      # Add market stats
      mar_stat <- readRDS(file.path(ROOT, "05_Data/02_preprocessed/034_mar_stats_v01.rds"))
      # Add store attributes
      dt <- merge(dt,
                  mar_stat$rev_all_med,
                  by = "ma_nr",
                  all.x = T)
      dt <- merge(dt,
                  mar_stat$rev_group_2_med,
                  by = c("ma_nr", "group_2"),
                  all.x = T)
      return(dt)
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : group_1 estimation 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_group_1_multi= function() {
      # For all group_1 to predict
      for (group_1_sel in self$plan[typ != "head", unique(group_1)]) {
        # Debug: group_1_sel=8615
        cat("Processing group_1", group_1_sel, "\n")
        # Load data for train and test
        dt <- self$get_hist_data_group_1_multi(group_1_sel)
        # Compute additional features
        dt[, n_scb_vl := n_pack_vl * pack_plan]
        dt[, sales_qty_uplift_scaled := sales_qty_uplift / n_pack_s]
        # Define target and features
        target <- "sales_qty_uplift" # sales_qty_uplift_scaled
        feat <- c("price", "rev_all_med", "rev_group_2_med", "n_scb_vl") #, "group_1") #, "ma_vf", "clus")
        # Complete cases
        dt[, cc := complete.cases(dt[, c(target, feat), with = F])]
        # Train test
        dt[yr_cw_ind < self$yr_cw_ind & cc == T, tt := "train"]
        dt[yr_cw_ind == self$yr_cw_ind & cc == T, tt := "test"]
        dt[, .N, by = tt]
        # If not enough data
        if (nrow(dt[tt == "train"]) < 50 | # Enough train data
            nrow(dt[tt == "test" & !is.na(aq_cut)]) < 10 # Something to compare aq to
            ) {
          loginfo("Skipping group_1 %s as not enough data to estimate.", group_1_sel)
          next
        }
        # TODO: Warning for which stores no prediction is possible 
        # caret
        bootControl <- trainControl(method="cv", 
                                    number = 10)
        set.seed(1234)
        fit <- train(x = dt[tt == "train", c(feat), with = F],
                     y = dt[tt == "train", get(target)],
                     method = "glmnet", # rf, rpart
                     trControl = bootControl, 
                     tuneGrid = NULL) # set to NULL to swich off tuning grid
        # Predict
        dt[cc == T, 
           est := predict(fit, newdata = .SD[, c(feat), with = F] # * n_pack_s
        )]
        # Optimize uplift
        res_opt_train <- optimize(f = self$cost_fcn, 
                            interval = c(0.5, 3), 
                            dt = dt, 
                            penalty = 10, 
                            alpha = 0.5, 
                            tt_ = "train",
                            maximum = T)
        res_opt_test <- optimize(f = self$cost_fcn, 
                                  interval = c(0.5, 3), 
                                  dt = dt, 
                                  penalty = 10, 
                                  alpha = 0.5, 
                                  tt_ = "test",
                                  maximum = T)
        # Get optimal simulation
        sim <- self$sim(uplift = res_opt_train$maximum, dt)
        sim_res_opt <- sim$perf
        # Store stats 
        self$res <- rbind(
          self$res,
          data.table(
            group_1 = group_1_sel,
            yr_cw_ind = self$yr_cw_ind,
            n_train = dt[cc == T & tt == "train", .N],
            n_test = dt[cc == T & tt == "test", .N],
            n_id_train = dt[cc == T & tt == "train", length(unique(id))],
            n_id_test = dt[cc == T & tt == "test", length(unique(id))],
            R2_resam = mean(fit$resample$Rsquared),
            opt_uplift_train = res_opt_train$maximum,
            opt_uplift_test = res_opt_test$maximum,
            opt_cost_train = res_opt_train$objectipack, 
            opt_cost_test = res_opt_test$objectipack, 
            opt_delta_aq_test = sim_res_opt[tt == "test", delta_aq],
            opt_delta_rev_rel_test = sim_res_opt[tt == "test", delta_rev_rel]
          )
        )
        
        # Store simulation results
        append_table(name = self$tab_res,
                     value = sim$data,
                     db = self$db_res)
      } # for all group_1
      
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Cost function
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    cost_fcn = function(uplift, dt, penalty = 10, alpha = 0.5, tt_) {
      sim <- self$sim(uplift, dt)
      cost = self$compute_cost(uplift, perf = sim$perf[tt == tt_], penalty, alpha)
      return(cost)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Comput performance for simulation using uplift
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    sim = function(uplift, dt) {

      # Simulate supply
      dt <- self$set_sim_sup(dt, uplift)
      # Simulate sales
      dt <- self$set_sim_packr(dt, uplift)
      # Simulate repacknue
      dt[, sim_packr_um_br := price * sim_sales_qty]
      # Compute AQ
      dt[, sim_aq := sim_sales_qty / sim_sup_menge * 100]
      # Filter simulation data
      dt <- dt[!is.na(sup_menge) & 
                 !is.na(aq_cut) & 
                 !is.na(est) & 
                 !is.na(sim_aq) # Can happen when estimate is zero and hence packr is zero
               ]
      # Performance train and test
      perf <- dt[, 
                 .(uplift = uplift,
                   sum_sup_real = sum(sup_menge, na.rm = T),
                   sum_sup_sim = sum(sim_sup_menge, na.rm = T),
                   sum_packr_real = sum(sales_qty, na.rm = T),
                   sum_packr_sim = sum(sim_sales_qty, na.rm = T),
                   sum_rev = sum(sim_packr_um_br, na.rm = T),
                   aq_real = round(mean(aq_cut, na.rm = T), 1),
                   aq_sim = round(mean(sim_aq, na.rm = T), 1),
                   delta_aq = round(mean(sim_aq - aq_cut, na.rm = T), 1),
                   delta_rev = sum(sim_packr_um_br, na.rm = T) - sum(packr_um_br, na.rm = T),
                   delta_rev_rel = round((sum(sim_packr_um_br, na.rm = T) - sum(packr_um_br, na.rm = T)) / 
                                           sum(packr_um_br, na.rm = T) * 100, 1)
                 ),
                 by = tt]
      return(list(perf = perf, 
                  data = dt))
    },
    
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Compute cost for optimization
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    compute_cost = function(uplift, perf, penalty = 10, alpha = 0.5) {
      
      # Compute cost function
      cost_aq <- ifelse(perf$delta_aq < 0, 
                        perf$delta_aq * penalty, 
                        perf$delta_aq)
      cost_rev <- ifelse(perf$delta_rev_rel < 0, 
                         perf$delta_rev_rel * penalty, 
                         perf$delta_rev_rel)
      cost <- alpha * cost_aq + (1 - alpha) * cost_rev
      
      return(cost) 
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get simulated supply, incl. pack constraints if desired
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    set_sim_sup = function(dt, uplift) {
      
      # General uplift for simulated supug
      dt[, sim_sup_menge := round(est * uplift)]
      
      # Set simulated supply
      if(self$s$is_pack_constraint) {
        # Apply constraints to single due to pack: round up to next pack; zeros stay zeros
        dt[typ == "single", n_pack_req := ceiling(sim_sup_menge / pack_plan)]
        
        # Lists diff_price
        # How many pack would be required for each fuss
        dt[typ_liste == "diff_price", 
           n_pack_req_list_up := ceiling(sim_sup_menge / pack_plan),
           by = c("ma_nr", "yr_cw_ind", "id_head")]
        dt[typ_liste == "diff_price" & is.finite(n_pack_req_list_up), 
           n_pack_req := max(n_pack_req_list_up), 
           by = c("ma_nr", "yr_cw_ind", "id_head")]
        
        # Lists same_price
        dt[typ_liste == "same_price", 
           n_pack_req_list_gp := ceiling(sim_sup_menge / pack_plan)]
        dt[typ_liste == "same_price" & is.finite(n_pack_req_list_gp), 
           n_pack_req := max(n_pack_req_list_gp), 
           by = c("ma_nr", "yr_cw_ind", "id_head")]
        
        # Compute supuge via number of pack required
        dt[, sim_sup_menge := n_pack_req * pack_plan]
        
      } # If no pack constraint, do nothing
      
      # Set minimum pack
      if(self$s$is_min_one_pack_sup) {
        dt[typ != "head" & sim_sup_menge == 0, 
           sim_sup_menge := pack_plan] # pack_plan is number of articles for one pack
      }
      
      return(dt) # not sure why needed as := should modify dt by reference
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get simulated sales
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    set_sim_packr = function(dt, uplift) {
      # Compute potential packr menge based on sales
      dt[, sim_sales_qty_pot := sales_qty] # Init
      
      # Uplift sellout
      dt[aq_cut == 100, 
                       sim_sales_qty_pot := round(sales_qty * self$s$uplift_sellout) ]
      
      # Compute sim_sales_qty
      dt[, sim_sales_qty := sim_sales_qty_pot] # Init: Sell full potential when available
      # Constraint packr <= sup
      dt[sim_sup_menge < sim_sales_qty_pot, 
                       sim_sales_qty := sim_sup_menge] # Cannot sell more than menge sup
      
      return(dt) # not sure why needed as := should modify dt by reference
      
      # Debug: checking dt[sim_sup_menge < sim_sales_qty_pot][1:2], ok
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get historic data 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_hist_data_same_multi = function(art_sel) {
      # Load historic data
      con <- dbConnect(SQLite(), self$db_res)
      sql <- paste0("SELECT * FROM '", self$tab_master, "' ",
                    "WHERE id IN ('", paste(art_sel, collapse = "', '"), "') ",
                    "AND typ in ('single', 'fuss') ",
                    "AND yr_cw_ind <= ", self$yr_cw_ind, ";") # Get train and test
      dt <- as.data.table(dbGetQuery(con, sql))
      dbDisconnect(con)
      # Compute uplift for out of stock
      dt[, sales_qty_uplift := sales_qty] # Init
      dt[aq_cut == 100, 
         sales_qty_uplift := round(sales_qty * self$s$uplift_sellout)] 
      # Add market stats
      mar_stat <- readRDS(file.path(ROOT, "05_Data/02_preprocessed/034_mar_stats_v01.rds"))
      # Add store attributes
      dt <- merge(dt,
                  mar_stat$rev_all_med,
                  by = "ma_nr",
                  all.x = T)
      dt <- merge(dt,
                  mar_stat$rev_group_2_med,
                  by = c("ma_nr", "group_2"),
                  all.x = T)
      return(dt)
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Same estimation 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_same_multi = function() {
      # For all group_1 to predict
      for (art_sel in self$plan[typ != "head", unique(id)]) {
        # Debug: art_sel=3247451
        
        cat("Processing article", art_sel, "\n")
        
        # Load data for train and test
        dt <- self$get_hist_data_same_multi(art_sel)
        if (nrow(dt) == 0) next
        # Compute additional features
        dt[, n_scb_vl := n_pack_vl * pack_plan]
        # Train test
        dt[yr_cw_ind < yr_cw_ind_, tt := "train"]
        dt[yr_cw_ind == yr_cw_ind_, tt := "test"]
        if(dt[tt == "train", .N] < 20) {
          loginfo("No train data available...")
          next
        }
        dt[, .N, by = tt]
        
        # Define target and features
        target <- "sales_qty_uplift"
        feat <- c("price", "rev_all_med", "rev_group_2_med", "n_scb_vl") #, "group_1") #, "ma_vf", "clus")
        # Complete cases
        dt[, cc := complete.cases(dt[, c(target, feat), with = F])]
        # TODO: Warning for which stores no prediction is possible 
        # caret
        bootControl <- trainControl(method="cv", 
                                    number = 2)
        set.seed(1234)
        fit <- train(x = dt[cc == T & tt == "train", c(feat), with = F],
                     y = dt[cc == T & tt == "train", get(target)],
                     method = "rpart", # rf, rpart
                     trControl = bootControl, 
                     tuneGrid = NULL) # set to NULL to swich off tuning grid
        # Predict
        dt[cc == T, 
           est := predict(fit, newdata = .SD[, c(feat), with = F]
           )]
        # Store results
        pred <- dt[tt == "test", .(id, ma_nr, yr_cw_ind, group_1, estimator = "same", est )]
        self$res <- rbind(
          self$res,
          pred)
        # Directly to table
        # TODO: make nice
        cat("Writing to table ...\n")
        append_table(name = s$tab_res,
                     value = pred,
                     db = s$db_res)
      } # for all group_1
      
    },  # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get historic data for stores
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_hist_single = function(mar_sel) {
      # Load historic data
      con <- dbConnect(SQLite(), self$db_res)
      sql <- paste0("SELECT * FROM '", self$tab_master, "' ",
                    "WHERE ma_nr IN ('", paste(mar_sel, collapse = "', '"), "') ",
                    "AND id IN ('", paste(self$art2pred, collapse = "', '"), "') ",
                    "AND yr_cw_ind <= ", self$yr_cw_ind, ";")
      dt <- as.data.table(dbGetQuery(con, sql))
      dbDisconnect(con)
      # Compute uplift for out of stock
      dt[, sales_qty_uplift := sales_qty] # Init
      dt[aq_cut == 100, 
         sales_qty_uplift := round(sales_qty * self$s$uplift_sellout)] 
      # Add market stats
      mar_stat <- readRDS(file.path(ROOT, "05_Data/02_preprocessed/034_mar_stats_v01.rds"))
      # Add store attributes
      dt <- merge(dt,
                  mar_stat$rev_all_med,
                  by = "ma_nr",
                  all.x = T)
      dt <- merge(dt,
                  mar_stat$rev_group_2_med,
                  by = c("ma_nr", "group_2"),
                  all.x = T)

      return(dt)
    }, # method
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Estimate demand using data from same article in same store only
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    estim_same_single = function() {
      # Get data
      loginfo("Loading data for all stores....")
      dt <- self$get_hist_single(self$mar2pred)
      # Define target and features
      target <- "sales_qty_uplift"
      feat <- c()
      # Complete cases
      dt[, cc := complete.cases(dt[, c(target, feat), with = F])]
      # Train test
      dt[yr_cw_ind < self$yr_cw_ind & cc == T, tt := "train"]
      dt[yr_cw_ind == self$yr_cw_ind & cc == T, tt := "test"]
      dt[, .N, by = tt]
      # Estimate where data is available
      est <- dt[tt == "train", 
                      .(
                        est = mean(get(target)),
                        n_train = as.numeric(.N)), 
                      by = c("id", "ma_nr")]
      # Predict train and test
      dt <- merge(dt,
            est,
            by = c("id", "ma_nr"),
            all.x = T)
      loginfo("Computing optimal uplift ....")
      
      # Get optimal uplift for each store and idseperately
      for (art_sel in intersect(self$art2pred, dt[tt == "train", unique(id)])) {
        loginfo("Article ... %s", art_sel)
        for (mar_sel in self$mar2pred) {
          loginfo("Market ... %s", mar_sel)
          dt_tmp <- dt[id == art_sel & ma_nr == mar_sel]
          if(nrow(dt_tmp[tt == "test" & !is.na(est)]) == 0 |
             nrow(dt_tmp[tt == "test" & !is.na(aq_cut)]) == 0) {
            next
          }
          # No optimization possible if no sells in train data
          if(all(dt_tmp[tt == "train", sales_qty] == 0)) {
            next
          }
          # Optimize uplift
          res_opt_train <- optimize(f = self$cost_fcn, 
                                    interval = c(0.5, 3), 
                                    dt = dt_tmp, 
                                    penalty = 10, 
                                    alpha = 0.5, 
                                    tt_ = "train",
                                    maximum = T)
          res_opt_test <- optimize(f = self$cost_fcn, 
                                   interval = c(0.5, 3), 
                                   dt = dt_tmp, 
                                   penalty = 10, 
                                   alpha = 0.5, 
                                   tt_ = "test",
                                   maximum = T)
          # Get optimal simulation
          sim <- self$sim(uplift = res_opt_train$maximum, dt_tmp)
          sim_res_opt <- sim$perf
          # Store stats 
          self$res <- rbind(
            self$res,
            data.table(
              id = art_sel,
              ma_nr = mar_sel,
              yr_cw_ind = self$yr_cw_ind,
              n_train = dt_tmp[cc == T & tt == "train", .N],
              n_test = dt_tmp[cc == T & tt == "test", .N],
              n_id_train = dt_tmp[cc == T & tt == "train", length(unique(id))],
              n_id_test = dt_tmp[cc == T & tt == "test", length(unique(id))],
              opt_uplift_train = res_opt_train$maximum,
              opt_uplift_test = res_opt_test$maximum,
              opt_cost_train = res_opt_train$objectipack, 
              opt_cost_test = res_opt_test$objectipack, 
              opt_delta_aq_test = sim_res_opt[tt == "test", delta_aq],
              opt_delta_rev_test = sim_res_opt[tt == "test", delta_rev]
            )
          )
          # Store simulation results (here in object not in db as too slow with all the iterations)
          self$res_raw <- rbind(
            self$res_raw,
            sim$data
          )
        } # for all markets
      } # for all articles
      # Store simulation results in db
      append_table(name = self$tab_res,
                   value = self$res_raw,
                   db = self$db_res)
      
    } # method
    
    
    
  ) # Public
) # Class



# ================================================================================================x
# R6:Simulate_aq ----
#   + Input is output table of Dem_est_single_store, potentially sepackral weeks
#   
# ================================================================================================x
Simulate_aq <- R6Class(
  classname = "Simulate_aq",
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    dat_mar_art = NA, # Main working table
    tab_art_ma_aw = NA,
    db_res = NA,
    estimator = NA,
    plan = NA,
    res = NA,
    idx = NA,
    s = NA,
    
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(db_res, 
                          tab_art_ma_aw, 
                          plan, 
                          dem_est, 
                          estimator, 
                          s) {
      # Store in object
      self$db_res <- db_res
      self$tab_art_ma_aw <- tab_art_ma_aw
      self$estimator <- estimator
      self$plan <- plan
      self$s <- s
      # Get historic aq table
      self$get_hist_aq_table(dem_est)
      # Add estimators to data dat_mar_art
      n_b4 <- nrow(self$dat_mar_art)
      self$dat_mar_art <- merge(self$dat_mar_art, 
                                dem_est, 
                                by = c("ma_nr", "id", "yr_cw_ind"), 
                                all.x = T)
      if(n_b4 != nrow(self$dat_mar_art)) {
        stop("Join failed")
      }
      self$simulate_aq(estimator)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get base table with AQ
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_hist_aq_table = function(dem_est) {
      yr_cw_ind_ <- dem_est[, unique(yr_cw_ind)] # Which weeks
      if(length(yr_cw_ind_) != 1) {
        stop("One week expected")
      }
      con <- dbConnect(SQLite(), self$db_res)
      sql <- paste0("SELECT * FROM `", self$tab_art_ma_aw, "` ",
                    "WHERE yr_cw_ind IN (", paste(yr_cw_ind_, sep = ","), ");")
      self$dat_mar_art <- as.data.table(dbGetQuery(con, sql))
      dbDisconnect(con)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get estimator index: Which subset of data is used as basis to compare estimator?
    #                 Note estimator usually copackrs a subset of data.
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_estimator_index = function() {
      idx <- self$dat_mar_art[, typ %in% c("single", "fuss") & 
                                !is.na(get(self$estimator)) & # only where demand estimate exists
                                !is.na(aq_cut)] # not considering some data errors (sample indicates only a few)
      return(idx)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get simulated supply, incl. pack constraints if desired
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_sim_sup = function() {
      # Set equal to estimator
      self$dat_mar_art[, sim_sup_menge := get(self$estimator) ]
      
      # General uplift for simulated supug
      self$dat_mar_art[, sim_sup_menge := round(sim_sup_menge * self$s$uplift_general)]
      
      # Set simulated supply
      if(self$s$is_pack_constraint) {
        # Apply constraints to single due to pack: round up to next pack; zeros stay zeros
        self$dat_mar_art[typ == "single", n_pack_req := ceiling(sim_sup_menge / pack_plan)]
        
        # Lists diff_price
        # How many pack would be required for each fuss
        self$dat_mar_art[typ_liste == "diff_price", 
                         n_pack_req_list_up := ceiling(sim_sup_menge / pack_plan),
                         by = c("ma_nr", "yr_cw_ind", "id_head")]
        self$dat_mar_art[typ_liste == "diff_price" & is.finite(n_pack_req_list_up), 
                         n_pack_req := max(n_pack_req_list_up), 
                         by = c("ma_nr", "yr_cw_ind", "id_head")]
        
        # Lists same_price
        self$dat_mar_art[typ_liste == "same_price", 
                         n_pack_req_list_gp := ceiling(sim_sup_menge / pack_plan)]
        self$dat_mar_art[typ_liste == "same_price" & is.finite(n_pack_req_list_gp), 
                         n_pack_req := max(n_pack_req_list_gp), 
                         by = c("ma_nr", "yr_cw_ind", "id_head")]
        
        # Compute supuge via number of pack required
        self$dat_mar_art[, sim_sup_menge := n_pack_req * pack_plan]
        
        
      } else {
        # Set simulated supug
        self$dat_mar_art[, sim_sup_menge := sim_sup_menge]
      }
      
      # Set minimum pack
      if(self$s$is_min_one_pack_sup) {
        self$dat_mar_art[typ != "head" & sim_sup_menge == 0, 
                         sim_sup_menge := pack_plan]
      }
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get simulated sales
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_sim_packr = function() {
      # Compute potential packr menge based on sales
      self$dat_mar_art[, sim_sales_qty_pot := sales_qty] # Init
      
      # If assuming no zero sells
      if(self$s$is_no_zero_sells) {
        self$dat_mar_art[sales_qty == 0, sim_sales_qty_pot := 1] # Expect no zero sells
      }
      
      # If assuming reduced zero sells
      if(self$s$zero_sells_reduction != 0) {
        n_zero <- self$dat_mar_art[sales_qty == 0, .N]
        set.seed(123)
        idx_change <- sample(x = 1:n_zero, size = round(n_zero * self$s$zero_sells_reduction))
        self$dat_mar_art$id <- 1:nrow(self$dat_mar_art)
        ids_change <- self$dat_mar_art[sales_qty == 0, id][idx_change]
        self$dat_mar_art[id %in% ids_change, sim_sales_qty_pot := 1] # Expect some sales
        self$dat_mar_art$id <- NULL
      }
      
      
      # Uplift sellout
      self$dat_mar_art[aq_cut == 100, 
                       sim_sales_qty_pot := round(sales_qty * self$s$uplift_sellout) ]
      
      # Compute sim_sales_qty
      self$dat_mar_art[, sim_sales_qty := sim_sales_qty_pot] # Init: Sell full potential when available
      self$dat_mar_art[sim_sup_menge < sim_sales_qty_pot, 
                       sim_sales_qty := sim_sup_menge] # Cannot sell more than menge sup
      
      # Debug: checking self$dat_mar_art[sim_sup_menge < sim_sales_qty_pot][1:2], ok
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Simulate AQ based on estimated supply
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    simulate_aq = function(estimator) {
      # Define subset of relevant articles for estimator for AQ computation and comparison with other estimators
      idx <- self$get_estimator_index()
      # Set simulated supply
      self$get_sim_sup()
      # Set simulated sales
      self$get_sim_packr()
      # Simulate repacknue
      self$dat_mar_art[, sim_packr_um_br := price * sim_sales_qty]
      # Compute AQ
      self$dat_mar_art[, sim_aq := sim_sales_qty / sim_sup_menge * 100] 
      # Simulation result stats
      n_art <- self$dat_mar_art[typ %in% c("single", "fuss"), .N] # Number of articles
      n_art_scope <- sum(idx, na.rm = T)
      
      sim <- self$dat_mar_art
      res <- data.table(
        estimator = estimator,
        sum_sup_real = self$dat_mar_art[idx,
                                        sum(sup_menge, na.rm = T)],
        sum_sup_sim = self$dat_mar_art[idx,
                                       sum(sim_sup_menge, na.rm = T)],
        sum_packr_real = self$dat_mar_art[idx,
                                       sum(sales_qty, na.rm = T)],
        sum_packr_sim = self$dat_mar_art[idx,
                                       sum(sim_sales_qty, na.rm = T)],
        aq_real = round(self$dat_mar_art[idx, 
                                         mean(aq_cut)], 1), 
        aq_sim = round(self$dat_mar_art[idx & sim_sup_menge > 0, # exclude zero sup
                                        mean(sim_aq, na.rm = T)], 1), 
        copackr_est = round(self$dat_mar_art[typ %in% c("single", "fuss") & 
                                             !is.na(get(estimator)), 
                                           .N] / n_art * 100, 1),
        zeros_rel = round(self$dat_mar_art[typ %in% c("single", "fuss") & 
                                             sim_sup_menge == 0, 
                                           .N] / n_art * 100, 1),
        rev_real_subset = self$dat_mar_art[idx, 
                                           sum(packr_um_br, na.rm = T)],
        rev_sim_subset = self$dat_mar_art[idx, 
                                          sum(sim_packr_um_br, na.rm = T)],
        rev_sim_sub_plus = self$dat_mar_art[idx & sim_packr_um_br > packr_um_br, # Add. rev. in simulation
                                           sum(sim_packr_um_br - packr_um_br, na.rm = T)],
        rev_sim_sub_minus = self$dat_mar_art[idx & sim_packr_um_br < packr_um_br, 
                                           sum(sim_packr_um_br - packr_um_br, na.rm = T)] # Less rev. in simulation
      )

      # Compute changes
      res[, aq_delta := aq_sim - aq_real ]
      res[, rev_delta := rev_sim_subset - rev_real_subset]
      res[, rev_delta_rel := round(rev_delta / rev_real_subset * 100, 1) ]
      
      # Store
      self$res <- res
      self$idx <- idx
    }
    
  ) # Public
) # Class


# ================================================================================================x
# R6:sales_np ----
# ================================================================================================x
sales_np <- R6Class(
  classname = "sales_np",
  
  # ==============================================================================================x
  # Private ----
  # ==============================================================================================x
  private = list(
    # --------------------------------------------------------------------------------------------x
    # Description   : Connect to db
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    connect = function() {
      self$con <- dbConnect(SQLite(), self$db)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Disconnect db
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    disconnect = function() {
      dbDisconnect(self$con)
    }
    
  ), # End private
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    db = NA, # DB
    tab = NA, # Table
    con = NA, # DB connection
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(db, tab) {
      self$db <- db
      self$tab <- tab
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get x-week aggregate of sales on market-article lepackl
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_week_agg = function(weeks, cw_, year_) {
      # Get calendar week counter mapping
      map_counter_cw <- get_week_year_index()
      cw_counter <- map_counter_cw[cw == cw_ & year == year_, yr_cw_ind] 
      # Query data
      private$connect()
      sql <- paste0("SELECT * FROM '", self$tab, "' ",
                    "WHERE yr_cw_ind IN (", paste(cw_counter + weeks, collapse = ", "), ")")
      dt <- as.data.table(dbGetQuery(self$con, sql))
      private$disconnect()
      # Aggregate
      dt_agg <- dt[sales_qty > 0,
                   .(sales_qty = sum(sales_qty, na.rm = T),
                     packr_um_br = sum(packr_um_br, na.rm = T),
                     packr_roher = sum(packr_roher, na.rm = T)),
                   by = c("id", "ma_nr")]
      # Add cols
      dt_agg[, c("cw", "year", "yr_cw_ind") := list(cw_, year_, cw_counter)]
      return(dt_agg)
    }
  )
)


# ================================================================================================x
# R6:Plan ----
# ================================================================================================x
Plan <- R6Class(
  classname = "Plan",
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    db = NA, # DB
    tab = NA, # Table
    data = NA, # Plan data
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(db, tab) {
      self$db <- db
      self$tab <- tab
      self$load_data()
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    load_data = function() {
      con <- dbConnect(SQLite(), self$db)
      # Load from DB
      plan <- as.data.table(dbReadTable(con, self$tab))
      # Add yr_cr_ind
      plan <- merge(plan, get_week_year_index(), by = c("year", "cw"), all.x = T)
      # Remopack dups
      plan <- plan[!duplicated(plan[, .(yr_cw_ind, id)])]
      # Add group_2
      plan[, group_2 := substr(group_1, 1, 2)] # Add group_2
      
      dbDisconnect(con)
      
      self$data <- plan
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get mapping of id to ref id in long format
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_ref_mapping = function() {
      # Max number of entries per id
      n_ <- max(unlist(lapply(strsplit(x = self$data$id_ref, split = ",", fixed = T), length)))
      # Extract
      id_ref <- as.data.table(str_split_fixed(string = self$data$id_ref, pattern = ",", n = n_))
      # Add id
      id_ref <- cbind(self$data[, .(new_id, id)], id_ref)
      # To long format
      id_ref <- melt(id_ref, id.vars = c("new_id", "id"))
      # Clean up 
      id_ref <- id_ref[value != ""]
      id_ref$variable <- NULL
      setnames(id_ref, "value", "ref")
      return(id_ref)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get mapping of id to number of pack per packrteilerschluessel
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    extract_n_pack_vs = function() {
      
      # Preprocessing (todo: to loading script)
      self$data[, pack_per_schl := tolower(pack_per_schl)] # tolower
      self$data[, pack_per_schl := gsub(pattern = "\\r\\n", replacement = "", x = pack_per_schl)] # Remopack line breaks
      self$data[, pack_per_schl := gsub(pattern = ":", replacement = "", x = pack_per_schl, fixed = T)] # Remopack :
      self$data[, pack_per_schl := trim(pack_per_schl)] # trim
      
      # --------------------------------------------------------------------------------------------x
      # Description   : Conpackrt packctor of strings of n_pack to data.table with one col for each vs
      # Input         : 
      # Review status : 
      # --------------------------------------------------------------------------------------------x
      extr_n_pack_str <- function(n_pack_str) {
        res <- data.table()
        for (x in n_pack_str) {
          # x="8-6-3-2"
          n_pack <- unlist(str_split(string = x, pattern = "-"))
          names(n_pack) <- c("n_pack_vl", "n_pack_l", "n_pack_m", "n_pack_s")
          res <- rbind(res,  data.table(n_pack_str = x, t(n_pack)))
        }
        return(res)
      }
      
      # Init
      dt_n_pack <- data.table()
      
      # For all entries
      for(new_id_ in self$data$new_id) {
        # new_id_=5
        row <- self$data[new_id == new_id_]
        
        # Only keep rows of interest
        if (row$typ == "fuss" | 
            is.na(row$pack_per_schl) |
            !grepl(pattern = "\\d+-\\d+-\\d+-\\d+", x = row$pack_per_schl)) {
          next
          }
        
        # Case 1 with vb
        if ( grepl(pattern = "vb ?", x = row$pack_per_schl)){
          # Split wrt vb
          split_vb <- unlist(str_split(string = row$pack_per_schl, pattern = "vb ?"))
          # Remopack parts not of interest
          idx_vs <- grepl(pattern = "\\d+-\\d+-\\d+-\\d+", x = split_vb)
          split_vb <- split_vb[idx_vs]
          # Extract vb
          vb <- trim(unlist(str_extract_all(string = split_vb, pattern = "\\d+ ")))
          # Extract n_pack strings
          n_pack_str <- unlist(str_extract_all(string = row$pack_per_schl, pattern = "\\d+-\\d+-\\d+-\\d+"))
          # Init table
          dt_tmp <- extr_n_pack_str(n_pack_str)
          # Add vb 
          dt_tmp <- cbind(vb, dt_tmp)
          # Add further cols
          dt_tmp <- data.table(id = row$id, 
                               yr_cw_ind = row$yr_cw_ind,
                               dt_tmp)
        } else {
          # Case 2: single 
          n_pack_str <- str_extract(string = row$pack_per_schl, pattern = "\\d+-\\d+-\\d+-\\d+")
          # Init table
          dt_tmp <- extr_n_pack_str(n_pack_str)
          # Add vb 
          dt_tmp <- cbind(vb = c(40, 65), dt_tmp)
          # Add further cols
          dt_tmp <- data.table(id = row$id, 
                               yr_cw_ind = row$yr_cw_ind,
                               dt_tmp)
        }
        
        # Postprocessing: workaround for cases like "2-2-1-1121 dortmund"
        dt_tmp[n_pack_s > n_pack_m, n_pack_s := substr(x = n_pack_s, start = 1, stop = 1)]
        
        # Store in dt for all ids
        dt_n_pack <- rbind(dt_n_pack, dt_tmp)
      }
      
      # Staging
      cols <- c("n_pack_vl", "n_pack_l", "n_pack_m", "n_pack_s")
      dt_n_pack[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
      
      return(dt_n_pack)
    }
    
  )
)

# ================================================================================================x
# R6:Market  ----
# ================================================================================================x
Market <- R6Class(
  classname = "Market",
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    db = NA, # DB
    tab = NA, # Table
    data = NA, # Plan data
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(db, tab) {
      self$db <- db
      self$tab <- tab
      self$load_data()
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    load_data = function() {
      con <- dbConnect(SQLite(), self$db)
      # Load from DB
      self$data <- as.data.table(dbReadTable(con, self$tab))
      # Remopack dups
      self$data <- self$data[!duplicated(ma_nr)]
      dbDisconnect(con)
    }
  )
)

# ================================================================================================x
# R6:Extract multiple xlxs for plan data  ----
# ================================================================================================x
Extract_multi_xls <- R6Class(
  classname = "Extract_multi_xls",
  
  # ==============================================================================================x
  # Private ----
  # ==============================================================================================x
  private = list(
    
  ), # End private
  
  # ==============================================================================================x
  # Public ----
  # ==============================================================================================x
  public = list(
    
    # Attributes
    xls_source_folder = NA,
    file_name_pattern = NA, 
    file_list = NA,
    s = NA, # Settings
    
    # Use case specific
    cw = NA,
    year = NA,
    
    # --------------------------------------------------------------------------------------------x
    # Constructor
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    initialize = function(xls_source_folder, file_name_pattern, s) {
      self$xls_source_folder <- xls_source_folder
      self$file_name_pattern <- file_name_pattern
      self$file_list <- list.files(path = xls_source_folder,
                                   pattern = file_name_pattern,
                                   full.names = F)
      self$s <- s
      # Use case specifics
      self$cw <- as.numeric(substring(self$file_list, 14, 15))
      self$year <- as.numeric(substring(self$file_list, 17, 20))
      
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Reads sheet from xlsx
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    read_xls = function(file, sheet = 1) {
      tab <- as.data.table(
        read_excel(path = file, 
                   sheet = sheet,
                   col_names = F,
                   col_types = "text"))
      # Add id
      tab$new_id_raw <- 1:nrow(tab)
      return(tab)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Find row and column of pattern in sheet
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    find_pattern_sheet = function(file, sheet = 1, pattern, fixed = T) {
      # Read sheet
      tab <- self$read_xls(file = file, sheet = sheet)
      # Index
      idx_tab <- tab[, lapply(FUN = grepl, pattern = pattern, X = .SD, fixed = T), .SDcols = colnames(tab)]
      count <- unlist(lapply(idx_tab, sum))
      idx_first <- unlist(lapply(idx_tab, match, x = 1 )) # row of first occurance of pattern
      return(list(idx_tab = idx_tab, count = count, idx_first = idx_first))
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get relevant columns from plan 
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_cols = function(dt) {
      # identify rows for headers
      header_row <- rownames(dt)[grep(T, apply(dt, 1, function(x) any("blanco" %in% x)))]
      if(length(header_row)!=1) {
        print(header_row)
        stop("header row No. wrong.")
      }
      # search col name pattern in header rows
      cols <- list()
      # col id reference
      cols$id_ref <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("reference" %in% x)))]
      # col id
      cols$id <-colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("id" %in% x & !"reference" %in% x)))]
      # col article description + Werbekreise 
      cols$art_sup <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any(grepl("description", x) & grepl("flyer", x))))]
      # col pos
      cols$pos <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("position" %in% x)))]
      # col packrpackungseinheit
      cols$pack_plan <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("pack" %in% x & !"delivery" %in% x)))]
      # col group_1
      cols$group_1 <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("Waren-" %in% x)))]
      # col packrteilschlüssel
      cols$pack_per_schl <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("packaging" %in% x)))]
      # col packrkaufspreis
      cols$price <- colnames(dt)[grep(T, apply(dt[1:header_row], 2, function(x) any("forcasting\price" %in% x)))]
      return(cols)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get table of which row subheader starts and ends for one sheet
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_subheaders = function(file, sheet = 1) {
      # Read sheet
      tab <- self$read_xls(file = file)
      cols <- self$get_cols(tab)
      # check if one and only one col was selected
      if(max(lengths(cols))!=1 | min(lengths(cols))==0){
        stop("Columns wrong.")
      }
      # Find row of header for id
      row_head <- which(select(tab, cols$id)[,1] == "id")
      row_first_id <- row_head + 1 # First row of ids
      row_last_id <- max(which(!is.na(select(tab, cols$id)[,1]))) # Last row of ids
      # Find subheader rows
      dat <- data.table(n = 1:nrow(tab), select(tab, cols$id), select(tab, cols$art_sup)) # Werbekreise and idS
      colnames(dat) <- c("n", "id", "wk")
      sh <- dat[n > row_first_id & n < row_last_id & is.na(id) & !is.na(wk), -"id"]
      sh[, row_start := n + 1]
      sh[, row_end := c(sh$n[2:nrow(sh)] - 1, row_last_id)]
      # Check that next section starts after end of prev
      if(!all(sh$row_end[1:(nrow(sh)-1)] < sh$row_start[2:nrow(sh)])) {
        print(file)
        print(sh)
        stop("Subheaders wrong.")
      }
      # Deal with no entries in subsection: filter out
      sh <- sh[row_start < row_end]
      return(sh)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get table of which row subheader starts and ends for all files
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    get_subheaders_all_files = function() {
      # Build table in long format
      sh <- data.table()
      # For all files
      for (i in 1:length(self$file_list)) {
        file_ <- self$file_list[i]
        file <- file.path(self$xls_source_folder, file_)
        cat("Getting sub header for", file_, "\n")
        sh_ <- self$get_subheaders(file)
        sh <- rbind(sh, cbind(sh_, 
                              cw = self$cw[i], 
                              year = self$year[i], 
                              time = paste(self$year[i], sprintf("%02d", self$cw[i]), sep = "-")))
      }
      sh <- sh[order(time)] # order
      return(sh)
    },
    
    # --------------------------------------------------------------------------------------------x
    # Description   : Get plan data
    # Input         : 
    # Review status : 
    # --------------------------------------------------------------------------------------------x
    read_plan_data = function() {
      # Init
      plan <- data.table()
      # For all files
      for (i in 1:length(self$file_list)) {
        # Debug: i=3
        file_ <- self$file_list[i]
        cat("Building plan data for", file_, "\n")
        file <- file.path(self$xls_source_folder, file_)
        cw_ <- self$cw[i]
        year_ <- self$year[i]
        # Read sheet
        tab <- self$read_xls(file = file)
        cols <- self$get_cols(tab)
        # For all Werbekreise
        for (wk_ in self$s$wk_scope) {
          # Debug: wk_="Werbeartikel \r\nNahkauf + REWE + CK + CG + Welt" 
          sh_ <- sh[wk == wk_ & cw == cw_ & year == year_]
          if (nrow(sh_) == 0) {
            next
          }
          # Read segment from sheet (hardcoded here)
          tab_ <- select(tab, new_id_raw, # Unique id row for sheet
                         cols$id_ref,
                         cols$id,
                         cols$art_sup,
                         cols$pos,
                         cols$pack_plan,
                         cols$group_1,
                         cols$pack_per_schl,
                         cols$price)
          row_num <- unlist(apply(sh_, 1, function(x) seq(x[3], x[4])))
          tab_ <- tab_[new_id_raw %in% row_num]
          colnames(tab_) <-c("new_id_raw","id_ref", "id", "art_sup", "pos", "pack_plan", "group_1","pack_per_schl", "price")
          # Add additional columns
          tab_$werbekreis <- wk_
          tab_$cw <- cw_
          tab_$year <- year_
          # Amend plan table
          plan <- rbind(plan, tab_)
        } # for all wk
        
      } # for all files
      # Add id
      plan[, new_id := 1:nrow(plan)]
      # Do not order plan, as order is important later to reconstruct stuecklisten!!!
      return(plan)
    }
    
  ) # End public
) # End class


