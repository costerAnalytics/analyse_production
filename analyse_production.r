library(dotenv)
load_dot_env()
library(RPostgres)
library(dplyr)
library(stringr)
library(lme4)
library(ggplot2)
library(tidyr)

for(VAR in c('pg_host', 'pg_dbname', 'pg_user', 'pg_pwd', 'pg_schema', 'betrieb')){
  assign(VAR, Sys.getenv(VAR))
  if(VAR == '') stop(paste('Missing', VAR))
}

if(!exists('pgdb') || dbIsValid(pgdb)) pgdb <- dbConnect(Postgres(), host = pg_host,
                  dbname = pg_dbname, user = pg_user, pass=pg_pwd)
quer <- function(...) dbGetQuery(pgdb, paste0(...))

predict_curve <- function(curve, day){
  a <- curve[1]
  b <- curve[2]
  c <- curve[3]
  return(sapply(day, \(x){a + b * x + c * exp(-.05 * x)}))
}
compute_total_production <- function(curve, d1, d2){
  a <- curve[1]
  b <- curve[2]
  c <- curve[3]
  primitive <- function(x, a, b, c){
	return(a * x + 1 / 2 * b * x^2 - c / .05 * exp(-.05 * x))
  }
  return(primitive(d2, a, b, c) - primitive(d1, a, b, c))
}


quarters <- expand.grid(year=2016:2022, quarter=1:4) |> 
  arrange(year, quarter) |>
  filter(year < 2022 | quarter < 2)
quarters <- quarters |> transform(
  yearqtr = paste(year, quarter, sep = '-'),
  first_305 = NA_real_,
  later_305 = NA_real_,
  fist_30_60 = NA_real_,
  later_30_60 = NA_real_,
  first_60_120 = NA_real_,
  later_60_120 = NA_real_)


for(i in 1:nrow(quarters)){
  calvings <- quer(str_glue('select tier, laktation from {pg_schema}.hw_kalbung where 
                            extract(quarter from datum) = {quarters$quarter[i]} and
                            extract(year from datum) = {quarters$year[i]}'))
  milk_controls <- quer(str_glue('
    with aux as (select 
      id,
      tier,
      laktation,
      datum,
      lead(datum) over(partition by tier order by laktation) next_datum
    from {pg_schema}.hw_kalbung),
    calvings as (select 
      id,
      tier,
      laktation,
      datum,
      coalesce(next_datum, now()::date) next_datum
    from aux where
      extract(quarter from datum) = {quarters$quarter[i]} and
      extract(year from datum) = {quarters$year[i]})
    select 
      m.id,
      m.tier,
      m.datum,
      c.datum calving,
      m.datum - c.datum dil,
      c.laktation,
      m.mkg
    from {pg_schema}.hw_mlp m
    right join calvings c on
      m.tier = c.tier and
      m.datum >= c.datum and
      m.datum < c.next_datum
    where 
      m.mkg is not null and
      m.mkg > 0
    order by m.tier, m.datum
    '))
  
  milk_controls <- milk_controls |> 
    transform(lactation_cat = ifelse(laktation == 1, 'first', 'later')) |>
    transform(lactation_cat = factor(lactation_cat))
  
  mm <- lmer(mkg ~ -1  + lactation_cat + 
               dil:lactation_cat + I(exp(-0.05 * dil)):lactation_cat +
               (dil | tier) + (0 + I(exp(-0.05 * dil)) | tier),
             data=milk_controls)
  
  feff <- fixef(mm)
  curves <- list()
  curves$first_lactation <- c(feff[1], feff[3], feff[5])
  curves$later_lactation <- c(feff[2], feff[4], feff[6])
  names(curves[[1]]) <- names(curves[[2]]) <- c('a', 'b', 'c')
  
  quarters$first_305[i] = compute_total_production(curves$first_lactation, d1=0, d2=305)
  quarters$later_305[i] = compute_total_production(curves$later_lactation, d1=0, d2=305)
  quarters$fist_30_60[i] = compute_total_production(curves$first_lactation, d1=30, d2=60)
  quarters$later_30_60[i] = compute_total_production(curves$later_lactation, d1=30, d2=60)
  quarters$first_60_120[i] = compute_total_production(curves$first_lactation, d1=60, d2=120)
  quarters$later_60_120[i] = compute_total_production(curves$later_lactation, d1=60, d2=120)
  
}

quarters <- quarters |> gather(
  key=lactation_period, value=production, first_305:later_60_120)

quarters_30_60 <- quarters |> 
  filter(grepl('30_60', lactation_period)) |>
  mutate(lactation = gsub('_30_60', '', lactation_period))
quarters_60_120 <- quarters |> 
  filter(grepl('60_120', lactation_period)) |>
  mutate(lactation = gsub('_60_120', '', lactation_period))
quarters_305 <- quarters |> 
  filter(grepl('305', lactation_period)) |>
  mutate(lactation = gsub('_305', '', lactation_period))

plt_30_60 <- ggplot(data = quarters_30_60, aes(x=yearqtr, y=production, group=lactation)) +
  geom_line(aes(col=lactation))
plt_30_60 <- plt_30_60 + 
  theme(panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(), 
        axis.line = element_line(colour = "black")) +
  xlab('Calving in quarter') +
  ylab('30-60 days production (kg)') +
  ggtitle(paste0(betrieb, ' 30-60 days')) +
  theme(axis.text.x=element_text(angle=-45, hjust=0.001)) +
  theme(plot.title = element_text(hjust = 0.5))
plot(plt_30_60)

plt_60_120 <- ggplot(data = quarters_60_120, aes(x=yearqtr, y=production, group=lactation)) +
  geom_line(aes(col=lactation))
plt_60_120 <- plt_60_120 + 
  theme(panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(), 
        axis.line = element_line(colour = "black")) +
  xlab('Calving in quarter') +
  ylab('60-120 days production (kg)') +
  ggtitle(paste0(betrieb, ' 60-120 days')) +
  theme(axis.text.x=element_text(angle=-45, hjust=0.001)) +
  theme(plot.title = element_text(hjust = 0.5))
plot(plt_60_120)

plt_305 <- ggplot(data = quarters_305, aes(x=yearqtr, y=production, group=lactation)) +
  geom_line(aes(col=lactation))
plt_305 <- plt_305 + 
  theme(panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank(), 
        panel.background = element_blank(), 
        axis.line = element_line(colour = "black")) +
  xlab('Calving in quarter') +
  ylab('305 days production (kg)') +
  ggtitle(paste0(betrieb, ' 305 days')) +
  theme(axis.text.x=element_text(angle=-45, hjust=0.001)) +
  theme(plot.title = element_text(hjust = 0.5))
plot(plt_305)



write.table(
  quarters |> select(yearqtr, lactation_period, production),
  file=paste0('quarters_', betrieb, '.txt'),
  sep='\t', quote=FALSE, col.names=TRUE,row.names=FALSE, dec=',')

