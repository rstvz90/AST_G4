library(data.table)
library(lubridate)

gc()
rm(list = ls())

# FUNCION DE AGRUPACION

agrupamiento_km <- function(Fecha_hora) {
  
  index_na <- which(is.na(Fecha_hora))
  for (i in index_na) {
    Fecha_hora[i] <- Fecha_hora[i - 1]
  }
  return(Fecha_hora)
}

# KILOMETROS

km <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/km_tp_st.csv", 
            dec = ",")

km[, Ficha := substr(Ficha, 2, 5)]
km[, Ficha := as.numeric(Ficha)]

km <- km[!(is.na(Ficha)),]
km <- km[!(is.na(Linea)),]
km <- km[!Linea == "",]

lineas_a_filtrar <- km[, .(cant_ficha= uniqueN(Ficha)), by = "Linea"]
lineas_a_filtrar <- lineas_a_filtrar[cant_ficha < 10, "Linea"]

km <- km[!Linea %in% lineas_a_filtrar[[1]],] #filtro las lineas con menos de 10 coches

km <- km[!(is.na(Fecha)),]
# Se filtran Unidades de Negocio que no son de interes
km <- km[UN != "SAF"]
km <- km[UN != ""]
km <- km[UN != "Pap"]

km[,  Mes := month(Fecha)]
km[, Semana := floor_date(Fecha, unit="week")]
km[, Year := year(Fecha)]

km_dia <- km[, .(KM = sum(Km, na.rm = TRUE)), by = .(Fecha, Linea)]
km_semana <- km[, .(KM = sum(Km, na.rm = TRUE)), by = .(Semana, Linea)]
km_mes <- km[, .(KM = sum(Km, na.rm = TRUE)), by = .(Mes, Year, Linea)]

fwrite(km_dia, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/KM_dia.csv")
fwrite(km_semana, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/KM_semana.csv")
fwrite(km_mes, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/KM_mes.csv")

# CARGAS COMBUSTIBLE

lts <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/lts_tp_st.csv", 
            dec = ",")

lts[, Ficha := substr(Ficha, 2, 5)]

lts[, Ficha := as.numeric(Ficha)]

lts <- lts[!(is.na(Ficha)),]
lts <- lts[!(is.na(FechaCorregida)),]

lts <- lts[ConsumoTotal>0 & ConsumoTotal<400,]

lts[,  Mes := month(FechaCorregida)]

lts[, Semana := floor_date(FechaCorregida, unit="week")]

consumo <- merge.data.table(km, lts, 
                            by.x = c("Fecha", "Ficha"),
                            by.y = c("FechaCorregida", "Ficha"),
                            all = TRUE)

setorder(consumo, Ficha, -Fecha)

consumo[,Fecha_hora := agrupamiento_km(`Fecha Hora consumo`), by = "Ficha"]

lts_agrupado <- consumo[, .(LTS = sum(ConsumoTotal, na.rm = TRUE),
                            #KM = sum(Km, na.rm = TRUE),
                            Linea = first(sort(Linea)),
                            Texto = first(sort(Texto)),
                            Almacen = first(sort(CodAlmacen)),
                            Producto = first(sort(ProductName))
                            ), 
                        by = .(Ficha, Fecha_hora)
                        ]

lts_agrupado <- lts_agrupado[!is.na(Linea),]
lts_agrupado <- lts_agrupado[!is.na(Ficha),]

lts_agrupado <- lts_agrupado[!Linea == "",]

lts_agrupado[, Fecha := as.Date(Fecha_hora)]
lts_agrupado <- lts_agrupado[hour(Fecha_hora) < 6, Fecha := Fecha - 1]

lts_agrupado <- lts_agrupado[!is.na(Fecha),]

lts_agrupado[, Mes := month(Fecha)]
lts_agrupado[, Semana := floor_date(Fecha, unit = "week")]
lts_agrupado[, Year := year(Fecha)]

lts_dia <- lts_agrupado[, .(LTS = sum(LTS, na.rm = TRUE)), by = .(Fecha, Linea)]
lts_semana <- lts_agrupado[, .(LTS = sum(LTS, na.rm = TRUE)), by = .(Semana, Linea)]
lts_mes <- lts_agrupado[, .(LTS = sum(LTS, na.rm = TRUE)), by = .(Mes, Year, Linea)]

fwrite(lts_dia, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/LTS_dia.csv")
fwrite(lts_semana, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/LTS_semana.csv")
fwrite(lts_mes, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/LTS_mes.csv")

# CONSUMO

consumo_agrupado <- consumo[, .(LTS = sum(ConsumoTotal, na.rm = TRUE),
                                KM = sum(Km, na.rm = TRUE),
                                Linea = first(sort(Linea))
), 
by = .(Ficha, Fecha_hora)
]

consumo_agrupado <- consumo_agrupado[!is.na(Linea),]
consumo_agrupado <- consumo_agrupado[!is.na(Ficha),]

consumo_agrupado <- consumo_agrupado[!Linea == "",]

consumo_agrupado[, Fecha := as.Date(Fecha_hora)]
consumo_agrupado <- consumo_agrupado[hour(Fecha_hora) < 6, Fecha := Fecha - 1]

consumo_agrupado <- consumo_agrupado[!is.na(Fecha),]

consumo_agrupado[, Mes := month(Fecha)]
consumo_agrupado[, Semana := floor_date(Fecha, unit = "week")]
consumo_agrupado[, Year := year(Fecha)]

consumo_dia <- consumo_agrupado[, .(LTS = sum(LTS, na.rm = TRUE),
                            KM = sum(KM, na.rm = TRUE)), 
                        by = .(Fecha, Linea)]

consumo_semana <- consumo_agrupado[, .(LTS = sum(LTS, na.rm = TRUE),
                               KM = sum(KM, na.rm = TRUE)), 
                           by = .(Semana, Linea)]

consumo_mes <- consumo_agrupado[, .(LTS = sum(LTS, na.rm = TRUE),
                            KM = sum(KM, na.rm = TRUE)), 
                            by = .(Mes, Year, Linea)]

# el consumo se mide en litros cada 100km
consumo_dia[, Consumo := LTS / KM * 100]
consumo_semana[, Consumo := LTS / KM * 100]
consumo_mes[, Consumo := LTS / KM * 100]

fwrite(consumo_dia, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/CONSUMO_dia.csv")
fwrite(consumo_semana, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/CONSUMO_semana.csv")
fwrite(consumo_mes, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/CONSUMO_mes.csv")








