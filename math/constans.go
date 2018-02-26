package math

const L float64 = 200
const L_ind float64 = 200
const L_join float64 = 2000

const b float64 = 10

const L_b = 10 // так как предполагается, что число записей в промежутчной таблице в одном блоке в 10 раз больше

const C_comp float64 = 0.01
const C_move float64 = 0.01
const C_filter float64 = 0.000000001
const C_b float64 = 0.0002
const C_join = 0
const C_join_io = 0

const D float64 = 18432 // длина блока таблицы в байтах 18Kb
const D_ind float64 = 16384 // длина блока индекса 16Kb
const K float64 = 4 // количество тактов на простую операцию

const OnlineTransactionType = "online"
const OfflineTransactionType = "offline"