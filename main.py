import ocel as oc
import pandas as pd
import numpy as np
from functools import reduce
import datetime as dt

InDec = pd.read_csv('/Users/mjacobo/Downloads/InternationalDeclarations.csv')
InDec["Complete Timestamp"] = pd.to_datetime(InDec["Complete Timestamp"], format='%Y/%m/%d %H:%M:%S.%f')
InDec["Complete Timestamp_s"] = InDec["Complete Timestamp"].shift(-1)
# InDec["Complete Timestamp_s"] = InDec["Complete Timestamp"].shift(-1)
InDec["Duration_hr"] = (InDec["Complete Timestamp_s"] - InDec["Complete Timestamp"]) / np.timedelta64(1, 'h')

filter = InDec["Resource"] != "SYSTEM"
# filter = InDec["Activity"] == "Permit SUBMITTED by EMPLOYEE"
filter2 = InDec["Amount"] != 0
filter3 = InDec["Duration_hr"] >= 0
# activity1 = InDec.where(filter & filter2).copy()
# print(activity1.dropna(axis='rows'))
InDecDF = InDec.sort_values(["Case ID", "Complete Timestamp"]).where(filter & filter2 & filter3)
prod = InDecDF.groupby("Case ID")["Amount"].count().reset_index(name="Products").copy()
ktab = InDecDF.groupby("Case ID")["Amount"].max().reset_index(name="K").copy()
ltab = InDecDF.groupby("Case ID")["Duration_hr"].sum().reset_index(name="L").copy()

dfs = [prod, ktab, ltab]
df_final = reduce(lambda left,right: pd.merge(left,right,on='Case ID'), dfs)
df_final["prty"] = df_final["Products"] / df_final["L"]

print(df_final)



# InConDF = InDecDF[["Case ID", "Amount", "Duration_hr"]].groupby(["Case ID"]).agg(['max', 'sum', 'count',
# 'std']).copy() InConDF.set

# print(InConDF[["Case ID"], ["Amount"], ["max"], ["Duration_hr"], ["sum"]])

# filter = InDec["Resource"] != "SYSTEM"
# InDec = InDec.sort_values(["Case ID", "Complete Timestamp"]). \
#    where(filter)
# print(InDec[["Case ID", "Activity",  "Complete Timestamp", "Duration_hr"]])

# d = dict(tuple(InDecDF.sort_values(["Case ID", "Complete Timestamp"]).groupby("Case ID")))
# print(InDec[["Case ID", "Activity", "Complete Timestamp", "RequestedAmount", "Duration_hr"]])
# print(d["declaration 76667"][["Case ID", "Activity", "AdjustedAmount", "Duration_hr"]])
# resultDI = np.array([])
# for indx in d.keys():
#    print(d[indx].aggregate(['sum', 'min']))
#        print(d[indx][["Case ID", "Amount", "Duration_hr"]].groupby(["Case ID"])
#                 .agg(['max', 'sum', 'count', 'std']))
#    resultDI.put(indx, d[indx][["Case ID", "Amount", "Duration_hr"]].groupby(["Case ID"])
#                 .agg(['max', 'sum', 'count', 'std']))

#    con = pd.DataFrame(d[indx][["Case ID"]].groupby(["Case ID"]))
#    con["production"] = d[indx][["Case ID", "Amount"]].groupby(["Case ID"]).agg(['count'])
#    con["Amount"] = d[indx][["Case ID", "Amount"]].groupby(["Case ID"]).agg(['max'])
#    con["Duration_hr"] = d[indx][["Case ID", "Duration_hr"]].groupby(["Case ID"]).agg(['sum'])
#    print(con)
# resultDict.add(con)

# result = np.reshape(resultDI, (resultDI.shape[0], resultDI.shape[2]))
# print(result.shape)
# resultDF = pd.DataFrame(result)
# print(resultDF)
#    print(d[indx][["Case ID", "Amount", "Duration_hr"]]
#          .groupby(["Case ID"]).agg(['max', 'sum', 'count', 'std']))
#    print(type(d[indx]))
#   print(d[indx][["Case ID", "Activity", "Resource", "Complete Timestamp", "Amount", "Duration_hr", "org:role"]])

# print(InDec)

# InDec.where(filter, inplace=True)


# print(InDec["Complete Timestamp"].diff())

# print(InDec[["Activity", "Complete Timestamp"]].dropna())
