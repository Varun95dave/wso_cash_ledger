import re
import pandas as pd
import datetime

def data_preprocessor(bank_filepath,transaction_filepath):

    bdf = pd.read_csv(bank_filepath, encoding="ISO-8859-1")
    tdf = pd.read_csv(transaction_filepath, encoding="ISO-8859-1")

    print(f"bank file has {bdf.shape[0]} records")

    bdf_cols = list(bdf.columns)
    tdf_cols = list(tdf.columns)

    bdf['settled_date'] = bdf['Settled'].apply(lambda dt: datetime.datetime.strptime(dt, "%d-%m-%Y") if type(dt) == str else dt)
    bdf['Amount_float'] = [float(re.sub(',', '', amt)) if amt != ' -   ' else 0 for amt in bdf['Amount']]
    tdf['settle_pay_date'] = tdf['Settle / Pay Date'].apply(lambda dt: datetime.datetime.strptime(dt, "%d-%m-%Y") if type(dt) == str else dt)
    tdf['a_week_prior_pay_date'] = tdf['settle_pay_date'].apply(lambda dt: dt + datetime.timedelta(days=-7))
    tdf['Final Amount'] = [float(re.sub(',', '', amt)) if type(amt) != float else 0 for amt in tdf['Transaction Amount Reporting Equivalent']]

    return bdf, tdf, bdf_cols, tdf_cols

def rule_executor(filter_dict, is_bdf_grouped, bdf, tdf, left_key_lst, right_key_lst):

    ## Filteration
    for col, val_list in filter_dict.items():
        bdf = bdf[bdf[col].isin(val_list)]

    ## Aggregation if grouped
    if is_bdf_grouped:
        bdfg = bdf.groupby(['Settled'], as_index=False)['Amount_float'].sum()
        bdf = pd.merge(bdf, bdfg, how='left', on=['Settled'], suffixes=('_l', '_r'))

    ## Joining bank and trasaction dataframes
    bt_df = pd.merge(bdf, tdf, how='inner', left_on=left_key_lst, right_on=right_key_lst)
    bt_df = bt_df[(bt_df['settled_date'] >= bt_df['a_week_prior_pay_date']) & (bt_df['settled_date'] <= bt_df['settle_pay_date'])]

    return bt_df

def wired_calculations(bdf,tdf):

    t_df = tdf[tdf['Wire Reference Number'].notnull()]
    t_df2 = t_df.groupby(['Settle / Pay Date'], as_index=False)['Final Amount'].sum()
    t_df21 = pd.merge(t_df, t_df2, how='left', on=['Settle / Pay Date'], suffixes=('_l', '_r'))

    print(f"From {tdf.shape[0]} records {t_df.shape[0]} have wire info present.")

    ## Mapping misc fees
    ct_df1 = rule_executor({'Action':['Misc Fee']}, False, bdf, t_df, ['Amount'],['Transaction Amount Reporting Equivalent'])
    ct_df1.to_csv("intermediate_v1.csv", index=False)

    # Mapping borrowing and Up front fees
    ct_df2 = rule_executor({'Action': ['Upfront Fee','Borrowing']}, True, bdf, t_df, ['Amount_float_r'],['Final Amount'])
    ct_df2 = ct_df2.rename(columns={'Amount_float_r': 'Amount_float'})
    ct_df2 = ct_df2.drop(columns=['Amount_float_l'])
    ct_df2.to_csv("intermediate_v2.csv", index=False)

    # Mapping Buy and Up front(buy) fees and Mapping Upfront Fee (Buy), Buy and Commitment Fee
    ct_df3 = rule_executor({'Action': ['Buy', 'Upfront Fee (Buy)','Commitment Fee']}, True, bdf, t_df21, ['Amount_float_r'], ['Final Amount_r'])
    ct_df3 = ct_df3.rename(columns={'Final Amount_r': 'Final Amount', 'Amount_float_r': 'Amount_float'})
    ct_df3 = ct_df3.drop(columns=['Final Amount_l', 'Amount_float_l'])
    ct_df3.to_csv("intermediate_v3.csv", index=False)

    # Mapping LIBOR Interest
    ct_df4 = rule_executor({'Action': ['LIBOR Interest']}, True, bdf, t_df21, ['Amount_float_r'], ['Final Amount_r'])
    ct_df4 = ct_df4.rename(columns={'Final Amount_r': 'Final Amount', 'Amount_float_r': 'Amount_float'})
    ct_df4 = ct_df4.drop(columns=['Final Amount_l','Amount_float_l'])
    ct_df4.to_csv("intermediate_v4.csv", index=False)

    # Mapping Buy Equity
    bdf['equity_flag'] = ['Y' if 'Equity' in elm else 'N' for elm in bdf['Source']]
    ct_df5 = rule_executor({'equity_flag':['Y']}, False, bdf, t_df, ['Amount'], ['Transaction Amount Reporting Equivalent'])
    ct_df5.drop(columns=['equity_flag'], inplace=True)
    ct_df5.to_csv("intermediate_v5.csv", index=False)

    wired_df = pd.concat([ct_df1,ct_df2,ct_df3,ct_df4])

    return wired_df

def unwired_calculations(bdf, tdf):

    t_df = tdf[tdf['Wire Reference Number'].isnull()]
    print(f"From {tdf.shape[0]} records {t_df.shape[0]} have no wire info present.")

    ## Mapping LIBOR Borrowing
    t_df1 = t_df[t_df['Detailed Transaction Type Name'] == 'CR-MONEY TRANSFER CREDIT']
    t_df2 = t_df1[t_df1['Transaction Description 1'].str.startswith('ORD CUST:',na= False)]
    t_df1 = t_df1[t_df1['Transaction Description 1'] == 'ORD CUST: 066297311 ING CAPITAL LLC LOANS AGENCY 1133 AVE OF']

    uct_df1 = rule_executor({'Action': ['LIBOR Borrowing']}, False, bdf, t_df1, ['Amount'], ['Transaction Amount Reporting Equivalent'])
    uct_df1.to_csv("intermediate_u1.csv", index=False)

    ## Mapping Interest
    uct_df2 = rule_executor({'Action': ['Interest']}, False, bdf, t_df2, ['Amount'],['Transaction Amount Reporting Equivalent'])
    uct_df2.to_csv("intermediate_u2.csv", index=False)

    unwired_df = pd.concat([uct_df1, uct_df2])

    return unwired_df

def unmatch_extractor(match_df,df_col_list,comparison_df, key):

    compare_df = match_df[df_col_list]
    compare_df = compare_df.drop_duplicates()
    compare_df = compare_df.rename(columns={col: f"{col}_right" for col in df_col_list})
    unmatched_df = pd.merge(comparison_df, compare_df, how='outer', left_on=df_col_list,right_on=list(compare_df.columns), indicator=True)
    unmatched_df = unmatched_df[unmatched_df[key].isnull()]
    return unmatched_df


bdf,tdf, bdf_col_lst, tdf_col_lst = data_preprocessor("WSO Cash Ledger.csv","Settled_Cash_Statement_20231214181247_303365293.csv")

unwired_df = unwired_calculations(bdf, tdf)

wired_df = wired_calculations(bdf, tdf)
print(wired_df.shape[0],unwired_df.shape[0])

matched_df =  pd.concat([wired_df,unwired_df])
matched_df.to_csv("matched_df.csv", index=False)

unamtched_bank_df = unmatch_extractor(matched_df,bdf_col_lst,bdf, 'Traded_right')
unamtched_bank_df.to_csv("unmatched_bank_df.csv", index=False)

unamtched_transaction_df = unmatch_extractor(matched_df,tdf_col_lst,tdf, 'Location Code_right')
unamtched_transaction_df.to_csv("unmatched_transaction_df.csv", index=False)

print(unamtched_bank_df.shape[0],unamtched_transaction_df.shape[0])


