from audit_mapper_v2 import *
# from audit_mapper import *
import streamlit as st

uploaded_files = st.file_uploader("Choose CSV files", accept_multiple_files=True, type='csv')

if len(uploaded_files) == 2:
    for uploaded_file in uploaded_files:
        bytes_data = uploaded_file.read()
        st.write("filename:", uploaded_file.name)

    # df = audit_calculations(uploaded_files[0].name,uploaded_files[1].name)

    bdf, tdf, bdf_col_lst, tdf_col_lst = data_preprocessor(uploaded_files[0].name,uploaded_files[1].name)

    unwired_df = unwired_calculations(bdf, tdf)
    wired_df = wired_calculations(bdf, tdf)

    st.write(f"Wired dataframe has {wired_df.shape[0]} records mapped while unwired has {unwired_df.shape[0]} records mapped.")

    matched_df = pd.concat([wired_df, unwired_df])
    # matched_df.to_csv("matched_df.csv", index=False)

    unamtched_bank_df = unmatch_extractor(matched_df, bdf_col_lst, bdf, 'Traded_right')
    # unamtched_bank_df.to_csv("unmatched_bank_df.csv", index=False)

    unamtched_transaction_df = unmatch_extractor(matched_df, tdf_col_lst, tdf, 'Location Code_right')
    # unamtched_transaction_df.to_csv("unmatched_transaction_df.csv", index=False)

    print(unamtched_bank_df.shape[0], unamtched_transaction_df.shape[0])

    st.title(':blue[Matched Report]')
    st.dataframe(matched_df)
    st.write(f"There are {matched_df.shape[0]} matched records.")

    st.title(':blue[Unmatched Report: Bank Statement]')
    st.dataframe(unamtched_bank_df)
    st.write(f"There are {unamtched_bank_df.shape[0]} unmatched bank records.")

    st.title(':blue[Unmatched Report: Transaction Statement]')
    st.dataframe(unamtched_transaction_df)
    st.write(f"There are {unamtched_transaction_df.shape[0]} unmatched transaction records.")