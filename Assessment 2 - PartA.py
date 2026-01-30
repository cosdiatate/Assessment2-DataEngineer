import pandas as pd

def part_online_retail_dataset():
    # load Dataset
    df = pd.read_csv('online_retail_dataset.csv')

    # Coluns Names
    print("Column names")
    print(df.columns)

    # Review the first 10 rows
    print("\nFirst 10 rows")
    print(df.head(10))

    # Review the dataType of each column
    print(df.dtypes)

    # Total de rows and columns
    print(df.shape)

    #DATA CLEANING:

    #missing values
    print("\nMissing values:")
    print(df.isnull().sum())

   # Remove duplicates + drop critical nulls
    df = df.drop_duplicates()
    df = df.dropna(subset=["InvoiceDate", "StockCode", "InvoiceNo"])

    df["ID"] = pd.to_numeric(df["ID"], errors="coerce")
    df = df.dropna(subset=["ID"])
    df["ID"] = df["ID"].astype("int64")

    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["UnitPrice"] = df["UnitPrice"].fillna(df["UnitPrice"].mean())

    # date only (remove hours)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce", dayfirst=True).dt.date
    df = df.dropna(subset=["InvoiceDate"])

    # missing values afer cleaning
    print("\nMissing values after cleaning:")
    print(df.isnull().sum())

    df = df[["ID", "InvoiceNo", "InvoiceDate", "Description", "Quantity", "UnitPrice", "ItemTotal"]]
    df.to_csv("online_retail_dataset_clean.csv", index=False)


    print("Saved online_retail_dataset_clean.csv:", df.shape)
    return df


def part_online_retail_dataset_demographic():
    # load Dataset
    df = pd.read_csv('online_retail_dataset_demographic.csv')

    # Coluns Names
    print("Column names")
    print(df.columns)

    # Review the first 10 rows
    print("\nFirst 10 rows")
    print(df.head(10))

    # Review the dataType of each column
    print(df.dtypes)

    # Total de rows and columns
    print("\nTotal de rows and columns:")
    print(df.shape)

    #missing values
    print("\nMissing values:")
    print(df.isnull().sum())

    df = df.drop_duplicates()
    df = df.dropna(subset=["ID"])

    df["ID"] = pd.to_numeric(df["ID"], errors="coerce")
    df = df.dropna(subset=["ID"])
    df["ID"] = df["ID"].astype("int64")

    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce").fillna(0).astype(int)
    df["Country"] = df["Country"].fillna("Unknown").astype(str).str.strip()

    # missing values afer cleaning
    print("\nMissing values after cleaning:")
    print(df.isnull().sum())

    df.to_csv("online_retail_dataset_demographic_clean.csv", index=False)

    print("Saved online_retail_dataset_demographic_clean.csv:", df.shape)
    return df


def part_integrate_datasets():
    df_txn = pd.read_csv("online_retail_dataset_clean.csv")
    df_demo = pd.read_csv("online_retail_dataset_demographic_clean.csv")

    df_txn["ID"] = df_txn["ID"].astype("int64")
    df_demo["ID"] = df_demo["ID"].astype("int64")

    df_unified = df_txn.merge(df_demo, on="ID", how="left")
    print("Unified before filter:", df_unified.shape)

    # remove empty customers (after join)
    df_unified["CustomerID"] = pd.to_numeric(df_unified["CustomerID"], errors="coerce").fillna(0).astype(int)
    df_unified = df_unified[df_unified["CustomerID"] != 0]

    print("Unified after filter:", df_unified.shape)

    df_unified.to_csv("online_retail_unified.csv", index=False)
    print("Saved online_retail_unified.csv")

    return df_unified


if __name__ == "__main__":
    part_online_retail_dataset()
    part_online_retail_dataset_demographic()
    part_integrate_datasets()