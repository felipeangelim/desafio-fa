import pandas as pd
import numpy as np



def build_features(sales_df : pd.DataFrame, comp_prices_df : pd.DataFrame) -> pd.DataFrame:
    
    comp_prices_df : pd.DataFrame = prepare_comp_prices(comp_prices_df)
    sales_df : pd.DataFrame = weight_aggregate_sales_df(sales_df)


    df = pd.merge(sales_df.rename(columns={"date_order":"date"})\
             .groupby(["prod_id",
                            "date",
                            "value_per_item"])\
             .agg({"qty_order" : "sum"})\
             .reset_index(),
          comp_prices_df.groupby(["prod_id","date"])\
              .price.agg(["min","max","mean", "median"])\
              .reset_index(),
         on=["date", "prod_id"]
          )

    #day_agg = df.groupby(["prod_id", "date"])\
    #     .agg({"qty_order" : "sum"})\
    #     .rename(columns={"qty_order" : "qty_day"})\
    #     .reset_index()

    
    #df = pd.merge(df,
    #    day_agg,
    #    on=["prod_id", "date"])#

    day_agg = df.copy()


    day_agg_shift = day_agg
    day_agg_shift["date"]  = day_agg_shift["date"].apply(lambda x: x + pd.Timedelta(days=1))
    day_agg_shift.rename(columns={"qty_order": "qty_day_shift"}, inplace=True)
    day_agg_shift = day_agg_shift[["prod_id", "date", "qty_day_shift"]]
    df = pd.merge(df,
                day_agg_shift,
            on=["prod_id", "date"])
    
    df["diff_min_pct"] = (df.value_per_item - df["min"])/df["min"]
    df["diff_mean_pct"] = (df.value_per_item - df["mean"])/df["mean"]

    df = df.rename(columns={"value_per_item" : "price"})
    return df



def prepare_comp_prices(comp_prices_df : pd.DataFrame) -> pd.DataFrame:
    comp_prices_df.columns = list(map(str.lower, comp_prices_df.columns))
    comp_prices_df["date_extraction"] = pd.to_datetime(comp_prices_df["date_extraction"])
    comp_prices_df["date"] = pd.to_datetime(comp_prices_df["date_extraction"].dt.date)
    comp_prices_df =  comp_prices_df.rename(columns = {"competitor_price" : "price"})[["prod_id", "date", "price"]]
    return comp_prices_df

def weight_aggregate_sales_df(sales_df : pd.DataFrame) -> pd.DataFrame:

    sales_df.columns = list(map(str.lower, sales_df.columns))
    sales_df["date_order"] = pd.to_datetime(sales_df.date_order)
    sales_df["value_per_item"] = sales_df.revenue/sales_df.qty_order

    # Agg by prod id, date order
    sales_df = sales_df.groupby(["prod_id", "date_order", "value_per_item"])\
                        .agg({"qty_order" : "sum"})\
                        .reset_index()

    # Number of orders per day
    day_agg = sales_df.groupby(["prod_id", "date_order"])\
            .agg({"qty_order" : "sum"})\
            .rename(columns={"qty_order" : "qty_day"})\
            .reset_index()

    # Add qty_day column
    sales_df = pd.merge(sales_df,
                        day_agg,
                        on=["prod_id", "date_order"])

    # Weight the value per item by the number of orders in the day
    sales_df["value_per_item"] = sales_df.qty_order/sales_df.qty_day*sales_df.value_per_item
    sales_df = sales_df.groupby(["prod_id", "date_order"])\
                        .agg({"value_per_item" : "sum", "qty_order" : "sum"})\
                        .reset_index()

    return sales_df


if __name__ == "__main__":
    from pathlib import Path
    Path(__file__)  / Path()

    comp_prices_df = pd.read_csv( Path(__file__).parent  / Path("data/comp_prices.csv") )
    sales_df = pd.read_csv( Path(__file__) .parent / Path("data/sales.csv") )

    build_features(sales_df, comp_prices_df)