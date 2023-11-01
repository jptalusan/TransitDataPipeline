mkdir ../data/complete_APC
gsutil -m cp -r gs://apc-historical-datasets/APC_Wego/complete-with-remark-repartitioned-nodupes.parquet/year=2023/month=9/day=12/route_id=18/* ../data/complete_APC/