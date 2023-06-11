import pandas as pd
import csv

def main() :
  df_a = pd.read_csv('data/a.csv', header=None, names=["s3Bucket", "objKey", "ETag"])
  df_a_1 = pd.read_csv('data/a_2.csv', header=None, names=["s3Bucket", "objKey", "ETag"])

  df_a = pd.concat([df_a, df_a_1])

  df_b = pd.read_csv('data/b.csv', header=None, names=["s3Bucket", "objKey", "ETag"])

  df_a.set_index(['objKey', 'ETag'], inplace=True)
  df_b.set_index(['objKey', 'ETag'], inplace=True)

  df_join = pd.merge(df_a, df_b, on=['objKey', 'ETag'], how='outer')
  #print(df_join)

  objKeyCopy = df_join.loc[df_join['s3Bucket_y'].isnull()].reset_index()[['s3Bucket_x','objKey']]
  objKeyDelete = df_join.loc[df_join['s3Bucket_x'].isnull()].reset_index()[['s3Bucket_y','objKey']]
  
  print(objKeyDelete)
  print(objKeyCopy)

  objKeyDelete.to_csv("out/del.csv", header=False, index=False, quoting=csv.QUOTE_NONNUMERIC)
  objKeyCopy.to_csv("out/copy.csv", header=False, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__" :
  main()