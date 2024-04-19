import sys
import pymssql
from sentence_transformers import SentenceTransformer, util
import pandas as pd
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

# extra packages
from airflow.decorators import dag,task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models import Variable


default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2)
    "depends_on_past": False,
}  


@dag(dag_id='semantic_analysis',
    description='Semantic Analysis of Client',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['semantic_analysis','Python','decorators']
) 


#Return ClusterID
def return_cluster_id(cluster_name: str):
    try:
        mssql_hook=MsSqlHook(mssql_conn_id='FDTestDatabase_mssql')

        sql_stmt=f"""SELECT TOP 1 ClusterId FROM SA.SemanticCluster WHERE ClusterName='{cluster_name}';"""

        existing_id=mssql_hook.get_records(sql=sql_stmt)

        if existing_id[0][0] :
            return existing_id[0][0]
        else:
            insert_stmt=f"""INSERT INTO SA.SemanticCluster (ClusterName) VALUES ('{cluster_name}')"""
            mssql_hook.run(sql=insert_stmt)

            #Retrive the new_id
            new_id=mssql_hook.get_records(sql=sql_stmt)

            return new_cluster_id[0][0]
    except Exception as e:
        print(f"Error: {e}")


def main_function():
    @task
    def process_queeue():
        mssql_hook=MsSqlHook(mssql_conn_id='FDTestDatabase_mssql')
        sql_stmt=f"""
                SELECT TOP 1
                SA.SemanticProject.Id AS ProjectId,
                SA.SemanticProject.ClientId AS ClientId,
                SA.SemanticQueue.Id AS QueueId,
                SA.SemanticQueue.QueueStatus AS QueueStatus,
                SA.SemanticModel.Name AS ModelName
            FROM
                SA.SemanticProject
            INNER JOIN SA.SemanticQueue ON
                SA.SemanticQueue.ProjectId = SA.SemanticProject.Id
            INNER JOIN SA.SemanticModel ON
                SA.SemanticProject.ModelId = SA.SemanticModel.Id
            WHERE
                SA.SemanticProject.IsActive = 1
                AND (SA.SemanticQueue.QueueStatus = 20);"""
        queue=mssql_hook.get_records(sql=sql_stmt)

        projectid=queue[0][0]
        queueid=queue[0][2]
        transformer=queue[0][4]
        formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    

        if queue:
            update_semantic_queue=f"""UPDATE SA.SemanticQueue SET QueueStatus = 21 WHERE  Id={queueid}"""
            mssql_hook.run(sql=update_semantic_queue)

            #Get Keywords
            sql_keywords_stmt=f"""SELECT
                            DISTINCT Keyword,
                            SA.SemanticRawData.SemanticKeywordId
                        FROM
                            SA.SemanticRawData
                        INNER JOIN SA.SemanticKeywordLibrary ON
                            SA.SemanticKeywordLibrary.SemanticKeywordId = SA.SemanticRawData.SemanticKeywordId
                        WHERE
                            ProjectId ={projectid}"""
            rowkeywords=mssql_hook.get_records(sql=sql_keywords_stmt)

            count_keywords=len(rowkeywords)

            if count_keywords==0:
                print("Raw Keyword Not Found")

                update_semantic_queue=f"""UPDATE SA.SemanticQueue SET QueueStatus = 2,
                                        CompletedDate=%s WHERE  Id={queueid}"""
                mssql_hook.run(sql=update_semantic_queue, parameters=[formatted_datetime])

                update_semantic_project=f"""UPDATE SA.SemanticProject set ModifiedTS=%s
                                        WHERE  Id={projectid}"""
                mssql_hook.run(sql=update_semantic_project, parameters=[formatted_datetime])

            else:
                cluster_accuracy = 85
                min_cluster_size = 2

                acceptable_confidence = .8

                # store the data
                cluster_name_list = []
                keyword_list = []
                df_all = []

                df=pd.DataFrame(rowkeywords,columns = ['Keyword', 'SemanticKeywordId'])
                print("Total Rows : ", df.shape[0])

                corpus_set_all=set(df['Keyword'])
                cluster=True

                cluster_accuracy = cluster_accuracy / 100
                model = SentenceTransformer(transformer)

                while cluster:
                    corpus_sentences = list(corpus_set_all)
                    check_len = len(corpus_sentences)                
                    corpus_embeddings = model.encode(corpus_sentences, batch_size=256, show_progress_bar=True,
                                                        convert_to_tensor=True)
                    clusters = util.community_detection(corpus_embeddings, min_community_size=min_cluster_size,
                                                        threshold=cluster_accuracy)

                    for keyword, cluster in enumerate(clusters):
                        for sentence_id in cluster[0:]:
                            keyword_list.append(corpus_sentences[sentence_id])
                            cluster_name_list.append("Cluster {}, #{} Elements ".format(keyword + 1, len(cluster)))

                    df_new = pd.DataFrame(None)
                    df_new["Cluster Name"] = cluster_name_list
                    df_new["Keyword"]=keyword_list

                    have = set(df_new["Keyword"])

                    corpus_set = set(corpus_set_all) - have
                    remaining = len(corpus_set)
                    print("Total Unclustered Keywords: ", remaining)

                    if check_len == remaining:
                        break

                #Work on data
                df_new1["Cluster Name"] = cluster_name_list
                df_new1["Keyword"] = keyword_list

                df_all.append(df_new1)

                df = df.merge(df_new.drop_duplicates("Keyword"), how="left", on="Keyword")

                df["Length"] = df["Keyword"].astype(str).map(len)
                df = df.sort_values(by="Length", ascending=True)

                df["Cluster Name"] = df.groupby("Cluster Name")["Keyword"].transform("first")
                df.sort_values(["Cluster Name", "Keyword"], ascending=[True, True], inplace=True)

                df["Cluster Name"] = df["Cluster Name"].fillna("zzz_no_cluster")

                print("Start to Update DB")
                remaining = len(df)

                i=1

                for index, row in df.iterrows():
                    id = row['SemanticKeywordId']
                    clusterName = row['Cluster Name']
                    print(i, len(df))
                    i=i+1
                    cluster_id = return_cluster_id(clusterName)

                    if cluster_id is not None:
                        sql_stmt=f"""UPDATE SA.SemanticRawData set ClusterId = {cluster_id}
                                    WHERE ProjectId={projectid} and SemanticKeywordId={id}"""
                        mssql_hook.run(sql=sql_stmt)
                    else:
                        print("Error in the process :: Insert and getting Cluster Id.")

                print("Complete Update DB")
                queueid = queue['QueueId']
                update_queue_stmt=f"""UPDATE SA.SemanticQueue set QueueStatus =30, CompletedDate =%s
                                    WHERE  Id={queueid}"""
                mssql_hook.run(sql=update_queue_stmt,parameters=formatted_datetime)

                print("Complete Update Queue")

                update_project_stmt=f"""UPDATE SA.SemanticProject set ModifiedTS=%s
                                    WHERE  Id={projectid}"""
                mssql_hook.run(sql=update_project_stmt,parameters=formatted_datetime)

                print("Complete Update Project")
        else:
            print("Queue is Empty")

main_function()
