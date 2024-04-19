#import dependency
from sentence_transformers import SentenceTransformer, util
import pandas as pd
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from pandas import DataFrame
import json

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False
}  

#Return ClusterID
def return_cluster_id(cluster_name: str) -> int:
    try:
        mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')

        sql_stmt=f"""SELECT TOP 1 ClusterId FROM SA.SemanticCluster WHERE ClusterName='{cluster_name}';"""

        existing_id=mssql_hook.get_records(sql=sql_stmt)

        if existing_id:
            return existing_id[0][0]
        else:
            insert_stmt=f"""INSERT INTO SA.SemanticCluster (ClusterName) VALUES ('{cluster_name}')"""
            mssql_hook.run(sql=insert_stmt)

            #Retrive the new_cluster_id
            new_cluster_id=mssql_hook.get_records(sql=sql_stmt)

            return new_cluster_id[0][0]
    except Exception as e:
        print(f"Error: {e}")

def get_queue_details(**kwargs):

    ti=kwargs["ti"]

    mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')
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
            AND (SA.SemanticQueue.QueueStatus = 20)
        ORDER BY SA.SemanticQueue.Priority DESC;"""

    queue=mssql_hook.get_records(sql=sql_stmt)

    flag=1
    if queue:
        projectid=queue[0][0]
        queueid=queue[0][2]
        transformer=queue[0][4]
        formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        ti.xcom_push('projectid',projectid)
        ti.xcom_push('queueid',queueid)
        ti.xcom_push('transformer',transformer)
        ti.xcom_push('formatted_datetime',formatted_datetime)
        ti.xcom_push('flag',flag)
    else:
        flag=0
        ti.xcom_push('flag',flag)

def queue_is_empty() -> None:

    print("Queue is Empty")

    return None

def update_queue_status_21(**kwargs) -> None:
    ti=kwargs["ti"]
    queueid=ti.xcom_pull(task_ids='get_queue_details_tk',key='queueid')

    mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')
    update_semantic_queue=f"""UPDATE SA.SemanticQueue SET QueueStatus = 21 WHERE  Id={queueid}"""
    mssql_hook.run(sql=update_semantic_queue)

    return None

def get_keywords(**kwargs) -> json:
    ti=kwargs["ti"]

    projectid=ti.xcom_pull(task_ids='get_queue_details_tk',key='projectid')

    mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')
    sql_keywords_stmt=f"""SELECT
                        DISTINCT Keyword,
                        SA.SemanticRawData.SemanticKeywordId
                    FROM
                        SA.SemanticRawData
                    INNER JOIN SA.SemanticKeywordLibrary ON
                        SA.SemanticKeywordLibrary.SemanticKeywordId = SA.SemanticRawData.SemanticKeywordId
                    WHERE
                        ProjectId ={projectid};"""

    rowkeywords=mssql_hook.get_records(sql=sql_keywords_stmt)

    count_keywords=len(rowkeywords)
    ti.xcom_push('count_keywords',count_keywords)

    df = pd.DataFrame(rowkeywords, columns = ['Keyword', 'SemanticKeywordId'])
    df = df.to_json()

    return df

def if_keyword_not_found(**kwargs) -> None:
    ti=kwargs["ti"]

    queueid=ti.xcom_pull(task_ids='get_queue_details_tk',key='queueid')
    projectid=ti.xcom_pull(task_ids='get_queue_details_tk',key='projectid')
    formatted_datetime=ti.xcom_pull(task_ids='get_queue_details_tk',key='formatted_datetime')

    mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')

    update_semantic_queue=f"""UPDATE SA.SemanticQueue SET QueueStatus = 2,
                            CompletedDate=%s WHERE  Id={queueid}"""
    mssql_hook.run(sql=update_semantic_queue, parameters=[formatted_datetime])

    update_semantic_project=f"""UPDATE SA.SemanticProject set ModifiedTS=%s
                            WHERE  Id={projectid}"""
    mssql_hook.run(sql=update_semantic_project, parameters=[formatted_datetime])

    return None

def queue_process_final(**kwargs) -> None:
    ti=kwargs["ti"]

    queueid=ti.xcom_pull(task_ids='get_queue_details_tk',key='queueid')
    projectid=ti.xcom_pull(task_ids='get_queue_details_tk',key='projectid')
    transformer=ti.xcom_pull(task_ids='get_queue_details_tk',key='transformer')
    formatted_datetime=ti.xcom_pull(task_ids='get_queue_details_tk',key='formatted_datetime')
    rowkeywordsdf=ti.xcom_pull(task_ids='get_keywords_tk',key='return_value')

    mssql_hook=MsSqlHook(mssql_conn_id='internaltoolset_mssql')

    df=pd.read_json(rowkeywordsdf)
    cluster_accuracy = 85
    min_cluster_size = 2

    acceptable_confidence = .8

    cluster_name_list = []
    keyword_list = []
    df_all = []

    print("Total Rows : ", df.shape[0])

    corpus_set_all=set(df["Keyword"])
    cluster=True
    cluster_accuracy = cluster_accuracy / 100
    model = SentenceTransformer(transformer)

    while cluster:
        corpus_sentences = list(corpus_set_all)
        check_len = len(corpus_sentences)                
        corpus_embeddings = model.encode(corpus_sentences, batch_size=256, show_progress_bar=False,
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
        # print("Total Unclustered Keywords: ", remaining)

        cluster=False

    #Work on data
    df_new1 = pd.DataFrame(None)
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
        id = row["SemanticKeywordId"]
        clusterName = row["Cluster Name"]
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
    update_queue_stmt=f"""UPDATE SA.SemanticQueue set QueueStatus =30, CompletedDate =%s
                        WHERE  Id={queueid}"""
    mssql_hook.run(sql=update_queue_stmt,parameters=[formatted_datetime])

    print("Complete Update Queue")

    update_project_stmt=f"""UPDATE SA.SemanticProject set ModifiedTS=%s
                        WHERE  Id={projectid}"""
    mssql_hook.run(sql=update_project_stmt,parameters=[formatted_datetime])

    print("Complete Update Project")

    return None
    
def determine_queue_isempty_branch(**kwargs):
    ti=kwargs["ti"]
    flag=ti.xcom_pull(task_ids='get_queue_details_tk',key='flag')

    if flag == 1:
        return 'update_queue_status_21_tk' 
    else:
        return 'queue_is_empty_tk'


def determine_keyword_length_isempty_branch(**kwargs):
    ti=kwargs["ti"]
    count_keywords=ti.xcom_pull(task_ids='get_keywords_tk',key='count_keywords')

    if count_keywords == 0:
        return 'if_keyword_not_found_tk'
    else:
        return 'queue_process_final_tk'


with DAG(
    dag_id='semantic_analysis_v2',
    description='Semantic Analysis of Client',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='*/2 * * * *',
    catchup=False,
    tags=['semantic_analysis', 'Python', 'decorators']
) as dag:
    get_queue_details_task = PythonOperator(
        task_id='get_queue_details_tk',
        python_callable=get_queue_details
    )
    determine_queue_isempty_branch_task = BranchPythonOperator(
        task_id='determine_queue_isempty_branch_tk',
        python_callable=determine_queue_isempty_branch
    )
    queue_is_empty_task = PythonOperator(
        task_id='queue_is_empty_tk',
        python_callable=queue_is_empty
    )
    update_queue_status_21_task = PythonOperator(
        task_id='update_queue_status_21_tk',
        python_callable=update_queue_status_21
    )
    get_keywords_task = PythonOperator(
        task_id='get_keywords_tk',
        python_callable=get_keywords
        # trigger_rule='none_failed'
    )
    determine_keyword_length_isempty_branch_task = BranchPythonOperator(
        task_id='determine_keyword_length_isempty_branch_tk',
        python_callable=determine_keyword_length_isempty_branch
    )
    if_keyword_not_found_task = PythonOperator(
        task_id='if_keyword_not_found_tk',
        python_callable=if_keyword_not_found
    )
    queue_process_final_task = PythonOperator(
        task_id='queue_process_final_tk',
        python_callable=queue_process_final
    )

#set task dependency

get_queue_details_task >> determine_queue_isempty_branch_task >> [ queue_is_empty_task , update_queue_status_21_task ] 
update_queue_status_21_task >> get_keywords_task >> determine_keyword_length_isempty_branch_task
determine_keyword_length_isempty_branch_task >> [ if_keyword_not_found_task , queue_process_final_task ]



