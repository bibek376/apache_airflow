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

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    'email_on_failure': True,
    'email_on_retry': True,
    "email": ["suvash.shrestha@annalect.com"],
    "depends_on_past": False,
}  

#Check and return Cluster Id
def return_cluster_id(dbConnection, clusterName):
    try:
        cursorCluster = dbConnection.cursor(as_dict=True)

        # Check if data already exists
        query = f"SELECT ClusterId FROM SA.SemanticCluster WHERE ClusterName=%s"
        cursorCluster.execute(query, clusterName)
        existing_id = cursorCluster.fetchone()

        if existing_id:
            # Data exists, return the existing ID
            return existing_id["ClusterId"]
        else:
            # Data doesn't exist, insert into the table
            insert_query = f"INSERT INTO SA.SemanticCluster (ClusterName) VALUES (%s)"
            cursorCluster.execute(insert_query, clusterName)
            dbConnection.commit()

            # Retrieve the new ID
            cursorCluster.execute(query, clusterName)
            new_id = cursorCluster.fetchone()
            return new_id["ClusterId"]
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        cursorCluster.close()

def ProcessQueue():

    #Declare SQL Server Details
    server = "192.168.0.27"
    username = "imt"
    password = "Password123!@"
    database = "internaltoolset"

    #Check if there is queue to process
    dbConnection = pymssql.connect(server, username, password, database)
    cursorQueue = dbConnection.cursor(as_dict=True)

    cursorQueue.execute('SELECT SA.SemanticProject.Id AS ProjectId, SA.SemanticProject.ClientId AS ClientId, SA.SemanticQueue.Id AS QueueId, SA.SemanticQueue.QueueStatus AS QueueStatus, SA.SemanticModel.Name AS ModelName FROM SA.SemanticProject INNER JOIN SA.SemanticQueue ON SA.SemanticQueue.ProjectId = SA.SemanticProject.Id INNER JOIN SA.SemanticModel ON SA.SemanticProject.ModelId = SA.SemanticModel.Id WHERE SA.SemanticProject.IsActive=1 AND (SA.SemanticQueue.QueueStatus = 20)')
    queue = cursorQueue.fetchone()

    if queue:
        ##
        projectId = queue["ProjectId"]
        print(projectId)

        ##
        cursorQueue.execute("UPDATE SA.SemanticQueue SET QueueStatus = 21 WHERE  Id=%s", queue['QueueId'])
        dbConnection.commit()
        
        #Get Keywords
        cursoKeywords = dbConnection.cursor(as_dict=True)

        cursoKeywords.execute('SELECT DISTINCT Keyword, SA.SemanticRawData.SemanticKeywordId FROM SA.SemanticRawData INNER JOIN SA.SemanticKeywordLibrary ON SA.SemanticKeywordLibrary.SemanticKeywordId = SA.SemanticRawData.SemanticKeywordId WHERE ProjectId=%s' %(projectId))
        rowkeywords = cursoKeywords.fetchall()

        df_count = rowkeywords
        count_keywords = len(df_count)

        if count_keywords==0:
            print("Raw Keyword Not Found")

            queueId = queue['QueueId']
            projectId = queue['ProjectId']
            cursorQueue.execute("UPDATE SA.SemanticQueue set QueueStatus =2, CompletedDate ='" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "' WHERE  Id=%s", queueId)

            cursorProject = dbConnection.cursor(as_dict=True)
            cursorProject.execute("UPDATE SA.SemanticProject set ModifiedTS ='" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "' WHERE  Id=%s", projectId)
            dbConnection.commit()

            cursorProject.close()
            cursorQueue.close()
            cursoKeywords.close()
            dbConnection.close()
        else:

            #Cluster Variable Declaration

            cluster_accuracy = 85  
            # 0-100 (100 = very tight clusters, but higher percentage of no_cluster groups)
            min_cluster_size = 2  
            # set the minimum size of cluster groups. (Lower number = tighter groups)

            transformer = queue["ModelName"]
            print(transformer)
            #transformer = 'all-mpnet-base-v2'  # provides the best quality
            #transformer = 'all-MiniLM-L6-v2'  # 5 times faster and still offers good quality

            acceptable_confidence = .8

            # store the data
            cluster_name_list = []
            keyword_list = []
            df_all = []

            #Preparing Data
            df_temp = pd.DataFrame(None)
            df_temp = rowkeywords
            count_rows = len(df_temp)
            print("Total Rows : ", count_rows)

            df = pd.DataFrame.from_dict(df_temp)

            ##
            corpus_set = set(df['Keyword'])
            corpus_set_all = corpus_set
            cluster = True

            ##
            cluster_accuracy = cluster_accuracy / 100
            model = SentenceTransformer(transformer)

            ##Getting Cluster Data

            while cluster:

                corpus_sentences = list(corpus_set)
                check_len = len(corpus_sentences)

                corpus_embeddings = model.encode(corpus_sentences, batch_size=256, show_progress_bar=True, convert_to_tensor=True)
                clusters = util.community_detection(corpus_embeddings, min_community_size=min_cluster_size, threshold=cluster_accuracy)

                for keyword, cluster in enumerate(clusters):
                    #print("\nCluster {}, #{} Elements ".format(keyword + 1, len(cluster)))

                    for sentence_id in cluster[0:]:
                        #print("\t", corpus_sentences[sentence_id])
                        keyword_list.append(corpus_sentences[sentence_id])
                        cluster_name_list.append("Cluster {}, #{} Elements ".format(keyword + 1, len(cluster)))

                df_new = pd.DataFrame(None)
                df_new['Cluster Name'] = cluster_name_list
                df_new["Keyword"] = keyword_list
                
                have = set(df_new["Keyword"])

                corpus_set = set(corpus_set_all) - have
                remaining = len(corpus_set)
                print("Total Unclustered Keywords: ", remaining)

                if check_len == remaining:
                    break

            #Work on data
            df_new1 = pd.DataFrame(None)
            df_new1['Cluster Name'] = cluster_name_list
            df_new1["Keyword"] = keyword_list

            df_all.append(df_new1)

            df = df.merge(df_new.drop_duplicates('Keyword'), how='left', on="Keyword")

            df['Length'] = df['Keyword'].astype(str).map(len)
            df = df.sort_values(by="Length", ascending=True)

            df['Cluster Name'] = df.groupby('Cluster Name')['Keyword'].transform('first')
            df.sort_values(['Cluster Name', "Keyword"], ascending=[True, True], inplace=True)

            df['Cluster Name'] = df['Cluster Name'].fillna("zzz_no_cluster")

            print("Start to Update DB")

            cursorUpdate = dbConnection.cursor(as_dict=True)
            remaining = len(df)
            i=1

            for index, row in df.iterrows():
                id = row['SemanticKeywordId']
                clusterName = row['Cluster Name']
                print(i, len(df))
                i=i+1
                cluster_id = return_cluster_id(dbConnection, clusterName)

                if cluster_id is not None:
                    cursorUpdate.execute("UPDATE SA.SemanticRawData set ClusterId = %s WHERE ProjectId=%s and SemanticKeywordId=%s" %(cluster_id, projectId, id))
                    dbConnection.commit()
                else:
                    print("Error in the process :: Insert and getting Cluster Id.")

                ##

            print("Complete Update DB")

            queueId = queue['QueueId']
            cursorQueue.execute("UPDATE SA.SemanticQueue set QueueStatus =30, CompletedDate ='" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "' WHERE  Id=%s", queueId)
            dbConnection.commit()

            print("Complete Update Queue")

            cursorProject = dbConnection.cursor(as_dict=True)
            cursorProject.execute("UPDATE SA.SemanticProject set ModifiedTS ='" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "' WHERE  Id=%s", projectId)
            dbConnection.commit()

            print("Complete Update Project")

            cursorProject.close()
            cursorQueue.close()
            cursorUpdate.close()
            cursoKeywords.close()
            dbConnection.close()
    else:
        print("Queue is Empty")
        cursorQueue.close()
        dbConnection.close()

with DAG('semantic-analysis', schedule_interval=timedelta(minutes=2), default_args=default_args, catchup=False) as dag:

    ProcessQueue= PythonOperator(
        task_id='ProcessQueue',
        python_callable=ProcessQueue
    )

ProcessQueue

